"""Read a bounded XLSX workbook for the electricity comparison core."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from io import BytesIO
from pathlib import Path
import re
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, ZIP_DEFLATED, ZIP_STORED, ZipFile

from openpyxl import load_workbook
from openpyxl.cell.cell import ERROR_CODES
from openpyxl.cell.text import Text
from openpyxl.utils.cell import (
    coordinate_from_string, column_index_from_string, get_column_letter,
)

from tfm_lakehouse.excel_bdg2_comparison import Cell, compare_electricity


class XlsxAdapterError(ValueError):
    """Raised when a workbook cannot be interpreted without ambiguity."""


@dataclass(frozen=True)
class XlsxInput:
    """Rows and representations; use the integrated comparison for verification."""

    records: tuple[dict, ...]
    representations: tuple[dict, ...]
    date_system: str
    source_columns: tuple[str, ...] | None = None
    worksheet: str | None = None


_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_NUMERIC = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_ISO_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\Z")
_MAX_FILE = 64 * 1024 * 1024
_MAX_XML = 32 * 1024 * 1024
_MAX_TOTAL = 128 * 1024 * 1024
_MAX_ENTRIES = 4096
_MAX_ROWS = 20_001


def _xml_parts(data: bytes) -> dict[str, bytes]:
    """Bound archive expansion and reject XML entity declarations before openpyxl."""
    parts = {}
    try:
        with ZipFile(BytesIO(data)) as archive:
            infos = archive.infolist()
            if len(infos) > _MAX_ENTRIES or len({info.filename for info in infos}) != len(infos):
                raise XlsxAdapterError("Too many or duplicate archive entries")
            if sum(info.file_size for info in infos) > _MAX_TOTAL:
                raise XlsxAdapterError("Workbook expands beyond the resource limit")
            for info in infos:
                if info.flag_bits & 1 or info.compress_type not in (ZIP_STORED, ZIP_DEFLATED):
                    raise XlsxAdapterError("Unsupported encrypted or compressed archive entry")
                if info.filename.lower().endswith("vbaproject.bin"):
                    raise XlsxAdapterError("Macro-enabled workbooks are unsupported")
                if info.is_dir():
                    continue
                if not info.filename.lower().endswith((".xml", ".rels")):
                    raise XlsxAdapterError("Non-XML workbook parts are unsupported")
                if info.filename.lower().endswith((".xml", ".rels")):
                    if info.file_size > _MAX_XML:
                        raise XlsxAdapterError("XML part exceeds the resource limit")
                    content = archive.read(info)
                    try:
                        text = content.decode("utf-8-sig")
                    except UnicodeError as exc:
                        raise XlsxAdapterError("Only UTF-8 XML is supported") from exc
                    if "\x00" in text or "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
                        raise XlsxAdapterError("XML declarations or encoding are unsupported")
                    parts[info.filename] = content
    except (BadZipFile, KeyError, RuntimeError, OSError) as exc:
        raise XlsxAdapterError("Invalid XLSX archive") from exc
    return parts


def _sheet_cells(content: bytes, max_columns: int) -> tuple[dict[str, tuple[str, str | None]], int]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise XlsxAdapterError("Invalid worksheet XML") from exc
    cells = {}
    rows = set()
    previous_row = 0
    for row in root.iter(_NS + "row"):
        token = row.get("r", "")
        if not token.isascii() or not token.isdigit():
            raise XlsxAdapterError("Worksheet row number is missing or invalid")
        row_number = int(token)
        if row_number <= previous_row or row_number > _MAX_ROWS or row_number in rows:
            raise XlsxAdapterError("Duplicate or excessive worksheet row")
        rows.add(row_number)
        previous_row = row_number
        for cell in row.findall(_NS + "c"):
            coordinate = cell.get("r", "")
            try:
                column, number = coordinate_from_string(coordinate)
                index = column_index_from_string(column)
            except (ValueError, TypeError) as exc:
                raise XlsxAdapterError("Invalid cell coordinate") from exc
            if (number != row_number or index > max_columns or coordinate in cells
                    or coordinate != f"{column}{number}"):
                raise XlsxAdapterError("Duplicate or unexpected worksheet cell")
            values = cell.findall(_NS + "v")
            formulas = cell.findall(_NS + "f")
            inline = cell.findall(_NS + "is")
            raw_type = cell.get("t", "n")
            if raw_type not in {"n", "s", "str", "inlineStr", "b", "d", "e"}:
                raise XlsxAdapterError("Unsupported worksheet cell type")
            if len(values) > 1 or len(formulas) > 1 or len(inline) > 1:
                raise XlsxAdapterError("Ambiguous worksheet cell structure")
            if not formulas:
                if raw_type in {"s", "b", "d", "e"} and (
                    not values or not values[0].text
                ):
                    raise XlsxAdapterError("Typed cell is missing its value")
                if raw_type == "inlineStr" and values:
                    raise XlsxAdapterError("Inline string contains a value token")
                if raw_type != "inlineStr" and inline:
                    raise XlsxAdapterError("Unexpected inline string content")
            cells[coordinate] = (raw_type, values[0].text if values else None)
    max_row = max(rows, default=0)
    if not rows or rows != set(range(1, max_row + 1)):
        raise XlsxAdapterError("Missing or noncontiguous worksheet rows")
    if root.find(".//" + _NS + "mergeCell") is not None:
        raise XlsxAdapterError("Merged cells are unsupported")
    return cells, max_row


def _check_shared_strings(parts: dict[str, bytes]) -> None:
    """Reject strings that the pinned reader would silently rewrite."""
    try:
        manifest = ET.fromstring(parts["[Content_Types].xml"])
        entries = [node.get("PartName", "").lstrip("/") for node in manifest
                   if node.get("ContentType", "").endswith(".sharedStrings+xml")]
        if len(entries) > 1:
            raise XlsxAdapterError("Ambiguous shared string tables")
        if entries:
            table = ET.fromstring(parts[entries[0]])
            for item in table.iter(_NS + "si"):
                if "x005F_" in Text.from_tree(item).content:
                    raise XlsxAdapterError("Shared string would be normalized by the reader")
    except XlsxAdapterError:
        raise
    except (KeyError, ET.ParseError, TypeError, ValueError) as exc:
        raise XlsxAdapterError("Invalid shared string table or content types") from exc


def _serial_timestamp(token: str, date_system: str) -> tuple[str, dict[str, str]]:
    """Decode seconds within a bounded 15-digit decimal and binary64 envelope.

    This is a conservative interpretation policy, not proof that the source had
    no genuine subsecond component. Both the adjustment and bound are retained.
    """
    if not _NUMERIC.fullmatch(token):
        raise XlsxAdapterError("Invalid date serial")
    try:
        value = Decimal(token)
        if (not value.is_finite() or value < (1 if date_system == "1900" else 0)
                or value > 2_958_465 or value.as_tuple().exponent < -32):
            raise XlsxAdapterError("Unsupported date serial")
        numerator, denominator = value.as_integer_ratio()
        if date_system == "1900" and 60 * denominator <= numerator < 61 * denominator:
            raise XlsxAdapterError("Excel's fictitious 1900-02-29 is unsupported")
        seconds, remainder = divmod(numerator * 86_400, denominator)
        if 2 * remainder > denominator:
            seconds += 1
        error_seconds = Fraction(abs(seconds * denominator - numerator * 86_400), denominator)
        # Excel documents 15-digit precision and binary64. The acceptance
        # envelope allows half a 15-digit decimal quantum and one binary ULP.
        decimal_power = value.adjusted() - 14
        decimal_unit = (Fraction(10 ** decimal_power) if decimal_power >= 0
                        else Fraction(1, 10 ** -decimal_power))
        if numerator:
            binary_exponent = numerator.bit_length() - denominator.bit_length()
            if (numerator < denominator << binary_exponent if binary_exponent >= 0
                    else numerator << -binary_exponent < denominator):
                binary_exponent -= 1
            ulp_power = binary_exponent - 52
        else:
            ulp_power = -1074
        binary_ulp = (Fraction(2 ** ulp_power) if ulp_power >= 0
                      else Fraction(1, 2 ** -ulp_power))
        bound_seconds = (decimal_unit / 2 + binary_ulp) * 86_400
        if error_seconds > bound_seconds:
            raise XlsxAdapterError("Date serial exceeds the second-decoding bound")
        day = seconds // 86_400
        if date_system == "1900" and day == 60:
            raise XlsxAdapterError("Excel's fictitious 1900-02-29 is unsupported")
        base = datetime(1904, 1, 1) if date_system == "1904" else datetime(1899, 12, 31)
        if date_system == "1900" and day > 60:
            seconds -= 86_400
        timestamp = (base + timedelta(seconds=seconds)).isoformat(sep=" ")
        return timestamp, {
            "decoding": "exact" if not error_seconds else "rounded_within_bound",
            "error_seconds": str(error_seconds),
            "bound_seconds": str(bound_seconds),
        }
    except XlsxAdapterError:
        raise
    except (InvalidOperation, OverflowError, ValueError) as exc:
        raise XlsxAdapterError("Unsupported date serial") from exc


def read_xlsx_electricity(
    path: str | Path, building_ids: Sequence[str], *, worksheet: str = "Hoja1",
    expected_rows: int = 17_544,
) -> XlsxInput:
    """Read one worksheet; exact measurement values refer to stored OOXML tokens.

    Formula caches are ignored. A workbook's saved tokens cannot prove that the
    values were unchanged before saving. Inputs exceeding the limits fail closed.
    """
    if (not isinstance(building_ids, Sequence) or isinstance(building_ids, (str, bytes))
            or not building_ids or any(not isinstance(bid, str) or not bid
                                      or bid.strip() != bid or bid.casefold() == "timestamp"
                                      for bid in building_ids)):
        raise XlsxAdapterError("Invalid building identifiers")
    ids = tuple(building_ids)
    if (len({bid.casefold() for bid in ids}) != len(ids)
            or not isinstance(worksheet, str) or not worksheet
            or type(expected_rows) is not int or not 0 < expected_rows < _MAX_ROWS):
        raise XlsxAdapterError("Invalid worksheet or expected dimensions")
    with open(path, "rb") as source:
        data = source.read(_MAX_FILE + 1)
    if len(data) > _MAX_FILE:
        raise XlsxAdapterError("Workbook exceeds the resource limit")
    parts = _xml_parts(data)
    _check_shared_strings(parts)
    try:
        workbook = load_workbook(BytesIO(data), read_only=True, data_only=False,
                                 keep_links=False, keep_vba=False)
    except Exception as exc:
        raise XlsxAdapterError("Workbook could not be read") from exc
    rows = None
    try:
        if worksheet not in workbook.sheetnames:
            raise XlsxAdapterError("Required worksheet is missing")
        sheet = workbook[worksheet]
        sheet_path = sheet._worksheet_path  # read-only worksheet path in pinned openpyxl 3.1.5
        if sheet_path not in parts:
            raise XlsxAdapterError("Worksheet XML is unavailable")
        raw_cells, max_row = _sheet_cells(parts[sheet_path], len(ids) + 1)
        if max_row != expected_rows + 1:
            raise XlsxAdapterError("Unexpected worksheet row count")
        date_system = "1904" if workbook.epoch.year == 1904 else "1900"
        sheet.reset_dimensions()
        rows = sheet.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=len(ids) + 1)
        header_cells = next(rows)
        header = [cell.value for cell in header_cells]
        if (any(not isinstance(item, str) or not item or item.strip() != item for item in header)
                or len({item.casefold() for item in header}) != len(header)
                or set(header) != {"timestamp", *ids}
                or any(cell.data_type != "s" for cell in header_cells)):
            raise XlsxAdapterError("Invalid or unexpected worksheet headers")
        positions = {name: header.index(name) for name in ("timestamp", *ids)}
        records, representations = [], []
        for row_number, row in enumerate(rows, start=2):
            timestamp_cell = row[positions["timestamp"]]
            timestamp_coordinate = f'{get_column_letter(positions["timestamp"] + 1)}{row_number}'
            timestamp_raw = raw_cells.get(timestamp_coordinate, (None, None))
            decoding = {}
            if timestamp_cell.data_type == "s" and isinstance(timestamp_cell.value, str):
                timestamp = timestamp_cell.value
            elif timestamp_cell.data_type == "d" and timestamp_raw[0] == "n" and timestamp_raw[1]:
                timestamp, decoding = _serial_timestamp(timestamp_raw[1], date_system)
            elif timestamp_cell.data_type == "d" and timestamp_raw[0] == "d" and timestamp_raw[1]:
                try:
                    if not _ISO_TIMESTAMP.fullmatch(timestamp_raw[1]):
                        raise ValueError("Incomplete ISO timestamp")
                    value = datetime.fromisoformat(timestamp_raw[1])
                    if value.tzinfo is not None or value.microsecond:
                        raise ValueError("Non-nominal or fractional timestamp")
                    timestamp = value.isoformat(sep=" ")
                except ValueError as exc:
                    raise XlsxAdapterError("Unsupported ISO timestamp") from exc
            else:
                raise XlsxAdapterError("Unsupported timestamp cell")
            record = {"timestamp": timestamp}
            observed = {"timestamp": {"kind": timestamp_cell.data_type,
                                      "raw_type": timestamp_raw[0],
                                      "raw": timestamp_raw[1], **decoding}}
            for bid in ids:
                cell = row[positions[bid]]
                coordinate = f"{get_column_letter(positions[bid] + 1)}{row_number}"
                raw_type, token = raw_cells.get(coordinate, (None, None))
                if cell.data_type == "f":
                    if not isinstance(cell.value, str):
                        raise XlsxAdapterError("Unsupported formula")
                    result = Cell("formula", cell.value)
                elif cell.data_type == "e":
                    if cell.value not in ERROR_CODES:
                        raise XlsxAdapterError("Unsupported error cell")
                    result = Cell("text", cell.value)
                elif cell.data_type == "s":
                    result = Cell("text", cell.value if cell.value is not None else "")
                elif raw_type in {"inlineStr", "str"} and cell.value is None:
                    result = Cell("text", "")
                elif cell.value is None and token is None:
                    result = Cell("null", None)
                elif cell.data_type == "n" and raw_type == "n" and token and _NUMERIC.fullmatch(token):
                    try:
                        number = Decimal(token)
                    except InvalidOperation as exc:
                        raise XlsxAdapterError("Invalid stored numeric token") from exc
                    if not number.is_finite():
                        raise XlsxAdapterError("Nonfinite stored numeric token")
                    result = Cell("exact", number)
                else:
                    raise XlsxAdapterError("Unsupported measurement cell")
                record[bid] = result
                observed[bid] = {"kind": cell.data_type, "raw": token}
            records.append(record)
            representations.append(observed)
        return XlsxInput(tuple(records), tuple(representations), date_system, tuple(header), worksheet)
    except XlsxAdapterError:
        raise
    except Exception as exc:
        raise XlsxAdapterError("Invalid worksheet content") from exc
    finally:
        try:
            if rows is not None:
                rows.close()
        finally:
            workbook.close()


def compare_xlsx_electricity(
    csv_records: Sequence[dict], xlsx: XlsxInput, building_ids: Sequence[str], *,
    expected_rows: int = 17_544, expected_buildings: int = 16, example_limit: int = 20,
) -> dict:
    """Compare decoded labels, keeping adjusted timestamps globally unverified.

    The core operates on strings and cannot see the original XLSX serials. A
    caller bypassing this wrapper must independently inspect representations.
    """
    if not isinstance(xlsx, XlsxInput) or len(xlsx.records) != len(xlsx.representations):
        raise XlsxAdapterError("Invalid XLSX comparison input")
    if xlsx.date_system not in {"1900", "1904"}:
        raise XlsxAdapterError("Invalid XLSX date system")
    for record, observed in zip(xlsx.records, xlsx.representations):
        try:
            metadata = observed["timestamp"]
            kind, raw_type, raw = metadata["kind"], metadata["raw_type"], metadata["raw"]
            if kind == "d" and raw_type == "n" and isinstance(raw, str):
                timestamp, decoding = _serial_timestamp(raw, xlsx.date_system)
                if (record["timestamp"] != timestamp
                        or any(metadata.get(key) != value for key, value in decoding.items())):
                    raise XlsxAdapterError("Timestamp decoding metadata is inconsistent")
            elif kind == "d" and raw_type == "d" and isinstance(raw, str):
                if (not _ISO_TIMESTAMP.fullmatch(raw)
                        or record["timestamp"] != datetime.fromisoformat(raw).isoformat(sep=" ")
                        or "decoding" in metadata):
                    raise XlsxAdapterError("ISO timestamp metadata is inconsistent")
            elif kind == "s" and raw_type in {"s", "str", "inlineStr"}:
                if not isinstance(record["timestamp"], str) or "decoding" in metadata:
                    raise XlsxAdapterError("Text timestamp metadata is inconsistent")
            else:
                raise XlsxAdapterError("Unsupported timestamp metadata")
        except XlsxAdapterError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise XlsxAdapterError("Invalid timestamp metadata") from exc
    result = compare_electricity(
        csv_records, xlsx.records, building_ids, expected_rows=expected_rows,
        expected_buildings=expected_buildings, example_limit=example_limit,
    )
    adjusted = [
        {"row": index + 2, **observed["timestamp"]}
        for index, observed in enumerate(xlsx.representations)
        if observed["timestamp"].get("decoding") == "rounded_within_bound"
    ]
    result["nominal_comparison_verified"] = result["verified"]
    result["timestamp_decoding"] = {
        "adjusted_rows": len(adjusted),
        "examples": adjusted[:example_limit],
    }
    if adjusted:
        result["verified"] = False
    return result
