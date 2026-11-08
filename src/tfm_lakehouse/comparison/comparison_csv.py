"""Read selected electricity CSV columns for the comparison core."""
from __future__ import annotations

from collections.abc import Collection, Iterator, Sequence
import csv
from pathlib import Path
from typing import TextIO

from tfm_lakehouse.comparison.excel_bdg2_comparison import Cell


class CsvAdapterError(ValueError):
    """Raised when a CSV schema or record is unsuitable for comparison."""


def iter_csv_electricity(
    path: str | Path, building_ids: Sequence[str],
) -> Iterator[dict[str, str | Cell]]:
    """Yield parsed records in order, retaining the parser's exact field values.

    The CSV may contain additional source columns. They are validated as part
    of each complete record but are not included in the comparison projection.
    The complete file is validated only if this iterator is fully consumed.
    CSV quoting is syntax, so parser values do not preserve serialized bytes.
    """
    ids = _validated_buildings(building_ids, allow_empty=False)
    with open(path, "r", encoding="utf-8-sig", newline="") as source:
        yield from iter_csv_electricity_stream(source, ids)


def iter_csv_electricity_stream(
    source: TextIO, building_ids: Sequence[str], *,
    allowed_columns: Collection[str] | None = None, allow_empty: bool = False,
) -> Iterator[dict[str, str | Cell]]:
    """Parse a caller-owned stream; optional allowed columns constrain wide inputs."""
    ids = _validated_buildings(building_ids, allow_empty=allow_empty)
    try:
        reader = csv.reader(source, delimiter=",", quotechar='"', strict=True)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise CsvAdapterError("CSV header is missing") from exc
        if (not header or any(not name or name.strip() != name for name in header)
                or len({name.casefold() for name in header}) != len(header)):
            raise CsvAdapterError("Empty, duplicate or ambiguous CSV header")
        if "timestamp" not in header or any(bid not in header for bid in ids):
            raise CsvAdapterError("Required CSV columns are missing")
        if allowed_columns is not None and set(header) - {"timestamp", *allowed_columns}:
            raise CsvAdapterError("Electricity column has no matching metadata identifier")
        positions = {name: header.index(name) for name in ("timestamp", *ids)}
        for row in reader:
            if len(row) != len(header):
                raise CsvAdapterError(
                    f"Inconsistent CSV record width near line {reader.line_num}"
                )
            yield {
                "timestamp": row[positions["timestamp"]],
                **{bid: Cell("text", row[positions[bid]]) for bid in ids},
            }
    except (csv.Error, UnicodeError) as exc:
        raise CsvAdapterError("Malformed or non-UTF-8 CSV") from exc


def _validated_buildings(building_ids: Sequence[str], *, allow_empty: bool) -> tuple[str, ...]:
    if (not isinstance(building_ids, Sequence) or isinstance(building_ids, (str, bytes))
            or (not building_ids and not allow_empty)):
        raise CsvAdapterError("building_ids must be a nonempty sequence")
    ids = tuple(building_ids)
    if any(not isinstance(bid, str) or not bid or bid.strip() != bid
           or bid.casefold() == "timestamp" for bid in ids):
        raise CsvAdapterError("Invalid building identifier")
    if len({bid.casefold() for bid in ids}) != len(ids):
        raise CsvAdapterError("Duplicate or ambiguous building identifiers")
    return ids
