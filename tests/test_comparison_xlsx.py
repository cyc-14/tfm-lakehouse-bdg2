"""Synthetic XLSX fixtures for the read-only comparison adapter."""
from datetime import datetime
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from openpyxl import Workbook
from openpyxl.utils.datetime import MAC_EPOCH

from tfm_lakehouse.comparison_csv import iter_csv_electricity
from tfm_lakehouse.comparison_xlsx import (
    XlsxAdapterError, compare_xlsx_electricity, read_xlsx_electricity,
)
from tfm_lakehouse.excel_bdg2_comparison import Cell, compare_electricity


def workbook_file(tmp_path, rows, *, epoch=None, sheet="Hoja1", iso_dates=False):
    workbook = Workbook()
    workbook.iso_dates = iso_dates
    workbook.active.title = sheet
    if epoch is not None:
        workbook.epoch = epoch
    for row in rows:
        workbook.active.append(row)
    path = tmp_path / "synthetic.xlsx"
    workbook.save(path)
    workbook.close()
    return path


def read(path, *, expected_rows=2, ids=("a", "b")):
    return read_xlsx_electricity(path, ids, expected_rows=expected_rows)


def rewrite_sheet(path: Path, original: bytes, replacement: bytes):
    with ZipFile(path) as archive:
        parts = [(item.filename, archive.read(item)) for item in archive.infolist()]
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for name, content in parts:
            if name == "xl/worksheets/sheet1.xml":
                assert original in content
                content = content.replace(original, replacement, 1)
            archive.writestr(name, content)


def make_shared_string(path: Path, text: str):
    with ZipFile(path) as archive:
        parts = [(item.filename, archive.read(item)) for item in archive.infolist()]
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for name, content in parts:
            if name == "xl/worksheets/sheet1.xml":
                original = b'<c r="B2" t="inlineStr"><is><t>' + text.encode() + b'</t></is></c>'
                assert original in content
                content = content.replace(original, b'<c r="B2" t="s"><v>0</v></c>')
            elif name == "[Content_Types].xml":
                declaration = (b'<Override PartName="/xl/sharedStrings.xml" ContentType="'
                               b'application/vnd.openxmlformats-officedocument.spreadsheetml.'
                               b'sharedStrings+xml"/>')
                content = content.replace(b"</Types>", declaration + b"</Types>")
            archive.writestr(name, content)
        table = (b'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                 b'<si><t>' + text.replace("x", "&#120;", 1).encode() + b'</t></si></sst>')
        archive.writestr("xl/sharedStrings.xml", table)


def test_csv_and_xlsx_integration_and_source_immutability(tmp_path):
    path = workbook_file(tmp_path, [
        ["timestamp", "a", "b"], ["2020-01-01 00:00:00", 1, None],
        ["2020-01-01 01:00:00", -2, "NaN"],
    ])
    before = path.read_bytes()
    csv_path = tmp_path / "synthetic.csv"
    csv_data = b'timestamp,a,b\n2020-01-01 00:00:00,1.00,\n2020-01-01 01:00:00,-2,NaN\n'
    csv_path.write_bytes(csv_data)
    xlsx = read(path)
    left = list(iter_csv_electricity(csv_path, ["a", "b"]))
    result = compare_electricity(left, xlsx.records, ["a", "b"],
                                 expected_rows=2, expected_buildings=2)
    assert result["verified"]
    assert xlsx.records[0]["a"] == Cell("exact", Decimal("1"))
    assert xlsx.records[0]["b"] == Cell("null", None)
    assert xlsx.records[1]["b"] == Cell("text", "NaN")
    assert result == compare_electricity(left, read(path).records, ["a", "b"],
                                         expected_rows=2, expected_buildings=2)
    assert path.read_bytes() == before and csv_path.read_bytes() == csv_data


def test_stored_numeric_token_is_not_rounded_through_float(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", 1, 0]])
    rewrite_sheet(path, b"<v>1</v>", b"<v>1.00000000000000000001</v>")
    result = read(path, expected_rows=1)
    assert result.representations[0]["a"]["raw"] == "1.00000000000000000001"
    assert result.records[0]["a"] == Cell("exact", Decimal("1.00000000000000000001"))
    other = ({"timestamp": "2020-01-01 00:00:00", "a": Cell("text", "1"),
              "b": Cell("text", "0")},)
    comparison = compare_electricity(other, result.records, ["a", "b"],
                                     expected_rows=1, expected_buildings=2)
    assert comparison["outcomes"]["numeric_mismatch"] == 1
    assert not comparison["verified"]


@pytest.mark.parametrize("epoch", [None, MAC_EPOCH])
def test_both_date_systems_convert_exact_nominal_serials(tmp_path, epoch):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1, 0, 0), 0, -1],
                                   [datetime(2020, 1, 1, 12, 0), 1, 2]], epoch=epoch)
    result = read(path)
    assert [row["timestamp"] for row in result.records] == [
        "2020-01-01 00:00:00", "2020-01-01 12:00:00"]
    assert result.date_system == ("1904" if epoch else "1900")
    assert result.representations[0]["timestamp"]["raw"]


def test_1900_fake_leap_day_and_fractional_second_are_rejected(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(1900, 2, 28), 1, 2]])
    rewrite_sheet(path, b"<v>59</v>", b"<v>60</v>")
    with pytest.raises(XlsxAdapterError, match="fictitious"):
        read(path, expected_rows=1)
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1), 1, 2]])
    rewrite_sheet(path, b"<v>43831</v>", b"<v>43831.0000000001</v>")
    with pytest.raises(XlsxAdapterError, match="second"):
        read(path, expected_rows=1)


def test_ordinary_hour_with_inexact_stored_serial_is_recorded(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1, 1, 0), 1, 2]])
    result = read(path, expected_rows=1)
    assert result.records[0]["timestamp"] == "2020-01-01 01:00:00"
    assert result.representations[0]["timestamp"]["decoding"] == "rounded_within_bound"
    assert result.representations[0]["timestamp"]["raw"]


@pytest.mark.parametrize("epoch", [None, MAC_EPOCH])
@pytest.mark.parametrize("hour", [0, 1, 2, 12, 23])
def test_hourly_dates_for_both_excel_date_systems(tmp_path, epoch, hour):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 2, 29, hour), 1, 2]], epoch=epoch)
    result = read(path, expected_rows=1)
    assert result.records[0]["timestamp"] == f"2020-02-29 {hour:02}:00:00"


@pytest.mark.parametrize("token,accepted", [
    ("43831.00000000005", True), ("43831.00000000006", False),
    ("43830.99999999995", True), ("43830.99999999994", False),
    ("43831.000001", False),
])
def test_boundaries_and_genuine_subsecond_values(tmp_path, token, accepted):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1), 1, 2]])
    rewrite_sheet(path, b"<v>43831</v>", f"<v>{token}</v>".encode())
    if accepted:
        result = read(path, expected_rows=1)
        assert result.records[0]["timestamp"] == "2020-01-01 00:00:00"
        observed = result.representations[0]["timestamp"]
        assert observed["raw"] == token and observed["decoding"] == "rounded_within_bound"
    else:
        with pytest.raises(XlsxAdapterError, match="bound"):
            read(path, expected_rows=1)


@pytest.mark.parametrize("token", ["59.99999999999999", "60.99999999999999"])
def test_rounding_cannot_enter_or_leave_excel_fictitious_day(tmp_path, token):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(1900, 2, 28), 1, 2]])
    rewrite_sheet(path, b"<v>59</v>", f"<v>{token}</v>".encode())
    with pytest.raises(XlsxAdapterError, match="fictitious"):
        read(path, expected_rows=1)


def test_hourly_serial_decoding_does_not_round_consumption(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1, 1), 1, 0]])
    rewrite_sheet(path, b'<c r="B2" t="n"><v>1</v></c>',
                  b'<c r="B2" t="n"><v>1.00000000000000000001</v></c>')
    result = read(path, expected_rows=1)
    csv_rows = ({"timestamp": "2020-01-01 01:00:00", "a": Cell("text", "1"),
                 "b": Cell("text", "0")},)
    comparison = compare_electricity(csv_rows, result.records, ["a", "b"],
                                     expected_rows=1, expected_buildings=2)
    assert comparison["outcomes"]["numeric_mismatch"] == 1
    assert not comparison["verified"]


def test_integrated_comparison_cannot_verify_subsecond_value_within_bound(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1), 1, 2]])
    rewrite_sheet(path, b"<v>43831</v>", b"<v>43831.00000000005</v>")
    xlsx = read(path, expected_rows=1)
    csv_rows = ({"timestamp": "2020-01-01 00:00:00", "a": Cell("text", "1"),
                 "b": Cell("text", "2")},)
    result = compare_xlsx_electricity(csv_rows, xlsx, ["a", "b"],
                                      expected_rows=1, expected_buildings=2)
    assert result["nominal_comparison_verified"]
    assert not result["verified"]
    assert result["timestamp_decoding"]["adjusted_rows"] == 1
    assert result["timestamp_decoding"]["examples"][0]["raw"] == "43831.00000000005"


def test_integrated_comparison_allows_verified_exact_timestamp(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1), 1, 2]])
    xlsx = read(path, expected_rows=1)
    csv_rows = ({"timestamp": "2020-01-01 00:00:00", "a": Cell("text", "1"),
                 "b": Cell("text", "2")},)
    result = compare_xlsx_electricity(csv_rows, xlsx, ["a", "b"],
                                      expected_rows=1, expected_buildings=2)
    assert result["verified"] and result["timestamp_decoding"]["adjusted_rows"] == 0


@pytest.mark.parametrize("change", ["missing", "unknown", "error", "label"])
def test_integrated_comparison_rejects_inconsistent_timestamp_metadata(tmp_path, change):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1, 1), 1, 2]])
    xlsx = read(path, expected_rows=1)
    records = deepcopy(xlsx.records)
    observed = deepcopy(xlsx.representations)
    if change == "missing":
        del observed[0]["timestamp"]["decoding"]
    elif change == "unknown":
        observed[0]["timestamp"]["decoding"] = "unknown"
    elif change == "error":
        observed[0]["timestamp"]["error_seconds"] = "0"
    else:
        records[0]["timestamp"] = "2020-01-01 02:00:00"
    modified = replace(xlsx, records=records, representations=observed)
    with pytest.raises(XlsxAdapterError, match="inconsistent"):
        compare_xlsx_electricity(records, modified, ["a", "b"],
                                 expected_rows=1, expected_buildings=2)


def test_duplicate_hourly_serials_preserve_multiplicity(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1, 1), 1, 2],
                                   [datetime(2020, 1, 1, 1), 3, 4]])
    result = read(path)
    assert [row["timestamp"] for row in result.records] == [
        "2020-01-01 01:00:00", "2020-01-01 01:00:00"]
    comparison = compare_electricity(result.records, result.records, ["a", "b"],
                                     expected_rows=2, expected_buildings=2)
    assert comparison["duplicate_alignment_ambiguities"] == 1
    assert not comparison["verified"]


@pytest.mark.parametrize("token", ["2020-01-01", "2020-01-01T00:00"])
def test_incomplete_iso_dates_are_not_filled_in(tmp_path, token):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1), 1, 2]], iso_dates=True)
    rewrite_sheet(path, b"2020-01-01T00:00:00", token.encode())
    with pytest.raises(XlsxAdapterError):
        read(path, expected_rows=1)


def test_complete_iso_date_is_converted_without_timezone(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   [datetime(2020, 1, 1), 1, 2]], iso_dates=True)
    assert read(path, expected_rows=1).records[0]["timestamp"] == "2020-01-01 00:00:00"


def test_formulas_and_errors_do_not_verify_even_with_formula_cache(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", "=1+1", "#DIV/0!"]])
    rewrite_sheet(path, b"<f>1+1</f><v />", b"<f>1+1</f><v>2</v>")
    result = read(path, expected_rows=1)
    assert result.records[0]["a"] == Cell("formula", "=1+1")
    assert result.representations[0]["b"]["kind"] == "e"
    assert result.records[0]["b"] == Cell("text", "#DIV/0!")
    other = ({"timestamp": "2020-01-01 00:00:00", "a": Cell("text", "2"),
              "b": Cell("text", "1")},)
    compared = compare_electricity(other, result.records, ["a", "b"],
                                   expected_rows=1, expected_buildings=2)
    assert compared["outcomes"]["representation_ambiguity"] == 1
    assert compared["outcomes"]["invalid_non_missing"] == 1
    assert not compared["verified"]


def test_empty_string_and_null_are_preserved_as_distinct_representations(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", "", None]])
    result = read(path, expected_rows=1)
    assert result.records[0]["a"] == Cell("text", "")
    assert result.records[0]["b"] == Cell("null", None)


def test_invalid_error_token_cannot_be_accepted_as_numeric(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", "#DIV/0!", 1]])
    rewrite_sheet(path, b"<v>#DIV/0!</v>", b"<v>0</v>")
    with pytest.raises(XlsxAdapterError, match="error cell"):
        read(path, expected_rows=1)


@pytest.mark.parametrize("replacement", [
    b'<c r="B2" t="s"/>', b'<c r="B2" t="b"/>', b'<c r="B2" t="d"/>',
    b'<c r="B2" t="inlineStr"><v>1</v></c>',
    b'<c r="B2" t="unknown"/>',
])
def test_malformed_typed_cells_are_not_classified_as_missing(tmp_path, replacement):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", 1, 2]])
    rewrite_sheet(path, b'<c r="B2" t="n"><v>1</v></c>', replacement)
    with pytest.raises(XlsxAdapterError):
        read(path, expected_rows=1)


def test_unsafe_xml_is_rejected_before_workbook_parsing(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", 1, 2]])
    rewrite_sheet(path, b"<worksheet ", b'<!DOCTYPE worksheet [<!ENTITY x "1">]><worksheet ')
    with pytest.raises(XlsxAdapterError, match="XML declarations"):
        read(path, expected_rows=1)


def test_non_xml_extension_cannot_bypass_archive_validation(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", 1, 2]])
    with ZipFile(path, "a") as archive:
        archive.writestr("xl/workbook.bin", '<!DOCTYPE x [<!ENTITY e "1">]><x/>')
    with pytest.raises(XlsxAdapterError, match="Non-XML"):
        read(path, expected_rows=1)


@pytest.mark.parametrize("text", ["x005F_1", "x005F_NaN"])
def test_shared_strings_that_openpyxl_would_normalize_are_rejected(tmp_path, text):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", text, 1]])
    make_shared_string(path, text)
    with pytest.raises(XlsxAdapterError, match="normalized"):
        read(path, expected_rows=1)


def test_reordered_and_missing_timestamps_are_reported(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 01:00:00", 1, 2],
                                   ["2020-01-01 00:00:00", 3, 4]])
    xlsx = read(path)
    left = tuple(reversed(xlsx.records))
    result = compare_electricity(left, xlsx.records, ["a", "b"],
                                 expected_rows=2, expected_buildings=2)
    assert result["timestamp_coverage_equal"] and not result["timestamp_order_equal"]
    changed = ({**left[0], "timestamp": "2020-01-01 02:00:00"}, left[1])
    result = compare_electricity(changed, xlsx.records, ["a", "b"],
                                 expected_rows=2, expected_buildings=2)
    assert result["missing_timestamp_occurrences_in_excel"] == 1
    assert result["additional_timestamp_occurrences_in_excel"] == 1
    assert not result["verified"]


@pytest.mark.parametrize("headers", [["timestamp", "a", "a"],
                                    ["timestamp", "a", "A"],
                                    ["timestamp", "a", "c"],
                                    ["timestamp", "a"]])
def test_invalid_headers_are_rejected(tmp_path, headers):
    path = workbook_file(tmp_path, [headers, ["2020-01-01 00:00:00", 1, 2]])
    with pytest.raises(XlsxAdapterError):
        read(path, expected_rows=1)


def test_missing_sheet_wrong_row_count_and_bad_archive_fail(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", 1, 2]], sheet="Other")
    with pytest.raises(XlsxAdapterError, match="worksheet"):
        read(path, expected_rows=1)
    with pytest.raises(XlsxAdapterError, match="row count"):
        read_xlsx_electricity(path, ["a", "b"], worksheet="Other", expected_rows=2)
    path.write_bytes(b"invalid zip")
    with pytest.raises(XlsxAdapterError):
        read(path, expected_rows=1)


def test_duplicate_rows_are_left_to_multiset_comparison(tmp_path):
    path = workbook_file(tmp_path, [["timestamp", "a", "b"],
                                   ["2020-01-01 00:00:00", 1, 2],
                                   ["2020-01-01 00:00:00", 3, 4]])
    xlsx = read(path)
    cross = ({"timestamp": "2020-01-01 00:00:00", "a": Cell("text", "1"),
              "b": Cell("text", "4")},
             {"timestamp": "2020-01-01 00:00:00", "a": Cell("text", "3"),
              "b": Cell("text", "2")})
    result = compare_electricity(cross, xlsx.records, ["a", "b"],
                                 expected_rows=2, expected_buildings=2)
    assert result["duplicate_row_multiset_mismatches"] == 1
    assert not result["verified"]
