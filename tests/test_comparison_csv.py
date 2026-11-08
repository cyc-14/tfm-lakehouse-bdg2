"""Synthetic CSV adapter tests; no original measurement files are accessed."""
from pathlib import Path

import pytest

from tfm_lakehouse.comparison_csv import CsvAdapterError, iter_csv_electricity
from tfm_lakehouse.excel_bdg2_comparison import Cell, compare_electricity


T1 = "2020-01-01 00:00:00"
T2 = "2020-01-01 01:00:00"


def source(tmp_path: Path, content: bytes) -> Path:
    path = tmp_path / "synthetic.csv"
    path.write_bytes(content)
    return path


def test_preserves_fields_order_duplicates_and_source_bytes(tmp_path):
    content = (b'\xef\xbb\xbftimestamp,a,b,unselected\r\n'
               b'2020-01-01 00:00:00,"1.00",,ignored\r\n'
               b'2020-01-01 00:00:00,"a,b",NaN,"other, value"\r\n')
    path = source(tmp_path, content)
    rows = list(iter_csv_electricity(path, ["a", "b"]))
    assert rows == [
        {"timestamp": T1, "a": Cell("text", "1.00"), "b": Cell("text", "")},
        {"timestamp": T1, "a": Cell("text", "a,b"), "b": Cell("text", "NaN")},
    ]
    assert path.read_bytes() == content


def test_synthetic_adapter_output_integrates_with_comparison_core(tmp_path):
    path = source(tmp_path, b'timestamp,a,b\n2020-01-01 00:00:00,1.00,\n'
                            b'2020-01-01 01:00:00,-2,0\n')
    csv_rows = list(iter_csv_electricity(path, ["a", "b"]))
    excel_rows = [
        {"timestamp": T1, "a": Cell("text", "1"), "b": Cell("null", None)},
        {"timestamp": T2, "a": Cell("text", "-2"), "b": Cell("text", "0")},
    ]
    first = compare_electricity(csv_rows, excel_rows, ["a", "b"],
                                expected_rows=2, expected_buildings=2)
    assert first["verified"]
    assert first["outcomes"]["numeric_representation_equivalent"] == 1
    assert first["outcomes"]["missing_equivalent"] == 1
    assert first == compare_electricity(csv_rows, excel_rows, ["a", "b"],
                                        expected_rows=2, expected_buildings=2)


def test_duplicate_groups_and_changed_timestamp_reach_core_unchanged(tmp_path):
    path = source(tmp_path, b'timestamp,a,b\n2020-01-01 00:00:00,1,2\n'
                            b'2020-01-01 00:00:00,3,4\n')
    left = list(iter_csv_electricity(path, ["a", "b"]))
    cross_swapped = [
        {"timestamp": T1, "a": Cell("text", "1"), "b": Cell("text", "4")},
        {"timestamp": T1, "a": Cell("text", "3"), "b": Cell("text", "2")},
    ]
    result = compare_electricity(left, cross_swapped, ["a", "b"],
                                 expected_rows=2, expected_buildings=2)
    assert result["duplicate_row_multiset_mismatches"] == 1
    assert result["compared_positions"] == 0
    assert not result["verified"]
    cross_swapped[1]["timestamp"] = T2
    result = compare_electricity(left, cross_swapped, ["a", "b"],
                                 expected_rows=2, expected_buildings=2)
    assert result["missing_timestamp_occurrences_in_excel"] == 1
    assert result["additional_timestamp_occurrences_in_excel"] == 1
    assert not result["verified"]


@pytest.mark.parametrize("content", [
    b'', b'timestamp,a,a\n', b'timestamp,a,A\n', b'timestamp,a,\n',
    b'timestamp,a, a\n', b'timestamp,b\n', b'TIMESTAMP,a\n',
    b'timestamp,a\n2020-01-01 00:00:00\n',
    b'timestamp,a\n2020-01-01 00:00:00,1,extra\n',
    b'timestamp,a\n2020-01-01 00:00:00,"unterminated\n',
    b'timestamp,a\n2020-01-01 00:00:00,\xff\n',
])
def test_invalid_schema_and_malformed_records_fail(tmp_path, content):
    path = source(tmp_path, content)
    with pytest.raises(CsvAdapterError):
        list(iter_csv_electricity(path, ["a"]))
    assert path.read_bytes() == content


@pytest.mark.parametrize("building_ids", [[], ["a", "a"], ["a", "A"],
                                         [" a"], ["timestamp"], [None]])
def test_invalid_building_identifiers_are_rejected(tmp_path, building_ids):
    path = source(tmp_path, b'timestamp,a\n2020-01-01 00:00:00,1\n')
    with pytest.raises(CsvAdapterError):
        list(iter_csv_electricity(path, building_ids))


def test_csv_quoted_newlines_are_parsed_without_modifying_text(tmp_path):
    path = source(tmp_path, b'timestamp,a\n"2020-01-01 00:00:00","1\n2"\n')
    assert list(iter_csv_electricity(path, ["a"])) == [
        {"timestamp": T1, "a": Cell("text", "1\n2")},
    ]


def test_empty_physical_record_is_rejected(tmp_path):
    path = source(tmp_path, b'timestamp,a\n\n2020-01-01 00:00:00,1\n')
    with pytest.raises(CsvAdapterError, match="record width"):
        list(iter_csv_electricity(path, ["a"]))


def test_unquoted_quote_retains_python_csv_reader_behavior(tmp_path):
    path = source(tmp_path, b'timestamp,a\n2020-01-01 00:00:00,a"b\n')
    assert list(iter_csv_electricity(path, ["a"]))[0]["a"] == Cell("text", 'a"b')


def test_partial_iteration_does_not_claim_to_validate_following_records(tmp_path):
    path = source(tmp_path, b'timestamp,a\n2020-01-01 00:00:00,1\nshort\n')
    rows = iter_csv_electricity(path, ["a"])
    assert next(rows)["a"] == Cell("text", "1")
    with pytest.raises(CsvAdapterError):
        next(rows)
