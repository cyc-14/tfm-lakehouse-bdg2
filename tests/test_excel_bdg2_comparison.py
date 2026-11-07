"""Synthetic in-memory tests for the Excel–BDG2 comparison core."""
from copy import deepcopy
from decimal import Decimal

import pytest

from tfm_lakehouse.excel_bdg2_comparison import Cell, ComparisonError, compare_electricity


T1 = "2020-01-01 00:00:00"
T2 = "2020-01-01 01:00:00"
T3 = "2020-01-01 02:00:00"


def text(value):
    return Cell("text", value)


def row(timestamp, a, b):
    return {"timestamp": timestamp, "a": a, "b": b}


def compare(left, right, **kwargs):
    return compare_electricity(left, right, ["a", "b"], expected_rows=2,
                               expected_buildings=2, **kwargs)


def test_exact_decimal_missing_and_original_representations():
    left = [row(T1, text("1.00"), text("")), row(T2, text("0"), text("-2.5"))]
    right = [row(T1, text("1"), Cell("parsed_null", None)),
             row(T2, text("0"), Cell("exact", Decimal("-2.5")))]
    before = deepcopy((left, right))
    result = compare(left, right)
    assert result["verified"] is True
    assert result["outcomes"]["exact_equal"] == 1
    assert result["outcomes"]["numeric_representation_equivalent"] == 2
    assert result["outcomes"]["missing_equivalent"] == 1
    assert result["compared_missing_representation_differences"] == 1
    assert result["sources"]["excel"]["missing_representations"]["parsed_null"] == 1
    assert (left, right) == before


@pytest.mark.parametrize("missing", [Cell("null", None), Cell("parsed_null", None),
                                    text(""), text("NaN")])
def test_r5_missing_values_are_equivalent(missing):
    left = [row(T1, text("NaN"), text("-1")), row(T2, text("0"), text("1"))]
    right = [row(T1, missing, text("-1")), row(T2, text("0"), text("1"))]
    result = compare(left, right)
    assert result["verified"] is True
    assert result["outcomes"]["missing_equivalent"] == 1


@pytest.mark.parametrize("value,expected", [
    (text("2"), "numeric_mismatch"),
    (text("1.00000000000000000000000001"), "numeric_mismatch"),
    (text("bad"), "invalid_non_missing"),
    (text(""), "missingness_mismatch"),
    (Cell("float", 1.0), "representation_ambiguity"),
    (Cell("formula", "=1"), "representation_ambiguity"),
])
def test_mismatches_and_ambiguities_cannot_verify(value, expected):
    left = [row(T1, text("1"), text("0")), row(T2, text("1"), text("1"))]
    right = [row(T1, value, text("0")), row(T2, text("1"), text("1"))]
    result = compare(left, right)
    assert result["verified"] is False
    assert result["outcomes"][expected] == 1


def test_float_comparison_does_not_infer_equality_even_when_both_floats_match():
    left = [row(T1, Cell("float", 0.1), text("1")), row(T2, text("1"), text("1"))]
    result = compare(left, deepcopy(left))
    assert result["outcomes"]["representation_ambiguity"] == 1
    assert not result["verified"]


def test_decimal_values_with_different_exponents_keep_representation_distinct():
    left = [row(T1, Cell("exact", Decimal("1.00")), text("1")),
            row(T2, text("1"), text("1"))]
    right = [row(T1, Cell("exact", Decimal("1.0")), text("1")), left[1]]
    result = compare(left, right)
    assert result["outcomes"]["numeric_representation_equivalent"] == 1
    assert result["verified"]


def test_coverage_and_order_are_separate_and_invalid_labels_are_not_aligned():
    left = [row(T1, text("1"), text("1")), row(T2, text("2"), text("2"))]
    reordered = compare(left, list(reversed(left)))
    assert reordered["timestamp_coverage_equal"]
    assert not reordered["timestamp_order_equal"]
    assert not reordered["verified"]
    changed = compare(left, [left[0], row(T3, text("2"), text("2"))])
    assert changed["missing_timestamp_occurrences_in_excel"] == 1
    assert changed["additional_timestamp_occurrences_in_excel"] == 1
    assert changed["uncompared_positions"] == {"csv": 2, "excel": 2}
    invalid = compare(left, [left[0], row("2020-02-30 01:00:00", text("2"), text("2"))])
    assert invalid["sources"]["excel"]["invalid_timestamps"] == 1
    assert invalid["timestamp_coverage_equal"] is None
    assert not invalid["verified"]
    different_invalid = compare(
        [left[0], row("bad1", text("2"), text("2"))],
        [left[0], row("bad2", text("2"), text("2"))],
    )
    assert different_invalid["timestamp_coverage_equal"] is None
    both_reordered = compare(list(reversed(left)), list(reversed(left)))
    assert both_reordered["timestamp_order_equal"]
    assert both_reordered["sources"]["csv"]["non_hourly_transitions"] == 1
    assert not both_reordered["verified"]
    both_with_gap = [left[0], row(T3, text("2"), text("2"))]
    assert not compare(both_with_gap, both_with_gap)["verified"]


def test_duplicate_groups_match_whole_rows_and_preserve_multiplicity():
    left = [row(T1, text("1"), text("2")), row(T1, text("3"), text("4"))]
    reversed_rows = compare(left, list(reversed(left)))
    assert reversed_rows["duplicate_row_multiset_mismatches"] == 0
    assert reversed_rows["compared_positions"] == 0
    assert reversed_rows["duplicate_alignment_ambiguities"] == 1
    assert not reversed_rows["verified"]
    cross_swapped = [row(T1, text("1"), text("4")), row(T1, text("3"), text("2"))]
    result = compare(left, cross_swapped)
    assert result["duplicate_row_multiset_mismatches"] == 1
    assert result["compared_positions"] == 0
    assert result["uncompared_positions"] == {"csv": 4, "excel": 4}
    assert not result["verified"]


def test_duplicate_mismatch_examples_show_unmatched_rows():
    left = [row(T1, text("1"), text("2")), row(T1, text("3"), text("4"))]
    right = [left[0], row(T1, text("5"), text("6"))]
    result = compare(left, right)
    example = next(item for item in result["examples"]
                   if item["type"] == "duplicate_row_multiset_mismatch")
    assert example["matched_complete_rows"] == 1
    assert example["unmatched_csv_rows"] == example["unmatched_excel_rows"] == 1
    assert example["csv_sample"]["a"]["value_repr"] == repr("3")
    assert example["excel_sample"]["a"]["value_repr"] == repr("5")


def test_duplicate_representation_permutations_are_not_assigned_to_pairs():
    left = [row(T1, text(""), text("1")), row(T1, text("NaN"), text("1"))]
    right = list(reversed(left))
    result = compare(left, right)
    assert result["duplicate_row_multiset_mismatches"] == 0
    assert result["compared_missing_representation_differences"] == 0
    assert result["outcomes_scope"] == "compared_positions_only"
    assert result["compared_positions"] == 0
    assert not result["verified"]


def test_ambiguous_duplicate_group_never_produces_verified_matches():
    rows = [row(T1, Cell("float", 1.0), text("2")), row(T1, text("3"), text("4"))]
    result = compare(rows, deepcopy(rows))
    assert result["compared_positions"] == 0
    assert result["duplicate_alignment_ambiguities"] == 1
    assert not result["verified"]


@pytest.mark.parametrize("bad_rows,building_ids", [
    ([{"timestamp": T1, "a": text("1")}], ["a", "b"]),
    ([{"timestamp": T1, "a": text("1"), "b": text("2"), "c": text("3")}], ["a", "b"]),
    ([row(T1, text("1"), text("2"))], ["a", "A"]),
    ([row(T1, text("1"), text("2"))], ["a", "wrong"]),
    ([row(T1, "1", text("2"))], ["a", "b"]),
])
def test_invalid_identifiers_columns_or_cell_schema_fail(bad_rows, building_ids):
    with pytest.raises(ComparisonError):
        compare_electricity(bad_rows, bad_rows, building_ids,
                            expected_rows=1, expected_buildings=2)


def test_incorrect_row_count_and_bounded_deterministic_examples():
    rows = [row(T1, text("bad"), text("2"))]
    before = deepcopy(rows)
    first = compare(rows, rows, example_limit=1)
    assert first == compare(rows, rows, example_limit=1)
    assert len(first["examples"]) == 1
    assert first["sources"]["csv"]["rows"] == 1
    assert not first["verified"]
    assert rows == before
