"""Synthetic tests for the strict BDG2 comparison profile."""
from datetime import datetime, timedelta
from fractions import Fraction

import pytest
from openpyxl import Workbook

from tfm_lakehouse.comparison.bdg2_comparison import (
    APPROVED_BUILDINGS, compare_bdg2, validate_bdg2_calendar,
)
from tfm_lakehouse.comparison.comparison_csv import iter_csv_electricity
from tfm_lakehouse.comparison.comparison_xlsx import XlsxInput, read_xlsx_electricity
from tfm_lakehouse.comparison.excel_bdg2_comparison import Cell, ComparisonError


@pytest.fixture(scope="module")
def labels():
    current = datetime(2016, 1, 1)
    end = datetime(2018, 1, 1)
    result = []
    while current < end:
        result.append(current.strftime("%Y-%m-%d %H:%M:%S"))
        current += timedelta(hours=1)
    return tuple(result)


@pytest.fixture(scope="module")
def complete(labels):
    zero = Cell("text", "0")
    records = tuple({"timestamp": label, **dict.fromkeys(APPROVED_BUILDINGS, zero)}
                    for label in labels)
    timestamp_metadata = {"timestamp": {"kind": "s", "raw_type": "inlineStr", "raw": None}}
    observations = (timestamp_metadata,) * len(records)
    xlsx = XlsxInput(records, observations, "1900", ("timestamp", *APPROVED_BUILDINGS), "Hoja1")
    return records, xlsx


def test_approved_identifiers_are_the_exact_ordered_list():
    assert APPROVED_BUILDINGS == (
        "Robin_lodging_Dorthy", "Robin_lodging_Pricilla", "Robin_lodging_Armand",
        "Fox_lodging_Wallace", "Fox_lodging_Winifred", "Fox_lodging_Warren",
        "Peacock_lodging_Chloe", "Peacock_lodging_Terrie", "Peacock_lodging_Nova",
        "Bull_lodging_Leonard", "Bull_lodging_Terence", "Bull_lodging_Lettie",
        "Bull_lodging_Carie", "Bull_lodging_Jeremiah", "Bull_lodging_Anibal",
        "Bull_lodging_Danielle",
    )


@pytest.mark.parametrize("ids,reason", [
    (APPROVED_BUILDINGS[:-1], "missing="),
    (APPROVED_BUILDINGS[:-1] + ("unexpected",), "unexpected="),
    (APPROVED_BUILDINGS[:-1] + (APPROVED_BUILDINGS[0],), "duplicates="),
    (tuple(reversed(APPROVED_BUILDINGS)), "order_mismatch=True"),
])
def test_invalid_selection_is_rejected_before_comparison(ids, reason):
    with pytest.raises(ComparisonError, match=reason):
        compare_bdg2([], XlsxInput((), (), "1900"), ids)


def test_selection_failure_counts_all_ids_and_bounds_examples():
    ids = APPROVED_BUILDINGS + ("unexpected_a", "unexpected_b")
    with pytest.raises(ComparisonError) as caught:
        compare_bdg2([], XlsxInput((), (), "1900"), ids, example_limit=1)
    assert "unexpected_count=2" in str(caught.value)
    assert "unexpected_a" in str(caught.value) and "unexpected_b" not in str(caught.value)
    with pytest.raises(ComparisonError) as duplicates:
        compare_bdg2([], XlsxInput((), (), "1900"), APPROVED_BUILDINGS + (ids[0], ids[0]))
    assert "duplicate_ids_count=1" in str(duplicates.value)
    assert "duplicate_excess_occurrences=2" in str(duplicates.value)


def test_full_grid_includes_leap_day_and_exact_boundaries(labels):
    assert len(labels) == 17_544
    assert labels[0] == "2016-01-01 00:00:00"
    assert labels[-1] == "2017-12-31 23:00:00"
    assert "2016-02-29 23:00:00" in labels
    result = validate_bdg2_calendar([{"timestamp": label} for label in labels])
    assert result["coverage_matches"] and result["order_matches"]
    assert result["missing_hours"] == result["unexpected_occurrences"] == 0


@pytest.mark.parametrize("change", ["start", "end", "shift", "duplicate", "additional", "reorder"])
def test_calendar_reports_coverage_and_order_independently(labels, change):
    altered = list(labels)
    if change == "start":
        altered[0] = "2015-12-31 23:00:00"
    elif change == "end":
        altered[-1] = "2018-01-01 00:00:00"
    elif change == "shift":
        altered = [(datetime.fromisoformat(label) + timedelta(days=366)).isoformat(sep=" ")
                   for label in labels]
    elif change == "duplicate":
        altered[50] = altered[49]
    elif change == "additional":
        altered.append("2018-01-01 00:00:00")
    else:
        altered[10], altered[11] = altered[11], altered[10]
    records = [{"timestamp": label} for label in altered]
    first = validate_bdg2_calendar(records, example_limit=2)
    assert first == validate_bdg2_calendar(records, example_limit=2)
    assert all(len(value) <= 2 for value in first["examples"].values())
    assert not first["order_matches"]
    if change == "reorder":
        assert first["coverage_matches"] and first["order_mismatched_positions"] == 2
    elif change == "duplicate":
        assert first["missing_hours"] == 1 and first["duplicate_excess_occurrences"] == 1
    elif change == "additional":
        assert first["missing_hours"] == 0 and first["unexpected_occurrences"] == 1
    elif change == "shift":
        assert len(altered) == 17_544 and first["missing_hours"] == 8784
        assert first["unexpected_occurrences"] == 8784
    else:
        assert first["missing_hours"] == first["unexpected_occurrences"] == 1


def test_strict_profile_reports_comparison_without_claiming_source_identity(complete):
    records, xlsx = complete
    result = compare_bdg2(records, xlsx, APPROVED_BUILDINGS, example_limit=2)
    assert result["nominal_temporal_agreement"]
    assert result["electricity_value_equivalence"] is True
    assert result["comparison"]["compared_positions"] == 280_704
    assert result["comparison_of_supplied_records_eligible"]
    assert result["source_hash_binding"] == "not_verified"
    assert not result["verified"] and not result["final_verification_eligible"]


@pytest.mark.parametrize("cell,expected", [
    (Cell("text", "0.00000000000000000001"), False),
    (Cell("null", None), False),
    (Cell("float", 0.0), None),
])
def test_value_status_is_separate_from_temporal_agreement(complete, cell, expected):
    records, xlsx = complete
    changed = list(xlsx.records)
    changed[0] = {**changed[0], APPROVED_BUILDINGS[0]: cell}
    modified = XlsxInput(tuple(changed), xlsx.representations, xlsx.date_system,
                         xlsx.source_columns, xlsx.worksheet)
    result = compare_bdg2(records, modified, APPROVED_BUILDINGS, example_limit=1)
    assert result["nominal_temporal_agreement"]
    assert result["electricity_value_equivalence"] is expected
    assert not result["comparison_of_supplied_records_eligible"]
    reason = "electricity_value_mismatch" if expected is False else "electricity_values_unresolved"
    assert reason in result["verification_limitations"]
    assert not result["verified"]


def test_adjusted_timestamp_does_not_mask_exact_value_agreement(complete):
    records, xlsx = complete
    observed = list(xlsx.representations)
    error = Fraction(432, 100_000_000)
    bound = (Fraction(1, 20_000_000_000) + Fraction(1, 2 ** 37)) * 86_400
    observed[0] = {"timestamp": {"kind": "d", "raw_type": "n",
                                 "raw": "42370.00000000005",
                                 "decoding": "rounded_within_bound",
                                 "error_seconds": str(error), "bound_seconds": str(bound)}}
    modified = XlsxInput(xlsx.records, tuple(observed), "1900", xlsx.source_columns, "Hoja1")
    result = compare_bdg2(records, modified, APPROVED_BUILDINGS)
    assert result["nominal_temporal_agreement"]
    assert result["electricity_value_equivalence"] is True
    assert result["temporal_adjustments"]["adjusted_rows"] == 1
    assert result["temporal_adjustments"]["maximum_error_seconds"] == str(error)
    assert "temporal_adjustments" in result["verification_limitations"]
    assert not result["comparison_of_supplied_records_eligible"] and not result["verified"]


def test_exact_serial_and_text_timestamps_can_agree_nominally(complete):
    records, xlsx = complete
    observed = list(xlsx.representations)
    bound = (Fraction(1, 20_000_000_000) + Fraction(1, 2 ** 37)) * 86_400
    observed[0] = {"timestamp": {"kind": "d", "raw_type": "n", "raw": "42370",
                                 "decoding": "exact", "error_seconds": "0",
                                 "bound_seconds": str(bound)}}
    modified = XlsxInput(xlsx.records, tuple(observed), "1900", xlsx.source_columns, "Hoja1")
    result = compare_bdg2(records, modified, APPROVED_BUILDINGS)
    assert result["nominal_temporal_agreement"]
    assert result["electricity_value_equivalence"] is True
    assert result["temporal_adjustments"]["adjusted_rows"] == 0
    assert result["comparison_of_supplied_records_eligible"]
    assert not result["verified"]


def test_reordered_rows_do_not_claim_temporal_order(complete):
    records, xlsx = complete
    changed = list(xlsx.records)
    changed[10], changed[11] = changed[11], changed[10]
    modified = XlsxInput(tuple(changed), xlsx.representations, "1900", xlsx.source_columns, "Hoja1")
    result = compare_bdg2(records, modified, APPROVED_BUILDINGS)
    assert result["calendar"]["xlsx"]["coverage_matches"]
    assert not result["nominal_temporal_agreement"]
    assert result["electricity_value_equivalence"] is True
    assert not result["comparison_of_supplied_records_eligible"]


def test_duplicate_groups_do_not_create_individual_value_pairings(complete):
    records, xlsx = complete
    changed = list(records)
    changed[10] = changed[9]
    modified = XlsxInput(tuple(changed), xlsx.representations, "1900", xlsx.source_columns, "Hoja1")
    result = compare_bdg2(tuple(changed), modified, APPROVED_BUILDINGS)
    assert result["comparison"]["duplicate_alignment_ambiguities"] == 1
    assert result["electricity_value_equivalence"] is None
    assert not result["verified"]


def test_duplicate_complete_row_difference_is_reported_without_cell_pairing(complete):
    records, xlsx = complete
    left = list(records)
    left[10] = left[9]
    right = list(left)
    right[10] = {**right[10], APPROVED_BUILDINGS[0]: Cell("text", "9")}
    modified = XlsxInput(tuple(right), xlsx.representations, "1900", xlsx.source_columns, "Hoja1")
    result = compare_bdg2(tuple(left), modified, APPROVED_BUILDINGS)
    assert result["comparison"]["duplicate_row_multiset_mismatches"] == 1
    assert result["electricity_value_equivalence"] is False
    assert "electricity_value_mismatch" in result["verification_limitations"]
    assert not result["verified"]


def test_wrong_calendar_does_not_become_complete_value_equivalence(complete):
    records, xlsx = complete
    changed = list(records)
    changed[0] = {**changed[0], "timestamp": "2015-12-31 23:00:00"}
    modified = XlsxInput(tuple(changed), xlsx.representations, "1900", xlsx.source_columns, "Hoja1")
    result = compare_bdg2(tuple(changed), modified, APPROVED_BUILDINGS)
    assert result["comparison"]["timestamp_coverage_equal"]
    assert not result["nominal_temporal_agreement"]
    assert result["electricity_value_equivalence"] is None
    assert "nominal_calendar_or_order_mismatch" in result["verification_limitations"]
    assert not result["verified"]


def test_additional_wide_csv_columns_are_not_selected(tmp_path):
    path = tmp_path / "synthetic.csv"
    columns = ["timestamp", *APPROVED_BUILDINGS, "Other_building"]
    path.write_text(",".join(columns) + "\n2016-01-01 00:00:00," + ",".join(["0"] * 17) + "\n")
    record = next(iter_csv_electricity(path, APPROVED_BUILDINGS))
    assert tuple(record) == ("timestamp", *APPROVED_BUILDINGS)
    assert "Other_building" not in record


def test_xlsx_source_header_order_is_not_silently_rewritten(tmp_path):
    workbook = Workbook()
    workbook.active.title = "Hoja1"
    workbook.active.append(["timestamp", *reversed(APPROVED_BUILDINGS)])
    workbook.active.append(["2016-01-01 00:00:00", *([0] * 16)])
    path = tmp_path / "synthetic.xlsx"
    workbook.save(path)
    workbook.close()
    xlsx = read_xlsx_electricity(path, APPROVED_BUILDINGS, expected_rows=1)
    assert xlsx.source_columns == ("timestamp", *reversed(APPROVED_BUILDINGS))
    with pytest.raises(ComparisonError, match="reordered"):
        compare_bdg2([], xlsx, APPROVED_BUILDINGS)
