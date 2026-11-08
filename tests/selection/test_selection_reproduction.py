"""Synthetic tests for R5 counts and reproducible selection."""
from datetime import datetime, timedelta

import pytest

from tfm_lakehouse.selection.building_selection import SelectionError
from tfm_lakehouse.comparison.excel_bdg2_comparison import Cell
from tfm_lakehouse.selection.selection_reproduction import reproduce_selection


def metadata(building_id, *, primary="Lodging/residential", sub="Dormitory", sqm="4000"):
    return {"building_id": building_id, "primaryspaceusage": primary,
            "sub_primaryspaceusage": sub, "sqm": sqm}


@pytest.fixture(scope="module")
def labels():
    start = datetime(2016, 1, 1)
    return tuple((start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
                 for i in range(17_544))


def test_r5_missingness_boundaries_area_categories_and_invalidity(labels):
    rows = [metadata("a", sqm="3500"), metadata("b", sqm="6000"),
            metadata("c", sub="Other - Lodging/Residential"),
            metadata("hotel", sub="Hotel"), metadata("low", sqm="3499.9"),
            metadata("high", sqm="6000.1"), metadata("case", primary="lodging/residential"),
            metadata("wrong", sub="dormitory")]
    missing = (Cell("null", None), Cell("text", ""), Cell("text", "NaN"))
    electricity = [
        {"timestamp": label,
         "a": missing[i % 3] if i < 1403 else Cell("text", "0"),
         "b": Cell("text", "NaN") if i < 1404 else Cell("text", "-1"),
         "c": Cell("text", "bad") if i == 0 else Cell("text", "1")}
        for i, label in enumerate(labels)
    ]
    result = reproduce_selection(rows, electricity)
    assert result["classification_scope"] == "area-qualified building columns only"
    counts = {item["building_id"]: item for item in result["building_counts"]}
    assert counts["a"]["missing"] == 1403 and counts["a"]["zero"] == 17_544 - 1403
    assert (counts["a"]["missing_null"], counts["a"]["missing_empty"],
            counts["a"]["missing_nan"]) == (468, 468, 467)
    assert counts["b"]["missing"] == 1404 and counts["b"]["negative"] == 17_544 - 1404
    assert counts["c"]["invalid"] == 1 and counts["c"]["non_missing"] == 17_544
    assert result["selection"]["selected_building_ids"] == ["a", "c"]
    reasons = {item["building_id"]: item["reason"] for item in result["selection"]["exclusions"]}
    assert reasons == {"hotel": "subcategory_excluded", "low": "area_below_min",
                       "high": "area_above_max", "case": "main_use_mismatch",
                       "wrong": "subcategory_excluded", "b": "missingness_over_8_percent"}
    assert result["invalid_cells"] == 1 and not result["selection_reproduction_eligible"]
    assert not result["approved_selection"]["same_set"]
    assert result == reproduce_selection(rows, electricity)


@pytest.mark.parametrize("change", ["duplicate", "missing", "unexpected", "reordered"])
def test_invalid_grid_prevents_missingness_counts(labels, change):
    altered = list(labels)
    if change == "duplicate":
        altered[1] = altered[0]
    elif change == "missing":
        altered.pop()
    elif change == "unexpected":
        altered[-1] = "2018-01-01 00:00:00"
    else:
        altered[0], altered[1] = altered[1], altered[0]
    records = [{"timestamp": label, "a": Cell("text", "")} for label in altered]
    result = reproduce_selection([metadata("a")], records)
    assert result["selection"] is None
    assert "building_counts" not in result and not result["selection_reproduction_eligible"]


@pytest.mark.parametrize("bad_metadata", [
    [metadata("a"), metadata("a")],
    [metadata("a"), metadata("a", sqm="5000")],
    [{"building_id": "a"}],
])
def test_duplicate_conflicting_and_invalid_metadata_fail(labels, bad_metadata):
    records = [{"timestamp": label, "a": Cell("text", "0")} for label in labels]
    with pytest.raises(SelectionError):
        reproduce_selection(bad_metadata, records)


def test_unexpected_electricity_column_fails(labels):
    records = [{"timestamp": label, "a": Cell("text", "0"), "extra": Cell("text", "0")}
               for label in labels]
    with pytest.raises(SelectionError, match="columns"):
        reproduce_selection([metadata("a")], records)


def test_variable_sequence_is_snapshotted_before_grid_and_counting(labels):
    class Changing(list):
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            if self.iterations > 1:
                return iter([{**row, "timestamp": labels[0]} for row in list.__iter__(self)])
            return list.__iter__(self)

    rows = Changing({"timestamp": label, "a": Cell("text", "0")} for label in labels)
    result = reproduce_selection([metadata("a")], rows)
    assert rows.iterations == 1
    assert result["grid"]["coverage_matches"]
    assert result["building_counts"][0]["numeric"] == 17_544
