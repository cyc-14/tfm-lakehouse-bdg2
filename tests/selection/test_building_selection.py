"""Synthetic unit tests for the approved H04 building-selection rules."""
from copy import deepcopy

import pytest

from tfm_lakehouse.selection.building_selection import SelectionError, verify_building_selection


def record(building_id="b1", *, primary="Lodging/residential", sub="Dormitory", sqm="4000"):
    return {"building_id": building_id, "primaryspaceusage": primary,
            "sub_primaryspaceusage": sub, "sqm": sqm}


def summary(building_id="b1", non_missing_count=17_544):
    return {"building_id": building_id, "non_missing_count": non_missing_count}


def test_exact_categories_bounds_and_8_percent_threshold():
    rows = [record("low", sqm="3500"), record("high", sqm="6000"),
            record("hotel", sub="Hotel"), record("other_case", sub="dormitory"),
            record("spaced", sub="Dormitory "), record("wrong_main", primary="lodging/residential")]
    counts = [summary("low", 16_141), summary("high", 16_140)]
    result = verify_building_selection(rows, counts)
    assert result["selected_building_ids"] == ["low"]
    assert result["area_qualified_building_ids"] == ["low", "high"]
    assert result["stage_counts"] == {
        "metadata": 6, "main_use": 5, "subcategory": 2, "area": 2, "selected": 1,
    }
    assert {item["building_id"]: item["reason"] for item in result["exclusions"]} == {
        "hotel": "subcategory_excluded", "other_case": "subcategory_excluded",
        "spaced": "subcategory_excluded", "wrong_main": "main_use_mismatch",
        "high": "missingness_over_8_percent",
    }


@pytest.mark.parametrize("subcategory", [
    "Dormitory", "Lodging/residential", "Other - Lodging/Residential",
])
def test_each_approved_subcategory_is_included(subcategory):
    result = verify_building_selection([record(sub=subcategory)], [summary()])
    assert result["selected_building_ids"] == ["b1"]


@pytest.mark.parametrize("sqm,reason", [
    ("3499.999", "area_below_min"), ("6000.001", "area_above_max"),
    (None, "area_missing"), ("", "area_missing"), ("not-a-number", "area_invalid"),
    ("NaN", "area_invalid"), ("Infinity", "area_invalid"),
    (" 3500", "area_invalid"), ("3_500", "area_invalid"), (-1, "area_invalid"),
    (3500.0, "area_invalid"),
])
def test_area_boundary_and_invalid_values(sqm, reason):
    rows = [record(sqm=sqm)]
    result = verify_building_selection(rows, [])
    assert result["exclusions"] == [{"building_id": "b1", "reason": reason}]


@pytest.mark.parametrize("rows,summaries", [
    ([record(), record()], [summary()]),
    ([record("")], []),
    ([record(" b1")], []),
    ([{**record(), "extra": "unexpected"}], [summary()]),
    ([record()], [summary(), summary()]),
    ([record()], [summary("unexpected")]),
])
def test_ambiguous_or_inconsistent_identifiers_and_schemas_fail(rows, summaries):
    with pytest.raises(SelectionError):
        verify_building_selection(rows, summaries)


@pytest.mark.parametrize("count", [True, -1, 17_545, 1.5])
def test_invalid_measurement_counts_fail(count):
    with pytest.raises(SelectionError):
        verify_building_selection([record()], [summary(non_missing_count=count)])


def test_output_is_deterministic_and_inputs_are_not_mutated():
    rows = [record("b2", sqm="6000"), record("b1", sqm="3500")]
    counts = [summary("b2", 16_141), summary("b1", 16_141)]
    before = deepcopy((rows, counts))
    first = verify_building_selection(rows, counts)
    second = verify_building_selection(rows, counts)
    assert first == second
    assert first["selected_building_ids"] == ["b2", "b1"]
    assert (rows, counts) == before
    assert first["missingness_basis"].startswith("explicit non_missing_count")
