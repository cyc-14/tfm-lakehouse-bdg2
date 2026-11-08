"""Reproduce selection from explicitly supplied metadata and electricity records."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from fractions import Fraction

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS, validate_bdg2_calendar
from tfm_lakehouse.building_selection import (
    _DENOMINATOR, SelectionError, qualify_buildings, verify_building_selection,
)
from tfm_lakehouse.excel_bdg2_comparison import Cell, _classify


class _SelectionCounter:
    """Count classified cells after the caller has validated the complete grid."""

    def __init__(self, building_ids: Sequence[str]):
        self.building_ids = tuple(building_ids)
        self.counts = {
            bid: {"missing": 0, "invalid": 0, "ambiguous": 0,
                  "numeric": 0, "zero": 0, "negative": 0,
                  "missing_null": 0, "missing_parsed_null": 0,
                  "missing_empty": 0, "missing_nan": 0}
            for bid in self.building_ids
        }
        self.rows = 0

    def consume(self, record: Mapping[str, object]) -> None:
        if not isinstance(record, Mapping) or set(record) != {"timestamp", *self.building_ids}:
            raise SelectionError("Electricity record columns do not match the qualified buildings")
        for bid in self.building_ids:
            cell = record[bid]
            if not isinstance(cell, Cell):
                raise SelectionError("Electricity values must be explicit Cell representations")
            category, value = _classify(cell)
            count = self.counts[bid]
            if category == "number":
                count["numeric"] += 1
                count["zero"] += value == 0
                count["negative"] += value < 0
            else:
                count[category] += 1
                if category == "missing":
                    representation = ({"": "missing_empty", "NaN": "missing_nan"}[cell.value]
                                      if cell.kind == "text" else f"missing_{cell.kind}")
                    count[representation] += 1
        self.rows += 1

    def finish(self, metadata: Sequence[Mapping[str, object]]) -> dict:
        if self.rows != _DENOMINATOR:
            raise SelectionError("Missingness counts require exactly 17544 validated rows")
        qualification = qualify_buildings(metadata)
        if tuple(qualification["area_qualified_building_ids"]) != self.building_ids:
            raise SelectionError("Qualified identifiers changed during counting")
        summaries = [{"building_id": bid, "non_missing_count": self.rows - data["missing"]}
                     for bid, data in self.counts.items()]
        selection = verify_building_selection(metadata, summaries)
        selected = tuple(selection["selected_building_ids"])
        approved = set(APPROVED_BUILDINGS)
        unexpected = sorted(set(selected) - approved)
        missing = [bid for bid in APPROVED_BUILDINGS if bid not in selected]
        invalid = sum(item["invalid"] for item in self.counts.values())
        ambiguous = sum(item["ambiguous"] for item in self.counts.values())
        return {
            "selection": selection,
            "classification_scope": "area-qualified building columns only",
            "building_counts": [
                {"building_id": bid, **data, "non_missing": self.rows - data["missing"],
                 "missing_percent_exact": str(Fraction(100 * data["missing"], self.rows))}
                for bid, data in self.counts.items()
            ],
            "approved_selection": {
                "missing": missing, "unexpected": unexpected,
                "same_set": not missing and not unexpected,
                "same_order": selected == APPROVED_BUILDINGS,
            },
            "invalid_cells": invalid,
            "ambiguous_cells": ambiguous,
            "selection_reproduction_eligible": (
                selected == APPROVED_BUILDINGS and not invalid and not ambiguous
            ),
        }


def reproduce_selection(
    metadata: Sequence[Mapping[str, object]], electricity: Sequence[Mapping[str, object]],
) -> dict:
    """Snapshot inputs, validate the full nominal grid, then derive R5 counts."""
    if (not isinstance(metadata, Sequence) or isinstance(metadata, (str, bytes))
            or not isinstance(electricity, Sequence) or isinstance(electricity, (str, bytes))):
        raise SelectionError("Metadata and electricity must be sequences")
    metadata_snapshot = tuple(dict(row) if isinstance(row, Mapping) else row for row in metadata)
    electricity_snapshot = tuple(dict(row) if isinstance(row, Mapping) else row for row in electricity)
    grid = validate_bdg2_calendar(electricity_snapshot)
    if not grid["coverage_matches"] or not grid["order_matches"]:
        return {"grid": grid, "selection_reproduction_eligible": False, "selection": None}
    qualification = qualify_buildings(metadata_snapshot)
    counter = _SelectionCounter(qualification["area_qualified_building_ids"])
    for row in electricity_snapshot:
        counter.consume(row)
    return {"grid": grid, **counter.finish(metadata_snapshot)}
