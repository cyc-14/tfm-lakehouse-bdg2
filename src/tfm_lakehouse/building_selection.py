"""Pure verifier for the approved synthetic H04 building-selection rules."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
import re


class SelectionError(ValueError):
    """Raised when selection inputs are ambiguous or violate the contract."""


_METADATA_FIELDS = {"building_id", "primaryspaceusage", "sub_primaryspaceusage", "sqm"}
_SUMMARY_FIELDS = {"building_id", "non_missing_count"}
_INCLUDED_SUBCATEGORIES = frozenset({
    "Dormitory", "Lodging/residential", "Other - Lodging/Residential",
})
_DENOMINATOR = 17_544
_DECIMAL_TEXT = re.compile(
    r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z"
)


def verify_building_selection(
    metadata: Sequence[Mapping[str, object]],
    measurement_summaries: Sequence[Mapping[str, object]],
) -> dict:
    """Apply the approved selection to supplied records without reading files.

    Area values must be strings, integers, or Decimals. Floats are rejected to
    keep boundary decisions exact. Missingness summaries must cover exactly
    the buildings reaching the area-qualified stage.
    """
    rows = _validated_records(metadata, _METADATA_FIELDS, "metadata")
    summaries = _validated_records(measurement_summaries, _SUMMARY_FIELDS, "summary")
    summary_by_id = {row["building_id"]: row for row in summaries}
    if len(summary_by_id) != len(summaries):
        raise SelectionError("Duplicate building_id in measurement summaries")

    staged = {"metadata": len(rows), "main_use": 0, "subcategory": 0,
              "area": 0, "selected": 0}
    exclusions: list[dict[str, str]] = []
    selected: list[str] = []
    area_ids: list[str] = []
    for row in rows:
        building_id = row["building_id"]
        if (not isinstance(row["primaryspaceusage"], str)
                or not isinstance(row["sub_primaryspaceusage"], str)):
            raise SelectionError(f"Invalid category value for building_id: {building_id}")
        if row["primaryspaceusage"] != "Lodging/residential":
            exclusions.append({"building_id": building_id, "reason": "main_use_mismatch"})
            continue
        staged["main_use"] += 1
        if row["sub_primaryspaceusage"] not in _INCLUDED_SUBCATEGORIES:
            exclusions.append({"building_id": building_id, "reason": "subcategory_excluded"})
            continue
        staged["subcategory"] += 1
        area, reason = _parse_area(row["sqm"])
        if reason:
            exclusions.append({"building_id": building_id, "reason": reason})
            continue
        if not Decimal("3500") <= area <= Decimal("6000"):
            reason = "area_below_min" if area < Decimal("3500") else "area_above_max"
            exclusions.append({"building_id": building_id, "reason": reason})
            continue
        staged["area"] += 1
        area_ids.append(building_id)

    if set(summary_by_id) != set(area_ids):
        raise SelectionError(
            "Measurement summaries must cover exactly the area-qualified building IDs"
        )
    for building_id in area_ids:
        count = summary_by_id[building_id]["non_missing_count"]
        if type(count) is not int or not 0 <= count <= _DENOMINATOR:
            raise SelectionError("non_missing_count must be an integer from 0 to 17544")
        missing = _DENOMINATOR - count
        if 100 * missing > 8 * _DENOMINATOR:
            exclusions.append({"building_id": building_id, "reason": "missingness_over_8_percent"})
        else:
            selected.append(building_id)
    staged["selected"] = len(selected)
    return {
        "denominator": _DENOMINATOR,
        "missingness_basis": "explicit non_missing_count; source tokens not classified here",
        "stage_counts": staged,
        "area_qualified_building_ids": area_ids,
        "selected_building_ids": selected,
        "exclusions": exclusions,
    }


def _validated_records(records, expected_fields: set[str], label: str) -> list[dict]:
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise SelectionError(f"{label} must be a sequence of mappings")
    result = []
    seen: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, Mapping) or set(record) != expected_fields:
            raise SelectionError(f"Invalid {label} schema at record {index}")
        building_id = record["building_id"]
        if not isinstance(building_id, str) or not building_id or building_id.strip() != building_id:
            raise SelectionError(f"Invalid building_id at {label} record {index}")
        if building_id in seen:
            raise SelectionError(f"Duplicate building_id in {label}: {building_id}")
        seen.add(building_id)
        result.append(dict(record))
    return result


def _parse_area(value: object) -> tuple[Decimal | None, str | None]:
    if value is None or value == "":
        return None, "area_missing"
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        return None, "area_invalid"
    if isinstance(value, str) and not _DECIMAL_TEXT.fullmatch(value):
        return None, "area_invalid"
    try:
        area = Decimal(value) if isinstance(value, (str, int)) else value
    except (InvalidOperation, ValueError):
        return None, "area_invalid"
    if not area.is_finite() or area < 0:
        return None, "area_invalid"
    return area, None
