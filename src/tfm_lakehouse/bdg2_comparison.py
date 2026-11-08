"""Strict, in-memory BDG2 comparison profile; source provenance is external."""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from fractions import Fraction

from tfm_lakehouse.comparison_xlsx import XlsxInput, compare_xlsx_electricity
from tfm_lakehouse.excel_bdg2_comparison import ComparisonError


APPROVED_BUILDINGS = (
    "Robin_lodging_Dorthy", "Robin_lodging_Pricilla", "Robin_lodging_Armand",
    "Fox_lodging_Wallace", "Fox_lodging_Winifred", "Fox_lodging_Warren",
    "Peacock_lodging_Chloe", "Peacock_lodging_Terrie", "Peacock_lodging_Nova",
    "Bull_lodging_Leonard", "Bull_lodging_Terence", "Bull_lodging_Lettie",
    "Bull_lodging_Carie", "Bull_lodging_Jeremiah", "Bull_lodging_Anibal",
    "Bull_lodging_Danielle",
)
_START = datetime(2016, 1, 1)
_END = datetime(2017, 12, 31, 23)
_HOUR_COUNT = int((_END - _START) // timedelta(hours=1)) + 1
_EXPECTED_LABELS = tuple(
    (_START + timedelta(hours=index)).strftime("%Y-%m-%d %H:%M:%S")
    for index in range(_HOUR_COUNT)
)
_EXPECTED_SET = frozenset(_EXPECTED_LABELS)


def _require_buildings(building_ids: Sequence[str], example_limit: int) -> None:
    if (not isinstance(building_ids, Sequence) or isinstance(building_ids, (str, bytes))
            or any(not isinstance(item, str) for item in building_ids)):
        raise ComparisonError("Selected building identifiers must be a sequence of strings")
    counts = Counter(building_ids)
    duplicates = sorted(item for item, count in counts.items() if count > 1)
    missing = sorted(set(APPROVED_BUILDINGS) - counts.keys())
    unexpected = sorted(counts.keys() - set(APPROVED_BUILDINGS))
    if duplicates or missing or unexpected or tuple(building_ids) != APPROVED_BUILDINGS:
        raise ComparisonError(
            f"Invalid selection: missing_count={len(missing)}, missing={missing[:example_limit]}, "
            f"unexpected_count={len(unexpected)}, unexpected={unexpected[:example_limit]}, "
            f"duplicate_ids_count={len(duplicates)}, "
            f"duplicate_excess_occurrences={sum(counts[item] - 1 for item in duplicates)}, "
            f"duplicates={duplicates[:example_limit]}, "
            f"order_mismatch={tuple(building_ids) != APPROVED_BUILDINGS}"
        )


def validate_bdg2_calendar(records: Sequence[Mapping], *, example_limit: int = 20) -> dict:
    """Check the exact nominal grid independently of another source's coverage."""
    if (not isinstance(records, Sequence) or isinstance(records, (str, bytes))
            or type(example_limit) is not int or example_limit < 0):
        raise ComparisonError("Invalid calendar input or example limit")
    labels = []
    for record in records:
        if not isinstance(record, Mapping) or "timestamp" not in record:
            raise ComparisonError("Each record must contain a timestamp")
        labels.append(record["timestamp"])
    counts = Counter(label for label in labels if isinstance(label, str))
    invalid_types = sum(not isinstance(label, str) for label in labels)
    missing = sorted(_EXPECTED_SET - counts.keys())
    unexpected = sorted((label, count) for label, count in counts.items()
                        if label not in _EXPECTED_SET)
    duplicates = sorted((label, count) for label, count in counts.items() if count > 1)
    mismatch_count = 0
    order_examples = []
    for index in range(max(len(labels), _HOUR_COUNT)):
        actual = labels[index] if index < len(labels) else None
        expected = _EXPECTED_LABELS[index] if index < _HOUR_COUNT else None
        if actual != expected or index >= len(labels) or index >= _HOUR_COUNT:
            mismatch_count += 1
            if len(order_examples) < example_limit:
                order_examples.append({"row": index + 1, "expected": expected,
                                       "actual_repr": repr(actual)})
    coverage = (not missing and not unexpected and not duplicates and not invalid_types
                and len(labels) == _HOUR_COUNT)
    return {
        "expected_rows": _HOUR_COUNT, "actual_rows": len(labels),
        "coverage_matches": coverage,
        "order_matches": mismatch_count == 0,
        "missing_hours": len(missing),
        "unexpected_occurrences": sum(count for _, count in unexpected) + invalid_types,
        "invalid_timestamp_types": invalid_types,
        "duplicate_groups": len(duplicates),
        "duplicate_excess_occurrences": sum(count - 1 for _, count in duplicates),
        "order_mismatched_positions": mismatch_count,
        "examples": {
            "missing": missing[:example_limit],
            "unexpected": [{"timestamp": label, "count": count}
                           for label, count in unexpected[:example_limit]],
            "duplicates": [{"timestamp": label, "count": count}
                           for label, count in duplicates[:example_limit]],
            "order": order_examples,
        },
    }


def validate_bdg2_xlsx_structure(xlsx: XlsxInput) -> None:
    """Require the approved worksheet and original ordered header."""
    if not isinstance(xlsx, XlsxInput):
        raise ComparisonError("XLSX adapter output is required")
    if xlsx.worksheet != "Hoja1":
        raise ComparisonError("The expected XLSX worksheet is Hoja1")
    if xlsx.source_columns != ("timestamp", *APPROVED_BUILDINGS):
        raise ComparisonError("XLSX source columns are missing, unexpected or reordered")


def compare_bdg2(
    csv_records: Sequence[Mapping], xlsx: XlsxInput, building_ids: Sequence[str], *,
    example_limit: int = 20,
) -> dict:
    """Compare supplied records using the strict profile, without certifying sources.

    CSV records are the selected projection of a potentially wider CSV. Workbook
    headers must retain the approved order. Source hashes and provenance are not
    supplied to this function; consequently its global ``verified`` is false.
    """
    if type(example_limit) is not int or example_limit < 0:
        raise ComparisonError("example_limit must be a nonnegative integer")
    _require_buildings(building_ids, example_limit)
    validate_bdg2_xlsx_structure(xlsx)
    csv_calendar = validate_bdg2_calendar(csv_records, example_limit=example_limit)
    xlsx_calendar = validate_bdg2_calendar(xlsx.records, example_limit=example_limit)
    comparison = compare_xlsx_electricity(
        csv_records, xlsx, APPROVED_BUILDINGS, expected_rows=_HOUR_COUNT,
        expected_buildings=len(APPROVED_BUILDINGS), example_limit=example_limit,
    )
    calendars_match = all(item["coverage_matches"] and item["order_matches"]
                          for item in (csv_calendar, xlsx_calendar))
    temporal_agreement = (calendars_match and comparison["timestamp_coverage_equal"] is True
                          and comparison["timestamp_order_equal"])
    outcomes = comparison["outcomes"]
    disproved = bool(outcomes["numeric_mismatch"] or outcomes["missingness_mismatch"]
                     or comparison["duplicate_row_multiset_mismatches"])
    source_classes = [source["cell_classes"] for source in comparison["sources"].values()]
    incomplete = (not csv_calendar["coverage_matches"] or not xlsx_calendar["coverage_matches"]
                  or comparison["compared_positions"] != _HOUR_COUNT * len(APPROVED_BUILDINGS)
                  or any(item["invalid"] or item["ambiguous"] for item in source_classes)
                  or comparison["duplicate_alignment_ambiguities"] > 0)
    value_equivalence = False if disproved else None if incomplete else True
    adjusted = [Fraction(item["timestamp"]["error_seconds"])
                for item in xlsx.representations
                if item["timestamp"].get("decoding") == "rounded_within_bound"]
    records_eligible = (temporal_agreement and value_equivalence is True
                        and not adjusted and comparison["verified"])
    limitations = ["source_identity_and_hash_binding_not_verified"]
    if not temporal_agreement:
        limitations.append("nominal_calendar_or_order_mismatch")
    if value_equivalence is False:
        limitations.append("electricity_value_mismatch")
    elif value_equivalence is None:
        limitations.append("electricity_values_unresolved")
    if adjusted:
        limitations.append("temporal_adjustments")
    return {
        "profile": "bdg2_2016_2017",
        "structural_status": {"selected_identifiers_valid": True,
                              "xlsx_column_order_valid": True,
                              "xlsx_worksheet_valid": True,
                              "source_identity": "not_verified"},
        "source_hash_binding": "not_verified",
        "calendar": {"csv": csv_calendar, "xlsx": xlsx_calendar},
        "nominal_temporal_agreement": temporal_agreement,
        "electricity_value_equivalence": value_equivalence,
        "electricity_value_scope": "supplied records at paired nominal labels",
        "unresolved_comparison_limitations": {
            "invalid_cells": sum(item["invalid"] for item in source_classes),
            "ambiguous_cells": sum(item["ambiguous"] for item in source_classes),
            "uncompared_positions": comparison["uncompared_positions"],
            "duplicate_alignment_ambiguities": comparison["duplicate_alignment_ambiguities"],
        },
        "temporal_adjustments": {**comparison["timestamp_decoding"],
                                 "maximum_error_seconds": str(max(adjusted, default=Fraction(0)))},
        "comparison_of_supplied_records_eligible": bool(records_eligible),
        "final_verification_eligible": False,
        "verified": False,
        "verification_limitations": limitations,
        "comparison": comparison,
    }
