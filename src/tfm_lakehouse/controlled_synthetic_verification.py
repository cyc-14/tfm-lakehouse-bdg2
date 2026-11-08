"""Controlled synthetic assessment; this module cannot authorize real sources."""
from __future__ import annotations

from dataclasses import dataclass
import io
import os
import re
import warnings

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS, _EXPECTED_LABELS
from tfm_lakehouse.synthetic_selection_workflow import SelectionGridError, run_synthetic_workflow


@dataclass(frozen=True)
class SyntheticSpec:
    """Explicit synthetic inputs; source labels are identifiers, not paths."""

    source_labels: tuple[str, str, str]
    expected_sha256: tuple[str, str, str]
    max_bytes: tuple[int, int, int]
    dataset_variant: str
    dataset_version: str
    worksheet: str
    building_ids: tuple[str, ...]
    period_start: str
    period_end: str
    evidence_destination: str | None = None


_STAGES = (
    "configuration", "metadata_bytes_schema", "metadata_integrity", "selection_filters",
    "electricity_grid", "electricity_integrity", "selection_counts", "selection_reconciliation",
    "xlsx_integrity", "xlsx_structure", "comparison", "comparison_assessment",
    "parser_warnings", "source_provenance", "real_authorization", "evidence_output",
)
_LABEL = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class SyntheticContractError(ValueError):
    """A synthetic-only configuration was rejected before reading streams."""


def _validate(spec: SyntheticSpec, sources: tuple[object, ...]) -> None:
    if not isinstance(spec, SyntheticSpec):
        raise SyntheticContractError("invalid_spec")
    if "TFM_REAL_CONFIG" in os.environ:
        raise SyntheticContractError("real_configuration_present")
    if spec.dataset_variant != "synthetic" or not isinstance(spec.dataset_version, str) or not spec.dataset_version:
        raise SyntheticContractError("synthetic_variant_and_version_required")
    if (not isinstance(spec.source_labels, tuple) or len(spec.source_labels) != 3
            or any(not isinstance(item, str) or not _LABEL.fullmatch(item) for item in spec.source_labels)
            or len(set(spec.source_labels)) != 3):
        raise SyntheticContractError("three_distinct_source_labels_required")
    if (not isinstance(spec.expected_sha256, tuple) or len(spec.expected_sha256) != 3
            or any(not isinstance(item, str) or not _SHA.fullmatch(item) for item in spec.expected_sha256)):
        raise SyntheticContractError("three_expected_digests_required")
    if (not isinstance(spec.max_bytes, tuple) or len(spec.max_bytes) != 3
            or any(type(limit) is not int or limit <= 0 for limit in spec.max_bytes)):
        raise SyntheticContractError("three_positive_byte_limits_required")
    if (spec.worksheet != "Hoja1" or spec.building_ids != APPROVED_BUILDINGS
            or spec.period_start != _EXPECTED_LABELS[0] or spec.period_end != _EXPECTED_LABELS[-1]):
        raise SyntheticContractError("profile_identity_or_period_mismatch")
    if spec.evidence_destination is not None:
        raise SyntheticContractError("evidence_output_not_authorized")
    if len(sources) != 3 or any(type(source) is not io.BytesIO for source in sources):
        raise SyntheticContractError("synthetic_memory_streams_required")
    if len({id(source) for source in sources}) != 3:
        raise SyntheticContractError("sources_must_be_distinct")


def _safe_examples(comparison: dict) -> list[dict]:
    """Retain locations and categories, never cell values or whole-row samples."""
    allowed = {"type", "source", "row", "timestamp", "timestamp_repr", "building_id",
               "reason", "occurrences", "csv_rows", "excel_rows", "matched_complete_rows",
               "unmatched_csv_rows", "unmatched_excel_rows"}
    return [{key: value for key, value in item.items() if key in allowed}
            for item in comparison["comparison"]["examples"]]


def _summary(result: dict) -> dict:
    selection = result["selection_reproduction"]
    comparison = result["comparison"]
    summary = {
        "source_bytes": {name: {"sha256": identity.sha256, "bytes_read": identity.bytes_read}
                         for name, identity in result["source_byte_identity"].items()},
        "source_origin_verified": False,
        "path_stability_verified": False,
        "timestamp_serialization_identity": "not_established",
        "funnel": result["funnel"],
        "selection": selection["selection"],
        "approved_selection": selection["approved_selection"],
        "missingness_counts": selection["building_counts"],
        "electricity_calendar": result["electricity_grid"],
        "temporal_adjustment_trace": result["temporal_adjustment_trace"],
        "limitations": result["verification_limitations"],
        "comparison": None,
        "nominal_and_value_agreement": None,
        "synthetic_records_eligible": result["synthetic_records_eligible"],
        "complete_verification_eligible": False,
        "verified": False,
    }
    if comparison is not None:
        core = comparison["comparison"]
        summary["comparison"] = {
            "nominal_temporal_agreement": comparison["nominal_temporal_agreement"],
            "electricity_value_equivalence": comparison["electricity_value_equivalence"],
            "compared_positions": core["compared_positions"],
            "expected_positions": core["expected_positions_per_source"],
            "uncompared_positions": core["uncompared_positions"],
            "outcomes": core["outcomes"],
            "calendar": comparison["calendar"],
            "temporal_adjustments": comparison["temporal_adjustments"],
            "unresolved": comparison["unresolved_comparison_limitations"],
            "examples": _safe_examples(comparison),
        }
        values = comparison["electricity_value_equivalence"]
        summary["nominal_and_value_agreement"] = (
            False if not comparison["nominal_temporal_agreement"]
            else None if values is None else values
        )
    return summary


def assess_synthetic(
    spec: SyntheticSpec, metadata: io.BytesIO, electricity: io.BytesIO, xlsx: io.BytesIO,
) -> dict:
    """Assess in-memory synthetic fixtures without opening paths or writing outputs.

    Exact BytesIO types prevent this entry point from opening caller-owned files;
    they do not establish the origin of bytes already placed in memory. Failures
    contain categories only, never exception messages or source measurements.
    """
    stages = {name: "not_run" for name in _STAGES}
    stages.update({"metadata_integrity": "unresolved", "electricity_integrity": "unresolved",
                   "source_provenance": "unresolved", "real_authorization": "not_granted",
                   "evidence_output": "disabled"})
    report = {"stages": stages, "source_labels": None, "dataset": None,
              "profile": None, "checked_digests": {}, "assessment": None,
              "blocking_reasons": [], "verified": False}
    try:
        _validate(spec, (metadata, electricity, xlsx))
    except SyntheticContractError as exc:
        stages["configuration"] = "failed"
        report["blocking_reasons"] = [{"stage": "configuration", "code": str(exc)}]
        return report
    stages["configuration"] = "passed"
    report["source_labels"] = dict(zip(("metadata", "electricity", "xlsx"), spec.source_labels))
    report["dataset"] = {"declared_variant": spec.dataset_variant,
                         "declared_version": spec.dataset_version}
    report["profile"] = {"worksheet": spec.worksheet, "building_ids": spec.building_ids,
                         "period_start": spec.period_start, "period_end": spec.period_end,
                         "expected_rows": len(_EXPECTED_LABELS),
                         "expected_positions": len(_EXPECTED_LABELS) * len(APPROVED_BUILDINGS)}

    failures = []

    def observe(stage: str, status: str) -> None:
        stages[stage] = status
        if status == "failed":
            failures.append(stage)
        if status == "passed" and stage == "metadata_bytes_schema":
            stages["metadata_integrity"] = "passed"
            report["checked_digests"]["metadata"] = spec.expected_sha256[0]
        elif status == "passed" and stage == "electricity_grid":
            report["checked_digests"]["electricity_grid_pass"] = spec.expected_sha256[1]
        elif status == "passed" and stage == "selection_counts":
            stages["electricity_integrity"] = "passed"
            report["checked_digests"]["electricity_count_pass"] = spec.expected_sha256[1]
        elif status == "passed" and stage == "xlsx_integrity":
            report["checked_digests"]["xlsx"] = spec.expected_sha256[2]

    failure = None
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        try:
            result = run_synthetic_workflow(
                metadata, electricity, xlsx,
                metadata_sha256=spec.expected_sha256[0], electricity_sha256=spec.expected_sha256[1],
                xlsx_sha256=spec.expected_sha256[2], max_metadata_bytes=spec.max_bytes[0],
                max_electricity_bytes=spec.max_bytes[1], max_xlsx_bytes=spec.max_bytes[2],
                _stage_observer=observe,
            )
        except Exception as exc:
            failure = exc
    stages["parser_warnings"] = "unresolved" if captured else "passed"
    report["parser_warning_count"] = len(captured)
    if failure is not None:
        failed = failures[-1] if failures else "workflow"
        report["blocking_reasons"] = [
            {"stage": item, "code": "selection_not_eligible"} for item in failures[:-1]
        ] + [{"stage": failed, "code": type(failure).__name__}]
        if captured:
            report["blocking_reasons"].append({"stage": "parser_warnings", "code": "warnings_present"})
        if isinstance(failure, SelectionGridError):
            report["checked_digests"]["electricity_grid_pass"] = spec.expected_sha256[1]
            report["grid_failure"] = {key: failure.report[key] for key in
                                      ("missing_hours", "unexpected_occurrences",
                                       "duplicate_groups", "order_mismatched_positions")}
        return report
    report["assessment"] = _summary(result)
    report["blocking_reasons"] = [{"stage": "assessment", "code": reason}
                                  for reason in result["verification_limitations"]]
    if captured:
        report["assessment"]["synthetic_records_eligible"] = False
        if stages["comparison_assessment"] == "passed":
            stages["comparison_assessment"] = "unresolved"
        report["blocking_reasons"].append({"stage": "parser_warnings", "code": "warnings_present"})
    return report
