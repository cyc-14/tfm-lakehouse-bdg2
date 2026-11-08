"""Controlled read-only entry point; caller declarations are not proof of consent."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat
import tomllib
import warnings

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS, _EXPECTED_LABELS
from tfm_lakehouse.config import config_from_mapping
from tfm_lakehouse.synthetic_selection_workflow import SelectionGridError, _run_verified_workflow


@dataclass(frozen=True)
class RealVerificationSpec:
    """Exact intended scope, including byte limits and no output destination."""

    config_path: Path
    metadata_path: Path
    electricity_path: Path
    xlsx_path: Path
    expected_sha256: tuple[str, str, str]
    max_bytes: tuple[int, int, int]
    dataset_version: str
    electricity_variant: str
    worksheet: str
    building_ids: tuple[str, ...]
    period_start: str
    period_end: str
    evidence_destination: None = None


@dataclass(frozen=True)
class ExecutionAuthorization:
    """Caller-attested human approval of the precise scope and operations.

    This object is a declaration, not evidence that the named approval occurred.
    """

    scope: RealVerificationSpec
    approval_reference: str
    operations: tuple[str, ...]


_OPERATIONS = ("read_sources", "hash_sources", "compare_in_memory", "no_persistent_output")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_STAGES = ("metadata_bytes_schema", "selection_filters", "electricity_grid",
           "selection_counts", "selection_reconciliation", "xlsx_integrity",
           "xlsx_structure", "comparison", "comparison_assessment")


class VerificationContractError(ValueError):
    """Invalid or unapproved scope; message is a non-sensitive category."""


def _validate_scope(spec: RealVerificationSpec, authorization: ExecutionAuthorization | None) -> None:
    if not isinstance(spec, RealVerificationSpec):
        raise VerificationContractError("invalid_spec")
    if (not isinstance(authorization, ExecutionAuthorization) or authorization.scope != spec
            or not isinstance(authorization.approval_reference, str)
            or not authorization.approval_reference.strip()
            or authorization.operations != _OPERATIONS):
        raise VerificationContractError("specific_authorization_required")
    paths = (spec.config_path, spec.metadata_path, spec.electricity_path, spec.xlsx_path)
    if any(not isinstance(p, Path) or not p.is_absolute() or ".." in p.parts for p in paths):
        raise VerificationContractError("absolute_paths_required")
    if len(set(paths)) != 4:
        raise VerificationContractError("distinct_paths_required")
    if (not isinstance(spec.expected_sha256, tuple) or len(spec.expected_sha256) != 3
            or any(not isinstance(d, str) or not _SHA.fullmatch(d) for d in spec.expected_sha256)):
        raise VerificationContractError("three_expected_digests_required")
    if (not isinstance(spec.max_bytes, tuple) or len(spec.max_bytes) != 3
            or any(type(n) is not int or n <= 0 for n in spec.max_bytes)):
        raise VerificationContractError("three_positive_byte_limits_required")
    if spec.dataset_version != "BDG2 v1.0" or spec.electricity_variant != "raw":
        raise VerificationContractError("dataset_variant_mismatch")
    if (spec.worksheet != "Hoja1" or spec.building_ids != APPROVED_BUILDINGS
            or spec.period_start != _EXPECTED_LABELS[0] or spec.period_end != _EXPECTED_LABELS[-1]):
        raise VerificationContractError("profile_identity_or_period_mismatch")
    if spec.evidence_destination is not None:
        raise VerificationContractError("evidence_output_disabled")
    if os.environ.get("TFM_REAL_CONFIG") != str(spec.config_path):
        raise VerificationContractError("real_config_required")


def _open_source(path: Path):
    """Open regular files via no-follow directory descriptors, never for writing."""
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    directory = os.open("/", flags | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, flags | os.O_DIRECTORY, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(path.name, flags | os.O_NONBLOCK, dir_fd=directory)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise VerificationContractError("source_not_regular")
            return os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise
    finally:
        os.close(directory)


def _fingerprint(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _load_real_config(spec: RealVerificationSpec) -> None:
    """Parse a bounded, stable config descriptor without reading an input as TOML."""
    with _open_source(spec.config_path) as handle:
        before = _fingerprint(os.fstat(handle.fileno()))
        for path in (spec.metadata_path, spec.electricity_path, spec.xlsx_path):
            for component in reversed((path, *path.parents)):
                if stat.S_ISLNK(os.lstat(component).st_mode):
                    raise VerificationContractError("source_symlink")
            identity = os.stat(path, follow_symlinks=False)
            if (identity.st_dev, identity.st_ino) == before[:2]:
                raise VerificationContractError("config_source_alias")
        data = handle.read(65_537)
        if len(data) > 65_536:
            raise VerificationContractError("real_config_too_large")
        if (_fingerprint(os.fstat(handle.fileno())) != before
                or _fingerprint(os.stat(spec.config_path, follow_symlinks=False)) != before):
            raise VerificationContractError("real_config_changed")
    parsed = tomllib.loads(data.decode("utf-8"))
    config = config_from_mapping(parsed, spec.config_path)
    if (config.data_kind != "real" or config.metadata != spec.metadata_path
            or config.electricity != spec.electricity_path):
        raise VerificationContractError("real_config_scope_mismatch")


def _assessment(result: dict) -> dict:
    selection = result["selection_reproduction"]
    comparison = result["comparison"]
    assessment = {
        "source_origin_verified": False, "verified": False,
        "complete_verification_eligible": False,
        "selection_reproduced": selection["selection_reproduction_eligible"],
        "selection_same_set": selection["approved_selection"]["same_set"],
        "selection_same_order": selection["approved_selection"]["same_order"],
        "funnel_counts": result["funnel"]["observed"],
        "building_counts": [row for row in selection["building_counts"]
                            if row["building_id"] in APPROVED_BUILDINGS],
        "invalid_cells": selection["invalid_cells"],
        "ambiguous_cells": selection["ambiguous_cells"],
        "temporal_adjustments_observed": {
            "count": len(result["temporal_adjustment_trace"]),
            "examples": result["temporal_adjustment_trace"][:20],
        },
        "comparison": None,
    }
    if comparison is not None:
        core = comparison["comparison"]
        assessment["comparison"] = {
            "nominal_temporal_agreement": comparison["nominal_temporal_agreement"],
            "electricity_value_equivalence": comparison["electricity_value_equivalence"],
            "compared_positions": core["compared_positions"],
            "expected_positions": core["expected_positions_per_source"],
            "uncompared_positions": core["uncompared_positions"],
            "outcomes": core["outcomes"],
            "temporal_adjustments": comparison["temporal_adjustments"],
            "records_eligible": comparison["comparison_of_supplied_records_eligible"],
            "examples": [{k: v for k, v in item.items()
                          if k in {"type", "row", "source"}
                          or (k == "building_id" and v in APPROVED_BUILDINGS)
                          or (k == "timestamp" and v in _EXPECTED_LABELS)}
                         for item in core["examples"]],
        }
    return assessment


def verify_real_sources(spec: RealVerificationSpec, authorization: ExecutionAuthorization | None) -> dict:
    """Read only an explicitly declared scope; never create an output or certify origin."""
    stages = {stage: "not_run" for stage in _STAGES}
    report = {"configuration": "not_run", "source_integrity": "unresolved",
              "path_stability": "unresolved", "authorization": "not_established",
              "evidence_output": "disabled", "stages": stages,
              "assessment": None, "failure": None, "verified": False}
    try:
        _validate_scope(spec, authorization)
        # The project config is a second gate, not evidence of human authorization.
        _load_real_config(spec)
    except VerificationContractError as exc:
        report.update(configuration="failed", failure=str(exc))
        return report
    except Exception:
        report.update(configuration="failed", failure="invalid_real_config")
        return report
    report["configuration"] = "passed"
    report["authorization"] = "caller_attested_not_independently_verified"
    paths = (spec.metadata_path, spec.electricity_path, spec.xlsx_path)
    try:
        with ExitStack() as stack:
            streams = [stack.enter_context(_open_source(path)) for path in paths]
            before = [_fingerprint(os.fstat(stream.fileno())) for stream in streams]
            if len({identity[:2] for identity in before}) != 3:
                raise VerificationContractError("source_alias")
            def observe(stage: str, status: str) -> None:
                stages[stage] = status
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always")
                result = _run_verified_workflow(
                    *streams, metadata_sha256=spec.expected_sha256[0],
                    electricity_sha256=spec.expected_sha256[1], xlsx_sha256=spec.expected_sha256[2],
                    max_metadata_bytes=spec.max_bytes[0], max_electricity_bytes=spec.max_bytes[1],
                    max_xlsx_bytes=spec.max_bytes[2], _stage_observer=observe,
                )
            after = [_fingerprint(os.fstat(stream.fileno())) for stream in streams]
            current = [_fingerprint(os.stat(path, follow_symlinks=False)) for path in paths]
            if before != after or after != current:
                raise VerificationContractError("source_changed")
            report["source_integrity"] = "matched_expected_sha256"
            report["path_stability"] = "matched_at_checks_only"
            report["parser_warning_count"] = len(captured)
            report["assessment"] = _assessment(result)
            if any(status in {"failed", "unresolved"} for status in stages.values()):
                report["failure"] = "verification_blocked"
            if captured:
                report["failure"] = "reader_warnings"
                report["assessment"]["comparison"] = None
    except VerificationContractError as exc:
        report["failure"] = str(exc)
    except SelectionGridError as exc:
        report["failure"] = "SelectionGridError"
        report["grid_failure"] = {key: exc.report[key] for key in
                                  ("missing_hours", "unexpected_occurrences", "duplicate_groups",
                                   "order_mismatched_positions")}
    except Exception as exc:
        report["failure"] = type(exc).__name__
    return report
