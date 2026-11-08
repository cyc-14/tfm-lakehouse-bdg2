"""Synthetic-only orchestration of verified bytes, selection and comparison."""
from __future__ import annotations

from collections.abc import Sequence
import csv
from typing import BinaryIO, TextIO

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS, compare_bdg2, validate_bdg2_calendar
from tfm_lakehouse.building_selection import SelectionError, qualify_buildings
from tfm_lakehouse.comparison_csv import iter_csv_electricity_stream
from tfm_lakehouse.comparison_xlsx import read_xlsx_snapshot
from tfm_lakehouse.selection_reproduction import _SelectionCounter
from tfm_lakehouse.source_provenance import ProvenanceError, parse_verified_csv, read_verified_snapshot


class SelectionGridError(SelectionError):
    """An electricity grid cannot support a reproducible selection."""

    def __init__(self, report: dict):
        super().__init__("Electricity grid is incomplete, duplicated, unexpected or reordered")
        self.report = report


def _metadata_records(source: TextIO) -> list[dict]:
    reader = csv.reader(source, delimiter=",", quotechar='"', strict=True)
    try:
        header = next(reader)
    except StopIteration as exc:
        raise SelectionError("Metadata CSV header is missing") from exc
    required = ("building_id", "primaryspaceusage", "sub_primaryspaceusage", "sqm")
    if (not header or any(not item or item.strip() != item for item in header)
            or len({item.casefold() for item in header}) != len(header)
            or not set(required).issubset(header)):
        raise SelectionError("Metadata CSV headers are invalid or incomplete")
    positions = {name: header.index(name) for name in required}
    records = []
    for row in reader:
        if len(row) != len(header):
            raise SelectionError("Metadata CSV record width is inconsistent")
        if len(records) >= 100_000:
            raise SelectionError("Metadata record limit exceeded")
        records.append({name: row[index] for name, index in positions.items()})
    return records


def _grid_pass(source: TextIO, candidates: Sequence[str], metadata_ids: Sequence[str]) -> dict:
    timestamps = []
    for row in iter_csv_electricity_stream(
        source, candidates, allowed_columns=metadata_ids, allow_empty=True,
    ):
        if len(timestamps) >= 20_000:
            raise SelectionError("Electricity record limit exceeded")
        timestamps.append({"timestamp": row["timestamp"]})
    return validate_bdg2_calendar(timestamps)


def _count_pass(source: TextIO, metadata: list[dict], candidates: Sequence[str],
                metadata_ids: Sequence[str]) -> tuple[dict, tuple[dict, ...] | None]:
    counter = _SelectionCounter(candidates)
    can_project = set(APPROVED_BUILDINGS).issubset(candidates)
    projected = []
    for row in iter_csv_electricity_stream(
        source, candidates, allowed_columns=metadata_ids, allow_empty=True,
    ):
        if counter.rows >= 17_544:
            raise SelectionError("Electricity rows exceed the validated grid")
        counter.consume(row)
        if can_project:
            projected.append({"timestamp": row["timestamp"],
                              **{bid: row[bid] for bid in APPROVED_BUILDINGS}})
    report = counter.finish(metadata)
    return report, tuple(projected) if can_project else None


def run_synthetic_workflow(
    metadata_source: BinaryIO, electricity_source: BinaryIO, xlsx_source: BinaryIO, *,
    metadata_sha256: str, electricity_sha256: str, xlsx_sha256: str,
    max_metadata_bytes: int, max_electricity_bytes: int, max_xlsx_bytes: int,
) -> dict:
    """Run only with synthetic streams; no paths, files, outputs or real-data gate.

    Caller-owned streams must begin at offset zero. Both CSV passes hash the
    exact bytes parsed and must match the same expected digest. No result is
    returned until all three identities and parsing steps have succeeded.
    """
    metadata, metadata_identity = parse_verified_csv(
        metadata_source, metadata_sha256, _metadata_records, max_bytes=max_metadata_bytes,
    )
    qualified = qualify_buildings(metadata)
    candidates = tuple(qualified["area_qualified_building_ids"])
    metadata_ids = tuple(row["building_id"] for row in metadata)
    grid, grid_identity = parse_verified_csv(
        electricity_source, electricity_sha256,
        lambda stream: _grid_pass(stream, candidates, metadata_ids),
        max_bytes=max_electricity_bytes,
    )
    if not grid["coverage_matches"] or not grid["order_matches"]:
        raise SelectionGridError(grid)
    try:
        if electricity_source.seek(0) != 0:
            raise ProvenanceError("Could not rewind electricity source")
    except (OSError, ValueError) as exc:
        raise ProvenanceError("Could not rewind electricity source") from exc
    (selection, projected), count_identity = parse_verified_csv(
        electricity_source, electricity_sha256,
        lambda stream: _count_pass(stream, metadata, candidates, metadata_ids),
        max_bytes=max_electricity_bytes,
    )
    if grid_identity != count_identity:
        raise ProvenanceError("Electricity passes did not consume the same bytes")
    snapshot, xlsx_identity = read_verified_snapshot(xlsx_source, xlsx_sha256,
                                                     max_bytes=max_xlsx_bytes)
    xlsx = read_xlsx_snapshot(snapshot, APPROVED_BUILDINGS)
    adjustment_trace = [
        {"row": index + 2, "decoded_timestamp": xlsx.records[index]["timestamp"],
         "raw": item["timestamp"]["raw"], "error_seconds": item["timestamp"]["error_seconds"],
         "bound_seconds": item["timestamp"]["bound_seconds"]}
        for index, item in enumerate(xlsx.representations)
        if item["timestamp"].get("decoding") == "rounded_within_bound"
    ]
    comparison = None
    if selection["selection_reproduction_eligible"] and projected is not None:
        comparison = compare_bdg2(projected, xlsx, APPROVED_BUILDINGS)
    stages = selection["selection"]["stage_counts"]
    observed_funnel = [stages[name] for name in ("main_use", "subcategory", "area", "selected")]
    limitations = ["synthetic_only", "source_origin_not_verified", "path_stability_not_verified",
                   "real_data_execution_not_authorized"]
    if not selection["approved_selection"]["same_set"]:
        limitations.append("selection_identifiers_mismatch")
    elif not selection["approved_selection"]["same_order"]:
        limitations.append("selection_order_mismatch")
    if selection["invalid_cells"]:
        limitations.append("invalid_non_missing_cells")
    if selection["ambiguous_cells"]:
        limitations.append("ambiguous_cells")
    if adjustment_trace and comparison is None:
        limitations.append("temporal_adjustments_not_compared")
    if comparison:
        limitations.extend(
            f"comparison:{reason}" for reason in comparison["verification_limitations"]
            if reason != "source_identity_and_hash_binding_not_verified"
        )
    return {
        "source_byte_identity": {
            "metadata": metadata_identity, "electricity": count_identity, "xlsx": xlsx_identity,
        },
        "source_identity_status": {"all_supplied_sha256_match": True,
                                   "source_origin_verified": False,
                                   "path_stability_verified": False},
        "electricity_grid": grid,
        "selection_reproduction": selection,
        "temporal_adjustment_trace": adjustment_trace,
        "funnel": {"observed": observed_funnel, "historical_reference": [168, 107, 24, 16],
                   "matches_historical_reference": observed_funnel == [168, 107, 24, 16]},
        "comparison": comparison,
        "synthetic_records_eligible": bool(
            selection["selection_reproduction_eligible"] and comparison
            and comparison["comparison_of_supplied_records_eligible"]
        ),
        "verified": False,
        "verification_limitations": limitations,
    }
