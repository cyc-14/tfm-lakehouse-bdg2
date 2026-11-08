"""Bind an explicitly selected synthetic Bronze publication to H05.3."""
import json
import hashlib
import re

from pyspark.sql import DataFrame, SparkSession, functions as F

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.snapshot import (
    SnapshotError, _read_verified_tables, _same_configuration, _validate_tables,
)
from tfm_lakehouse.silver.silver_electricity_integrated import transform_selected_electricity
from tfm_lakehouse.silver.silver_electricity import _validate_columns

_RUN_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def transform_published_synthetic_electricity(
    spark: SparkSession, config: BronzeConfig, run_id: str,
) -> tuple[dict, DataFrame]:
    """Read one named synthetic publication, then transform its electricity frame.

    H02 validates each version, commit metadata, schema and count at read time.
    The returned Spark plan remains lazy: history and data must stay available.
    This interface cannot prove external source origin or prevent storage tampering.
    """
    if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id):
        raise SnapshotError("Invalid explicit publication run ID")
    if config.data_kind != "synthetic":
        raise SnapshotError("This binding only accepts synthetic Bronze publications")
    config.validate()
    path = config.namespace / "_runs" / (run_id + ".json")
    try:
        if path.is_symlink():
            raise SnapshotError("Publication report symlinks are unsupported")
        report_bytes = path.read_bytes()
        report = json.loads(report_bytes)
    except SnapshotError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SnapshotError("Explicit publication report is unavailable or malformed") from exc
    if not _same_configuration(report, config):
        raise SnapshotError("Publication configuration is incompatible")
    if report.get("status") != "passed" or report.get("run_id") != run_id:
        raise SnapshotError("Explicit publication is failed or has a mismatched run ID")
    _validate_tables(report, config, path)
    for item in report["tables"]:
        if (not isinstance(item.get("source_sha256"), str)
                or not _SHA256.fullmatch(item["source_sha256"])
                or type(item.get("output_rows")) is not int or item["output_rows"] <= 0
                or not isinstance(item.get("output_schema"), dict)):
            raise SnapshotError("Publication table metadata is incomplete")
    frames = _read_verified_tables(spark, config, report, path)
    electricity = frames["electricity"]
    try:
        _validate_columns(electricity.schema, APPROVED_BUILDINGS)
    except ValueError as exc:
        raise SnapshotError("Published electricity schema is incompatible with H05.3") from exc
    item = next(item for item in report["tables"] if item["name"] == "electricity")
    expected = {"_bronze_source_uri": item["source_uri"],
                "_bronze_source_sha256": item["source_sha256"],
                "_bronze_data_kind": config.data_kind,
                "_bronze_dataset_reference": config.dataset_reference}
    mismatch = None
    for name, value in expected.items():
        difference = ~F.col(name).eqNullSafe(F.lit(value))
        mismatch = difference if mismatch is None else mismatch | difference
    try:
        if electricity.where(mismatch).limit(1).count():
            raise SnapshotError("Electricity lineage differs from its publication")
    except SnapshotError:
        raise
    except Exception as exc:
        raise SnapshotError("Electricity lineage could not be checked") from exc
    selection = {"run_id": run_id, "report_path": str(path),
                 "finished_at_utc": report.get("finished_at_utc"), "tables": report["tables"]}
    result = transform_selected_electricity(electricity, selection=selection)
    assessment = {
        "publication_run_id": run_id,
        "manifest_sha256": hashlib.sha256(report_bytes).hexdigest(),
        "source_tables": sorted(
            [{key: item[key] for key in ("name", "table_path", "delta_version", "source_sha256")}
             for item in report["tables"]], key=lambda item: item["name"]),
        "manifest_compatibility": "passed",
        "snapshot_binding": "h02_version_metadata_schema_count_checked",
        "input_schema": "passed",
        "input_lineage": "matched_publication_at_check",
        "lineage_propagation": "projection_constructed",
        "transformation_correctness": "not_verified_for_this_input",
        "source_authenticity": "not_established",
        "snapshot_retention": "not_guaranteed",
        "spark_evaluation": "lazy_after_h02_read_checks",
    }
    return assessment, result
