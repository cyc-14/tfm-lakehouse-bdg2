"""Full-source reloads without deduplication or analytical normalization."""
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.csv_source import SourceError, inspect_source, read_source

LOGGER = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_bronze(spark: SparkSession, config: BronzeConfig) -> dict:
    config.validate()
    run_id = uuid4().hex
    report = {
        "run_id": run_id, "status": "running", "started_at_utc": _now(),
        "data_kind": config.data_kind, "dataset_reference": config.dataset_reference,
        "write_strategy": "full_snapshot_overwrite_per_source",
        "cross_table_atomic": False, "tables": [],
    }
    reports = config.namespace / "_runs"
    reports.mkdir(parents=True, exist_ok=True)
    report_path = reports / f"{run_id}.json"
    report["report_path"] = str(report_path)
    prepared = []
    # Column pruning can hide malformed CSV fields during count(), even with FAILFAST.
    pruning = spark.conf.get("spark.sql.csv.parser.columnPruning.enabled")
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    try:
        # Reject invalid inputs before either table can replace a previous snapshot.
        sources = [(name, inspect_source(path, name)) for name, path in
                   (("electricity", config.electricity), ("metadata", config.metadata))]
        for name, info in sources:
            frame = read_source(spark, info).persist(StorageLevel.DISK_ONLY)
            prepared.append((name, info, frame))
            count = frame.count()
            if count == 0:
                raise SourceError(f"Fuente sin registros (solo cabecera): {info.path}")
            target = config.namespace / "bronze" / name
            if target.exists() and not DeltaTable.isDeltaTable(spark, str(target)):
                raise IngestionError(f"Destino existente no es una tabla Delta: {target}")
            report["tables"].append({
                "name": name, "status": "validated", "source_uri": info.path.as_uri(),
                "source_sha256": info.sha256, "source_bytes": info.size_bytes,
                "input_rows": count, "input_schema": frame.schema.jsonValue(),
                "table_path": str(target),
            })
        # Sources must stay immutable; detect changes before the first commit.
        for name, info, _ in prepared:
            if inspect_source(info.path, name).sha256 != info.sha256:
                raise SourceError(f"La fuente cambió durante la lectura: {info.path}")

        for (name, info, frame), item in zip(prepared, report["tables"], strict=True):
            enriched = (
                frame.withColumn("_bronze_source_uri", F.lit(info.path.as_uri()))
                .withColumn("_bronze_source_sha256", F.lit(info.sha256))
                .withColumn("_bronze_data_kind", F.lit(config.data_kind))
                .withColumn("_bronze_dataset_reference", F.lit(config.dataset_reference))
            )
            target = item["table_path"]
            # Full replacement makes reruns idempotent without removing source duplicates.
            metadata = json.dumps({
                "source_sha256": info.sha256, "data_kind": config.data_kind,
                "dataset_reference": config.dataset_reference,
            }, sort_keys=True)
            (enriched.write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").option("userMetadata", metadata).save(target))
            item["status"] = "committed"
            recovered = spark.read.format("delta").load(target)
            output_count = recovered.count()
            # Delta makes persisted fields nullable; compare names and types, not nullability.
            expected_types = [(f.name, f.dataType) for f in enriched.schema]
            actual_types = [(f.name, f.dataType) for f in recovered.schema]
            if output_count != item["input_rows"] or actual_types != expected_types:
                raise IngestionError(f"No coincide la lectura posterior de {name}")
            item.update(
                status="verified", output_rows=output_count,
                output_schema=recovered.schema.jsonValue(),
                delta_version=DeltaTable.forPath(spark, target).history(1).first()["version"],
                read_back_verified=True,
            )
            LOGGER.info("%s: entrada=%d salida=%d tabla=%s", name, item["input_rows"],
                        output_count, target)
        report["status"] = "passed"
        return report
    except Exception as exc:
        report.update(status="failed", error_type=type(exc).__name__, error=str(exc))
        LOGGER.exception("Ingesta fallida; informe %s", report_path)
        raise IngestionError(f"Ingesta fallida: {exc}. Informe: {report_path}") from exc
    finally:
        report["finished_at_utc"] = _now()
        for _, _, frame in prepared:
            frame.unpersist()
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", pruning)
        # Keep failure reports too: a later table can fail after an earlier table commits.
        temporary_report = report_path.with_suffix(".tmp")
        temporary_report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary_report.replace(report_path)
