"""Full-source reloads with versioned publication and recoverable failure reports."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.csv_source import SourceError, inspect_source, read_source, source_sha256

LOGGER = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class _Failures:
    """Keep the first diagnostic cause and the first control-flow cancellation."""

    primary: BaseException | None = None
    cancellation: BaseException | None = None
    cancellation_phase: str | None = None

    def record(self, error: BaseException, phase: str) -> None:
        if self.primary is None:
            self.primary = error
        elif error is not self.primary and error is not self.cancellation:
            self.primary.add_note(f"Incidencia secundaria en {phase}: {error}")
        if not isinstance(error, Exception) and self.cancellation is None:
            self.cancellation = error
            self.cancellation_phase = phase

    def propagate(self) -> None:
        error = self.cancellation if self.cancellation is not None else self.primary
        if error is not None:
            if error is not self.primary:
                raise error from self.primary
            raise error


@contextmanager
def _namespace_lock(namespace: Path):
    # The prototype runs in WSL/Linux; this lock coordinates this pipeline's local writers.
    import fcntl

    handle = (namespace / "_ingestion.lock").open("a")
    failures = _Failures()
    locked = False
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise IngestionError("Ya hay otra ingesta ejecutándose en este namespace.") from exc
        locked = True
        yield
    except BaseException as exc:
        failures.record(exc, "namespace")
    finally:
        if locked:
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            except BaseException as exc:
                failures.record(exc, "unlock_namespace")
        try:
            handle.close()
        except BaseException as exc:
            failures.record(exc, "close_namespace_lock")
    failures.propagate()


def _write_report(path: Path, report: dict, *, failures: _Failures | None = None) -> None:
    temporary = path.with_suffix(".tmp")
    local = _Failures()
    handle = None
    created = False
    try:
        handle = temporary.open("x", encoding="utf-8")
        created = True
        try:
            json.dump(report, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        except BaseException as exc:
            local.record(exc, "serialize_report")
        finally:
            try:
                handle.close()
            except BaseException as exc:
                local.record(exc, "close_report")
        if local.primary is None:
            # This replacement is the publication boundary, not the caller's return.
            temporary.replace(path)
    except BaseException as exc:
        local.record(exc, "publish_report")
    finally:
        if created:
            try:
                temporary.unlink(missing_ok=True)
            except BaseException as exc:
                local.record(exc, "remove_report_temporary")
    if failures is not None:
        if local.primary is not None:
            failures.record(local.primary, "publication")
        if local.cancellation is not None and local.cancellation is not local.primary:
            failures.record(local.cancellation, "publication")
    local.propagate()


def _finish_report(report: dict, failures: _Failures, completed: bool) -> None:
    report["finished_at_utc"] = _now()
    report["status"] = "passed" if completed and failures.primary is None else "failed"
    if failures.primary is not None:
        report.update(error_type=type(failures.primary).__name__, error=str(failures.primary))
    if failures.cancellation is not None:
        report["interruption"] = {
            "error_type": type(failures.cancellation).__name__,
            "error": str(failures.cancellation), "phase": failures.cancellation_phase,
        }


def run_bronze(spark: SparkSession, config: BronzeConfig) -> dict:
    config.validate()
    reports = config.namespace / "_runs"
    reports.mkdir(parents=True, exist_ok=True)
    with _namespace_lock(config.namespace):
        return _run_locked(spark, config, reports)


def _run_locked(spark: SparkSession, config: BronzeConfig, reports: Path) -> dict:
    run_id = uuid4().hex
    report_path = reports / f"{run_id}.json"
    report = {
        "report_contract_version": 1, "run_id": run_id, "status": "running",
        "started_at_utc": _now(), "data_kind": config.data_kind,
        "configured_sources": {name: path.resolve().as_uri() for name, path in config.sources.items()},
        "dataset_reference": config.dataset_reference,
        "write_strategy": "full_snapshot_overwrite_per_source",
        "cross_table_atomic": False, "tables": [], "report_path": str(report_path),
        "cleanup_errors": [],
    }
    prepared = []
    failures = _Failures()
    completed = False
    pruning = None
    try:
        # Column pruning can hide malformed CSV fields during count(), even with FAILFAST.
        pruning = spark.conf.get("spark.sql.csv.parser.columnPruning.enabled")
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
        sources = [(name, inspect_source(path, name)) for name, path in config.sources.items()]
        for name, info in sources:
            frame = read_source(spark, info).persist(StorageLevel.DISK_ONLY)
            prepared.append((name, info, frame))
            count = frame.count()
            if count == 0:
                raise SourceError(f"Fuente sin registros (solo cabecera): {info.path}")
            if count != info.rows:
                raise SourceError(
                    f"El lector CSV y Spark no coinciden en {name}: {info.rows} frente a {count}."
                )
            target = config.table_path(name)
            if target.exists() and not DeltaTable.isDeltaTable(spark, str(target)):
                raise IngestionError(f"Destino existente no es una tabla Delta: {target}")
            report["tables"].append({
                "name": name, "status": "validated", "source_uri": info.path.as_uri(),
                "source_sha256": info.sha256, "source_bytes": info.size_bytes,
                "validated_csv_rows": info.rows, "input_rows": count,
                "input_schema": frame.schema.jsonValue(), "table_path": str(target),
            })
        # Check again after materialization; all subsequent writes use the cached records.
        for _, info, _ in prepared:
            if source_sha256(info.path) != info.sha256:
                raise SourceError(f"La fuente cambió durante la lectura: {info.path}")

        for (name, info, frame), item in zip(prepared, report["tables"], strict=True):
            enriched = (
                frame.withColumn("_bronze_source_uri", F.lit(info.path.as_uri()))
                .withColumn("_bronze_source_sha256", F.lit(info.sha256))
                .withColumn("_bronze_data_kind", F.lit(config.data_kind))
                .withColumn("_bronze_dataset_reference", F.lit(config.dataset_reference))
            )
            metadata = json.dumps({
                "run_id": run_id, "table_name": name, "source_sha256": info.sha256,
                "data_kind": config.data_kind, "dataset_reference": config.dataset_reference,
            }, sort_keys=True)
            target = item["table_path"]
            # Replace the snapshot rather than appending; retain duplicates from the source.
            (enriched.write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").option("userMetadata", metadata).save(target))
            item["status"] = "committed"
            # Identify this commit, not a later version from an unrelated Delta writer.
            versions = (DeltaTable.forPath(spark, target).history()
                        .where(F.col("userMetadata") == F.lit(metadata))
                        .select("version").limit(2).collect())
            if len(versions) != 1:
                raise IngestionError(f"No se pudo identificar el commit de {name}.")
            version = versions[0]["version"]
            item["delta_version"] = version
            recovered = spark.read.format("delta").option("versionAsOf", version).load(target)
            count = recovered.count()
            # Delta makes persisted fields nullable; compare names and types, not nullability.
            if count != item["input_rows"] or (
                [(f.name, f.dataType) for f in recovered.schema]
                != [(f.name, f.dataType) for f in enriched.schema]
            ):
                raise IngestionError(f"No coincide la lectura posterior de {name}")
            item.update(
                status="verified", output_rows=count,
                output_schema=recovered.schema.jsonValue(), read_back_verified=True,
            )
            LOGGER.info("%s: entrada=%d salida=%d versión=%d tabla=%s",
                        name, item["input_rows"], count, version, target)
        completed = (len(report["tables"]) == len(config.sources)
                     and all(item["status"] == "verified" for item in report["tables"]))
        if not completed:
            raise IngestionError("No se completaron todas las verificaciones Bronze.")
    except BaseException as exc:
        failures.record(exc, "work")
        LOGGER.exception("Ingesta fallida; informe %s", report_path)
    finally:
        # Cleanup is best-effort; it must not hide the original error or prevent reporting.
        for name, _, frame in prepared:
            try:
                frame.unpersist()
            except BaseException as exc:
                report["cleanup_errors"].append(
                    {"operation": f"unpersist:{name}", "error_type": type(exc).__name__,
                     "error": str(exc)}
                )
                failures.record(exc, f"unpersist:{name}")
        if pruning is not None:
            try:
                spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", pruning)
            except BaseException as exc:
                report["cleanup_errors"].append(
                    {"operation": "restore_csv_pruning", "error_type": type(exc).__name__,
                     "error": str(exc)}
                )
                failures.record(exc, "restore_csv_pruning")
        _finish_report(report, failures, completed)
        try:
            _write_report(report_path, report, failures=failures)
        except BaseException as exc:
            failures.record(exc, "publication")
            # The final name may already contain a complete publication. Never demote it.
            # If no final name exists, make one bounded attempt to record the failure.
            try:
                report_path.lstat()
                absent = False
            except FileNotFoundError:
                absent = True
            except BaseException as inspection_error:
                failures.record(inspection_error, "inspect_publication")
                absent = False
            if absent:
                _finish_report(report, failures, False)
                try:
                    _write_report(report_path, report, failures=failures)
                except BaseException as secondary:
                    failures.record(secondary, "publication_retry")
            LOGGER.exception("No se pudo publicar el informe %s", report_path)
    if failures.cancellation is not None:
        failures.propagate()
    if failures.primary is not None:
        raise IngestionError(
            f"Ingesta fallida: {failures.primary}. Informe previsto: {report_path}"
        ) from failures.primary
    return report
