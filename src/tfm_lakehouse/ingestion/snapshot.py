"""Read only versions belonging to a complete, compatible Bronze publication."""
import json
import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

from tfm_lakehouse.config import BronzeConfig

LOGGER = logging.getLogger(__name__)


class SnapshotError(RuntimeError):
    pass


def _same_configuration(report: dict, config: BronzeConfig) -> bool:
    return isinstance(report, dict) and (
        type(report.get("report_contract_version")) is int
        and report["report_contract_version"] == 1
        and report.get("data_kind") == config.data_kind
        and report.get("dataset_reference") == config.dataset_reference
        and report.get("configured_sources") == {
            name: source.resolve().as_uri() for name, source in config.sources.items()
        }
    )


def _validate_tables(report: dict, config: BronzeConfig, path: Path) -> None:
    tables = report.get("tables")
    if not isinstance(tables, list) or any(not isinstance(item, dict) for item in tables):
        raise SnapshotError(f"Lista de tablas inválida en {path}")
    names = [item.get("name") for item in tables]
    if any(not isinstance(name, str) for name in names) or (
        len(names) != len(config.sources) or set(names) != set(config.sources)
    ):
        raise SnapshotError(f"Tablas incompletas o duplicadas en {path}")
    for item in tables:
        try:
            same_path = (Path(item["table_path"]).resolve()
                         == config.table_path(item["name"]).resolve())
            same_source = item["source_uri"] == config.sources[item["name"]].resolve().as_uri()
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise SnapshotError(f"Ruta inválida en {path}") from exc
        if not same_path or not same_source:
            raise SnapshotError(f"El informe apunta a otra tabla o fuente: {path}")


def read_bronze_snapshot(spark: SparkSession, config: BronzeConfig) -> tuple[dict, dict[str, DataFrame]]:
    config.validate()
    directory = config.namespace / "_runs"
    candidates = sorted(directory.glob("*.json"), key=lambda path: (path.stat().st_mtime_ns, path.name),
                        reverse=True)
    latest_attempt = None
    for path in candidates:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            LOGGER.warning("Se omite un informe ilegible: %s", path)
            continue
        if not _same_configuration(report, config):
            continue
        if latest_attempt is None:
            latest_attempt = {"run_id": report.get("run_id"), "status": report.get("status")}
        if report.get("status") != "passed":
            continue
        _validate_tables(report, config, path)
        if not isinstance(report.get("run_id"), str) or not report["run_id"]:
            raise SnapshotError(f"El informe satisfactorio carece de run_id: {path}")
        frames = {}
        for item in report["tables"]:
            name = item["name"]
            version = item.get("delta_version")
            if type(version) is not int or version < 0 or item.get("status") != "verified":
                raise SnapshotError(f"Versión o estado inválido para {name} en {path}")
            try:
                expected_schema = StructType.fromJson(item["output_schema"])
                expected_metadata = json.dumps({
                    "run_id": report["run_id"], "table_name": name,
                    "source_sha256": item["source_sha256"],
                    "data_kind": config.data_kind,
                    "dataset_reference": config.dataset_reference,
                }, sort_keys=True)
                # A recreated table can reuse a version, schema, and row count.
                commits = (DeltaTable.forPath(spark, str(config.table_path(name))).history()
                           .where(F.col("version") == F.lit(version))
                           .select("userMetadata").limit(2).collect())
                if len(commits) != 1 or commits[0]["userMetadata"] != expected_metadata:
                    raise SnapshotError(
                        f"El commit de {name} no pertenece a la ejecución publicada {report['run_id']}."
                    )
                frame = (spark.read.format("delta").option("versionAsOf", version)
                         .load(str(config.table_path(name))))
                if frame.schema != expected_schema or frame.count() != item["output_rows"]:
                    raise SnapshotError(f"No coincide el snapshot publicado de {name}")
            except SnapshotError:
                raise
            except Exception as exc:
                raise SnapshotError(
                    f"No se puede recuperar {name} en la versión {version}; no se usará la última versión."
                ) from exc
            frames[name] = frame
        selection = {
            "run_id": report["run_id"], "report_path": str(path),
            "finished_at_utc": report.get("finished_at_utc"), "tables": report["tables"],
            "latest_attempt": latest_attempt,
            "using_previous_successful_run": latest_attempt["run_id"] != report["run_id"],
        }
        return selection, frames
    raise SnapshotError(
        "No existe una ejecución completa compatible con esta configuración. "
        "Ejecute ingest-bronze; los informes antiguos sin contrato de publicación no son válidos."
    )
