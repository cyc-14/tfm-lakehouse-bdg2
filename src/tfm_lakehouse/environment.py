"""Verify Delta persistence; importing the package alone is insufficient."""
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
from tempfile import TemporaryDirectory
import tomllib
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


_REPORT_OPERATIONS = (os.open, os.mkdir, os.link, os.unlink)


def validate_environment_report_target(path: Path, *, project_root: Path) -> Path:
    """Validate a new report without creating directories or starting Spark."""
    if (not all(operation in os.supports_dir_fd for operation in _REPORT_OPERATIONS)
            or _REPORT_OPERATIONS[2] not in os.supports_follow_symlinks
            or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY")):
        raise ValueError("La publicación segura requiere las primitivas de Linux/WSL.")
    root = Path(os.path.abspath(project_root))
    expanded = path.expanduser()
    if ".." in expanded.parts or "\0" in str(expanded):
        raise ValueError("No se admiten '..' ni caracteres nulos en el destino del informe.")
    target = root / expanded
    allowed = root / "data/checks/environment"
    if target.parent != allowed or target.suffix != ".json":
        raise ValueError("El informe debe ser un JSON nuevo en data/checks/environment/.")
    for component in (target, *target.parents):
        if component.is_symlink():
            raise ValueError("No se admiten enlaces simbólicos en el destino del informe.")
        if component != target and component.exists() and not component.is_dir():
            raise ValueError("Un componente del destino no es un directorio.")
    manifest = root / "pyproject.toml"
    if manifest.is_symlink() or not (root / "src/tfm_lakehouse").is_dir():
        raise ValueError("Ejecute --report desde la raíz del proyecto.")
    try:
        with manifest.open("rb") as handle:
            project = tomllib.load(handle)
        if project.get("project", {}).get("name") != "tfm-lakehouse":
            raise ValueError("Ejecute --report desde la raíz del proyecto.")
    except (OSError, tomllib.TOMLDecodeError, AttributeError) as exc:
        raise ValueError("Ejecute --report desde la raíz del proyecto.") from exc
    if os.path.lexists(target):
        raise FileExistsError(f"El informe ya existe; elija un nombre nuevo: {target.name}")
    return target


def _open_report_directory(root: Path) -> int:
    """Open each component without following links; create only reserved directories."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptors = []
    result = None
    failure = None
    try:
        descriptor = os.open(root.anchor, flags)
        descriptors.append(descriptor)
        for component in root.parts[1:]:
            descriptor = os.open(component, flags, dir_fd=descriptor)
            descriptors.append(descriptor)
        for component in ("data", "checks", "environment"):
            try:
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
            except FileExistsError:
                pass
            descriptor = os.open(component, flags, dir_fd=descriptor)
            descriptors.append(descriptor)
        result = descriptors.pop()
    except BaseException as exc:
        failure = exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except BaseException as exc:
                failure = _report_cleanup_failure(failure, exc)
        if failure is not None and result is not None:
            try:
                os.close(result)
            except BaseException as exc:
                failure = _report_cleanup_failure(failure, exc)
    if failure is not None:
        raise failure
    return result


def _report_cleanup_failure(primary: BaseException | None, secondary: BaseException
                            ) -> BaseException:
    if primary is None:
        return secondary
    if isinstance(primary, Exception) and not isinstance(secondary, Exception):
        secondary.__cause__ = primary
        return secondary
    primary.add_note(f"No se pudo completar la limpieza del informe: {secondary}")
    return primary


def write_environment_report(target: Path, report: dict, *, project_root: Path) -> None:
    """Publish complete JSON with no replacement, using a trusted directory descriptor."""
    target = validate_environment_report_target(target, project_root=project_root)
    payload = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    temporary = f".environment-{uuid4().hex}.tmp"
    descriptor = _open_report_directory(Path(os.path.abspath(project_root)))
    created = False
    failure = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        file_descriptor = os.open(temporary, flags, 0o600, dir_fd=descriptor)
        created = True
        handle = None
        try:
            handle = os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n")
            handle.write(payload)
        except BaseException as exc:
            failure = exc
        finally:
            try:
                if handle is not None:
                    handle.close()
                else:
                    os.close(file_descriptor)
            except BaseException as exc:
                failure = _report_cleanup_failure(failure, exc)
        if failure is None:
            # Unlike replace(), link() rejects a destination created after validation.
            os.link(temporary, target.name, src_dir_fd=descriptor,
                    dst_dir_fd=descriptor, follow_symlinks=False)
    except BaseException as exc:
        failure = exc
    finally:
        if created:
            try:
                os.unlink(temporary, dir_fd=descriptor)
            except FileNotFoundError:
                pass
            except BaseException as exc:
                failure = _report_cleanup_failure(failure, exc)
        try:
            os.close(descriptor)
        except BaseException as exc:
            failure = _report_cleanup_failure(failure, exc)
    if failure is not None:
        raise failure


def verify_delta(spark: SparkSession) -> dict:
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("label", StringType(), True),
        StructField("value", DoubleType(), True),
    ])
    expected = [(1, "synthetic-zero", 0.0), (2, "synthetic-decimal", 12.5),
                (3, "synthetic-missing", None)]
    with TemporaryDirectory(prefix="tfm-delta-check-") as temporary:
        target = Path(temporary) / "table"
        spark.createDataFrame(expected, schema).write.format("delta").mode("errorifexists").save(str(target))
        if not list((target / "_delta_log").glob("*.json")):
            raise RuntimeError("Falta el registro de transacciones Delta.")
        if not list(target.glob("*.parquet")):
            raise RuntimeError("Faltan archivos de datos Parquet.")
        recovered = spark.read.format("delta").load(str(target))
        rows = [tuple(row) for row in recovered.orderBy("id").collect()]
        if recovered.schema != schema or rows != expected or recovered.count() != 3:
            raise RuntimeError("No coinciden esquema, valores o número de filas recuperados.")
        return {
            "status": "passed", "data_kind": "synthetic", "rows_written": 3,
            "rows_read": len(rows), "schema": recovered.schema.jsonValue(),
            "delta_log_verified": True, "parquet_verified": True,
            "temporary_table_removed_on_exit": True,
            "python": platform.python_version(), "pyspark": spark.version,
            "delta_spark": version("delta-spark"),
            "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "maven_packages": spark.conf.get("spark.jars.packages"),
        }
