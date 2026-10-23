"""Local UTF-8 CSV contract based on the published BDG2 sources."""
from dataclasses import dataclass
from pathlib import Path
import csv
import hashlib

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class SourceError(ValueError):
    pass


@dataclass(frozen=True)
class SourceInfo:
    path: Path
    columns: tuple[str, ...]
    sha256: str
    size_bytes: int


def inspect_source(path: Path, source_name: str) -> SourceInfo:
    if source_name not in {"electricity", "metadata"}:
        raise SourceError(f"Tipo de fuente desconocido: {source_name}")
    path = path.resolve()
    if not path.is_file():
        raise SourceError(f"No existe el archivo de entrada: {path}")
    try:
        # Hash incrementally to avoid loading the full CSV into memory.
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        with path.open(encoding="utf-8-sig", newline="") as handle:
            columns = next(csv.reader(handle, strict=True), None)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise SourceError(f"No se puede inspeccionar {path}: {exc}") from exc
    if not columns:
        raise SourceError(f"Archivo vacío o sin cabecera: {path}")
    if columns[0].startswith("version https://git-lfs.github.com/spec/"):
        raise SourceError(f"{path} es un puntero Git LFS; falta descargar el CSV.")
    lowered = [name.casefold() for name in columns]
    if any(not name.strip() for name in columns) or len(lowered) != len(set(lowered)):
        raise SourceError(f"Cabeceras vacías o duplicadas: {path}")
    if any(name.startswith("_bronze_") for name in lowered):
        raise SourceError("El prefijo _bronze_ está reservado para trazabilidad.")
    # Delta rejects these characters without column mapping; do not rename source columns.
    if any(any(char in name for char in " ,;{}()\n\t=") for name in columns):
        raise SourceError("Cabecera no compatible con Delta; revise la fuente sin renombrarla.")
    required = {"timestamp"} if source_name == "electricity" else {"building_id"}
    if not required.issubset(columns):
        raise SourceError(f"Faltan columnas obligatorias {sorted(required)} en {path}")
    if source_name == "electricity" and len(columns) < 2:
        raise SourceError("Electricidad requiere timestamp y al menos una columna de edificio.")
    return SourceInfo(path, tuple(columns), digest, path.stat().st_size)


def read_source(spark: SparkSession, info: SourceInfo) -> DataFrame:
    # Type inference could alter source timestamps, identifiers, and numeric text.
    schema = StructType([StructField(name, StringType(), True) for name in info.columns])
    return (
        spark.read.schema(schema)
        .options(header=True, enforceSchema=False, mode="FAILFAST",
                 encoding="UTF-8", inferSchema=False, multiLine=True, escape='"',
                 ignoreLeadingWhiteSpace=False, ignoreTrailingWhiteSpace=False,
                 maxColumns=max(20480, len(info.columns)))
        .csv(str(info.path))
    )
