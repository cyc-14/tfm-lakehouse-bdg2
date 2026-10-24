"""Validate complete local CSV sources before Spark parses or persists them."""
from dataclasses import dataclass
from pathlib import Path
import csv
import hashlib
from urllib.parse import unquote, urlsplit

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
    rows: int


def source_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def inspect_source(path: Path, source_name: str) -> SourceInfo:
    required_by_source = {
        "electricity": {"timestamp"},
        "metadata": {"building_id"},
        "weather": {"timestamp", "site_id"},
    }
    if source_name not in required_by_source:
        raise SourceError(f"Tipo de fuente desconocido: {source_name}")
    path = path.resolve()
    # Hadoop globbing must not select files other than the one whose hash is recorded.
    if any(char in str(path) for char in "*?[]{}\\"):
        raise SourceError(f"No se permiten metacaracteres de glob en la ruta: {path}")
    if not path.is_file():
        raise SourceError(f"No existe el archivo de entrada: {path}")
    try:
        before = path.stat()
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, delimiter=",", quotechar='"', doublequote=True,
                                escapechar=None, skipinitialspace=False, strict=True)
            columns = next(reader, None)
            if not columns:
                raise SourceError(f"Archivo vacío o sin cabecera: {path}")
            if columns[0].startswith("version https://git-lfs.github.com/spec/"):
                raise SourceError(f"{path} es un puntero Git LFS; falta descargar el CSV.")
            lowered = [name.casefold() for name in columns]
            if any(not name.strip() for name in columns) or len(lowered) != len(set(lowered)):
                raise SourceError(f"Cabeceras vacías o duplicadas: {path}")
            if any(name.startswith("_bronze_") for name in lowered):
                raise SourceError("El prefijo _bronze_ está reservado para trazabilidad.")
            if any(any(char in name for char in " ,;{}()\n\t=") for name in columns):
                raise SourceError("Cabecera no compatible con Delta; revise la fuente sin renombrarla.")
            required = required_by_source[source_name]
            if not required.issubset(columns):
                raise SourceError(f"Faltan columnas obligatorias {sorted(required)} en {path}")
            if source_name == "electricity" and len(columns) < 2:
                raise SourceError("Electricidad requiere timestamp y al menos una columna de edificio.")
            rows = 0
            # The full stream must be decoded: a header-only check misses invalid UTF-8 and EOF quotes.
            for row in reader:
                if not row:
                    continue
                if len(row) != len(columns):
                    raise SourceError(
                        f"Número de campos incorrecto en {path}, registro {rows + 1}: "
                        f"{len(row)} frente a {len(columns)}."
                    )
                rows += 1
        digest = source_sha256(path)
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_ino, after.st_size, after.st_mtime_ns
        ):
            raise SourceError(f"La fuente cambió durante la validación: {path}")
    except (OSError, UnicodeError, csv.Error) as exc:
        raise SourceError(f"No se puede validar {path}: {exc}") from exc
    return SourceInfo(path, tuple(columns), digest, after.st_size, rows)


def read_source(spark: SparkSession, info: SourceInfo) -> DataFrame:
    # Type inference could alter source timestamps, identifiers, and numeric text.
    schema = StructType([StructField(name, StringType(), True) for name in info.columns])
    frame = (
        spark.read.schema(schema)
        .options(header=True, enforceSchema=False, mode="FAILFAST",
                 encoding="UTF-8", inferSchema=False, multiLine=True, escape='"',
                 unescapedQuoteHandling="RAISE_ERROR",
                 ignoreLeadingWhiteSpace=False, ignoreTrailingWhiteSpace=False,
                 maxColumns=max(20480, len(info.columns)))
        .csv(str(info.path))
    )
    actual = set()
    for uri in frame.inputFiles():
        parsed = urlsplit(uri)
        if parsed.scheme not in {"", "file"} or parsed.netloc not in {"", "localhost"}:
            raise SourceError(f"Spark abrió una fuente no local: {uri}")
        actual.add(Path(unquote(parsed.path)).resolve())
    if actual != {info.path}:
        raise SourceError(f"Spark abrió archivos distintos de la fuente inspeccionada: {info.path}")
    return frame
