"""Verify Delta persistence; importing the package alone is insufficient."""
from importlib.metadata import version
from pathlib import Path
import platform
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


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
