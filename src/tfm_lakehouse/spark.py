"""Centralized configuration for local Spark and Delta sessions."""
from contextlib import contextmanager
from pathlib import Path
import os
import sys
from typing import Iterator

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@contextmanager
def spark_session(
    app_name: str = "tfm-lakehouse",
    master: str = "local[2]",
    ivy_dir: Path | None = None,
) -> Iterator[SparkSession]:
    # An existing session may not have the required Delta extensions.
    if SparkSession.getActiveSession() is not None or SparkSession._instantiatedSession is not None:
        raise RuntimeError("Cierre la SparkSession existente antes de iniciar el proyecto.")
    if master != "local" and not (master.startswith("local[") and master.endswith("]")):
        raise ValueError("Este prototipo admite únicamente Spark local.")
    builder = (
        SparkSession.builder.master(master).appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if ivy_dir is not None:
        builder = builder.config("spark.jars.ivy", str(ivy_dir.resolve()))
    # SparkContext reads PYSPARK_PYTHON when choosing workers; use the driver's interpreter.
    previous = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = None
    try:
        # The Python package does not bundle Delta JARs; Ivy resolves them on first startup.
        spark = configure_spark_with_delta_pip(
            builder.config("spark.pyspark.python", sys.executable)
        ).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        yield spark
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            if previous is None:
                os.environ.pop("PYSPARK_PYTHON", None)
            else:
                os.environ["PYSPARK_PYTHON"] = previous
