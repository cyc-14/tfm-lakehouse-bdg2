"""Lossless, provisional electricity reshaping; no Silver persistence or parsing."""
from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructType

from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.snapshot import read_bronze_snapshot

_LINEAGE_COLUMNS = (
    "_bronze_source_uri",
    "_bronze_source_sha256",
    "_bronze_data_kind",
    "_bronze_dataset_reference",
)


def _validate_columns(schema: StructType, columns: Sequence[str]) -> tuple[str, ...]:
    """Validate the current Bronze string schema without Spark actions."""
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence):
        raise ValueError("Electricity columns must be an explicit sequence of names")
    selected = tuple(columns)
    if not selected or any(not isinstance(name, str) or not name.strip() for name in selected):
        raise ValueError("Select at least one nonempty electricity column name")
    if len(set(selected)) != len(selected):
        raise ValueError("Duplicate electricity column names are not allowed")

    names = schema.fieldNames()
    folded = [name.casefold() for name in names]
    if any(not name.strip() for name in names) or len(set(folded)) != len(names):
        raise ValueError("Bronze column names must be nonempty and unambiguous ignoring case")
    if not {"timestamp", *_LINEAGE_COLUMNS}.issubset(names):
        raise ValueError("Missing timestamp or required Bronze lineage columns")
    if any(not isinstance(field.dataType, StringType) for field in schema):
        raise ValueError("Every Bronze input column must have StringType")
    if any(name.casefold().startswith("_bronze_") and name not in _LINEAGE_COLUMNS
           for name in names):
        raise ValueError("Unsupported Bronze technical columns; review the input contract")
    for name in selected:
        if name.casefold() == "timestamp" or name.casefold().startswith("_bronze_"):
            raise ValueError("Temporal and Bronze technical columns are not electricity columns")
        if name not in names:
            raise ValueError(f"Electricity column does not exist: {name!r}")
    return selected


def _literal_column(name: str) -> Column:
    # Quote the entire identifier, including embedded backticks, not a SQL expression.
    return F.col("`" + name.replace("`", "``") + "`")


def electricity_to_long(bronze: DataFrame, electricity_columns: Sequence[str]) -> DataFrame:
    """Emit one row per input occurrence and explicitly selected building column.

    All inputs must be strings under the current Bronze contract. Values,
    timestamps, nulls and multiplicity are preserved; output order is unspecified.
    This lazy projection performs no counts, parsing, filtering, writes or joins.
    It cannot authenticate a caller-supplied DataFrame as a published snapshot.
    Use read_electricity_long() when publication identity is required.
    """
    selected = _validate_columns(bronze.schema, electricity_columns)
    measurements = F.array(*[
        F.struct(F.lit(name).alias("building_id"),
                 _literal_column(name).alias("electricity_value_original"))
        for name in selected
    ])
    # Each array has exactly len(selected) non-null structs, even for null values.
    expanded = bronze.select(
        _literal_column("timestamp").alias("timestamp_original"),
        *[_literal_column(name) for name in _LINEAGE_COLUMNS],
        F.explode(measurements).alias("_measurement"),
    )
    return expanded.select(
        F.col("_measurement.building_id").alias("building_id"),
        "timestamp_original",
        F.col("_measurement.electricity_value_original").alias("electricity_value_original"),
        *_LINEAGE_COLUMNS,
    )


def read_electricity_long(
    spark: SparkSession, config: BronzeConfig, electricity_columns: Sequence[str],
) -> tuple[dict, DataFrame]:
    """Read a complete Bronze publication and attach its electricity identity.

    The existing reader verifies all configured sources and their recorded Delta
    versions. This function adds no writes or cross-layer transaction guarantee.
    The returned selection is the reader's original publication context.
    """
    selection, frames = read_bronze_snapshot(spark, config)
    item = next(item for item in selection["tables"] if item["name"] == "electricity")
    result = electricity_to_long(frames["electricity"], electricity_columns)
    return selection, (
        result.withColumn("_bronze_source_name", F.lit("electricity"))
        .withColumn("_bronze_snapshot_run_id", F.lit(selection["run_id"]))
        .withColumn("_bronze_delta_version", F.lit(item["delta_version"]))
        .withColumn("_bronze_table_path", F.lit(item["table_path"]))
    )
