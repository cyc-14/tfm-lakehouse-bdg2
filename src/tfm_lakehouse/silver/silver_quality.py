"""Conservative, non-persisting diagnostics for selected electricity records."""
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, IntegralType, StringType,
)

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS

_NUMERIC_STATUSES = (
    "missing_null", "missing_token", "finite", "invalid_syntax",
    "nonfinite_literal", "overflow", "underflow", "scale_exceeded",
)
_TIME_STATUSES = (
    "missing_null", "missing_empty", "invalid_format", "invalid_calendar",
    "invalid_time", "valid_nominal",
)
_CONTEXT = (
    "_bronze_source_name", "_bronze_snapshot_run_id", "_bronze_delta_version",
    "_bronze_table_path",
)


def _validate_schema(frame: DataFrame) -> tuple[str, ...]:
    names = frame.schema.fieldNames()
    if any(not name.strip() for name in names) or len({n.casefold() for n in names}) != len(names):
        raise ValueError("Input column names must be nonempty and unambiguous")
    required = {
        "building_id": StringType(), "timestamp_original": StringType(),
        "electricity_value_original": StringType(),
        "electricity_value_numeric": DecimalType(38, 18),
        "value_parse_status": StringType(), "timestamp_parse_status": StringType(),
        "nominal_date": DateType(), "nominal_second_of_day": IntegerType(),
        "is_missing": BooleanType(), "is_zero": BooleanType(), "is_negative": BooleanType(),
        **{name: StringType() for name in _LINEAGE_COLUMNS},
    }
    for name, expected in required.items():
        if name not in names or frame.schema[name].dataType != expected:
            raise ValueError(f"Missing or incompatible Silver field: {name}")
    present = tuple(name for name in _CONTEXT if name in names)
    if present and present != _CONTEXT:
        raise ValueError("Publication context must contain all four fields or none")
    for name in present:
        kind = frame.schema[name].dataType
        if not isinstance(kind, IntegralType if name == "_bronze_delta_version" else StringType):
            raise ValueError(f"Incompatible publication field: {name}")
    return ("building_id", *_LINEAGE_COLUMNS, *present)


def summarize_electricity_quality(frame: DataFrame) -> DataFrame:
    """Summarize existing H05.2 statuses by building and declared Bronze context.

    Validation executes one Spark action and rejects unknown buildings or status
    values instead of filtering them. The returned aggregation is lazy; callers
    must keep the input snapshot available and stable until it is evaluated.
    Input validation evaluates the supplied frame; this function opens no paths
    directly and performs no writes. It does not establish source authenticity
    or authorize processing real data.
    """
    groups = _validate_schema(frame)
    status = F.col("value_parse_status")
    number = F.col("electricity_value_numeric")
    finite = status == "finite"
    invalid = (
        F.col("building_id").isNull() | ~F.col("building_id").isin(*APPROVED_BUILDINGS)
        | F.col("value_parse_status").isNull()
        | ~F.col("value_parse_status").isin(*_NUMERIC_STATUSES)
        | F.col("timestamp_parse_status").isNull()
        | ~F.col("timestamp_parse_status").isin(*_TIME_STATUSES)
        | ~F.col("is_missing").eqNullSafe(status.isin("missing_null", "missing_token"))
        | (finite & (number.isNull()
                     | ~F.col("is_zero").eqNullSafe(number == F.lit(0))
                     | ~F.col("is_negative").eqNullSafe(number < F.lit(0))))
        | (~finite & (number.isNotNull() | F.col("is_zero").isNotNull()
                      | F.col("is_negative").isNotNull()))
        | ((F.col("timestamp_parse_status") == "valid_nominal")
           & (F.col("timestamp_original").isNull() | F.col("nominal_date").isNull()
              | F.col("nominal_second_of_day").isNull()))
    )
    if frame.where(invalid).limit(1).count():
        raise ValueError("Unexpected building identifier or inconsistent interpretation")

    # Invalid labels, including nulls, are deliberately excluded from repeated
    # nominal keys. Window partitioning includes provenance to avoid mixing runs.
    key = Window.partitionBy(*groups, "timestamp_original")
    repeated = ((F.col("timestamp_parse_status") == "valid_nominal")
                & (F.sum(F.when(F.col("timestamp_parse_status") == "valid_nominal", 1)
                         .otherwise(0)).over(key) > 1))
    annotated = frame.withColumn("__h056_repeated_key", repeated)
    def count_if(condition):
        return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).cast("long")

    metrics = [F.count(F.lit(1)).alias("total_occurrences")]
    metrics.extend(count_if(F.col("value_parse_status") == status).alias("numeric_" + status)
                   for status in _NUMERIC_STATUSES)
    metrics.extend([
        count_if(F.col("is_missing") == F.lit(True)).alias("missing_occurrences"),
        count_if(F.col("is_zero") == F.lit(True)).alias("zero_occurrences"),
        count_if(F.col("is_negative") == F.lit(True)).alias("negative_occurrences"),
    ])
    metrics.extend(count_if(F.col("timestamp_parse_status") == status).alias("timestamp_" + status)
                   for status in _TIME_STATUSES)
    metrics.extend([
        count_if(F.col("timestamp_parse_status") != "valid_nominal")
        .alias("unclassifiable_timestamps"),
        F.countDistinct(F.when(F.col("__h056_repeated_key"), F.col("timestamp_original")))
        .alias("repeated_nominal_keys"),
        count_if(F.col("__h056_repeated_key")).alias("occurrences_in_repeated_nominal_keys"),
    ])
    return annotated.groupBy(*groups).agg(*metrics)
