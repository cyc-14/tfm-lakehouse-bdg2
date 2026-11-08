"""Exact linear IQR diagnostics over the approved nominal electricity period."""
from datetime import date
from decimal import Decimal, Inexact, localcontext

from pyspark.sql import Column, DataFrame, Window, functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.silver.silver_electricity import _literal_column
from tfm_lakehouse.silver.silver_electricity_numeric import _interpret_udf as _numeric_udf
from tfm_lakehouse.silver.silver_electricity_temporal import _interpret_udf as _temporal_udf
from tfm_lakehouse.silver.silver_quality import _validate_schema

IQR_METHOD = "electricity-iqr-linear-decimal-v1"
EXPECTED_NOMINAL_HOURS = 17544
MINIMUM_COVERAGE_COUNT = 16667
_START, _END = date(2016, 1, 1), date(2017, 12, 31)
_DECIMAL_METRICS = ("q1", "q3", "iqr", "lower_limit", "upper_limit")
_COUNTS = ("eligible_observation_count", "covered_nominal_hours", "off_grid_observation_count")
_OUTPUTS = (
    *_DECIMAL_METRICS, *_COUNTS, "valid_observation_count", "expected_nominal_hours",
    "minimum_coverage_count", "valid_observation_coverage_percentage",
    "nominal_hour_coverage_percentage", "valid_observation_coverage_accepted",
    "nominal_hour_coverage_accepted", "iqr_evaluable_count", "iqr_outlier_count",
    "iqr_outlier_percentage", "iqr_zero_spread", "iqr_calculation_status",
    "iqr_accepted", "iqr_method", "is_iqr_outlier", "iqr_evaluation_status",
)
_EXACT_RESULT = StructType([
    *[StructField(name, StringType(), True) for name in _DECIMAL_METRICS],
    StructField("iqr_zero_spread", BooleanType(), True),
    StructField("iqr_calculation_status", StringType(), False),
])


def _validate_input(frame: DataFrame) -> None:
    """Check schema and interpretations with existing parsers; read no paths."""
    _validate_schema(frame)
    reserved = {name.casefold() for name in _OUTPUTS}
    if any(name.casefold() in reserved or name.casefold().startswith("__iqr_")
           for name in frame.columns):
        raise ValueError("Input column collides with a reserved IQR field")
    numeric = _numeric_udf(F.col("electricity_value_original"))
    temporal = _temporal_udf(F.col("timestamp_original"))
    invalid = (F.col("building_id").isNull()
               | ~F.col("building_id").isin(*APPROVED_BUILDINGS))
    expected = {
        "electricity_value_numeric": numeric["value"],
        "value_parse_status": numeric["status"],
        "is_missing": numeric["status"].isin("missing_null", "missing_token"),
        "is_zero": numeric["zero"], "is_negative": numeric["negative"],
        "nominal_date": temporal["date"], "nominal_second_of_day": temporal["second"],
        "timestamp_parse_status": temporal["status"],
    }
    for name, value in expected.items():
        invalid = invalid | ~F.col(name).eqNullSafe(value)
    if frame.where(invalid).limit(1).count():
        raise ValueError("Unexpected building identifier or inconsistent Silver interpretation")


def _eligible() -> Column:
    return ((F.col("value_parse_status") == "finite")
            & (F.col("timestamp_parse_status") == "valid_nominal")
            & F.col("nominal_date").between(_START, _END))


def _exact_statistics(
    count: int, q1_low: Decimal | None, q1_high: Decimal | None,
    q3_low: Decimal | None, q3_high: Decimal | None,
) -> tuple:
    """Interpolate only four order statistics, retaining the full decimal range.

    Quartiles need at most scale 20; thresholds at most scale 21 and 42 total
    digits. Precision 80 with an Inexact trap rejects unexpected rounding.
    Spark percentile returns Double, so it cannot implement this numeric contract.
    """
    if count < 4:
        return (None,) * 6 + ("insufficient_population",)
    if any(value is None for value in (q1_low, q1_high, q3_low, q3_high)):
        raise ValueError("Missing IQR order statistic for an evaluable population")
    with localcontext() as context:
        context.prec = 80
        context.traps[Inexact] = True
        q1 = q1_low + (q1_high - q1_low) * Decimal((count - 1) % 4) / 4
        q3 = q3_low + (q3_high - q3_low) * Decimal((3 * (count - 1)) % 4) / 4
        spread = q3 - q1
        if spread < 0:
            raise ValueError("Unordered IQR statistics")
        limits = (q1, q3, spread, q1 - Decimal("1.5") * spread,
                  q3 + Decimal("1.5") * spread)
        exact = tuple(format(value.normalize(), "f") for value in limits)
        return (*exact, spread == 0, "calculated")


_statistics_udf = F.udf(_exact_statistics, _EXACT_RESULT)


def _outlier(value: Decimal | None, lower: str | None, upper: str | None) -> bool | None:
    if value is None or lower is None or upper is None:
        return None
    return value < Decimal(lower) or value > Decimal(upper)


_outlier_udf = F.udf(_outlier, BooleanType())


def _statistics(frame: DataFrame) -> DataFrame:
    # Project controlled internal names; no input extras enter windows or groups.
    population = frame.where(_eligible()).select(
        "building_id", "electricity_value_numeric", "nominal_date", "nominal_second_of_day",
    )
    group = Window.partitionBy("building_id")
    ordered = group.orderBy("electricity_value_numeric").rowsBetween(
        Window.unboundedPreceding, Window.currentRow,
    )
    ranked = (population.withColumn("__iqr_n", F.count(F.lit(1)).over(group))
              .withColumn("__iqr_rank", F.count(F.lit(1)).over(ordered)))
    # Integer division avoids floating positions. For m=n-1, floor(3m/4) is
    # m-floor(m/4)-1 when m%4!=0, avoiding overflow from forming 3m in a Long.
    m = F.col("__iqr_n") - 1
    quarter = F.expr("(__iqr_n - 1) DIV 4")
    remainder = m % 4
    q1_index = quarter + 1
    q3_index = m - quarter - F.when(remainder != 0, 1).otherwise(0) + 1
    positions = (
        q1_index, q1_index + F.when(remainder != 0, 1).otherwise(0),
        q3_index, q3_index + F.when(remainder != 0, 1).otherwise(0),
    )
    on_grid = F.col("nominal_second_of_day") % 3600 == 0
    aggregates = [
        F.count(F.lit(1)).alias("eligible_observation_count"),
        F.countDistinct(F.when(on_grid, F.struct("nominal_date", "nominal_second_of_day")))
        .alias("covered_nominal_hours"),
        F.sum(F.when(~on_grid, 1).otherwise(0)).cast("long").alias("off_grid_observation_count"),
    ]
    aggregates.extend(
        F.max(F.when(F.col("__iqr_rank") == position, F.col("electricity_value_numeric")))
        .alias(f"__iqr_order_{index}") for index, position in enumerate(positions)
    )
    grouped = ranked.groupBy("building_id").agg(*aggregates)
    buildings = frame.sparkSession.createDataFrame(
        [(name,) for name in APPROVED_BUILDINGS], "building_id string",
    )
    complete = buildings.join(grouped, "building_id", "left").fillna(0, subset=list(_COUNTS))
    exact = _statistics_udf(
        F.col("eligible_observation_count"), *[F.col(f"__iqr_order_{i}") for i in range(4)],
    )
    result = complete.select("building_id", *_COUNTS, exact.alias("__iqr_exact"))
    result = result.select("building_id", *_COUNTS, "__iqr_exact.*")
    observation_ok = F.col("eligible_observation_count") >= MINIMUM_COVERAGE_COUNT
    grid_ok = F.col("covered_nominal_hours") >= MINIMUM_COVERAGE_COUNT
    calculated = F.col("iqr_calculation_status") == "calculated"
    return (result
            .withColumn("valid_observation_count", F.col("eligible_observation_count"))
            .withColumn("expected_nominal_hours", F.lit(EXPECTED_NOMINAL_HOURS))
            .withColumn("minimum_coverage_count", F.lit(MINIMUM_COVERAGE_COUNT))
            .withColumn("valid_observation_coverage_percentage",
                        100.0 * F.col("valid_observation_count") / EXPECTED_NOMINAL_HOURS)
            .withColumn("nominal_hour_coverage_percentage",
                        100.0 * F.col("covered_nominal_hours") / EXPECTED_NOMINAL_HOURS)
            .withColumn("valid_observation_coverage_accepted", observation_ok)
            .withColumn("nominal_hour_coverage_accepted", grid_ok)
            .withColumn("iqr_evaluable_count", F.when(calculated, F.col("eligible_observation_count")))
            .withColumn("iqr_accepted", calculated & observation_ok & grid_ok)
            .withColumn("iqr_method", F.lit(IQR_METHOD)))


def _annotate(frame: DataFrame, statistics: DataFrame) -> DataFrame:
    # Statistics are constructed here, one row per approved building. External
    # summaries are never accepted, so duplicate keys cannot multiply records.
    fields = ("lower_limit", "upper_limit", "iqr_calculation_status", "iqr_accepted")
    lookup = statistics.select("building_id", *fields)
    joined = frame.join(F.broadcast(lookup), "building_id", "left")
    evaluable = _eligible() & (F.col("iqr_calculation_status") == "calculated")
    status = (F.when(F.col("value_parse_status") != "finite", "numeric_not_eligible")
              .when(F.col("timestamp_parse_status") != "valid_nominal", "invalid_nominal_timestamp")
              .when(~F.col("nominal_date").between(_START, _END), "outside_study_period")
              .when(F.col("iqr_calculation_status") == "insufficient_population",
                    "insufficient_population")
              .when(evaluable, "evaluated")
              .otherwise("calculation_error"))
    return joined.select(
        *[_literal_column(name) for name in frame.columns],
        F.when(evaluable, _outlier_udf(
            F.col("electricity_value_numeric"), F.col("lower_limit"), F.col("upper_limit"),
        )).alias("is_iqr_outlier"),
        status.alias("iqr_evaluation_status"), F.col("iqr_accepted"),
    )


def summarize_electricity_iqr(frame: DataFrame) -> DataFrame:
    """Return 16 per-building IQR results; exact decimal metrics are strings.

    Existing parsers validate the input in one Spark action. All returned work
    is lazy and requires an available, stable input until evaluation. Populations
    span the full nominal 2016-2017 period, across all supplied provenance groups.
    No publication identity is established, no paths are opened directly and no
    writes or cache ownership are introduced. Calculation errors fail execution.
    """
    _validate_input(frame)
    statistics = _statistics(frame)
    annotated = _annotate(frame, statistics)
    counts = annotated.groupBy("building_id").agg(
        F.sum(F.when(F.col("is_iqr_outlier"), 1).otherwise(0)).cast("long")
        .alias("__iqr_outliers"),
    )
    result = statistics.join(counts, "building_id", "left")
    result = result.withColumn(
        "iqr_outlier_count", F.when(F.col("iqr_calculation_status") == "calculated",
                                    F.coalesce(F.col("__iqr_outliers"), F.lit(0))),
    )
    return (result.withColumn("iqr_outlier_percentage",
                              100.0 * F.col("iqr_outlier_count") / F.col("iqr_evaluable_count"))
            .drop("__iqr_outliers"))


def annotate_electricity_iqr(frame: DataFrame) -> DataFrame:
    """Append nullable outlier flag, evaluation status and building acceptance.

    Low coverage does not suppress calculated flags. Every original occurrence,
    field and value is preserved; output ordering is unspecified. The same lazy
    input lifetime and publication limitations as the summary apply.
    """
    _validate_input(frame)
    return _annotate(frame, _statistics(frame))
