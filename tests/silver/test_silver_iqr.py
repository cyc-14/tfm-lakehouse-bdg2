"""Invented populations and independent rational oracles for the IQR engine."""
from collections import Counter
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from statistics import quantiles

import pytest
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import (
    BooleanType, DecimalType, LongType, StringType, StructField, StructType,
)

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS
from tfm_lakehouse.silver.silver_electricity_numeric import interpret_electricity_numeric
from tfm_lakehouse.silver.silver_electricity_temporal import interpret_electricity_temporal
from tfm_lakehouse.silver.silver_iqr import (
    IQR_METHOD, _exact_statistics, _outlier, annotate_electricity_iqr,
    summarize_electricity_iqr,
)

_LONG_SCHEMA = StructType([StructField(name, StringType(), True) for name in (
    "building_id", "timestamp_original", "electricity_value_original", *_LINEAGE_COLUMNS,
)])
_MAX = "99999999999999999999.999999999999999999"
_POPULATIONS = (
    [], ["1"], ["1", "2"], ["1", "2", "3"],
    ["0", "1", "2", "3"], ["0", "1", "2", "3", "4"],
    ["-10", "-1", "0", "5", "8", "30"], ["7"] * 8,
    ["0"] * 8 + ["1e-18"], ["-100"] + ["0"] * 8,
    ["0"] * 12 + ["100"] * 3,
    ["-1e-18", "0", "1e-18", "2e-18"],
    ["-" + _MAX, "-" + _MAX, _MAX, _MAX],
    ["99999999999999999999.00000000000000000" + str(i) for i in range(4)],
    ["-3", "0", "0", "0", "0", "0", "2", "2", "5"],
    ["0", "1", "1", "2", "2", "4", "4", "4"],
)


def _oracle(values):
    # stdlib inclusive quantiles implement the same linear rank convention as
    # pandas, using Fraction here to avoid copying the production Decimal math.
    q1, _, q3 = quantiles([Fraction(value) for value in values], n=4, method="inclusive")
    spread = q3 - q1
    return q1, q3, spread, q1 - Fraction(3, 2) * spread, q3 + Fraction(3, 2) * spread


def _frame(spark, records):
    rows = [(building, timestamp, value, "invented-uri", "invented-hash",
             "synthetic", "invented-reference") for building, timestamp, value in records]
    return _interpret(spark.createDataFrame(rows, _LONG_SCHEMA))


def _interpret(frame):
    return interpret_electricity_temporal(interpret_electricity_numeric(frame))


@pytest.mark.unit
@pytest.mark.parametrize("values", _POPULATIONS)
def test_decimal_interpolation_against_independent_inclusive_quantiles(values):
    ordered = sorted(map(Decimal, values))
    n = len(ordered)
    if n < 4:
        actual = _exact_statistics(n, None, None, None, None)
        assert actual == (None,) * 6 + ("insufficient_population",)
        return
    # This test supplies order statistics; the Spark integration below verifies
    # their distributed selection independently against complete populations.
    a, ar = divmod(n - 1, 4)
    b, br = divmod(3 * (n - 1), 4)
    actual = _exact_statistics(n, ordered[a], ordered[a + bool(ar)],
                               ordered[b], ordered[b + bool(br)])
    assert tuple(map(Fraction, actual[:5])) == _oracle(values)
    assert actual[5:] == (_oracle(values)[2] == 0, "calculated")


@pytest.mark.unit
def test_exact_arithmetic_is_independent_of_decimal_context_and_fails_explicitly():
    with localcontext() as context:
        context.prec = 6
        metrics = _exact_statistics(4, Decimal("-" + _MAX), Decimal("-" + _MAX),
                                    Decimal(_MAX), Decimal(_MAX))
    assert tuple(map(Fraction, metrics[:5])) == _oracle(["-" + _MAX, "-" + _MAX, _MAX, _MAX])
    assert abs(Fraction(metrics[3])) > Fraction(_MAX)
    with pytest.raises(ValueError, match="Missing"):
        _exact_statistics(4, None, Decimal(0), Decimal(0), Decimal(0))
    with pytest.raises(ValueError, match="Unordered"):
        _exact_statistics(4, Decimal(2), Decimal(2), Decimal(1), Decimal(1))


@pytest.mark.unit
def test_exact_approved_selection_and_nominal_grid_contract():
    assert APPROVED_BUILDINGS == (
        "Robin_lodging_Dorthy", "Robin_lodging_Pricilla", "Robin_lodging_Armand",
        "Fox_lodging_Wallace", "Fox_lodging_Winifred", "Fox_lodging_Warren",
        "Peacock_lodging_Chloe", "Peacock_lodging_Terrie", "Peacock_lodging_Nova",
        "Bull_lodging_Leonard", "Bull_lodging_Terence", "Bull_lodging_Lettie",
        "Bull_lodging_Carie", "Bull_lodging_Jeremiah", "Bull_lodging_Anibal",
        "Bull_lodging_Danielle",
    )
    assert (date(2018, 1, 1) - date(2016, 1, 1)).days * 24 == 17544
    assert Fraction(16666, 17544) < Fraction(95, 100) <= Fraction(16667, 17544)


@pytest.mark.unit
@pytest.mark.parametrize("value,lower,upper,expected", [
    ("-3", "-3", "5", False), ("5", "-3", "5", False),
    ("-3.000000000000000001", "-3", "5", True),
    ("5.000000000000000001", "-3", "5", True),
    ("0", "0", "0", False), ("1e-18", "0", "0", True),
    (None, "0", "0", None), ("1", None, None, None),
])
def test_outlier_comparison_uses_exact_strict_thresholds(value, lower, upper, expected):
    assert _outlier(Decimal(value) if value is not None else None, lower, upper) is expected


@pytest.mark.integration
@pytest.mark.parametrize("ansi,zone", [("true", "UTC"), ("false", "America/New_York")])
def test_spark_order_statistics_flags_and_missing_buildings(spark, ansi, zone):
    previous = {key: spark.conf.get(key) for key in
                ("spark.sql.ansi.enabled", "spark.sql.session.timeZone")}
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        spark.conf.set("spark.sql.session.timeZone", zone)
        records = [(building, "2016-02-29 00:00:00", value)
                   for building, values in zip(APPROVED_BUILDINGS, _POPULATIONS) for value in values]
        frame = _frame(spark, records).repartition(3)
        summary = summarize_electricity_iqr(frame)
        actual = {row.building_id: row.asDict() for row in summary.collect()}
        assert len(actual) == 16 and set(actual) == set(APPROVED_BUILDINGS)
        assert summary.schema["q1"].dataType == StringType()
        assert summary.schema["eligible_observation_count"].dataType == LongType()
        annotated = annotate_electricity_iqr(frame)
        assert annotated.schema["is_iqr_outlier"].dataType == BooleanType()
        assert annotated.schema["is_iqr_outlier"].nullable
        flags = annotated.collect()
        assert len(flags) == len(records)
        for building, values in zip(APPROVED_BUILDINGS, _POPULATIONS):
            row = actual[building]
            assert row["eligible_observation_count"] == row["valid_observation_count"] == len(values)
            assert row["covered_nominal_hours"] == bool(values)
            assert row["expected_nominal_hours"] == 17544
            assert row["minimum_coverage_count"] == 16667
            assert row["iqr_method"] == IQR_METHOD
            assert row["iqr_accepted"] is False
            assert row["valid_observation_coverage_accepted"] is False
            assert row["nominal_hour_coverage_accepted"] is False
            selected = [item for item in flags if item.building_id == building]
            if len(values) < 4:
                assert row["iqr_calculation_status"] == "insufficient_population"
                for field in ("q1", "q3", "iqr", "lower_limit", "upper_limit",
                              "iqr_zero_spread", "iqr_evaluable_count", "iqr_outlier_count",
                              "iqr_outlier_percentage"):
                    assert row[field] is None
                assert all(item.is_iqr_outlier is None
                           and item.iqr_evaluation_status == "insufficient_population"
                           for item in selected)
                continue
            expected = _oracle(values)
            assert tuple(Fraction(row[name]) for name in
                         ("q1", "q3", "iqr", "lower_limit", "upper_limit")) == expected
            assert row["iqr_zero_spread"] == (expected[2] == 0)
            assert row["iqr_calculation_status"] == "calculated"
            outliers = sum(Fraction(value) < expected[3] or Fraction(value) > expected[4]
                           for value in values)
            assert row["iqr_outlier_count"] == outliers
            assert row["iqr_evaluable_count"] == len(values)
            assert row["iqr_outlier_percentage"] == pytest.approx(
                100 * outliers / len(values), abs=1e-12,
            )
            for item in selected:
                value = Fraction(item.electricity_value_original)
                assert item.is_iqr_outlier == (value < expected[3] or value > expected[4])
                assert item.iqr_evaluation_status == "evaluated"
                assert item.iqr_accepted is False
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


@pytest.mark.integration
def test_population_nominal_calendar_multisets_lineage_and_no_writes(spark, monkeypatch):
    building = APPROVED_BUILDINGS[0]
    records = [
        (building, "2016-01-01 00:00:00", "-3"),
        (building, "2016-02-29 11:00:00", "0"),
        (building, "2017-12-31 23:00:00", "5"),
        (building, "2017-11-05 01:00:00", "0"),
        (building, "2017-11-05 01:00:00", "0"),
        (building, "2017-11-05 01:30:00", "0"),
        (building, "2017-11-05 01:30:00", "2"),
        (building, "2017-03-12 11:37:00", "2"),
    ]
    invalid = [("2016-01-01 01:00:00", text) for text in
               (None, "", "NaN", "bad", "Infinity", "-Infinity", "1e50", "1e-20", "1.1e-18")]
    invalid += [(text, "1") for text in
                (None, "", "bad", "2017-02-29 00:00:00", "2016-02-30 00:00:00",
                 "2016-01-01 24:00:00", "2015-12-31 23:00:00", "2018-01-01 00:00:00")]
    frame = _frame(spark, records + [(building, time, value) for time, value in invalid])
    # Two provenance groups belong to one complete building distribution.
    frame = (frame.withColumn("_bronze_source_name", F.lit("electricity"))
             .withColumn("_bronze_snapshot_run_id",
                         F.when(F.col("is_zero"), "run-a").otherwise("run-b"))
             .withColumn("_bronze_delta_version", F.lit(3))
             .withColumn("_bronze_table_path", F.lit("/invented-path"))
             .withColumn("extra.with.dot", F.lit("keep"))
             .withColumn("extra`tick", F.lit(None).cast("string")))
    before = Counter(map(tuple, frame.collect()))
    def forbidden(*args, **kwargs):
        pytest.fail("The IQR engine must not write or collect populations on the driver")
    with monkeypatch.context() as guard:
        guard.setattr(DataFrameWriter, "save", forbidden)
        guard.setattr(DataFrameWriter, "saveAsTable", forbidden)
        guard.setattr(DataFrame, "collect", forbidden)
        guard.setattr(DataFrame, "toPandas", forbidden)
        annotated = annotate_electricity_iqr(frame)
        summary = summarize_electricity_iqr(frame)
    assert annotated.schema.fields[:len(frame.schema)] == frame.schema.fields
    after = annotated.collect()
    assert len(after) == sum(before.values())
    assert Counter(tuple(row[:len(frame.columns)]) for row in after) == before
    assert Counter(map(tuple, frame.collect())) == before
    row = next(row for row in summary.collect() if row.building_id == building)
    assert row.eligible_observation_count == 8
    assert row.covered_nominal_hours == 4 and row.off_grid_observation_count == 3
    assert (Fraction(row.lower_limit), Fraction(row.upper_limit)) == (Fraction(-3), Fraction(5))
    assert row.iqr_outlier_count == 0  # Both strict boundaries occur in the input.
    for item in after:
        if item.timestamp_original in ("2015-12-31 23:00:00", "2018-01-01 00:00:00"):
            assert item.iqr_evaluation_status == "outside_study_period"
            assert item.is_iqr_outlier is None
        elif item.value_parse_status != "finite":
            assert item.iqr_evaluation_status == "numeric_not_eligible"
            assert item.is_iqr_outlier is None
        elif item.timestamp_parse_status != "valid_nominal":
            assert item.iqr_evaluation_status == "invalid_nominal_timestamp"
            assert item.is_iqr_outlier is None
        else:
            assert item.iqr_evaluation_status == "evaluated"
            assert item.is_iqr_outlier is False
    again = {r.building_id: r.asDict()
             for r in summarize_electricity_iqr(frame.repartition(4)).collect()}
    assert again[building] == row.asDict()


def _hourly(spark, building, size, distinct, off_grid=False):
    source = spark.range(size).withColumn("slot", F.col("id") % distinct)
    day = F.date_add(F.lit(date(2016, 1, 1)), F.floor(F.col("slot") / 24).cast("int"))
    label = (F.lit("2016-01-01 11:37:00") if off_grid else
             F.format_string("%s %02d:00:00", day.cast("string"), F.col("slot") % 24))
    return source.select(
        F.lit(building).alias("building_id"), label.alias("timestamp_original"),
        F.lit("0").alias("electricity_value_original"),
        *[F.lit("synthetic").alias(name) for name in _LINEAGE_COLUMNS],
    )


@pytest.mark.integration
def test_both_coverage_boundaries_duplicates_and_zero_spread_acceptance(spark):
    specifications = [(16666, 16666, False), (16667, 16667, False),
                      (16667, 16666, False), (17667, 16667, False), (16667, 1, True)]
    source = None
    for building, (size, distinct, off_grid) in zip(APPROVED_BUILDINGS, specifications):
        part = _hourly(spark, building, size, distinct, off_grid)
        source = part if source is None else source.unionByName(part)
    unusable = spark.createDataFrame([
        (APPROVED_BUILDINGS[1], time, value, *(["synthetic"] * 4))
        for time, value in [("2016-01-01 00:00:00", "NaN"), ("bad", "1")]
    ], _LONG_SCHEMA)
    source = source.unionByName(unusable)
    frame = _interpret(source)
    result = {row.building_id: row for row in summarize_electricity_iqr(frame).collect()}
    for index, (size, distinct, off_grid) in enumerate(specifications):
        row = result[APPROVED_BUILDINGS[index]]
        covered = 0 if off_grid else distinct
        assert row.eligible_observation_count == row.valid_observation_count == size
        assert row.covered_nominal_hours == covered
        assert row.off_grid_observation_count == (size if off_grid else 0)
        assert row.valid_observation_coverage_accepted == (size >= 16667)
        assert row.nominal_hour_coverage_accepted == (covered >= 16667)
        assert row.iqr_accepted == (size >= 16667 and covered >= 16667)
        assert row.valid_observation_coverage_percentage == pytest.approx(100 * size / 17544)
        assert row.nominal_hour_coverage_percentage == pytest.approx(100 * covered / 17544)
        assert row.iqr_zero_spread is True and row.iqr_calculation_status == "calculated"
        assert row.iqr_outlier_count == 0 and row.iqr_outlier_percentage == 0
    assert result[APPROVED_BUILDINGS[3]].valid_observation_coverage_percentage > 100
    # Verify accepted and rejected flags on actual Spark rows, without collecting
    # the large population or creating all 16 building-hour combinations.
    annotated = annotate_electricity_iqr(frame)
    flags = annotated.groupBy(
        "building_id", "iqr_accepted", "is_iqr_outlier", "iqr_evaluation_status",
    ).count().collect()
    assert len(flags) == 7
    for row in flags:
        assert row.iqr_accepted == result[row.building_id].iqr_accepted
    annotated_counts = Counter()
    for row in flags:
        annotated_counts[row.building_id] += row["count"]
    assert annotated_counts == {
        APPROVED_BUILDINGS[index]: size + (2 if index == 1 else 0)
        for index, (size, _, _) in enumerate(specifications)
    }
    assert all(row.is_iqr_outlier is False for row in flags if row.iqr_evaluation_status == "evaluated")
    not_evaluated = [row for row in flags if row.is_iqr_outlier is None]
    assert len(not_evaluated) == 2
    assert all(row.iqr_accepted is True for row in not_evaluated)


@pytest.mark.integration
def test_empty_input_and_all_unusable_buildings_remain_visible(spark):
    empty = _frame(spark, [])
    result = summarize_electricity_iqr(empty).collect()
    assert len(result) == 16 and set(row.building_id for row in result) == set(APPROVED_BUILDINGS)
    assert all(row.eligible_observation_count == row.covered_nominal_hours == 0
               and row.iqr_evaluable_count is None and row.iqr_outlier_count is None
               and row.iqr_outlier_percentage is None and row.iqr_accepted is False for row in result)
    assert annotate_electricity_iqr(empty).count() == 0
    unusable = _frame(spark, [(APPROVED_BUILDINGS[0], "bad", None)] * 4)
    bad = next(row for row in summarize_electricity_iqr(unusable).collect()
               if row.building_id == APPROVED_BUILDINGS[0])
    assert bad.eligible_observation_count == 0
    assert bad.iqr_calculation_status == "insufficient_population"
    assert annotate_electricity_iqr(unusable).count() == 4


@pytest.mark.integration
def test_rejects_schema_collisions_and_incoherent_interpretations(spark):
    frame = _frame(spark, [(APPROVED_BUILDINGS[0], "2016-01-01 00:00:00", "1")])
    schemas = [
        frame.drop("is_zero"), frame.withColumn("electricity_value_numeric", F.lit(1.0)),
        frame.withColumn("electricity_value_numeric", F.lit(1).cast(DecimalType(38, 17))),
        frame.withColumn("nominal_second_of_day", F.lit(0).cast("long")),
        frame.withColumn("nominal_date", F.lit("2016-01-01")),
        frame.withColumn("_bronze_delta_version", F.lit(0)),
        frame.select("*", F.col("building_id").alias("BUILDING_ID")),
        frame.withColumn("Q1", F.lit("occupied")), frame.withColumn("__IQR_n", F.lit(1)),
        frame.withColumn("iqr_accepted", F.lit(False)),
    ]
    for invalid in schemas:
        for operation in (summarize_electricity_iqr, annotate_electricity_iqr):
            with pytest.raises(ValueError):
                operation(invalid)
    changes = {
        "building_id": "unapproved", "value_parse_status": "unknown",
        "is_missing": True, "is_zero": None, "is_negative": True,
        "timestamp_parse_status": "invalid_format", "nominal_second_of_day": -1,
        "timestamp_original": "2016-01-01 01:00:00", "electricity_value_original": "2",
    }
    for name, value in changes.items():
        invalid = frame.withColumn(name, F.lit(value).cast(frame.schema[name].dataType))
        with pytest.raises(ValueError, match="inconsistent|Unexpected"):
            summarize_electricity_iqr(invalid)
    for name in ("building_id", "value_parse_status", "nominal_date"):
        invalid = frame.withColumn(name, F.lit(None).cast(frame.schema[name].dataType))
        with pytest.raises(ValueError, match="inconsistent|Unexpected"):
            annotate_electricity_iqr(invalid)


@pytest.mark.integration
def test_native_percentile_method_agrees_but_double_representation_loses_precision(spark):
    safe = spark.createDataFrame([(Decimal(value),) for value in ("0", "1", "2", "3", "4")],
                                 "value decimal(38,18)")
    native = safe.agg(F.percentile("value", [0.25, 0.75]).alias("q")).first()
    expected = _oracle(["0", "1", "2", "3", "4"])
    assert native.q == [float(expected[0]), float(expected[1])]
    large = _POPULATIONS[13]
    native_frame = spark.createDataFrame([(Decimal(value),) for value in large], "value decimal(38,18)")
    native = native_frame.agg(F.percentile("value", [0.25, 0.75]).alias("q"))
    assert native.schema["q"].dataType.elementType.typeName() == "double"
    q = native.first().q
    assert q[0] == q[1]  # Valid Decimal inputs have collapsed in Double arithmetic.
    row = next(row for row in summarize_electricity_iqr(
        _frame(spark, [(APPROVED_BUILDINGS[0], "2016-01-01 00:00:00", v) for v in large]),
    ).collect() if row.building_id == APPROVED_BUILDINGS[0])
    assert Fraction(row.q1) != Fraction(row.q3)
    metrics = (row.q1, row.q3, row.iqr, row.lower_limit, row.upper_limit)
    assert tuple(map(Fraction, metrics)) == _oracle(large)
