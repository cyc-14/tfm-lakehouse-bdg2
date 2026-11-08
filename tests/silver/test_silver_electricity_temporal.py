"""Synthetic tests for nominal electricity timestamp interpretation."""
from collections import Counter
from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, StringType, StructField, StructType,
)

from tfm_lakehouse.silver.silver_electricity import electricity_to_long
from tfm_lakehouse.silver import silver_electricity_numeric as numeric
from tfm_lakehouse.silver import silver_electricity_temporal as temporal


CASES = [
    (None, None, None, "missing_null"), ("", None, None, "missing_empty"),
    ("0001-01-01 00:00:00", date(1, 1, 1), 0, "valid_nominal"),
    ("9999-12-31 23:59:59", date(9999, 12, 31), 86399, "valid_nominal"),
    ("2000-02-29 12:59:59", date(2000, 2, 29), 46799, "valid_nominal"),
    ("2024-02-29 00:01:00", date(2024, 2, 29), 60, "valid_nominal"),
    ("2017-03-12 02:30:00", date(2017, 3, 12), 9000, "valid_nominal"),
    ("2017-11-05 01:30:00", date(2017, 11, 5), 5400, "valid_nominal"),
    *[(s, None, None, "invalid_calendar") for s in (
        "1900-02-29 00:00:00", "2023-02-29 00:00:00", "2024-02-30 00:00:00",
        "2024-04-31 00:00:00", "2024-00-01 00:00:00", "2024-13-01 00:00:00",
        "2024-01-00 00:00:00", "0000-01-01 00:00:00", "2024-02-30 24:60:60",
    )],
    *[(s, None, None, "invalid_time") for s in (
        "2024-01-01 24:00:00", "2024-01-01 00:60:00", "2024-01-01 00:00:60",
    )],
    *[(s, None, None, "invalid_format") for s in (
        "2024-01-01 00:00", "2024-01-01T00:00:00", "2024-01-01 00:00:00Z",
        "2024-01-01 00:00:00+00:00", " 2024-01-01 00:00:00",
        "2024-01-01 00:00:00 ", "2024-01-01 00:00:00\n", "２０２４-01-01 00:00:00",
        "2024/01/01 00:00:00", "2024-1-01 00:00:00", "2024-01-01 -1:00:00",
        "2024-01-01 00:0:00", "2024-01-01 00:00:0",
    )],
]


def enriched_schema():
    return StructType(
        [StructField(name, StringType(), True) for name in numeric._REQUIRED]
        + [StructField(name, typ, name not in ("value_parse_status", "is_missing"))
           for name, typ in temporal._NUMERIC_TYPES.items()]
    )


@pytest.mark.unit
@pytest.mark.parametrize("text,expected_date,second,status", CASES)
def test_nominal_interpretation(text, expected_date, second, status):
    assert temporal._interpret(text) == (expected_date, second, status)


@pytest.mark.unit
def test_input_schema_validation():
    base = enriched_schema()
    temporal._validate_input_schema(base)
    for field in base:
        with pytest.raises(ValueError, match="Missing"):
            temporal._validate_input_schema(StructType([f for f in base if f.name != field.name]))
        wrong = IntegerType() if not isinstance(field.dataType, IntegerType) else StringType()
        with pytest.raises(ValueError):
            temporal._validate_input_schema(StructType([
                StructField(f.name, wrong if f.name == field.name else f.dataType, f.nullable)
                for f in base
            ]))
    for name in ("nominal_date", "nominal_second_of_day", "timestamp_parse_status"):
        with pytest.raises(ValueError, match="collides"):
            temporal._validate_input_schema(StructType(base.fields + [StructField(name.upper(), StringType())]))
    for name in ("building_id", "is_missing"):
        with pytest.raises(ValueError, match="unambiguous"):
            temporal._validate_input_schema(StructType(base.fields + [StructField(name.upper(), StringType())]))
        with pytest.raises(ValueError, match="unambiguous"):
            temporal._validate_input_schema(StructType(base.fields + [StructField(name, StringType())]))
    with pytest.raises(ValueError, match="integer"):
        temporal._validate_input_schema(StructType(base.fields + [StructField("_bronze_delta_version", StringType())]))
    for name in ("value_parse_status", "is_missing"):
        with pytest.raises(ValueError, match="non-nullable"):
            temporal._validate_input_schema(StructType([
                StructField(f.name, f.dataType, True if f.name == name else f.nullable)
                for f in base
            ]))


@pytest.mark.integration
@pytest.mark.parametrize("ansi,zone,policy", [
    ("true", "UTC", "CORRECTED"), ("false", "America/New_York", "LEGACY"),
    ("true", "America/New_York", "EXCEPTION"), ("false", "UTC", "CORRECTED"),
])
def test_composition_preserves_rows_and_is_configuration_independent(spark, ansi, zone, policy):
    keys = ("spark.sql.ansi.enabled", "spark.sql.session.timeZone", "spark.sql.legacy.timeParserPolicy")
    previous = [spark.conf.get(key) for key in keys]
    try:
        for key, value in zip(keys, (ansi, zone, policy)):
            spark.conf.set(key, value)
        schema = StructType([StructField(n, StringType(), True) for n in
                             ("timestamp", "site_a", "site_b", *numeric._LINEAGE_COLUMNS)])
        rows = [(s, "1.5", None, "uri", "hash", "synthetic", "ref") for s, *_ in CASES]
        rows += [rows[0], rows[7], rows[7]]
        wide = spark.createDataFrame(rows, schema)
        long = electricity_to_long(wide, ["site_a", "site_b"])
        enriched = numeric.interpret_electricity_numeric(long).withColumn(
            "_bronze_delta_version", F.lit(3)).withColumn("extra.metadata", F.lit(None).cast("string"))
        result = temporal.interpret_electricity_temporal(enriched)
        assert result.schema.fields[:len(enriched.schema)] == enriched.schema.fields
        assert [(f.name, f.dataType, f.nullable) for f in result.schema.fields[len(enriched.schema):]] == [
            ("nominal_date", DateType(), True), ("nominal_second_of_day", IntegerType(), True),
            ("timestamp_parse_status", StringType(), False),
        ]
        actual = result.collect()
        assert len(actual) == 2 * len(rows)
        assert Counter(tuple(row)[:len(enriched.schema)] for row in actual) == Counter(map(tuple, enriched.collect()))
        expected = {s: (d, seconds, status) for s, d, seconds, status in CASES}
        for row in actual:
            assert (row.nominal_date, row.nominal_second_of_day, row.timestamp_parse_status) == expected[row.timestamp_original]
        empty = temporal.interpret_electricity_temporal(spark.createDataFrame([], enriched.schema))
        assert empty.schema == result.schema
        assert empty.collect() == []
    finally:
        for key, value in zip(keys, previous):
            spark.conf.set(key, value)
