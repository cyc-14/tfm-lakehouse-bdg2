"""End-to-end synthetic verification of the H05.2 contract."""
from collections import Counter
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, StringType, StructField,
    StructType,
)

from tfm_lakehouse.silver_electricity import electricity_to_long
from tfm_lakehouse.silver_electricity_numeric import interpret_electricity_numeric
from tfm_lakehouse.silver_electricity_temporal import interpret_electricity_temporal


# Explicit expectations are independent of the implementation's parser.
VALUES = [
    ("1.25", "finite", Decimal("1.25")), (None, "missing_null", None),
    ("", "missing_token", None), ("NaN", "missing_token", None),
    ("-0e999999", "finite", Decimal(0)), ("-2.5000", "finite", Decimal("-2.5")),
    ("x", "invalid_syntax", None), ("Infinity", "nonfinite_literal", None),
    ("1e999999", "overflow", None), ("1e-999999", "underflow", None),
    ("1.1e-18", "scale_exceeded", None),
    ("99999999999999999999.999999999999999999", "finite", Decimal("99999999999999999999.999999999999999999")),
    ("-1e-18", "finite", Decimal("-1e-18")),
    ("99999999999999999999.9999999999999999991", "overflow", None),
]
DATES = [
    ("2017-03-12 02:30:00", date(2017, 3, 12), 9000, "valid_nominal"),
    ("2000-02-29 23:59:59", date(2000, 2, 29), 86399, "valid_nominal"),
    ("0001-01-01 00:00:00", date(1, 1, 1), 0, "valid_nominal"),
    (None, None, None, "missing_null"), ("", None, None, "missing_empty"),
    ("2017-01-01T00:00:00", None, None, "invalid_format"),
    ("1900-02-29 25:00:00", None, None, "invalid_calendar"),
    ("2017-01-01 24:00:00", None, None, "invalid_time"),
]
LINEAGE = ("_bronze_source_uri", "_bronze_source_sha256", "_bronze_data_kind", "_bronze_dataset_reference")
DERIVED = [
    ("electricity_value_numeric", DecimalType(38, 18), True),
    ("value_parse_status", StringType(), False), ("is_missing", BooleanType(), False),
    ("is_zero", BooleanType(), True), ("is_negative", BooleanType(), True),
    ("nominal_date", DateType(), True), ("nominal_second_of_day", IntegerType(), True),
    ("timestamp_parse_status", StringType(), False),
]


def compose(frame):
    return interpret_electricity_temporal(interpret_electricity_numeric(frame))


@pytest.mark.integration
def test_10000_row_h052_composition_preserves_multisets_and_schema(spark):
    """5,000 wide records cycle through 56 cases, including repeated rows."""
    schema = StructType([StructField(name, StringType(), True) for name in
                         ("timestamp", "site_a", "site_b", *LINEAGE)])
    wide_rows = [(DATES[i % len(DATES)][0], VALUES[i % len(VALUES)][0],
                  VALUES[(i + 5) % len(VALUES)][0], None, "hash", "synthetic", "ref")
                 for i in range(5000)]
    expected_long = Counter((building, row[0], row[position], *row[3:])
                            for row in wide_rows
                            for building, position in (("site_a", 1), ("site_b", 2)))
    number_expected = {value: (status, num) for value, status, num in VALUES}
    time_expected = {value: (day, second, status) for value, day, second, status in DATES}
    wide = spark.createDataFrame(wide_rows, schema)
    long = electricity_to_long(wide, ["site_a", "site_b"])
    assert len(long.schema) == 7
    long_rows = long.collect()
    assert len(long_rows) == 10000
    assert Counter(map(tuple, long_rows)) == expected_long
    baseline = compose(long)
    assert len(baseline.schema) == 15
    assert baseline.schema.fields[:7] == long.schema.fields
    assert [(f.name, f.dataType, f.nullable) for f in baseline.schema.fields[7:]] == DERIVED
    assert compose(spark.createDataFrame([], long.schema)).schema == baseline.schema

    # Simulate the reader's four publication columns; no Delta reader or write.
    published = (long.withColumn("_bronze_source_name", F.lit("electricity"))
                 .withColumn("_bronze_snapshot_run_id", F.lit("synthetic-run"))
                 .withColumn("_bronze_delta_version", F.lit(7))
                 .withColumn("_bronze_table_path", F.lit("synthetic-path")))
    assert len(published.schema) == 11
    numeric = interpret_electricity_numeric(published)
    numeric_rows = numeric.collect()
    assert len(numeric_rows) == 10000
    assert numeric.schema.fields[:11] == published.schema.fields
    assert Counter(tuple(row[:7]) for row in numeric_rows) == expected_long
    assert all(tuple(row[7:11]) == ("electricity", "synthetic-run", 7, "synthetic-path")
               for row in numeric_rows)
    result = interpret_electricity_temporal(numeric)
    assert len(result.schema) == 19
    assert result.schema.fields[:16] == numeric.schema.fields
    assert [(f.name, f.dataType, f.nullable) for f in result.schema.fields[11:]] == DERIVED
    assert compose(spark.createDataFrame([], published.schema)).schema == result.schema
    actual = result.collect()
    assert len(actual) == 10000
    assert Counter(tuple(row[:16]) for row in actual) == Counter(map(tuple, numeric_rows))
    assert Counter(tuple(row[:7]) for row in actual) == expected_long
    assert max(expected_long.values()) > 1
    for row in actual:
        assert tuple(row[7:11]) == ("electricity", "synthetic-run", 7, "synthetic-path")
        status, number = number_expected[row.electricity_value_original]
        assert (row.value_parse_status, row.electricity_value_numeric) == (status, number)
        assert row.is_missing == (status in ("missing_null", "missing_token"))
        assert (row.is_zero, row.is_negative) == ((number == 0, number < 0)
                                                 if number is not None else (None, None))
        assert (row.nominal_date, row.nominal_second_of_day, row.timestamp_parse_status) == time_expected[row.timestamp_original]
    # Reorder and repartition the same inputs; compare complete output multisets.
    reordered = spark.createDataFrame(list(reversed(wide_rows)), schema).repartition(3)
    again_long = electricity_to_long(reordered, ["site_a", "site_b"])
    again = compose(again_long).collect()
    assert Counter(tuple(row[:7]) for row in again) == expected_long
    assert Counter(tuple(row[:7]) + tuple(row[11:]) for row in actual) == Counter(map(tuple, again))
