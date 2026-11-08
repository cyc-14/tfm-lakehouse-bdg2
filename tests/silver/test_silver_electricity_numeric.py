"""Synthetic checks for exact electricity numeric interpretation."""
from collections import Counter
from decimal import Decimal
from fractions import Fraction
import random

import pytest
from pyspark.sql.types import (
    BooleanType, DecimalType, IntegerType, StringType, StructField, StructType,
)

from tfm_lakehouse.silver import silver_electricity_numeric as numeric
from tfm_lakehouse.silver.silver_electricity import electricity_to_long


CASES = [
    (None, "missing_null", None), ("", "missing_token", None),
    ("NaN", "missing_token", None), ("Infinity", "nonfinite_literal", None),
    ("+Infinity", "nonfinite_literal", None), ("-Infinity", "nonfinite_literal", None),
    *[(s, "invalid_syntax", None) for s in
      (" 1", "1 ", "1\n", "1,5", "1e", "1e+", "١", ".", "nan", "Inf")],
    *[(s, "finite", Decimal(s)) for s in
      ("1", "-12.34", ".5", "+.5", "1.", "01", "1e2", "-2.50E-2",
       "1e-18", "-1e-18", "99999999999999999999.999999999999999999",
       "-99999999999999999999.999999999999999999", "1.0000000000000000000",
       "-0", "+0.00")],
    ("99999999999999999999.9999999999999999991", "overflow", None),
    ("1e-19", "underflow", None), ("1.1e-18", "scale_exceeded", None),
    ("10000000000000000000.0000000000000000001", "scale_exceeded", None),
    ("1e" + "9" * 5000, "overflow", None),
    ("-1e-" + "9" * 5000, "underflow", None),
    ("-0e" + "9" * 5000, "finite", Decimal(0)),
]


def schema(*extra):
    return StructType([StructField(name, StringType(), True) for name in numeric._REQUIRED]
                      + list(extra))


@pytest.mark.unit
@pytest.mark.parametrize("value,status,expected", CASES)
def test_exact_numeric_classification(value, status, expected):
    result, actual_status, zero, negative = numeric._interpret(value)
    assert (actual_status, result) == (status, expected)
    assert (zero, negative) == ((expected == 0, expected < 0) if expected is not None
                                else (None, None))


@pytest.mark.unit
def test_deterministic_values_against_rational_reference():
    rng = random.Random(5201)
    maximum = Fraction(10 ** 38 - 1, 10 ** 18)
    quantum = Fraction(1, 10 ** 18)
    for _ in range(1000):
        coefficient = rng.randrange(-(10 ** 40), 10 ** 40)
        exponent = rng.randrange(-60, 30)
        text = f"{coefficient}e{exponent}"
        value = Fraction(coefficient) * (Fraction(10) ** exponent)
        magnitude = abs(value)
        if magnitude > maximum:
            status = "overflow"
        elif 0 < magnitude < quantum:
            status = "underflow"
        elif (value / quantum).denominator != 1:
            status = "scale_exceeded"
        else:
            status = "finite"
        result, actual, zero, negative = numeric._interpret(text)
        assert actual == status
        if status == "finite":
            assert Fraction(result) == value
            assert (zero, negative) == (value == 0, value < 0)
        else:
            assert (result, zero, negative) == (None, None, None)


@pytest.mark.unit
def test_schema_validation():
    numeric._validate_long_schema(schema(StructField("_bronze_delta_version", IntegerType())))
    for name in numeric._REQUIRED:
        fields = [f for f in schema() if f.name != name]
        with pytest.raises(ValueError, match="Missing"):
            numeric._validate_long_schema(StructType(fields))
        fields.append(StructField(name, IntegerType()))
        with pytest.raises(ValueError, match="StringType"):
            numeric._validate_long_schema(StructType(fields))
    for name in numeric._RESERVED:
        with pytest.raises(ValueError, match="collides"):
            numeric._validate_long_schema(schema(StructField(name.upper(), StringType())))
    with pytest.raises(ValueError, match="unambiguous"):
        numeric._validate_long_schema(schema(StructField("BUILDING_ID", StringType())))
    with pytest.raises(ValueError, match="integer"):
        numeric._validate_long_schema(schema(StructField("_bronze_delta_version", StringType())))


@pytest.mark.integration
@pytest.mark.parametrize("ansi,timezone", [("true", "UTC"), ("false", "America/New_York")])
def test_spark_preserves_rows_schema_and_exact_values(spark, ansi, timezone):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    original_timezone = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        spark.conf.set("spark.sql.session.timeZone", timezone)
        input_schema = schema(StructField("_bronze_delta_version", IntegerType(), False),
                              StructField("extra.metadata", StringType(), True))
        rows = [("site", None, value, None, "hash", "synthetic", "ref", 3, None)
                for value, _, _ in CASES]
        rows += [rows[0], rows[0]]
        frame = spark.createDataFrame(rows, input_schema)
        output = numeric.interpret_electricity_numeric(frame)
        assert output.schema.fields[:len(input_schema)] == input_schema.fields
        fields = output.schema.fields[len(input_schema):]
        assert [(f.name, f.dataType, f.nullable) for f in fields] == [
            ("electricity_value_numeric", DecimalType(38, 18), True),
            ("value_parse_status", StringType(), False),
            ("is_missing", BooleanType(), False),
            ("is_zero", BooleanType(), True), ("is_negative", BooleanType(), True),
        ]
        collected = output.collect()
        assert Counter(tuple(row)[:len(input_schema)] for row in collected) == Counter(rows)
        expected = {value: (status, number) for value, status, number in CASES}
        for row in collected:
            status, number = expected[row.electricity_value_original]
            assert (row.value_parse_status, row.electricity_value_numeric) == (status, number)
            assert row.is_missing == (status in ("missing_null", "missing_token"))
            assert (row.is_zero, row.is_negative) == (
                (number == 0, number < 0) if number is not None else (None, None))
        empty = numeric.interpret_electricity_numeric(spark.createDataFrame([], input_schema))
        assert empty.schema == output.schema
        assert empty.collect() == []
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
        spark.conf.set("spark.sql.session.timeZone", original_timezone)


@pytest.mark.integration
def test_h051_to_numeric_preserves_long_schema_and_multiplicity(spark):
    wide_schema = StructType([
        StructField(name, StringType(), True)
        for name in ("timestamp", "site_a", "site_b", *numeric._LINEAGE_COLUMNS)
    ])
    wide = spark.createDataFrame(
        [("label", "1.0", None, "uri", "hash", "synthetic", "ref")] * 2,
        wide_schema,
    )
    long = electricity_to_long(wide, ["site_a", "site_b"])
    result = numeric.interpret_electricity_numeric(long)
    assert result.schema.fields[:len(long.schema)] == long.schema.fields
    assert Counter(tuple(row)[:len(long.schema)] for row in result.collect()) == Counter(
        map(tuple, long.collect())
    )
    assert result.count() == 4
