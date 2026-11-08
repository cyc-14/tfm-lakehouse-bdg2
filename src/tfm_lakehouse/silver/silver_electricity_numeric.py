"""Exact, non-persisting interpretation of long-format electricity values."""
import re
from decimal import Decimal

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType, ByteType, DecimalType, IntegerType, LongType, ShortType,
    StringType, StructField, StructType,
)

from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS, _literal_column

_GRAMMAR = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", re.ASCII)
_REQUIRED = ("building_id", "timestamp_original", "electricity_value_original", *_LINEAGE_COLUMNS)
_RESERVED = (
    "electricity_value_numeric", "value_parse_status", "is_missing", "is_zero",
    "is_negative", "nominal_date", "nominal_second_of_day", "timestamp_parse_status",
)
_RESULT = StructType([
    StructField("value", DecimalType(38, 18), True),
    StructField("status", StringType(), False),
    StructField("zero", BooleanType(), True),
    StructField("negative", BooleanType(), True),
])


def _validate_long_schema(schema: StructType) -> None:
    names = schema.fieldNames()
    folded = [name.casefold() for name in names]
    if any(not name.strip() for name in names) or len(set(folded)) != len(names):
        raise ValueError("Input column names must be nonempty and unambiguous ignoring case")
    if set(folded) & {name.casefold() for name in _RESERVED}:
        raise ValueError("Input column collides with a reserved H05.2 field")
    if not set(_REQUIRED).issubset(names):
        raise ValueError("Missing mandatory H05.1 fields")
    for name in _REQUIRED:
        if not isinstance(schema[name].dataType, StringType):
            raise ValueError(f"Mandatory field {name!r} must have StringType")
    for name in ("_bronze_source_name", "_bronze_snapshot_run_id", "_bronze_table_path"):
        if name in names and not isinstance(schema[name].dataType, StringType):
            raise ValueError(f"Publication field {name!r} must have StringType")
    if "_bronze_delta_version" in names and not isinstance(
        schema["_bronze_delta_version"].dataType, (ByteType, ShortType, IntegerType, LongType)
    ):
        raise ValueError("Publication Delta version must have an integer type")


def _interpret(value: str | None) -> tuple[Decimal | None, str, bool | None, bool | None]:
    """Classify using digit lengths; never expand an exponent or round a value."""
    def failure(status: str) -> tuple[None, str, None, None]:
        return None, status, None, None

    if value is None:
        return failure("missing_null")
    if value in ("", "NaN"):
        return failure("missing_token")
    if value in ("Infinity", "+Infinity", "-Infinity"):
        return failure("nonfinite_literal")
    if _GRAMMAR.fullmatch(value) is None:
        return failure("invalid_syntax")
    negative = value.startswith("-")
    unsigned = value.lstrip("+-")
    parts = re.split("[eE]", unsigned, maxsplit=1)
    mantissa = parts[0]
    integer, _, fraction = mantissa.partition(".")
    digits = (integer + fraction).lstrip("0")
    if not digits:
        return Decimal(0), "finite", True, False

    # Saturate exponent parsing at a bound larger than any possible cancellation
    # by the input's own significand length. This avoids Python's integer digit
    # limit and retains the exact comparison outcome for arbitrarily long inputs.
    exponent = 0
    if len(parts) == 2:
        raw_exponent = parts[1]
        exp_negative = raw_exponent.startswith("-")
        exp_digits = raw_exponent.lstrip("+-").lstrip("0")
        bound = len(value) + 100
        exponent = (bound if len(exp_digits) > len(str(bound))
                    else min(int(exp_digits or "0"), bound))
        if exp_negative:
            exponent = -exponent
    exponent -= len(fraction)
    significant = digits.rstrip("0")
    exponent += len(digits) - len(significant)
    order = len(significant) + exponent
    if order > 20 or (order == 20 and len(significant) > 38
                      and significant[:38] == "9" * 38):
        return failure("overflow")
    if order < -17:
        return failure("underflow")
    if exponent < -18:
        return failure("scale_exceeded")
    # The preceding bounds ensure at most 38 significant decimal digits.
    number = Decimal((int(negative), tuple(int(digit) for digit in significant), exponent))
    return number, "finite", False, negative


_interpret_udf = F.udf(_interpret, _RESULT)


def interpret_electricity_numeric(frame: DataFrame) -> DataFrame:
    """Append the five numeric fields while preserving every input occurrence.

    This lazy transformation neither authenticates lineage nor writes data.
    All eight eventual H05.2 output names are reserved to prevent collisions.
    """
    _validate_long_schema(frame.schema)
    result = _interpret_udf(_literal_column("electricity_value_original"))
    status = result.getField("status")
    # Coalesce expresses the guaranteed non-null status in Spark's schema.
    status = F.coalesce(status, F.lit("invalid_syntax"))
    return frame.select(
        *[_literal_column(name) for name in frame.columns],
        result.getField("value").alias("electricity_value_numeric"),
        status.alias("value_parse_status"),
        status.isin("missing_null", "missing_token").alias("is_missing"),
        result.getField("zero").alias("is_zero"),
        result.getField("negative").alias("is_negative"),
    )
