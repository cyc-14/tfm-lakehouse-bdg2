"""Conservative interpretation of nominal electricity timestamp labels."""
from datetime import date
import re

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, StringType, StructField,
    StructType,
)

from tfm_lakehouse.silver_electricity import _literal_column
from tfm_lakehouse.silver_electricity_numeric import _validate_long_schema

_FORMAT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
_NUMERIC_TYPES = {
    "electricity_value_numeric": DecimalType(38, 18),
    "value_parse_status": StringType(),
    "is_missing": BooleanType(),
    "is_zero": BooleanType(),
    "is_negative": BooleanType(),
}
_RESULT = StructType([
    StructField("date", DateType(), True),
    StructField("second", IntegerType(), True),
    StructField("status", StringType(), False),
])


def _validate_input_schema(schema: StructType) -> None:
    names = schema.fieldNames()
    folded = [name.casefold() for name in names]
    if any(not name.strip() for name in names) or len(set(folded)) != len(names):
        raise ValueError("Input column names must be nonempty and unambiguous ignoring case")
    for name, data_type in _NUMERIC_TYPES.items():
        if name not in names:
            raise ValueError(f"Missing mandatory numeric field {name!r}")
        if schema[name].dataType != data_type:
            raise ValueError(f"Incompatible type for numeric field {name!r}")
        if name in ("value_parse_status", "is_missing") and schema[name].nullable:
            raise ValueError(f"Numeric field {name!r} must be non-nullable")
    # The numeric validator checks the H05.1 and publication fields and rejects
    # all remaining casefold collisions with H05.2 reserved names.
    _validate_long_schema(StructType([f for f in schema if f.name not in _NUMERIC_TYPES]))


def _interpret(value: str | None) -> tuple[date | None, int | None, str]:
    """Interpret a calendar label without assigning a physical instant."""
    if value is None:
        return None, None, "missing_null"
    if value == "":
        return None, None, "missing_empty"
    if _FORMAT.fullmatch(value) is None:
        return None, None, "invalid_format"
    year, month, day = int(value[:4]), int(value[5:7]), int(value[8:10])
    try:
        nominal_date = date(year, month, day)
    except ValueError:
        return None, None, "invalid_calendar"
    hour, minute, second = int(value[11:13]), int(value[14:16]), int(value[17:19])
    if hour > 23 or minute > 59 or second > 59:
        return None, None, "invalid_time"
    return nominal_date, 3600 * hour + 60 * minute + second, "valid_nominal"


_interpret_udf = F.udf(_interpret, _RESULT)


def interpret_electricity_temporal(frame: DataFrame) -> DataFrame:
    """Append nominal calendar fields to an enriched H05.2.1 DataFrame.

    This lazy projection preserves all input occurrences and performs no writes.
    The labels are not interpreted as instants or assigned a time zone.
    """
    _validate_input_schema(frame.schema)
    result = _interpret_udf(_literal_column("timestamp_original"))
    return frame.select(
        *[_literal_column(name) for name in frame.columns],
        result.getField("date").alias("nominal_date"),
        result.getField("second").alias("nominal_second_of_day"),
        F.coalesce(result.getField("status"), F.lit("invalid_format")).alias(
            "timestamp_parse_status"
        ),
    )
