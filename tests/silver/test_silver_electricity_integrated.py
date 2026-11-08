"""Synthetic checks for the selected-building H05.3 orchestration."""
from collections import Counter
from decimal import Decimal

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS
from tfm_lakehouse.silver.silver_electricity_integrated import (
    _publication_identity, transform_selected_electricity,
)


@pytest.mark.unit
@pytest.mark.parametrize("selection", [None, {}, {"run_id": "run", "tables": []},
    {"run_id": "run", "tables": [{"name": "electricity", "delta_version": -1, "table_path": "p"}]},
    {"run_id": "run", "tables": [{"name": "electricity", "delta_version": True, "table_path": "p"}]},
    {"run_id": "run", "tables": [{"name": "electricity", "delta_version": 0, "table_path": ""}]},
    {"run_id": "run", "tables": [{"name": "electricity"}, {"name": "electricity"}]},
])
def test_invalid_publication_identity_is_rejected(selection):
    with pytest.raises(ValueError):
        _publication_identity(selection)


@pytest.mark.unit
def test_exact_publication_identity_is_preserved():
    selection = {"run_id": "r1", "tables": [{"name": "metadata"},
                 {"name": "electricity", "delta_version": 7, "table_path": "/synthetic"}]}
    assert _publication_identity(selection) == ("r1", 7, "/synthetic")


def schema(*names):
    return StructType([StructField(name, StringType(), True) for name in names])


@pytest.mark.integration
def test_selected_integration_preserves_occurrences_lineage_and_quality(spark):
    values = (None, "", "NaN", "0", "-2.5", "1.25", "bad", "1e50", "1e-20",
              "1.1e-18", "-0", "Infinity", "4", "-1e-18", ".5", "99999999999999999999.999999999999999999")
    rows = [
        ("2017-11-05 01:30:00", *values, "uri", "hash", "synthetic", "ref", "unselected"),
        ("2017-11-05 01:30:00", *values, "uri", "hash", "synthetic", "ref", "unselected"),
        ("invalid", *values, None, None, None, None, "unselected"),
    ]
    fields = ("timestamp", *APPROVED_BUILDINGS, *_LINEAGE_COLUMNS, "other_building")
    bronze = spark.createDataFrame(rows, schema(*fields))
    context = {"run_id": "run-synthetic", "tables": [
        {"name": "electricity", "delta_version": 7, "table_path": "/synthetic/electricity"}]}
    result = transform_selected_electricity(bronze, selection=context)
    actual = result.collect()
    assert len(actual) == len(rows) * 16
    assert Counter((r.building_id, r.timestamp_original, r.electricity_value_original,
                    *(r[name] for name in _LINEAGE_COLUMNS)) for r in actual) == Counter(
        (name, row[0], row[i + 1], *row[17:21])
        for row in rows for i, name in enumerate(APPROVED_BUILDINGS))
    assert Counter(map(tuple, bronze.collect())) == Counter(rows)
    assert all((r._bronze_source_name, r._bronze_snapshot_run_id, r._bronze_delta_version,
                r._bronze_table_path) == ("electricity", "run-synthetic", 7,
                                           "/synthetic/electricity") for r in actual)
    statuses = Counter(r.value_parse_status for r in actual)
    assert statuses == {"missing_null": 3, "missing_token": 6, "finite": 24,
                        "invalid_syntax": 3, "overflow": 3, "underflow": 3,
                        "scale_exceeded": 3, "nonfinite_literal": 3}
    assert Counter(r.timestamp_parse_status for r in actual) == {"valid_nominal": 32, "invalid_format": 16}
    zero = next(r for r in actual if r.electricity_value_original == "0")
    negative = next(r for r in actual if r.electricity_value_original == "-2.5")
    assert (zero.electricity_value_numeric, zero.is_zero, zero.is_negative) == (Decimal(0), True, False)
    assert (negative.electricity_value_numeric, negative.is_zero, negative.is_negative) == (Decimal("-2.5"), False, True)
    without_context = transform_selected_electricity(bronze).collect()
    assert len(without_context) == len(actual)
    assert "_bronze_snapshot_run_id" not in without_context[0].asDict()
    again = transform_selected_electricity(bronze.repartition(2), selection=context).collect()
    assert Counter(map(tuple, again)) == Counter(map(tuple, actual))


@pytest.mark.integration
def test_selected_input_schema_fails_closed(spark):
    fields = ("timestamp", *APPROVED_BUILDINGS, *_LINEAGE_COLUMNS)
    for names in (fields[:-1], tuple(n for n in fields if n != APPROVED_BUILDINGS[0]),
                  (*fields, "_bronze_unknown"), (*fields, APPROVED_BUILDINGS[0].upper())):
        with pytest.raises(ValueError):
            transform_selected_electricity(spark.createDataFrame([], schema(*names)))
