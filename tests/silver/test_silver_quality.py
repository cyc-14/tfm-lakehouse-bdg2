"""Synthetic checks for conservative Silver quality summaries."""
from collections import Counter

import pytest
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import StringType, StructField, StructType

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS
from tfm_lakehouse.silver.silver_electricity_integrated import transform_selected_electricity
from tfm_lakehouse.silver.silver_quality import summarize_electricity_quality


def selected_frame(spark):
    columns = ("timestamp", *APPROVED_BUILDINGS, *_LINEAGE_COLUMNS)
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    # The first two rows are identical; the third shares their valid label only.
    examples = [
        ("2017-01-01 00:00:00", None), ("2017-01-01 00:00:00", None),
        ("2017-01-01 00:00:00", "0"), ("bad", "-2"), ("bad", "broken"),
        (None, "Infinity"), ("2017-02-30 00:00:00", "1e50"),
        ("2017-01-01 25:00:00", "1e-20"), ("", "1.1e-18"),
        ("2017-01-02 00:00:00", "NaN"),
    ]
    rows = [(time, *([value] * 16), "uri", "hash", "synthetic", "fixture")
            for time, value in examples]
    bronze = spark.createDataFrame(rows, schema)
    context = {"run_id": "synthetic-run", "tables": [
        {"name": "electricity", "delta_version": 3, "table_path": "/synthetic"}]}
    return transform_selected_electricity(bronze, selection=context)


@pytest.mark.integration
def test_quality_counts_are_conservative_stable_and_never_write(spark, tmp_path, monkeypatch):
    frame = selected_frame(spark).cache()
    before_schema = frame.schema
    before = Counter(map(tuple, frame.collect()))
    assert sum(before.values()) == 160 and max(before.values()) == 2
    def forbidden(*args, **kwargs):
        pytest.fail("Quality diagnostics must not write Delta or other tables")
    monkeypatch.setattr(DataFrameWriter, "save", forbidden)
    monkeypatch.setattr(DataFrameWriter, "saveAsTable", forbidden)
    summary = summarize_electricity_quality(frame)
    first = {row.building_id: row.asDict() for row in summary.collect()}
    again = {row.building_id: row.asDict()
             for row in summarize_electricity_quality(frame.repartition(3)).collect()}
    assert set(first) == set(APPROVED_BUILDINGS)
    assert first == again
    for row in first.values():
        assert row["total_occurrences"] == 10
        assert [row["numeric_" + name] for name in (
            "missing_null", "missing_token", "finite", "invalid_syntax", "nonfinite_literal",
            "overflow", "underflow", "scale_exceeded")] == [2, 1, 2, 1, 1, 1, 1, 1]
        assert (row["missing_occurrences"], row["zero_occurrences"],
                row["negative_occurrences"]) == (3, 1, 1)
        assert [row["timestamp_" + name] for name in (
            "missing_null", "missing_empty", "invalid_format", "invalid_calendar",
            "invalid_time", "valid_nominal")] == [1, 1, 2, 1, 1, 4]
        assert (row["unclassifiable_timestamps"], row["repeated_nominal_keys"],
                row["occurrences_in_repeated_nominal_keys"]) == (6, 1, 3)
        assert (row["_bronze_snapshot_run_id"], row["_bronze_delta_version"],
                row["_bronze_source_uri"]) == ("synthetic-run", 3, "uri")
    assert frame.schema == before_schema
    assert Counter(map(tuple, frame.collect())) == before
    assert list(tmp_path.iterdir()) == []
    frame.unpersist()


@pytest.mark.integration
def test_rejects_unknown_building_bad_status_and_inconsistent_flags(spark):
    frame = selected_frame(spark)
    variants = [
        frame.withColumn("building_id", F.lit("not-approved")),
        frame.withColumn("building_id", F.lit(None).cast("string")),
        frame.withColumn("value_parse_status", F.lit("unknown")),
        frame.withColumn("timestamp_parse_status", F.lit(None).cast("string")),
        frame.withColumn("is_missing", F.lit(False)),
    ]
    for invalid in variants:
        with pytest.raises(ValueError, match="Unexpected"):
            summarize_electricity_quality(invalid)
    with pytest.raises(ValueError, match="context"):
        summarize_electricity_quality(frame.drop("_bronze_delta_version"))
    with pytest.raises(ValueError, match="field"):
        summarize_electricity_quality(frame.drop("is_zero"))


@pytest.mark.integration
def test_empty_input_and_publication_context_partition(spark):
    frame = selected_frame(spark).cache()
    assert summarize_electricity_quality(frame.limit(0)).count() == 0
    second = frame.withColumn("_bronze_snapshot_run_id", F.lit("other-run"))
    rows = summarize_electricity_quality(frame.unionByName(second)).collect()
    assert len(rows) == 32
    assert all(row.total_occurrences == 10 and row.repeated_nominal_keys == 1
               and row.occurrences_in_repeated_nominal_keys == 3 for row in rows)
    frame.unpersist()
