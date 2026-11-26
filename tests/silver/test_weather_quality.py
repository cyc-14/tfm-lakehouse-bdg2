"""Synthetic weather quality and coverage populations."""
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from tfm_lakehouse.silver import weather_enrichment as association
from tfm_lakehouse.silver import weather_quality as quality


@pytest.mark.unit
@pytest.mark.parametrize("token,state", [
    (None, "null"), ("", "empty"), ("NaN", "missing_token"), (" 9 ", "invalid_syntax"),
    ("nan", "invalid_syntax"), ("Infinity", "nonfinite"), ("-Infinity", "nonfinite"),
    ("1e20", "overflow"), ("1e-19", "underflow"), ("1.0000000000000000001", "scale_exceeded"),
    ("99999999999999999999.999999999999999999", "finite"),
    ("0.000000000000000001", "finite"), ("-0", "finite"), ("+1.25e1", "finite"),
])
def test_exact_numeric_quality(token, state):
    for name in association.VARIABLES:
        assert association._quality(token, name)[1] == state


@pytest.mark.unit
def test_quality_api_rejects_changed_context_and_weather(monkeypatch, tmp_path):
    evidence = {"contextual": {"id": "context"}, "silver": {"source_run_id": "run"},
                "manifest_sha256": "manifest", "weather_files_sha256": "weather"}
    monkeypatch.setattr(association, "associate_synthetic_weather", lambda *_: (evidence, None))
    monkeypatch.setattr(quality.contextual_reader, "read_synthetic_contextual", lambda *_: ({"id": "changed"}, None))
    with pytest.raises(quality.iqr.IQRPublicationError, match="Contextual publication changed"):
        quality.diagnose_synthetic_weather_quality(None, None, "s", "c", tmp_path)
    monkeypatch.setattr(quality.contextual_reader, "read_synthetic_contextual", lambda *_: (evidence["contextual"], None))
    monkeypatch.setattr(quality.silver, "_manifest", lambda *_: ({}, "manifest"))
    monkeypatch.setattr(quality.iqr, "_fingerprint", lambda *_: "changed")
    config = SimpleNamespace(table_path=lambda _: tmp_path)
    with pytest.raises(quality.iqr.IQRPublicationError, match="Weather source changed"):
        quality.diagnose_synthetic_weather_quality(None, config, "s", "c", tmp_path)
    checks = iter(("weather", "changed"))
    monkeypatch.setattr(quality.iqr, "_fingerprint", lambda *_: next(checks))
    monkeypatch.setattr(quality, "_read_verified_tables", lambda *_: {"weather": None})
    monkeypatch.setattr(quality, "_sensitivity", lambda *_: None)
    config.namespace = tmp_path
    with pytest.raises(quality.iqr.IQRPublicationError, match="Weather source changed"):
        quality.diagnose_synthetic_weather_quality(None, config, "s", "c", tmp_path)


@pytest.mark.integration
def test_monthly_coverage_and_sensitivity_are_reconciled(monkeypatch):
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[2]").appName("weather-quality-test")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        context = spark.createDataFrame([
            ("a", "S", "2016-01-01 12:00:00"),
            ("a", "S", "2016-01-01 12:30:00"),
            ("a", "S", "2016-01-01 12:30:00"),
            ("b", "S", "2016-01-01 12:30:00"),
            ("a", "S", "2016-01-01 13:00:00"),
            ("a", "S", "2016-01-01 14:20:00"),
            ("a", "S", "bad"),
            ("c", None, "2016-01-01 12:00:00"),
            ("a", "S", "2016-02-01 00:00:00"),
            ("d", "D", "2016-01-01 12:00:00"),
            ("e", "L", "2016-01-01 12:00:00"),
        ], "building_id string, metadata_site_id string, timestamp_original string")
        observations = spark.createDataFrame([
            ("S", "2016-01-01 12:00:00", "1", "-1", "x"),
            ("S", "2016-01-01 13:00:00", "2", "0", "x"),
            ("S", "2016-01-01 13:00:00", "2", "0", "y"),
            ("S", "2016-01-01 15:00:00", "3", "0", "x"),
            ("S", "2016-01-31 23:30:00", "4", "0", "x"),
            ("D", "2016-01-01 13:00:00", "5", "0", "x"),
            ("D", "2016-01-01 13:00:00", "5", "0", "x"),
            ("L", "2016-01-01 12:15:00", "6", "0", "x"),
        ], "site_id string, timestamp string, airTemperature string, precipDepth1HR string, lineage string")
        result = association._associate(context, observations)
        rows = quality._coverage(result).where("scope = 'site'").collect()
        for row in rows:
            assert row.electrical_rows == (row.eligible_rows + row.missing_site_rows +
                                            row.invalid_site_rows + row.invalid_timestamp_rows)
            assert row.eligible_rows == (row.associated_rows + row.unmatched_rows +
                                          row.duplicate_identical_rows + row.duplicate_conflicting_rows)
        jan = next(r for r in rows if r.metadata_site_id == "S" and r.period == "2016-01")
        assert (jan.electrical_rows, jan.eligible_rows, jan.associated_rows) == (6, 6, 4)
        assert (jan.exact_rows, jan.approximate_rows, jan.earlier_rows, jan.later_rows) == (1, 3, 3, 0)
        assert (jan.duplicate_conflicting_rows, jan.unmatched_rows) == (1, 1)
        assert (jan.eligible_electrical_keys, jan.associated_electrical_keys, jan.associated_weather_keys) == (4, 2, 1)
        assert jan.row_coverage == 4 / 6 and jan.label_coverage == 2 / 4
        assert jan.exact_electrical_keys + jan.approximate_electrical_keys + jan.rejected_electrical_keys + jan.unmatched_electrical_keys == jan.eligible_electrical_keys
        bad = next(r for r in rows if r.metadata_site_id == "S" and r.period == "unassigned")
        assert bad.invalid_timestamp_rows == 1 and bad.row_coverage is None
        missing = next(r for r in rows if r.metadata_site_id is None)
        assert missing.missing_site_rows == 1 and missing.row_coverage is None
        feb = next(r for r in rows if r.metadata_site_id == "S" and r.period == "2016-02")
        assert feb.earlier_rows == 1 and feb.offset_min_seconds == -1800
        later = next(r for r in rows if r.metadata_site_id == "L")
        assert (later.later_rows, later.offset_max_seconds, later.distance_mean_seconds) == (1, 900, 900.0)
        variables = quality._variables(result).where(
            "scope = 'site' AND metadata_site_id = 'S' AND period = '2016-01'").collect()
        rain = next(r for r in variables if r.variable == "precipDepth1HR")
        assert (rain.associated_rows, rain.finite_rows, rain.potential_special_code_rows) == (4, 4, 4)
        assert rain.available_rows == 4 and rain.numerically_interpretable_rows == 4
        assert rain.finite_without_special_rows == 0
        assert rain.available_weather_keys == 1
        clouds = next(r for r in variables if r.variable == "cloudCoverage")
        for row in variables:
            assert sum(row[state + "_rows"] for state in quality.QUALITY_STATES) == row.associated_rows
            assert row.associated_rows + row.not_associated_rows == row.electrical_rows
        assert clouds.column_missing_rows == 4 and clouds.not_associated_rows == 2
        zero_variable = (quality._variables(result)
                         .where("scope = 'site' AND metadata_site_id IS NULL AND variable = 'airTemperature'")
                         .first())
        assert zero_variable.associated_rows == 0 and zero_variable.available_ratio is None
        assert zero_variable.interpretable_ratio is None
        sensitivity = quality._sensitivity(context.repartition(2), observations.repartition(2))
        measured = (sensitivity.where("scope = 'site' AND metadata_site_id = 'S' AND period = '2016-01'")
                    .select("tolerance_seconds", "associated_rows", "rejected_duplicate_rows",
                            "associated_rows_gain", "rejected_duplicate_rows_change").collect())
        assert sorted(tuple(r) for r in measured) == [
            (0, 1, 1, None, None), (900, 1, 1, 0, 0),
            (1800, 4, 1, 3, 0), (3600, 5, 1, 1, 0)]
        duplicate_change = (sensitivity.where("scope = 'site' AND metadata_site_id = 'D'")
                            .select("tolerance_seconds", "rejected_duplicate_rows",
                                    "rejected_duplicate_rows_change").collect())
        assert sorted(tuple(r) for r in duplicate_change) == [(0, 0, None), (900, 0, 0),
                                                               (1800, 0, 0), (3600, 1, 1)]
        other = quality._coverage(association._associate(context.repartition(3), observations.repartition(3)))
        assert sorted(tuple(r) for r in other.where("scope = 'site' AND metadata_site_id = 'S'").collect()) == \
            sorted(tuple(r) for r in quality._coverage(result).where("scope = 'site' AND metadata_site_id = 'S'").collect())
        other_association = association._associate(context.repartition(3), observations.repartition(3))
        compare_variables = lambda df: sorted(tuple(r) for r in quality._variables(df).where(
            "scope = 'site' AND metadata_site_id = 'S' AND period = '2016-01'").collect())
        assert compare_variables(other_association) == compare_variables(result)
        other_sensitivity = quality._sensitivity(context.repartition(3), observations.repartition(3))
        measure = lambda df: {r.tolerance_seconds: tuple(r) for r in df.where(
            "scope = 'site' AND metadata_site_id = 'S' AND period = '2016-01'").collect()}
        assert measure(other_sensitivity) == measure(sensitivity)
        tokens = [None, "", "NaN", "bad", "Infinity", "1e20", "1e-19",
                  "1.0000000000000000001", "1", "-1", "9"]
        labels = [(datetime(2016, 3, 1) + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
                  for i in range(len(tokens))]
        columns = list(association.VARIABLES)
        matrix_context = spark.createDataFrame([("matrix", "M", label) for label in labels], context.schema)
        schema = "site_id string, timestamp string, " + ", ".join(f"{c} string" for c in columns)
        matrix_weather = spark.createDataFrame([tuple(["M", label] + [token] * len(columns))
                                                for label, token in zip(labels, tokens)], schema)
        matrix = quality._variables(association._associate(matrix_context, matrix_weather))
        matrix_rows = matrix.where("scope = 'site'").collect()
        assert {row.variable for row in matrix_rows} == set(columns)
        for row in matrix_rows:
            assert row.associated_rows == len(tokens)
            assert sum(row[state + "_rows"] for state in quality.QUALITY_STATES) == row.associated_rows
            assert row.finite_rows == 3
            assert all(row[state + "_rows"] == 1 for state in quality.QUALITY_STATES
                       if state not in ("finite", "column_missing"))
            assert row.potential_special_code_rows == int(row.variable in
                ("cloudCoverage", "precipDepth1HR", "precipDepth6HR"))
    finally:
        spark.stop()
