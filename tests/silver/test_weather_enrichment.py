"""Nominal weather association with entirely invented input rows."""
from collections import Counter
import sys

import pytest
from pyspark.sql import SparkSession

from tfm_lakehouse.silver import weather_enrichment as weather


@pytest.mark.unit
def test_nominal_and_quality_interpretation():
    assert weather._nominal_seconds("2016-02-29 00:00:00") - weather._nominal_seconds("2016-02-28 23:59:59") == 1
    assert weather._nominal_seconds("2017-02-29 00:00:00") is None
    assert weather._nominal_seconds("2016-03-13 02:30:00") is not None
    assert weather._quality("NaN", "airTemperature")[1] == "missing_token"
    assert weather._quality("", "airTemperature")[1] == "empty"
    assert weather._quality("-1e0", "precipDepth1HR")[2]
    assert weather._quality("9.0", "cloudCoverage")[2]
    assert weather._quality(" 9 ", "cloudCoverage") == (None, "invalid_syntax", False)
    assert weather._quality("1e-19", "airTemperature")[1] == "underflow"
    assert weather._quality(None, "airTemperature")[1] == "null"
    assert weather._quality("Infinity", "airTemperature")[1] == "nonfinite"
    assert weather._quality("1e20", "airTemperature")[1] == "overflow"
    assert weather._quality("1.0000000000000000001", "airTemperature")[1] == "scale_exceeded"
    assert all(weather._quality("0", name)[1] == "finite" for name in weather.VARIABLES)


@pytest.mark.unit
def test_api_rejects_wrong_identity_and_missing_weather(monkeypatch, tmp_path):
    identity = {"source_run_id": "run", "manifest_sha256": "digest"}
    monkeypatch.setattr(weather.iqr, "_workspace", lambda *_: tmp_path)
    monkeypatch.setattr(weather.iqr, "_verify_silver", lambda *_: (identity, None))
    monkeypatch.setattr(weather.contextual_reader, "read_synthetic_contextual",
                        lambda *_: ({"intent": {"silver": {"different": True}}}, None))
    with pytest.raises(weather.iqr.IQRPublicationError, match="identity differs"):
        weather.associate_synthetic_weather(None, None, "silver", "context", tmp_path)
    monkeypatch.setattr(weather.contextual_reader, "read_synthetic_contextual",
                        lambda *_: ({"intent": {"silver": identity}}, None))
    monkeypatch.setattr(weather.silver, "_manifest", lambda *_: ({"tables": []}, "digest"))
    with pytest.raises(weather.iqr.IQRPublicationError, match="Weather source is unavailable"):
        weather.associate_synthetic_weather(None, None, "silver", "context", tmp_path)


@pytest.mark.integration
def test_distributed_matching_and_multiplicity(monkeypatch):
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[2]").appName("weather-association-test")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        context = spark.createDataFrame([
            ("a", "S", "2016-01-01 12:00:00"),
            ("a", "S", "2016-01-01 12:29:00"),
            ("a", "S", "2016-01-01 12:30:00"),
            ("a", "S", "2016-01-01 12:31:00"),
            ("a", "S", "2016-01-01 12:31:00"),
            ("b", "S", "2016-01-01 12:30:00"),
            ("a", "S", "2016-01-01 14:30:00"),
            ("a", "S", "2016-01-01 15:30:01"),
            ("a", "T", "2016-01-01 12:00:00"),
            ("a", None, "2016-01-01 12:00:00"),
            ("a", "S", "bad"),
            ("a", "S", "2016-03-13 02:30:00"),
            ("a", "S", "2016-11-06 01:30:00"),
            ("a", "S", "2016-02-29 00:00:00"),
            ("a", "S", "2017-01-01 00:00:00"),
            ("a", "S", "2016-01-01 16:01:00"),
            ("a", "U", "2016-01-01 12:30:01"),
            ("a", "U", "2016-01-01 11:30:00"),
            ("a", "X", "2016-01-01 12:00:00"),
            ("a", "S", "2016-05-01 00:00:00"),
            ("a", "Y", "2016-01-01 12:17:30"),
        ], "building_id string, metadata_site_id string, timestamp_original string")
        observations = spark.createDataFrame([
            ("S", "2016-01-01 12:00:00", "1", "x"),
            ("S", "2016-01-01 13:00:00", "2", "x"),
            ("S", "2016-01-01 14:00:00", "3", "x"),
            ("S", "2016-01-01 14:00:00", "3", "x"),
            ("S", "2016-01-01 15:00:00", "4", "x"),
            ("S", "2016-01-01 16:00:00", "5", "x"),
            ("S", "2016-01-01 16:00:00", "6", "x"),
            ("T", "2016-01-01 12:00:00", "7", "x"),
            ("S", "2016-03-13 02:00:00", "8", "x"),
            ("S", "2016-11-06 01:00:00", "9", "x"),
            ("S", "2016-02-28 23:30:00", "10", "x"),
            ("S", "2016-12-31 23:30:00", "11", "x"),
            ("U", "2016-01-01 12:00:00", "12", "x"),
            ("X", "2016-01-01 12:00:00", "13", "x"),
            ("X", "2016-01-01 12:01:00", "14", "x"),
            ("S", "2016-04-30 23:30:00", "15", "x"),
            ("Y", "2016-01-01 12:17:30", "16", "x"),
        ], "site_id string, timestamp string, airTemperature string, lineage string")
        with pytest.raises(weather.iqr.IQRPublicationError, match="keys are missing"):
            weather._associate(context, observations.drop("timestamp"))
        with pytest.raises(weather.iqr.IQRPublicationError, match="collision"):
            weather._associate(context.withColumnRenamed("building_id", "weather_match_type"), observations)
        results = []
        complete_results = []
        for zone, partitions in [("UTC", 1), ("America/New_York", 3)]:
            spark.conf.set("spark.sql.session.timeZone", zone)
            result = weather._associate(context.repartition(partitions), observations.repartition(partitions))
            complete_results.append(Counter(tuple(row) for row in result.collect()))
            rows = result.select("building_id", "metadata_site_id", "timestamp_original",
                                 "weather_association_status", "weather_timestamp_original",
                                 "weather_offset_seconds", "weather_airTemperature_original").collect()
            results.append(Counter(tuple(row) for row in rows))
            assert not result.select(*context.columns).exceptAll(context).limit(1).count()
            assert not context.exceptAll(result.select(*context.columns)).limit(1).count()
        assert results[0] == results[1]
        assert complete_results[0] == complete_results[1]
        rows = results[0]
        def found(label, site="S", building="a"):
            return [r for r in rows if r[0:3] == (building, site, label)][0]
        assert found("2016-01-01 12:00:00")[5:] == (0, "1")
        assert found("2016-01-01 12:29:00")[5:] == (-1740, "1")
        assert found("2016-01-01 12:30:00")[5:] == (-1800, "1")
        assert found("2016-01-01 12:31:00")[5:] == (1740, "2")
        assert rows[found("2016-01-01 12:31:00")] == 2
        assert found("2016-01-01 14:30:00")[3:] == ("duplicate_identical", "2016-01-01 14:00:00", -1800, None)
        full = next(dict(zip(result.columns, row)) for row in complete_results[0]
                    if row[2] == "2016-01-01 14:30:00")
        assert full["weather_match_type"] == "approximate"
        assert found("2016-01-01 15:30:01")[3] == "duplicate_conflicting"
        assert found("2016-01-01 16:01:00")[3] == "duplicate_conflicting"
        assert found("2016-01-01 12:00:00", "T")[6] == "7"
        assert found("2016-01-01 12:00:00", None)[3] == "missing_site"
        assert found("bad")[3] == "invalid_timestamp"
        assert found("2016-02-29 00:00:00")[5] == -1800
        assert found("2017-01-01 00:00:00")[5] == -1800
        assert found("2016-03-13 02:30:00")[5] == -1800
        assert found("2016-11-06 01:30:00")[5] == -1800
        assert found("2016-01-01 12:30:01", "U")[3] == "missing_observation"
        assert found("2016-01-01 11:30:00", "U")[5] == 1800
        assert found("2016-01-01 12:00:00", "X")[5:] == (0, "13")
        assert found("2016-01-01 12:30:00", "S", "b")[5:] == (-1800, "1")
        assert found("2016-05-01 00:00:00")[5] == -1800
        assert found("2016-01-01 12:17:30", "Y")[5:] == (0, "16")
    finally:
        spark.stop()
