"""Real synthetic Delta provenance for preliminary weather identity."""
import pytest
import json
from pyspark.sql import functions as F

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_identity as identity
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint


@pytest.mark.integration
def test_weather_identity_binds_historical_delta_and_iqr(spark, tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    electricity, metadata, weather = (raw / name for name in ("electricity.csv", "metadata.csv", "weather.csv"))
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                           + "2016-01-01 12:30:00," + ",".join(["1"] * 16) + "\n")
    metadata.write_text("building_id,site_id\n" + "\n".join(f"{name},S" for name in APPROVED_BUILDINGS) + "\n")
    weather.write_text("site_id,timestamp,airTemperature\nS,2016-01-01 12:00:00,12\n")
    config = BronzeConfig(raw, tmp_path / "bronze", electricity, metadata, "synthetic", "invented", weather=weather)
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    contextual_publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    (spark.read.format("delta").option("versionAsOf", 0).load(str(config.table_path("weather")))
     .withColumn("airTemperature", F.lit("99"))
     .write.format("delta").mode("append").save(str(config.table_path("weather"))))

    def forbidden(*args, **kwargs):
        pytest.fail("Weather identity must not use IQR results")

    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    intent, frame = identity.build_synthetic_weather_identity(spark, config, "silver", "context", "weather", tmp_path)
    assert intent["weather_delta_version"] == 0
    assert intent["weather_source_uri"] == weather.resolve().as_uri()
    assert intent["contextual"]["intent"]["publication_id"] == "context"
    assert intent["content_evidence"] == "pending_durable_capture"
    assert intent["tolerance_seconds"] == 1800 and intent["inclusive"] is True
    assert frame.count() == 16
    assert {r.weather_airTemperature_original for r in frame.select("weather_airTemperature_original").distinct().collect()} == {"12"}
    assert intent["weather_files_sha256"] == iqr._fingerprint(config.table_path("weather"))
    verified = identity.verify_synthetic_weather_identity(spark, config, "silver", "context", "weather",
                                                         tmp_path, intent, frame)
    assert verified["status"] == "reconciled_for_this_action"
    assert verified["durable_content_verified"] is False
    commit = config.table_path("weather") / "_delta_log" / "00000000000000000000.json"
    original = commit.read_bytes()
    try:
        records = [json.loads(line) for line in original.decode().splitlines()]
        next(item["commitInfo"] for item in records if "commitInfo" in item)["userMetadata"] = "altered"
        commit.write_text("\n".join(json.dumps(item) for item in records) + "\n")
        assert weather_tree_fingerprint(config.table_path("weather")) != intent["weather_files_sha256"]
        with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
            identity.verify_synthetic_weather_identity(spark, config, "silver", "context", "weather",
                                                       tmp_path, intent, frame)
    finally:
        commit.write_bytes(original)
