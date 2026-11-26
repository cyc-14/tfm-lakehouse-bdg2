"""Synthetic Bronze, Silver and contextual identity binding for weather."""
import pytest
from pyspark.sql import functions as F

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication, silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_enrichment as weather


@pytest.mark.integration
def test_weather_api_binds_sources_and_rejects_mutations(spark, tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    electricity, metadata, observations = (raw / name for name in ("electricity.csv", "metadata.csv", "weather.csv"))
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                           + "2016-01-01 12:30:00," + ",".join(["1"] * 16) + "\n")
    metadata.write_text("building_id,site_id\n" + "\n".join(f"{name},S" for name in APPROVED_BUILDINGS) + "\n")
    observations.write_text("site_id,timestamp,airTemperature\nS,2016-01-01 12:00:00,12\n")
    config = BronzeConfig(raw, tmp_path / "bronze", electricity, metadata, "synthetic", "invented",
                          weather=observations)
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    contextual_publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    # A newer weather Delta version must not replace the manifest's snapshot.
    (spark.read.format("delta").option("versionAsOf", 0).load(str(config.table_path("weather")))
     .write.format("delta").mode("append").save(str(config.table_path("weather"))))

    def forbidden(*args, **kwargs):
        pytest.fail("Weather association must not read or calculate IQR")

    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    evidence, frame = weather.associate_synthetic_weather(spark, config, "silver", "context", tmp_path)
    rows = frame.select("weather_association_status", "weather_offset_seconds",
                        "weather_airTemperature_original", "weather_cloudCoverage_status").collect()
    assert len(rows) == 16
    assert all(tuple(row) == ("associated_unique", -1800, "12", "column_missing") for row in rows)
    assert evidence["weather_version"] == next(t["delta_version"] for t in bronze["tables"] if t["name"] == "weather")
    assert evidence["association_policy"] == weather.ASSOCIATION_POLICY
    assert len(evidence["weather_files_sha256"]) == 64
    assert evidence["contextual"]["intent"]["publication_id"] == "context"
    original = weather._associate

    def changed(*args):
        result = original(*args)
        (config.table_path("weather") / "synthetic-change").write_text("changed")
        return result

    monkeypatch.setattr(weather, "_associate", changed)
    with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
        weather.associate_synthetic_weather(spark, config, "silver", "context", tmp_path)
    monkeypatch.setattr(weather, "_associate", original)
    (config.table_path("weather") / "synthetic-change").unlink()
    read_tables = weather._read_verified_tables

    def altered_lineage(*args):
        tables = read_tables(*args)
        tables["weather"] = tables["weather"].withColumn("_bronze_source_sha256", F.lit("wrong"))
        return tables

    monkeypatch.setattr(weather, "_read_verified_tables", altered_lineage)
    with pytest.raises(iqr.IQRPublicationError, match="Weather lineage differs"):
        weather.associate_synthetic_weather(spark, config, "silver", "context", tmp_path)
