"""Synthetic Delta publication, interruption and explicit recovery."""
import hashlib

import pytest

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_capture as capture
from tfm_lakehouse.silver import weather_publication as publication


@pytest.mark.integration
def test_delta_commit_survives_explicit_recovery(spark, tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    electricity, metadata, weather = (raw / name for name in ("electricity.csv", "metadata.csv", "weather.csv"))
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n" +
                           ("2016-01-01 12:30:00," + ",".join(["1"] * 16) + "\n") * 2)
    metadata.write_text("building_id,site_id\n" + "\n".join(f"{name},S" for name in APPROVED_BUILDINGS) + "\n")
    weather.write_text("site_id,timestamp,airTemperature\nS,2016-01-01 12:00:00,12\n")
    config = BronzeConfig(raw, tmp_path / "bronze", electricity, metadata, "synthetic", "invented", weather=weather)
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    contextual_publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    def forbidden(*args, **kwargs):
        pytest.fail("Weather publication must not access IQR")
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    captured = capture.create_synthetic_weather_capture(spark, config, "silver", "context", "weather",
                                                        "capture", "owner", tmp_path)
    original = silver._exclusive_json
    def interrupted(path, value):
        if path.name == "completed.json" and path.parent.parent.name == "weather-publications":
            raise OSError("synthetic interruption before marker")
        return original(path, value)
    with monkeypatch.context() as fault:
        fault.setattr(silver, "_exclusive_json", interrupted)
        with pytest.raises(OSError, match="interruption"):
            publication.publish_synthetic_weather(spark, config, "capture", "owner", "weather", tmp_path)
    folder = tmp_path / "weather-publications" / "weather"
    commit = folder / "table" / "_delta_log" / "00000000000000000000.json"
    initial = hashlib.sha256(commit.read_bytes()).hexdigest()
    assert not (folder / "completed.json").exists()
    source_change = config.table_path("weather") / "synthetic-mutation"
    source_change.write_text("changed")
    try:
        with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
            publication.publish_synthetic_weather(spark, config, "capture", "owner", "weather", tmp_path)
        assert hashlib.sha256(commit.read_bytes()).hexdigest() == initial
        assert not (folder / "completed.json").exists()
    finally:
        source_change.unlink()
    capture_change = tmp_path / "weather-captures" / "capture" / "table" / "synthetic-mutation"
    capture_change.write_text("changed")
    try:
        with pytest.raises(iqr.IQRPublicationError, match="Capture tree changed"):
            publication.publish_synthetic_weather(spark, config, "capture", "owner", "weather", tmp_path)
        assert not (folder / "completed.json").exists()
    finally:
        capture_change.unlink()
    recovered = publication.publish_synthetic_weather(spark, config, "capture", "owner", "weather", tmp_path)
    assert hashlib.sha256(commit.read_bytes()).hexdigest() == initial
    assert recovered["delta_version"] == 0 and recovered["delta_table_id"]
    assert recovered["intent"]["capture"]["delta_table_id"] == captured["delta_table_id"]
    frame = spark.read.format("delta").option("versionAsOf", 0).load(str(folder / "table"))
    assert frame.count() == 32
    assert len(list((folder / "table" / "_delta_log").glob("[0-9]*.json"))) == 1
    substitute, held = tmp_path / "substitute", tmp_path / "held-publication-table"
    (frame.write.format("delta").mode("errorifexists")
     .option("userMetadata", iqr._canonical(recovered["intent"])).save(str(substitute)))
    table = folder / "table"
    table.rename(held)
    substitute.rename(table)
    try:
        with pytest.raises(iqr.IQRPublicationError, match="completion evidence differs"):
            publication.publish_synthetic_weather(spark, config, "capture", "owner", "weather", tmp_path)
    finally:
        table.rename(substitute)
        held.rename(table)
