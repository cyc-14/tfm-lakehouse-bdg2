"""Explicit read of a synthetic weather publication over retained Delta sources."""
import pytest

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_capture as capture
from tfm_lakehouse.silver import weather_publication as publication
from tfm_lakehouse.silver import weather_reader as reader
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint


@pytest.mark.integration
def test_explicit_reader_delta_sources_and_no_evidence_writes(spark, tmp_path, monkeypatch):
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
    capture.create_synthetic_weather_capture(spark, config, "silver", "context", "weather",
                                             "capture", "owner", tmp_path)
    published = publication.publish_synthetic_weather(spark, config, "capture", "owner", "weather", tmp_path)
    def forbidden(*_args, **_kwargs):
        pytest.fail("Reader must not publish, recover or access IQR")
    monkeypatch.setattr(publication, "publish_synthetic_weather", forbidden)
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    before = weather_tree_fingerprint(tmp_path)
    evidence, frame = reader.read_synthetic_weather(spark, config, "weather", tmp_path)
    assert evidence == published and evidence["delta_version"] == 0
    assert frame.schema.jsonValue() == evidence["physical_schema"]
    assert frame.count() == 32
    assert weather_tree_fingerprint(tmp_path) == before
    marker = tmp_path / "weather-publications" / "weather" / "completed.json"
    original = marker.read_bytes()
    try:
        marker.write_text("{")
        changed = weather_tree_fingerprint(tmp_path)
        with pytest.raises(iqr.IQRPublicationError, match="evidence"):
            reader.read_synthetic_weather(spark, config, "weather", tmp_path)
        assert weather_tree_fingerprint(tmp_path) == changed
    finally:
        marker.write_bytes(original)
    table_change = tmp_path / "weather-publications" / "weather" / "table" / "synthetic-change"
    table_change.write_text("changed")
    try:
        changed = weather_tree_fingerprint(tmp_path)
        with pytest.raises(iqr.IQRPublicationError, match="completion evidence differs"):
            reader.read_synthetic_weather(spark, config, "weather", tmp_path)
        assert weather_tree_fingerprint(tmp_path) == changed
    finally:
        table_change.unlink()
    weather_change = config.table_path("weather") / "synthetic-change"
    weather_change.write_text("changed")
    try:
        changed = weather_tree_fingerprint(tmp_path)
        with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
            reader.read_synthetic_weather(spark, config, "weather", tmp_path)
        assert weather_tree_fingerprint(tmp_path) == changed
    finally:
        weather_change.unlink()
    capture_folder = tmp_path / "weather-captures" / "capture"
    held = tmp_path / "temporarily-unavailable-capture"
    capture_folder.rename(held)
    try:
        changed = weather_tree_fingerprint(tmp_path)
        with pytest.raises(iqr.IQRPublicationError, match="Capture is unavailable"):
            reader.read_synthetic_weather(spark, config, "weather", tmp_path)
        assert weather_tree_fingerprint(tmp_path) == changed
    finally:
        held.rename(capture_folder)
