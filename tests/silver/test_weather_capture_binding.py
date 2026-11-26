"""Synthetic Delta capture with retained source and failure evidence."""
import pytest
import json

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_capture as capture
from tfm_lakehouse.silver import weather_identity


@pytest.mark.integration
def test_capture_delta_lifecycle_and_integrity(spark, tmp_path, monkeypatch):
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
        pytest.fail("Capture must not access IQR")
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    original_builder = weather_identity.build_synthetic_weather_identity
    calls = 0
    def repartitioned(*args):
        nonlocal calls
        intent, view = original_builder(*args)
        calls += 1
        return intent, view.repartition(1 if calls % 2 else 3)
    monkeypatch.setattr(weather_identity, "build_synthetic_weather_identity", repartitioned)
    created = capture.create_synthetic_weather_capture(spark, config, "silver", "context", "weather",
                                                        "capture", "owner", tmp_path)
    assert created["delta_version"] == 0 and created["delta_table_id"]
    folder = tmp_path / "weather-captures" / "capture"
    data = spark.read.format("delta").option("versionAsOf", 0).load(str(folder / "table"))
    assert data.count() == 32
    spark.catalog.clearCache()
    verified = capture.verify_synthetic_weather_capture(spark, config, "capture", "owner", tmp_path)
    assert verified == created
    with pytest.raises(iqr.IQRPublicationError, match="ownership"):
        capture.verify_synthetic_weather_capture(spark, config, "capture", "other", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="already exists"):
        capture.create_synthetic_weather_capture(spark, config, "silver", "context", "weather",
                                                 "capture", "owner", tmp_path)
    extra = folder / "table" / "unexpected"
    extra.write_text("changed")
    with pytest.raises(iqr.IQRPublicationError, match="tree changed"):
        capture.verify_synthetic_weather_capture(spark, config, "capture", "owner", tmp_path)
    extra.unlink()
    marker = folder / "completed.json"
    original = marker.read_bytes()
    forged = json.loads(original)
    forged["delta_table_id"] = "wrong"
    marker.write_text(json.dumps(forged))
    with pytest.raises(iqr.IQRPublicationError, match="commit evidence differs"):
        capture.verify_synthetic_weather_capture(spark, config, "capture", "owner", tmp_path)
    marker.write_text("{")
    with pytest.raises(iqr.IQRPublicationError):
        capture.verify_synthetic_weather_capture(spark, config, "capture", "owner", tmp_path)
    marker.write_bytes(original)


@pytest.mark.integration
def test_partial_reservation_does_not_retry(spark, tmp_path, monkeypatch):
    namespace = tmp_path / "weather-captures"
    namespace.mkdir()
    folder = namespace / "partial"
    folder.mkdir()
    (folder / "intent.json").write_text("{}")
    monkeypatch.setattr(capture, "_namespace", lambda *_: (tmp_path, namespace))
    with pytest.raises(iqr.IQRPublicationError, match="incomplete"):
        capture.verify_synthetic_weather_capture(spark, None, "partial", "owner", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="already exists"):
        capture.create_synthetic_weather_capture(spark, None, "s", "c", "p", "partial", "owner", tmp_path)
    assert (folder / "intent.json").read_text() == "{}"
