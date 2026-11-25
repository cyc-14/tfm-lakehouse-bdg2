"""Integrated synthetic H06 workflows and weather/IQR independence."""
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_diagnostics
from test_silver_iqr_publication import _create_silver


def _weather_config(config):
    weather = config.raw_root / "weather.csv"
    weather.write_text("timestamp,site_id,airTemperature\n"
                       "2016-01-01 00:00:00,SITE,12\n"
                       "2016-01-01 01:00:00,SITE,13\n")
    config.metadata.write_text("building_id,site_id,timezone\n" +
                               "\n".join(f"{name},SITE,UTC" for name in APPROVED_BUILDINGS) + "\n")
    return replace(config, weather=weather)


@pytest.mark.integration
def test_available_weather_diagnostic_with_accepted_iqr(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    config = _weather_config(config)
    start = datetime(2016, 1, 1)
    rows = [(start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S") + "," + ",".join(["1"] * 16)
            for i in range(16667)]
    config.electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                                  + "\n".join(rows) + "\n")
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "weather_source", tmp_path)
    published = iqr.publish_synthetic_iqr(spark, config, "weather_source", "accepted", tmp_path)
    state, _, _ = iqr.read_synthetic_iqr_pair(spark, config, tmp_path,
                                              silver_publication_id="weather_source",
                                              iqr_publication_id="accepted")
    assert state["iqr_status"] == "available"
    before = {"bronze": iqr._fingerprint(config.lakehouse_root),
              "silver": iqr._fingerprint(tmp_path / "silver-publications"),
              "iqr": iqr._fingerprint(tmp_path / "iqr-publications")}
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *a, **k: pytest.fail("IQR recalculated"))
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR consulted"))
    diagnostic = weather_diagnostics.diagnose_synthetic_weather(spark, config, "weather_source", tmp_path)
    assert diagnostic["weather_status"] == "available"
    assert diagnostic["bronze_run_id"] == bronze["run_id"]
    assert diagnostic["silver"]["delta_table_id"] == published["intent"]["silver"]["delta_table_id"]
    assert {"bronze": iqr._fingerprint(config.lakehouse_root),
            "silver": iqr._fingerprint(tmp_path / "silver-publications"),
            "iqr": iqr._fingerprint(tmp_path / "iqr-publications")} == before


@pytest.mark.integration
def test_h06_workflow_publish_recover_and_explicit_read(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    config = _weather_config(config)
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "weather_source", tmp_path)
    iqr_result = iqr.publish_synthetic_iqr(spark, config, "weather_source", "limits", tmp_path)
    state, _, _ = iqr.read_synthetic_iqr_pair(spark, config, tmp_path,
                                              silver_publication_id="weather_source",
                                              iqr_publication_id="limits")
    assert state["iqr_status"] == "not_accepted"
    before = {"bronze": iqr._fingerprint(config.lakehouse_root),
              "silver": iqr._fingerprint(tmp_path / "silver-publications"),
              "iqr": iqr._fingerprint(tmp_path / "iqr-publications")}
    with monkeypatch.context() as patch:
        patch.setattr(iqr, "summarize_electricity_iqr", lambda *a, **k: pytest.fail("IQR recalculated"))
        patch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR consulted"))
        weather = weather_diagnostics.diagnose_synthetic_weather(spark, config, "weather_source", tmp_path)
    assert weather["weather_status"] == "available"
    intent = identity.build_synthetic_contextual_intent(spark, config, "weather_source", "context", tmp_path)
    assert intent["silver"]["delta_table_id"] == iqr_result["intent"]["silver"]["delta_table_id"]
    original = silver._exclusive_json

    def interrupted(path, value):
        if path.name == "completed.json" and "contextual-publications" in str(path):
            raise OSError("synthetic marker interruption")
        return original(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", interrupted)
        with pytest.raises(OSError, match="marker interruption"):
            publication.publish_synthetic_contextual(spark, config, "weather_source", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    log = folder / "table" / "_delta_log" / "00000000000000000000.json"
    before_log = log.read_bytes()
    with pytest.raises(silver.SilverPublicationError):
        reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    result = publication.publish_synthetic_contextual(spark, config, "weather_source", "context", tmp_path)
    evidence, frame = reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert evidence == result and intent == result["intent"]
    assert frame.count() == 128 and log.read_bytes() == before_log
    contextual = frame.select("metadata_available", "metadata_site_id", "metadata_timezone",
                              "metadata_timezone_status").distinct().collect()
    assert [tuple(row) for row in contextual] == [(True, "SITE", "UTC", "valid")]
    assert "airTemperature" not in frame.columns
    assert len(list(log.parent.glob("*.json"))) == 1
    assert {"bronze": iqr._fingerprint(config.lakehouse_root),
            "silver": iqr._fingerprint(tmp_path / "silver-publications"),
            "iqr": iqr._fingerprint(tmp_path / "iqr-publications")} == before
