"""End-to-end binding of distributed weather diagnostics to synthetic sources."""
import pytest

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_quality


@pytest.mark.integration
def test_quality_api_uses_verified_sources_without_iqr(spark, tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    electricity, metadata, weather = (raw / name for name in ("electricity.csv", "metadata.csv", "weather.csv"))
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                           + "2016-01-01 12:30:00," + ",".join(["1"] * 16) + "\n")
    metadata.write_text("building_id,site_id\n" + "\n".join(f"{name},S" for name in APPROVED_BUILDINGS) + "\n")
    weather.write_text("site_id,timestamp,airTemperature\nS,2016-01-01 12:00:00,12\n")
    config = BronzeConfig(raw, tmp_path / "bronze", electricity, metadata, "synthetic", "invented",
                          weather=weather)
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    contextual_publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)

    def forbidden(*args, **kwargs):
        pytest.fail("Quality diagnostics must not access IQR")

    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    evidence, reports = weather_quality.diagnose_synthetic_weather_quality(
        spark, config, "silver", "context", tmp_path)
    assert evidence["weather_version"] == next(t["delta_version"] for t in bronze["tables"] if t["name"] == "weather")
    site = reports["coverage"].where("scope = 'site'").first()
    assert (site.electrical_rows, site.associated_rows, site.associated_weather_keys) == (16, 16, 1)
    air = reports["variables"].where("scope = 'site' AND variable = 'airTemperature'").first()
    assert (air.associated_rows, air.finite_rows, air.available_weather_keys) == (16, 16, 1)
    sensitivity = reports["sensitivity"].where("scope = 'site'").select(
        "tolerance_seconds", "associated_rows", "associated_rows_gain").collect()
    assert sorted(tuple(row) for row in sensitivity) == [(0, 0, None), (900, 0, 0),
                                                         (1800, 16, 16), (3600, 16, 0)]
