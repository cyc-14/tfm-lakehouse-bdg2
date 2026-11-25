"""Invented fixtures for IQR Delta identity, ordering and failure recovery."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta
from dataclasses import replace
import json
import os
from pathlib import Path
from threading import Event

from delta.tables import DeltaTable
import pytest
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.ingestion.snapshot import SnapshotError
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import silver_iqr_orchestration as orchestration
from tfm_lakehouse.silver import metadata_diagnostics as diagnostics
from tfm_lakehouse.silver import metadata_enrichment as enrichment
from tfm_lakehouse.silver import contextual_identity as contextual_identity
from tfm_lakehouse.silver import weather_diagnostics as weather_diagnostics
from tfm_lakehouse.silver.silver_iqr import IQR_METHOD, summarize_electricity_iqr


def _create_silver(spark, root, name="silver", *, interrupt=False):
    raw = root / "raw"
    raw.mkdir(exist_ok=True)
    electricity, metadata = raw / "electricity.csv", raw / "metadata.csv"
    labels = ["2016-01-01 00:00:00", "2016-01-01 01:00:00",
              "2016-01-01 01:00:00", "2016-02-29 02:00:00", "2017-12-31 11:37:00",
              "", "2018-01-01 00:00:00", "bad"]
    rows = []
    for index, label in enumerate(labels):
        values = ["0" if index < 4 else "100"] * 16
        values[1] = ("99999999999999999999.00000000000000000" + str(index)
                     if index < 4 else "NaN")
        values[2] = (("-99999999999999999999.999999999999999999" if index < 2
                      else "99999999999999999999.999999999999999999") if index < 4 else "NaN")
        values[-1] = "NaN"
        if index == 0:
            values[-2] = ""
        rows.append(label + "," + ",".join(values))
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                           + "\n".join(rows) + "\n")
    metadata.write_text("building_id\n" + "\n".join(APPROVED_BUILDINGS) + "\n")
    config = BronzeConfig(raw, root / "bronze", electricity, metadata, "synthetic", "invented")
    report = run_bronze(spark, config)
    if interrupt:
        original = silver._exclusive_json

        def stop(path, value):
            if path.name == "completed.json":
                raise OSError("invented Silver interruption")
            return original(path, value)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(silver, "_exclusive_json", stop)
            with pytest.raises(OSError, match="interruption"):
                silver.publish_synthetic_silver(spark, config, report["run_id"], name, root)
        return config, report, None
    result = silver.publish_synthetic_silver(spark, config, report["run_id"], name, root)
    return config, report, result


@pytest.fixture
def published(spark, tmp_path):
    config, bronze, result = _create_silver(spark, tmp_path)
    return tmp_path, config, bronze, result


@pytest.mark.unit
def test_contextual_zone_hashes_actual_bytes_without_cache(tmp_path, monkeypatch):
    from zoneinfo import ZoneInfo
    source = Path("/usr/share/zoneinfo/UTC")
    if not source.is_file():
        pytest.skip("System UTC timezone resource unavailable")
    content = source.read_bytes()
    path = tmp_path / "zone"
    path.write_bytes(content)
    ZoneInfo("UTC")  # A previously cached object must not provide the evidence.
    monkeypatch.setattr(contextual_identity._tzpath, "find_tzfile", lambda _: str(path))
    status, resource, digest = contextual_identity._controlled_zone("UTC")
    import hashlib
    assert status == "valid" and resource == "file:" + str(path)
    assert digest == hashlib.sha256(content).hexdigest()
    alternate = Path("/usr/share/zoneinfo/Etc/GMT+1")
    if alternate.is_file():
        path.write_bytes(alternate.read_bytes())
        assert contextual_identity._controlled_zone("UTC")[2] != digest
    path.write_bytes(b"corrupt timezone")
    with pytest.raises((ValueError, EOFError)):
        contextual_identity._controlled_zone("UTC")


@pytest.mark.unit
def test_contextual_zone_missing_and_invalid():
    assert contextual_identity._controlled_zone(None)[0] == "null"
    assert contextual_identity._controlled_zone("")[0] == "empty"
    assert contextual_identity._controlled_zone("invalid/unknown/invented")[0] == "unknown"
    assert contextual_identity._controlled_zone("../invalid")[0] == "unknown"


@pytest.mark.unit
def test_contextual_zone_lookup_precedence_and_worker_disagreement(tmp_path, monkeypatch):
    from io import BytesIO
    from types import SimpleNamespace
    source = Path("/usr/share/zoneinfo/UTC")
    if not source.is_file():
        pytest.skip("System UTC timezone resource unavailable")
    content = source.read_bytes()
    path = tmp_path / "first"
    path.write_bytes(content)
    monkeypatch.setattr(contextual_identity._tzpath, "find_tzfile", lambda _: str(path))
    monkeypatch.setattr(contextual_identity._common, "load_tzdata", lambda _: pytest.fail("fallback used"))
    assert contextual_identity._controlled_zone("UTC")[1] == "file:" + str(path)
    monkeypatch.setattr(contextual_identity._tzpath, "find_tzfile", lambda _: None)
    monkeypatch.setattr(contextual_identity._common, "load_tzdata", lambda _: BytesIO(content))
    assert contextual_identity._controlled_zone("UTC")[1] == "tzdata:UTC"
    one = SimpleNamespace(metadata_timezone="UTC", status="valid", resource="a", sha256="1")
    two = SimpleNamespace(metadata_timezone="UTC", status="valid", resource="a", sha256="2")
    with pytest.raises(iqr.IQRPublicationError, match="worker evidence differs"):
        contextual_identity._merge_zone_evidence([one, two])


@pytest.mark.integration
def test_contextual_intent_with_real_worker_timezone_and_replay(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    config.metadata.write_text("building_id,timezone\n" +
                               "\n".join(f"{name},UTC" for name in APPROVED_BUILDINGS) + "\n")
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "context_source", tmp_path)
    intent = contextual_identity.build_synthetic_contextual_intent(
        spark, config, "context_source", "context", tmp_path)
    assert intent["timezone_evidence"][0]["timezone"] == "UTC"
    assert intent["timezone_evidence"][0]["status"] == "valid"
    assert len(intent["timezone_evidence"][0]["sha256"]) == 64
    with pytest.raises(iqr.IQRPublicationError, match="evidence differs"):
        contextual_identity.verify_synthetic_contextual_intent(
            spark, config, "silver", "context", tmp_path, intent)
    metadata_path = config.table_path("metadata")
    held = tmp_path / "held-metadata"
    metadata_path.rename(held)
    try:
        with pytest.raises((iqr.IQRPublicationError, SnapshotError, OSError)):
            contextual_identity.build_synthetic_contextual_intent(
                spark, config, "context_source", "context", tmp_path)
    finally:
        held.rename(metadata_path)
    changed = deepcopy(intent)
    changed["timezone_evidence"][0]["sha256"] = "0" * 64
    with pytest.raises(iqr.IQRPublicationError, match="evidence differs"):
        contextual_identity.verify_synthetic_contextual_intent(
            spark, config, "context_source", "context", tmp_path, changed)
    from io import BytesIO
    import hashlib
    from zoneinfo import ZoneInfo
    first_zone, second_zone = Path("/usr/share/zoneinfo/UTC"), Path("/usr/share/zoneinfo/Etc/GMT+1")
    if first_zone.is_file() and second_zone.is_file():
        resource = tmp_path / "synthetic-zone"
        resource.write_bytes(first_zone.read_bytes())
        def controlled(name):
            if name is None:
                return "null", None, None
            data = resource.read_bytes()
            ZoneInfo.from_file(BytesIO(data), key=name)
            return "valid", "file:" + str(resource), hashlib.sha256(data).hexdigest()
        monkeypatch.setattr(contextual_identity, "_controlled_zone", controlled)
        pinned = contextual_identity.build_synthetic_contextual_intent(
            spark, config, "context_source", "context", tmp_path)
        resource.write_bytes(second_zone.read_bytes())
        with pytest.raises(iqr.IQRPublicationError, match="evidence differs"):
            contextual_identity.verify_synthetic_contextual_intent(
                spark, config, "context_source", "context", tmp_path, pinned)
    manifest = config.namespace / "_runs" / (bronze["run_id"] + ".json")
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(silver.SilverPublicationError):
        contextual_identity.build_synthetic_contextual_intent(
            spark, config, "context_source", "context", tmp_path)


@pytest.mark.integration
def test_contextual_intent_rejects_worker_evidence_disagreement(spark, published, monkeypatch):
    from pyspark import TaskContext
    root, config, _, _ = published
    existing = contextual_identity.enrich_synthetic_metadata
    def repartitioned(*args):
        diagnostic, frame = existing(*args)
        return diagnostic, frame.repartition(4)
    def inconsistent(value):
        return "null", "invented", str(TaskContext.get().partitionId())
    monkeypatch.setattr(contextual_identity, "enrich_synthetic_metadata", repartitioned)
    monkeypatch.setattr(contextual_identity, "_controlled_zone", inconsistent)
    with pytest.raises(iqr.IQRPublicationError, match="worker evidence differs"):
        contextual_identity.build_synthetic_contextual_intent(spark, config, "silver", "context", root)


@pytest.mark.integration
def test_contextual_intent_deterministic_without_iqr(spark, published, monkeypatch):
    root, config, bronze, result = published
    newer = run_bronze(spark, config)
    assert newer["run_id"] != bronze["run_id"]
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *_: pytest.fail("IQR must not run"))
    first = contextual_identity.build_synthetic_contextual_intent(spark, config, "silver", "context", root)
    second = contextual_identity.build_synthetic_contextual_intent(spark, config, "silver", "context", root)
    assert first == second
    assert first["silver"]["delta_table_id"] == result["delta_table_id"]
    assert first["bronze_run_id"] == bronze["run_id"]
    with pytest.raises(iqr.IQRPublicationError, match="Unsupported"):
        contextual_identity.build_synthetic_contextual_intent(spark, config, "silver", "context", root,
                                                            enrichment_version="different")


@pytest.mark.unit
def test_contextual_intent_rejects_real_config(tmp_path):
    from types import SimpleNamespace
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        contextual_identity.build_synthetic_contextual_intent(
            None, SimpleNamespace(data_kind="real"), "silver", "context", tmp_path)


@pytest.mark.unit
def test_weather_transition_and_special_tokens_are_diagnostic_only():
    assert weather_diagnostics._transition_sensitive("2016-03-13 02:30:00", "America/New_York")
    assert weather_diagnostics._transition_sensitive("2016-11-06 01:30:00", "America/New_York")
    assert not weather_diagnostics._transition_sensitive("2016-01-01 01:00:00", "America/New_York")
    assert weather_diagnostics._special("-1e0", "precipDepth1HR")
    assert weather_diagnostics._special("+9", "cloudCoverage")
    assert not weather_diagnostics._special("9.0000000000000000001", "cloudCoverage")


@pytest.mark.unit
def test_weather_diagnostic_rejects_real_configuration(tmp_path):
    from types import SimpleNamespace
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        weather_diagnostics.diagnose_synthetic_weather(None, SimpleNamespace(data_kind="real"), "silver", tmp_path)


@pytest.mark.integration
def test_weather_diagnostic_unavailable_keeps_silver_identity(spark, published, monkeypatch):
    root, config, bronze, result = published
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *_: pytest.fail("IQR must not run"))
    weather = weather_diagnostics.diagnose_synthetic_weather(spark, config, "silver", root)
    assert weather["weather_status"] == "unavailable"
    assert weather["silver"]["delta_table_id"] == result["delta_table_id"]
    assert weather["bronze_run_id"] == bronze["run_id"]
    assert all(row["status"] == "missing_site_column" for row in weather["mappings"])


@pytest.mark.integration
def test_weather_without_site_column_reports_ambiguous_metadata(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    config.metadata.write_text("building_id\n" + "\n".join(APPROVED_BUILDINGS)
                               + "\n" + APPROVED_BUILDINGS[0] + "\n")
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "duplicate_meta", tmp_path)
    result = weather_diagnostics.diagnose_synthetic_weather(spark, config, "duplicate_meta", tmp_path)
    assert result["weather_status"] == "unavailable"
    assert result["mappings"][0]["status"] == "ambiguous_metadata"
    assert not result["metadata_site_column_available"]
    assert result["metadata_duplicates"]["selected"]["identical"] == 1


@pytest.mark.integration
def test_weather_diagnostic_historical_duplicates_coverage_and_lineage(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    weather_file = config.raw_root / "weather.csv"
    metadata_rows = [f"{name},SITE,America/New_York" for name in APPROVED_BUILDINGS]
    metadata_rows[1] = f"{APPROVED_BUILDINGS[1]},NO_WEATHER,America/New_York"
    metadata_rows[2] = f"{APPROVED_BUILDINGS[2]}, ,America/New_York"
    metadata_rows.append(f"{APPROVED_BUILDINGS[0]},OTHER,America/New_York")
    config.metadata.write_text("building_id,site_id,timezone\n" + "\n".join(metadata_rows) + "\n")
    weather_file.write_text("timestamp,site_id,airTemperature,precipDepth1HR,cloudCoverage\n"
                            "2016-01-01 00:00:00,SITE,1,-1,9\n"
                            "2016-01-01 00:00:00,SITE,1,-1,9\n"
                            "2016-01-01 00:00:00,SITE,2,-1,9\n"
                            "2016-01-01 01:00:00,SITE,,NaN,0\n"
                            "2016-01-01 02:00:00,SITE,1,0,0\n"
                            "2016-01-01 02:00:00,SITE,1,0,0\n"
                            "2016-01-01 00:00:00,OTHER,1,0,0\n"
                            "2016-01-01 00:00:00,OTHER,1,0,0\n"
                            "2016-03-13 02:30:00,SITE,bad,0,0\n"
                            "2016-11-06 01:30:00,SITE,3,0,0\n"
                            "invalid,SITE,1,0,0\n")
    config = replace(config, weather=weather_file)
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "with_weather", tmp_path)
    weather_file.write_text("timestamp,site_id,airTemperature,precipDepth1HR,cloudCoverage\n"
                            "2016-01-01 00:00:00,SITE,99,0,0\n")
    run_bronze(spark, config)
    iqr.publish_synthetic_iqr(spark, config, "with_weather", "weather_iqr", tmp_path)
    before_iqr = iqr._fingerprint(tmp_path / "iqr-publications" / "weather_iqr")
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *_: pytest.fail("IQR must not run"))
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR must not be read"))
    before_silver = iqr._fingerprint(tmp_path / "silver-publications" / "with_weather")
    before_weather = iqr._fingerprint(config.table_path("weather"))
    output = weather_diagnostics.diagnose_synthetic_weather(spark, config, "with_weather", tmp_path)
    assert output["weather_status"] == "available" and output["bronze_run_id"] == bronze["run_id"]
    assert output["weather_version"] == next(t["delta_version"] for t in bronze["tables"] if t["name"] == "weather")
    duplicate_by_site = {row["site_id"]: row for row in output["duplicates"]}
    assert duplicate_by_site["SITE"]["conflicting_keys"] == 1
    assert duplicate_by_site["SITE"]["identical_keys"] == 1
    assert duplicate_by_site["SITE"]["exact_duplicate_extra_rows"] == 2
    assert duplicate_by_site["OTHER"]["identical_keys"] == 1
    assert output["mappings"][0]["status"] == "ambiguous_metadata"
    assert output["mappings"][2]["status"] == "invalid_site"
    assert "NO_WEATHER" in output["missing_weather_sites"]
    assert not output["available_variables"]["windSpeed"]
    assert any(r["off_grid"] for r in output["temporal_status"] if r["off_grid"] is not None)
    assert output["transition_diagnostics"][0]["sensitive_labels"] == 0
    assert any(row["sensitive_labels"] == 2 for row in output["transition_diagnostics"])
    assert any(row["status"] == "invalid_format" for row in output["temporal_status"])
    january = [r for r in output["variable_period_profiles"] if r["variable"] == "precipDepth1HR" and r["period"] == "2016-01"]
    site_profile = next(row for row in january if row["site_id"] == "SITE")
    assert site_profile["special_code_count"] == 3 and site_profile["missing"] == 1
    assert any(row["site_id"] == "NO_WEATHER" and row["rows"] == 0
               and row["missing_weather_labels"] > 0 for row in january)
    assert iqr._fingerprint(tmp_path / "silver-publications" / "with_weather") == before_silver
    assert iqr._fingerprint(config.table_path("weather")) == before_weather
    assert iqr._fingerprint(tmp_path / "iqr-publications" / "weather_iqr") == before_iqr
    original_reader = weather_diagnostics._read_verified_tables
    def bad_lineage(*args):
        frames = original_reader(*args)
        frames["weather"] = frames["weather"].withColumn("_bronze_source_sha256", F.lit("wrong"))
        return frames
    with monkeypatch.context() as fault:
        fault.setattr(weather_diagnostics, "_read_verified_tables", bad_lineage)
        with pytest.raises(iqr.IQRPublicationError, match="lineage differs"):
            weather_diagnostics.diagnose_synthetic_weather(spark, config, "with_weather", tmp_path)
    manifest = config.namespace / "_runs" / (bronze["run_id"] + ".json")
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(silver.SilverPublicationError):
        weather_diagnostics.diagnose_synthetic_weather(spark, config, "with_weather", tmp_path)


@pytest.mark.unit
@pytest.mark.parametrize("value,status", [(None, "null"), ("", "empty"), ("NaN", "nonfinite"),
    ("Infinity", "nonfinite"), ("bad", "invalid"), ("0", "zero"), ("-1", "negative"),
    ("1e-19", "positive_nonrepresentable"), ("1e20", "positive_nonrepresentable"),
    ("1.1234567890123456789", "positive_nonrepresentable"), ("1.0000000000000000000", "valid"),
    ("1e-18", "valid"), ("1e2", "valid")])
def test_metadata_area_exact_classification(value, status):
    number, actual = enrichment._area(value)
    assert actual == status
    assert (number is not None) == (status == "valid")


@pytest.mark.unit
@pytest.mark.parametrize("value,status", [(None, "null"), ("", "empty"),
                                         ("UTC", "valid"), ("unknown/invented", "unknown")])
def test_metadata_timezone_classification(value, status):
    assert enrichment._timezone(value) == status


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["identical", "conflicting"])
def test_enrichment_rejects_selected_duplicates_before_join(monkeypatch, kind):
    duplicate = {"identical": 0, "conflicting": 0}
    duplicate[kind] = 1
    monkeypatch.setattr(enrichment, "diagnose_synthetic_metadata", lambda *a: {"duplicates": {"selected": duplicate}})
    with pytest.raises(iqr.IQRPublicationError, match="duplicated"):
        enrichment.enrich_synthetic_metadata(None, None, "silver", None)


@pytest.mark.integration
def test_enrichment_preserves_exact_silver_with_missing_metadata_columns(spark, published, monkeypatch):
    root, config, _, silver_result = published
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *_: pytest.fail("IQR must not run"))
    diagnostic, frame = enrichment.enrich_synthetic_metadata(spark, config, "silver", root)
    source = spark.read.format("delta").option("versionAsOf", 0).load(silver_result["table_path"])
    assert not frame.select(*source.columns).exceptAll(source).count()
    assert not source.exceptAll(frame.select(*source.columns)).count()
    assert frame.where(F.col("metadata_sqm_status") != "missing_column").count() == 0
    assert frame.where(F.col("metadata_site_id_status") != "missing_column").count() == 0
    assert len(diagnostic["building_quality"]) == 16


@pytest.mark.integration
def test_enrichment_context_and_unrelated_duplicates_with_existing_iqr(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    records = [f"{name},SITE,{index + 1}.0,UTC,Lodging,Dormitory"
               for index, name in enumerate(APPROVED_BUILDINGS[:-1])]
    records += ["OTHER,SITE,10,UTC,Lodging,Dormitory"] * 2
    records.append(f"{APPROVED_BUILDINGS[-1].lower()},SITE,10,UTC,Lodging,Dormitory")
    config.metadata.write_text("building_id,site_id,sqm,timezone,primaryspaceusage,sub_primaryspaceusage\n"
                               + "\n".join(records) + "\n")
    (tmp_path / "silver-publications" / "silver").rename(tmp_path / "old-silver")
    bronze = run_bronze(spark, config)
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    iqr.publish_synthetic_iqr(spark, config, "silver", "limits", tmp_path)
    iqr_fingerprint = iqr._fingerprint(tmp_path / "iqr-publications" / "limits")
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *_: pytest.fail("IQR must not run"))
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR must not be read"))
    diagnostic, frame = enrichment.enrich_synthetic_metadata(spark, config, "silver", tmp_path)
    assert diagnostic["duplicates"]["unrelated"]["identical"] == 1
    assert diagnostic["missing_buildings"] == [APPROVED_BUILDINGS[-1]]
    assert frame.where(F.col("building_id") == APPROVED_BUILDINGS[-1]).where(
        F.col("metadata_sqm_status") != "missing_row").count() == 0
    row = frame.where(F.col("building_id") == APPROVED_BUILDINGS[0]).first()
    assert row.metadata_sqm_numeric == 1 and row.metadata_sqm_status == "valid"
    assert row.metadata_timezone_status == "valid" and row.metadata_site_id_status == "present"
    assert weather_diagnostics.diagnose_synthetic_weather(spark, config, "silver", tmp_path)["weather_status"] == "unavailable"
    assert iqr._fingerprint(tmp_path / "iqr-publications" / "limits") == iqr_fingerprint


@pytest.mark.integration
def test_enrichment_full_cohort_and_identical_duplicate_rejection(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    records = [f"{name},SITE,123.45,UTC,Lodging,Dormitory" for name in APPROVED_BUILDINGS]
    header = "building_id,site_id,sqm,timezone,primaryspaceusage,sub_primaryspaceusage\n"
    config.metadata.write_text(header + "\n".join(records) + "\n")
    (tmp_path / "silver-publications" / "silver").rename(tmp_path / "old-silver")
    bronze = run_bronze(spark, config)
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    report, frame = enrichment.enrich_synthetic_metadata(spark, config, "silver", tmp_path)
    assert not report["missing_buildings"] and len(report["building_quality"]) == 16
    assert frame.where(~F.col("metadata_available") | (F.col("metadata_sqm_status") != "valid")).count() == 0
    config.metadata.write_text(header + "\n".join(records + [records[0]]) + "\n")
    (tmp_path / "silver-publications" / "silver").rename(tmp_path / "second-silver")
    bronze = run_bronze(spark, config)
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="duplicated"):
        enrichment.enrich_synthetic_metadata(spark, config, "silver", tmp_path)


@pytest.mark.integration
def test_enrichment_rejects_colliding_silver_columns(spark, published, monkeypatch):
    root, config, _, _ = published
    diagnose = enrichment.diagnose_synthetic_metadata
    verify = iqr._verify_silver
    def diagnose_then_collide(*args):
        result = diagnose(*args)
        def collide(*arguments):
            identity, frame = verify(*arguments)
            return identity, frame.withColumn("metadata_sqm_status", F.lit("collision"))
        monkeypatch.setattr(iqr, "_verify_silver", collide)
        return result
    monkeypatch.setattr(enrichment, "diagnose_synthetic_metadata", diagnose_then_collide)
    with pytest.raises(iqr.IQRPublicationError, match="collision"):
        enrichment.enrich_synthetic_metadata(spark, config, "silver", root)


@pytest.mark.integration
def test_enrichment_does_not_read_accepted_iqr(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    start = datetime(2016, 1, 1)
    rows = [(start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S") + "," + ",".join(["1"] * 16)
            for i in range(16667)]
    config.electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                                  + "\n".join(rows) + "\n")
    (tmp_path / "silver-publications" / "silver").rename(tmp_path / "old-silver")
    bronze = run_bronze(spark, config)
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "silver", tmp_path)
    iqr.publish_synthetic_iqr(spark, config, "silver", "accepted", tmp_path)
    state, _, _ = iqr.read_synthetic_iqr_pair(spark, config, tmp_path,
                                              silver_publication_id="silver", iqr_publication_id="accepted")
    assert state["iqr_status"] == "available"
    before = iqr._fingerprint(tmp_path / "iqr-publications" / "accepted")
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *_: pytest.fail("IQR must not run"))
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR must not be read"))
    _, frame = enrichment.enrich_synthetic_metadata(spark, config, "silver", tmp_path)
    assert frame.count() == 16667 * 16
    assert weather_diagnostics.diagnose_synthetic_weather(spark, config, "silver", tmp_path)["weather_status"] == "unavailable"
    assert iqr._fingerprint(tmp_path / "iqr-publications" / "accepted") == before


@pytest.mark.integration
def test_metadata_diagnostics_bind_historical_version_and_preserve_evidence(spark, published):
    root, config, bronze, silver_result = published
    before = iqr._fingerprint(root / "silver-publications" / "silver")
    config.metadata.write_text("building_id,site_id\nnew-building,new-site\n")
    newer = run_bronze(spark, config)
    assert newer["run_id"] != bronze["run_id"]
    result = diagnostics.diagnose_synthetic_metadata(spark, config, "silver", root)
    assert result["bronze_run_id"] == bronze["run_id"]
    assert result["metadata_version"] == next(t["delta_version"] for t in bronze["tables"]
                                               if t["name"] == "metadata")
    assert result["counts"]["selected"]["rows"] == 16
    assert not result["missing_buildings"] and not result["available_columns"]["site_id"]
    assert result["silver"]["delta_table_id"] == silver_result["delta_table_id"]
    assert iqr._fingerprint(root / "silver-publications" / "silver") == before
    enriched, view = enrichment.enrich_synthetic_metadata(spark, config, "silver", root)
    assert enriched["bronze_run_id"] == bronze["run_id"]
    assert not enriched["available_columns"]["site_id"] and view.count() == silver_result["output_rows"]


@pytest.mark.integration
def test_metadata_diagnostics_classify_duplicates_and_empty_fields(spark, tmp_path):
    root = tmp_path
    config, bronze, _ = _create_silver(spark, root)
    selected = APPROVED_BUILDINGS[0]
    config.metadata.write_text("building_id,site_id,sqm,timezone,primaryspaceusage,sub_primaryspaceusage\n"
                               f"{selected},A,,UTC,Lodging,Dormitory\n"
                               f"{selected},A,,UTC,Lodging,Dormitory\n"
                               f"{selected},B,10,UTC,Lodging,Dormitory\n"
                               "other,A,1,UTC,Lodging,Dormitory\n"
                               "other,A,1,UTC,Lodging,Dormitory\n"
                               ",A,1,UTC,Lodging,Dormitory\n")
    (root / "silver-publications" / "silver").rename(root / "original-silver")
    next_bronze = run_bronze(spark, config)
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    silver.publish_synthetic_silver(spark, config, next_bronze["run_id"], "silver", root)
    result = diagnostics.diagnose_synthetic_metadata(spark, config, "silver", root)
    assert result["duplicates"]["selected"] == {"identical": 0, "conflicting": 1}
    assert result["duplicates"]["unrelated"] == {"identical": 1, "conflicting": 0}
    assert result["counts"]["null_building_id"] == 1
    assert result["counts"]["selected"]["fields"]["sqm"]["null"] == 2
    assert len(result["missing_buildings"]) == 15
    with pytest.raises(iqr.IQRPublicationError, match="duplicated"):
        enrichment.enrich_synthetic_metadata(spark, config, "silver", root)


@pytest.mark.integration
def test_metadata_diagnostics_reject_changed_manifest_and_bad_lineage(spark, published, monkeypatch):
    root, config, bronze, _ = published
    metadata = config.table_path("metadata")
    before = iqr._fingerprint(metadata)
    original_reader = diagnostics._read_verified_tables
    def wrong_lineage(*args):
        frames = original_reader(*args)
        frames["metadata"] = frames["metadata"].withColumn("_bronze_source_sha256", F.lit("wrong"))
        return frames
    with monkeypatch.context() as fault:
        fault.setattr(diagnostics, "_read_verified_tables", wrong_lineage)
        with pytest.raises(iqr.IQRPublicationError, match="lineage differs"):
            diagnostics.diagnose_synthetic_metadata(spark, config, "silver", root)
    manifest = config.namespace / "_runs" / (bronze["run_id"] + ".json")
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(silver.SilverPublicationError):
        diagnostics.diagnose_synthetic_metadata(spark, config, "silver", root)
    with pytest.raises(silver.SilverPublicationError):
        enrichment.enrich_synthetic_metadata(spark, config, "silver", root)
    assert iqr._fingerprint(metadata) == before


@pytest.mark.unit
def test_metadata_diagnostics_reject_real_config(tmp_path):
    from types import SimpleNamespace
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        diagnostics.diagnose_synthetic_metadata(None, SimpleNamespace(data_kind="real"), "silver", tmp_path)


@pytest.mark.integration
def test_metadata_diagnostics_distinguish_empty_from_null(spark, published, monkeypatch):
    root, config, _, _ = published
    original = diagnostics._read_verified_tables
    def with_empty_values(*args):
        frames = original(*args)
        base = frames["metadata"].withColumn(
            "site_id", F.when(F.col("building_id") == APPROVED_BUILDINGS[0], F.lit(""))
            .otherwise(F.lit(None).cast("string")))
        extra = (base.limit(1).withColumn("building_id", F.lit(""))
                 .withColumn("site_id", F.lit("")))
        frames["metadata"] = base.unionByName(extra)
        return frames
    monkeypatch.setattr(diagnostics, "_read_verified_tables", with_empty_values)
    result = diagnostics.diagnose_synthetic_metadata(spark, config, "silver", root)
    assert result["counts"]["empty_building_id"] == 1
    assert result["counts"]["null_building_id"] == 0
    assert result["counts"]["selected"]["fields"]["site_id"] == {"null": 15, "empty": 1}


@pytest.mark.integration
def test_orchestrated_transient_retry_preserves_silver_and_replays(spark, published, monkeypatch):
    root, config, bronze, original = published
    checkpoint = iqr._destination_checkpoint
    calls = []

    def interrupt_once(folder):
        calls.append(folder)
        if len(calls) == 1:
            with monkeypatch.context() as fault:
                def interrupted(_):
                    raise OSError(11, "injected")
                fault.setattr(iqr.os, "lstat", interrupted)
                checkpoint(folder)
        checkpoint(folder)

    monkeypatch.setattr(iqr, "_destination_checkpoint", interrupt_once)
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "orchestrated", root)
    assert result["status"] == "not_accepted"
    assert result["iqr_attempts"] == 2
    assert result["silver"] == original
    replay = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "orchestrated", root)
    assert replay["iqr_attempts"] == 1
    assert replay["iqr"]["delta_table_id"] == result["iqr"]["delta_table_id"]
    assert DeltaTable.forPath(spark, result["iqr"]["table_path"]).history().count() == 1


@pytest.mark.integration
def test_orchestrated_recovery_of_verified_commit_without_marker(spark, published, monkeypatch):
    root, config, bronze, original = published
    write_json = silver._exclusive_json
    def interrupt_final(path, value):
        if path.name == "completed.json" and path.parent.name == "recover":
            raise OSError("injected")
        return write_json(path, value)
    with monkeypatch.context() as fault:
        fault.setattr(silver, "_exclusive_json", interrupt_final)
        with pytest.raises(OSError):
            iqr.publish_synthetic_iqr(spark, config, "silver", "recover", root)
    folder = root / "iqr-publications" / "recover"
    log = folder / "silver_iqr_limits" / "_delta_log" / "00000000000000000000.json"
    before = log.read_bytes()
    def transient(_):
        raise iqr._RetryCandidate("injected") from OSError(11, "injected")
    monkeypatch.setattr(iqr, "_destination_checkpoint", transient)
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "recover", root)
    assert result["status"] == "not_accepted"
    assert result["iqr_attempts"] == 2 and result["silver"] == original
    assert (folder / "completed.json").is_file() and log.read_bytes() == before


@pytest.mark.integration
def test_orchestrated_recovery_rejects_partial_delta(spark, published, monkeypatch):
    root, config, bronze, original = published
    def partial_write(writer, path=None, *args, **kwargs):
        target = Path(path)
        target.mkdir()
        (target / "partial").write_text("incomplete")
        raise OSError("interrupted")
    with monkeypatch.context() as fault:
        fault.setattr(DataFrameWriter, "save", partial_write)
        with pytest.raises(OSError):
            iqr.publish_synthetic_iqr(spark, config, "silver", "partial", root)
    folder = root / "iqr-publications" / "partial"
    before = iqr._fingerprint(folder)
    def transient(_):
        raise iqr._RetryCandidate("injected") from OSError(11, "injected")
    monkeypatch.setattr(iqr, "_destination_checkpoint", transient)
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "partial", root)
    assert result["status"] == "partial_success" and result["silver"] == original
    assert result["iqr_attempts"] == 1
    assert iqr._fingerprint(folder) == before


@pytest.mark.integration
def test_orchestrated_recovery_of_reservation_without_delta(spark, published, monkeypatch):
    root, config, bronze, original = published
    def interrupted(*args, **kwargs):
        raise OSError("before Delta")
    with monkeypatch.context() as fault:
        fault.setattr(DataFrameWriter, "save", interrupted)
        with pytest.raises(OSError):
            iqr.publish_synthetic_iqr(spark, config, "silver", "reserved", root)
    folder = root / "iqr-publications" / "reserved"
    assert (folder / "intent.json").is_file() and not (folder / "silver_iqr_limits").exists()
    def transient(_):
        raise iqr._RetryCandidate("injected") from OSError(11, "injected")
    monkeypatch.setattr(iqr, "_destination_checkpoint", transient)
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "reserved", root)
    assert result["status"] == "not_accepted" and result["iqr_attempts"] == 2
    assert result["silver"] == original and (folder / "completed.json").is_file()


@pytest.mark.integration
def test_orchestrated_recovery_rejects_changed_silver(spark, published, monkeypatch):
    root, config, bronze, _ = published
    marker = root / "silver-publications" / "silver" / "completed.json"
    def mutate(_):
        marker.write_text('{}')
        raise iqr._RetryCandidate("injected") from OSError(11, "injected")
    monkeypatch.setattr(iqr, "_destination_checkpoint", mutate)
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "changed", root)
    assert result["status"] == "partial_success"
    assert result["iqr_attempts"] == 1
    assert not (root / "iqr-publications" / "changed").exists()


@pytest.mark.integration
def test_recovery_rejects_changed_bronze_manifest(spark, published, monkeypatch):
    root, config, bronze, original = published
    marker = config.namespace / "_runs" / (bronze["run_id"] + ".json")
    calls = []
    publisher = orchestration.publish_synthetic_silver
    def publish_silver(*args, **kwargs):
        calls.append(1)
        return publisher(*args, **kwargs)
    def mutate(_):
        marker.write_bytes(marker.read_bytes() + b" ")
        raise iqr._RetryCandidate("injected") from OSError(11, "injected")
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", publish_silver)
    monkeypatch.setattr(iqr, "_destination_checkpoint", mutate)
    before = iqr._fingerprint(root / "silver-publications" / "silver")
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "changed_bronze", root)
    assert result["status"] == "partial_success" and result["silver"] == original
    assert result["iqr_attempts"] == 1
    assert isinstance(result["error"], iqr.IQRPublicationError)
    assert "Silver intent or Bronze provenance differs" in str(result["error"])
    assert len(calls) == 1 and iqr._fingerprint(root / "silver-publications" / "silver") == before
    assert not (root / "iqr-publications" / "changed_bronze").exists()


@pytest.mark.integration
def test_recovery_rejects_coherent_replacement_silver_identity(spark, published, monkeypatch):
    root, config, bronze, _ = published
    _, replacement_bronze, _ = _create_silver(spark, root, "other")
    namespace = root / "silver-publications"
    calls, fingerprints = [], []
    publisher = orchestration.publish_synthetic_silver
    def publish_silver(*args, **kwargs):
        calls.append(1)
        return publisher(*args, **kwargs)
    def replace_source(_):
        (namespace / "silver").rename(root / "original-silver")
        (namespace / "other").rename(root / "other-silver")
        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
        replacement = silver.publish_synthetic_silver(
            spark, config, replacement_bronze["run_id"], "silver", root)
        identity, _ = iqr._verify_silver(spark, config, root, "silver")
        assert identity["delta_table_id"] == replacement["delta_table_id"]
        fingerprints.append(iqr._fingerprint(namespace / "silver"))
        raise iqr._RetryCandidate("injected") from OSError(11, "injected")
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", publish_silver)
    monkeypatch.setattr(iqr, "_destination_checkpoint", replace_source)
    result = orchestration.publish_synthetic_silver_iqr(
        spark, config, bronze["run_id"], "silver", "replaced", root)
    assert result["status"] == "partial_success" and len(calls) == 1
    assert result["iqr_attempts"] == 1
    assert "identity changed" in str(result["error"])
    assert not (root / "iqr-publications" / "replaced").exists()
    assert (root / "original-silver" / "completed.json").is_file()
    assert iqr._fingerprint(namespace / "silver") == fingerprints[0]


@pytest.mark.integration
def test_concurrent_orchestrators_do_not_duplicate_iqr(spark, published, monkeypatch):
    root, config, bronze, _ = published
    entered, release = Event(), Event()
    checkpoint = iqr._destination_checkpoint
    def hold(folder):
        entered.set()
        if not release.wait(90):
            raise RuntimeError("timed out waiting for competing orchestrator")
        checkpoint(folder)
    monkeypatch.setattr(iqr, "_destination_checkpoint", hold)
    def publish():
        return orchestration.publish_synthetic_silver_iqr(
            spark, config, bronze["run_id"], "silver", "concurrent", root)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(publish)
        try:
            assert entered.wait(90)
            second = executor.submit(publish).result(timeout=90)
        finally:
            release.set()
        first_result = first.result(timeout=90)
    assert second["status"] == "partial_success" and second["iqr_attempts"] == 1
    assert first_result["status"] == "not_accepted"
    assert DeltaTable.forPath(spark, first_result["iqr"]["table_path"]).history().count() == 1


def _log(actions):
    return b"\n".join(iqr._canonical(action).encode() for action in actions)


def _actions():
    return [{"commitInfo": {"timestamp": 1234567890123, "userMetadata": "{}"}},
            {"metaData": {"id": "invented-table", "configuration": {}}},
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}]


@pytest.mark.unit
@pytest.mark.parametrize("case", ["missing_info", "duplicate_info", "missing_time", "null_time",
                                  "bool_time", "string_time", "negative_time", "large_time",
                                  "wrong_metadata", "missing_id", "ict", "ict_feature",
                                  "duplicate_json_field", "invalid_json", "nonfinite_json"])
def test_commit_timestamp_decoder_rejects_ambiguous_and_unsupported_evidence(case):
    actions = _actions()
    if case == "missing_info":
        actions.pop(0)
    elif case == "duplicate_info":
        actions.append(deepcopy(actions[0]))
    elif case == "missing_time":
        del actions[0]["commitInfo"]["timestamp"]
    elif case.endswith("_time"):
        actions[0]["commitInfo"]["timestamp"] = {
            "null_time": None, "bool_time": True, "string_time": "123",
            "negative_time": -1, "large_time": 2**63,
        }[case]
    elif case == "wrong_metadata":
        actions[0]["commitInfo"]["userMetadata"] = "another intent"
    elif case == "missing_id":
        del actions[1]["metaData"]["id"]
    elif case == "ict":
        actions[0]["commitInfo"]["inCommitTimestamp"] = 123
    elif case == "ict_feature":
        actions[2]["protocol"]["writerFeatures"] = ["inCommitTimestamp"]
    raw = _log(actions)
    if case == "duplicate_json_field":
        raw = b'{"commitInfo":{"timestamp":1,"timestamp":2}}'
    elif case == "invalid_json":
        raw = b'{"commitInfo":'
    elif case == "nonfinite_json":
        raw = b'{"commitInfo":{"timestamp":NaN}}'
    with pytest.raises(iqr.IQRPublicationError):
        iqr._commit_info(raw, {})


@pytest.mark.unit
def test_epoch_and_latest_selection_have_no_naming_or_completion_tiebreak():
    assert iqr._commit_info(_log(_actions()), {}) == ("invented-table", 1234567890123)
    old = {"publication_id": "z-last-name", "commit_timestamp_ms": 10}
    new = {"publication_id": "a-first-name", "commit_timestamp_ms": 20, "iqr_accepted": False}
    old["iqr_accepted"] = True
    assert iqr._latest([new, old]) is new
    with pytest.raises(iqr.IQRPublicationError, match="tied"):
        iqr._latest([new, {**old, "commit_timestamp_ms": 20}])
    with pytest.raises(iqr.IQRPublicationError, match="No finalized"):
        iqr._latest([])


@pytest.mark.unit
def test_workspace_identifiers_method_and_local_writer_exclusion(tmp_path):
    raw = tmp_path / "raw"
    config = BronzeConfig(raw, tmp_path / "bronze", raw / "electricity.csv",
                          raw / "metadata.csv", "synthetic", "invented")
    for change in ({"method_version": "not-approved"}, {"publication_id": "../escape"},
                   {"silver_publication_id": "../escape"}):
        arguments = dict(silver_publication_id="silver", publication_id="iqr", workspace=tmp_path)
        arguments.update(change)
        with pytest.raises(iqr.IQRPublicationError):
            iqr.publish_synthetic_iqr(None, config, **arguments)
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        iqr.publish_synthetic_iqr(None, replace(config, data_kind="real"), "silver", "iqr", tmp_path)
    with pytest.raises(silver.SilverPublicationError):
        overlap = tmp_path / "iqr-publications"
        iqr._workspace(tmp_path, replace(config, raw_root=overlap,
                                        electricity=overlap / "electricity.csv",
                                        metadata=overlap / "metadata.csv"))
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(silver.SilverPublicationError):
        iqr._workspace(link, config)
    with iqr._lock(tmp_path):
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            with iqr._lock(tmp_path):
                pytest.fail("A second writer acquired the namespace")
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            iqr.publish_synthetic_iqr(None, config, "silver", "blocked", tmp_path)
    lock = tmp_path / "iqr-publications" / ".publication.lock"
    os.link(lock, tmp_path / "hard-link")
    with pytest.raises(silver.SilverPublicationError, match="Linked"):
        with iqr._lock(tmp_path):
            pytest.fail("Hardlinked lock was accepted")


@pytest.mark.integration
def test_actual_delta_replay_persisted_annotation_and_preservation(spark, published, monkeypatch):
    root, config, bronze, silver_result = published
    folder = root / "silver-publications" / "silver"
    before_silver = iqr._fingerprint(folder)
    before_bronze = {name: iqr._fingerprint(config.table_path(name)) for name in config.sources}
    source = spark.read.format("delta").option("versionAsOf", 0).load(silver_result["table_path"])
    source_rows = source.collect()
    original = Counter(map(tuple, source_rows))
    assert any(row.timestamp_original is None for row in source_rows)
    assert any(row.electricity_value_original is None for row in source_rows)
    assert max(original.values()) > 1
    expected = {r.building_id: r.asDict() for r in summarize_electricity_iqr(source).collect()}
    actual_save = DataFrameWriter.save
    writes = []

    def guard(writer, path=None, *args, **kwargs):
        assert path is not None and str(root / "iqr-publications") in str(path)
        writes.append(str(path))
        return actual_save(writer, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "save", guard)
    first = iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    assert first["output_rows"] == 16 and first["delta_version"] == 0
    assert first["intent"]["method_version"] == IQR_METHOD
    assert first["intent"]["silver"]["delta_table_id"] == silver_result["delta_table_id"]
    assert first["intent"]["silver"]["source_run_id"] == bronze["run_id"]
    assert first["commit_timestamp_ms"] > 0
    same = iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    assert same == first and len(writes) == 1
    assert DeltaTable.forPath(spark, first["table_path"]).history().count() == 1
    state, reread, limits = iqr.read_synthetic_iqr_pair(spark, config, root)
    assert state["iqr_status"] == "not_accepted" and state["warnings"] == ["iqr_not_accepted"]
    assert len(state["unaccepted_buildings"]) == 16
    rows = limits.collect()
    for row in rows:
        assert row._silver_publication_id == "silver" and row._iqr_publication_id == "limits"
        assert {name: row[name] for name in expected[row.building_id]} == expected[row.building_id]
    first_building = next(row for row in rows if row.building_id == APPROVED_BUILDINGS[0])
    assert first_building.iqr_zero_spread and first_building.iqr_outlier_count == 1
    assert first_building.iqr_outlier_percentage == 20 and first_building.off_grid_observation_count == 1
    assert first_building.valid_observation_count > first_building.covered_nominal_hours
    missing = next(row for row in rows if row.building_id == APPROVED_BUILDINGS[-1])
    assert missing.q1 is None and missing.iqr_outlier_count is None
    _, view = iqr.annotate_synthetic_iqr_pair(spark, config, root)
    annotated = view.collect()
    assert view.schema.fields[:len(source.schema)] == source.schema.fields
    assert Counter(tuple(row[:len(source.columns)]) for row in annotated) == original
    assert Counter(map(tuple, reread.collect())) == original
    assert sum(row.is_iqr_outlier is True for row in annotated) > 0
    assert any(row.is_iqr_outlier is None for row in annotated)
    assert {row.iqr_evaluation_status for row in annotated} >= {
        "numeric_not_eligible", "invalid_nominal_timestamp", "outside_study_period", "evaluated",
    }
    assert iqr._fingerprint(folder) == before_silver
    assert {name: iqr._fingerprint(config.table_path(name)) for name in config.sources} == before_bronze
    first["intent"]["parameters"]["multiplier"] = "999"
    first["intent"]["parameters"]["percentiles"].append("0.5")
    assert iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root) == same
    intent_file = root / "iqr-publications" / "limits" / "intent.json"
    original_intent = intent_file.read_bytes()
    for parent in ({}, {**same["intent"]["silver"], "publication_id": "foreign"}):
        altered = deepcopy(same["intent"])
        altered["silver"] = parent
        intent_file.write_text(json.dumps(altered))
        with pytest.raises(iqr.IQRPublicationError, match="identity|intent"):
            iqr.read_synthetic_iqr_pair(spark, config, root)
    intent_file.write_bytes(original_intent)


@pytest.mark.integration
@pytest.mark.parametrize("stage", ["before_reservation", "after_reservation", "before_delta",
                                   "during_delta", "after_commit", "verification",
                                   "before_final", "after_final"])
def test_injected_failures_reconcile_or_block_without_overwrite(spark, published, monkeypatch, stage):
    root, config, _, _ = published
    original_json, original_save = silver._exclusive_json, DataFrameWriter.save

    def interrupted(*args, **kwargs):
        raise OSError("invented interruption")

    def json_failure(path, value):
        if stage == "after_reservation" and path.name == "intent.json":
            original_json(path, value)
            interrupted()
        if stage == "before_final" and path.name == "completed.json":
            interrupted()
        original_json(path, value)
        if stage == "after_final" and path.name == "completed.json":
            interrupted()

    def save_failure(writer, path=None, *args, **kwargs):
        if stage == "during_delta":
            target = Path(path)
            target.mkdir()
            (target / "partial").write_text("invented partial write")
            interrupted()
        if stage == "before_delta":
            interrupted()
        result = original_save(writer, path, *args, **kwargs)
        if stage == "after_commit":
            interrupted()
        return result

    with monkeypatch.context() as fault:
        fault.setattr(silver, "_exclusive_json", json_failure)
        fault.setattr(DataFrameWriter, "save", save_failure)
        if stage == "before_reservation":
            fault.setattr(iqr, "_intent", interrupted)
        if stage == "verification":
            fault.setattr(iqr, "_verify_output", interrupted)
        with pytest.raises(OSError, match="interruption"):
            iqr.publish_synthetic_iqr(spark, config, "silver", "fault", root)
    folder = root / "iqr-publications" / "fault"
    if stage == "before_final":
        with monkeypatch.context() as guard:
            guard.setattr(iqr, "summarize_electricity_iqr",
                          lambda *_: pytest.fail("Pending reading must not run IQR"))
            state, _, limits = iqr.read_synthetic_iqr_pair(spark, config, root)
            assert state["iqr_status"] == "pending" and limits is None
            with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
                iqr.read_synthetic_iqr_pair(spark, config, root,
                                            silver_publication_id="silver", iqr_publication_id="fault")
    if stage == "during_delta":
        before = iqr._fingerprint(folder)
        with pytest.raises((OSError, silver.SilverPublicationError)):
            iqr.publish_synthetic_iqr(spark, config, "silver", "fault", root)
        assert iqr._fingerprint(folder) == before
    else:
        result = iqr.publish_synthetic_iqr(spark, config, "silver", "fault", root)
        assert result["output_rows"] == 16
        assert DeltaTable.forPath(spark, result["table_path"]).history().count() == 1


@pytest.mark.integration
def test_conflicts_corruption_unknown_destinations_and_source_mutation(spark, published):
    root, config, _, result = published
    completed = iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    folder = root / "iqr-publications" / "limits"
    marker = folder / "completed.json"
    good_bytes = marker.read_bytes()
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="method"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root, method_version="other")
    assert iqr._fingerprint(folder) == before
    marker.write_text("{")
    with pytest.raises(iqr.IQRPublicationError):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    marker.write_bytes(good_bytes)
    intent_file = folder / "intent.json"
    intent_bytes = intent_file.read_bytes()
    intent = json.loads(intent_bytes)
    intent["parameters"]["multiplier"] = "2"
    intent_file.write_text(json.dumps(intent))
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    intent_file.write_bytes(intent_bytes)
    unknown = root / "iqr-publications" / "unknown"
    unknown.mkdir()
    other_intent = deepcopy(completed["intent"])
    other_intent["publication_id"] = "unknown"
    (unknown / "intent.json").write_text(json.dumps(other_intent))
    (unknown / "foreign").write_text("preserve")
    with pytest.raises(iqr.IQRPublicationError, match="Unknown|unavailable"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "unknown", root)
    assert (unknown / "foreign").read_text() == "preserve"
    missing = root / "iqr-publications" / "missing_intent" / "silver_iqr_limits"
    missing.mkdir(parents=True)
    (missing / "partial").write_text("preserve partial evidence")
    with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "missing_intent", root)
    assert (missing / "partial").read_text() == "preserve partial evidence"
    limits = spark.read.format("delta").load(completed["table_path"])
    limits.limit(1).write.format("delta").mode("append").save(completed["table_path"])
    with pytest.raises(iqr.IQRPublicationError, match="history"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    source = spark.read.format("delta").load(result["table_path"])
    source.limit(1).write.format("delta").mode("append").save(result["table_path"])
    with pytest.raises(silver.SilverPublicationError, match="history"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    assert json.loads(marker.read_text()) == completed


@pytest.mark.integration
@pytest.mark.parametrize("case", ["missing", "incomplete", "bad_json", "wrong_id", "wrong_version",
                                  "wrong_schema", "wrong_count", "missing_bronze"])
def test_silver_identity_verified_before_iqr_reservation(spark, published, case):
    root, config, bronze, result = published
    marker = root / "silver-publications" / "silver" / "completed.json"
    chosen = "silver"
    if case == "missing":
        chosen = "absent"
    elif case == "incomplete":
        marker.unlink()
    elif case == "bad_json":
        marker.write_text("{")
    elif case == "missing_bronze":
        path = config.namespace / "_runs" / (bronze["run_id"] + ".json")
        report = json.loads(path.read_text())
        del report["tables"][0]["source_sha256"]
        path.write_text(json.dumps(report))
    else:
        value = dict(result)
        key, replacement = {"wrong_id": ("delta_table_id", "wrong"),
                            "wrong_version": ("delta_version", 1),
                            "wrong_schema": ("schema", {}), "wrong_count": ("output_rows", 1)}[case]
        value[key] = replacement
        marker.write_text(json.dumps(value))
    with pytest.raises((silver.SilverPublicationError, SnapshotError)):
        iqr.publish_synthetic_iqr(spark, config, chosen, "blocked", root)
    assert not (root / "iqr-publications" / "blocked").exists()


@pytest.mark.integration
def test_forged_silver_with_correct_count_and_missing_building_is_rejected(spark, published):
    root, config, _, result = published
    original_folder = root / "silver-publications" / "silver"
    before = iqr._fingerprint(original_folder)
    source = spark.read.format("delta").option("versionAsOf", 0).load(result["table_path"])
    # Keep count and schema while replacing one approved building with duplicates.
    forged = source.where(F.col("building_id") != APPROVED_BUILDINGS[-1]).unionByName(
        source.where(F.col("building_id") == APPROVED_BUILDINGS[0]),
    )
    folder = root / "silver-publications" / "forged"
    folder.mkdir()
    intent = json.loads((original_folder / "intent.json").read_text())
    intent["publication_id"] = "forged"
    target = folder / "table"
    (forged.write.format("delta").mode("errorifexists")
     .option("userMetadata", iqr._canonical(intent)).save(str(target)))
    table_id = DeltaTable.forPath(spark, str(target)).detail().select("id").first().id
    completed = {**result, "publication_id": "forged", "table_path": str(target),
                 "delta_table_id": table_id}
    silver._exclusive_json(folder / "intent.json", intent)
    silver._exclusive_json(folder / "completed.json", completed)
    with pytest.raises(silver.SilverPublicationError, match="rows differ"):
        iqr.publish_synthetic_iqr(spark, config, "forged", "blocked", root)
    assert not (root / "iqr-publications" / "blocked").exists()
    assert iqr._fingerprint(original_folder) == before


@pytest.mark.integration
def test_commit_recency_recovery_history_pending_and_latest_unaccepted(spark, tmp_path):
    config, bronze_a, _ = _create_silver(spark, tmp_path, "z_older", interrupt=True)
    folder_a = tmp_path / "silver-publications" / "z_older"
    log_a = folder_a / "table" / "_delta_log" / "00000000000000000000.json"
    original_commit = log_a.read_bytes()
    _, _, newer = _create_silver(spark, tmp_path, "a_newer")
    recovered = silver.publish_synthetic_silver(spark, config, bronze_a["run_id"], "z_older", tmp_path)
    assert log_a.read_bytes() == original_commit
    old_limits = iqr.publish_synthetic_iqr(spark, config, "z_older", "older_limits", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        iqr.publish_synthetic_iqr(spark, config, "a_newer", "older_limits", tmp_path)
    os.utime(log_a, (2000000000, 2000000000))
    native_time = (DeltaTable.forPath(spark, str(folder_a / "table")).history()
                   .select(F.unix_millis("timestamp").alias("epoch_ms")).first().epoch_ms)
    assert native_time == 2000000000000
    state, _, limits = iqr.read_synthetic_iqr_pair(spark, config, tmp_path)
    assert state["silver"]["publication_id"] == "a_newer"
    assert state["silver"]["delta_table_id"] == newer["delta_table_id"]
    assert state["iqr_status"] == "pending" and limits is None
    with pytest.raises(iqr.IQRPublicationError, match="pending"):
        iqr.annotate_synthetic_iqr_pair(spark, config, tmp_path)
    historical, _, _ = iqr.read_synthetic_iqr_pair(
        spark, config, tmp_path, silver_publication_id="z_older", iqr_publication_id="older_limits",
    )
    assert historical["silver"]["delta_table_id"] == recovered["delta_table_id"]
    assert historical["iqr"]["delta_table_id"] == old_limits["delta_table_id"]
    with pytest.raises(iqr.IQRPublicationError, match="different"):
        iqr.read_synthetic_iqr_pair(spark, config, tmp_path,
                                    silver_publication_id="a_newer", iqr_publication_id="older_limits")
    original_finalizer = silver._exclusive_json

    def interrupt_old_limits(path, value):
        if path.parent.name == "z_limits" and path.name == "completed.json":
            raise OSError("invented older IQR interruption")
        return original_finalizer(path, value)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", interrupt_old_limits)
        with pytest.raises(OSError, match="interruption"):
            iqr.publish_synthetic_iqr(spark, config, "a_newer", "z_limits", tmp_path)
    old_iqr_log = (tmp_path / "iqr-publications" / "z_limits" / "silver_iqr_limits"
                   / "_delta_log" / "00000000000000000000.json")
    before_recovery = old_iqr_log.read_bytes()
    latest = iqr.publish_synthetic_iqr(spark, config, "a_newer", "a_limits", tmp_path)
    first = iqr.publish_synthetic_iqr(spark, config, "a_newer", "z_limits", tmp_path)
    assert old_iqr_log.read_bytes() == before_recovery
    assert latest["commit_timestamp_ms"] > first["commit_timestamp_ms"]
    state, _, _ = iqr.read_synthetic_iqr_pair(spark, config, tmp_path)
    assert state["iqr"]["intent"]["publication_id"] == "a_limits"
    assert state["iqr_status"] == "not_accepted" and state["warnings"]
    assert state["silver"]["commit_timestamp_ms"] > historical["silver"]["commit_timestamp_ms"]


@pytest.mark.integration
def test_source_changes_during_calculation_block_finalization(spark, published, monkeypatch):
    root, config, _, _ = published
    original = iqr._verify_output

    def changed(*args):
        result = original(*args)
        (root / "silver-publications" / "silver" / "external_change").write_text("invented change")
        return result

    monkeypatch.setattr(iqr, "_verify_output", changed)
    with pytest.raises(iqr.IQRPublicationError, match="changed"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "changing", root)
    folder = root / "iqr-publications" / "changing"
    assert (folder / "silver_iqr_limits" / "_delta_log").is_dir()
    assert not (folder / "completed.json").exists()

