"""Additional synthetic recovery and integrity scenarios for H06."""
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from threading import Event
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import StructType

from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_diagnostics
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.ingestion.snapshot import SnapshotError
from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
@pytest.mark.parametrize("stage", ["before_reservation", "after_reservation", "before_delta",
                                   "during_delta", "after_commit", "verification", "after_final"])
def test_publication_checkpoints(spark, tmp_path, monkeypatch, stage):
    config, _, _ = _create_silver(spark, tmp_path)
    original_json, original_save = silver._exclusive_json, DataFrameWriter.save
    original_verify = publication._verify_output
    original_build = publication.identity.build_synthetic_contextual_intent

    def stop():
        raise OSError("injected interruption")

    def write_json(path, value):
        original_json(path, value)
        if (stage == "after_reservation" and path.name == "intent.json") or (
                stage == "after_final" and path.name == "completed.json"):
            stop()

    def write_delta(writer, path=None, *args, **kwargs):
        if "contextual-publications" not in str(path):
            return original_save(writer, path, *args, **kwargs)
        if stage == "before_delta":
            stop()
        if stage == "during_delta":
            target = Path(path)
            target.mkdir()
            (target / "partial").write_text("partial synthetic write")
            stop()
        result = original_save(writer, path, *args, **kwargs)
        if stage == "after_commit":
            stop()
        return result

    def build(*args, **kwargs):
        if stage == "before_reservation":
            stop()
        return original_build(*args, **kwargs)

    def verify(*args, **kwargs):
        if stage == "verification":
            stop()
        return original_verify(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", write_json)
        patch.setattr(DataFrameWriter, "save", write_delta)
        patch.setattr(publication.identity, "build_synthetic_contextual_intent", build)
        patch.setattr(publication, "_verify_output", verify)
        with pytest.raises(OSError, match="injected interruption"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    if stage == "after_final":
        marker = folder / "completed.json"
        valid = marker.read_bytes()
        marker.write_text("{")
        before = iqr._fingerprint(folder)
        with pytest.raises(iqr.IQRPublicationError, match="evidence"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
        assert iqr._fingerprint(folder) == before
        marker.write_bytes(valid)
    if stage == "during_delta":
        before = iqr._fingerprint(folder)
        with pytest.raises((OSError, silver.SilverPublicationError)):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
        assert iqr._fingerprint(folder) == before
    else:
        result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
        assert result["delta_version"] == 0
        logs = list((folder / "table" / "_delta_log").glob("*.json"))
        assert len(logs) == 1


@pytest.mark.integration
@pytest.mark.parametrize("mutation", ["extra_file", "commit_timestamp"])
def test_metadata_evidence_mutation_after_reservation(spark, tmp_path, monkeypatch, mutation):
    config, _, _ = _create_silver(spark, tmp_path)
    save = DataFrameWriter.save

    def interrupt(writer, path=None, *args, **kwargs):
        if "contextual-publications" in str(path):
            raise OSError("before contextual commit")
        return save(writer, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "save", interrupt)
        with pytest.raises(OSError, match="before contextual commit"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    if mutation == "extra_file":
        marker = config.table_path("metadata") / "invented-extra-source-evidence"
        marker.write_text("changed after reservation")
    else:
        log = config.table_path("metadata") / "_delta_log" / "00000000000000000000.json"
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        commit = next(action["commitInfo"] for action in actions if "commitInfo" in action)
        commit["timestamp"] += 1
        content = ("\n".join(json.dumps(action, separators=(",", ":")) for action in actions) + "\n")
        filesystem = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        stream = filesystem.create(spark._jvm.org.apache.hadoop.fs.Path(log.as_uri()), True)
        try:
            stream.write(bytearray(content.encode()))
        finally:
            stream.close()
    before = iqr._fingerprint(folder)
    with pytest.raises((SnapshotError, silver.SilverPublicationError, OSError)):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert iqr._fingerprint(folder) == before


@pytest.mark.integration
def test_cooperative_concurrent_contextual_publishers(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    entered, release = Event(), Event()
    original = publication.identity.build_synthetic_contextual_intent

    def pause(*args, **kwargs):
        entered.set()
        assert release.wait(60)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(publication.identity, "build_synthetic_contextual_intent", pause)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(publication.publish_synthetic_contextual,
                                spark, config, "silver", "context", tmp_path)
            assert entered.wait(60)
            second = pool.submit(publication.publish_synthetic_contextual,
                                 spark, config, "silver", "context", tmp_path)
            try:
                with pytest.raises(iqr.IQRPublicationError, match="active"):
                    second.result(timeout=60)
            finally:
                release.set()
            result = first.result(timeout=300)
    logs = list((tmp_path / "contextual-publications" / "context" / "table" / "_delta_log").glob("*.json"))
    assert len(logs) == 1 and result["delta_version"] == 0


@pytest.mark.integration
def test_recovery_checks_silver_identity_and_retained_sources(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    newer = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, newer["run_id"], "other", tmp_path)
    original = DataFrameWriter.save

    def interrupt(writer, path=None, *args, **kwargs):
        if "contextual-publications" in str(path):
            raise OSError("reserved")
        return original(writer, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "save", interrupt)
        with pytest.raises(OSError, match="reserved"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "other", "context", tmp_path)
    source = config.table_path("metadata")
    held = tmp_path / "held-metadata"
    source.rename(held)
    try:
        with pytest.raises((SnapshotError, silver.SilverPublicationError, OSError)):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    finally:
        held.rename(source)
    folder = tmp_path / "silver-publications" / "silver"
    extra = folder / "invented-extra"
    extra.write_text("modified")
    try:
        with pytest.raises(silver.SilverPublicationError):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    finally:
        extra.unlink()
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert result["delta_version"] == 0


@pytest.mark.integration
def test_timezone_resource_change_after_reservation(spark, tmp_path, monkeypatch):
    first, second = Path("/usr/share/zoneinfo/UTC"), Path("/usr/share/zoneinfo/Etc/GMT+1")
    if not first.is_file() or not second.is_file():
        pytest.skip("Synthetic timezone resources unavailable")
    config, _, _ = _create_silver(spark, tmp_path)
    config.metadata.write_text("building_id,timezone\n" +
                               "\n".join(f"{name},UTC" for name in APPROVED_BUILDINGS) + "\n")
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "context_source", tmp_path)
    resource = tmp_path / "timezone-resource"
    first_bytes = first.read_bytes()
    resource.write_bytes(first_bytes)

    def controlled(name):
        if name is None:
            return "null", None, None
        data = resource.read_bytes()
        ZoneInfo.from_file(BytesIO(data), key=name)
        return "valid", "file:" + str(resource), sha256(data).hexdigest()

    monkeypatch.setattr(publication.identity, "_controlled_zone", controlled)
    original = silver._exclusive_json

    def reserve_then_change(path, value):
        original(path, value)
        if path.name == "intent.json" and "contextual-publications" in str(path):
            resource.write_bytes(second.read_bytes())
            raise OSError("timezone changed")

    with monkeypatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", reserve_then_change)
        with pytest.raises(OSError, match="timezone changed"):
            publication.publish_synthetic_contextual(spark, config, "context_source", "context", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "context_source", "context", tmp_path)
    resource.write_bytes(first_bytes)
    result = publication.publish_synthetic_contextual(spark, config, "context_source", "context", tmp_path)
    assert result["delta_version"] == 0


@pytest.mark.integration
def test_exact_output_and_existing_iqr_independence(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    iqr.publish_synthetic_iqr(spark, config, "silver", "limits", tmp_path)
    iqr_before = iqr._fingerprint(tmp_path / "iqr-publications")
    bronze_before = iqr._fingerprint(config.lakehouse_root)
    silver_before = iqr._fingerprint(tmp_path / "silver-publications")
    _, expected = publication.identity.enrich_synthetic_metadata(spark, config, "silver", tmp_path)
    expected_schema = expected.schema.jsonValue()
    expected_rows = expected.collect()

    def forbidden(*args, **kwargs):
        pytest.fail("Contextual publication must not calculate or read IQR or weather")

    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    monkeypatch.setattr(weather_diagnostics, "diagnose_synthetic_weather", forbidden)
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert result["intent"]["schema"] == expected_schema
    actual = spark.read.format("delta").option("versionAsOf", 0).load(result["table_path"])
    normalized = expected_schema.copy()
    normalized["fields"] = [{**field, "nullable": True} for field in expected_schema["fields"]]
    assert actual.schema.jsonValue() == normalized
    captured = spark.createDataFrame(expected_rows, expected.schema)
    assert not captured.exceptAll(actual).limit(1).count()
    assert not actual.exceptAll(captured).limit(1).count()
    electrical = (spark.read.format("delta").option("versionAsOf", 0)
                  .load(result["intent"]["silver"]["table_path"]))
    projected = actual.select(*electrical.columns)
    assert not electrical.exceptAll(projected).limit(1).count()
    assert not projected.exceptAll(electrical).limit(1).count()
    altered = captured.withColumn("metadata_available", ~F.col("metadata_available"))
    with pytest.raises(iqr.IQRPublicationError, match="multiset differs"):
        publication._verify_output(spark, Path(result["table_path"]), altered, result["intent"])
    bad_schema = captured.withColumnRenamed("metadata_available", "unexpected_field")
    with pytest.raises(iqr.IQRPublicationError, match="schema differs"):
        publication._verify_output(spark, Path(result["table_path"]), bad_schema, result["intent"])
    assert iqr._fingerprint(tmp_path / "iqr-publications") == iqr_before
    assert iqr._fingerprint(config.lakehouse_root) == bronze_before
    assert iqr._fingerprint(tmp_path / "silver-publications") == silver_before


@pytest.mark.integration
def test_context_schema_nullability_change_rejected_before_reservation(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    original = publication.identity.enrich_synthetic_metadata
    calls = 0

    def alter_second_view(*args, **kwargs):
        nonlocal calls
        diagnostic, frame = original(*args, **kwargs)
        calls += 1
        if calls == 2:
            schema = frame.schema.jsonValue()
            field = next(field for field in schema["fields"] if not field["nullable"])
            field["nullable"] = True
            frame = spark.createDataFrame(frame.rdd, StructType.fromJson(schema))
        return diagnostic, frame

    monkeypatch.setattr(publication.identity, "enrich_synthetic_metadata", alter_second_view)
    with pytest.raises(iqr.IQRPublicationError, match="schema differs"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert not (tmp_path / "contextual-publications" / "context").exists()
