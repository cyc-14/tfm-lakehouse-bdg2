"""Evidence mutation tests for explicit contextual reading."""
import json

import pytest
from pyspark.sql import functions as F

from tfm_lakehouse.ingestion.snapshot import SnapshotError
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
def test_missing_evidence_and_concurrent_writer_do_not_modify_namespace(spark, tmp_path):
    config, bronze, _ = _create_silver(spark, tmp_path)
    publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    namespace = tmp_path / "contextual-publications"
    folder = namespace / "context"
    with publication._lock(tmp_path):
        before = iqr._fingerprint(namespace)
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(namespace) == before
    for name in ("intent.json", "completed.json", "table"):
        source, held = folder / name, tmp_path / ("held-" + name)
        source.rename(held)
        try:
            before = iqr._fingerprint(namespace)
            with pytest.raises((OSError, silver.SilverPublicationError)):
                reader.read_synthetic_contextual(spark, config, "context", tmp_path)
            assert iqr._fingerprint(namespace) == before
        finally:
            held.rename(source)
    manifest = config.namespace / "_runs" / (bronze["run_id"] + ".json")
    original = manifest.read_bytes()
    try:
        manifest.write_bytes(original + b" ")
        before = iqr._fingerprint(namespace)
        with pytest.raises((SnapshotError, silver.SilverPublicationError)):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(namespace) == before
    finally:
        manifest.write_bytes(original)
    source, held = config.table_path("metadata"), tmp_path / "held-metadata"
    source.rename(held)
    try:
        before = iqr._fingerprint(namespace)
        with pytest.raises((SnapshotError, OSError, silver.SilverPublicationError)):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(namespace) == before
    finally:
        held.rename(source)


@pytest.mark.integration
def test_corrupt_commit_schema_and_rows_are_rejected(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    namespace = tmp_path / "contextual-publications"
    folder = namespace / "context"
    table = folder / "table"
    log = table / "_delta_log" / "00000000000000000000.json"
    original = log.read_bytes()
    try:
        log.write_bytes(original + b'{"commitInfo":{}}\n')
        before = iqr._fingerprint(namespace)
        with pytest.raises(iqr.IQRPublicationError, match="ambiguous"):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(namespace) == before
    finally:
        log.write_bytes(original)
    frame = spark.read.format("delta").option("versionAsOf", 0).load(str(table))
    intent = result["intent"]
    variants = (frame.drop("metadata_available"),
                frame.withColumn("metadata_available", ~F.col("metadata_available")))
    for index, variant in enumerate(variants):
        alternative = tmp_path / f"alternative-{index}"
        (variant.write.format("delta").mode("errorifexists")
         .option("userMetadata", iqr._canonical(intent)).save(str(alternative)))
        held = tmp_path / f"held-table-{index}"
        table.rename(held)
        alternative.rename(table)
        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
        try:
            before = iqr._fingerprint(namespace)
            with pytest.raises(iqr.IQRPublicationError, match="schema differs|multiset differs"):
                reader.read_synthetic_contextual(spark, config, "context", tmp_path)
            assert iqr._fingerprint(namespace) == before
        finally:
            table.rename(alternative)
            held.rename(table)
            spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()


@pytest.mark.integration
def test_type_sensitive_marker_silver_identity_and_existing_iqr(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    iqr.publish_synthetic_iqr(spark, config, "silver", "limits", tmp_path)
    publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    namespace = tmp_path / "contextual-publications"
    iqr_before = iqr._fingerprint(tmp_path / "iqr-publications")
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR read"))
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *a, **k: pytest.fail("IQR recalculation"))
    result, frame = reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert result["output_rows"] == frame.count() == 128
    assert iqr._fingerprint(tmp_path / "iqr-publications") == iqr_before
    marker = namespace / "context" / "completed.json"
    original = marker.read_bytes()
    altered = json.loads(original)
    altered["delta_version"] = False
    try:
        marker.write_text(json.dumps(altered))
        before = iqr._fingerprint(namespace)
        with pytest.raises(iqr.IQRPublicationError, match="completion"):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(namespace) == before
    finally:
        marker.write_bytes(original)
    silver_marker = tmp_path / "silver-publications" / "silver" / "completed.json"
    original = silver_marker.read_bytes()
    altered = json.loads(original)
    altered["delta_table_id"] = "invented-different-id"
    try:
        silver_marker.write_text(json.dumps(altered))
        before = iqr._fingerprint(namespace)
        with pytest.raises(silver.SilverPublicationError):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(namespace) == before
    finally:
        silver_marker.write_bytes(original)
