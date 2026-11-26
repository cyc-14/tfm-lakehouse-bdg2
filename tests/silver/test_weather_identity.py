"""Synthetic physical tree and preliminary identity evidence checks."""
from copy import deepcopy
import sys
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import weather_identity as identity
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint


@pytest.mark.unit
def test_tree_fingerprint_matches_d24_and_detects_physical_changes(tmp_path):
    root = tmp_path / "weather"
    log = root / "_delta_log"
    log.mkdir(parents=True)
    commit = log / "00000000000000000000.json"
    commit.write_bytes(b"commit-0")
    data = root / "part-0.parquet"
    data.write_bytes(b"A" * (1024 * 1024 + 13))
    original = weather_tree_fingerprint(root)
    assert original == iqr._fingerprint(root)
    data.write_bytes(b"B")
    assert weather_tree_fingerprint(root) != original
    data.write_bytes(b"A" * (1024 * 1024 + 13))
    extra = root / "extra"
    extra.write_bytes(b"x")
    assert weather_tree_fingerprint(root) != original
    extra.unlink()
    renamed = root / "renamed"
    data.rename(renamed)
    assert weather_tree_fingerprint(root) != original
    renamed.rename(data)
    commit.write_bytes(b"commit-altered")
    assert weather_tree_fingerprint(root) != original
    commit.unlink()
    assert weather_tree_fingerprint(root) != original
    data.unlink()
    assert weather_tree_fingerprint(root) != original


@pytest.mark.unit
def test_fingerprint_rejects_missing_or_linked_files(tmp_path):
    with pytest.raises(iqr.IQRPublicationError):
        weather_tree_fingerprint(tmp_path / "missing")
    root = tmp_path / "root"
    root.mkdir()
    (root / "link").symlink_to(tmp_path / "outside")
    with pytest.raises(Exception):
        weather_tree_fingerprint(root)


@pytest.mark.unit
def test_invalid_id_and_type_sensitive_identity(monkeypatch, tmp_path):
    with pytest.raises(iqr.IQRPublicationError, match="identifier"):
        identity.build_synthetic_weather_identity(None, None, "silver", "context", "../bad", tmp_path)
    expected = {"contract_version": 1, "inclusive": True, "schema": {"fields": []},
                "silver": {"publication_id": "silver"}, "contextual": {"id": "context"},
                "manifest_sha256": "manifest", "weather_delta_version": 0}
    monkeypatch.setattr(identity.iqr, "_workspace", lambda *_: tmp_path)
    monkeypatch.setattr(identity, "build_synthetic_weather_identity", lambda *_: (expected, None))
    for field, value in (("contract_version", True), ("inclusive", 1),
                         ("association_policy", "changed"), ("contract_version", 2),
                         ("silver", {"publication_id": "other"}), ("contextual", {"id": "other"}),
                         ("manifest_sha256", "changed"), ("weather_delta_version", 1)):
        changed = deepcopy(expected)
        changed[field] = value
        with pytest.raises(iqr.IQRPublicationError, match="identity differs"):
            identity.verify_synthetic_weather_identity(None, None, "silver", "context", "weather", tmp_path, changed, None)


@pytest.mark.integration
def test_exact_multiset_reconciliation_and_schema(monkeypatch, tmp_path):
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[2]").appName("weather-identity-test")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        reference = spark.createDataFrame([("a", "1"), ("a", "1"), ("b", "2")], "key string, value string")
        expected = {"schema": reference.schema.jsonValue(), "weather_files_sha256": "tree",
                    "silver": {}, "contextual": {"version": 2}}
        monkeypatch.setattr(identity, "build_synthetic_weather_identity", lambda *_: (expected, reference))
        monkeypatch.setattr(identity.iqr, "_workspace", lambda *_: tmp_path)
        monkeypatch.setattr(identity, "weather_tree_fingerprint", lambda *_: "tree")
        monkeypatch.setattr(identity.iqr, "_stable_silver", lambda *_: None)
        monkeypatch.setattr(identity.contextual_reader, "read_synthetic_contextual",
                            lambda *_: ({"version": 2}, None))
        config = SimpleNamespace(table_path=lambda _: tmp_path)
        for zone, partitions in (("UTC", 1), ("America/New_York", 3)):
            spark.conf.set("spark.sql.session.timeZone", zone)
            result = identity.verify_synthetic_weather_identity(
                spark, config, "silver", "context", "weather", tmp_path, expected,
                reference.repartition(partitions))
            assert result["status"] == "reconciled_for_this_action"
            assert result["durable_content_verified"] is False
        bad = spark.createDataFrame([("a", "1"), ("b", "2"), ("b", "2")], reference.schema)
        with pytest.raises(iqr.IQRPublicationError, match="multiset differs"):
            identity.verify_synthetic_weather_identity(spark, config, "silver", "context", "weather", tmp_path, expected, bad)
        with pytest.raises(iqr.IQRPublicationError, match="schema differs"):
            identity.verify_synthetic_weather_identity(spark, config, "silver", "context", "weather", tmp_path,
                                                       expected, reference.withColumnRenamed("value", "other"))
        monkeypatch.setattr(identity, "weather_tree_fingerprint", lambda *_: "changed")
        with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
            identity.verify_synthetic_weather_identity(spark, config, "silver", "context", "weather", tmp_path,
                                                       expected, reference)
        source = tmp_path / "source"
        source.mkdir()
        (source / "part").write_bytes(b"before")
        expected["weather_files_sha256"] = weather_tree_fingerprint(source)
        config.table_path = lambda _: source
        monkeypatch.setattr(identity, "weather_tree_fingerprint", weather_tree_fingerprint)
        monkeypatch.setattr(identity.iqr, "_stable_silver", lambda *_: (source / "part").write_bytes(b"after"))
        with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
            identity.verify_synthetic_weather_identity(spark, config, "silver", "context", "weather", tmp_path,
                                                       expected, reference)
    finally:
        spark.stop()
