"""Synthetic-only Silver publication, recovery and path checks."""
import json
import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.silver_electricity import _LINEAGE_COLUMNS
from tfm_lakehouse.silver_electricity_integrated import transform_selected_electricity
from tfm_lakehouse.silver_synthetic_publication import (
    SilverPublicationError, _validate_workspace, publish_synthetic_silver,
)


@pytest.fixture
def config_and_manifest(tmp_path):
    raw, lake = tmp_path / "raw", tmp_path / "lake"
    cfg = BronzeConfig(raw, lake, raw / "electricity.csv", raw / "metadata.csv", "synthetic", "fixture")
    report_dir = cfg.namespace / "_runs"
    report_dir.mkdir(parents=True)
    report = {"tables": [
        {"name": "electricity", "table_path": str(cfg.table_path("electricity")),
         "delta_version": 2, "source_sha256": "a" * 64, "output_rows": 2},
        {"name": "metadata", "table_path": str(cfg.table_path("metadata")),
         "delta_version": 3, "source_sha256": "b" * 64, "output_rows": 1}]}
    path = report_dir / "run1.json"
    path.write_text(json.dumps(report))
    return cfg, report, path


@pytest.mark.unit
def test_workspace_rejects_real_external_symlink_and_bidirectional_overlap(config_and_manifest, tmp_path):
    cfg, _, _ = config_and_manifest
    assert _validate_workspace(tmp_path, cfg) == tmp_path
    with pytest.raises(SilverPublicationError):
        _validate_workspace(tmp_path, replace(cfg, data_kind="real"))
    with pytest.raises(SilverPublicationError):
        _validate_workspace(tmp_path, replace(cfg, raw_root=Path("/not-in-workspace")))
    link = tmp_path / "linked"
    link.symlink_to(tmp_path / "raw", target_is_directory=True)
    with pytest.raises(SilverPublicationError):
        _validate_workspace(tmp_path, replace(cfg, raw_root=link))
    for lake in (tmp_path / "silver-publications", tmp_path / "silver-publications" / "bronze"):
        with pytest.raises(SilverPublicationError):
            _validate_workspace(tmp_path, replace(cfg, lakehouse_root=lake))


@pytest.mark.unit
def test_rejects_unsupported_ids_and_transform_before_source_access(config_and_manifest, tmp_path):
    cfg, _, _ = config_and_manifest
    for run, publication, version in (("../x", "ok", "h05.3-v1"), ("run1", "../x", "h05.3-v1"),
                                      ("run1", "ok", "h05.3-v2")):
        with pytest.raises(SilverPublicationError):
            publish_synthetic_silver(None, cfg, run, publication, tmp_path, transform_version=version)
    assert not (tmp_path / "silver-publications").exists()


@pytest.mark.unit
def test_link_inside_bronze_tree_blocks_before_reader(config_and_manifest, tmp_path, monkeypatch):
    import tfm_lakehouse.silver_synthetic_publication as publication
    cfg, _, _ = config_and_manifest
    nested = cfg.table_path("electricity") / "_delta_log"
    nested.mkdir(parents=True)
    (nested / "linked").symlink_to(tmp_path / "outside")
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity",
                        lambda *args: pytest.fail("reader must not run"))
    with pytest.raises(SilverPublicationError, match="Linked"):
        publish_synthetic_silver(None, cfg, "run1", "publication1", tmp_path)


@pytest.mark.unit
@pytest.mark.parametrize("name", ["manifest", "lock"])
def test_hardlinked_manifest_or_lock_rejected_before_reader(config_and_manifest, tmp_path, monkeypatch, name):
    import tfm_lakehouse.silver_synthetic_publication as publication
    cfg, _, report = config_and_manifest
    extra = tmp_path / "other"
    if name == "manifest":
        os.link(report, extra)
    else:
        namespace = tmp_path / "silver-publications"
        namespace.mkdir()
        os.link(report, namespace / ".publication.lock")
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity",
                        lambda *args: pytest.fail("reader must not run"))
    with pytest.raises(SilverPublicationError, match="Linked"):
        publish_synthetic_silver(None, cfg, "run1", "publication1", tmp_path)


@pytest.mark.unit
def test_parsed_manifest_identity_mismatch_stops_before_write(config_and_manifest, tmp_path, monkeypatch):
    import tfm_lakehouse.silver_synthetic_publication as publication
    cfg, _, _ = config_and_manifest
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity", lambda *args: (
        {"snapshot_binding": "h02_version_metadata_schema_count_checked",
         "manifest_sha256": "wrong", "source_tables": []}, None))
    with pytest.raises(SilverPublicationError, match="binding"):
        publish_synthetic_silver(None, cfg, "run1", "publication1", tmp_path)
    assert not (tmp_path / "silver-publications" / "publication1").exists()


def synthetic_silver(spark):
    names = ("timestamp", *APPROVED_BUILDINGS, *_LINEAGE_COLUMNS)
    schema = StructType([StructField(name, StringType(), True) for name in names])
    values = [None, "bad", "-1.5", "0"] + ["1.25"] * 12
    bronze = spark.createDataFrame([("2017-11-05 01:30:00", *values, "uri", "hash", "synthetic", "fixture")] * 2, schema)
    selection = {"run_id": "run1", "tables": [{"name": "electricity", "delta_version": 2,
                                                  "table_path": "/synthetic/electricity"}]}
    return transform_selected_electricity(bronze, selection=selection)


def fake_assessment(path, report):
    tables = [{key: item[key] for key in ("name", "table_path", "delta_version", "source_sha256")}
              for item in report["tables"]]
    return {"snapshot_binding": "h02_version_metadata_schema_count_checked",
            "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source_tables": sorted(tables, key=lambda item: item["name"])}


@pytest.mark.integration
def test_delta_commit_retry_recovery_and_conflicts(spark, config_and_manifest, tmp_path, monkeypatch):
    import tfm_lakehouse.silver_synthetic_publication as publication
    cfg, report, report_path = config_and_manifest
    frame = synthetic_silver(spark)
    calls = []
    def fake_bound(*args):
        calls.append(args[2])
        return fake_assessment(report_path, report), frame
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity", fake_bound)
    actual_json = publication._exclusive_json
    def interrupt_final(path, value):
        if path.name == "completed.json":
            raise OSError("synthetic interruption")
        return actual_json(path, value)
    monkeypatch.setattr(publication, "_exclusive_json", interrupt_final)
    with pytest.raises(OSError, match="interruption"):
        publish_synthetic_silver(spark, cfg, "run1", "publication1", tmp_path)
    target = tmp_path / "silver-publications" / "publication1" / "table"
    assert target.exists()
    assert not (target.parent / "completed.json").exists()
    monkeypatch.setattr(publication, "_exclusive_json", actual_json)
    result = publish_synthetic_silver(spark, cfg, "run1", "publication1", tmp_path)
    assert result["status"] == "passed" and result["output_rows"] == 32 and result["delta_version"] == 0
    assert result["delta_table_id"]
    persisted = spark.read.format("delta").option("versionAsOf", 0).load(str(target)).collect()
    assert len(persisted) == 32
    assert sum(row.electricity_value_original is None and row.is_missing for row in persisted) == 2
    assert sum(row.electricity_value_original == "bad" and row.value_parse_status == "invalid_syntax"
               for row in persisted) == 2
    assert sum(row.electricity_value_original == "-1.5" and row.is_negative for row in persisted) == 2
    same = publish_synthetic_silver(spark, cfg, "run1", "publication1", tmp_path)
    assert same == result
    with pytest.raises(SilverPublicationError, match="another Silver ID"):
        publish_synthetic_silver(spark, cfg, "run1", "publication2", tmp_path)
    report["tables"][0]["delta_version"] = 99
    report_path.write_text(json.dumps(report))
    with pytest.raises(SilverPublicationError, match="conflicts"):
        publish_synthetic_silver(spark, cfg, "run1", "publication1", tmp_path)
    assert calls == ["run1"] * 5


@pytest.mark.integration
def test_write_failures_and_unknown_target_stop_without_cleanup(spark, config_and_manifest,
                                                                 tmp_path, monkeypatch):
    import tfm_lakehouse.silver_synthetic_publication as publication
    cfg, report, report_path = config_and_manifest
    frame = synthetic_silver(spark)
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity",
                        lambda *args: (fake_assessment(report_path, report), frame))
    original = publication._exclusive_json
    def fail_before(path, value):
        if path.name == "intent.json":
            raise OSError("before write")
        return original(path, value)
    monkeypatch.setattr(publication, "_exclusive_json", fail_before)
    with pytest.raises(OSError, match="before write"):
        publish_synthetic_silver(spark, cfg, "run1", "before", tmp_path)
    monkeypatch.setattr(publication, "_exclusive_json", original)
    with pytest.raises(SilverPublicationError, match="evidence"):
        publish_synthetic_silver(spark, cfg, "run1", "before", tmp_path)


@pytest.mark.integration
def test_interrupted_write_leaves_partial_target_and_retry_fails_closed(spark, config_and_manifest,
                                                                        tmp_path, monkeypatch):
    import tfm_lakehouse.silver_synthetic_publication as publication
    from pyspark.sql.readwriter import DataFrameWriter
    cfg, report, report_path = config_and_manifest
    frame = synthetic_silver(spark)
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity",
                        lambda *args: (fake_assessment(report_path, report), frame))
    original_save = DataFrameWriter.save
    def fail_write(self, path, *args, **kwargs):
        Path(path).mkdir()
        raise OSError("partial write")
    monkeypatch.setattr(DataFrameWriter, "save", fail_write)
    with pytest.raises(OSError, match="partial write"):
        publish_synthetic_silver(spark, cfg, "run1", "partial", tmp_path)
    monkeypatch.setattr(DataFrameWriter, "save", original_save)
    with pytest.raises(SilverPublicationError, match="incomplete"):
        publish_synthetic_silver(spark, cfg, "run1", "partial", tmp_path)
    assert (tmp_path / "silver-publications" / "partial" / "table").is_dir()


@pytest.mark.integration
def test_unrelated_target_is_not_modified(spark, config_and_manifest, tmp_path, monkeypatch):
    import tfm_lakehouse.silver_synthetic_publication as publication
    cfg, report, report_path = config_and_manifest
    frame = synthetic_silver(spark)
    monkeypatch.setattr(publication, "transform_published_synthetic_electricity",
                        lambda *args: (fake_assessment(report_path, report), frame))
    folder = tmp_path / "silver-publications" / "unknown"
    folder.mkdir(parents=True)
    marker = folder / "foreign"
    marker.write_text("preserve")
    with pytest.raises(SilverPublicationError):
        publish_synthetic_silver(spark, cfg, "run1", "unknown", tmp_path)
    assert marker.read_text() == "preserve"


@pytest.mark.integration
def test_actual_synthetic_bronze_to_silver_binding(spark, tmp_path):
    from tfm_lakehouse.ingestion.bronze import run_bronze
    raw = tmp_path / "raw"
    raw.mkdir()
    electricity, metadata = raw / "electricity.csv", raw / "metadata.csv"
    row = "2017-01-01 00:00:00," + ",".join(["-2.5", "0", "bad", "NaN"] + ["1"] * 12) + "\n"
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n" + row + row)
    metadata.write_text("building_id\n" + "\n".join(APPROVED_BUILDINGS) + "\n")
    cfg = BronzeConfig(raw, tmp_path / "lake", electricity, metadata, "synthetic", "fixture")
    bronze = run_bronze(spark, cfg)
    result = publish_synthetic_silver(spark, cfg, bronze["run_id"], "integrated", tmp_path)
    assert result["input_rows"] == 2 and result["output_rows"] == 32
    assert result["source_run_id"] == bronze["run_id"]
    actual = spark.read.format("delta").option("versionAsOf", result["delta_version"]).load(result["table_path"])
    assert actual.count() == 32
