"""Synthetic H05.4 publication binding; fake history does not prove Delta authenticity."""
import json
from types import SimpleNamespace

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion import snapshot
from tfm_lakehouse.ingestion.snapshot import SnapshotError
from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS
from tfm_lakehouse.silver.silver_snapshot_binding import transform_published_synthetic_electricity


@pytest.fixture
def publication(tmp_path):
    raw = tmp_path / "raw"
    cfg = BronzeConfig(raw, tmp_path / "lake", raw / "electricity.csv", raw / "metadata.csv",
                       "synthetic", "fixture-v1")
    names = ("timestamp", *APPROVED_BUILDINGS, *_LINEAGE_COLUMNS)
    electric_schema = StructType([StructField(name, StringType(), True) for name in names])
    metadata_schema = StructType([StructField("building_id", StringType(), True)])
    tables = []
    for name, source in cfg.sources.items():
        tables.append({"name": name, "source_uri": source.resolve().as_uri(),
                       "source_sha256": "a" * 64 if name == "electricity" else "b" * 64,
                       "table_path": str(cfg.table_path(name)), "status": "verified",
                       "delta_version": 3 if name == "electricity" else 9,
                       "output_rows": 2 if name == "electricity" else 1,
                       "output_schema": (electric_schema if name == "electricity" else metadata_schema).jsonValue()})
    report = {"report_contract_version": 1, "run_id": "chosen", "status": "passed",
              "data_kind": "synthetic", "dataset_reference": "fixture-v1",
              "configured_sources": {name: source.resolve().as_uri() for name, source in cfg.sources.items()},
              "tables": tables}
    directory = cfg.namespace / "_runs"
    directory.mkdir(parents=True)
    path = directory / "chosen.json"
    path.write_text(json.dumps(report))
    return cfg, report, path


@pytest.mark.unit
@pytest.mark.parametrize("case", ["missing", "failed", "wrong_run", "incompatible", "incomplete",
                                  "missing_hash", "bad_hash", "bad_schema", "bad_count", "zero_count",
                                  "source_mismatch", "escape"])
def test_explicit_publication_rejects_invalid_inputs_without_fallback(publication, monkeypatch, case):
    import tfm_lakehouse.silver.silver_snapshot_binding as binding
    cfg, report, path = publication
    if case == "missing":
        path.unlink()
    elif case == "failed":
        report["status"] = "failed"
    elif case == "wrong_run":
        report["run_id"] = "other"
    elif case == "incompatible":
        report["dataset_reference"] = "different"
    elif case == "incomplete":
        report["tables"].pop()
    elif case == "missing_hash":
        del report["tables"][0]["source_sha256"]
    elif case == "bad_hash":
        report["tables"][0]["source_sha256"] = "invalid"
    elif case == "bad_schema":
        report["tables"][0]["output_schema"] = None
    elif case == "bad_count":
        report["tables"][0]["output_rows"] = True
    elif case == "zero_count":
        report["tables"][0]["output_rows"] = 0
    elif case == "source_mismatch":
        report["tables"][0]["source_uri"] = "file:///different-source.csv"
    if case != "missing":
        path.write_text(json.dumps(report))
    monkeypatch.setattr(binding, "_read_verified_tables", lambda *args: pytest.fail("unexpected read"))
    with pytest.raises(SnapshotError):
        transform_published_synthetic_electricity(object(), cfg, "../chosen" if case == "escape" else "chosen")


@pytest.mark.unit
def test_real_configuration_and_caller_frame_cannot_bypass_binding(publication):
    from dataclasses import replace
    cfg, _, _ = publication
    with pytest.raises(SnapshotError, match="synthetic"):
        transform_published_synthetic_electricity(object(), replace(cfg, data_kind="real"), "chosen")
    with pytest.raises(TypeError):
        transform_published_synthetic_electricity(object(), cfg, "chosen", selection={"run_id": "fake"})


class FakeColumn:
    def __eq__(self, other):
        return other


def fake_history_and_reader(monkeypatch, cfg, report, frames, mode=None):
    calls = []
    items = {item["name"]: item for item in report["tables"]}

    class History:
        def __init__(self, name):
            self.name = name
        def history(self):
            return self
        def where(self, version):
            calls.append(("history", self.name, version))
            self.version = version
            return self
        def select(self, _):
            return self
        def limit(self, _):
            return self
        def collect(self):
            item = items[self.name]
            if self.version != item["delta_version"] or mode == "wrong_version":
                return []
            metadata = json.dumps({"run_id": report["run_id"], "table_name": self.name,
                "source_sha256": item["source_sha256"], "data_kind": cfg.data_kind,
                "dataset_reference": cfg.dataset_reference}, sort_keys=True)
            if mode == "wrong_commit":
                metadata = "substituted"
            return [{"userMetadata": metadata}]

    class Reader:
        def format(self, name):
            assert name == "delta"
            return self
        def option(self, name, version):
            assert name == "versionAsOf"
            self.version = version
            return self
        def load(self, path):
            name = next(n for n in items if str(cfg.table_path(n)) == path)
            calls.append(("load", name, self.version))
            if mode == "unavailable" or self.version != items[name]["delta_version"]:
                raise OSError("synthetic version unavailable")
            return frames[name]

    monkeypatch.setattr(snapshot, "DeltaTable", SimpleNamespace(
        forPath=lambda _, path: History(next(n for n in items if str(cfg.table_path(n)) == path))))
    monkeypatch.setattr(snapshot, "F", SimpleNamespace(col=lambda _: FakeColumn(), lit=lambda x: x))
    return SimpleNamespace(read=Reader()), calls


@pytest.mark.integration
def test_explicit_binding_pins_each_version_and_preserves_silver(spark, publication, monkeypatch):
    cfg, report, path = publication
    schema = StructType.fromJson(report["tables"][0]["output_schema"])
    values = [None, "bad", "-2.5", "0"] + ["1"] * 12
    source = report["tables"][0]
    wide = spark.createDataFrame([("2017-11-05 01:30:00", *values, source["source_uri"],
                                   source["source_sha256"], "synthetic", "fixture-v1")] * 2, schema)
    metadata = spark.createDataFrame([("example",)], StructType.fromJson(report["tables"][1]["output_schema"]))
    fake, calls = fake_history_and_reader(monkeypatch, cfg, report, {"electricity": wide, "metadata": metadata})
    assessment, silver = transform_published_synthetic_electricity(fake, cfg, "chosen")
    rows = silver.collect()
    assert len(rows) == 32
    assert all(r._bronze_snapshot_run_id == "chosen" and r._bronze_delta_version == 3 for r in rows)
    assert sum(r.electricity_value_original is None and r.value_parse_status == "missing_null" for r in rows) == 2
    assert sum(r.electricity_value_original == "bad" and r.value_parse_status == "invalid_syntax" for r in rows) == 2
    assert all(r.timestamp_parse_status == "valid_nominal" for r in rows)
    assert ("load", "electricity", 3) in calls and ("load", "metadata", 9) in calls
    assert assessment["source_authenticity"] == "not_established"
    assert assessment["input_lineage"] == "matched_publication_at_check"
    assert assessment["transformation_correctness"] == "not_verified_for_this_input"
    assert assessment["spark_evaluation"] == "lazy_after_h02_read_checks"


@pytest.mark.integration
@pytest.mark.parametrize("case", ["wrong_commit", "wrong_version", "unavailable", "wrong_schema", "missing_lineage"])
def test_binding_rejects_version_and_schema_failures(spark, publication, monkeypatch, case):
    cfg, report, path = publication
    electric_schema = StructType.fromJson(report["tables"][0]["output_schema"])
    if case == "missing_lineage":
        electric_schema = StructType(electric_schema.fields[:-1])
        report["tables"][0]["output_schema"] = electric_schema.jsonValue()
        path.write_text(json.dumps(report))
    elif case == "wrong_schema":
        electric_schema = StructType(electric_schema.fields[:-1])
    values = [None] * len(electric_schema)
    wide = spark.createDataFrame([tuple(values)] * 2, electric_schema)
    metadata = spark.createDataFrame([("example",)], StructType.fromJson(report["tables"][1]["output_schema"]))
    fake, calls = fake_history_and_reader(monkeypatch, cfg, report, {"electricity": wide, "metadata": metadata}, case)
    with pytest.raises((SnapshotError, ValueError)):
        transform_published_synthetic_electricity(fake, cfg, "chosen")
    assert all(c[2] in (3, 9) for c in calls)


@pytest.mark.integration
@pytest.mark.parametrize("field,value", [(name, value) for name in _LINEAGE_COLUMNS
                                         for value in (None, "incorrect")])
def test_binding_rejects_mismatched_or_null_lineage(spark, publication, monkeypatch, field, value):
    cfg, report, _ = publication
    schema = StructType.fromJson(report["tables"][0]["output_schema"])
    item = report["tables"][0]
    record = ["2017-01-01 00:00:00", *(["1"] * 16), item["source_uri"],
              item["source_sha256"], "synthetic", "fixture-v1"]
    record[schema.fieldNames().index(field)] = value
    wide = spark.createDataFrame([tuple(record)] * 2, schema)
    metadata = spark.createDataFrame([("example",)], StructType.fromJson(report["tables"][1]["output_schema"]))
    fake, _ = fake_history_and_reader(monkeypatch, cfg, report, {"electricity": wide, "metadata": metadata})
    with pytest.raises(SnapshotError, match="lineage"):
        transform_published_synthetic_electricity(fake, cfg, "chosen")
