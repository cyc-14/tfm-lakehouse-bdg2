"""Synthetic publication state machine and bounded-driver checks."""
import ast
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import weather_publication as publication


@pytest.fixture
def simulated(tmp_path, monkeypatch):
    namespace, captures = tmp_path / "weather-publications", tmp_path / "weather-captures"
    captures.mkdir()
    source = captures / "capture"
    source.mkdir()
    monkeypatch.setattr(publication, "_namespace", lambda *_: (tmp_path, namespace))
    monkeypatch.setattr(publication.capture, "_namespace", lambda *_: (tmp_path, captures))
    evidence = {"intent": {"weather_identity": {"silver": {}, "weather_files_sha256": "x",
                                                 "publication_id": "pub"}}}
    monkeypatch.setattr(publication, "_verified_capture", lambda *_: (source, evidence, "capture-hash"))
    monkeypatch.setattr(publication, "_stable_capture", lambda *_: None)
    writes = []
    class Writer:
        def format(self, *_): return self
        def mode(self, *_): return self
        def option(self, *_): return self
        def save(self, path):
            writes.append(path)
            Path(path).mkdir()
            (Path(path) / "valid").write_text("delta")
    class Reader:
        def format(self, *_): return self
        def option(self, *_): return self
        def load(self, *_): return SimpleNamespace(write=Writer())
    spark = SimpleNamespace(read=Reader())
    def verify(_spark, _config, _root, table, intent, *_):
        iqr._require((table / "valid").is_file(), "Partial publication table")
        return {"contract_version": 1, "publication_id": intent["publication_id"], "intent": intent,
                "delta_table_id": "uuid", "delta_version": 0}
    monkeypatch.setattr(publication, "_verify_table", verify)
    return SimpleNamespace(root=tmp_path, namespace=namespace, spark=spark, writes=writes)


@pytest.mark.unit
def test_commit_without_marker_recovers_without_second_write(simulated, monkeypatch):
    original = publication.silver._exclusive_json
    def interrupted(path, value):
        if path.name == "completed.json":
            raise OSError("interrupted before completion")
        return original(path, value)
    with monkeypatch.context() as fault:
        fault.setattr(publication.silver, "_exclusive_json", interrupted)
        with pytest.raises(OSError, match="interrupted"):
            publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    folder = simulated.namespace / "pub"
    assert (folder / "table" / "valid").is_file() and not (folder / "completed.json").exists()
    first = publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    second = publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    assert first == second and len(simulated.writes) == 1
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_weather(simulated.spark, None, "capture", "other", "pub", simulated.root)


@pytest.mark.unit
def test_partial_table_and_incompatible_evidence_fail_closed(simulated, monkeypatch):
    original = publication.silver._exclusive_json
    def stop_before_write(path, value):
        if path.name == "intent.json":
            original(path, value)
            raise OSError("interrupted after reservation")
        return original(path, value)
    with monkeypatch.context() as fault:
        fault.setattr(publication.silver, "_exclusive_json", stop_before_write)
        with pytest.raises(OSError, match="reservation"):
            publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    result = publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    assert result["delta_version"] == 0 and len(simulated.writes) == 1
    folder = simulated.namespace / "pub"
    (folder / "extra").write_text("retain")
    with pytest.raises(iqr.IQRPublicationError, match="unknown"):
        publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    assert (folder / "extra").read_text() == "retain"


@pytest.mark.unit
def test_publication_path_and_driver_collection_are_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(publication.iqr, "_workspace", lambda *_: tmp_path)
    config = SimpleNamespace(raw_root=tmp_path / "raw", lakehouse_root=tmp_path / "bronze")
    assert publication._namespace(tmp_path, config)[1] == tmp_path / "weather-publications"
    config.raw_root = tmp_path / "weather-publications" / "raw"
    with pytest.raises(iqr.IQRPublicationError, match="overlaps"):
        publication._namespace(tmp_path, config)
    tree = ast.parse(inspect.getsource(publication))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in ("collect", "toPandas", "toLocalIterator")


@pytest.mark.unit
def test_rejects_publication_id_different_from_capture_intention(simulated):
    with pytest.raises(iqr.IQRPublicationError, match="ID differs"):
        publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "other", simulated.root)
    assert not (simulated.namespace / "other").exists()


@pytest.mark.unit
def test_partial_table_is_never_repaired(simulated, monkeypatch):
    class BrokenWriter:
        def format(self, *_): return self
        def mode(self, *_): return self
        def option(self, *_): return self
        def save(self, path):
            Path(path).mkdir()
            (Path(path) / "partial").write_text("retained")
            raise OSError("partial write")
    class Reader:
        def format(self, *_): return self
        def option(self, *_): return self
        def load(self, *_): return SimpleNamespace(write=BrokenWriter())
    spark = SimpleNamespace(read=Reader())
    with pytest.raises(OSError, match="partial write"):
        publication.publish_synthetic_weather(spark, None, "capture", "owner", "pub", simulated.root)
    partial = simulated.namespace / "pub" / "table" / "partial"
    with pytest.raises(iqr.IQRPublicationError, match="Partial publication table"):
        publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    assert partial.read_text() == "retained" and not simulated.writes


@pytest.mark.unit
def test_type_sensitive_marker(simulated):
    import json
    result = publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
    marker = simulated.namespace / "pub" / "completed.json"
    forged = dict(result)
    forged["delta_version"] = False
    marker.write_text(json.dumps(forged))
    with pytest.raises(iqr.IQRPublicationError, match="completion evidence differs"):
        publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)


@pytest.mark.unit
def test_capture_mutation_rejected_before_other_sources(tmp_path):
    source = tmp_path / "actual-capture"
    source.mkdir()
    (source / "data").write_text("original")
    before = publication.weather_tree_fingerprint(source)
    (source / "data").write_text("changed")
    with pytest.raises(iqr.IQRPublicationError, match="Capture evidence changed"):
        publication._stable_capture(None, None, tmp_path, source, before, {})


@pytest.mark.unit
def test_changed_publication_table_identity_rejected(simulated, monkeypatch):
    original = publication.publish_synthetic_weather(
        simulated.spark, None, "capture", "owner", "pub", simulated.root)
    monkeypatch.setattr(publication, "_verify_table", lambda *_: {**original, "delta_table_id": "substituted"})
    with pytest.raises(iqr.IQRPublicationError, match="completion evidence differs"):
        publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)


@pytest.mark.unit
def test_cooperative_concurrent_publication_is_rejected(simulated):
    with publication.capture._lock(simulated.namespace):
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            publication.publish_synthetic_weather(simulated.spark, None, "capture", "owner", "pub", simulated.root)
