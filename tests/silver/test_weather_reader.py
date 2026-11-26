"""Read-only weather reader, evidence and independent-process locking checks."""
import fcntl
import ast
import inspect
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import weather_reader as reader


@pytest.fixture
def scenario(tmp_path, monkeypatch):
    publications, captures = tmp_path / "weather-publications", tmp_path / "weather-captures"
    publications.mkdir()
    captures.mkdir()
    (publications / ".capture.lock").touch()
    (captures / ".capture.lock").touch()
    monkeypatch.setattr(reader.publication, "_namespace", lambda *_: (tmp_path, publications))
    monkeypatch.setattr(reader.capture, "_namespace", lambda *_: (tmp_path, captures))
    def expected(pub):
        capture_id = "capture_" + pub
        evidence = {"intent": {"weather_identity": {"publication_id": pub}}}
        intent = {"contract_version": 1, "publication_id": pub, "capture_id": capture_id,
                  "owner_id": "owner", "capture_folder_sha256": "capture-hash", "capture": evidence}
        marker = {"contract_version": 1, "publication_id": pub, "intent": intent, "delta_version": 0}
        return intent, marker, evidence
    for pub in ("older", "newer"):
        intent, marker, _ = expected(pub)
        folder = publications / pub
        folder.mkdir()
        (folder / "table").mkdir()
        (folder / "table" / "file").write_text(pub)
        (folder / "intent.json").write_text(json.dumps(intent))
        (folder / "completed.json").write_text(json.dumps(marker))
    def capture_evidence(_spark, _config, _root, _namespace, capture_id, owner):
        pub = capture_id.removeprefix("capture_")
        return captures / capture_id, expected(pub)[2], "capture-hash"
    monkeypatch.setattr(reader.publication, "_verified_capture", capture_evidence)
    monkeypatch.setattr(reader.publication, "_verify_table", lambda _s, _c, _r, _t, intent, *_: expected(intent["publication_id"])[1])
    monkeypatch.setattr(reader.publication, "_stable_capture", lambda *_: None)
    class Read:
        def format(self, value): assert value == "delta"; return self
        def option(self, key, value): assert (key, value) == ("versionAsOf", 0); return self
        def load(self, path): return path
    return SimpleNamespace(root=tmp_path, publications=publications, captures=captures,
                           spark=SimpleNamespace(read=Read()), expected=expected)


@pytest.mark.unit
def test_explicit_older_read_does_not_write_or_choose_latest(scenario, monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Reader must not publish, recover or use IQR")
    monkeypatch.setattr(reader.publication, "publish_synthetic_weather", forbidden)
    monkeypatch.setattr(reader.iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(reader.iqr, "read_synthetic_iqr_pair", forbidden)
    before = reader.weather_tree_fingerprint(scenario.root)
    marker, path = reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    assert marker == scenario.expected("older")[1] and path.endswith("/older/table")
    assert reader.weather_tree_fingerprint(scenario.root) == before


@pytest.mark.unit
def test_invalid_id_missing_lock_and_incomplete_publication_fail_without_writes(scenario):
    before = reader.weather_tree_fingerprint(scenario.root)
    with pytest.raises(iqr.IQRPublicationError, match="identifier"):
        reader.read_synthetic_weather(scenario.spark, None, "../bad", scenario.root)
    with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
        reader.read_synthetic_weather(scenario.spark, None, "missing", scenario.root)
    assert reader.weather_tree_fingerprint(scenario.root) == before
    (scenario.captures / ".capture.lock").unlink()
    before = reader.weather_tree_fingerprint(scenario.root)
    with pytest.raises(iqr.IQRPublicationError, match="lock is unavailable"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    assert reader.weather_tree_fingerprint(scenario.root) == before
    (scenario.captures / ".capture.lock").touch()
    (scenario.publications / "older" / "completed.json").unlink()
    before = reader.weather_tree_fingerprint(scenario.root)
    with pytest.raises(iqr.IQRPublicationError, match="incomplete"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    assert reader.weather_tree_fingerprint(scenario.root) == before
@pytest.mark.unit
def test_evidence_types_corruption_and_changes_during_read_are_rejected(scenario, monkeypatch):
    folder = scenario.publications / "older"
    marker = folder / "completed.json"
    original = marker.read_text()
    altered = json.loads(original)
    altered["delta_version"] = False
    marker.write_text(json.dumps(altered))
    with pytest.raises(iqr.IQRPublicationError, match="completion evidence differs"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    marker.write_text("{")
    with pytest.raises(iqr.IQRPublicationError, match="evidence"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    marker.write_text(original)
    intent = folder / "intent.json"
    original_intent = intent.read_text()
    altered_intent = json.loads(original_intent)
    altered_intent["contract_version"] = True
    intent.write_text(json.dumps(altered_intent))
    with pytest.raises(iqr.IQRPublicationError, match="identity or contract differs"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    intent.write_text(original_intent)
    original_verify = reader.publication._verify_table
    def modified(*args):
        result = original_verify(*args)
        (folder / "intent.json").write_text((folder / "intent.json").read_text() + " ")
        return result
    monkeypatch.setattr(reader.publication, "_verify_table", modified)
    with pytest.raises(iqr.IQRPublicationError, match="evidence changed"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)


@pytest.mark.unit
def test_reader_rejects_missing_table_and_capture(scenario, monkeypatch):
    folder = scenario.publications / "older"
    table = folder / "table"
    held = scenario.root / "held-table"
    table.rename(held)
    before = reader.weather_tree_fingerprint(scenario.root)
    with pytest.raises(iqr.IQRPublicationError, match="incomplete"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    assert reader.weather_tree_fingerprint(scenario.root) == before
    held.rename(table)
    def absent(*_):
        raise iqr.IQRPublicationError("Capture is unavailable")
    monkeypatch.setattr(reader.publication, "_verified_capture", absent)
    before = reader.weather_tree_fingerprint(scenario.root)
    with pytest.raises(iqr.IQRPublicationError, match="Capture is unavailable"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
    assert reader.weather_tree_fingerprint(scenario.root) == before


@pytest.mark.unit
def test_source_change_during_verification_is_rejected(scenario, monkeypatch):
    source = scenario.root / "synthetic-weather"
    source.mkdir()
    data = source / "part"
    data.write_text("before")
    baseline = reader.weather_tree_fingerprint(source)
    original_verify = reader.publication._verify_table
    def mutate(*args):
        result = original_verify(*args)
        data.write_text("after")
        return result
    def stable(*_args):
        iqr._require(reader.weather_tree_fingerprint(source) == baseline, "Weather source changed")
    monkeypatch.setattr(reader.publication, "_verify_table", mutate)
    monkeypatch.setattr(reader.publication, "_stable_capture", stable)
    with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
        reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)


@pytest.mark.unit
@pytest.mark.parametrize("lock_name", ["weather-publications", "weather-captures"])
def test_independent_writer_process_blocks_reader(scenario, lock_name):
    lock = scenario.root / lock_name / ".capture.lock"
    code = ("import fcntl,sys; f=open(sys.argv[1], 'rb'); "
            "fcntl.flock(f, fcntl.LOCK_EX); print('locked',flush=True); sys.stdin.read()")
    process = subprocess.Popen([sys.executable, "-c", code, str(lock)], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "locked"
        before = reader.weather_tree_fingerprint(scenario.root)
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            reader.read_synthetic_weather(scenario.spark, None, "older", scenario.root)
        assert reader.weather_tree_fingerprint(scenario.root) == before
    finally:
        process.stdin.close()
        assert process.wait(timeout=10) == 0


@pytest.mark.unit
def test_reader_cleanup_failures_preserve_primary(tmp_path, monkeypatch):
    class Handle:
        def close(self): raise OSError("close failed")
    monkeypatch.setattr(reader.silver, "_open_regular", lambda *_: Handle())
    def flock(_, operation):
        if operation == fcntl.LOCK_UN: raise OSError("unlock failed")
    monkeypatch.setattr(reader.fcntl, "flock", flock)
    primary = ValueError("primary")
    with pytest.raises(ValueError) as caught:
        with reader._read_lock(tmp_path):
            raise primary
    assert caught.value is primary
    assert [str(e) for e in primary._h06_cleanup_failures] == ["unlock failed", "close failed"]


@pytest.mark.unit
def test_reader_has_no_writer_or_unbounded_collection_calls():
    tree = ast.parse(inspect.getsource(reader))
    forbidden = {"collect", "toPandas", "toLocalIterator", "mkdir", "write", "save",
                 "_exclusive_json", "publish_synthetic_weather"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in forbidden
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "os":
            assert node.attr != "O_CREAT"
