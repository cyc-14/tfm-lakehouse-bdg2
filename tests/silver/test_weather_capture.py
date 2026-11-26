"""Synthetic capture reservation, isolation and cleanup checks."""
import fcntl
import ast
import inspect
from types import SimpleNamespace

import pytest

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import weather_capture as capture


@pytest.mark.unit
def test_namespace_rejects_overlapping_paths_and_links(tmp_path, monkeypatch):
    monkeypatch.setattr(capture.iqr, "_workspace", lambda *_: tmp_path)
    config = SimpleNamespace(raw_root=tmp_path / "raw", lakehouse_root=tmp_path / "bronze")
    assert capture._namespace(tmp_path, config)[1] == tmp_path / "weather-captures"
    config.raw_root = tmp_path / "weather-captures" / "raw"
    with pytest.raises(iqr.IQRPublicationError, match="overlaps"):
        capture._namespace(tmp_path, config)
    config.raw_root = tmp_path / "raw"
    (tmp_path / "weather-publications").symlink_to(tmp_path / "other")
    with pytest.raises(Exception):
        capture._namespace(tmp_path, config)


@pytest.mark.unit
def test_reservation_conflict_and_cooperative_lock(tmp_path, monkeypatch):
    namespace = tmp_path / "weather-captures"
    monkeypatch.setattr(capture, "_namespace", lambda *_: (tmp_path, namespace))
    namespace.mkdir()
    (namespace / "existing").mkdir()
    with pytest.raises(iqr.IQRPublicationError, match="already exists"):
        capture.create_synthetic_weather_capture(None, None, "s", "c", "p", "existing", "owner", tmp_path)
    with capture._lock(namespace):
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            with capture._lock(namespace):
                pass
    with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
        capture.verify_synthetic_weather_capture(None, None, "absent", "owner", tmp_path)


@pytest.mark.unit
def test_lock_cleanup_failure_preserves_primary_exception(tmp_path, monkeypatch):
    original = capture.fcntl.flock
    def failing_unlock(handle, operation):
        if operation == fcntl.LOCK_UN:
            raise OSError("synthetic unlock failure")
        return original(handle, operation)
    monkeypatch.setattr(capture.fcntl, "flock", failing_unlock)
    primary = ValueError("primary")
    with pytest.raises(ValueError) as caught:
        with capture._lock(tmp_path / "namespace"):
            raise primary
    assert caught.value is primary
    assert any("synthetic unlock failure" in note for note in primary.__notes__)


@pytest.mark.unit
def test_multiple_cleanup_failures_keep_primary_even_when_notes_fail(tmp_path, monkeypatch):
    class BrokenNotes(ValueError):
        def add_note(self, note):
            raise RuntimeError("notes unavailable")
    class Handle:
        def close(self):
            raise OSError("close failed")
    monkeypatch.setattr(capture.silver, "_open_regular", lambda *_: Handle())
    def flock(_, operation):
        if operation == fcntl.LOCK_UN:
            raise OSError("unlock failed")
    monkeypatch.setattr(capture.fcntl, "flock", flock)
    primary = BrokenNotes("primary")
    with pytest.raises(BrokenNotes) as caught:
        with capture._lock(tmp_path / "namespace"):
            raise primary
    assert caught.value is primary
    assert [str(error) for error in primary._h06_cleanup_failures] == ["unlock failed", "close failed"]
    with pytest.raises(OSError, match="unlock failed") as caught:
        with capture._lock(tmp_path / "other"):
            pass
    assert [str(error) for error in caught.value._h06_cleanup_failures] == ["close failed"]


@pytest.mark.unit
def test_interrupted_write_retains_reservation_without_completion(tmp_path, monkeypatch):
    namespace = tmp_path / "weather-captures"
    monkeypatch.setattr(capture, "_namespace", lambda *_: (tmp_path, namespace))
    class Writer:
        def format(self, *_): return self
        def mode(self, *_): return self
        def option(self, *_): return self
        def save(self, path):
            from pathlib import Path
            partial = Path(path)
            partial.mkdir()
            (partial / "partial").write_text("retained")
            raise OSError("interrupted write")
    frame = SimpleNamespace(write=Writer())
    monkeypatch.setattr(capture.weather_identity, "build_synthetic_weather_identity", lambda *_: ({}, frame))
    with pytest.raises(OSError, match="interrupted write"):
        capture.create_synthetic_weather_capture(None, None, "s", "c", "p", "broken", "owner", tmp_path)
    folder = namespace / "broken"
    assert (folder / "intent.json").is_file() and (folder / "table" / "partial").is_file()
    assert not (folder / "completed.json").exists()
    with pytest.raises(iqr.IQRPublicationError, match="incomplete"):
        capture.verify_synthetic_weather_capture(None, None, "broken", "owner", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="already exists"):
        capture.create_synthetic_weather_capture(None, None, "s", "c", "p", "broken", "owner", tmp_path)


@pytest.mark.unit
def test_post_commit_failure_never_writes_completion(tmp_path, monkeypatch):
    namespace = tmp_path / "weather-captures"
    monkeypatch.setattr(capture, "_namespace", lambda *_: (tmp_path, namespace))
    class Writer:
        def format(self, *_): return self
        def mode(self, *_): return self
        def option(self, *_): return self
        def save(self, path):
            from pathlib import Path
            Path(path).mkdir()
    parent = {"weather_files_sha256": "tree", "silver": {}}
    monkeypatch.setattr(capture.weather_identity, "build_synthetic_weather_identity",
                        lambda *_: (parent, SimpleNamespace(write=Writer())))
    monkeypatch.setattr(capture, "weather_tree_fingerprint", lambda *_: "tree")
    monkeypatch.setattr(capture, "_verify", lambda *_: {"physical_tree_sha256": "tree"})
    def fail(*_):
        raise OSError("source changed")
    monkeypatch.setattr(capture.iqr, "_stable_silver", fail)
    config = SimpleNamespace(table_path=lambda _: tmp_path)
    with pytest.raises(OSError, match="source changed"):
        capture.create_synthetic_weather_capture(None, config, "s", "c", "p", "committed", "owner", tmp_path)
    assert (namespace / "committed" / "table").is_dir()
    assert not (namespace / "committed" / "completed.json").exists()


@pytest.mark.unit
def test_real_source_tree_change_during_materialization_blocks_completion(tmp_path, monkeypatch):
    from pathlib import Path
    namespace = tmp_path / "weather-captures"
    source = tmp_path / "weather-source"
    source.mkdir()
    (source / "part").write_text("before")
    source_hash = capture.weather_tree_fingerprint(source)
    monkeypatch.setattr(capture, "_namespace", lambda *_: (tmp_path, namespace))
    class Writer:
        def format(self, *_): return self
        def mode(self, *_): return self
        def option(self, *_): return self
        def save(self, path):
            Path(path).mkdir()
            (source / "part").write_text("after")
    parent = {"weather_files_sha256": source_hash, "silver": {}}
    monkeypatch.setattr(capture.weather_identity, "build_synthetic_weather_identity",
                        lambda *_: (parent, SimpleNamespace(write=Writer())))
    monkeypatch.setattr(capture, "_verify", lambda _spark, _config, _root, folder, _intent:
                        {"physical_tree_sha256": capture.weather_tree_fingerprint(folder / "table")})
    config = SimpleNamespace(table_path=lambda _: source)
    with pytest.raises(iqr.IQRPublicationError, match="Weather source changed"):
        capture.create_synthetic_weather_capture(None, config, "s", "c", "p", "changed", "owner", tmp_path)
    assert not (namespace / "changed" / "completed.json").exists()


@pytest.mark.unit
def test_commit_line_is_bounded(tmp_path):
    log = tmp_path / "_delta_log"
    log.mkdir()
    (log / "00000000000000000000.json").write_bytes(b"x" * (capture._MAX_COMMIT_LINE + 1))
    with pytest.raises(iqr.IQRPublicationError, match="exceeds limit"):
        capture._commit(None, tmp_path, {})


@pytest.mark.unit
def test_capture_has_no_unbounded_row_collection():
    tree = ast.parse(inspect.getsource(capture))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        assert node.func.attr not in ("toPandas", "toLocalIterator")
        if node.func.attr == "collect":
            limited = node.func.value
            assert isinstance(limited, ast.Call) and isinstance(limited.func, ast.Attribute)
            assert limited.func.attr == "limit" and isinstance(limited.args[0], ast.Constant)
            assert limited.args[0].value == 2
