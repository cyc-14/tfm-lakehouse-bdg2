"""Failure-path resource cleanup tests without a Spark JVM."""
from types import SimpleNamespace

import pytest

from tfm_lakehouse.silver import metadata_diagnostics as metadata
from tfm_lakehouse.silver import silver_iqr_publication as iqr

pytestmark = pytest.mark.unit


class Frame:
    def __init__(self, name, calls, failure=None, persist_failure=None):
        self.name = name
        self.calls = calls
        self.failure = failure
        self.persist_failure = persist_failure
        self.columns = []

    def persist(self, _level):
        if self.persist_failure:
            raise self.persist_failure
        return self

    def count(self):
        return 128

    def unpersist(self):
        self.calls.append(self.name)
        if self.failure:
            raise self.failure


def _calculation(monkeypatch, source, expected):
    monkeypatch.setattr(iqr, "_verify_silver", lambda *a: ({"output_rows": 128}, source))
    monkeypatch.setattr(iqr, "_stable_silver", lambda *a: None)
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *a: expected)
    monkeypatch.setattr(iqr, "_records", lambda *a: [])
    return iqr._calculation(None, None, None, "silver")


def test_calculation_success_releases_both_frames(monkeypatch):
    calls = []
    with _calculation(monkeypatch, Frame("source", calls), Frame("result", calls)):
        pass
    assert calls == ["result", "source"]


def test_operation_error_preserves_identity_traceback_and_existing_cause(monkeypatch):
    calls = []
    primary, cause = ValueError("operation failed"), LookupError("original cause")
    source, result = Frame("source", calls), Frame("result", calls)

    def operation():
        with _calculation(monkeypatch, source, result):
            raise primary from cause

    with pytest.raises(ValueError) as caught:
        operation()
    assert caught.value is primary and caught.value.__cause__ is cause
    assert any(frame.name == "operation" for frame in caught.traceback)
    assert calls == ["result", "source"]


def test_multiple_cleanup_failures_keep_primary_and_full_diagnostics(monkeypatch):
    calls = []
    primary, cause = ValueError("operation failed"), LookupError("original cause")
    source = Frame("source", calls, OSError("source failure"))
    result = Frame("result", calls, RuntimeError("result failure"))

    def operation():
        with _calculation(monkeypatch, source, result):
            raise primary from cause

    with pytest.raises(ValueError) as caught:
        operation()
    assert caught.value is primary and primary.__cause__ is cause
    assert any(frame.name == "operation" for frame in caught.traceback)
    assert calls == ["result", "source"]
    notes = "\n".join(primary.__notes__)
    assert "result failure" in notes and "source failure" in notes
    assert "Traceback" in notes and "unpersist" in notes


def test_control_flow_exception_remains_primary(monkeypatch):
    calls = []
    primary = KeyboardInterrupt("cancelled")
    with pytest.raises(KeyboardInterrupt) as caught:
        with _calculation(monkeypatch, Frame("source", calls, OSError("release failed")),
                          Frame("result", calls)):
            raise primary
    assert caught.value is primary and calls == ["result", "source"]
    assert "release failed" in "\n".join(primary.__notes__)


def test_note_failure_logs_cleanup_errors_and_attempts_both(monkeypatch, caplog):
    class NoteFailure(ValueError):
        def add_note(self, note):
            raise RuntimeError("cannot attach note")

    calls = []
    primary = NoteFailure("operation failed")
    first, second = OSError("result failure"), RuntimeError("source failure")
    with pytest.raises(NoteFailure) as caught:
        with _calculation(monkeypatch, Frame("source", calls, second),
                          Frame("result", calls, first)):
            raise primary
    assert caught.value is primary and calls == ["result", "source"]
    assert [record.exc_info[1] for record in caplog.records] == [first, second]
    assert primary._h06_cleanup_failures == [first, second]


def test_format_failure_logs_original_cleanup_error(monkeypatch, caplog):
    calls = []
    primary, cleanup = ValueError("operation"), OSError("cleanup")
    monkeypatch.setattr(iqr.traceback, "format_exception", lambda *a: 1 / 0)
    with pytest.raises(ValueError) as caught:
        with iqr._frame_cleanup() as frames:
            frames.append(("frame", Frame("frame", calls, cleanup)))
            raise primary
    assert caught.value is primary and calls == ["frame"]
    assert caplog.records[0].exc_info[1] is cleanup


def test_exception_forbidding_cleanup_attribute_keeps_notes_and_releases(monkeypatch):
    class RestrictedError(ValueError):
        @property
        def _h06_cleanup_failures(self):
            return ()

    calls = []
    primary = RestrictedError("operation")
    with pytest.raises(RestrictedError) as caught:
        with _calculation(monkeypatch, Frame("source", calls, OSError("source")),
                          Frame("result", calls, OSError("result"))):
            raise primary
    assert caught.value is primary and calls == ["result", "source"]
    assert "source" in "\n".join(primary.__notes__)
    assert "result" in "\n".join(primary.__notes__)


def test_success_cleanup_failure_inside_unrelated_exception_is_not_suppressed():
    calls = []
    try:
        raise ValueError("unrelated")
    except ValueError:
        with pytest.raises(OSError, match="release failed"):
            with iqr._frame_cleanup() as frames:
                frames.append(("frame", Frame("frame", calls, OSError("release failed"))))
    assert calls == ["frame"]


def test_success_then_multiple_cleanup_failures_propagates_first(monkeypatch):
    calls = []
    first, second = OSError("first cleanup"), RuntimeError("second cleanup")
    with pytest.raises(OSError) as caught:
        with _calculation(monkeypatch, Frame("source", calls, second), Frame("result", calls, first)):
            pass
    assert caught.value is first and calls == ["result", "source"]
    assert "second cleanup" in "\n".join(first.__notes__)
    assert any(frame.name == "unpersist" for frame in caught.traceback)


@pytest.mark.parametrize("failed", ["source", "result"])
def test_persist_failure_still_attempts_cleanup(monkeypatch, failed):
    calls = []
    primary = RuntimeError("persist failed")
    source = Frame("source", calls, persist_failure=primary if failed == "source" else None)
    result = Frame("result", calls, persist_failure=primary if failed == "result" else None)
    with pytest.raises(RuntimeError) as caught:
        with _calculation(monkeypatch, source, result):
            pytest.fail("persist must fail")
    assert caught.value is primary
    assert calls == (["source"] if failed == "source" else ["result", "source"])


def _metadata_setup(monkeypatch, tmp_path, frame):
    item = {"name": "metadata", "source_uri": "synthetic", "source_sha256": "hash",
            "delta_version": 0}
    report = {"tables": [item], "status": "passed", "run_id": "run"}
    parent = {"source_run_id": "run", "manifest_sha256": "digest", "source_tables": []}
    config = SimpleNamespace(namespace=tmp_path, data_kind="synthetic", dataset_reference="invented",
                             table_path=lambda _: tmp_path / "metadata")
    monkeypatch.setattr(iqr, "_workspace", lambda *a: tmp_path)
    monkeypatch.setattr(iqr, "_verify_silver", lambda *a: (parent, None))
    monkeypatch.setattr(iqr, "_fingerprint", lambda *a: "files")
    monkeypatch.setattr(iqr.silver, "_manifest", lambda *a: (report, "digest"))
    monkeypatch.setattr(iqr.silver, "_table_identity", lambda *a: [])
    monkeypatch.setattr(metadata, "_same_configuration", lambda *a: True)
    monkeypatch.setattr(metadata, "_validate_tables", lambda *a: None)
    monkeypatch.setattr(metadata, "_read_verified_tables", lambda *a: {"metadata": frame})
    return config


def test_metadata_operation_error_survives_cleanup_failure(monkeypatch, tmp_path):
    calls = []
    frame = Frame("metadata", calls, OSError("metadata cleanup failure"))
    config = _metadata_setup(monkeypatch, tmp_path, frame)
    with pytest.raises(iqr.IQRPublicationError, match="lineage is missing") as caught:
        metadata.diagnose_synthetic_metadata(None, config, "silver", tmp_path)
    assert calls == ["metadata"]
    assert "metadata cleanup failure" in "\n".join(caught.value.__notes__)
    assert any(frame.name == "diagnose_synthetic_metadata" for frame in caught.traceback)


def test_metadata_failed_persist_attempts_cleanup_preserving_primary(monkeypatch, tmp_path):
    calls = []
    primary = RuntimeError("metadata persist failed")
    frame = Frame("metadata", calls, OSError("metadata cleanup failure"), primary)
    config = _metadata_setup(monkeypatch, tmp_path, frame)
    with pytest.raises(RuntimeError) as caught:
        metadata.diagnose_synthetic_metadata(None, config, "silver", tmp_path)
    assert caught.value is primary and calls == ["metadata"]
    assert "metadata cleanup failure" in "\n".join(primary.__notes__)
