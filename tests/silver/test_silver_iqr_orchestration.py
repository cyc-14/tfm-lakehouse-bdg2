"""Synthetic orchestration and conservative retry checks."""
import errno
from contextlib import contextmanager
from pathlib import Path

import pytest

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_iqr_orchestration as orchestration


@pytest.mark.unit
@pytest.mark.parametrize("number", [errno.EINTR, errno.EAGAIN])
def test_only_instrumented_transient_destination_error_is_classified(monkeypatch, number):
    def fail(_):
        raise OSError(number, "interrupted")
    monkeypatch.setattr(iqr.os, "lstat", fail)
    with pytest.raises(iqr._RetryCandidate) as raised:
        iqr._destination_checkpoint(Path("/tmp/invented"))
    assert raised.value.__cause__.errno == number


@pytest.mark.unit
def test_unknown_destination_error_is_not_retryable(monkeypatch):
    def fail(_):
        raise OSError(errno.EIO, "failure")
    monkeypatch.setattr(iqr.os, "lstat", fail)
    with pytest.raises(OSError):
        iqr._destination_checkpoint(Path("/tmp/invented"))


@pytest.mark.unit
def test_silver_failure_prevents_iqr(monkeypatch):
    def fail(*args):
        raise ValueError("failure")
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", fail)
    result = orchestration.publish_synthetic_silver_iqr(None, None, "run", "silver", "iqr", None)
    assert result["status"] == "technical_failure"
    assert result["iqr_attempts"] == 0


@pytest.mark.unit
@pytest.mark.parametrize("failure", [iqr.IQRPublicationError("conflict"), OSError("unknown")])
def test_unknown_and_integrity_failures_do_not_retry(monkeypatch, failure):
    calls = []
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", lambda *a: {"source_run_id": "run"})
    monkeypatch.setattr(iqr, "_workspace", lambda *a: None)
    monkeypatch.setattr(iqr, "_verify_silver", lambda *a: ({"source_run_id": "run"}, None))
    def publish(*args, **kwargs):
        calls.append(kwargs)
        raise failure
    monkeypatch.setattr(iqr, "_orchestrated_iqr", publish)
    result = orchestration.publish_synthetic_silver_iqr(None, None, "run", "silver", "iqr", None)
    assert result["status"] == "partial_success"
    assert result["error"] is failure
    assert len(calls) == 1


@pytest.mark.unit
def test_failed_retry_exhausts_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", lambda *a: {"source_run_id": "run"})
    monkeypatch.setattr(iqr, "_workspace", lambda *a: None)
    monkeypatch.setattr(iqr, "_verify_silver", lambda *a: ({"source_run_id": "run"}, None))
    def publish(*args, **kwargs):
        calls.append(kwargs)
        if kwargs.get("recover"):
            kwargs["on_recovery_start"]()
        raise iqr._RetryCandidate("injected")
    monkeypatch.setattr(iqr, "_orchestrated_iqr", publish)
    result = orchestration.publish_synthetic_silver_iqr(None, None, "run", "silver", "iqr", None)
    assert result["status"] == "partial_success"
    assert result["iqr_attempts"] == len(calls) == 2


@pytest.mark.unit
def test_rejected_recovery_preflight_is_not_a_second_attempt(monkeypatch):
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", lambda *a: {"source_run_id": "run"})
    monkeypatch.setattr(iqr, "_workspace", lambda *a: None)
    monkeypatch.setattr(iqr, "_verify_silver", lambda *a: ({"source_run_id": "run"}, None))
    def publish(*args, **kwargs):
        if kwargs.get("recover"):
            raise iqr.IQRPublicationError("changed source")
        raise iqr._RetryCandidate("transient")
    monkeypatch.setattr(iqr, "_orchestrated_iqr", publish)
    result = orchestration.publish_synthetic_silver_iqr(None, None, "run", "silver", "iqr", None)
    assert result["status"] == "partial_success" and result["iqr_attempts"] == 1


@pytest.mark.unit
@pytest.mark.parametrize("pair_status, expected", [("available", "success"),
                                                   ("not_accepted", "not_accepted")])
def test_completed_pair_status_does_not_retry(monkeypatch, pair_status, expected):
    calls = []
    identity = {"source_run_id": "run"}
    monkeypatch.setattr(orchestration, "publish_synthetic_silver", lambda *a: identity)
    monkeypatch.setattr(iqr, "_workspace", lambda *a: None)
    monkeypatch.setattr(iqr, "_verify_silver", lambda *a: (identity, None))
    def publish(*args, **kwargs):
        calls.append(kwargs)
        return {"publication_id": "iqr"}, True
    monkeypatch.setattr(iqr, "_orchestrated_iqr", publish)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **kw: (
        {"silver": identity, "iqr_status": pair_status}, None, None))
    result = orchestration.publish_synthetic_silver_iqr(None, None, "run", "silver", "iqr", None)
    assert result["status"] == expected
    assert result["iqr_attempts"] == len(calls) == 1


@pytest.mark.unit
def test_recovery_checks_reservation_and_publishes_under_same_lock(tmp_path, monkeypatch):
    folder = tmp_path / "iqr-publications" / "limits"
    folder.mkdir(parents=True)
    (folder / "intent.json").write_text('{"identity":"original"}')
    locked = []

    @contextmanager
    def lock(_):
        locked.append(True)
        try:
            yield folder.parent
        finally:
            locked.pop()

    @contextmanager
    def calculation(*args):
        assert locked
        yield {"identity": "original"}, None, None, None

    def publish(*args, **kwargs):
        assert locked
        return {"published": True}

    monkeypatch.setattr(iqr, "_lock", lock)
    monkeypatch.setattr(iqr, "_calculation", calculation)
    monkeypatch.setattr(iqr, "_intent", lambda *args: {"identity": "original"})
    monkeypatch.setattr(iqr, "_publish_locked", publish)
    starts = []
    result, wrote = iqr._orchestrated_iqr(
        None, None, tmp_path, "silver", "limits", {"identity": "original"}, recover=True,
        on_recovery_start=lambda: starts.append(bool(locked)))
    assert wrote and result == {"published": True} and not locked
    assert starts == [True]
    (folder / "intent.json").write_text('{"identity":')
    with pytest.raises(iqr.IQRPublicationError):
        iqr._orchestrated_iqr(
            None, None, tmp_path, "silver", "limits", {"identity": "original"}, recover=True)
    assert not locked

    with pytest.raises(iqr.IQRPublicationError, match="identity changed"):
        iqr._orchestrated_iqr(
            None, None, tmp_path, "silver", "limits", {"identity": "changed"}, recover=True)


@pytest.mark.unit
def test_recovery_lock_contention_fails_closed(tmp_path):
    with iqr._lock(tmp_path):
        with pytest.raises(iqr.IQRPublicationError):
            iqr._orchestrated_iqr(None, None, tmp_path, "silver", "limits", {}, recover=True)


@pytest.mark.unit
def test_completed_publication_is_verified_without_republication(tmp_path, monkeypatch):
    folder = tmp_path / "limits"
    folder.mkdir()
    (folder / "intent.json").write_text('{}')
    (folder / "completed.json").write_text('{"status":"passed"}')
    (folder / "silver_iqr_limits").mkdir()
    @contextmanager
    def lock(_):
        yield tmp_path
    @contextmanager
    def calculation(*args):
        yield {}, None, None, None
    monkeypatch.setattr(iqr, "_lock", lock)
    monkeypatch.setattr(iqr, "_calculation", calculation)
    monkeypatch.setattr(iqr, "_intent", lambda *a: {})
    monkeypatch.setattr(iqr, "_publication_frame", lambda *a: None)
    monkeypatch.setattr(iqr, "_verify_output", lambda *a: ({"status": "passed"}, None))
    monkeypatch.setattr(iqr, "_stable_silver", lambda *a: None)
    monkeypatch.setattr(iqr, "_publish_locked", lambda *a, **kw: pytest.fail("republished"))
    starts = []
    result, wrote = iqr._orchestrated_iqr(None, None, tmp_path, "silver", "limits", {}, recover=True,
                                         on_recovery_start=lambda: starts.append(True))
    assert result == {"status": "passed"} and not wrote and not starts
