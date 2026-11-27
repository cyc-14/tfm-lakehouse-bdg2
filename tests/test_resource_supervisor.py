"""Synthetic checks for partial D44 supervision primitives."""
import os
from pathlib import Path
import signal
import pytest
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace


@pytest.fixture
def campaign(tmp_path, monkeypatch):
    root = tmp_path / 'campaign'
    root.mkdir()
    (root / 'temp').mkdir()
    (root / 'evidence').mkdir()
    monkeypatch.setattr(workspace, 'validate_authorized_campaign', lambda value: value.root)
    return workspace.Campaign(root, 'campaign', 'owner')


def test_preflight_rejects_foreign_before_workspace_inspection(campaign, monkeypatch):
    monkeypatch.setattr(workspace, 'validate_authorized_campaign', lambda *_: pytest.fail('inspected'))
    with pytest.raises(supervisor.SupervisorError, match='Unauthorized'):
        supervisor.preflight(campaign, Path('/invented-protected/pytest-foreign'))


def test_preflight_requires_new_base_and_rejects_dangling_link(campaign):
    target = campaign.root / 'temp' / 'pytest-new'
    assert supervisor.preflight(campaign, target) == campaign.root
    target.symlink_to(campaign.root / 'missing')
    with pytest.raises(supervisor.SupervisorError, match='already exists'):
        supervisor.preflight(campaign, target)


def test_evidence_is_exclusive_and_preserved(campaign):
    folder = supervisor.reserve_evidence(campaign, 'run-1')
    before = (folder / 'reservation.json').read_bytes()
    with pytest.raises(supervisor.SupervisorError, match='reservation'):
        supervisor.reserve_evidence(campaign, 'run-1')
    assert (folder / 'reservation.json').read_bytes() == before


def test_identity_and_signaling_fail_closed(monkeypatch):
    identity = supervisor.read_process_identity(os.getpid())
    assert identity.pid == os.getpid() and identity.start_ticks > 0
    monkeypatch.setattr(os, 'kill', lambda *_: pytest.fail('signal sent'))
    with pytest.raises(supervisor.SupervisorError, match='no signal sent'):
        supervisor.signal_verified_child(identity, signal.SIGTERM)
