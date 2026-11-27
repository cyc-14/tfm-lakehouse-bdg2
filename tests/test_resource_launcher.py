"""Real, synthetic child handshake without Spark or cleanup."""
import os
import uuid
from pathlib import Path
import pytest
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace


def test_real_synthetic_campaign_probe_and_exclusive_claim():
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    base = campaign.root / 'temp' / ('pytest-' + uuid.uuid4().hex)
    result = supervisor.launch_probe(campaign, base, 'probe-' + uuid.uuid4().hex)
    assert result['status'] == 'probe_passed'
    assert result['identity']['pid'] == result['identity']['session_id']
    assert not os.path.lexists(base)
    with pytest.raises(supervisor.SupervisorError, match='already claimed'):
        supervisor.launch_probe(campaign, base, 'probe-' + uuid.uuid4().hex)


def test_foreign_base_rejected_without_inspecting_campaign(monkeypatch):
    campaign = workspace.Campaign(Path('/invented'), 'x', 'owner')
    monkeypatch.setattr(workspace, 'validate_authorized_campaign', lambda *_: pytest.fail('inspected'))
    with pytest.raises(supervisor.SupervisorError, match='Unauthorized'):
        supervisor.preflight(campaign, Path('/other/pytest-x'))


def test_concurrent_claims_admit_at_most_one():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    base = campaign.root / 'temp' / ('pytest-' + uuid.uuid4().hex)
    barrier = Barrier(2)
    def run():
        barrier.wait(timeout=10)
        try:
            return supervisor.launch_probe(campaign, base, 'probe-' + uuid.uuid4().hex)['status']
        except supervisor.SupervisorError:
            return 'rejected'
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: run(), range(2)))
    assert sorted(results) == ['probe_passed', 'rejected']


def test_launch_failure_preserves_reservation(monkeypatch):
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    base = campaign.root / 'temp' / ('pytest-' + uuid.uuid4().hex)
    run_id = 'probe-' + uuid.uuid4().hex
    original = supervisor.subprocess.Popen
    def fail(*args, **kwargs):
        if kwargs.get('start_new_session'):
            raise OSError('synthetic startup failure')
        return original(*args, **kwargs)
    monkeypatch.setattr(supervisor.subprocess, 'Popen', fail)
    with pytest.raises(supervisor.SupervisorError, match='handshake failed'):
        supervisor.launch_probe(campaign, base, run_id)
    assert (campaign.root / 'evidence' / run_id / 'reservation.json').is_file()
    assert (campaign.root / 'evidence' / run_id / 'failure.json').is_file()
    assert not (campaign.root / 'evidence' / run_id / 'completed.json').exists()
