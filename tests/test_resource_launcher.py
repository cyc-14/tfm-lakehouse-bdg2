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


@pytest.fixture
def registered_probe():
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    token = uuid.uuid4().hex
    base = campaign.root / 'temp' / ('pytest-' + token)
    run = 'probe-' + token
    return campaign, base, run, campaign.root / 'evidence' / run


def test_registered_workload_verifies_fixture_and_reaps_child(registered_probe):
    import hashlib
    import json

    campaign, base, run, evidence = registered_probe
    fixture = campaign.root / 'raw' / (run + '.txt')
    with fixture.open('xb') as handle:
        handle.write(b'invented fixture')
    result = supervisor.launch_workload(
        campaign, base, run, handler='synthetic_probe_v1',
        parameters=supervisor.ProbeParameters(fixture.name),
    )
    assert result['configuration']['source'] == {
        'path': str(fixture), 'sha256': hashlib.sha256(b'invented fixture').hexdigest(),
    }
    assert json.loads((evidence / 'probe-output.json').read_bytes())['source'] == (
        result['configuration']['source']
    )
    assert json.loads((evidence / 'completed.json').read_bytes()) == result
    pid = result['identity']['pid']
    with pytest.raises(ChildProcessError):
        os.waitpid(pid, os.WNOHANG)


@pytest.mark.parametrize('handler,params,error', [
    ('unknown', supervisor.ProbeParameters(), 'Unknown workload handler'),
    ('synthetic_probe_v1', {'source_name': 'invented.txt'}, 'Invalid workload parameters'),
    ('synthetic_probe_v1', supervisor.ProbeParameters('../foreign.txt'), 'Unauthorized synthetic source'),
    ('synthetic_probe_v1', supervisor.ProbeParameters('/outside/foreign.txt'), 'Unauthorized synthetic source'),
])
def test_invalid_registration_is_rejected_before_inspection(monkeypatch, handler, params, error):
    monkeypatch.setattr(supervisor, 'preflight', lambda *_: pytest.fail('inspected workspace'))
    with pytest.raises(supervisor.SupervisorError, match=error):
        supervisor.launch_workload(None, None, 'run', handler=handler, parameters=params)


def test_wrong_campaign_owner_is_rejected(registered_probe):
    campaign, base, run, evidence = registered_probe
    wrong = workspace.Campaign(campaign.root, campaign.campaign_id, 'wrong-owner')
    with pytest.raises(supervisor.SupervisorError, match='Campaign preflight failed'):
        supervisor.launch_probe(wrong, base, run)
    assert not os.path.lexists(evidence)


def test_foreign_campaign_never_inspected(monkeypatch):
    root = Path('/invented-protected/campaign')
    foreign = workspace.Campaign(root, 'campaign', 'owner')
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: pytest.fail('inspected'))
    with pytest.raises(supervisor.SupervisorError, match='Campaign preflight failed'):
        supervisor.launch_probe(foreign, root / 'temp' / 'pytest-new', 'run')


def _assert_rejected(evidence):
    import json

    failure = json.loads((evidence / 'failure.json').read_bytes())
    assert failure['reaped'] and failure['exit_code'] != 0
    assert (evidence / 'launch.json').is_file()
    for name in ('probe-output.json', 'completed.json'):
        assert not os.path.lexists(evidence / name)


@pytest.mark.parametrize('field', ['owner_id', 'protected_roots', 'source', 'destination'])
def test_child_recomputes_configuration_before_payload(registered_probe, monkeypatch, field):
    campaign, base, run, evidence = registered_probe
    original = supervisor._configuration

    def disagree(*args):
        config = original(*args)
        config[field] = {
            'owner_id': 'wrong-owner', 'protected_roots': ['/invented'],
            'source': {'path': '/invented-protected/unread.txt', 'sha256': 'invalid'},
            'destination': '/invented-protected/unwritten.json',
        }[field]
        return config

    monkeypatch.setattr(supervisor, '_configuration', disagree)
    with pytest.raises(supervisor.SupervisorError, match='prematurely'):
        supervisor.launch_probe(campaign, base, run)
    assert not (evidence / 'child-identity.json').exists()
    _assert_rejected(evidence)


def test_child_requires_valid_grant_before_payload(registered_probe, monkeypatch):
    campaign, base, run, evidence = registered_probe
    send = supervisor._send

    def deny(channel, message):
        send(channel, {'denied': True} if 'ticket_sha256' in message else message)

    monkeypatch.setattr(supervisor, '_send', deny)
    with pytest.raises(supervisor.SupervisorError, match='prematurely'):
        supervisor.launch_probe(campaign, base, run)
    assert (evidence / 'child-identity.json').is_file()
    _assert_rejected(evidence)


def test_failure_immediately_after_popen_leaves_child_receipt(registered_probe, monkeypatch):
    campaign, base, run, evidence = registered_probe

    def interrupt(_):
        raise RuntimeError('interrupted immediately after Popen')

    monkeypatch.setattr(supervisor, '_optional_pidfd', interrupt)
    with pytest.raises(RuntimeError, match='immediately after Popen'):
        supervisor.launch_probe(campaign, base, run)
    assert (evidence / 'child-pid.json').is_file()
    _assert_rejected(evidence)


def test_parent_protocol_deadline_closes_channel_and_reaps(registered_probe, monkeypatch):
    campaign, base, run, evidence = registered_probe

    def time_out(channel):
        raise TimeoutError('injected deadline')

    monkeypatch.setattr(supervisor, '_receive', time_out)
    with pytest.raises(supervisor.SupervisorError, match='handshake failed'):
        supervisor.launch_probe(campaign, base, run)
    _assert_rejected(evidence)


def test_receive_wait_is_bounded_on_open_channel(monkeypatch):
    import socket

    monkeypatch.setattr(supervisor, '_TIMEOUT', 0.02)
    parent, child = socket.socketpair()
    with parent, child, pytest.raises(TimeoutError):
        supervisor._receive(parent)


def test_duplicate_protocol_fields_are_ambiguous():
    with pytest.raises(supervisor.SupervisorError, match='Duplicate protocol field'):
        supervisor._decode(b'{"handler":"synthetic_probe_v1","handler":"unknown"}')


@pytest.mark.parametrize('attribute', ['start_ticks', 'session_id', 'process_group'])
def test_membership_mismatch_rejects_actual_child(registered_probe, monkeypatch, attribute):
    from dataclasses import replace

    campaign, base, run, evidence = registered_probe
    read_identity = supervisor.read_process_identity

    def mismatch(pid):
        actual = read_identity(pid)
        if pid != os.getpid():
            return replace(actual, **{attribute: getattr(actual, attribute) + 1})
        return actual

    monkeypatch.setattr(supervisor, 'read_process_identity', mismatch)
    with pytest.raises(supervisor.SupervisorError, match='membership differs'):
        supervisor.launch_probe(campaign, base, run)
    assert (evidence / 'child-pid.json').is_file()
    assert not (evidence / 'child-identity.json').exists()
    _assert_rejected(evidence)


def test_unrelated_pid_cannot_be_used_as_launched_child(registered_probe, monkeypatch):
    campaign, base, run, evidence = registered_probe
    read_record = supervisor._read_record

    def substitute(directory, name):
        record = read_record(directory, name)
        if name == 'child-pid.json':
            record['identity'] = vars(supervisor.read_process_identity(os.getpid()))
        return record

    monkeypatch.setattr(supervisor, '_read_record', substitute)
    with pytest.raises(supervisor.SupervisorError, match='identity or configuration differs'):
        supervisor.launch_probe(campaign, base, run)
    _assert_rejected(evidence)


def test_fixture_change_after_launch_preflight_is_rejected(registered_probe, monkeypatch):
    campaign, base, run, evidence = registered_probe
    fixture = campaign.root / 'raw' / (run + '.txt')
    with fixture.open('xb') as handle:
        handle.write(b'invented fixture')
    send = supervisor._send

    def change_source(channel, message):
        if 'configuration' in message:
            with fixture.open('ab') as handle:
                handle.write(b' changed during initialization')
        send(channel, message)

    monkeypatch.setattr(supervisor, '_send', change_source)
    with pytest.raises(supervisor.SupervisorError, match='prematurely'):
        supervisor.launch_workload(
            campaign, base, run, handler='synthetic_probe_v1',
            parameters=supervisor.ProbeParameters(fixture.name),
        )
    _assert_rejected(evidence)


def test_fixture_change_after_probe_preserves_partial_output(registered_probe, monkeypatch):
    import json

    campaign, base, run, evidence = registered_probe
    fixture = campaign.root / 'raw' / (run + '.txt')
    with fixture.open('xb') as handle:
        handle.write(b'invented fixture')
    receive = supervisor._receive

    def change_source(channel):
        message = receive(channel)
        if message.get('status') == 'probe_passed':
            with fixture.open('ab') as handle:
                handle.write(b' changed after the probe')
        return message

    monkeypatch.setattr(supervisor, '_receive', change_source)
    with pytest.raises(supervisor.SupervisorError, match='source changed before completion'):
        supervisor.launch_workload(
            campaign, base, run, handler='synthetic_probe_v1',
            parameters=supervisor.ProbeParameters(fixture.name),
        )
    assert (evidence / 'probe-output.json').is_file()
    assert not (evidence / 'completed.json').exists()
    failure = json.loads((evidence / 'failure.json').read_bytes())
    assert failure['reaped'] and failure['exit_code'] == 0


def test_pidfd_close_failure_cannot_leave_success_marker(registered_probe, monkeypatch):
    import json

    campaign, base, run, evidence = registered_probe
    close = os.close
    state = {'fd': None, 'keepalive': None}

    def owned_descriptor(pid):
        # An empty pipe stands in for an active, non-ready pidfd.
        read_fd, write_fd = os.pipe()
        state.update(fd=read_fd, keepalive=write_fd)
        return read_fd

    def fail_close(fd):
        close(fd)
        if fd == state['fd']:
            state['fd'] = None
            raise OSError('injected pidfd close failure')

    monkeypatch.setattr(supervisor, '_optional_pidfd', owned_descriptor)
    monkeypatch.setattr(os, 'close', fail_close)
    try:
        with pytest.raises(supervisor.SupervisorError, match='handshake failed'):
            supervisor.launch_probe(campaign, base, run)
    finally:
        if state['keepalive'] is not None:
            close(state['keepalive'])
    assert (evidence / 'probe-output.json').is_file()
    assert not (evidence / 'completed.json').exists()
    assert json.loads((evidence / 'failure.json').read_bytes())['reaped']


def test_evidence_close_after_completion_reports_warning(registered_probe, monkeypatch, caplog):
    import json

    campaign, base, run, evidence = registered_probe
    marker = evidence / 'completed.json'
    close = os.close

    def fail_after_completion(fd):
        failed = marker.is_file() and os.readlink(f'/proc/self/fd/{fd}') == str(evidence)
        close(fd)
        if failed:
            raise OSError('injected directory close failure')

    monkeypatch.setattr(os, 'close', fail_after_completion)
    result = supervisor.launch_probe(campaign, base, run)
    assert json.loads(marker.read_bytes()) == result
    assert 'Could not close the completed evidence directory' in caplog.text
