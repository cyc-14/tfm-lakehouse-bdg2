"""D46 enforcement on invented workloads and bounded, injected failures."""

from dataclasses import replace
import errno
import json
import os
import select
import signal
import socket
import subprocess
import sys
from threading import Event
import time
import uuid

import pytest

from tfm_lakehouse import resource_linux as linux
from tfm_lakehouse import resource_sampler as sampler
from tfm_lakehouse import resource_supervisor as launcher
from tfm_lakehouse import resource_watchdog as watchdog
from tfm_lakehouse import scalability_workspace as workspace


GIB = 1024**3
NS = 10**9
LEADER = launcher.ProcessIdentity(101, 4000, 101, 100, 101)


@pytest.fixture
def run():
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    token = uuid.uuid4().hex
    base = campaign.root / 'temp' / ('pytest-d46-' + token)
    name = 'd46-' + token
    evidence = campaign.root / 'evidence' / name

    def launch():
        assert not os.path.lexists(base)
        return launcher.launch_supervised_workload(
            campaign, base, name, handler='synthetic_probe_v1', parameters=launcher.ProbeParameters())

    return launch, evidence


def read(path):
    return json.loads(path.read_bytes())


def inject_sample(monkeypatch, field, value, *, enabled=None, once=False):
    """Replace only the policy input under test, using the production evaluator."""
    assess = sampler.assess
    affected = []
    storage = field in ('tmp_bytes', 'campaign_bytes', 'campaign_free_bytes', 'tmp_free_bytes')

    def injected(sample, *, storage_only=False):
        if ((not storage_only or storage) and (enabled is None or enabled.is_set()) and
                (not once or not affected)):
            sample = replace(sample, **{field: value})
            affected.append(field)
        return assess(sample, storage_only=storage_only)

    monkeypatch.setattr(sampler, 'assess', injected)
    return affected


def test_registered_workload_succeeds_only_after_initial_and_final_samples(run):
    launch, evidence = run
    result = launch()
    audit = result['watchdog']
    assert result['status'] == 'supervised_success' and result['evidence_status'] == 'confirmed'
    assert result['resources']['final_process_sample_written']
    assert result['resources']['final_storage_sample_written']
    assert audit['payload_granted'] and audit['outcome_received'] and audit['leader_reaped']
    assert audit['exit_code'] == 0 and audit['collection_complete']
    assert not audit['signals'] and not audit['unresolved_processes']
    assert read(evidence / 'completed.json') == result
    assert read(evidence / 'watchdog.json') == result


@pytest.mark.parametrize('field,threshold,reason', [
    ('rss_bytes', 4 * GIB, 'rss'), ('tmp_bytes', 2 * GIB, 'tmp'),
    ('campaign_bytes', 20 * GIB, 'campaign_storage'), ('effective_memory_fraction', .8, 'memory'),
    ('operation_seconds', 1800, 'operation_time'), ('campaign_seconds', 28800, 'campaign_time'),
    ('campaign_free_bytes', 25 * GIB - 1, 'campaign_free'), ('tmp_free_bytes', 3 * GIB - 1, 'tmp_free'),
])
def test_each_d44_stop_withholds_payload_at_initial_gate(run, monkeypatch, field, threshold, reason):
    launch, evidence = run
    assert (seen := inject_sample(monkeypatch, field, threshold)) == []
    result = launch()
    assert seen
    assert result['status'] == 'initial_rejected'
    assert result['watchdog']['first_stop']['reason'] == reason
    assert not result['watchdog']['payload_granted']
    assert not (evidence / 'probe-output.json').exists()
    assert not (evidence / 'completed.json').exists()
    assert read(evidence / 'failure.json')['watchdog']['first_stop']['reason'] == reason


@pytest.mark.parametrize('field,value,warning', [
    ('rss_bytes', 3.5 * GIB, 'rss'), ('tmp_bytes', 1.5 * GIB, 'tmp'),
    ('campaign_bytes', 15 * GIB, 'campaign_storage'), ('effective_memory_fraction', .7, 'memory'),
    ('operation_seconds', 1200, 'operation_time'),
])
def test_warning_does_not_signal(run, monkeypatch, field, value, warning):
    launch, _ = run
    inject_sample(monkeypatch, field, value)
    result = launch()
    assert result['status'] == 'supervised_success'
    assert warning in result['resources']['warnings']
    assert not result['watchdog']['signals']


def on_outcome(monkeypatch):
    ready = Event()
    receive = watchdog._Watchdog._receive_step

    def observe(channel, data):
        message = receive(channel, data)
        if message == {'status': 'probe_passed'}:
            ready.set()
        return message

    monkeypatch.setattr(watchdog._Watchdog, '_receive_step', staticmethod(observe))
    return ready


def test_actual_registered_child_is_terminated_for_latched_rss_after_payload(run, monkeypatch):
    launch, evidence = run
    after_payload = on_outcome(monkeypatch)
    affected = inject_sample(monkeypatch, 'rss_bytes', 4 * GIB, enabled=after_payload, once=True)
    result = launch()
    audit = result['watchdog']
    assert affected == ['rss_bytes']
    assert result['status'] == 'resource_terminated'
    assert audit['first_stop']['reason'] == 'rss'
    assert audit['payload_granted'] and audit['outcome_received']
    assert audit['exit_code'] == -signal.SIGTERM and audit['leader_reaped']
    assert [(entry['signal'], entry['result']) for entry in audit['signals']] == [('SIGTERM', 'sent')]
    assert audit['signals'][0]['identity'] == result['identity']
    assert (evidence / 'probe-output.json').exists() and not (evidence / 'completed.json').exists()
    assert read(evidence / 'failure.json')['watchdog']['collection_complete']


@pytest.fixture
def block_io(monkeypatch):
    entered, release = Event(), Event()
    owned = []
    submit = sampler._submit

    def submit_observed(lane, target, *fds):
        work = submit(lane, target, *fds)
        owned.append(work)
        return work

    def block():
        entered.set()
        assert release.wait(8)

    monkeypatch.setattr(sampler, '_submit', submit_observed)
    try:
        yield entered, release, block
    finally:
        release.set()
        assert all(work.done.wait(3) for work in owned)


@pytest.mark.parametrize('provider', ['memory', 'append'])
def test_safety_signal_does_not_wait_for_blocked_measurement_or_append(run, monkeypatch, block_io, provider):
    launch, evidence = run
    entered, release, block = block_io
    after_payload = on_outcome(monkeypatch)
    if provider == 'memory':
        original = sampler.ResourceSampler._memory

        def blocked(owner, *args):
            if after_payload.is_set():
                block()
            return original(owner, *args)

        monkeypatch.setattr(sampler.ResourceSampler, '_memory', blocked)
    else:
        inject_sample(monkeypatch, 'rss_bytes', 4 * GIB, enabled=after_payload)
        append = sampler._append

        def blocked(stream, record):
            if 'processes' in record and record['phase'] == 'final':
                block()
            return append(stream, record)

        monkeypatch.setattr(sampler, '_append', blocked)
    began = time.monotonic()
    result = launch()
    assert entered.is_set() and not release.is_set()
    assert time.monotonic() - began < 6
    audit = result['watchdog']
    assert result['status'] == ('monitoring_failure' if provider == 'memory' else 'resource_terminated')
    assert audit['exit_code'] == -signal.SIGTERM and audit['collection_complete']
    assert 'process' in result['resources']['unfinished_io']
    assert (evidence / 'resource-samples.jsonl').read_bytes().count(b'\n') >= 1
    assert not (evidence / 'completed.json').exists()


def test_blocked_watchdog_evidence_keeps_stop_even_when_no_marker(run, monkeypatch, block_io):
    launch, evidence = run
    entered, release, block = block_io
    after_payload = on_outcome(monkeypatch)
    inject_sample(monkeypatch, 'rss_bytes', 4 * GIB, enabled=after_payload)
    original = launcher._write_record

    def blocked(fd, name, value):
        if name in ('watchdog.json', 'failure.json'):
            block()
        return original(fd, name, value)

    monkeypatch.setattr(launcher, '_write_record', blocked)
    result = launch()
    assert entered.is_set() and not release.is_set()
    assert result['status'] == 'evidence_failure'
    assert result['previous_status'] == 'resource_terminated'
    assert result['watchdog']['first_stop']['reason'] == 'rss'
    assert result['watchdog']['exit_code'] == -signal.SIGTERM
    assert not (evidence / 'completed.json').exists()


class _Healthy:
    outcome_ns = None
    campaign_started_ns = None

    def check_health(self):
        return {'monitoring_ready': True}

    def stopping_reasons(self):
        return ()


class _Clock:
    def __init__(self):
        self.now = 100 * NS

    def __call__(self):
        return self.now


def test_grace_is_inclusive_and_per_verified_process():
    clock = _Clock()
    control = watchdog._Watchdog(None, LEADER, None, clock=clock)
    member = control.members[101]
    member.fd = 51
    member.attempts[signal.SIGTERM] = {'result': 'sent', 'monotonic_ns': clock() - NS,
                                       'confirmed_monotonic_ns': clock()}
    assert not control._pending_signals([member], clock() + 30 * NS - 1)
    assert control._pending_signals([member], clock() + 30 * NS) == [(member, signal.SIGKILL)]
    member.attempts[signal.SIGTERM]['result'] = 'worker_unavailable'
    assert not control._pending_signals([member], clock() + 31 * NS)
    member.attempts.clear()
    assert control._pending_signals([member], clock() + 31 * NS) == [(member, signal.SIGTERM)]


@pytest.mark.parametrize('budget,reason', [(1800, 'operation_time'), (28800, 'campaign_time')])
def test_time_budget_is_enforced_on_control_clock(budget, reason):
    clock = _Clock()
    health = _Healthy()
    control = watchdog._Watchdog(None, LEADER, None, clock=clock)
    started = clock()
    if reason == 'campaign_time':
        health.campaign_started_ns = started
        started += budget * NS
    clock.now += budget * NS - 1
    control._sync_health(health, started)
    assert control.first() is None
    clock.now += 1
    control._sync_health(health, started)
    assert control.first()['reason'] == reason and control.first()['kind'] == 'resource'


@pytest.mark.parametrize('fault', ['pid_reused', 'lost', 'changed_session', 'ambiguous_fd'])
def test_signal_revalidates_identity_without_numeric_pid_fallback(monkeypatch, fault):
    member = watchdog._Member(LEADER, 51)
    control = watchdog._Watchdog(None, LEADER, None)

    def stat(pid):
        if fault == 'lost':
            raise FileNotFoundError(errno.ENOENT, 'injected disappearance')
        if fault == 'pid_reused':
            return replace(LEADER, start_ticks=5000), [b'S']
        if fault == 'changed_session':
            return replace(LEADER, session_id=99), [b'S']
        return LEADER, [b'S']

    def poll(fd):
        if fault == 'ambiguous_fd':
            raise OSError(errno.EBADF, 'injected invalid pidfd')
        return False

    monkeypatch.setattr(launcher, '_process_stat', stat)
    monkeypatch.setattr(launcher, '_pidfd_exited', poll)
    monkeypatch.setattr(signal, 'pidfd_send_signal', lambda *_: pytest.fail('unverified signal'))
    monkeypatch.setattr(os, 'kill', lambda *_: pytest.fail('numeric kill'))
    monkeypatch.setattr(os, 'killpg', lambda *_: pytest.fail('group kill'))
    control._send_signals(sampler._IOWork(), [(member, signal.SIGTERM)], 51)
    assert control.first()['reason'] == 'signal_identity_unverified'
    assert member.attempts[signal.SIGTERM]['result'] != 'sent'
    assert control._pending_signals([member], time.monotonic_ns() + 31 * NS) == []


def test_real_ignoring_child_is_killed_only_after_injected_grace():
    program = ('import signal,sys; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
               'print("ready", flush=True); sys.stdin.read()')
    proc = subprocess.Popen([sys.executable, '-B', '-c', program], start_new_session=True,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    fd = control = None
    try:
        assert select.select([proc.stdout], [], [], 5)[0]
        assert proc.stdout.readline().strip() == 'ready'
        identity = launcher.read_process_identity(proc.pid)
        fd = launcher._optional_pidfd(proc.pid)
        assert fd is not None
        launcher._verify_member(proc, launcher.read_process_identity(os.getpid()), identity, fd)
        clock = _Clock()
        before_grace = []

        def wait(delay):
            time.sleep(.01)
            with control.lock:
                member = control.members[proc.pid]
                term = member.attempts.get(signal.SIGTERM)
                if term is None or term['result'] != 'sent':
                    clock.now += int(delay * NS)
                elif not before_grace:
                    clock.now = term['confirmed_monotonic_ns'] + 30 * NS - 1
                    before_grace.append(clock())
                else:
                    clock.now += int(delay * NS)

        control = watchdog._Watchdog(proc, identity, fd, clock=clock, wait=wait)
        control.fail('rss', 'resource')
        control.stop(_Healthy(), clock())
        result = control.report()
        assert before_grace
        term, kill = result['signals']
        assert term['result'] == kill['result'] == 'sent'
        assert term['signal'] == 'SIGTERM' and kill['signal'] == 'SIGKILL'
        assert kill['monotonic_ns'] - term['confirmed_monotonic_ns'] >= 30 * NS
        assert result['exit_code'] == -signal.SIGKILL and result['collection_complete']
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)
        proc.stdout.close()
        if control is not None:
            control.close()
        if fd is not None:
            os.close(fd)


def test_unresolved_or_zero_exit_after_stop_cannot_be_supervised_success():
    data = {'collection_complete': True, 'unresolved_processes': [], 'payload_granted': True,
            'outcome_received': True, 'exit_code': 0, 'signals': [],
            'first_stop': {'kind': 'resource'}}
    healthy = {'stop_required': False}
    assert watchdog._status(data, healthy) == 'resource_stop'
    assert watchdog._status(dict(data, unresolved_processes=[{'pid': 102}]), healthy) == 'incomplete_termination'
    assert watchdog._status(dict(data, first_stop=None, exit_code=7), healthy) == 'workload_failure'


def test_watchdog_reports_unexpected_actual_child_exit():
    proc = subprocess.Popen([sys.executable, '-B', '-c', 'import sys; sys.stdin.read(); sys.exit(7)'],
                            start_new_session=True, stdin=subprocess.PIPE)
    control = fd = None
    try:
        identity = launcher.read_process_identity(proc.pid)
        fd = launcher._optional_pidfd(proc.pid)
        launcher._verify_member(proc, launcher.read_process_identity(os.getpid()), identity, fd)
        proc.stdin.close()
        assert proc.wait(timeout=5) == 7
        control = watchdog._Watchdog(proc, identity, fd)
        parent, child = socket.socketpair()
        with parent, child:
            control.run(parent, {}, _Healthy(), time.monotonic_ns())
        report = control.report()
        assert report['first_stop']['reason'] == 'unexpected_child_exit'
        assert report['exit_code'] == 7 and report['collection_complete'] and not report['signals']
        assert watchdog._status(report, {'stop_required': False}) == 'workload_failure'
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)
        if control is not None:
            control.close()
        if fd is not None:
            os.close(fd)


def test_d25_keeps_primary_when_sampler_cleanup_also_fails(run, monkeypatch):
    launch, evidence = run
    original_finish = sampler.ResourceSampler.finish
    primary = RuntimeError('primary watchdog failure')

    def fail_run(*args):
        raise primary

    def fail_finish(owner):
        original_finish(owner)
        raise OSError('secondary sampler shutdown failure')

    monkeypatch.setattr(watchdog._Watchdog, 'run', fail_run)
    monkeypatch.setattr(sampler.ResourceSampler, 'finish', fail_finish)
    with pytest.raises(RuntimeError) as received:
        launch()
    assert received.value is primary
    assert any('secondary sampler shutdown failure' in note for note in primary.__notes__)
    assert not (evidence / 'completed.json').exists()
    assert read(evidence / 'failure.json')['watchdog']['leader_reaped']


def test_d25_cleanup_failure_cannot_publish_success_marker(run, monkeypatch):
    launch, evidence = run
    close = watchdog._Watchdog.close
    failures = []

    def fail_close(owner):
        close(owner)
        if not failures:
            failures.append('closed')
            raise OSError('injected pidfd cleanup failure')

    monkeypatch.setattr(watchdog._Watchdog, 'close', fail_close)
    with pytest.raises(OSError, match='injected pidfd cleanup failure'):
        launch()
    assert not (evidence / 'completed.json').exists()
    assert read(evidence / 'failure.json')['watchdog']['leader_reaped']


def test_health_recovery_cannot_clear_first_stop():
    reasons = [('rss', 10), ('invalid:operation_seconds', 12)]
    health = _Healthy()
    health.stopping_reasons = lambda: tuple(reasons)
    clock = _Clock()
    control = watchdog._Watchdog(None, LEADER, None, clock=clock)
    control._sync_health(health, clock())
    reasons.clear()
    assert health.check_health()['monitoring_ready']
    control._sync_health(health, clock())
    assert control.first()['reason'] == 'rss'
    assert [item['reason'] for item in control.reasons.values()] == ['rss', 'invalid:operation_seconds']


def test_unresolved_collection_after_deadline_with_no_real_signals(monkeypatch):
    clock = _Clock()
    proc = type('PendingProcess', (), {'poll': lambda self: None})()
    control = watchdog._Watchdog(proc, LEADER, None, clock=clock,
                                wait=lambda _: setattr(clock, 'now', clock() + NS // 2))
    member = control.members[LEADER.pid]
    member.fd = 42
    monkeypatch.setattr(control, '_live', lambda: [member])
    monkeypatch.setattr(control, '_submit_scan', lambda live, now: setattr(control, 'next_scan', now + NS))

    def pretend_signal(wave, now):
        for actor, sig in wave:
            actor.attempts[sig] = {'result': 'sent', 'monotonic_ns': now, 'identity': None,
                                   'confirmed_monotonic_ns': now}

    monkeypatch.setattr(control, '_submit_signals', pretend_signal)
    control.fail('rss', 'resource')
    control.stop(_Healthy(), clock())
    report = control.report()
    assert report['unresolved_processes'][0]['pid'] == LEADER.pid
    assert not report['collection_complete'] and not report['leader_reaped']
    assert report['reasons'][-1]['reason'] == 'termination_not_confirmed'
    assert [entry['signal'] for entry in report['signals']] == ['SIGTERM', 'SIGKILL']
    assert watchdog._status(report, {'stop_required': False}) == 'incomplete_termination'


def test_blocked_spawn_is_reported_without_signal_or_grant(run, monkeypatch, block_io):
    launch, evidence = run
    entered, release, block = block_io

    def blocked(*args):
        block()
        raise OSError(errno.EIO, 'injected exec failure')

    monkeypatch.setattr(launcher, '_spawn', blocked)
    result = launch()
    assert entered.is_set() and not release.is_set()
    assert result['status'] == 'incomplete_termination'
    audit = result['watchdog']
    assert audit['first_stop']['reason'] == 'spawn_unconfirmed'
    assert audit['unfinished_control_io'] == ['launch']
    assert audit['unresolved_processes'] == [{'pid': None, 'identity': 'spawn_unconfirmed'}]
    assert not audit['payload_granted'] and not audit['signals']
    assert not (evidence / 'completed.json').exists()


def test_no_pidfd_withholds_payload_and_reaps_direct_child(run, monkeypatch):
    launch, evidence = run
    monkeypatch.setattr(launcher, '_optional_pidfd', lambda pid: None)
    monkeypatch.setattr(signal, 'pidfd_send_signal', lambda *_: pytest.fail('unverified signal'))
    result = launch()
    assert result['status'] == 'monitoring_failure'
    audit = result['watchdog']
    assert audit['first_stop']['reason'] == 'leader_pidfd_unavailable'
    assert audit['leader_reaped'] and audit['collection_complete']
    assert not audit['payload_granted'] and not audit['signals']
    assert not (evidence / 'probe-output.json').exists()


def test_real_descendant_gets_own_term_and_leader_zero_is_still_a_stop():
    child_code = ('import os,signal,sys; signal.signal(signal.SIGTERM, signal.SIG_DFL); '
                  'print(os.getpid(), flush=True); sys.stdin.read()')
    leader = """
import select
import signal
import subprocess
import sys

signal.signal(signal.SIGTERM, signal.SIG_IGN)
with subprocess.Popen([sys.executable, '-B', '-c', sys.argv[1]], stdin=subprocess.PIPE,
                      stdout=subprocess.PIPE, text=True) as child:
    print(child.stdout.readline().strip(), flush=True)
    while child.poll() is None:
        if select.select([sys.stdin], [], [], .05)[0] and not sys.stdin.readline():
            child.stdin.close()
            break
"""
    proc = subprocess.Popen([sys.executable, '-B', '-c', leader, child_code], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, text=True, start_new_session=True)
    control = fd = None
    try:
        assert select.select([proc.stdout], [], [], 5)[0]
        child_pid = int(proc.stdout.readline())
        identity = launcher.read_process_identity(proc.pid)
        fd = launcher._optional_pidfd(proc.pid)
        launcher._verify_member(proc, launcher.read_process_identity(os.getpid()), identity, fd)
        control = watchdog._Watchdog(proc, identity, fd)
        control.payload_granted = True
        control.fail('rss', 'resource')
        control.stop(_Healthy(), time.monotonic_ns())
        audit = control.report()
        assert audit['exit_code'] == 0 and audit['leader_reaped']
        assert audit['collection_complete'] and not audit['unresolved_processes']
        assert any(entry['pid'] == child_pid and entry['signal'] == 'SIGTERM' and entry['result'] == 'sent'
                   for entry in audit['signals'])
        assert audit['exited_descendants'][0]['pid'] == child_pid
        assert watchdog._status(audit, {'stop_required': False}) == 'resource_terminated'
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)
        proc.stdout.close()
        if control is not None:
            control.close()
        if fd is not None:
            os.close(fd)


def test_observed_session_escape_remains_unresolved_after_reparenting(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace

    escaped = launcher.ProcessIdentity(102, 5000, 102, LEADER.pid, 102)
    current = {escaped.pid: escaped}
    monkeypatch.setattr(linux.os, 'scandir', lambda _: nullcontext(
        [SimpleNamespace(name=str(pid)) for pid in current]))
    monkeypatch.setattr(launcher, '_process_stat', lambda pid: (current[pid], [b'S']))
    tracker = linux.ProcessTracker()
    _, failures = linux._discover(LEADER, {}, tracker)
    assert failures and tracker.suspects == (escaped,)

    control = watchdog._Watchdog(None, LEADER, None)
    control.observe_members(tracker)
    current[escaped.pid] = replace(escaped, parent_pid=1)
    _, failures = linux._discover(LEADER, {}, tracker)
    assert not failures and not tracker.suspects
    finished = sampler._IOWork()
    finished.result = [], False
    finished.done.set()
    control.jobs['identity'] = finished, 10 * NS, True
    control._collect_jobs(10 * NS)
    assert not control.scan_clear
    assert control.first()['reason'] == 'membership_discovery_incomplete'
    assert control.unknown[0]['pid'] == escaped.pid
    assert control.unknown[0]['start_ticks'] == escaped.start_ticks
    assert escaped.pid not in control.members
