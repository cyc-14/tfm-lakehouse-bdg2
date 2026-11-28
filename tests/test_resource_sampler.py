"""Injected and live synthetic observations without Spark or resource stress."""

from contextlib import nullcontext
from dataclasses import replace
import errno
import json
import math
import os
from pathlib import Path, PurePosixPath
from threading import Event
from types import SimpleNamespace
import uuid

import pytest

from tfm_lakehouse import resource_linux as linux
from tfm_lakehouse import resource_sampler as sampler
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace


GIB = 1024**3
SECOND = 10**9
LEADER = supervisor.ProcessIdentity(101, 1000, 101, 99, 101)
MEMORY = {'scope': 'shared_system', 'memory': {'total_bytes': 10, 'used_bytes': 1},
          'swap': {'total_bytes': 0, 'used_bytes': 0}}
GROUP = (2, PurePosixPath('/batch'))


def process_fields(rss='1', user='10', system='2', state='S'):
    fields = [b'0'] * 22
    for index, value in ((0, state), (11, user), (12, system), (21, rss)):
        fields[index] = value.encode()
    return fields


@pytest.fixture
def proc(monkeypatch):
    entries = {101: (LEADER, process_fields())}
    monkeypatch.setattr(linux.os, 'scandir', lambda path: nullcontext(
        [SimpleNamespace(name=str(pid)) for pid in entries]))
    monkeypatch.setattr(supervisor, '_process_stat', entries.__getitem__)
    monkeypatch.setattr(supervisor, '_optional_pidfd', lambda pid: None)
    monkeypatch.setattr(linux, '_membership', lambda pid: GROUP)
    return entries


def test_session_children_orphans_groups_and_unrelated_processes(proc):
    child = supervisor.ProcessIdentity(102, 1010, 101, 101, 101)
    orphan = supervisor.ProcessIdentity(103, 1020, 103, 1, 101)
    other_session = supervisor.ProcessIdentity(104, 1030, 104, 1, 104)
    proc.update({child.pid: (child, process_fields(rss='2')),
                 orphan.pid: (orphan, process_fields(rss='3')),
                 other_session.pid: (other_session, process_fields(rss='9999'))})
    observed, groups, failures = linux.processes(LEADER, None, {})
    assert set(observed) == {LEADER, child, orphan} == set(groups)
    assert sum(metric['rss_bytes'] for metric in observed.values()) == 6 * os.sysconf('SC_PAGE_SIZE')
    assert all(metric['cpu_ticks'] == 12 for metric in observed.values())
    assert failures == []


def test_escaped_and_reused_pids_fail_closed(proc):
    reused = supervisor.ProcessIdentity(102, 2000, 101, 101, 101)
    escaped = supervisor.ProcessIdentity(103, 2001, 103, 101, 103)
    proc.update({102: (reused, process_fields()), 103: (escaped, process_fields())})
    old = replace(reused, start_ticks=1)
    observed, _, failures = linux.processes(LEADER, None, {102: old})
    assert set(observed) == {LEADER}
    assert {failure['detail'] for failure in failures} == {
        'Descendant outside the verified session', 'Observed PID was reused'}
    with pytest.raises(linux.InvalidMeasurement, match='session leader'):
        linux.processes(replace(LEADER, start_ticks=1), None, {})


@pytest.mark.parametrize('fault', ['missing', 'unavailable', 'invalid', 'reused', 'zombie'])
def test_member_can_disappear_or_change_during_sample(proc, monkeypatch, fault):
    child = supervisor.ProcessIdentity(102, 1010, 101, 101, 101)
    proc[102] = child, process_fields()
    calls = 0

    def read(pid):
        nonlocal calls
        if pid == child.pid:
            calls += 1
        if pid == child.pid and calls == 3:
            if fault == 'missing':
                raise FileNotFoundError
            if fault == 'unavailable':
                raise PermissionError(errno.EACCES, 'injected')
            if fault == 'invalid':
                raise ValueError
            if fault == 'reused':
                return replace(child, start_ticks=child.start_ticks + 1), process_fields()
            return child, process_fields(state='Z')
        return proc[pid]

    monkeypatch.setattr(supervisor, '_process_stat', read)
    observed, _, failures = linux.processes(LEADER, None, {})
    assert set(observed) == {LEADER}
    expected = {'reused': 'invalid', 'zombie': 'unavailable'}.get(fault, fault)
    assert [(failure['metric'], failure['status']) for failure in failures] == [
        ('process_membership', expected)]


def test_root_pidfd_exit_invalidates_session(proc, monkeypatch):
    monkeypatch.setattr(supervisor, '_pidfd_exited', lambda _: True)
    with pytest.raises(linux.InvalidMeasurement, match='session leader'):
        linux.processes(LEADER, 42, {})


@pytest.mark.parametrize('field,value', [(0, b'!'), (1, b'+99'), (19, b'4_000')])
def test_malformed_proc_stat_identity_is_rejected(monkeypatch, field, value):
    fields = process_fields()
    fields[field] = value
    monkeypatch.setattr(Path, 'read_bytes', lambda path: b'101 (a ) process) ' + b' '.join(fields))
    with pytest.raises(supervisor.SupervisorError, match='identity unavailable'):
        supervisor.read_process_identity(101)


@pytest.mark.parametrize('metric,fields,expected', [
    ('rss_bytes', process_fields(rss='?'), None),
    ('cpu_ticks', process_fields(user='-1'), None),
    ('rss_bytes', process_fields(rss='0'), 0),
    ('cpu_ticks', process_fields(user='0', system='0'), 0),
])
def test_missing_process_counters_remain_distinct_from_zero(proc, metric, fields, expected):
    proc[101] = LEADER, fields
    observed, _, failures = linux.processes(LEADER, None, {})
    assert observed[LEADER][metric] == expected
    assert [(failure['metric'], failure['status']) for failure in failures] == (
        [(metric, 'invalid')] if expected is None else [])


@pytest.mark.parametrize('memory_fields,swap_fields,memory,swap,failure', [
    ('MemTotal: 8 kB\nMemAvailable: 8 kB', 'SwapTotal: 0 kB\nSwapFree: 0 kB', 0, 0, None),
    ('MemTotal: 8 kB', 'SwapTotal: 0 kB\nSwapFree: 0 kB', None, 0, ('system_memory', 'missing')),
    ('MemTotal: 8 MB\nMemAvailable: 5 kB', 'SwapTotal: 8 kB\nSwapFree: 4 kB',
     None, 4096, ('system_memory', 'invalid')),
    ('MemTotal: 8 kB\nMemAvailable: 5 kB', 'SwapTotal: 8 kB\nSwapFree: 9 kB',
     3072, None, ('system_swap', 'invalid')),
])
def test_meminfo_partial_records(monkeypatch, memory_fields, swap_fields, memory, swap, failure):
    monkeypatch.setattr(linux.Path, 'read_text', lambda path, **kwargs:
                        memory_fields + '\n' + swap_fields)
    values, failures = linux.system_memory()
    for key, expected in [('memory', memory), ('swap', swap)]:
        assert (None if values[key] is None else values[key]['used_bytes']) == expected
    assert [(item['metric'], item['status']) for item in failures] == ([failure] if failure else [])


def control(path, **values):
    path.mkdir(parents=True, exist_ok=True)
    for name, content in values.items():
        (path / name).write_text(str(content), encoding='ascii')
    return path


def simulate_cgroup(monkeypatch, root, version, member, mounted_root='/'):
    with linux._absolute_directory(root) as fd:
        mount_id = linux._mount_id(fd)
    mounts = {1: [], 2: []}
    mounts[version].append((PurePosixPath(mounted_root), root, mount_id))
    monkeypatch.setattr(linux, '_memory_mounts', lambda: mounts)
    monkeypatch.setattr(linux, '_membership', lambda pid: (version, PurePosixPath(member)))
    monkeypatch.setattr(linux, '_recheck_member', lambda *args: None)
    return {LEADER: (version, PurePosixPath(member))}


@pytest.mark.parametrize('child_used,child_limit,child_ratio', [(0, 'max', None), (9, 10, .9), (0, 0, 1)])
def test_cgroup_nested_limits_each_use_own_denominator(
        tmp_path, monkeypatch, child_used, child_limit, child_ratio):
    root = control(tmp_path / 'cgroups', **{'cgroup.controllers': 'memory'})
    control(root / 'batch', **{'memory.current': 80, 'memory.max': 100})
    control(root / 'batch/probe', **{'memory.current': child_used, 'memory.max': child_limit})
    memberships = simulate_cgroup(monkeypatch, root, 2, '/batch/probe')
    groups, failures = linux.cgroup_memory(memberships)
    assert failures == []
    assert [group.get('utilization_fraction') for group in groups] == [child_ratio, .8, None]
    assert groups[0]['unlimited'] == (child_limit == 'max')
    assert groups[2]['status'] == 'not_applicable'

    monkeypatch.setattr(linux, 'system_memory', lambda: (MEMORY, []))
    recorder = sampler.ResourceSampler(None, None, None)
    recorder.identity = LEADER
    system, cgroup, effective = recorder._memory(memberships, [])
    assert system['scope'] == 'shared_system'
    assert cgroup['scope'] == 'shared_cgroup_hierarchies'
    assert effective == max(.8, child_ratio or 0)


def test_partial_mount_never_reads_its_parent(tmp_path, monkeypatch):
    root = control(tmp_path / 'visible', **{'cgroup.controllers': 'memory'})
    memberships = simulate_cgroup(monkeypatch, root, 2, '/team', mounted_root='/team')
    op = linux._absolute_directory

    def confined(path):
        assert path == root
        return op(path)

    monkeypatch.setattr(linux, '_absolute_directory', confined)
    groups, failures = linux.cgroup_memory(memberships)
    assert groups[0]['status'] == 'not_applicable'
    assert [failure['detail'] for failure in failures] == ['Hidden cgroup ancestors']


def test_cgroup_without_visible_memory_controller_cannot_prove_root_coverage(tmp_path, monkeypatch):
    root = control(tmp_path / 'cgroups', **{'cgroup.controllers': 'cpu io pids'})
    memberships = simulate_cgroup(monkeypatch, root, 2, '/')
    monkeypatch.setattr(linux, 'system_memory', lambda: (MEMORY, []))
    recorder = sampler.ResourceSampler(None, None, None)
    recorder.identity = LEADER
    failures = []
    system, cgroup, pressure = recorder._memory(memberships, failures)
    assert system == MEMORY and pressure is None
    assert cgroup['groups'][0]['status'] == 'invalid'
    assert failures[0]['detail'] == 'Cannot establish visible cgroup memory coverage'


def test_v2_missing_enabled_interface_and_rotating_mount_fail_closed(tmp_path, monkeypatch):
    root = control(tmp_path / 'cgroups', **{'cgroup.controllers': 'memory',
                                            'cgroup.subtree_control': 'memory'})
    (root / 'vacant').mkdir()
    memberships = simulate_cgroup(monkeypatch, root, 2, '/vacant')
    groups, failures = linux.cgroup_memory(memberships)
    assert groups[0]['status'] == 'missing' and failures[0]['metric'] == 'cgroup_memory'
    monkeypatch.setattr(linux, '_mount_id', lambda fd: 0)
    groups, failures = linux.cgroup_memory(memberships)
    assert all(group['status'] == 'invalid' for group in groups)
    assert all(failure['detail'] == 'Memory hierarchy mount changed' for failure in failures)


def test_legacy_hierarchical_limit_tracks_hidden_or_changing_ancestors(tmp_path, monkeypatch):
    root = tmp_path / 'cgroups'
    page_size = os.sysconf('SC_PAGE_SIZE')
    unlimited = (2**63 - 1) // page_size * page_size

    def legacy(directory, used, limit, hierarchical):
        control(directory, **{'memory.use_hierarchy': 1, 'memory.usage_in_bytes': used,
                              'memory.limit_in_bytes': limit,
                              'memory.stat': f'hierarchical_memory_limit {hierarchical}\n'})

    legacy(root, 9, unlimited, unlimited)
    legacy(root / 'ancestor', 8, 10, 10)
    legacy(root / 'ancestor/probe', 2, unlimited, 10)
    memberships = simulate_cgroup(monkeypatch, root, 1, '/ancestor/probe')
    groups, failures = linux.cgroup_memory(memberships)
    assert not failures and groups[0]['unlimited'] and groups[1]['utilization_fraction'] == .8
    legacy(root / 'ancestor/probe', 2, unlimited, 5)
    _, failures = linux.cgroup_memory(memberships)
    assert failures[0]['detail'] == 'Hidden or changing legacy cgroup limits'


def test_campaign_counts_allocated_blocks_and_refuses_directory_links(tmp_path):
    root = tmp_path / 'campaign'
    (root / 'nested').mkdir(parents=True)
    path = root / 'nested/file'
    path.write_bytes(b'fixture')
    other = tmp_path / 'outside'
    other.mkdir()
    with linux.campaign_directory(root) as fd:
        result = linux.campaign_storage(root, fd)
        assert result == {'allocated_bytes': sum(
            entry.stat().st_blocks for entry in (root, root / 'nested', path)) * 512, 'file_count': 1}
        (root / 'outside').symlink_to(other, target_is_directory=True)
        with pytest.raises(linux.InvalidMeasurement, match='link or a special file'):
            linux.campaign_storage(root, fd)


def test_campaign_refuses_unmatched_and_replaced_roots(tmp_path, monkeypatch):
    root = tmp_path / 'campaign'
    root.mkdir()
    with linux.campaign_directory(root) as fd:
        root.rename(tmp_path / 'old')
        root.mkdir()
        with pytest.raises(linux.InvalidMeasurement, match='Campaign directory changed'):
            linux.campaign_storage(root, fd)
    monkeypatch.setattr(linux, 'campaign_directory', lambda path: pytest.fail('unauthorized traversal'))
    with pytest.raises(workspace.WorkspaceError, match='outside'):
        with sampler._authorized_root(tmp_path, 'run', 0):
            pytest.fail('unauthorized workspace')


def test_tmp_occupancy_uses_total_minus_all_free_and_permitted_capacity(monkeypatch):
    monkeypatch.setattr(linux, '_absolute_directory', lambda path: nullcontext(41))
    monkeypatch.setattr(os, 'fstatvfs', lambda fd: SimpleNamespace(
        f_frsize=4096, f_blocks=100, f_bfree=70, f_bavail=50))
    monkeypatch.setattr(os, 'scandir', lambda *_: pytest.fail('enumerated /tmp'))
    assert linux.temporary_filesystem() == {
        'total_bytes': 100 * 4096, 'used_bytes': 30 * 4096, 'available_bytes': 50 * 4096}


@pytest.fixture
def observation(monkeypatch):
    recorder = sampler.ResourceSampler(None, None, None)
    recorder.identity = LEADER
    values = {
        'rss_bytes': 0, 'tmp_bytes': 0, 'campaign_bytes': 0,
        'effective_memory_fraction': 0, 'operation_seconds': 0, 'campaign_seconds': 0,
        'campaign_free_bytes': 25 * GIB, 'tmp_free_bytes': 3 * GIB,
    }
    now = 40_000 * SECOND
    monkeypatch.setattr(sampler.time, 'monotonic_ns', lambda: now)

    def collect(*, storage_age=0, **override):
        measured = dict(values, **override)
        recorder.operation_started_ns = now - int(measured['operation_seconds'] * SECOND)
        recorder.campaign_started_ns = now - int(measured['campaign_seconds'] * SECOND)
        recorder.storage = ({
            'started_monotonic_ns': now - storage_age,
            'campaign_tree': {'allocated_bytes': measured['campaign_bytes']},
            'campaign_filesystem': {'available_bytes': measured['campaign_free_bytes']},
            'temporary_filesystem': {'used_bytes': measured['tmp_bytes'],
                                     'available_bytes': measured['tmp_free_bytes']},
        }, [])
        monkeypatch.setattr(recorder, '_processes', lambda _: ({'rss_bytes': measured['rss_bytes']}, {}))
        monkeypatch.setattr(recorder, '_memory', lambda *_: (MEMORY, {}, measured['effective_memory_fraction']))
        return recorder._observation(now, 'utc', 'periodic')

    return collect


@pytest.mark.parametrize('field,warning,stop,label,below', [
    ('rss_bytes', math.ceil(3.2 * GIB), 4 * GIB, 'rss', 1),
    ('tmp_bytes', 1.5 * GIB, 2 * GIB, 'tmp', 1),
    ('campaign_bytes', 15 * GIB, 20 * GIB, 'campaign_storage', 1),
    ('effective_memory_fraction', .7, .8, 'memory', .000001),
    ('operation_seconds', 1200, 1800, 'operation_time', 1),
    ('campaign_seconds', None, 28800, 'campaign_time', 1),
])
def test_sampler_thresholds_below_and_at_boundary(observation, field, warning, stop, label, below):
    if warning is not None:
        assert observation(**{field: warning - below})['assessment']['warnings'] == ()
        assert observation(**{field: warning})['assessment']['warnings'] == (label,)
    assert observation(**{field: stop - below})['assessment']['stops'] == ()
    assert observation(**{field: stop})['assessment']['stops'] == (label,)


@pytest.mark.parametrize('field,minimum,label', [
    ('campaign_free_bytes', 25 * GIB, 'campaign_free'), ('tmp_free_bytes', 3 * GIB, 'tmp_free'),
])
def test_sampler_free_space_minima(observation, field, minimum, label):
    assert not observation(**{field: minimum})['assessment']['stop_required']
    assert observation(**{field: minimum - 1})['assessment']['stops'] == (label,)


def test_sampler_partial_critical_failure_does_not_erase_other_stops(observation):
    sample = observation(rss_bytes=None, tmp_bytes=2 * GIB)
    assert sample['policy_inputs']['rss_bytes'] is None
    assert sample['assessment']['stops'] == ('invalid:rss_bytes', 'tmp')


def test_expired_storage_sample_is_retained_but_excluded_from_policy(observation):
    sample = observation(storage_age=10 * SECOND, tmp_bytes=2 * GIB)
    assert sample['storage']['temporary_filesystem']['used_bytes'] == 2 * GIB
    assert sample['policy_inputs']['tmp_bytes'] is None
    assert {'invalid:tmp_bytes', 'storage_sampling_interval'} <= set(sample['assessment']['stops'])
    assert sample['measurement_failures'][0]['status'] == 'invalid'


def test_deltas_use_process_instance_across_reparenting(monkeypatch):
    recorder = sampler.ResourceSampler(None, None, None)
    recorder.identity = LEADER
    child = supervisor.ProcessIdentity(102, 1060, 101, 101, 101)
    observed = {LEADER: {'rss_bytes': 10, 'cpu_ticks': 12},
                child: {'rss_bytes': 4, 'cpu_ticks': 10}}
    monkeypatch.setattr(linux, 'processes', lambda *args: (observed, {}, []))
    processes, _ = recorder._processes([])
    assert processes['rss_bytes'] == 14 and processes['matched_cpu_delta_seconds'] is None
    observed = {LEADER: {'rss_bytes': 10, 'cpu_ticks': 12},
                replace(child, parent_pid=1): {'rss_bytes': 3, 'cpu_ticks': 20}}
    processes, _ = recorder._processes([])
    assert processes['matched_cpu_process_count'] == 2
    assert processes['matched_cpu_delta_seconds'] == 10 / os.sysconf('SC_CLK_TCK')


def test_clock_grid_skips_elapsed_slots_and_rejects_clock_discontinuity():
    assert sampler._advance(3 * SECOND, 3 * SECOND, SECOND) == 4 * SECOND
    assert sampler._advance(3 * SECOND, 8 * SECOND + 1234, SECOND) == 9 * SECOND
    failures = []
    assert sampler._elapsed(2 * SECOND, 2 * SECOND, 'clock', failures) == 0
    assert sampler._elapsed(2 * SECOND, SECOND, 'clock', failures) is None
    assert failures[0]['status'] == 'invalid'


def test_campaign_clock_is_retained_across_launches_and_fails_closed_on_reboot(tmp_path, monkeypatch):
    (tmp_path / 'evidence').mkdir()
    with linux._absolute_directory(tmp_path) as fd:
        assert sampler._campaign_clock(fd, 123) == sampler._campaign_clock(fd, 456) == 123
        path = tmp_path / 'evidence/resource-clock.json'
        captured = path.read_bytes()
        read_text = Path.read_text
        monkeypatch.setattr(Path, 'read_text', lambda file, *args, **kwargs:
                            'another-boot' if file.name == 'boot_id' else read_text(file, *args, **kwargs))
        with pytest.raises(linux.InvalidMeasurement, match='clock domain'):
            sampler._campaign_clock(fd, 789)
        assert path.read_bytes() == captured


@pytest.fixture
def launch():
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    token = uuid.uuid4().hex
    run = 'sampling-' + token
    base = campaign.root / 'temp' / ('pytest-' + token)
    assert not os.path.lexists(base)
    return campaign, base, run, campaign.root / 'evidence' / run


def records(evidence, name='resource-samples.jsonl'):
    with (evidence / name).open(encoding='ascii') as source:
        return [json.loads(line) for line in source]


def test_verified_probe_retains_streams_and_checkpoint(launch):
    campaign, base, run, evidence = launch
    given = supervisor.launch_probe(campaign, base, run)
    summary = given['resources']
    data = records(evidence)
    storage = records(evidence, 'resource-storage.jsonl')
    assert given['status'] == 'probe_passed'
    assert json.loads((evidence / 'resource-summary.json').read_bytes()) == summary
    assert [item['phase'] for item in data] == ['initial', 'final']
    assert [item['phase'] for item in storage] == ['initial', 'final']
    assert summary['samples_written'] == len(data) and summary['storage_samples_written'] == len(storage)
    assert summary['outcome_received'] and summary['final_process_sample_written']
    assert summary['final_storage_sample_written']
    assert summary['sampler_active_wall_seconds'] > 0
    assert summary['sampler_thread_cpu_seconds'] > 0
    first, final = data
    assert first['policy_inputs']['rss_bytes'] > 0
    assert [member['identity'] for member in final['processes']['members']] == [given['identity']]
    assert first['processes']['matched_cpu_delta_seconds'] is None
    assert final['processes']['matched_cpu_delta_seconds'] >= 0
    assert final['monotonic_ns'] >= final['elapsed_at_monotonic_ns'] >= first['elapsed_at_monotonic_ns']
    assert final['storage']['campaign_tree']['file_count'] >= first['storage']['campaign_tree']['file_count']
    assert final['system_memory']['swap']['used_bytes'] >= 0
    assert summary['sampled_maxima']['rss_bytes'] == max(item['policy_inputs']['rss_bytes'] for item in data)
    before = (evidence / 'resource-samples.jsonl').read_bytes()
    with pytest.raises(supervisor.SupervisorError, match='reservation'):
        supervisor.launch_probe(campaign, base, run)
    assert (evidence / 'resource-samples.jsonl').read_bytes() == before


def test_measurement_failure_retains_other_sources_and_does_not_stop_probe(launch, monkeypatch):
    _, _, _, evidence = launch

    def fail():
        raise PermissionError(errno.EACCES, 'injected')
    monkeypatch.setattr(linux, 'temporary_filesystem', fail)
    status = supervisor.launch_probe(*launch[:3])
    assert status['status'] == 'probe_passed'
    assert status['resources']['stop_required']
    assert {'invalid:tmp_bytes', 'invalid:tmp_free_bytes'} <= set(status['resources']['stops'])
    stored = records(evidence, 'resource-storage.jsonl')[-1]
    assert stored['storage']['temporary_filesystem'] is None
    assert stored['storage']['campaign_filesystem']['available_bytes'] > 0
    assert stored['measurement_failures'][0]['status'] == 'unavailable'
    assert records(evidence)[-1]['policy_inputs']['rss_bytes'] > 0


def test_append_failure_preserves_first_line_and_independent_storage(launch, monkeypatch):
    _, _, _, evidence = launch
    original = sampler._append

    def append(stream, record):
        if 'processes' in record and record['phase'] == 'final':
            raise OSError(errno.ENOSPC, 'injected')
        return original(stream, record)
    monkeypatch.setattr(sampler, '_append', append)
    status = supervisor.launch_probe(*launch[:3])
    assert status['status'] == 'probe_passed' and status['resources']['stop_required']
    assert status['resources']['outcome_received'] and not status['resources']['final_process_sample_written']
    assert status['resources']['capture_failures'][0]['errno'] == errno.ENOSPC
    assert [item['phase'] for item in records(evidence)] == ['initial']
    assert [item['phase'] for item in records(evidence, 'resource-storage.jsonl')] == ['initial', 'final']


def test_summary_write_failure_retained_in_completion_and_fallback(launch, monkeypatch):
    _, _, _, evidence = launch
    write = supervisor._write_record

    def fail(fd, name, value):
        if name == 'resource-summary.json':
            raise OSError(errno.ENOSPC, 'injected')
        write(fd, name, value)

    monkeypatch.setattr(supervisor, '_write_record', fail)
    status = supervisor.launch_probe(*launch[:3])
    assert status['status'] == 'probe_passed' and status['resources']['stop_required']
    assert records(evidence) and records(evidence, 'resource-storage.jsonl')
    assert json.loads((evidence / 'resource-failure.json').read_bytes()) == status['resources']
    assert json.loads((evidence / 'completed.json').read_bytes()) == status


def test_live_periodic_storage_threshold_retained_even_if_final_recovers(launch, monkeypatch):
    _, _, _, evidence = launch
    append = sampler._append
    periodic_storage = Event()

    def observe(stream, record):
        append(stream, record)
        if 'processes' not in record and record['phase'] == 'periodic':
            periodic_storage.set()

    invocations = 0

    def temp_capacity():
        nonlocal invocations
        invocations += 1
        return {'used_bytes': 2 * GIB if invocations == 2 else 0,
                'total_bytes': 8 * GIB, 'available_bytes': 6 * GIB}

    receive = supervisor._receive

    def hold_checkpoint(channel):
        result = receive(channel)
        if result.get('status') == 'probe_passed':
            assert periodic_storage.wait(9)
        return result

    monkeypatch.setattr(sampler, '_append', observe)
    monkeypatch.setattr(linux, 'temporary_filesystem', temp_capacity)
    monkeypatch.setattr(supervisor, '_receive', hold_checkpoint)
    result = supervisor.launch_probe(*launch[:3])
    storage = records(evidence, 'resource-storage.jsonl')
    assert result['status'] == 'probe_passed'
    assert 'tmp' in result['resources']['stops']
    assert [item['phase'] for item in storage] == ['initial', 'periodic', 'final']
    assert storage[1]['assessment']['stops'] == ['tmp'] and storage[-1]['assessment']['stops'] == []
    assert result['resources']['sampled_maxima']['tmp_bytes'] == 2 * GIB
    times = [item['storage']['started_monotonic_ns'] for item in storage]
    assert times[1] - times[0] >= 5 * SECOND
    process = records(evidence)
    periodic = [item['monotonic_ns'] for item in process if item['phase'] == 'periodic']
    assert len(periodic) >= 4
    assert [item['sequence'] for item in process] == list(range(1, len(process) + 1))
    assert all(abs((b - a) - SECOND) < SECOND for a, b in zip(periodic, periodic[1:]))


def test_tracker_keeps_session_escape_across_lost_parent_and_gaps(proc):
    child = supervisor.ProcessIdentity(102, 1010, 101, 101, 101)
    proc[102] = child, process_fields()
    tracker = linux.ProcessTracker()
    observed, _, failures = linux.processes(LEADER, None, {}, tracker)
    assert child in observed and not failures

    escaped = replace(child, session_id=102, process_group=102, parent_pid=1)
    proc[102] = escaped, process_fields()
    for _ in range(2):
        observed, _, failures = linux.processes(LEADER, None, {}, tracker)
        assert set(observed) == {LEADER} and failures
        assert tracker.events[0]['kind'] == 'session_changed'
        assert tracker.events[0]['start_ticks'] == child.start_ticks
    del proc[102]
    observed, _, failures = linux.processes(LEADER, None, {}, tracker)
    assert set(observed) == {LEADER} and failures
    assert tracker.events[0]['kind'] == 'lost'

    proc[102] = replace(child, start_ticks=2000, session_id=10), process_fields(rss='999')
    observed, _, failures = linux.processes(LEADER, None, {}, tracker)
    assert set(observed) == {LEADER} and failures
    assert tracker.events[0]['kind'] == 'pid_reused'
    assert 102 not in tracker.members


@pytest.mark.parametrize('event', ['before_browse', 'during_browse', 'same_tick', 'bad_pidfd'])
def test_tracker_distinguishes_confirmed_exit_from_ambiguous_pid(proc, monkeypatch, event):
    child = supervisor.ProcessIdentity(102, 1010, 101, 101, 101)
    tracker = linux.ProcessTracker()
    tracker.members[102] = (child, 42)
    closed = []
    monkeypatch.setattr(os, 'close', closed.append)
    if event != 'before_browse':
        proc[102] = child, process_fields(rss='999')
    if event == 'during_browse':
        def read(pid):
            if pid == 102:
                raise FileNotFoundError
            return proc[pid]
        monkeypatch.setattr(supervisor, '_process_stat', read)

    def pidfd_ready(fd):
        if event == 'bad_pidfd' and fd == 42:
            raise OSError(errno.EBADF, 'injected')
        return fd == 42
    monkeypatch.setattr(supervisor, '_pidfd_exited', pidfd_ready)
    observed, _, failures = linux.processes(LEADER, None, {102: child}, tracker)
    assert set(observed) == {LEADER}
    assert bool(failures) == (event != 'before_browse')
    expected = {'before_browse': ['exited'], 'during_browse': ['exited'],
                'same_tick': ['exited', 'ambiguous_pid'], 'bad_pidfd': ['ambiguous_pid']}
    assert [entry['kind'] for entry in tracker.events] == expected[event]
    assert closed == ([] if event == 'bad_pidfd' else [42])


def test_real_cooperative_descendant_changes_group_and_session():
    import select
    import subprocess
    import sys

    child = """
import os
import sys

print(os.getpid(), flush=True)
for command in sys.stdin:
    if command.strip() == 'group':
        os.setpgid(0, 0)
    else:
        os.setpgid(0, os.getsid(0))
        os.setsid()
    print('ready', flush=True)
"""
    leader = """
import subprocess
import sys

with subprocess.Popen([sys.executable, '-B', '-c', sys.argv[1]], stdin=subprocess.PIPE,
                      stdout=subprocess.PIPE, text=True) as child:
    print(child.stdout.readline().strip(), flush=True)
    for command in sys.stdin:
        child.stdin.write(command)
        child.stdin.flush()
        print(child.stdout.readline().strip(), flush=True)
    child.stdin.close()
"""
    proc = subprocess.Popen([sys.executable, '-B', '-c', leader, child], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, text=True, start_new_session=True)
    tracker = linux.ProcessTracker()
    fd = None

    def receive():
        assert select.select([proc.stdout], [], [], 5)[0]
        return proc.stdout.readline().strip()

    def request(command):
        proc.stdin.write(command + '\n')
        proc.stdin.flush()
        assert receive() == 'ready'

    try:
        child_pid = int(receive())
        identity = supervisor.read_process_identity(proc.pid)
        fd = supervisor._optional_pidfd(proc.pid)
        measured, _, failures = linux.processes(identity, fd, {}, tracker)
        assert any(item.pid == child_pid for item in measured) and not failures
        child_fd = tracker.members[child_pid][1]

        request('group')
        measured, _, failures = linux.processes(identity, fd, {}, tracker)
        assert any(item.pid == child_pid for item in measured) and not failures
        assert tracker.members[child_pid][1] == child_fd
        assert tracker.events[0]['kind'] == 'group_changed'

        request('session')
        measured, _, failures = linux.processes(identity, fd, {}, tracker)
        assert set(measured) == {identity} and failures
        assert tracker.events[0]['kind'] == 'session_changed'
    finally:
        proc.stdin.close()
        try:
            assert proc.wait(timeout=5) == 0
        finally:
            proc.stdout.close()
            tracker.close()
            if fd is not None:
                os.close(fd)


@pytest.fixture
def stalled(monkeypatch):
    entered, release = Event(), Event()
    jobs = []
    original = sampler._submit

    def track(lane, *args):
        job = original(lane, *args)
        jobs.append(job)
        return job

    def block():
        entered.set()
        assert release.wait(8)

    monkeypatch.setattr(sampler, '_submit', track)
    monkeypatch.setattr(sampler, '_SHUTDOWN_WAIT', .25)
    try:
        yield entered, release, jobs, block
    finally:
        release.set()
        assert all(job.done.wait(3) for job in jobs)


def test_blocked_proc_or_cgroup_provider_leaves_control_responsive(launch, stalled, monkeypatch):
    import time

    entered, _, _, block = stalled
    real = linux.cgroup_memory

    def blocked(*args):
        block()
        return real(*args)

    monkeypatch.setattr(linux, 'cgroup_memory', blocked)
    start = time.monotonic()
    result = supervisor.launch_probe(*launch[:3])
    assert entered.is_set() and time.monotonic() - start < 2.5
    assert result['status'] == 'probe_passed'
    summary = result['resources']
    assert summary['supervision_status'] == 'stop_required'
    assert 'process' in summary['unfinished_io']
    assert summary['deadline_misses'][0]['stage'] == 'system_and_cgroup_memory'
    assert not summary['final_process_sample_written'] and summary['final_storage_sample_written']
    assert json.loads((launch[3] / 'completed.json').read_bytes())['resources'] == summary


@pytest.mark.parametrize('phase', ['initial', 'final'])
def test_delayed_filesystem_enumeration_has_separate_bound(phase, launch, stalled, monkeypatch):
    import time

    entered, _, _, block = stalled
    real = linux._tree_usage
    invocations = 0

    def blocked(*args):
        nonlocal invocations
        invocations += 1
        if invocations == (1 if phase == 'initial' else 2):
            block()
        return real(*args)

    monkeypatch.setattr(linux, '_tree_usage', blocked)
    monkeypatch.setattr(sampler, '_STARTUP_WAIT', .20)
    start = time.monotonic()
    result = supervisor.launch_probe(*launch[:3])
    assert entered.is_set() and time.monotonic() - start < 2.5
    summary = result['resources']
    assert summary['supervision_status'] == 'stop_required'
    assert 'storage' in summary['unfinished_io']
    assert summary['deadline_misses'][0]['stage'] == 'campaign_tree'
    assert summary['deadline_misses'][0]['phase'] == phase
    assert summary['final_process_sample_written'] and not summary['final_storage_sample_written']
    assert summary['storage_samples_written'] == (0 if phase == 'initial' else 1)


def test_blocked_append_retains_prior_evidence_without_late_rewrite(launch, stalled, monkeypatch):
    entered, release, jobs, block = stalled
    real = sampler._append

    def blocked(stream, record):
        if 'processes' in record and record['phase'] == 'final':
            block()
        return real(stream, record)

    monkeypatch.setattr(sampler, '_append', blocked)
    result = supervisor.launch_probe(*launch[:3])
    summary = result['resources']
    assert entered.is_set() and summary['supervision_status'] == 'stop_required'
    assert summary['deadline_misses'][0]['stage'] == 'process_evidence'
    path = launch[3] / 'resource-samples.jsonl'
    before = path.read_bytes()
    assert summary['samples_written'] == 1 and b'"phase":"final"' not in before
    release.set()
    assert all(job.done.wait(3) for job in jobs)
    assert path.read_bytes() == before
    assert summary['unfinished_io'] == ['process']


def test_blocked_summary_does_not_hang_or_confirm_supervision(launch, stalled, monkeypatch):
    entered, release, jobs, block = stalled
    real = supervisor._write_record

    def blocked(fd, name, value):
        if name == 'resource-summary.json':
            block()
        return real(fd, name, value)

    monkeypatch.setattr(supervisor, '_write_record', blocked)
    monkeypatch.setattr(sampler, 'EVIDENCE_WAIT', .25)
    result = supervisor.launch_probe(*launch[:3])
    assert entered.is_set() and result['status'] == 'probe_passed'
    summary = result['resources']
    assert summary['supervision_status'] == 'stop_required' and summary['unfinished_io'] == ['resource_summary']
    assert summary['final_process_sample_written'] and summary['final_storage_sample_written']
    assert json.loads((launch[3] / 'completed.json').read_bytes())['resources'] == summary
    release.set()
    assert all(job.done.wait(3) for job in jobs)
    assert not (launch[3] / 'resource-summary.json').exists()


def test_returned_evidence_error_is_not_reported_as_pending(launch, monkeypatch):
    write = supervisor._write_record

    def fail(fd, name, value):
        if name in ('resource-summary.json', 'resource-failure.json'):
            raise TimeoutError(errno.ETIMEDOUT, 'injected provider error')
        write(fd, name, value)

    monkeypatch.setattr(supervisor, '_write_record', fail)
    result = supervisor.launch_probe(*launch[:3])
    summary = result['resources']
    assert summary['supervision_status'] == 'stop_required'
    assert summary['capture_failures'][-1]['errno'] == errno.ETIMEDOUT
    assert not summary['unfinished_io']


def test_blocked_terminal_verification_cannot_publish_success_later(launch, stalled, monkeypatch):
    entered, release, jobs, block = stalled
    real = supervisor._read_record

    def blocked(fd, name):
        if name == 'probe-output.json':
            block()
        return real(fd, name)

    monkeypatch.setattr(supervisor, '_read_record', blocked)
    monkeypatch.setattr(sampler, 'EVIDENCE_WAIT', .25)
    with pytest.raises(supervisor.CompletionEvidenceError, match='not confirmed'):
        supervisor.launch_probe(*launch[:3])
    assert entered.is_set()
    failure = json.loads((launch[3] / 'failure.json').read_bytes())
    assert failure['error_type'] == 'CompletionEvidenceError' and failure['reaped']
    release.set()
    assert all(job.done.wait(3) for job in jobs)
    assert not (launch[3] / 'completed.json').exists()


def test_timeout_during_completion_fsync_is_a_failed_return_even_with_visible_bytes(launch, stalled, monkeypatch):
    entered, release, jobs, block = stalled
    fsync = os.fsync
    marker = launch[3] / 'completed.json'

    def blocked(fd):
        if os.readlink(f'/proc/self/fd/{fd}') == str(marker):
            block()
        return fsync(fd)

    monkeypatch.setattr(os, 'fsync', blocked)
    monkeypatch.setattr(sampler, 'EVIDENCE_WAIT', .25)
    with pytest.raises(supervisor.CompletionEvidenceError):
        supervisor.launch_probe(*launch[:3])
    assert entered.is_set() and json.loads(marker.read_bytes())['status'] == 'probe_passed'
    assert json.loads((launch[3] / 'failure.json').read_bytes())['error_type'] == 'CompletionEvidenceError'
    release.set()
    assert all(job.done.wait(3) for job in jobs)


def test_repeated_deadlines_retain_slots_and_worker_owns_descriptors(stalled, monkeypatch):
    import time

    _, release, jobs, block = stalled
    monkeypatch.setattr(sampler, 'EVIDENCE_WAIT', .05)
    fd, writer = os.pipe()
    resumed_inodes = []

    def blocked(work, owned):
        block()
        resumed_inodes.append(os.fstat(owned).st_ino)
        work.check()
        raise AssertionError('continued after deadline')

    expected = os.fstat(fd).st_ino
    try:
        for _ in range(2):
            with pytest.raises(TimeoutError):
                sampler.bounded_evidence(blocked, fd)
        mark = time.monotonic()
        with pytest.raises(OSError, match='workers are occupied'):
            sampler.bounded_evidence(blocked, fd)
        assert time.monotonic() - mark < .25
        assert all(not job.done.is_set() for job in jobs)
    finally:
        os.close(fd)
        os.close(writer)
    release.set()
    assert all(job.done.wait(3) for job in jobs)
    assert resumed_inodes == [expected, expected]
    assert all(job.cancelled.is_set() and job.error is None for job in jobs)
    assert sampler.bounded_evidence(lambda _: 'recovered') == 'recovered'


def test_in_memory_health_reports_pending_and_stale_observations(monkeypatch):
    recorder = sampler.ResourceSampler(None, None, None)
    assert recorder.check_health() == {
        'monitoring_ready': False, 'stop_required': True, 'stops': ['observations_pending']}
    recorder.sample_count = recorder.storage_count = 1
    recorder.workers['process'] = sampler._IOWork()
    recorder.next_process_ns = 2 * SECOND
    monkeypatch.setattr(sampler.time, 'monotonic_ns', lambda: 3 * SECOND)
    overdue = recorder.check_health()
    assert overdue['stop_required'] and not overdue['monitoring_ready']
    assert 'process_sampling_interval' in overdue['stops']
    recorder.workers['process'].done.set()
    assert 'process_sampling_interval' in recorder.check_health()['stops']


def test_waiting_for_outcome_checks_health_with_no_child_message(monkeypatch):
    import socket

    observations = []
    monkeypatch.setattr(supervisor, '_TIMEOUT', .16)
    monkeypatch.setattr(supervisor, '_HEALTH_INTERVAL', .03)
    token = supervisor._RECEIVE_HEALTH.set(lambda: observations.append('checked'))
    try:
        parent, child = socket.socketpair()
        with parent, child, pytest.raises(TimeoutError):
            supervisor._receive(parent)
        assert len(observations) >= 3
    finally:
        supervisor._RECEIVE_HEALTH.reset(token)
