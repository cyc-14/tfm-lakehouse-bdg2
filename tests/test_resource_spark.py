"""Focused D39/D44/D46 checks for the fixed Spark workload."""

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Event
from types import SimpleNamespace
import uuid

import pytest

from tfm_lakehouse import resource_sampler as sampling
from tfm_lakehouse import resource_spark as probe
from tfm_lakehouse import resource_supervisor as launcher
from tfm_lakehouse import scalability_workspace as workspace


@pytest.fixture
def prepared_run():
    assert 'TFM_REAL_CONFIG' not in os.environ
    campaign = workspace.authorize_campaign(os.environ['H06_LAUNCH_CAMPAIGN'], 'launcher-test')
    token = uuid.uuid4().hex
    name = 'spark-' + token
    base = campaign.root / 'temp' / ('pytest-' + token)
    evidence = campaign.root / 'evidence' / name
    assert not os.path.lexists(base)
    return campaign, base, name, evidence


def _launch(run):
    campaign, base, name, _ = run
    return launcher.launch_supervised_workload(campaign, base, name, handler=launcher._SPARK_HANDLER,
                                               parameters=launcher.SparkProbeParameters())


def test_spark_registry_is_supervised_only_and_has_no_overrides(monkeypatch):
    monkeypatch.setattr(launcher, 'preflight', lambda *_: pytest.fail('unexpected filesystem access'))
    with pytest.raises(launcher.SupervisorError, match='requires supervised'):
        launcher.launch_workload(None, None, 'run', handler=launcher._SPARK_HANDLER,
                                 parameters=launcher.SparkProbeParameters())
    with pytest.raises(launcher.SupervisorError, match='Invalid workload parameters'):
        launcher._configuration(None, None, 'run', launcher._SPARK_HANDLER, launcher.ProbeParameters('other.txt'))
    with pytest.raises(TypeError):
        launcher.SparkProbeParameters(master='external')


def test_gateway_home_cannot_change_the_registered_executable():
    trusted = Path('/invented/venv/pyspark/bin/spark-submit')
    marker = object()
    launched = []

    def popen(command, **kwargs):
        launched.append(command)
        return marker

    module = SimpleNamespace(Popen=popen)
    captured = []
    with probe._owned_gateway(module, captured, trusted):
        with pytest.raises(launcher.SupervisorError, match='pinned installation'):
            module.Popen(['/invented/outside/spark-submit'])
        assert not launched and not captured
        assert module.Popen([str(trusted), 'pyspark-shell']) is marker
    assert captured == [marker] and module.Popen is popen


def test_ticket_binds_exact_offline_spark_configuration(prepared_run):
    campaign, base, name, evidence = prepared_run
    config = launcher._configuration(campaign, base, name, launcher._SPARK_HANDLER, launcher.SparkProbeParameters())
    profile = config['spark']
    assert profile['master'] == 'local[2]' and profile['input_values'] == list(range(8))
    assert profile['temporary'] == str(campaign.root / 'temp')
    assert profile['event_log'] == str(evidence / 'spark-events')
    assert set(profile['jars_sha256']) == set(workspace.CAMPAIGN_JARS)
    assert all(len(digest) == 64 for digest in profile['jars_sha256'].values())
    assert not evidence.exists()


def test_initial_d44_rejection_starts_neither_spark_driver_nor_jvm(prepared_run, monkeypatch):
    assess = sampling.assess

    def injected(sample, *, storage_only=False):
        return assess(replace(sample, tmp_bytes=2 * 1024**3), storage_only=storage_only)

    monkeypatch.setattr(sampling, 'assess', injected)
    result = _launch(prepared_run)
    assert result['status'] == 'initial_rejected'
    assert not result['watchdog']['payload_granted']
    assert not (prepared_run[3] / 'spark-starting.json').exists()
    assert result['spark']['heap']['status'] == 'unavailable'


def test_interrupted_spark_initialization_preserves_evidence(prepared_run, monkeypatch):
    campaign, base, name, _ = prepared_run
    config = launcher._configuration(campaign, base, name, launcher._SPARK_HANDLER, launcher.SparkProbeParameters())
    evidence = launcher.reserve_evidence(campaign, name)
    primary = RuntimeError('injected initialization interruption')

    def interrupted():
        raise primary

    monkeypatch.setattr(probe, '_load_spark', interrupted)
    monkeypatch.setenv('SPARK_LOCAL_IP', '127.0.0.1')
    monkeypatch.setenv('PYTHONDONTWRITEBYTECODE', '1')
    with launcher._directory(evidence) as fd, pytest.raises(RuntimeError) as error:
        probe.spark_probe(fd, config)
    assert error.value is primary
    assert json.loads((evidence / 'spark-starting.json').read_bytes())['profile_sha256']
    shutdown = json.loads((evidence / 'spark-shutdown.json').read_bytes())
    assert shutdown['error_type'] == 'RuntimeError'
    assert shutdown['gateway_exit_code'] is None and not shutdown['gateway_collected']
    assert not (evidence / 'probe-output.json').exists()


def test_d25_closes_and_waits_for_gateway_without_replacing_primary():
    attempts = []
    primary = RuntimeError('primary')

    def close():
        attempts.append('close')
        raise OSError('secondary')

    def wait(*, timeout):
        assert timeout > 0
        attempts.append('wait')
        raise OSError('later')

    gateway = SimpleNamespace(stdin=SimpleNamespace(close=close), wait=wait)
    assert probe._close_gateway(gateway, primary) is primary
    assert attempts == ['close', 'wait']
    assert 'secondary' in primary.__notes__[0] and 'later' in primary.__notes__[1]


def test_optional_heap_probe_reports_a_block_without_blocking_return():
    entered, released = Event(), Event()

    def read():
        entered.set()
        assert released.wait(3)
        return {'used_bytes': 0, 'maximum_bytes': -1, 'committed_bytes': 1}

    owner = probe._HeapProbe(read)
    owner.thread.start()
    try:
        assert entered.wait(1)
        evidence = owner.finish()
        assert evidence['status'] == 'pending'
        assert evidence['count'] == 0 and evidence['latest'] is None
    finally:
        released.set()
        owner.thread.join(2)
    assert owner.latest['used_bytes'] == 0
    assert owner.latest['maximum_bytes'] is None


def test_event_counters_count_attempts_not_associations_and_keep_missing_and_zero():
    metrics = {'Shuffle Read Metrics': {
        'Local Bytes Read': 7, 'Remote Bytes Read': 3, 'Remote Bytes Read To Disk': 2,
    }, 'Shuffle Write Metrics': {'Shuffle Bytes Written': 12},
        'Memory Bytes Spilled': 11, 'Disk Bytes Spilled': 0}
    first = {'Event': 'SparkListenerTaskEnd', 'Stage ID': 2, 'Stage Attempt ID': 0,
             'Task Info': {'Task ID': 4}, 'Task Metrics': metrics}
    second = {'Event': 'SparkListenerTaskEnd', 'Stage ID': 2, 'Stage Attempt ID': 1,
              'Task Info': {'Task ID': 5}}
    result = probe.event_metrics([
        {'Event': 'SparkListenerJobStart', 'Job ID': 8, 'Stage IDs': [2, 9]},
        {'Event': 'SparkListenerTaskStart', 'Stage ID': 9, 'Stage Attempt ID': 0},
        first, first, second, {'Event': 'SparkListenerApplicationEnd'},
    ])
    assert result['status'] == 'complete' and result['job_ids'] == [8]
    assert result['associated_stage_ids'] == [2, 9] and result['executed_stage_attempts'] == [[2, 0], [2, 1]]
    assert result['started_stage_attempts'] == [[9, 0]]
    assert result['observed_task_ends'] == 2
    measured = result['task_counters']
    assert measured['shuffle_read_bytes']['observed_sum'] == 10
    assert measured['shuffle_write_bytes']['observed_sum'] == 12
    assert measured['memory_spill_bytes']['observed_sum'] == 11
    assert measured['disk_spill_bytes']['observed_sum'] == 0
    assert not measured['disk_spill_bytes']['all_observed_task_ends']
    missing = probe.event_metrics([second], readable=False)
    assert missing['status'] == 'partial'
    assert all(counter['observed_sum'] is None for counter in missing['task_counters'].values())


def test_spark_roles_keep_process_group_changes_on_the_same_instance():
    daemon = {'pid': 105, 'start_ticks': 2, 'session_id': 101, 'process_group': 105, 'parent_pid': 102}
    before_group = dict(daemon, process_group=101)
    reused = dict(daemon, start_ticks=9)

    def sample(identity, rss, cpu):
        return {'processes': {'members': [{'identity': identity, 'rss_bytes': rss,
                                          'cpu_seconds_since_start': cpu, 'cpu_delta_seconds': None}]}}

    measures = [sample(before_group, 8, .1), sample(daemon, 9, .3), sample(reused, 10, .4)]
    audit = {'exited_descendants': [], 'unresolved_processes': [], 'signals': []}
    roles, combined, other = probe._process_evidence(measures, [('python_daemon', daemon)], audit, None)
    assert other == [reused] and combined == 9
    assert roles[0]['supervisor_verified'] and roles[0]['observed_process_groups'] == [101, 105]
    assert roles[0]['observations'] == 2 and roles[0]['sampled_max_cpu_seconds_since_start'] == .3


def _diagnostic(result):
    return {key: result.get(key) for key in ('status', 'watchdog', 'spark', 'evidence_error')}


@pytest.mark.integration
def test_registered_spark_normal_attribution_metrics_and_shutdown(prepared_run):
    result = _launch(prepared_run)
    assert result['status'] == 'supervised_success', _diagnostic(result)
    spark = result['spark']
    assert spark['ready']['temporary'] == str(prepared_run[0].root / 'temp')
    assert spark['ready']['spark_local'] == str(prepared_run[0].root / 'spark-local')
    assert spark['shutdown']['gateway_collected'] and spark['shutdown']['gateway_exit_code'] == 0
    assert result['watchdog']['leader_reaped'] and not result['watchdog']['unresolved_processes']
    roles = {p['role'] for p in spark['processes']}
    assert roles == {'python_driver', 'jvm_gateway', 'python_daemon', 'python_worker'}
    assert all(p['supervisor_verified'] and p['observations'] and p['sampled_max_rss_bytes'] > 0
               for p in spark['processes'])
    assert all(p['identity']['session_id'] == result['identity']['session_id'] for p in spark['processes'])
    assert spark['sampled_max_combined_python_rss_bytes'] > 0
    samples = [json.loads(line) for line in (prepared_run[3] / 'resource-samples.jsonl').read_bytes().splitlines()]
    assert any(spark['starting']['monotonic_ns'] <= sample['monotonic_ns'] < spark['ready']['monotonic_ns']
               for sample in samples)
    assert any(spark['operation']['started_monotonic_ns'] <= sample['monotonic_ns'] <
               spark['shutdown']['started_monotonic_ns'] for sample in samples)
    assert samples[-1]['phase'] == 'final'
    assert samples[-1]['monotonic_ns'] >= spark['shutdown']['finished_monotonic_ns']
    latest = spark['heap']['latest']
    assert spark['heap']['count'] > 0 and latest['maximum_bytes'] > latest['used_bytes'] >= 0
    assert spark['heap']['sampled_max_used_bytes'] >= latest['used_bytes']
    events = spark['events']
    assert events['status'] == 'complete' and events['job_ids']
    assert len(events['executed_stage_attempts']) >= 2 and events['observed_task_ends'] >= 4
    for key in ('shuffle_read_bytes', 'shuffle_write_bytes'):
        assert events['task_counters'][key]['observed_sum'] > 0
    for key in ('memory_spill_bytes', 'disk_spill_bytes'):
        assert events['task_counters'][key]['observed_sum'] == 0
    assert (prepared_run[3] / 'completed.json').exists()


@pytest.mark.integration
def test_registered_spark_injected_rss_stop_preserves_partial_evidence(prepared_run, monkeypatch):
    evidence = prepared_run[3]
    assess, collect = sampling.assess, sampling.ResourceSampler._processes
    fired = Event()

    def observed(owner, *args):
        processes, membership = collect(owner, *args)
        if processes and len(list(evidence.glob('spark-worker-*.json'))) >= 2:
            try:
                workers = {json.loads(path.read_bytes())['worker']['pid']
                           for path in evidence.glob('spark-worker-*.json')}
            except (OSError, ValueError):
                return processes, membership
            observed_pids = {item['identity']['pid'] for item in processes['members']}
            if len(workers) >= 2 and workers <= observed_pids:
                fired.set()
        return processes, membership

    def injected(sample, *, storage_only=False):
        if fired.is_set() and not storage_only:
            sample = replace(sample, rss_bytes=4 * 1024**3)
        return assess(sample, storage_only=storage_only)

    monkeypatch.setattr(sampling.ResourceSampler, '_processes', observed)
    monkeypatch.setattr(sampling, 'assess', injected)
    unrelated = subprocess.Popen([sys.executable, '-B', '-c', 'import sys; sys.stdin.read()'],
                                 stdin=subprocess.PIPE, start_new_session=True)
    try:
        result = _launch(prepared_run)
        assert unrelated.poll() is None
    finally:
        unrelated.stdin.close()
        unrelated.wait(timeout=5)
    assert fired.is_set(), _diagnostic(result)
    assert result['status'] == 'resource_terminated', _diagnostic(result)
    audit = result['watchdog']
    assert audit['first_stop']['reason'] == 'rss'
    assert audit['leader_reaped'] and audit['collection_complete'] and not audit['unresolved_processes']
    signals = [event for event in audit['signals'] if event['result'] == 'sent']
    assert signals and all(event['pid'] != unrelated.pid for event in signals)
    assert all(event['identity']['session_id'] == result['identity']['session_id'] for event in signals)
    spark = result['spark']
    assert {p['role'] for p in spark['processes']} == {'python_driver', 'jvm_gateway', 'python_daemon', 'python_worker'}
    assert all(p['supervisor_verified'] for p in spark['processes'])
    collected = {(p['pid'], p['start_ticks']) for p in audit['exited_descendants']}
    assert {(p['identity']['pid'], p['identity']['start_ticks']) for p in spark['processes']
            if p['role'] != 'python_driver'} <= collected
    assert not (evidence / 'completed.json').exists()
    assert (evidence / 'failure.json').exists() and (evidence / 'resource-samples.jsonl').exists()
    assert (evidence / 'spark-starting.json').exists()


def test_streaming_events_accept_large_lines_and_keep_zero(tmp_path):
    import json
    data = [
        {'Event': 'large', 'padding': 'a' * 500000},
        {'Event': 'SparkListenerJobStart', 'Job ID': 1, 'Stage IDs': [2]},
        {'Event': 'SparkListenerTaskEnd', 'Stage ID': 2, 'Stage Attempt ID': 0,
         'Task Info': {'Task ID': 3}, 'Task Metrics': {'Memory Bytes Spilled': 0}},
        {'Event': 'SparkListenerApplicationEnd'},
    ]
    (tmp_path / 'events').write_bytes(b''.join(json.dumps(event).encode() + b'\n' for event in data))
    with launcher._directory(tmp_path) as fd:
        result = probe.read_event_metrics(fd, 'events')
    assert result['status'] == 'complete' and result['records'] == 4
    assert result['task_counters']['memory_spill_bytes']['observed_sum'] == 0
    assert result['task_counters']['disk_spill_bytes']['observed_sum'] is None


@pytest.mark.parametrize('ending', [b'{', b'not-json\n'])
def test_streaming_events_keep_partial_prefix(tmp_path, ending):
    (tmp_path / 'events').write_bytes(
        b'{"Event":"SparkListenerJobStart","Job ID":1,"Stage IDs":[2]}\n' + ending)
    with launcher._directory(tmp_path) as fd:
        result = probe.read_event_metrics(fd, 'events')
    assert result['status'] == 'partial' and result['job_ids'] == [1]
    assert result['failure']


@pytest.mark.parametrize(('tail', 'final', 'status'), [
    (b'broken\n', True, 'partial'),
    (b'{"Event":"SparkListenerTaskEnd","Stage ID":"invalid","Stage Attempt ID":0}\n', True, 'invalid'),
    (b'{"Event":"SparkListenerJobStart","Job ID":true,"Stage IDs":"12"}\n', True, 'invalid'),
    (b'{"missing":"event"}\n', True, 'invalid'),
    (b'{"Event":"SparkListenerTaskEnd","Stage ID":2,"Stage Attempt ID":0,'
     b'"Task Info":{"Task ID":3},"Task Metrics":{"Memory Bytes Spilled":"0"}}\n', True, 'invalid'),
    (b'{"Event":"other","value":NaN}\n', True, 'partial'),
    (b'', False, 'partial'),
], ids=['malformed-json', 'malformed-task', 'malformed-job', 'missing-event-name',
        'malformed-counter', 'non-json-constant', 'in-progress'])
def test_event_log_app_end_does_not_hide_invalid_or_in_progress_evidence(tmp_path, tail, final, status):
    (tmp_path / 'events').write_bytes(b'{"Event":"SparkListenerApplicationEnd"}\n' + tail)
    with launcher._directory(tmp_path) as fd:
        result = probe.read_event_metrics(fd, 'events', final=final)
    assert result['status'] == status
