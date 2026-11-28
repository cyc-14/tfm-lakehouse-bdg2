"""The fixed D46 Spark probe and its observational evidence."""

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
from threading import Event, Lock, Thread
import time

from tfm_lakehouse import resource_supervisor as launcher
from tfm_lakehouse import scalability_workspace as workspace


_MASTER = 'local[2]'
_EVENT_DIR = 'spark-events'
_VERSION = '3.5.7'
_WORKER_HOLD = 2.0
_GATEWAY_WAIT = 8
_HEAP_INTERVAL = .5
_HEAP_JOIN = .2


def _digest_jar(path):
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or not 0 < info.st_size <= 16 * 1024**2:
                raise launcher.SupervisorError('Invalid offline campaign jar')
            digest = hashlib.sha256()
            read = 0
            while block := stream.read(65536):
                launcher._check_io()
                read += len(block)
                if read > info.st_size:
                    raise launcher.SupervisorError('Offline campaign jar grew while reading')
                digest.update(block)
            if read != info.st_size:
                raise launcher.SupervisorError('Offline campaign jar changed while reading')
            return digest.hexdigest()
    except OSError as exc:
        raise launcher.SupervisorError('Offline campaign jar unavailable') from exc


def spark_profile(root, run_id):
    jars = root / 'ivy' / 'jars'
    return {
        'version': _VERSION, 'master': _MASTER, 'input_values': list(range(8)), 'partitions': 2,
        'temporary': str(root / 'temp'), 'spark_local': str(root / 'spark-local'),
        'event_log': str(root / 'evidence' / run_id / _EVENT_DIR),
        'jars_sha256': {name: _digest_jar(jars / name) for name in workspace.CAMPAIGN_JARS},
    }


def _dependency_path():
    # The child uses -I -B -S. Do not execute .pth hooks or load an external Spark.
    installed = workspace.PROJECT_ROOT / '.venv' / 'lib' / 'python3.11' / 'site-packages'
    if not installed.is_dir():
        raise launcher.SupervisorError('Pinned Spark dependencies are unavailable')
    if str(installed) not in sys.path:
        sys.path.append(str(installed))
    return installed


def _load_spark():
    installed = _dependency_path()
    import pyspark
    import pyspark.java_gateway as gateway
    from tfm_lakehouse.spark import spark_session

    spark_home = installed / 'pyspark'
    if pyspark.__version__ != _VERSION or Path(pyspark.__file__).resolve().parent != spark_home.resolve():
        raise launcher.SupervisorError('Unexpected PySpark installation')
    return pyspark.SparkContext, gateway, spark_session, spark_home / 'bin' / 'spark-submit'


class _SparkTerminated(BaseException):
    """Allow the dedicated driver to unwind its owned Spark and gateway state."""


@contextmanager
def _owned_gateway(gateway_module, capture, expected_command):
    # Capture the single pinned PySpark launch before Java publishes the
    # connection record, so interrupted initialization has an owned handle.
    popen = gateway_module.Popen

    def start(command, **kwargs):
        if capture:
            raise launcher.SupervisorError('Unexpected second Spark gateway')
        if not command or Path(command[0]) != expected_command:
            raise launcher.SupervisorError('Spark launcher is outside the pinned installation')
        proc = popen(command, **kwargs)
        capture.append(proc)
        return proc

    gateway_module.Popen = start
    try:
        yield
    finally:
        gateway_module.Popen = popen


def _close_gateway(proc, primary):
    first = primary
    for operation in (lambda: proc.stdin.close(), lambda: proc.wait(timeout=_GATEWAY_WAIT)):
        try:
            operation()
        except BaseException as failure:
            if first is None:
                first = failure
            else:
                launcher._note(first, 'Spark gateway cleanup failed', failure)
    return first


def _heap_values(jvm):
    usage = jvm.java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
    return {'used_bytes': int(usage.getUsed()), 'committed_bytes': int(usage.getCommitted()),
            'maximum_bytes': int(usage.getMax())}


class _HeapProbe:
    """An optional JVM read may occupy one daemon thread, independent of D44."""

    def __init__(self, read):
        self.read = read
        self.stop = Event()
        self.lock = Lock()
        self.latest = None
        self.peak = None
        self.count = 0
        self.error = None
        self.thread = Thread(target=self._poll, daemon=True, name='spark-heap-diagnostic')

    def _poll(self):
        while not self.stop.is_set():
            try:
                values = {k: value if type(value) is int and value >= 0 else None
                          for k, value in self.read().items()}
                sample = {'monotonic_ns': time.monotonic_ns(), **values}
                with self.lock:
                    self.latest = sample
                    used = sample['used_bytes']
                    if used is not None and (self.peak is None or used > self.peak):
                        self.peak = used
                    self.count += 1
            except Exception as exc:
                with self.lock:
                    self.error = type(exc).__name__
                break
            self.stop.wait(_HEAP_INTERVAL)

    def finish(self):
        self.stop.set()
        self.thread.join(_HEAP_JOIN)
        with self.lock:
            pending = self.thread.is_alive()
            return {'status': ('pending' if pending else 'unavailable' if not self.count else
                               'partial' if self.error else 'observed'), 'count': self.count,
                    'latest': self.latest, 'sampled_max_used_bytes': self.peak,
                    'error_type': self.error}


def _execute(spark, profile, found, session, evidence_dir):
    def map_partition(index, rows):
        import os
        from pathlib import Path
        import sys
        import tempfile
        import time
        from pyspark import TaskContext
        from tfm_lakehouse import resource_supervisor as own

        worker = own.read_process_identity(os.getpid())
        daemon = own.read_process_identity(worker.parent_pid)
        if ('TFM_REAL_CONFIG' in os.environ or worker.session_id != session or daemon.session_id != session or
                daemon.parent_pid != found or daemon.process_group != daemon.pid or
                worker.process_group != daemon.process_group or
                sys.executable != own._interpreter() or
                any(os.environ.get(key) != profile['temporary'] for key in ('TMPDIR', 'TMP', 'TEMP')) or
                tempfile.gettempdir() != profile['temporary']):
            raise RuntimeError('Spark worker left the pinned profile')
        task = TaskContext.get()
        with own._directory(Path(evidence_dir)) as fd:
            own._write_record(fd, f'spark-worker-{index}-{task.taskAttemptId()}.json', {
                'worker': asdict(worker), 'daemon': asdict(daemon), 'temporary': tempfile.gettempdir(),
                'stage_id': task.stageId(), 'partition': index, 'attempt': task.attemptNumber(),
            })
        # Keep both real workers observable across the one-second process cadence.
        time.sleep(_WORKER_HOLD)
        for value in rows:
            yield value % 2, 1

    return sorted(spark.sparkContext.parallelize(profile['input_values'], profile['partitions'])
                  .mapPartitionsWithIndex(map_partition)
                  .partitionBy(profile['partitions']).collect())


def spark_probe(evidence_fd, configuration):
    if 'TFM_REAL_CONFIG' in os.environ:
        raise launcher.SupervisorError('Real configuration must be unset')
    campaign = workspace.authorize_campaign(configuration['campaign_id'], configuration['owner_id'])
    profile = configuration['spark']
    if str(campaign.root) != configuration['root'] or profile != spark_profile(campaign.root, configuration['run_id']):
        raise launcher.SupervisorError('Spark profile differs from the authorized ticket')
    if configuration['handler'] == launcher._BRONZE_HANDLER:
        from tfm_lakehouse.bronze_calibration import calibration_binding

        if calibration_binding(campaign) != configuration['bronze']:
            raise launcher.SupervisorError('Bronze calibration binding differs')
    if configuration['handler'] == launcher._SILVER_HANDLER:
        from tfm_lakehouse.silver_calibration import binding

        if binding(campaign, configuration['run_id'], fresh=True) != configuration['silver']:
            raise launcher.SupervisorError('Silver calibration binding differs')
    real_silver = configuration['handler'] == launcher._REAL_SILVER_HANDLER
    if real_silver:
        from tfm_lakehouse.silver import real_silver_workload

        prepared = real_silver_workload.prepare(campaign, evidence_fd, configuration)
    profile_sha = launcher._digest(profile)
    event_dir = Path(profile['event_log'])
    os.mkdir(_EVENT_DIR, mode=0o700, dir_fd=evidence_fd)
    began = time.monotonic_ns()
    driver = launcher.read_process_identity(os.getpid())
    launcher._write_record(evidence_fd, 'spark-starting.json', {
        'driver': asdict(driver), 'profile_sha256': profile_sha, 'monotonic_ns': began,
    })
    os.environ.update(SPARK_LOCAL_IP='127.0.0.1', PYTHONDONTWRITEBYTECODE='1')
    owned = []
    shutdown_started = None
    interrupted = False
    failure = None

    def terminate(_signal, _frame):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            if shutdown_started is None:
                raise _SparkTerminated('Spark driver termination requested')

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        context, gateway_module, spark_session, expected_command = _load_spark()
        with _owned_gateway(gateway_module, owned, expected_command):
            with spark_session('tfm-d46-synthetic-probe', master=_MASTER, campaign=campaign,
                               event_log=event_dir) as spark:
                gateway = context._gateway
                if len(owned) != 1 or gateway.proc is not owned[0]:
                    raise launcher.SupervisorError('Spark did not return its own gateway')
                jvm = launcher.read_process_identity(gateway.proc.pid)
                if (jvm.parent_pid != driver.pid or jvm.session_id != driver.session_id or
                        Path(os.readlink(f'/proc/{jvm.pid}/exe')).name != 'java'):
                    raise launcher.SupervisorError('Spark gateway identity differs')
                if real_silver and _heap_values(spark.sparkContext._jvm)['maximum_bytes'] != 1024**3:
                    raise launcher.SupervisorError('Electrical Silver requires the existing 1 GiB JVM heap')
                ready = time.monotonic_ns()
                launcher._write_record(evidence_fd, 'spark-ready.json', {
                    'jvm': asdict(jvm), 'app_id': spark.sparkContext.applicationId,
                    'temporary': profile['temporary'], 'spark_local': profile['spark_local'],
                    'initialization_ns': ready - began, 'monotonic_ns': ready,
                })
                heap = _HeapProbe(lambda: _heap_values(spark.sparkContext._jvm))
                heap.thread.start()
                operation = time.monotonic_ns()
                primary = None
                try:
                    rows = _execute(spark, profile, jvm.pid, driver.session_id, str(event_dir.parent))
                    if rows != [(0, 1)] * 4 + [(1, 1)] * 4:
                        raise launcher.SupervisorError('Unexpected result from fixed Spark input')
                    probe_finished = time.monotonic_ns()
                    if configuration['handler'] == launcher._BRONZE_HANDLER:
                        from tfm_lakehouse.bronze_calibration import execute

                        execute(spark, campaign, evidence_fd, configuration['bronze'])
                    if configuration['handler'] == launcher._SILVER_HANDLER:
                        from tfm_lakehouse.silver_calibration import execute

                        execute(spark, campaign, evidence_fd, configuration['run_id'], configuration['silver'])
                    if real_silver:
                        real_silver_workload.execute(spark, campaign, evidence_fd, configuration, prepared)
                    launcher._write_record(evidence_fd, 'spark-operation.json', {
                        'rows': len(rows), 'operation_ns': time.monotonic_ns() - operation,
                        'worker_probe_ns': probe_finished - operation,
                        'started_monotonic_ns': operation,
                    })
                except BaseException as exc:
                    primary = exc
                    raise
                finally:
                    shutdown_started = time.monotonic_ns()
                    try:
                        launcher._write_record(evidence_fd, 'spark-heap.json', heap.finish())
                    except BaseException as secondary:
                        if primary is None:
                            raise
                        launcher._note(primary, 'Could not retain Spark heap evidence', secondary)
    except BaseException as exc:
        failure = exc
    finally:
        if shutdown_started is None:
            shutdown_started = time.monotonic_ns()
        if owned:
            failure = _close_gateway(owned[0], failure)
        try:
            closed = time.monotonic_ns()
            launcher._write_record(evidence_fd, 'spark-shutdown.json', {
                'gateway_exit_code': owned[0].returncode if owned else None,
                'gateway_collected': bool(owned and owned[0].returncode is not None),
                'interrupted': interrupted, 'shutdown_ns': closed - shutdown_started,
                'started_monotonic_ns': shutdown_started, 'finished_monotonic_ns': closed,
                'error_type': type(failure).__name__ if failure is not None else None,
                'error': str(failure)[:750] if failure is not None else None,
            })
        except BaseException as cleanup:
            if failure is None:
                failure = cleanup
            else:
                launcher._note(failure, 'Could not write Spark shutdown evidence', cleanup)
        try:
            signal.signal(signal.SIGTERM, previous)
        except BaseException as cleanup:
            if failure is None:
                failure = cleanup
            else:
                launcher._note(failure, 'Could not restore Spark driver signal handler', cleanup)
    if failure is not None:
        raise failure
    if interrupted or not owned or owned[0].returncode != 0:
        raise launcher.SupervisorError('Spark gateway shutdown did not complete normally')
    launcher._write_record(evidence_fd, 'probe-output.json', {'source': None, 'spark_profile_sha256': profile_sha})


_COUNTERS = {
    'shuffle_read_bytes': (('Shuffle Read Metrics', 'Local Bytes Read'),
                           ('Shuffle Read Metrics', 'Remote Bytes Read')),
    'shuffle_write_bytes': (('Shuffle Write Metrics', 'Shuffle Bytes Written'),),
    'memory_spill_bytes': (('Memory Bytes Spilled',),),
    'disk_spill_bytes': (('Disk Bytes Spilled',),),
}


def _counter(metrics, fields):
    values = []
    for path in fields:
        value = metrics
        for key in path:
            if value is None:
                break
            if not isinstance(value, dict):
                raise ValueError('Invalid Spark task metrics')
            value = value.get(key)
        if value is not None:
            if type(value) is not int or value < 0:
                raise ValueError('Invalid Spark task counter')
            values.append(value)
    return sum(values) if len(values) == len(fields) else None


def _event_id(value):
    if type(value) is not int or value < 0:
        raise ValueError('Invalid Spark event identifier')
    return value


def _invalid_json_constant(_value):
    raise ValueError('Invalid JSON constant in Spark event')


def event_metrics(events, *, readable=True):
    """Aggregate observed task attempts without confusing association with execution."""
    jobs, associated, executed, attempts = set(), set(), set(), set()
    started = set()
    totals = dict.fromkeys(_COUNTERS, 0)
    present = dict.fromkeys(_COUNTERS, 0)
    ended = False
    for event in events:
        kind = event.get('Event')
        if not isinstance(kind, str) or not kind:
            raise ValueError('Invalid Spark event name')
        if kind == 'SparkListenerJobStart':
            stages = event['Stage IDs']
            if not isinstance(stages, list):
                raise ValueError('Invalid Spark stage identifiers')
            jobs.add(_event_id(event['Job ID']))
            associated.update(_event_id(stage) for stage in stages)
        elif kind == 'SparkListenerApplicationEnd':
            ended = True
        elif kind in ('SparkListenerTaskStart', 'SparkListenerTaskEnd'):
            stage = (_event_id(event['Stage ID']), _event_id(event['Stage Attempt ID']))
            if kind == 'SparkListenerTaskStart':
                started.add(stage)
                continue
            executed.add(stage)
            task = (*stage, _event_id(event['Task Info']['Task ID']))
            counters = {name: _counter(event.get('Task Metrics'), fields)
                        for name, fields in _COUNTERS.items()}
            if task in attempts:
                continue
            attempts.add(task)
            for name, measurement in counters.items():
                if measurement is not None:
                    totals[name] += measurement
                    present[name] += 1
    return {
        'status': ('complete' if ended and readable else 'partial' if jobs or started or executed else 'unavailable'),
        'application_end': ended,
        'job_ids': sorted(jobs) if jobs or ended else None,
        'associated_stage_ids': sorted(associated) if jobs or ended else None,
        'started_stage_attempts': [list(stage) for stage in sorted(started)] if started or ended else None,
        'executed_stage_attempts': [list(stage) for stage in sorted(executed)] if executed or ended else None,
        'observed_task_ends': len(attempts) if attempts or ended else None,
        'task_counters': {
            name: {'observed_sum': totals[name] if present[name] else None, 'reported_attempts': present[name],
                   'all_observed_task_ends': bool(attempts) and present[name] == len(attempts)}
            for name in _COUNTERS
        },
    }


def _read_jsonl(fd, name, *, cap=4 * 1024**2):
    """Read an exclusive, bounded log, preserving complete prefix records."""
    records = []
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        launcher._check_io()
        with os.fdopen(os.open(name, flags, dir_fd=fd), 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                return records, False
            remaining = cap
            while remaining > 0:
                launcher._check_io()
                line = stream.readline(min(remaining + 1, 256 * 1024))
                if not line:
                    return records, True
                if not line.endswith(b'\n') or len(line) > remaining:
                    break
                value = json.loads(line)
                if not isinstance(value, dict):
                    break
                records.append(value)
                remaining -= len(line)
    except (OSError, ValueError):
        pass
    return records, False


def read_event_metrics(fd, name, *, final=True):
    """Stream a bounded event log and retain valid prefix observations.

    Limits are 64 MiB, 2 MiB per line and 200,000 events. Sets retained by
    the event aggregator are bounded by the file and event count limits.
    """
    state = {'complete': False, 'failure': None, 'records': 0}

    def records():
        try:
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            with os.fdopen(os.open(name, flags, dir_fd=fd), 'rb') as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError('Invalid event file')
                consumed = 0
                while True:
                    launcher._check_io()
                    line = stream.readline(2 * 1024**2 + 1)
                    if not line:
                        state['complete'] = True
                        return
                    consumed += len(line)
                    if consumed > 64 * 1024**2 or len(line) > 2 * 1024**2:
                        raise ValueError('Event size limit reached')
                    if not line.endswith(b'\n'):
                        raise ValueError('Incomplete event line')
                    state['records'] += 1
                    if state['records'] > 200000:
                        raise ValueError('Event count limit reached')
                    value = json.loads(line, parse_constant=_invalid_json_constant)
                    if not isinstance(value, dict):
                        raise ValueError('Invalid event')
                    yield value
        except (OSError, ValueError, RecursionError) as exc:
            state['failure'] = type(exc).__name__

    try:
        result = event_metrics(records())
    except (KeyError, TypeError, ValueError) as exc:
        return {'status': 'invalid', 'failure': type(exc).__name__, 'records': state['records']}
    if not state['complete'] or not final:
        result['status'] = 'partial' if state['records'] else 'unavailable'
    result['failure'] = state['failure']
    result['records'] = state['records']
    return result


def _observation_file(fd, name):
    try:
        return launcher._read_record(fd, name)
    except (OSError, ValueError, launcher.SupervisorError):
        return None


def _key(identity):
    return tuple(identity[name] for name in ('pid', 'start_ticks', 'session_id'))


def _process_evidence(records, claims, audit, driver_identity):
    supervised, groups = {}, {}

    def register(identity):
        key = _key(identity)
        supervised[key] = identity
        groups.setdefault(key, set()).add(identity['process_group'])

    for identity in audit['exited_descendants'] + audit['unresolved_processes']:
        if 'start_ticks' in identity and identity.get('verification') != 'unconfirmed':
            register(identity)
    for signal in audit['signals']:
        if signal['identity'] is not None:
            register(signal['identity'])
    if driver_identity is not None:
        register(driver_identity)
    roles = {_key(identity): role for role, identity in claims}
    observations = {identity: [] for identity in roles}
    python_rss = []
    for record in records:
        sample = {_key(item['identity']): item for item in (record.get('processes') or {}).get('members', ())}
        for item in sample.values():
            register(item['identity'])
        for key in roles.keys() & sample.keys():
            observations[key].append(sample[key])
        # Aggregate concurrent Python RSS only with complete coverage of the
        # Python instances declared in the evidence. Earlier partial samples
        # are retained in the D44 JSONL, but do not become a zero here.
        python = [key for key, role in roles.items() if role != 'jvm_gateway']
        if python and all(key in sample and sample[key]['rss_bytes'] is not None for key in python):
            python_rss.append(sum(sample[key]['rss_bytes'] for key in python))
    return [
        {'role': roles[key], 'identity': identity,
         'supervisor_verified': identity['process_group'] in groups.get(key, ()),
         'observed_process_groups': sorted(groups.get(key, ())),
         'observations': len(observations[key]),
         'sampled_max_rss_bytes': max((sample['rss_bytes'] for sample in observations[key]
                                       if sample['rss_bytes'] is not None), default=None),
         'sampled_max_cpu_seconds_since_start': max(
             (sample['cpu_seconds_since_start'] for sample in observations[key]
              if sample['cpu_seconds_since_start'] is not None), default=None),
         'observed_cpu_delta_seconds': _sum_available(sample['cpu_delta_seconds'] for sample in observations[key])}
        for key, identity in sorted({_key(identity): identity for _, identity in claims}.items())
    ], max(python_rss, default=None), [supervised[key] for key in sorted(supervised.keys() - roles.keys())]


def _sum_available(values):
    available = [value for value in values if value is not None]
    return sum(available) if available else None


def read_spark_evidence(fd, result):
    """Corroborate child-reported roles with independent supervisor evidence."""
    start, ready, operation, heap, shutdown = (
        _observation_file(fd, name) for name in (
            'spark-starting.json', 'spark-ready.json', 'spark-operation.json',
            'spark-heap.json', 'spark-shutdown.json'))
    workers = []
    for name in sorted(os.listdir(fd)):
        launcher._check_io()
        if re.fullmatch(r'spark-worker-[01]-[0-9]{1,20}\.json', name):
            record = _observation_file(fd, name)
            if record is not None:
                workers.append(record)
    claims = []
    if start:
        claims.append(('python_driver', start['driver']))
    if ready:
        claims.append(('jvm_gateway', ready['jvm']))
    for record in workers:
        claims += [('python_daemon', record['daemon']), ('python_worker', record['worker'])]
    samples, _ = _read_jsonl(fd, 'resource-samples.jsonl')
    processes, python_max, other = _process_evidence(samples, claims, result['watchdog'], result.get('identity'))
    events = None
    if ready and re.fullmatch(r'local-[0-9]+', ready['app_id']):
        try:
            event_fd = os.open(_EVENT_DIR, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY, dir_fd=fd)
            try:
                for name in (ready['app_id'], ready['app_id'] + '.inprogress'):
                    candidate = read_event_metrics(event_fd, name, final=name == ready['app_id'])
                    if candidate['records'] or candidate['status'] == 'invalid':
                        events = candidate
                        break
            finally:
                os.close(event_fd)
        except OSError:
            pass
    if events is None:
        events = event_metrics((), readable=False)
    return {'starting': start, 'ready': ready, 'operation': operation, 'shutdown': shutdown,
            'heap': heap if heap is not None else {'status': 'unavailable', 'latest': None},
            'events': events, 'processes': processes, 'worker_tasks': workers, 'other_supervised_processes': other,
            'sampled_max_combined_python_rss_bytes': python_max}


def verify_spark_evidence(fd, result, evidence):
    profile = result['configuration']['spark']
    digest = launcher._digest(profile)
    marker = launcher._read_record(fd, 'probe-output.json')
    if marker != {'source': None, 'spark_profile_sha256': digest}:
        raise launcher.SupervisorError('Spark output differs from the authorized ticket')
    if profile != spark_profile(Path(result['configuration']['root']), result['configuration']['run_id']):
        raise launcher.SupervisorError('Offline Spark profile changed during execution')
    start, ready, operation, shutdown = (evidence[key] for key in ('starting', 'ready', 'operation', 'shutdown'))
    if not all((start, ready, operation, shutdown)):
        raise launcher.SupervisorError('Required Spark lifecycle evidence is missing')
    if (start['profile_sha256'] != digest or start['driver'] != result['identity'] or
            ready['temporary'] != profile['temporary'] or ready['spark_local'] != profile['spark_local'] or
            operation['rows'] != len(profile['input_values']) or not shutdown['gateway_collected'] or
            shutdown['gateway_exit_code'] != 0 or shutdown['interrupted']):
        raise launcher.SupervisorError('Spark lifecycle differs from the authorized profile')
    roles = {p['role'] for p in evidence['processes']}
    verified = (roles == {'python_driver', 'jvm_gateway', 'python_daemon', 'python_worker'} and
                all(p['supervisor_verified'] for p in evidence['processes']))
    tasks = evidence['worker_tasks']
    return (verified and {task['partition'] for task in tasks} == {0, 1} and
            all(task['temporary'] == profile['temporary'] for task in tasks))
