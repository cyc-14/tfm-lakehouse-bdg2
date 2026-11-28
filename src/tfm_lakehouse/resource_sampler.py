"""Retain D44 observations from a verified synthetic workload session."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import os
from pathlib import Path
from threading import BoundedSemaphore, Event, Lock, Thread
import time

from tfm_lakehouse import resource_linux as linux
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse.resource_limits import ResourceSample, assess


_NS_PER_SECOND = 1_000_000_000
PROCESS_INTERVAL_NS = _NS_PER_SECOND
STORAGE_INTERVAL_NS = 5 * _NS_PER_SECOND
_CLOCK_RECORD = 'resource-clock.json'
_LOG_RECORD = 'resource-samples.jsonl'
_STORAGE_RECORD = 'resource-storage.jsonl'
_SUMMARY_RECORD = 'resource-summary.json'
_FS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_STARTUP_WAIT = 1.0
_SHUTDOWN_WAIT = 1.0
EVIDENCE_WAIT = 1.0
# The synchronous launcher can be called concurrently. A wedged worker keeps
# its slot until it really exits. A later launch is never queued behind it.
_SLOTS = {lane: BoundedSemaphore(2) for lane in (
    'process', 'storage', 'evidence', 'identity', 'signal', 'launch')}


class _IOWork:
    def __init__(self):
        self.done = Event()
        self.cancelled = Event()
        self.activity = None
        self.error = None
        self.close_error = None
        self.result = None

    def begin(self, stage, phase):
        self.activity = (stage, phase, time.monotonic_ns())

    def stage(self, stage):
        _, phase, started = self.activity
        self.activity = (stage, phase, started)

    def check(self):
        if self.cancelled.is_set():
            raise _IOCancelled


class _IOCancelled(Exception):
    """A worker may finish the current syscall and must skip further work."""


class EvidenceDeadline(TimeoutError):
    """The waiting conductor could not confirm that evidence I/O returned."""


def _submit(lane, target, *descriptors):
    slot = _SLOTS[lane]
    if not slot.acquire(blocking=False):
        raise OSError(f'All {lane} I/O workers are occupied')
    work = _IOWork()
    owned = []
    ownership = Lock()
    entered = False

    def release():
        try:
            for fd in owned:
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError as exc:
                        if work.close_error is None:
                            work.close_error = exc
        finally:
            slot.release()
            work.done.set()

    def run():
        nonlocal entered
        with ownership:
            if work.cancelled.is_set():
                return
            entered = True
        token = supervisor._IO_CHECK.set(work.check)
        try:
            work.check()
            work.result = target(work, *owned)
        except _IOCancelled:
            pass
        except Exception as exc:
            work.error = exc
        finally:
            supervisor._IO_CHECK.reset(token)
            release()

    try:
        for fd in descriptors:
            owned.append(os.dup(fd) if fd is not None else None)
        Thread(target=run, name=f'd44-resource-{lane}', daemon=True).start()
    except BaseException:
        with ownership:
            work.cancelled.set()
            if not entered:
                release()
        raise
    return work


def bounded_evidence(target, *descriptors, published_on_return=False):
    """Run one transaction against owned FDs, with bounded admission and wait."""
    work = _submit('evidence', target, *descriptors)
    try:
        if not work.done.wait(EVIDENCE_WAIT):
            raise EvidenceDeadline('Evidence I/O did not complete within its deadline')
    except BaseException:
        work.cancelled.set()
        raise
    if work.error is not None:
        raise work.error
    if work.close_error is not None and not published_on_return:
        raise work.close_error
    return work.result


def _campaign_clock(root_fd, now_ns, *, nonblocking=False):
    domain = {'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
              'time_namespace': os.stat('/proc/self/ns/time').st_ino}
    evidence_fd = os.open('evidence', _FS, dir_fd=root_fd)
    try:
        fcntl.flock(evidence_fd, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        supervisor._check_io()
        try:
            anchor = supervisor._read_record(evidence_fd, _CLOCK_RECORD)
        except FileNotFoundError:
            anchor = dict(domain, started_monotonic_ns=now_ns)
            supervisor._write_record(evidence_fd, _CLOCK_RECORD, anchor)
        started = anchor.get('started_monotonic_ns')
        if (not domain['boot_id'] or type(started) is not int or started < 0
                or not supervisor._same(anchor, dict(domain, started_monotonic_ns=started))):
            raise linux.InvalidMeasurement('Campaign clock domain or start time differs')
        return started
    finally:
        os.close(evidence_fd)


def _capture(metric, read, failures):
    try:
        return read()
    except (*linux.READ_ERRORS, supervisor.SupervisorError, OverflowError) as exc:
        failures.append(linux.issue(metric, exc))
        return None


def _advance(deadline, now, interval):
    """Keep deadlines on a monotonic grid without accumulating catch-up work."""
    return deadline + ((now - deadline) // interval + 1) * interval


def _elapsed(started_ns, now_ns, metric, failures):
    if started_ns is not None and 0 <= started_ns <= now_ns:
        return (now_ns - started_ns) / _NS_PER_SECOND
    if started_ns is not None:
        failures.append(linux.issue(metric, linux.InvalidMeasurement('Monotonic clock regressed')))
    return None


def _open_stream(evidence_fd, name):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_APPEND | os.O_CLOEXEC
    fd = os.open(name, flags, 0o600, dir_fd=evidence_fd)
    try:
        supervisor._check_io()
        os.fsync(evidence_fd)
        return os.fdopen(fd, 'wb', buffering=0)
    except BaseException:
        os.close(fd)
        raise


def _append(stream, record):
    # The completed lines remain readable if the last write is interrupted.
    remaining = memoryview(supervisor._encode(record) + b'\n')
    while remaining:
        supervisor._check_io()
        written = stream.write(remaining)
        if not written:
            raise OSError('Short evidence write')
        remaining = remaining[written:]
    supervisor._check_io()
    os.fsync(stream.fileno())


def _storage_values(storage):
    tree = storage['campaign_tree'] or {}
    campaign = storage['campaign_filesystem'] or {}
    temp = storage['temporary_filesystem'] or {}
    return {
        'tmp_bytes': temp.get('used_bytes'), 'campaign_bytes': tree.get('allocated_bytes'),
        'campaign_free_bytes': campaign.get('available_bytes'), 'tmp_free_bytes': temp.get('available_bytes'),
    }


class ResourceSampler:
    """Collect into exclusive streams for a restricted launch.

    The launcher binds the verified leader with `start`. The workers own their
    descriptors and the control thread only reads or updates in-memory state.
    """

    def __init__(self, root, root_fd, evidence_fd, *, real_silver=None):
        self.root = root
        self.root_fd = root_fd
        self.evidence_fd = evidence_fd
        self.real_silver = real_silver
        self.identity = None
        self.pidfd = None
        self.operation_started_ns = None
        self.outcome_ns = None
        self.campaign_started_ns = None
        self.clock_failures = []
        self.stream = None
        self.storage_stream = None
        self.previous = {}
        self.members = linux.ProcessTracker()
        self.storage = None
        self.warnings = set()
        self.stops = set()
        self.stop_order = []
        self.maxima = {}
        self.sample_count = 0
        self.storage_count = 0
        self.wall_ns = 0
        self.cpu_ns = 0
        self.overhead_lock = Lock()
        self.capture_failures = {}
        self.stop = Event()
        self.checkpoint = Event()
        self.wake = {'process': Event(), 'storage': Event()}
        self.member_observer = None
        self.prepared = Event()
        self.workers = {}
        self.final_samples = set()
        self.unfinished = {}
        self.next_process_ns = None
        self.next_storage_ns = None
        self.last_monotonic_ns = None
        self.last_storage_ns = None
        self.summary = None

    def _processes(self, failures):
        data = _capture('process_membership', lambda: linux.processes(
            self.identity, self.pidfd,
            {pid: identity for (pid, _), (identity, _) in self.previous.items()}, self.members), failures)
        if self.member_observer is not None:
            self.member_observer(self.members)
        if data is None:
            self.previous = {}
            return None, {}
        observed, memberships, errors = data
        failures.extend(errors)
        clock_ticks = _capture('cpu_ticks', lambda: os.sysconf('SC_CLK_TCK'), failures)
        if clock_ticks is not None and clock_ticks <= 0:
            failures.append(linux.issue('cpu_ticks', linux.InvalidMeasurement('Invalid CPU clock rate')))
            clock_ticks = None
        records = []
        rss_complete = (self.identity in observed and not any(
            error['metric'] in ('process_membership', 'rss_bytes') for error in errors))
        rss_total = sum(metrics['rss_bytes'] for metrics in observed.values()) if rss_complete else None
        for identity, metrics in observed.items():
            counter = metrics['cpu_ticks']
            # Reparenting and group changes do not reset a verified instance's CPU counter.
            key = (identity.pid, identity.start_ticks)
            _, previous = self.previous.get(key, (None, None))
            delta = None
            if clock_ticks and counter is not None and previous is not None:
                if counter >= previous:
                    delta = (counter - previous) / clock_ticks
                else:
                    failures.append(dict(linux.issue(
                        'cpu_delta', linux.InvalidMeasurement('CPU counter regressed')), pid=identity.pid))
            group = memberships.get(identity)
            records.append({
                'identity': asdict(identity), 'rss_bytes': metrics['rss_bytes'],
                'cpu_seconds_since_start': None if counter is None or not clock_ticks else counter / clock_ticks,
                'cpu_delta_seconds': delta,
                'cgroup': None if group is None else {'version': group[0], 'path': str(group[1])},
            })
        self.previous = {(identity.pid, identity.start_ticks): (identity, metrics['cpu_ticks'])
                         for identity, metrics in observed.items()}
        deltas = [record['cpu_delta_seconds'] for record in records
                  if record['cpu_delta_seconds'] is not None]
        return {'scope': 'verified_workload_session', 'rss_bytes': rss_total,
                'matched_cpu_delta_seconds': sum(deltas) if deltas else None,
                'matched_cpu_process_count': len(deltas),
                'members': records, 'membership_events': self.members.events}, memberships

    def _memory(self, memberships, failures):
        system = _capture('system_memory', linux.system_memory, failures)
        if system is not None:
            system, errors = system
            failures.extend(errors)
        groups = _capture('cgroup_memory', lambda: linux.cgroup_memory(
            memberships, self.identity.pid, self.pidfd, self.members), failures)
        if groups is not None:
            groups, errors = groups
            failures.extend(errors)
        complete = (system and system['memory'] is not None and memberships and groups is not None
                    and not any(error['metric'] in ('process_membership', 'cgroup_membership',
                                                    'cgroup_memory') for error in failures))
        fraction = None
        cgroup_ratios = [group['utilization_fraction'] for group in groups or ()
                         if group.get('utilization_fraction') is not None]
        maximum_cgroup_ratio = max(cgroup_ratios, default=None)
        if complete:
            memory = system['memory']
            # Keep raw cgroup ratios in evidence even if a hard limit is
            # temporarily exceeded. Only the bounded policy input saturates.
            fraction = min(1.0, max(memory['used_bytes'] / memory['total_bytes'],
                                    maximum_cgroup_ratio or 0.0))
        return system, {'scope': 'shared_cgroup_hierarchies', 'groups': groups,
                        'maximum_visible_utilization_fraction': maximum_cgroup_ratio}, fraction

    def _add_overhead(self, wall, cpu):
        with self.overhead_lock:
            self.wall_ns += time.monotonic_ns() - wall
            self.cpu_ns += time.thread_time_ns() - cpu

    def _aggregate(self, warnings, stops, values):
        with self.overhead_lock:
            self.warnings.update(warnings)
            self._latch(stops)
            for key in ('rss_bytes', 'tmp_bytes', 'campaign_bytes', 'effective_memory_fraction'):
                value = values.get(key)
                if value is not None and (key not in self.maxima or value > self.maxima[key]):
                    self.maxima[key] = value

    def _latch(self, reasons):
        for reason in reasons:
            if reason not in self.stops:
                self.stops.add(reason)
                self.stop_order.append((reason, time.monotonic_ns()))

    def stopping_reasons(self):
        with self.overhead_lock:
            return tuple(self.stop_order)

    def checkpoint_ready(self):
        with self.overhead_lock:
            return len(self.final_samples) == 2

    def request_checkpoint(self):
        self.checkpoint.set()
        for wake in self.wake.values():
            wake.set()

    def _campaign_storage(self):
        usage = linux.campaign_storage(self.root, self.root_fd)
        if self.real_silver and self.real_silver['mode'] == 'real':
            from tfm_lakehouse.silver.real_silver_boundary import checked_directory, _identity

            mount = linux._mount_id(self.root_fd)
            for key, label in (('output', 'publication'), ('evidence', 'evidence')):
                path = Path(self.real_silver[key])
                workspace._reject_submounts(path)
                with checked_directory(path, mount) as fd:
                    if _identity(fd) != self.real_silver['reservations'][label]:
                        raise linux.InvalidMeasurement('Electrical Silver write directory changed')
                    blocks, files = linux._tree_usage(fd, mount)
                with checked_directory(path, mount) as fd:
                    if _identity(fd) != self.real_silver['reservations'][label]:
                        raise linux.InvalidMeasurement('Electrical Silver write directory changed')
                workspace._reject_submounts(path)
                usage['allocated_bytes'] += blocks * 512
                usage['file_count'] += files
        return usage

    def _refresh_storage(self, phase, work):
        now, cpu = time.monotonic_ns(), time.thread_time_ns()
        work.begin('campaign_tree', phase)
        utc = datetime.now(timezone.utc).isoformat()
        failures = []

        def measure(metric, read):
            work.check()
            work.stage(metric)
            return _capture(metric, read, failures)

        storage = {
            'started_monotonic_ns': now,
            'scope': {'campaign_tree': 'authorized_campaign',
                      'campaign_filesystem': 'filesystem_containing_campaign',
                      'temporary_filesystem': 'filesystem_containing_tmp'},
            'campaign_tree': measure('campaign_tree', self._campaign_storage),
            'campaign_filesystem': measure('campaign_filesystem', lambda: linux.filesystem(self.root_fd)),
            'temporary_filesystem': measure('tmp_filesystem', linux.temporary_filesystem),
        }
        if self.real_silver and self.real_silver['mode'] == 'real':
            storage['scope']['campaign_tree'] = 'authorized_campaign_and_electrical_silver_destinations'
        work.check()
        finished = time.monotonic_ns()
        storage['finished_monotonic_ns'] = finished
        storage['utc'] = utc
        if self.next_storage_ns is None:
            self.next_storage_ns = now
        late = (now - self.next_storage_ns >= STORAGE_INTERVAL_NS or
                finished - now >= STORAGE_INTERVAL_NS or
                finished - self.next_storage_ns >= 2 * STORAGE_INTERVAL_NS)
        if late:
            failures.append(linux.issue('storage_sampling_interval',
                                        linux.InvalidMeasurement('Storage sampling deadline missed')))
        if now >= self.next_storage_ns:
            self.next_storage_ns = _advance(self.next_storage_ns, now, STORAGE_INTERVAL_NS)
        # Publish the completed snapshot and its failures together. The process
        # sampler can keep its cadence while a filesystem observation is running.
        self.storage = storage, failures
        values = _storage_values(storage)
        assessment = assess(ResourceSample(None, values['tmp_bytes'], values['campaign_bytes'],
                                           None, None, None, values['campaign_free_bytes'],
                                           values['tmp_free_bytes']), storage_only=True)
        stops = (*assessment.stops, 'storage_sampling_interval') if late else assessment.stops
        self._aggregate(assessment.warnings, stops, values)
        if self.storage_stream is not None and 'storage_evidence' not in self.capture_failures:
            try:
                work.check()
                work.stage('storage_evidence')
                _append(self.storage_stream, {
                    'contract_version': 1, 'sequence': self.storage_count + 1, 'phase': phase,
                    'storage': storage, 'measurement_failures': failures, 'policy_inputs': values,
                    'assessment': {'warnings': assessment.warnings, 'stops': stops,
                                   'stop_required': bool(stops)},
                })
                with self.overhead_lock:
                    self.storage_count += 1
                    if phase == 'final':
                        self.final_samples.add('storage')
            except (OSError, ValueError) as exc:
                self._record_capture_failure('storage_evidence', exc)
        if time.monotonic_ns() - now >= STORAGE_INTERVAL_NS:
            self._aggregate((), ('storage_sampling_interval',), {})
        self._add_overhead(now, cpu)
        work.activity = None

    def _observation(self, now, utc, phase, work=None):
        failures = list(self.clock_failures)
        late = []
        if self.last_monotonic_ns is not None and now < self.last_monotonic_ns:
            late.append('monotonic_clock')
        self.last_monotonic_ns = now
        if self.next_process_ns is None:
            self.next_process_ns = now
        if now - self.next_process_ns >= PROCESS_INTERVAL_NS:
            late.append('process_sampling_interval')
        if now >= self.next_process_ns:
            self.next_process_ns = _advance(self.next_process_ns, now, PROCESS_INTERVAL_NS)
        process, memberships = self._processes(failures)
        if work is not None:
            work.check()
            work.stage('system_and_cgroup_memory')
        system, cgroups, memory = self._memory(memberships, failures)
        if work is not None:
            work.check()
        storage, storage_failures = self.storage or (None, [linux.issue(
            'storage', OSError('No confirmed storage observation'))])
        failures.extend(storage_failures)
        finished = time.monotonic_ns()
        storage_ns = storage['started_monotonic_ns'] if storage is not None else None
        storage_age = finished - storage_ns if storage_ns is not None else None
        stale_storage = storage_age is not None and storage_age >= 2 * STORAGE_INTERVAL_NS
        if stale_storage:
            failures.append(linux.issue('storage_sampling_interval',
                                        linux.InvalidMeasurement('Storage observation is stale')))
        if stale_storage or any(failure['metric'] == 'storage_sampling_interval'
                                for failure in storage_failures):
            late.append('storage_sampling_interval')
        if finished - now >= PROCESS_INTERVAL_NS and 'process_sampling_interval' not in late:
            late.append('process_sampling_interval')
        storage_values = _storage_values(storage or dict.fromkeys(
            ('campaign_tree', 'campaign_filesystem', 'temporary_filesystem')))
        if stale_storage:
            for field in storage_values:
                storage_values[field] = None
        elapsed_at = now if self.outcome_ns is None else min(now, self.outcome_ns)
        sample = ResourceSample(
            rss_bytes=process['rss_bytes'] if process is not None else None,
            effective_memory_fraction=memory,
            operation_seconds=_elapsed(self.operation_started_ns, elapsed_at, 'operation_clock', failures),
            campaign_seconds=_elapsed(self.campaign_started_ns, elapsed_at, 'campaign_clock', failures),
            **storage_values,
        )
        assessment = assess(sample)
        stops = (*assessment.stops, *late)
        values = asdict(sample)
        self._aggregate(assessment.warnings, stops, values)
        storage_refreshed = self.last_storage_ns != storage_ns
        self.last_storage_ns = storage_ns
        return {
            'contract_version': 1, 'sequence': self.sample_count + 1, 'phase': phase,
            'utc': utc, 'monotonic_ns': now, 'finished_monotonic_ns': finished,
            'elapsed_at_monotonic_ns': elapsed_at,
            'workload_session_id': self.identity.session_id, 'processes': process,
            'system_memory': system, 'cgroup_memory': cgroups, 'storage': storage,
            'storage_refreshed': storage_refreshed,
            'storage_age_seconds': None if storage_age is None else storage_age / _NS_PER_SECOND,
            'policy_inputs': values, 'measurement_failures': failures,
            'assessment': {'warnings': assessment.warnings, 'stops': stops, 'stop_required': bool(stops)},
        }

    def observe(self, phase, work):
        """Write one observation and retain prior records if an append fails."""
        if self.stream is None or 'process_evidence' in self.capture_failures:
            return
        now, cpu = time.monotonic_ns(), time.thread_time_ns()
        work.begin('process_membership', phase)
        try:
            work.check()
            record = self._observation(now, datetime.now(timezone.utc).isoformat(), phase, work)
            work.check()
            work.stage('process_evidence')
            _append(self.stream, record)
            with self.overhead_lock:
                self.sample_count += 1
                if phase == 'final':
                    self.final_samples.add('process')
            if time.monotonic_ns() - now >= PROCESS_INTERVAL_NS:
                self._aggregate((), ('process_sampling_interval',), {})
        except _IOCancelled:
            raise
        except Exception as exc:
            self._record_capture_failure('process_evidence', exc)
        finally:
            self._add_overhead(now, cpu)
            work.activity = None

    def _record_capture_failure(self, metric, error):
        with self.overhead_lock:
            self.capture_failures.setdefault(metric, linux.issue(metric, error))
            self._latch(('resource_capture',))

    def _poll_processes(self, work, evidence_fd, pidfd):
        self.pidfd = pidfd
        work.begin('process_evidence_open', 'initial')
        try:
            with _authorized_root(self.root, self.run_id, evidence_fd):
                work.check()
                with _open_stream(evidence_fd, _LOG_RECORD) as self.stream:
                    self.observe('initial', work)
                    self._poll('process', self.observe, work)
                    work.begin('process_evidence_close', 'shutdown')
        finally:
            self.members.close()

    def _poll_storage(self, work, evidence_fd):
        work.begin('campaign_boundary', 'initial')
        wall, cpu = time.monotonic_ns(), time.thread_time_ns()
        try:
            with _authorized_root(self.root, self.run_id, evidence_fd) as self.root_fd:
                work.check()
                work.stage('campaign_clock')
                self.campaign_started_ns = _capture(
                    'campaign_clock', lambda: _campaign_clock(self.root_fd, wall), self.clock_failures)
                work.check()
                work.stage('storage_evidence_open')
                try:
                    self.storage_stream = _open_stream(evidence_fd, _STORAGE_RECORD)
                except OSError as exc:
                    self._record_capture_failure('storage_evidence', exc)
                self._add_overhead(wall, cpu)
                try:
                    self._refresh_storage('initial', work)
                    work.check()
                    self.prepared.set()
                    self._poll('storage', self._refresh_storage, work)
                finally:
                    work.begin('storage_evidence_close', 'shutdown')
                    if self.storage_stream is not None:
                        self.storage_stream.close()
        finally:
            self.prepared.set()

    def _poll(self, lane, observe, work):
        attempted_checkpoint = False
        wake = self.wake[lane]
        while lane != 'process' or 'process_evidence' not in self.capture_failures:
            work.check()
            if (not attempted_checkpoint and self.outcome_ns is not None and
                    (self.stop.is_set() or self.checkpoint.is_set())):
                attempted_checkpoint = True
                observe('final', work)
            if self.stop.is_set():
                break
            next_due = self.next_process_ns if lane == 'process' else self.next_storage_ns
            delay = max(0, next_due - time.monotonic_ns()) / _NS_PER_SECOND
            if wake.wait(delay):
                wake.clear()
            else:
                observe('periodic', work)

    def _worker(self, lane, target, *descriptors):
        try:
            work = _submit(lane, target, *descriptors)
        except (OSError, RuntimeError) as exc:
            self._record_capture_failure(lane + '_worker', exc)
            return None
        with self.overhead_lock:
            self.workers[lane] = work
        return work

    def prepare(self, run_id):
        """Allow a bounded initial observation while withholding workload authority."""
        self.run_id = run_id
        work = self._worker('storage', self._poll_storage, self.evidence_fd)
        if work is not None and not self.prepared.wait(_STARTUP_WAIT):
            self._timeout('storage', work, 'initialization')
            work.cancelled.set()

    def start(self, identity: supervisor.ProcessIdentity, pidfd: int | None,
              operation_started_ns: int):
        """Bind this recorder to the leader only after the launcher verifies it."""
        self.identity, self.operation_started_ns = identity, operation_started_ns
        self._worker('process', self._poll_processes, self.evidence_fd, pidfd)

    def _timeout(self, lane, work, deadline):
        activity = work.activity
        stage, phase, started = activity if activity is not None else ('worker', None, None)
        with self.overhead_lock:
            self.unfinished.setdefault(lane, {
                'lane': lane, 'stage': stage, 'phase': phase,
                'started_monotonic_ns': started, 'deadline': deadline,
            })
            self._latch((lane + '_sampling_interval',))
        self._record_capture_failure(lane + '_' + deadline, TimeoutError(f'{lane} I/O deadline expired'))

    def check_health(self):
        """Classify overdue monitoring using only memory and a monotonic clock."""
        now = time.monotonic_ns()
        with self.overhead_lock:
            workers = tuple(self.workers.items())
        for lane, work in workers:
            activity = work.activity
            if work.done.is_set():
                error = work.error or work.close_error
                if error is not None:
                    self._record_capture_failure(lane + '_worker', error)
            else:
                interval = PROCESS_INTERVAL_NS if lane == 'process' else STORAGE_INTERVAL_NS
                next_due = self.next_process_ns if lane == 'process' else self.next_storage_ns
                if ((activity is not None and now - activity[2] >= interval) or
                        (next_due is not None and now - next_due >= interval)):
                    self._timeout(lane, work, 'observation')
        with self.overhead_lock:
            confirmed = bool(self.sample_count and self.storage_count)
            stops = self.stops if confirmed else self.stops | {'observations_pending'}
            return {'monitoring_ready': confirmed and not self.stops,
                    'stop_required': bool(stops), 'stops': sorted(stops)}

    def finish(self):
        """Request cooperative shutdown and wait only until a single deadline."""
        self.stop.set()
        for wake in self.wake.values():
            wake.set()
        deadline = time.monotonic() + _SHUTDOWN_WAIT
        for lane, work in self.workers.items():
            if not work.done.wait(max(0, deadline - time.monotonic())):
                self._timeout(lane, work, 'shutdown')
                work.cancelled.set()
        self.check_health()
        if self.identity is None:
            self._aggregate((), ('unverified_workload_session',), {})
        with self.overhead_lock:
            if self.outcome_ns is not None:
                self._latch('missing_final_' + lane + '_sample'
                            for lane in ('process', 'storage') if lane not in self.final_samples)
            return self._snapshot()

    def _snapshot(self):
        """Copy confirmed state while holding the state lock, never across I/O."""
        stop_required = bool(self.stops)
        return {
            'contract_version': 1, 'samples_written': self.sample_count,
            'storage_samples_written': self.storage_count,
            'sampled_maxima': dict(self.maxima),
            'sampler_active_wall_seconds': self.wall_ns / _NS_PER_SECOND,
            'sampler_thread_cpu_seconds': self.cpu_ns / _NS_PER_SECOND,
            'warnings': sorted(self.warnings), 'stops': sorted(self.stops),
            'stop_required': stop_required,
            'supervision_status': 'stop_required' if stop_required else 'observed',
            'capture_failures': [dict(failure) for failure in self.capture_failures.values()],
            'deadline_misses': [dict(item) for item in self.unfinished.values()],
            'unfinished_io': sorted(lane for lane, work in self.workers.items() if not work.done.is_set()),
            'outcome_received': self.outcome_ns is not None,
            'final_process_sample_written': 'process' in self.final_samples,
            'final_storage_sample_written': 'storage' in self.final_samples,
        }

    def persist(self, summary):
        """Write a fixed snapshot. The launch result accounts for an unconfirmed write."""
        def failed_summary(error, *, pending=False):
            failed = dict(summary, stop_required=True, supervision_status='stop_required',
                          stops=sorted(set(summary['stops']) | {'resource_capture'}))
            failed['capture_failures'] = [*summary['capture_failures'], linux.issue('resource_summary', error)]
            if pending:
                failed['unfinished_io'] = sorted([*summary['unfinished_io'], 'resource_summary'])
            return failed

        def publish(work, evidence_fd):
            with _authorized_root(self.root, self.run_id, evidence_fd):
                work.check()
                try:
                    supervisor._write_record(evidence_fd, _SUMMARY_RECORD, summary)
                except OSError as exc:
                    fallback = failed_summary(exc)
                    work.check()
                    supervisor._write_record(evidence_fd, 'resource-failure.json', fallback)
                    return fallback
            return summary

        try:
            return bounded_evidence(publish, self.evidence_fd)
        except (*linux.READ_ERRORS, supervisor.SupervisorError, RuntimeError) as exc:
            self._record_capture_failure('resource_summary', exc)
            # Keep the same collection snapshot on timeout. A late worker
            # cannot change the counts or classify this return as successful.
            return failed_summary(exc, pending=isinstance(exc, EvidenceDeadline))


@contextmanager
def _authorized_root(root, run_id, evidence_fd):
    if (workspace._lexical_root(root) not in workspace._AUTHORIZED or
            workspace._real_config_present()):
        raise linux.InvalidMeasurement('Sampler campaign is not authorized')
    workspace._id(run_id)
    with linux.campaign_directory(root) as root_fd:
        parent_fd = os.open('evidence', _FS, dir_fd=root_fd)
        try:
            run_fd = os.open(run_id, _FS, dir_fd=parent_fd)
            try:
                if (linux._mount_id(root_fd) != linux._mount_id(parent_fd) or
                        linux._mount_id(parent_fd) != linux._mount_id(run_fd) or
                        not os.path.samestat(os.fstat(evidence_fd), os.fstat(run_fd))):
                    raise linux.InvalidMeasurement('Sampler evidence directory differs')
            finally:
                os.close(run_fd)
        finally:
            os.close(parent_fd)
        yield root_fd


@contextmanager
def record_workload(root: Path, run_id: str, evidence_fd: int, *, real_silver=None) -> Iterator[ResourceSampler]:
    """Bind resource evidence to the verified launch without controlling it."""
    sampler = ResourceSampler(root, None, evidence_fd, real_silver=real_silver)
    primary = None
    try:
        sampler.prepare(run_id)
        yield sampler
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            sampler.summary = sampler.persist(sampler.finish())
        except BaseException as cleanup:
            if primary is None:
                raise
            supervisor._note(primary, 'Could not finalize resource evidence', cleanup)
