"""D46 enforcement for the restricted cooperative launcher."""

from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
import os
from pathlib import Path
import signal
import socket
import subprocess
from threading import Event, Lock
import time

from tfm_lakehouse import resource_linux as linux
from tfm_lakehouse import resource_sampler as sampling
from tfm_lakehouse import resource_supervisor as launcher
from tfm_lakehouse.resource_limits import CAMPAIGN_STOP_SECONDS, OPERATION_STOP_SECONDS, RESOURCE_STOPS


GRACE_SECONDS = 30
_TICK = .05
_CHECK_SECONDS = 1
_COLLECT_SECONDS = 1
_NS = 10**9


@dataclass
class _Member:
    identity: launcher.ProcessIdentity
    fd: int | None
    attempts: dict = field(default_factory=dict)


class _SpawnDeadline(TimeoutError):
    """The child has not been handed off and its identity is unconfirmed."""


def _spawn_bounded(socket_fd, evidence_fd, ticket):
    """Keep ownership with the launch worker until the conductor accepts it."""
    available, accepted = Event(), Event()
    result = {}

    def start(work, channel, fd):
        proc = None
        try:
            command = launcher._command(channel, fd)
            launcher._write_record(fd, 'launch.json', {'ticket': ticket, 'command': command})
            work.check()
            proc = launcher._spawn(command, channel, fd)
            result['proc'] = proc
        except BaseException as exc:
            result['error'] = exc
        finally:
            available.set()
        if proc is not None:
            accepted.wait(2 * _CHECK_SECONDS)
            if work.cancelled.is_set() or not accepted.is_set():
                # The already closed parent socket denies the grant. Do not
                # signal or wait indefinitely for a child without a receipt.
                try:
                    proc.wait(timeout=launcher._TIMEOUT + _COLLECT_SECONDS)
                except subprocess.TimeoutExpired:
                    pass

    deadline = time.monotonic() + _CHECK_SECONDS
    work = sampling._submit('launch', start, socket_fd, evidence_fd)
    try:
        if not available.wait(max(0, deadline - time.monotonic())):
            raise _SpawnDeadline('Launch worker did not hand off a child before the deadline')
        if 'error' in result:
            raise result['error']
    except BaseException:
        work.cancelled.set()
        raise
    finally:
        accepted.set()
    return result['proc'], work, deadline


def _identity_now(identity, fd, session):
    if fd is None:
        raise linux.InvalidMeasurement('Process has no owned pidfd')
    if launcher._pidfd_exited(fd):
        raise ProcessLookupError('Verified process has exited')
    try:
        current, fields = launcher._process_stat(identity.pid)
    except FileNotFoundError as exc:
        if launcher._pidfd_exited(fd):
            raise ProcessLookupError('Verified process has exited') from exc
        raise
    if (current.pid != identity.pid or current.start_ticks != identity.start_ticks or
            current.session_id != session):
        raise linux.InvalidMeasurement('Current process identity or session differs')
    if fields[0] in (b'Z', b'X', b'x') or launcher._pidfd_exited(fd):
        raise ProcessLookupError('Verified process has exited')
    return current


class _Watchdog:
    """Drive the control path in memory and delegate potentially blocked work."""

    def __init__(self, proc, identity, fd, *, clock=None, wait=None):
        self.proc = proc
        self.identity = identity
        self.clock = clock if clock is not None else time.monotonic_ns
        self.wait = wait if wait is not None else time.sleep
        self.lock = Lock()
        self.members = {identity.pid: _Member(identity, os.dup(fd) if fd is not None else None)}
        self.reasons = {}
        self.jobs = {}
        self.closed = False
        self.payload_granted = False
        self.outcome_received = False
        self.scan_clear = False
        self.next_scan = 0
        self.signal_not_before = None
        self.unknown = ()
        self.returncode = None
        self.collection_complete = False
        self.unfinished = set()

    def fail(self, reason, kind='monitoring', *, at=None, pid=None):
        with self.lock:
            self.reasons.setdefault((reason, pid), {
                'reason': reason, 'kind': kind, 'pid': pid,
                'monotonic_ns': self.clock() if at is None else at,
            })

    def first(self):
        with self.lock:
            return next(iter(self.reasons.values()), None)

    def _retain(self, identity, fd):
        with self.lock:
            if self.closed:
                return
            previous = self.members.get(identity.pid)
        if previous is None:
            owned = os.dup(fd) if fd is not None else None
            try:
                with self.lock:
                    if self.closed:
                        return
                    previous = self.members.get(identity.pid)
                    if previous is None:
                        self.members[identity.pid] = _Member(identity, owned)
                        owned = None
                        self.scan_clear = False
            finally:
                if owned is not None:
                    os.close(owned)
        if previous is not None and previous.identity.start_ticks != identity.start_ticks:
            self.fail('tracked_pid_reused', pid=identity.pid)
        elif fd is None:
            self.fail('member_pidfd_unavailable', pid=identity.pid)

    def observe_members(self, tracker):
        # Called on the sampling thread while it still owns these descriptors.
        for identity, fd in tracker.members.values():
            self._retain(identity, fd)
        if tracker.suspects:
            self._remember_unknown(asdict(identity) for identity in tracker.suspects)
            self.fail('membership_discovery_incomplete')

    def _remember_unknown(self, identities):
        with self.lock:
            if self.closed:
                return self.unknown
            proved = {(member.identity.pid, member.identity.start_ticks)
                      for member in self.members.values() if member.fd is not None}
            unverified = {(identity['pid'], identity['start_ticks']): identity
                          for identity in (*self.unknown, *identities)}
            self.unknown = tuple(identity for key, identity in unverified.items() if key not in proved)
            if self.unknown:
                self.scan_clear = False
            return self.unknown

    def _sync_health(self, sampler, started):
        health = sampler.check_health()
        for reason, at in sampler.stopping_reasons():
            self.fail(reason, 'resource' if reason in RESOURCE_STOPS else 'monitoring', at=at)
        now = self.clock()
        if (sampler.outcome_ns or now) - started >= OPERATION_STOP_SECONDS * _NS:
            self.fail('operation_time', 'resource')
        if sampler.campaign_started_ns is not None and now - sampler.campaign_started_ns >= CAMPAIGN_STOP_SECONDS * _NS:
            self.fail('campaign_time', 'resource')
        return health

    def _live(self):
        with self.lock:
            members = tuple(self.members.values())
        live = []
        for member in members:
            if member.identity.pid == self.identity.pid and self.returncode is not None:
                continue
            try:
                if member.fd is None or not launcher._pidfd_exited(member.fd):
                    live.append(member)
            except OSError:
                self.fail('pidfd_poll_failed', pid=member.identity.pid)
                live.append(member)
        return live

    def _scan(self, work, known, *fds):
        """Recheck the session without relying on resource collection or RSS."""
        session = self.identity.session_id
        anchor = None
        for member, fd in zip(known, fds, strict=True):
            try:
                _identity_now(member.identity, fd, session)
            except (OSError, ValueError, IndexError):
                continue
            anchor = member.identity, fd
            break
        suspects = []
        members, failures = linux._discover(self.identity, {}, outside_session=suspects if anchor else None)
        unknown = [asdict(identity) for identity in suspects]
        owned = {member.identity.pid: member.identity for member in known}
        with ExitStack() as descriptors:
            discovered = []
            for pid, identity in sorted(members.items()):
                work.check()
                if pid == self.identity.pid:
                    continue
                if pid in owned and owned[pid].start_ticks == identity.start_ticks:
                    continue
                try:
                    current, fields = launcher._process_stat(pid)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                if current == identity and fields[0] in (b'Z', b'X', b'x'):
                    continue
                if pid in owned or anchor is None:
                    unknown.append(asdict(identity))
                    continue
                try:
                    fd = launcher._optional_pidfd(pid)
                    if fd is not None:
                        descriptors.callback(os.close, fd)
                    current = _identity_now(identity, fd, session)
                    discovered.append((current, fd))
                except ProcessLookupError:
                    continue
                except (OSError, ValueError, IndexError):
                    unknown.append(asdict(identity))
            if discovered:
                # Keep the same live kernel instance at both ends of discovery.
                try:
                    _identity_now(anchor[0], anchor[1], session)
                except (OSError, ValueError, IndexError):
                    unknown.extend(asdict(identity) for identity, _ in discovered)
                else:
                    for identity, fd in discovered:
                        work.check()
                        self._retain(identity, fd)
        return unknown, bool(failures)

    def _send_signals(self, work, pending, *fds):
        for (member, sig), fd in zip(pending, fds, strict=True):
            work.check()
            pid = member.identity.pid
            status = 'unverified'
            current = None
            at = None
            confirmed = None
            try:
                current = _identity_now(member.identity, fd, self.identity.session_id)
                work.check()
                at = self.clock()
                with self.lock:
                    member.attempts[sig] = {'result': 'requested', 'monotonic_ns': at,
                                            'identity': asdict(current)}
                # Revalidation immediately precedes a signal through the owned
                # descriptor. PID reuse cannot redirect this syscall.
                work.check()
                signal.pidfd_send_signal(fd, sig)
                confirmed = self.clock()
                status = 'sent'
            except ProcessLookupError:
                status = 'exited'
            except (OSError, ValueError, IndexError) as exc:
                self.fail('signal_failed' if current is not None else 'signal_identity_unverified',
                          pid=pid)
                status = type(exc).__name__
            with self.lock:
                member.attempts[sig] = {'result': status, 'monotonic_ns': at,
                                        'confirmed_monotonic_ns': confirmed,
                                        'identity': asdict(current) if current is not None else None}

    def _submit_scan(self, live, now):
        known = tuple(live)
        try:
            work = sampling._submit('identity', lambda task, *fds: self._scan(task, known, *fds),
                                    *(member.fd for member in known))
        except (OSError, RuntimeError):
            self.fail('membership_worker_unavailable')
        else:
            self.jobs['identity'] = work, now + _CHECK_SECONDS * _NS, not known
        self.next_scan = now + _CHECK_SECONDS * _NS

    def _submit_signals(self, pending, now):
        name = 'term' if pending[0][1] == signal.SIGTERM else 'kill'
        with self.lock:
            for member, sig in pending:
                member.attempts[sig] = {'result': 'pending', 'monotonic_ns': None, 'identity': None}
        try:
            work = sampling._submit('signal', lambda task, *fds: self._send_signals(task, pending, *fds),
                                    *(member.fd for member, _ in pending))
        except (OSError, RuntimeError):
            self.fail('signal_worker_unavailable')
            with self.lock:
                for member, sig in pending:
                    member.attempts[sig]['result'] = 'worker_unavailable'
        else:
            self.jobs[name] = work, now + _CHECK_SECONDS * _NS, False

    def _collect_jobs(self, now):
        for name, (work, deadline, empty_scan) in tuple(self.jobs.items()):
            if work.done.is_set():
                del self.jobs[name]
                if work.error is not None or work.close_error is not None:
                    self.fail(name + '_worker_failed')
                    continue
                if name == 'identity' and work.result is not None:
                    unknown, incomplete = work.result
                    retained = self._remember_unknown(unknown)
                    if incomplete or unknown:
                        self.fail('membership_discovery_incomplete')
                    self.scan_clear = empty_scan and not incomplete and not retained
            elif now >= deadline:
                work.cancelled.set()
                self.fail(name + '_deadline')

    def _pending_signals(self, live, now):
        pending = []
        with self.lock:
            for member in live:
                if member.fd is None:
                    continue
                term = member.attempts.get(signal.SIGTERM)
                if term is None:
                    pending.append((member, signal.SIGTERM))
                elif (signal.SIGKILL not in member.attempts and term['result'] == 'sent' and
                      now - term['confirmed_monotonic_ns'] >= GRACE_SECONDS * _NS):
                    pending.append((member, signal.SIGKILL))
        return pending

    def stop(self, sampler, started):
        """Keep supervising until exit is confirmed or the bounded stop ends."""
        entered = self.clock()
        self.signal_not_before = entered + int(_TICK * _NS)
        # The first request can follow a bounded discovery attempt. A late
        # member without its own full TERM grace remains unresolved.
        deadline = entered + (GRACE_SECONDS + _CHECK_SECONDS + _COLLECT_SECONDS) * _NS
        had_live = True
        while True:
            now = self.clock()
            self._sync_health(sampler, started)
            self._collect_jobs(now)
            self.returncode = self.proc.poll()
            live = self._live()
            if had_live and not live:
                self.next_scan = now
            had_live = bool(live)
            if not live and self.returncode is not None and self.scan_clear and not self.unknown:
                self.collection_complete = True
                break
            if now >= deadline:
                self.fail('termination_not_confirmed')
                break
            if 'identity' not in self.jobs and now >= self.next_scan:
                self._submit_scan(live, now)
            if now >= self.signal_not_before and hasattr(signal, 'pidfd_send_signal'):
                pending = self._pending_signals(live, now)
                for name, sig in (('term', signal.SIGTERM), ('kill', signal.SIGKILL)):
                    wave = [(member, kind) for member, kind in pending if kind == sig]
                    if wave and name not in self.jobs:
                        self._submit_signals(wave, now)
            self.wait(_TICK)
        self.seal()

    def run(self, channel, grant, sampler, started):
        """Gate the payload, observe it, then verify the terminal checkpoint."""
        if self.members[self.identity.pid].fd is None:
            self.fail('leader_pidfd_unavailable')
        if not hasattr(signal, 'pidfd_send_signal'):
            self.fail('pidfd_signaling_unavailable')
        if self.first():
            channel.close()
            self.stop(sampler, started)
            return
        channel.setblocking(False)
        deadline = self.clock() + _CHECK_SECONDS * _NS
        data = bytearray()
        released = False
        while not self.first():
            now = self.clock()
            health = self._sync_health(sampler, started)
            if self.first():
                break
            self.returncode = self.proc.poll()
            if self.returncode is not None:
                if released and self.returncode == 0:
                    if not self._live():
                        self.collection_complete = True
                        return
                    self.fail('live_descendant_at_collection')
                    break
                self.fail('unexpected_child_exit', 'workload')
                break
            if not self.payload_granted:
                if health['monitoring_ready']:
                    # Small local socket write with no blocking wait. A short
                    # write never authorizes the child without its newline.
                    packet = launcher._encode(grant) + b'\n'
                    try:
                        if channel.send(packet) != len(packet):
                            raise OSError('Incomplete grant')
                    except OSError:
                        self.fail('grant_delivery_failed', 'workload')
                        break
                    self.payload_granted = True
                elif now >= deadline:
                    self.fail('initial_observations_unconfirmed')
            elif self.outcome_received:
                if not released and sampler.checkpoint_ready():
                    if any(member.identity.pid != self.identity.pid for member in self._live()):
                        self.fail('live_descendant_at_outcome')
                        break
                    sampler.stop.set()
                    for wake in sampler.wake.values():
                        wake.set()
                    channel.close()
                    released = True
                    deadline = now + launcher._TIMEOUT * _NS
                elif now >= deadline:
                    self.fail('shutdown_not_confirmed')
            else:
                try:
                    message = self._receive_step(channel, data)
                    if message is not None:
                        if message != {'status': 'probe_passed'}:
                            raise launcher.SupervisorError('Unrecognized workload outcome')
                        self.outcome_received = True
                        sampler.outcome_ns = now
                        sampler.request_checkpoint()
                        deadline = now + _CHECK_SECONDS * _NS
                except (OSError, ValueError, launcher.SupervisorError):
                    self.fail('workload_protocol_failed', 'workload')
            if not self.first():
                self.wait(_TICK)
        self.stop(sampler, started)

    @staticmethod
    def _receive_step(channel, data):
        try:
            block = channel.recv(8193 - len(data))
        except BlockingIOError:
            return None
        if not block:
            raise launcher.SupervisorError('Workload channel closed before outcome')
        data.extend(block)
        if len(data) > 8192:
            raise launcher.SupervisorError('Workload outcome too large')
        if b'\n' in data:
            if data[-1:] != b'\n' or data.count(b'\n') != 1:
                raise launcher.SupervisorError('Invalid workload outcome framing')
            return launcher._decode(bytes(data[:-1]))
        return None

    def report(self):
        live = self._live()
        live_pids = {member.identity.pid for member in live}
        with self.lock:
            return {
                'payload_granted': self.payload_granted, 'outcome_received': self.outcome_received,
                'first_stop': next(iter(self.reasons.values()), None), 'reasons': list(self.reasons.values()),
                'signals': [
                    {'pid': member.identity.pid, 'start_ticks': member.identity.start_ticks,
                     'signal': sig.name, **entry}
                    for member in self.members.values() for sig, entry in member.attempts.items()
                ],
                'unresolved_processes': [dict(asdict(member.identity), verification='previously_verified')
                                         for member in live] +
                                        [dict(identity, verification='unconfirmed') for identity in self.unknown],
                'exited_descendants': [asdict(member.identity) for member in self.members.values()
                                       if member.identity.pid != self.identity.pid and
                                       member.identity.pid not in live_pids],
                'unfinished_control_io': sorted(self.unfinished | {
                    name for name, (work, _, _) in self.jobs.items() if not work.done.is_set()}),
                'collection_complete': self.collection_complete and not live and not self.unknown,
                'exit_code': self.returncode, 'leader_reaped': self.returncode is not None,
            }

    def seal(self):
        for name, (work, _, _) in self.jobs.items():
            work.cancelled.set()
            if not work.done.is_set():
                self.unfinished.add(name)
        with self.lock:
            self.closed = True

    def close(self):
        self.seal()
        with self.lock:
            members = tuple(self.members.values())
        primary = None
        for member in members:
            fd, member.fd = member.fd, None
            if fd is not None:
                try:
                    os.close(fd)
                except OSError as exc:
                    if primary is None:
                        primary = exc
                    else:
                        launcher._note(primary, 'Could not close another watchdog pidfd', exc)
        if primary is not None:
            raise primary


def _bootstrap_report(proc, reasons, spawn_unconfirmed):
    unresolved = [{'pid': None, 'identity': 'spawn_unconfirmed'}] if spawn_unconfirmed else []
    returncode = None
    if proc is not None:
        try:
            returncode = proc.wait(timeout=_COLLECT_SECONDS)
        except subprocess.TimeoutExpired:
            unresolved.append({'pid': proc.pid, 'identity': 'unverified'})
    return {
        'payload_granted': False, 'outcome_received': False,
        'first_stop': reasons[0] if reasons else None, 'reasons': reasons, 'signals': [],
        'unresolved_processes': unresolved,
        'unfinished_control_io': ['launch'] if spawn_unconfirmed else [],
        'collection_complete': not unresolved, 'exit_code': returncode,
        'leader_reaped': returncode is not None, 'exited_descendants': [],
    }


def _status(report, resources):
    if not report['collection_complete'] or report['unresolved_processes']:
        return 'incomplete_termination'
    reason = report['first_stop']
    if reason is not None:
        if reason['kind'] == 'resource':
            if not report['payload_granted']:
                return 'initial_rejected'
            return ('resource_terminated' if any(event['result'] == 'sent' for event in report['signals'])
                    else 'resource_stop')
        if reason['kind'] == 'evidence':
            return 'evidence_failure'
        return 'monitoring_failure' if reason['kind'] == 'monitoring' else 'workload_failure'
    if resources['stop_required']:
        if any(failure['metric'] == 'resource_summary' for failure in resources['capture_failures']):
            return 'evidence_failure'
        return 'monitoring_failure'
    if not report['payload_granted'] or not report['outcome_received'] or report['exit_code'] != 0:
        return 'workload_failure'
    return 'supervised_success'


def _publish_result(evidence_fd, result):
    def publish(work, fd):
        record = result
        if result['configuration']['handler'] in launcher._SPARK_HANDLERS:
            from tfm_lakehouse.resource_spark import read_spark_evidence, verify_spark_evidence

            try:
                evidence = read_spark_evidence(fd, result)
            except (KeyError, TypeError, ValueError, OSError) as error:
                evidence = {'status': 'unavailable', 'error_type': type(error).__name__}
            record = dict(result, spark=evidence)
            if (record['status'] == 'supervised_success' and
                    (evidence.get('status') == 'unavailable' or not verify_spark_evidence(fd, record, evidence))):
                record.update(status='monitoring_failure', evidence_error='Spark roles not independently verified')
            if record['status'] == 'supervised_success' and result['configuration']['handler'] == launcher._BRONZE_HANDLER:
                from tfm_lakehouse.bronze_calibration import verify_result

                record['bronze'] = verify_result(fd, result)
            if record['status'] == 'supervised_success' and result['configuration']['handler'] == launcher._SILVER_HANDLER:
                from tfm_lakehouse.silver_calibration import verify_result

                record['silver'] = verify_result(fd, record)
            if record['status'] == 'supervised_success' and result['configuration']['handler'] == launcher._REAL_SILVER_HANDLER:
                from tfm_lakehouse.silver.real_silver_workload import verify_result

                record['real_silver'] = verify_result(fd, record)
        elif record['status'] == 'supervised_success':
            launcher._verify_probe_output(fd, result['configuration']['source'])
        work.check()
        launcher._write_record(fd, 'watchdog.json', record)
        work.check()
        name = 'completed.json' if record['status'] == 'supervised_success' else 'failure.json'
        launcher._write_record(fd, name, record)
        return record

    try:
        return sampling.bounded_evidence(publish, evidence_fd, published_on_return=True)
    except (OSError, ValueError, launcher.SupervisorError, RuntimeError) as error:
        assessment = dict(result)
        if isinstance(error, launcher.SupervisorError):
            assessment['status'] = 'workload_failure'
            assessment['workload_error'] = type(error).__name__
            assessment['evidence_status'] = 'pending'
        else:
            assessment['previous_status'] = result['status']
            assessment['status'] = 'evidence_failure'
            assessment['evidence_status'] = 'unconfirmed'
            assessment['evidence_error'] = type(error).__name__

        def publish_failure(work, fd):
            work.check()
            record = dict(assessment, evidence_status=(
                'confirmed' if assessment['evidence_status'] == 'pending' else 'partial'))
            launcher._write_record(fd, 'failure.json', record)
            return record

        try:
            return sampling.bounded_evidence(publish_failure, evidence_fd, published_on_return=True)
        except (OSError, ValueError, RuntimeError) as secondary:
            assessment['evidence_status'] = 'unconfirmed'
            assessment['failure_record_error'] = type(secondary).__name__
            return assessment


def run_supervised_child(evidence_fd, config, ticket):
    """Run only the registered launcher protocol and publish the D46 result."""
    proc = pidfd = watchdog = sampler = None
    primary = None
    early = []
    report = None
    spawn_unconfirmed = False

    def pregrant_health():
        sampler.check_health()
        if sampler.stopping_reasons():
            raise launcher.SupervisorError('Resource stop before identity grant')

    try:
        with ExitStack() as channels:
            parent, child = map(channels.enter_context, socket.socketpair())
            extra = {'real_silver': config['real_silver']} if 'real_silver' in config else {}
            sampler = channels.enter_context(sampling.record_workload(
                Path(config['root']), config['run_id'], evidence_fd, **extra))
            channels.enter_context(launcher._close_channel_on_failure(parent))
            try:
                pregrant_health()
                started = time.monotonic_ns()
                proc, launch_work, launch_deadline = _spawn_bounded(child.fileno(), evidence_fd, ticket)
                child.close()
                if (not launch_work.done.wait(max(0, launch_deadline - time.monotonic())) or
                        launch_work.close_error is not None):
                    raise launcher.SupervisorError('Launch descriptor release was not confirmed')
                pidfd = launcher._optional_pidfd(proc.pid)
                launcher._send(parent, ticket)
                token = launcher._RECEIVE_HEALTH.set(pregrant_health)
                try:
                    ready = launcher._receive(parent)
                finally:
                    launcher._RECEIVE_HEALTH.reset(token)
                identity, grant = launcher._verified_grant(evidence_fd, proc, pidfd, ticket, ready, supervised=True)
            except (OSError, ValueError, launcher.SupervisorError, KeyError, TypeError) as exc:
                spawn_unconfirmed = isinstance(exc, _SpawnDeadline)
                early.extend({'reason': reason, 'kind': 'resource' if reason in RESOURCE_STOPS else 'monitoring',
                              'pid': None, 'monotonic_ns': at}
                             for reason, at in sampler.stopping_reasons())
                if not early:
                    early.append({'reason': 'spawn_unconfirmed' if spawn_unconfirmed else
                                  'authorization_or_initialization_failed', 'kind': 'monitoring',
                                  'pid': proc.pid if proc is not None else None,
                                  'monotonic_ns': time.monotonic_ns(), 'error_type': type(exc).__name__})
            else:
                watchdog = _Watchdog(proc, identity, pidfd)
                sampler.member_observer = watchdog.observe_members
                try:
                    sampler.start(identity, pidfd, started)
                    watchdog.run(parent, grant, sampler, started)
                except BaseException as exc:
                    watchdog.fail('controller_interrupted', 'workload')
                    try:
                        watchdog.stop(sampler, started)
                    except BaseException as cleanup:
                        launcher._note(exc, 'Could not complete controlled stop', cleanup)
                    raise
            # No unauthorized child receives a grant. In error paths the
            # channel context closes first without replacing the primary error.
            parent.close()
        if watchdog is not None:
            if watchdog.collection_complete and watchdog._live():
                watchdog.collection_complete = False
                watchdog.fail('live_descendant_at_collection')
                watchdog.stop(sampler, started)
            summary_error = any(failure['metric'] == 'resource_summary'
                                for failure in sampler.summary['capture_failures'])
            if summary_error and not watchdog.first():
                watchdog.fail('resource_summary_unconfirmed', 'evidence')
            watchdog._sync_health(sampler, started)
            watchdog.seal()
            report = watchdog.report()
        else:
            report = _bootstrap_report(proc, early, spawn_unconfirmed)
        result = {'configuration': config,
                  'identity': asdict(watchdog.identity) if watchdog is not None else None,
                  'status': _status(report, sampler.summary), 'pidfd_used': pidfd is not None,
                  'resources': sampler.summary, 'watchdog': report, 'evidence_status': 'confirmed',
                  'sampler_evidence_status': ('partial' if sampler.summary['capture_failures'] else 'confirmed')}
        if watchdog is not None:
            watchdog.close()
        if pidfd is not None:
            fd, pidfd = pidfd, None
            os.close(fd)
        return _publish_result(evidence_fd, result)
    except BaseException as exc:
        primary = exc
        try:
            if watchdog is not None:
                watchdog.seal()
                diagnostic = report if report is not None else watchdog.report()
            else:
                diagnostic = _bootstrap_report(proc, early, spawn_unconfirmed)
            launcher._note(exc, 'D46 control state', diagnostic)
            failure = _publish_result(evidence_fd, {
                'configuration': config, 'status': 'workload_failure', 'watchdog': diagnostic,
                'resources': sampler.summary if sampler is not None else None, 'evidence_status': 'confirmed',
                'error_type': type(exc).__name__,
            })
            if failure['evidence_status'] != 'confirmed':
                launcher._note(exc, 'Watchdog failure evidence was not confirmed', failure)
        except BaseException as secondary:
            launcher._note(exc, 'Could not retain watchdog failure', secondary)
        raise
    finally:
        cleanup_errors = []
        if watchdog is not None:
            try:
                watchdog.close()
            except OSError as exc:
                cleanup_errors.append(exc)
        if pidfd is not None:
            try:
                os.close(pidfd)
            except OSError as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            if primary is None:
                primary = cleanup_errors.pop(0)
                for error in cleanup_errors:
                    launcher._note(primary, 'Could not close another supervision descriptor', error)
                raise primary
            for error in cleanup_errors:
                launcher._note(primary, 'Could not close supervision descriptor', error)
