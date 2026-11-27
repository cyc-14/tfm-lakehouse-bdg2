"""Restricted cooperative launcher for D44 synthetic workloads."""
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
import errno
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import secrets
import select
import socket
import stat
import subprocess
import sys
from threading import Thread
import time
from types import MappingProxyType

from tfm_lakehouse import scalability_workspace as workspace


class SupervisorError(RuntimeError):
    """A supervision safety check failed."""


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    start_ticks: int
    process_group: int
    parent_pid: int
    session_id: int = 0


def _identifier(value: str) -> None:
    try:
        workspace._id(value)
    except workspace.WorkspaceError as exc:
        raise SupervisorError('Invalid run identifier') from exc


def preflight(campaign: workspace.Campaign, basetemp: Path) -> Path:
    """Revalidate process-local authorization and reject unsafe pytest destinations.

    No destination is created. Both supervisor and child must call this before
    starting their own work. This does not attest a Spark session or launcher.
    """
    if 'TFM_REAL_CONFIG' in os.environ:
        raise SupervisorError('Real configuration must be unset')
    if not isinstance(campaign, workspace.Campaign):
        raise SupervisorError('Explicit campaign required')
    root = campaign.root
    if (not isinstance(basetemp, Path) or not basetemp.is_absolute()
            or '..' in basetemp.parts or basetemp.parent != root / 'temp'
            or not basetemp.name.startswith('pytest-')):
        raise SupervisorError('Unauthorized pytest base')
    _identifier(basetemp.name)
    try:
        workspace.validate_authorized_campaign(campaign)
        info = os.lstat(root / 'temp')
        if not stat.S_ISDIR(info.st_mode):
            raise SupervisorError('Invalid temporary parent')
        try:
            os.lstat(basetemp)
        except FileNotFoundError:
            return root
        raise SupervisorError('Pytest base already exists')
    except (OSError, workspace.WorkspaceError) as exc:
        raise SupervisorError('Campaign preflight failed') from exc


def reserve_evidence(campaign: workspace.Campaign, run_id: str) -> Path:
    """Reserve an exclusive evidence folder, preserving partial reservations."""
    _identifier(run_id)
    try:
        root = workspace.validate_authorized_campaign(campaign)
        directory = root / 'evidence' / run_id
        directory.mkdir(mode=0o700)
    except (OSError, workspace.WorkspaceError) as exc:
        raise SupervisorError('Evidence reservation failed') from exc
    with _directory(directory) as fd:
        _write_record(fd, 'reservation.json', {
            'contract_version': 1, 'run_id': run_id, 'campaign_id': campaign.campaign_id,
            'owner_id': campaign.owner_id, 'status': 'reserved'})
    with _directory(directory.parent) as fd:
        os.fsync(fd)
    return directory


def read_process_identity(pid: int) -> ProcessIdentity:
    """Read PID, process group and start time from procfs without command lines."""
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise SupervisorError('Invalid PID')
    try:
        content = Path(f'/proc/{pid}/stat').read_bytes()
        tail = content[content.rindex(b')') + 2:].split()
        return ProcessIdentity(pid, int(tail[19]), int(tail[2]), int(tail[1]), int(tail[3]))
    except (OSError, UnicodeError, ValueError, IndexError) as exc:
        raise SupervisorError('Process identity unavailable') from exc


def signal_verified_child(identity: ProcessIdentity, sig: int) -> None:
    """Keep signaling disabled until the watchdog defines its own policy."""
    raise SupervisorError('Watchdog signaling is disabled; no signal sent')


@dataclass(frozen=True)
class ProbeParameters:
    """Optionally inspect one small invented fixture in the campaign raw directory."""

    source_name: str | None = None


def _probe(evidence_fd, configuration):
    source = configuration['source']
    if source is not None and _source(Path(source['path'])) != source:
        raise SupervisorError('Synthetic source changed before the probe')
    _write_record(evidence_fd, 'probe-output.json', {'source': source})


# Both processes dispatch exclusively through this registry.
_HANDLERS = MappingProxyType({'synthetic_probe_v1': (ProbeParameters, _probe)})
_CHILD_MODULE = 'tfm_lakehouse.resource_supervisor_child'
_CHILD_SOURCE = workspace.PROJECT_ROOT / 'src'
# Ignore user and system site hooks. Import only from the project's source tree.
_BOOTSTRAP = (
    'import runpy,sys; sys.path.insert(0,sys.argv.pop(1)); '
    f'runpy.run_module({_CHILD_MODULE!r},run_name="__main__",alter_sys=True)'
)
_TIMEOUT = 15
_LOGGER = logging.getLogger(__name__)


def _interpreter():
    return str(workspace.PROJECT_ROOT / '.venv' / 'bin' / 'python')


def _command(channel_fd, evidence_fd):
    return [_interpreter(), '-I', '-B', '-S', '-c', _BOOTSTRAP, str(_CHILD_SOURCE),
            str(channel_fd), str(evidence_fd)]


def _validate_parameters(handler, parameters):
    if not isinstance(handler, str) or handler not in _HANDLERS:
        raise SupervisorError('Unknown workload handler')
    if type(parameters) is not _HANDLERS[handler][0]:
        raise SupervisorError('Invalid workload parameters')
    name = parameters.source_name
    if name is not None and (not isinstance(name, str) or
                             not re.fullmatch(r'[A-Za-z0-9_-]{1,100}\.txt', name)):
        raise SupervisorError('Unauthorized synthetic source name')


def _source(path):
    """Fingerprint a single regular fixture, bounded even if it changes size."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 65536:
                raise SupervisorError('Invalid synthetic fixture')
            data = handle.read(65537)
    except OSError as exc:
        raise SupervisorError('Synthetic fixture unavailable') from exc
    if len(data) > 65536 or len(data) != info.st_size:
        raise SupervisorError('Synthetic fixture size changed')
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


def _configuration(campaign, basetemp, run_id, handler, parameters):
    _identifier(run_id)
    if run_id == 'pytest-claims':
        raise SupervisorError('Reserved run identifier')
    _validate_parameters(handler, parameters)
    root = preflight(campaign, basetemp)
    protected = [workspace.PROJECT_ROOT / 'data' / name
                 for name in ('raw', 'lakehouse/real', 'checks')]
    if any(root.is_relative_to(path) or path.is_relative_to(root) for path in protected):
        raise SupervisorError('Campaign overlaps protected roots')
    return {'campaign_id': campaign.campaign_id, 'owner_id': campaign.owner_id,
            'root': str(root), 'basetemp': str(basetemp), 'run_id': run_id,
            'handler': handler, 'parameters': asdict(parameters),
            'protected_roots': list(map(str, protected)),
            'source': _source(root / 'raw' / parameters.source_name)
            if parameters.source_name is not None else None,
            'destination': str(root / 'evidence' / run_id / 'probe-output.json')}


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _same(actual, expected):
    return _encode(actual) == _encode(expected)


def _digest(value):
    return hashlib.sha256(_encode(value)).hexdigest()


def _decode(data):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise SupervisorError('Duplicate protocol field')
            result[key] = value
        return result

    value = json.loads(data, object_pairs_hook=unique)
    if not isinstance(value, dict) or _encode(value) != data:
        raise SupervisorError('Invalid protocol record')
    return value


def _write_record(directory, name, value):
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600,
                 dir_fd=directory)
    with os.fdopen(fd, 'wb') as handle:
        handle.write(_encode(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.fsync(directory)


def _read_record(directory, name):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise SupervisorError('Invalid evidence file')
        data = handle.read(8193)
    if len(data) > 8192:
        raise SupervisorError('Evidence record too large')
    return _decode(data)


@contextmanager
def _directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        yield fd
    except BaseException as primary:
        try:
            os.close(fd)
        except BaseException as error:
            _note(primary, 'Could not close directory', error)
        raise
    else:
        os.close(fd)


def _claim_base(root, basetemp, ticket):
    claims = root / 'evidence' / 'pytest-claims'
    claims.mkdir(mode=0o700, exist_ok=True)
    with _directory(claims.parent) as parent_fd:
        os.fsync(parent_fd)
    with _directory(claims) as claims_fd:
        try:
            _write_record(claims_fd, _claim_name(basetemp), ticket)
        except FileExistsError as exc:
            raise SupervisorError('Pytest base is already claimed') from exc


def _claim_name(basetemp):
    return _digest({'basetemp': str(basetemp)}) + '.json'


def _send(channel, value):
    data = _encode(value) + b'\n'
    if len(data) > 8192:
        raise SupervisorError('Handshake message too large')
    channel.settimeout(_TIMEOUT)
    channel.sendall(data)


def _receive(channel):
    deadline = time.monotonic() + _TIMEOUT
    data = bytearray()
    while len(data) < 8192:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Handshake deadline expired')
        channel.settimeout(remaining)
        block = channel.recv(1)
        if not block:
            raise SupervisorError('Handshake ended prematurely')
        if block == b'\n':
            return _decode(data)
        data.extend(block)
    raise SupervisorError('Handshake message too large')


def _optional_pidfd(pid):
    try:
        return os.pidfd_open(pid) if hasattr(os, 'pidfd_open') else None
    except OSError as exc:
        if exc.errno in (errno.ENOSYS, errno.EINVAL, errno.EPERM, errno.EACCES):
            return None
        raise


def _verify_member(proc, parent, identity, pidfd):
    """Check the cooperative protocol receipt against this launch and procfs."""
    if pidfd is not None:
        watcher = select.poll()
        watcher.register(pidfd, select.POLLIN)
        if watcher.poll(0):
            raise SupervisorError('Child has already exited')
    if (proc.poll() is not None or identity.pid != proc.pid or
            identity.pid != identity.session_id or identity.pid != identity.process_group or
            identity.parent_pid != parent.pid or read_process_identity(parent.pid) != parent or
            read_process_identity(proc.pid) != identity):
        raise SupervisorError('Child identity or workload membership differs')
    if not os.path.samefile(f'/proc/{proc.pid}/exe', _interpreter()):
        raise SupervisorError('Child executable differs')


def _failure_record(evidence_fd, proc, error):
    # Closing the channel first lets the controlled child exit on EOF. Reap
    # through the original Popen handle, never by signals or by an unowned PID.
    exit_code = None
    if proc is not None:
        try:
            exit_code = proc.wait(timeout=_TIMEOUT)
        except subprocess.TimeoutExpired:
            Thread(target=proc.wait, daemon=True, name='d44-child-reaper').start()
    _write_record(evidence_fd, 'failure.json', {'error_type': type(error).__name__,
                  'pid': proc.pid if proc is not None else None, 'exit_code': exit_code,
                  'reaped': proc is not None and exit_code is not None})


def _run_child(evidence_fd, config, ticket):
    proc, pidfd = None, None
    primary = None
    try:
        with ExitStack() as channels:
            parent_socket, child_socket = map(channels.enter_context, socket.socketpair())
            command = _command(child_socket.fileno(), evidence_fd)
            _write_record(evidence_fd, 'launch.json', {'ticket': ticket, 'command': command})
            proc = subprocess.Popen(command, pass_fds=(child_socket.fileno(), evidence_fd),
                                    start_new_session=True, env={'PATH': os.defpath, 'LANG': 'C.UTF-8'},
                                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL)
            child_socket.close()
            pidfd = _optional_pidfd(proc.pid)
            _send(parent_socket, ticket)
            ready = _receive(parent_socket)
            receipt = _read_record(evidence_fd, 'child-pid.json')
            identity = ProcessIdentity(**receipt['identity'])
            parent = ProcessIdentity(**ticket['parent'])
            if not _same(receipt, {'identity': asdict(identity), 'parent': asdict(parent)}) or not _same(
                    ready, {'ticket_sha256': _digest(ticket), 'identity': asdict(identity)}):
                raise SupervisorError('Child identity or configuration differs')
            _verify_member(proc, parent, identity, pidfd)
            grant = {'ticket_sha256': _digest(ticket), 'identity': asdict(identity)}
            _write_record(evidence_fd, 'child-identity.json', grant)
            _verify_member(proc, parent, identity, pidfd)
            _send(parent_socket, grant)
            outcome = _receive(parent_socket)
        if proc.wait(timeout=_TIMEOUT) != 0 or outcome != {'status': 'probe_passed'}:
            raise SupervisorError('Child did not complete the authorized probe')
        if not _same(_read_record(evidence_fd, 'probe-output.json'), {'source': config['source']}):
            raise SupervisorError('Probe output differs from the approved configuration')
        source = config['source']
        if source is not None and _source(Path(source['path'])) != source:
            raise SupervisorError('Synthetic source changed before completion')
        result = {'configuration': config, 'identity': asdict(identity), 'status': 'probe_passed',
                  'pidfd_used': pidfd is not None}
        if pidfd is not None:
            closing_fd, pidfd = pidfd, None
            os.close(closing_fd)
        _write_record(evidence_fd, 'completed.json', result)
        return result
    except BaseException as exc:
        primary = exc
        try:
            _failure_record(evidence_fd, proc, exc)
        except BaseException as failure:
            _note(exc, 'Failure evidence or reaping failed', failure)
        if isinstance(exc, (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError)):
            raise SupervisorError('Child launch or handshake failed') from exc
        raise
    finally:
        if pidfd is not None:
            try:
                os.close(pidfd)
            except OSError as exc:
                if primary is None:
                    raise
                _note(primary, 'Could not close pidfd', exc)


def _note(primary, operation, error):
    try:
        primary.add_note(f'{operation}: {error!r}')
    except BaseException:
        pass


def launch_workload(campaign: workspace.Campaign, basetemp: Path, run_id: str, *,
                    handler: str, parameters: ProbeParameters) -> dict:
    """Run a registered probe in an authorized D39 campaign and retain evidence.

    This cooperative launcher has no arbitrary command execution or signal API.
    Claims are retained after success and failure, so each base is single use.
    """
    if sys.executable != _interpreter():
        raise SupervisorError('Unapproved Python executable')
    config = _configuration(campaign, basetemp, run_id, handler, parameters)
    parent = read_process_identity(os.getpid())
    ticket = {'configuration': config, 'parent': asdict(parent), 'nonce': secrets.token_hex(32)}
    evidence = reserve_evidence(campaign, run_id)
    result = None
    try:
        with _directory(evidence) as evidence_fd:
            try:
                _claim_base(campaign.root, basetemp, ticket)
            except (OSError, SupervisorError) as exc:
                try:
                    _failure_record(evidence_fd, None, exc)
                except BaseException as failure:
                    _note(exc, 'Failure evidence could not be written', failure)
                raise
            result = _run_child(evidence_fd, config, ticket)
    except OSError:
        if result is None:
            raise
        # The completion record is already synced and the child was reaped.
        _LOGGER.warning('Could not close the completed evidence directory', exc_info=True)
    return result


def launch_probe(campaign: workspace.Campaign, basetemp: Path, run_id: str) -> dict:
    """Run the registered no-input probe with the original API."""
    return launch_workload(campaign, basetemp, run_id, handler='synthetic_probe_v1',
                           parameters=ProbeParameters())
