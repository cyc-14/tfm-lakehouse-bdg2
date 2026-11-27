"""Fail-closed building blocks for a future D44 supervisor.

These primitives do not launch workloads or establish workload membership.  The
caller must establish and retain membership; PID and process group alone do not
prove that a process belongs to the workload.
"""
from dataclasses import dataclass
import json
import os
from pathlib import Path
import stat
import hashlib
import secrets
import socket
import subprocess
import sys

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
    fd = os.open(directory / 'reservation.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as handle:
        json.dump({'contract_version': 1, 'run_id': run_id, 'campaign_id': campaign.campaign_id,
                   'owner_id': campaign.owner_id, 'status': 'reserved'}, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    return directory


def read_process_identity(pid: int) -> ProcessIdentity:
    """Read PID, process group and start time from procfs without command lines."""
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise SupervisorError('Invalid PID')
    try:
        content = Path(f'/proc/{pid}/stat').read_text(encoding='ascii')
        tail = content[content.rindex(')') + 2:].split()
        return ProcessIdentity(pid, int(tail[19]), int(tail[2]), int(tail[1]), int(tail[3]))
    except (OSError, UnicodeError, ValueError, IndexError) as exc:
        raise SupervisorError('Process identity unavailable') from exc


def signal_verified_child(identity: ProcessIdentity, sig: int) -> None:
    """Fail closed until a launcher establishes trustworthy workload membership."""
    raise SupervisorError('Workload membership is not implemented; no signal sent')


def _configuration(campaign, basetemp, run_id, handshake_fd):
    return {'campaign_id': campaign.campaign_id, 'owner_id': campaign.owner_id,
            'basetemp': str(basetemp), 'run_id': run_id, 'payload': 'synthetic_probe_v1',
            'executable': sys.executable, 'handshake_fd': handshake_fd, 'arguments': ['-I', '-B', '-m',
                          'tfm_lakehouse.resource_supervisor_child']}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _send(channel, value):
    data = json.dumps(value, sort_keys=True, separators=(',', ':')).encode() + b'\n'
    if len(data) > 8192:
        raise SupervisorError('Handshake message too large')
    channel.sendall(data)


def _receive(channel):
    data = bytearray()
    while len(data) < 8192:
        block = channel.recv(1)
        if not block:
            raise SupervisorError('Handshake ended prematurely')
        if block == b'\n':
            value = json.loads(data)
            if not isinstance(value, dict):
                raise SupervisorError('Invalid handshake')
            return value
        data.extend(block)
    raise SupervisorError('Handshake message too large')


def launch_probe(campaign: workspace.Campaign, basetemp: Path, run_id: str) -> dict:
    """Launch only the built-in synthetic probe; no general payloads or signals.

    Claims and evidence are retained permanently. A failed launch is never retried.
    Filesystem and process isolation remain cooperative against external actors.
    """
    _identifier(run_id)
    if sys.executable != str(workspace.PROJECT_ROOT / '.venv' / 'bin' / 'python'):
        raise SupervisorError('Unapproved Python executable')
    root = preflight(campaign, basetemp)
    claims = root / 'evidence' / 'pytest-claims'
    claims.mkdir(mode=0o700, exist_ok=True)
    workspace._check_tree(claims)
    claim = claims / (_digest({'basetemp': str(basetemp)}) + '.json')
    try:
        fd = os.open(claim, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump({'run_id': run_id, 'basetemp': str(basetemp)}, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise SupervisorError('Pytest base is already claimed') from exc
    evidence = reserve_evidence(campaign, run_id)
    nonce = secrets.token_hex(32)
    parent, child = socket.socketpair()
    parent.settimeout(15)
    config = _configuration(campaign, basetemp, run_id, child.fileno())
    proc = None
    try:
        proc = subprocess.Popen([sys.executable, '-I', '-B', '-m', 'tfm_lakehouse.resource_supervisor_child',
                                 str(child.fileno())], pass_fds=(child.fileno(),),
                                start_new_session=True, env={'PATH': os.defpath, 'LANG': 'C.UTF-8'},
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        child.close()
        fd = os.open(evidence / 'child-pid.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump({'pid': proc.pid}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        _send(parent, {'configuration': config, 'nonce': nonce})
        ready = _receive(parent)
        identity = read_process_identity(proc.pid)
        expected = {'configuration_sha256': _digest(config), 'nonce': nonce,
                    'pid': identity.pid, 'start_ticks': identity.start_ticks,
                    'process_group': identity.process_group, 'session_id': identity.session_id}
        if (ready != expected or identity.pid != identity.process_group or
                identity.pid != identity.session_id or identity.parent_pid != os.getpid()):
            raise SupervisorError('Child identity or configuration differs')
        fd = os.open(evidence / 'child-identity.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump({'identity': vars(identity), 'configuration': config}, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        _send(parent, {'authorized': nonce})
        outcome = _receive(parent)
        if outcome != {'status': 'probe_passed', 'nonce': nonce} or proc.wait(timeout=15) != 0:
            raise SupervisorError('Child did not complete the authorized probe')
        result = {'configuration': config, 'identity': vars(identity), 'status': 'probe_passed'}
        path = evidence / 'completed.json'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(result, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        return result
    except BaseException as exc:
        try:
            fd = os.open(evidence / 'failure.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                json.dump({'error_type': type(exc).__name__, 'pid': proc.pid if proc else None,
                           'exit_code': proc.poll() if proc else None}, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException as failure:
            try:
                exc.add_note(f'Failure evidence could not be written: {failure!r}')
            except BaseException:
                pass
        if isinstance(exc, (OSError, ValueError, subprocess.SubprocessError)):
            raise SupervisorError('Child launch or handshake failed') from exc
        raise
    finally:
        parent.close()
        child.close()
