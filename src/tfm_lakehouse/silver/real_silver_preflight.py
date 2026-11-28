"""Read-only preparation for an explicitly identified real electrical Silver request."""

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import stat
import subprocess
from uuid import UUID

from tfm_lakehouse import resource_linux
from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS

_ROOT = Path(__file__).absolute().parents[3]
_RAW = Path('data/raw/bdg2')
_BRONZE = Path('data/lakehouse/real')
_SOURCE = {
    'electricity': _RAW / 'data/meters/raw/electricity.csv',
    'metadata': _RAW / 'data/metadata/metadata.csv',
    'weather': _RAW / 'data/weather/weather.csv',
}
_MANIFEST = _RAW / 'source_manifest.json'
_REPORT_ID = re.compile(r'[a-f0-9]{32}\Z')
_ID = re.compile(r'[a-z0-9][a-z0-9_-]{0,95}\Z')
_SHA = re.compile(r'[a-f0-9]{64}\Z')
_PART = re.compile(r'part-[A-Za-z0-9_.-]+\.parquet\Z')


class RealSilverPreflightError(RuntimeError):
    """The exact proposed read-only scope is invalid or unverified."""


@dataclass(frozen=True)
class FileIdentity:
    relative_path: Path
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class TableIdentity:
    name: str
    version: int
    uuid: str
    active_file: str


@dataclass(frozen=True)
class RealSilverRequest:
    bronze_run_id: str
    publication_id: str
    tables: tuple[TableIdentity, ...]
    inputs: tuple[FileIdentity, ...]

    @property
    def output(self) -> Path:
        return _ROOT / _BRONZE / 'silver-publications' / self.publication_id

    @property
    def evidence(self) -> Path:
        return _ROOT / 'data/checks/real-silver' / self.publication_id


def _require(condition, message):
    if not condition:
        raise RealSilverPreflightError(message)


def _required_paths(request):
    _require(isinstance(request.bronze_run_id, str) and _REPORT_ID.fullmatch(request.bronze_run_id)
             and isinstance(request.publication_id, str) and _ID.fullmatch(request.publication_id),
             'Invalid real run or publication identifier')
    _require(isinstance(request.tables, tuple) and all(isinstance(t, TableIdentity) for t in request.tables)
             and tuple(table.name for table in request.tables) == tuple(_SOURCE),
             'Expected one ordered identity for each Bronze table')
    paths = {_MANIFEST, *_SOURCE.values(), _BRONZE / '_runs' / (request.bronze_run_id + '.json')}
    for table in request.tables:
        try:
            valid_uuid = str(UUID(table.uuid)) == table.uuid
        except (TypeError, ValueError, AttributeError):
            valid_uuid = False
        _require(type(table.version) is int and 0 <= table.version < len(request.inputs)
                 and valid_uuid and isinstance(table.active_file, str) and _PART.fullmatch(table.active_file),
                 'Invalid Bronze table identity')
        root = _BRONZE / 'bronze' / table.name
        paths.add(root / table.active_file)
        paths.update(root / '_delta_log' / f'{version:020}.json' for version in range(table.version + 1))
    return paths


def _excluded(path):
    command = ['git', '--no-optional-locks']
    env = dict(os.environ, GIT_OPTIONAL_LOCKS='0')
    return (
        subprocess.run(command + ['check-ignore', '-q', '--', str(path)], cwd=_ROOT,
                       env=env, check=False).returncode == 0
        and not subprocess.check_output(command + ['ls-files', '--', str(path)], cwd=_ROOT, env=env)
    )


def _verify_file(pin, root_fd, mount):
    parent = os.dup(root_fd)
    try:
        for part in pin.relative_path.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
            _require(resource_linux._mount_id(parent) == mount and os.fstat(parent).st_uid == os.getuid(),
                     'Unexpected mount or owner in real input path')
        name = pin.relative_path.name
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, 'rb') as handle:
            before = os.fstat(handle.fileno())
            _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == os.getuid()
                     and resource_linux._mount_id(handle.fileno()) == mount and before.st_size == pin.size_bytes,
                     'Real input mount, identity or size differs')
            remaining = pin.size_bytes
            digest = hashlib.sha256()
            while remaining:
                block = handle.read(min(remaining, 65536))
                _require(bool(block), 'Real input shrank during verification')
                digest.update(block)
                remaining -= len(block)
            after = os.fstat(handle.fileno())
            _require(not handle.read(1) and before.st_mtime_ns == after.st_mtime_ns
                     and os.path.samestat(before, os.stat(name, dir_fd=parent, follow_symlinks=False))
                     and digest.hexdigest() == pin.sha256, 'Real input fingerprint differs')
    finally:
        os.close(parent)


def verify_real_silver_inputs(request: RealSilverRequest, *, authorized: RealSilverRequest) -> None:
    """Verify an exact read-only grant before any payload or publication is authorized.

    The caller must obtain the independent grant from the reviewed audit. This
    preflight opens only pinned inputs and rechecks their bytes. Delta identities
    derive from the audit of those bytes. The proposed destinations remain lexical:
    a future supervised launcher must also verify and reserve them exclusively.
    """
    _require(isinstance(request, RealSilverRequest) and isinstance(authorized, RealSilverRequest)
             and request == authorized, 'An exact read-only authorization is required')
    _require('TFM_REAL_CONFIG' not in os.environ, 'Unset TFM_REAL_CONFIG for this audit')
    _require(isinstance(request.inputs, tuple) and all(isinstance(p, FileIdentity) for p in request.inputs),
             'Expected immutable real input identities')
    paths = _required_paths(request)
    pins = {pin.relative_path: pin for pin in request.inputs}
    _require(len(pins) == len(request.inputs) and set(pins) == paths
             and all(type(pin.size_bytes) is int and pin.size_bytes > 0
                     and isinstance(pin.sha256, str) and _SHA.fullmatch(pin.sha256) for pin in pins.values()),
             'Missing, additional or invalid real input fingerprints')
    _require(_ROOT.resolve(strict=True) == _ROOT, 'Repository root is not canonical')
    try:
        fs = subprocess.check_output(['findmnt', '-n', '-T', str(_ROOT), '-o', 'FSTYPE'], text=True).strip()
        _require(fs in {'ext4', 'xfs', 'btrfs'}, 'Real audit requires a local disk filesystem')
        _require(all(_excluded(p) for p in (*sorted(paths), request.output.relative_to(_ROOT),
                                           request.evidence.relative_to(_ROOT))),
                 'Inputs or proposed destinations are not Git-excluded')
        with resource_linux._absolute_directory(_ROOT) as fd:
            _require(os.fstat(fd).st_uid == os.getuid(), 'Unexpected repository owner')
            mount = resource_linux._mount_id(fd)
            for path in sorted(paths):
                _verify_file(pins[path], fd, mount)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RealSilverPreflightError('Could not verify the authorized real input scope') from exc


def electrical_cohort() -> tuple[str, ...]:
    """Return the shared, ordered H04 cohort for the real execution proposal."""
    return APPROVED_BUILDINGS
