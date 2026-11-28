"""Linux measurements for the restricted workload sampler."""

from contextlib import contextmanager
import os
from pathlib import Path, PurePosixPath
import re
import stat

from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace


class InvalidMeasurement(ValueError):
    """A kernel record or a measurement boundary is inconsistent."""


READ_ERRORS = (OSError, ValueError, IndexError, workspace.WorkspaceError)


def issue(metric, error):
    status = 'invalid'
    if isinstance(error, FileNotFoundError):
        status = 'missing'
    elif isinstance(error, OSError):
        status = 'unavailable'
    return {
        'metric': metric,
        'status': status,
        'error': type(error).__name__,
        'errno': getattr(error, 'errno', None),
        'detail': str(error) if isinstance(error, InvalidMeasurement) else None,
    }


def _uint(text):
    if not re.fullmatch(r'[0-9]{1,20}', text) or int(text) > 2**64 - 1:
        raise InvalidMeasurement('Expected a non-negative integer')
    return int(text)


class ProcessTracker:
    """Retain verified instances even when they leave the observed session."""

    def __init__(self):
        self.members = {}
        self.events = []
        self.suspects = ()

    def inspect(self, identities, states, failures):
        skip = set()
        self.events = []
        for pid, (original, fd) in list(self.members.items()):
            current = identities.get(pid)
            try:
                exited = supervisor._pidfd_exited(fd)
            except OSError:
                self._problem(pid, 'ambiguous_pid', 'Verified pidfd cannot be polled', failures)
                skip.add(pid)
                continue
            if current is not None and current.start_ticks != original.start_ticks:
                self._problem(pid, 'pid_reused', 'Observed PID was reused', failures)
                skip.add(pid)
                self._forget(pid)
            elif exited or (current is not None and states[pid] in (b'Z', b'X', b'x')):
                self.events.append({'pid': pid, 'start_ticks': original.start_ticks, 'kind': 'exited'})
                skip.add(pid)
                # A live proc record with a readable original pidfd could be a
                # replacement born within the very same kernel tick.
                if current is not None and states[pid] not in (b'Z', b'X', b'x'):
                    self._problem(pid, 'ambiguous_pid', 'Live PID after verified instance exited', failures)
                self._forget(pid)
            elif current is None:
                self._problem(pid, 'lost', 'Previously verified descendant is no longer observable', failures)
            elif current.session_id != original.session_id:
                self._problem(pid, 'session_changed', 'Verified descendant changed session', failures,
                              session_id=current.session_id, process_group=current.process_group)
                skip.add(pid)
            elif current.process_group != original.process_group:
                self.events.append({'pid': pid, 'start_ticks': original.start_ticks, 'kind': 'group_changed',
                                    'previous_process_group': original.process_group,
                                    'process_group': current.process_group})
        return skip

    def _problem(self, pid, kind, detail, failures, **event):
        original, _ = self.members[pid]
        self.events.append(dict(event, pid=pid, start_ticks=original.start_ticks, kind=kind))
        failures.append(dict(issue('process_membership', InvalidMeasurement(detail)), pid=pid))

    def _forget(self, pid):
        _, fd = self.members.pop(pid)
        if fd is not None:
            os.close(fd)

    def close(self):
        error = None
        for pid in list(self.members):
            try:
                self._forget(pid)
            except OSError as exc:
                if error is None:
                    error = exc
        if error is not None:
            raise error


def _discover(root, previous, tracker=None, *, outside_session=None):
    identities = {root.pid: root}
    states = {}
    failures = []
    supervisor._check_io()
    with os.scandir('/proc') as entries:
        for entry in entries:
            supervisor._check_io()
            if not entry.name.isdecimal() or int(entry.name) == root.pid:
                continue
            pid = int(entry.name)
            try:
                identity, fields = supervisor._process_stat(pid)
                identities[pid] = identity
                if tracker is not None and pid in tracker.members:
                    states[pid] = fields[0]
            except (FileNotFoundError, ProcessLookupError) as exc:
                if pid in previous or (tracker is not None and pid in tracker.members):
                    failures.append(issue('process_membership', exc))
            except READ_ERRORS as exc:
                failures.append(issue('process_membership', exc))

    # A new session cannot be joined from outside. Descendants in it may
    # change group or be reparented. A visible descendant outside it is
    # evidence of incomplete coverage, even if it was never sampled.
    members = {pid: identity for pid, identity in identities.items()
               if identity.session_id == root.session_id}
    children = {}
    for pid, identity in identities.items():
        if pid != root.pid:
            children.setdefault(identity.parent_pid, []).append(pid)
    pending = list(members)
    visited = set(members)
    while pending:
        for pid in children.get(pending.pop(), ()):
            if pid not in visited:
                visited.add(pid)
                pending.append(pid)
    escaped = visited.difference(members)
    escaped.update(pid for pid, old in previous.items()
                   if pid in identities and identities[pid].start_ticks == old.start_ticks
                   and pid not in members)
    if escaped:
        failures.append(issue('process_membership',
                              InvalidMeasurement('Descendant outside the verified session')))
    suspects = tuple(identities[pid] for pid in sorted(escaped))
    if outside_session is not None:
        outside_session.extend(suspects)
    if tracker is not None:
        tracker.suspects = suspects
        for pid in tracker.inspect(identities, states, failures):
            members.pop(pid, None)
    return members, failures


def _membership(pid):
    entries = []
    supervisor._check_io()
    for line in Path(f'/proc/{pid}/cgroup').read_text(encoding='utf-8').splitlines():
        index, controllers, path = line.split(':', 2)
        if controllers and 'memory' in controllers.split(','):
            entries.append((1, _cgroup_path(path)))
        elif index == '0' and not controllers:
            entries.append((2, _cgroup_path(path)))
    for version in (1, 2):
        paths = [path for kind, path in entries if kind == version]
        if len(paths) > 1:
            raise InvalidMeasurement('Ambiguous process memory cgroup')
        if paths:
            return version, paths[0]
    raise InvalidMeasurement('Memory cgroup membership unavailable')


def _read_member(identity, pidfd):
    if supervisor._pidfd_exited(pidfd):
        raise ProcessLookupError('Process exited before observing its resources')
    pid = identity.pid
    current, fields = supervisor._process_stat(pid)
    if current != identity:
        raise InvalidMeasurement('Process identity changed during sampling')
    if fields[0] in (b'Z', b'X', b'x'):
        raise ProcessLookupError('Process exited during sampling')

    failures = []
    metrics = {}
    for metric, fields_to_read, units in (
        ('rss_bytes', (21,), os.sysconf('SC_PAGE_SIZE')),
        ('cpu_ticks', (11, 12), 1),
    ):
        try:
            if units <= 0:
                raise InvalidMeasurement('Invalid kernel measurement unit')
            metrics[metric] = units * sum(_uint(fields[field].decode('ascii')) for field in fields_to_read)
        except READ_ERRORS as exc:
            metrics[metric] = None
            failures.append(issue(metric, exc))
    membership = None
    try:
        first = _membership(pid)
        if first != _membership(pid):
            raise InvalidMeasurement('Process moved between memory cgroups')
        membership = first
    except READ_ERRORS as exc:
        failures.append(issue('cgroup_membership', exc))

    _recheck_member(identity, pidfd)
    return metrics, membership, failures


def _recheck_member(identity, pidfd=None):
    current, fields = supervisor._process_stat(identity.pid)
    if current != identity:
        raise InvalidMeasurement('Process identity changed during sampling')
    if fields[0] in (b'Z', b'X', b'x') or supervisor._pidfd_exited(pidfd):
        raise ProcessLookupError('Process exited during sampling')


def processes(root, pidfd, previous, tracker=None):
    members, failures = _discover(root, previous, tracker)
    observed = {}
    memberships = {}
    # Check the session leader on both sides of the scan before returning any
    # measurement. Without its identity a reused numeric session proves nothing.
    if supervisor._pidfd_exited(pidfd) or supervisor._process_stat(root.pid)[0] != root:
        raise InvalidMeasurement('Verified session leader is unavailable')
    for pid, identity in sorted(members.items()):
        try:
            if pid in previous and previous[pid].start_ticks != identity.start_ticks:
                raise InvalidMeasurement('Observed PID was reused')
            if pid == root.pid:
                metrics, membership, errors = _read_member(identity, pidfd)
            else:
                retained = tracker is not None and pid in tracker.members
                fd = tracker.members[pid][1] if retained else supervisor._optional_pidfd(pid)
                try:
                    metrics, membership, errors = _read_member(identity, fd)
                    if tracker is not None:
                        tracker.members[pid] = (identity, fd)
                        retained = True
                finally:
                    if fd is not None and not retained:
                        os.close(fd)
            observed[identity] = metrics
            if membership is not None:
                memberships[identity] = membership
            for error in errors:
                failures.append(dict(error, pid=pid))
        except READ_ERRORS as exc:
            failures.append(dict(issue('process_membership', exc), pid=pid))
    if supervisor._pidfd_exited(pidfd) or supervisor._process_stat(root.pid)[0] != root:
        raise InvalidMeasurement('Verified session leader changed during sampling')
    _recheck_member(root, pidfd)
    return observed, memberships, failures


def system_memory():
    supervisor._check_io()
    data = Path('/proc/meminfo').read_text(encoding='ascii').splitlines()
    result = {'scope': 'shared_system', 'memory': None, 'swap': None}
    failures = []
    for name, total, remaining in (
        ('memory', 'MemTotal', 'MemAvailable'), ('swap', 'SwapTotal', 'SwapFree'),
    ):
        try:
            values = {}
            for line in data:
                key, _, contents = line.partition(':')
                if key in (total, remaining):
                    fields = contents.split()
                    if key in values or len(fields) != 2 or fields[1] != 'kB':
                        raise InvalidMeasurement('Invalid /proc/meminfo field')
                    values[key] = _uint(fields[0]) * 1024
            if total not in values or remaining not in values:
                raise FileNotFoundError('Required /proc/meminfo field unavailable')
            if values[remaining] > values[total] or (name == 'memory' and not values[total]):
                raise InvalidMeasurement('Inconsistent /proc/meminfo capacities')
            result[name] = {'total_bytes': values[total], 'available_bytes': values[remaining],
                            'used_bytes': values[total] - values[remaining]}
        except READ_ERRORS as exc:
            failures.append(issue('system_' + name, exc))
    return result, failures


def _cgroup_path(value):
    path = PurePosixPath(value)
    if path.root != '/' or '..' in path.parts or str(path) != value:
        raise InvalidMeasurement('Invalid cgroup path')
    return path


def _mount_path(value):
    return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), value)


def _memory_mounts():
    mounts = {1: [], 2: []}
    supervisor._check_io()
    for line in Path('/proc/self/mountinfo').read_text(encoding='utf-8').splitlines():
        raw_mount, raw_filesystem = line.split(' - ', 1)
        filesystem = raw_filesystem.split()
        if filesystem[0] == 'cgroup2':
            version = 2
        elif filesystem[0] == 'cgroup' and 'memory' in filesystem[2].split(','):
            version = 1
        else:
            continue
        fields = raw_mount.split()
        root = _cgroup_path(_mount_path(fields[3]))
        point = Path(_cgroup_path(_mount_path(fields[4])))
        mounts[version].append((root, point, _uint(fields[0])))
    return mounts


@contextmanager
def _group_directory(directory, mount_id):
    prohibited = (workspace.PROJECT_ROOT / 'data' / name for name in ('raw', 'lakehouse/real', 'checks'))
    if any(directory.is_relative_to(path) for path in prohibited):
        raise InvalidMeasurement('Cgroup mount overlaps a protected path')
    with _absolute_directory(directory) as fd:
        if _mount_id(fd) != mount_id:
            raise InvalidMeasurement('Memory hierarchy mount changed')
        yield fd


def _control(fd, name, mount_id):
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    supervisor._check_io()
    with os.fdopen(os.open(name, flags, dir_fd=fd), encoding='ascii') as handle:
        control_fd = handle.fileno()
        if not stat.S_ISREG(os.fstat(control_fd).st_mode) or _mount_id(control_fd) != mount_id:
            raise InvalidMeasurement('Invalid cgroup control file')
        supervisor._check_io()
        return handle.read().strip()


def _read_cgroup_group(version, directory, member_path, mount_root, mount_id):
    with _group_directory(directory, mount_id) as fd:
        return _read_cgroup_controls(version, fd, directory, member_path, mount_root, mount_id)


def _read_cgroup_controls(version, fd, directory, member_path, mount_root, mount_id):
    def read(name):
        return _control(fd, name, mount_id)

    details = {}
    if version == 1:
        if read('memory.use_hierarchy') != '1':
            raise InvalidMeasurement('Non-hierarchical legacy memory controller')
        current = _uint(read('memory.usage_in_bytes'))
        raw_limit = _uint(read('memory.limit_in_bytes'))
        # v1 exposes PAGE_COUNTER_MAX rounded down to the kernel page size.
        page_size = os.sysconf('SC_PAGE_SIZE')
        if page_size <= 0:
            raise InvalidMeasurement('Invalid page size for legacy cgroup memory limit')
        unlimited_floor = (2**63 - 1) // page_size * page_size
        unlimited = raw_limit >= unlimited_floor
        reported = [line.split()[1] for line in read('memory.stat').splitlines()
                    if line.split()[:1] == ['hierarchical_memory_limit']]
        if len(reported) != 1:
            raise InvalidMeasurement('Legacy hierarchical memory limit unavailable')
        hierarchical = _uint(reported[0])
        details['reported_hierarchical_limit_bytes'] = hierarchical if hierarchical < unlimited_floor else None
    else:
        try:
            raw = read('memory.max')
        except FileNotFoundError:
            try:
                read('memory.current')
            except FileNotFoundError:
                pass
            else:
                raise InvalidMeasurement('Incomplete cgroup memory interface')
            # v2 only exports the interfaces below the hierarchy root and
            # where the parent enabled its memory controller for children.
            if member_path == mount_root:
                if 'memory' not in read('cgroup.controllers').split():
                    raise InvalidMeasurement('Cannot establish visible cgroup memory coverage')
            else:
                with _group_directory(directory.parent, mount_id) as parent_fd:
                    if 'memory' in _control(parent_fd, 'cgroup.subtree_control', mount_id).split():
                        raise
                read('cgroup.type')
            return {'path': str(member_path), 'version': version, 'status': 'not_applicable'}
        current = _uint(read('memory.current'))
        unlimited = raw == 'max'
        raw_limit = None if unlimited else _uint(raw)
    limit = None if unlimited else raw_limit
    return {
        'path': str(member_path), 'version': version, 'status': 'valid',
        'used_bytes': current, 'limit_bytes': limit, 'unlimited': unlimited,
        # A hard limit of zero leaves no usable capacity, even at zero usage.
        'utilization_fraction': None if unlimited else (current / limit if limit else 1.0),
        **details,
    }


def cgroup_memory(memberships, leader_pid=None, leader_pidfd=None, tracker=None):
    mounts = _memory_mounts()
    groups = {}
    failures = []
    for version, member in sorted(set(memberships.values())):
        candidates = [mount for mount in mounts[version] if member.is_relative_to(mount[0])]
        if not candidates:
            failures.append(issue('cgroup_memory', FileNotFoundError('Memory hierarchy not mounted')))
            continue
        mount_root, mount_point, mount_id = min(candidates, key=lambda item: len(item[0].parts))
        if mount_root != PurePosixPath('/'):
            failures.append(issue('cgroup_memory', InvalidMeasurement('Hidden cgroup ancestors')))
        for path in (member, *member.parents):
            if not path.is_relative_to(mount_root):
                break
            key = (version, path)
            if key in groups:
                continue
            directory = mount_point / path.relative_to(mount_root)
            try:
                group = _read_cgroup_group(version, directory, path, mount_root, mount_id)
                if version == 2 and path == PurePosixPath('/') and group['status'] == 'valid':
                    failures.append(issue('cgroup_memory', InvalidMeasurement('Hidden cgroup ancestors')))
                groups[key] = group
            except READ_ERRORS as exc:
                failure = issue('cgroup_memory', exc)
                groups[key] = {'path': str(path), 'version': version, 'status': failure['status']}
                failures.append(dict(failure, cgroup=str(path)))
        if version == 1 and mount_root == PurePosixPath('/'):
            chain = [groups[(version, path)] for path in (member, *member.parents)]
            if all(group['status'] == 'valid' for group in chain):
                finite = [group['limit_bytes'] for group in chain if not group['unlimited']]
                if chain[0]['reported_hierarchical_limit_bytes'] != min(finite, default=None):
                    failures.append(issue('cgroup_memory',
                                          InvalidMeasurement('Hidden or changing legacy cgroup limits')))
    # It is unsafe to associate a successfully read hierarchy with a workload
    # whose member changed cgroups while the hierarchy was being read.
    for identity, expected in memberships.items():
        try:
            if _membership(identity.pid) != expected:
                raise InvalidMeasurement('Process moved between memory cgroups')
            fd = leader_pidfd if identity.pid == leader_pid else None
            if tracker is not None and identity.pid in tracker.members:
                fd = tracker.members[identity.pid][1]
            _recheck_member(identity, fd)
        except READ_ERRORS as exc:
            failures.append(dict(issue('cgroup_membership', exc), pid=identity.pid))
    return list(groups.values()), failures


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


@contextmanager
def _absolute_directory(root):
    """Open an absolute directory without following any symbolic links."""
    supervisor._check_io()
    fd = os.open('/', _DIRECTORY_FLAGS)
    try:
        for part in root.parts[1:]:
            supervisor._check_io()
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


@contextmanager
def campaign_directory(root):
    """Pin the campaign on the same mount as its authorized parent."""
    with _absolute_directory(root.parent) as parent_fd:
        fd = os.open(root.name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        try:
            if _mount_id(fd) != _mount_id(parent_fd):
                raise InvalidMeasurement('Campaign root mount changed')
            yield fd
        finally:
            os.close(fd)


def _mount_id(fd):
    supervisor._check_io()
    data = Path(f'/proc/self/fdinfo/{fd}').read_text(encoding='ascii').splitlines()
    values = [line.split()[1] for line in data if line.startswith('mnt_id:')]
    if len(values) != 1:
        raise InvalidMeasurement('Mount identity unavailable')
    return _uint(values[0])


def campaign_storage(root, root_fd):
    """Count allocated blocks and regular files in the pinned campaign tree."""
    supervisor._check_io()
    workspace._reject_submounts(root)
    _verify_campaign_directory(root, root_fd)
    mount = _mount_id(root_fd)
    blocks, count = _tree_usage(root_fd, mount)
    _verify_campaign_directory(root, root_fd)
    workspace._reject_submounts(root)
    return {'allocated_bytes': blocks * 512, 'file_count': count}


def _verify_campaign_directory(root, root_fd):
    with campaign_directory(root) as actual:
        if (_mount_id(actual) != _mount_id(root_fd) or
                not os.path.samestat(os.fstat(actual), os.fstat(root_fd))):
            raise InvalidMeasurement('Campaign directory changed')


def _tree_usage(fd, mount):
    blocks, files = os.fstat(fd).st_blocks, 0
    stack = [(fd, os.scandir(fd))]
    try:
        while stack:
            supervisor._check_io()
            directory, entries = stack[-1]
            entry = next(entries, None)
            if entry is None:
                _close_tree_frame(stack.pop(), fd)
                continue
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                blocks += info.st_blocks
                files += 1
                continue
            if not stat.S_ISDIR(info.st_mode):
                raise InvalidMeasurement('Campaign contains a link or a special file')
            child = os.open(entry.name, _DIRECTORY_FLAGS, dir_fd=directory)
            try:
                actual = os.fstat(child)
                if _mount_id(child) != mount or not os.path.samestat(actual, info):
                    raise InvalidMeasurement('Campaign directory or mount changed')
                stack.append((child, os.scandir(child)))
            except BaseException:
                os.close(child)
                raise
            blocks += actual.st_blocks
    finally:
        while stack:
            _close_tree_frame(stack.pop(), fd)
    return blocks, files


def _close_tree_frame(frame, root_fd):
    directory, entries = frame
    entries.close()
    if directory != root_fd:
        os.close(directory)


def temporary_filesystem():
    with _absolute_directory(Path('/tmp')) as fd:
        return filesystem(fd)


def filesystem(path_or_fd):
    supervisor._check_io()
    info = os.fstatvfs(path_or_fd) if isinstance(path_or_fd, int) else os.statvfs(path_or_fd)
    if info.f_frsize <= 0 or not 0 <= info.f_bavail <= info.f_bfree <= info.f_blocks:
        raise InvalidMeasurement('Invalid filesystem capacity')
    return {
        'total_bytes': info.f_blocks * info.f_frsize,
        'used_bytes': (info.f_blocks - info.f_bfree) * info.f_frsize,
        'available_bytes': info.f_bavail * info.f_frsize,
    }
