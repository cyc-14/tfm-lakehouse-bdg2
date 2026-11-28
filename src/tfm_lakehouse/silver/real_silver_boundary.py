"""Bind the audited electrical publication to a single supervised local launch."""

from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
import ctypes
import os
from pathlib import Path
import platform
import stat

from tfm_lakehouse import resource_linux as linux, resource_supervisor as supervisor
from tfm_lakehouse.silver import real_silver_preflight as preflight

BRONZE_RUN = 'd1705071767240b383851a522caa1dee'
PUBLICATION = 'electrical-' + BRONZE_RUN
CAMPAIGN = 'real-silver-' + BRONZE_RUN
OWNER = 'electrical-silver'
GRANT = 'electrical-silver-grant.json'
PROFILE = 'h05.3-real-electrical-v1'
TRANSFORM = 'h05.3-v1'
_STANDIN_RUN = '0' * 32
_AUDIT_BINDING = '798073bcbcac1f37df9100411ad3754bd7e16d01b0c307148f00eae879d0e784'
_PARENT_GRANT = ContextVar('real_silver_parent_grant', default=None)
_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _require(condition, message):
    if not condition:
        raise supervisor.SupervisorError(message)


def _scope(campaign):
    root = supervisor.workspace._lexical_root(campaign.root)
    _require(root.name == campaign.campaign_id, 'Electrical Silver campaign root differs')
    if campaign.campaign_id == CAMPAIGN and campaign.owner_id == OWNER:
        return 'real'
    _require(campaign.campaign_id.startswith('silver-standin-') and campaign.owner_id == 'standin-silver',
             'Unknown electrical Silver campaign identity')
    return 'standin'


def _locations(root, mode, request):
    if mode == 'real':
        return preflight._ROOT, request.output, request.evidence
    return (root / 'lakehouse', root / 'silver-publications' / request.publication_id,
            root / 'evidence' / (request.publication_id + '-publication'))


def authorization(campaign, request_data):
    """Describe the exact future grant. Building it does not authorize a launch."""
    mode = _scope(campaign)
    try:
        request = preflight.RealSilverRequest(
            request_data['bronze_run_id'], request_data['publication_id'],
            tuple(preflight.TableIdentity(**item) for item in request_data['tables']),
            tuple(preflight.FileIdentity(Path(item['relative_path']), item['size_bytes'], item['sha256'])
                  for item in request_data['inputs']),
        )
        standard = {'bronze_run_id': request.bronze_run_id, 'publication_id': request.publication_id,
                    'tables': request_data['tables'], 'inputs': request_data['inputs']}
        paths = preflight._required_paths(request)
        _require(standard == request_data and len(paths) == len(request.inputs) == 13
                 and {pin.relative_path for pin in request.inputs} == paths
                 and all(type(pin.size_bytes) is int and pin.size_bytes > 0
                         and preflight._SHA.fullmatch(pin.sha256) for pin in request.inputs),
                 'Invalid electrical Silver input identities')
    except (KeyError, TypeError, ValueError, preflight.RealSilverPreflightError) as exc:
        raise supervisor.SupervisorError('Invalid electrical Silver request') from exc
    if mode == 'real':
        _require(supervisor._digest(standard) == _AUDIT_BINDING, 'Real binding differs from the audited inputs')
    else:
        _require(request.bronze_run_id == _STANDIN_RUN and request.publication_id == 'electrical-' + _STANDIN_RUN
                 and tuple(t.version for t in request.tables) == (1, 1, 0), 'Invalid stand-in snapshot')
    root = campaign.root
    _, output, evidence = _locations(root, mode, request)
    run = request.publication_id
    return request, {
        'authorization': 'execute_once', 'mode': mode, 'request': standard,
        'campaign_id': campaign.campaign_id, 'owner_id': campaign.owner_id,
        'handler': supervisor._REAL_SILVER_HANDLER, 'profile': PROFILE, 'transform': TRANSFORM,
        'executable': supervisor._interpreter(), 'module': supervisor._CHILD_MODULE,
        'cohort': list(preflight.electrical_cohort()),
        'publication': str(output), 'evidence': str(evidence),
        'supervisor_evidence': str(root / 'evidence' / run),
        'temporary': str(root / 'temp'), 'spark_local': str(root / 'spark-local'),
        'spark_conf': str(root / 'spark-conf'), 'basetemp': str(root / 'temp' / ('pytest-' + run)),
        'effects': ['read_pinned_inputs', 'write_exclusive_publication_and_evidence',
                    'write_campaign_temporary_and_spark_local', 'write_campaign_supervisor_records',
                    'discard_process_output_to_dev_null'],
    }


def load_grant(campaign, approval_sha256):
    _require(isinstance(approval_sha256, str) and preflight._SHA.fullmatch(approval_sha256),
             'Explicit electrical Silver approval SHA-256 is required')
    with linux._absolute_directory(campaign.root) as root_fd:
        mount = linux._mount_id(root_fd)
    with checked_directory(campaign.root / 'spark-conf', mount) as conf:
        info = os.stat(GRANT, dir_fd=conf, follow_symlinks=False)
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                 and info.st_nlink == 1 and not info.st_mode & 0o022, 'Unsafe electrical Silver grant file')
        grant = supervisor._read_record(conf, GRANT)
        _require(os.path.samestat(info, os.stat(GRANT, dir_fd=conf, follow_symlinks=False)),
                 'Electrical Silver grant file moved')
    _require(isinstance(grant, dict) and supervisor._digest(grant) == approval_sha256,
             'Electrical Silver approval SHA-256 differs')
    request, expected = authorization(campaign, grant.get('request'))
    _require(supervisor._same(grant, expected), 'Electrical Silver grant differs from the permitted contract')
    return request, expected


@contextmanager
def checked_directory(path, mount):
    """Check each project component by descriptor, including mount and owner."""
    relative = path.relative_to(preflight._ROOT)
    with ExitStack() as stack:
        fd = stack.enter_context(linux._absolute_directory(preflight._ROOT))
        _check_directory(fd, mount)
        for part in relative.parts:
            supervisor._check_io()
            child = os.open(part, _DIR, dir_fd=fd)
            stack.callback(os.close, child)
            _check_directory(child, mount)
            fd = child
        yield fd


def _check_directory(fd, mount):
    info = os.fstat(fd)
    _require(info.st_uid == os.getuid() and not info.st_mode & 0o022 and linux._mount_id(fd) == mount,
             'Unexpected owner, permissions or mount in electrical Silver directory')


def _identity(fd):
    info = os.fstat(fd)
    return [linux._mount_id(fd), info.st_dev, info.st_ino]


def _destinations(grant):
    return {name: Path(grant[name]) for name in ('publication', 'evidence')}


def _validate_destinations(campaign, request, grant):
    base, _, _ = _locations(campaign.root, grant['mode'], request)
    paths = _destinations(grant)
    protected = [base / pin.relative_path for pin in request.inputs]
    protected.extend(Path(grant[k]) for k in ('temporary', 'spark_local', 'spark_conf', 'supervisor_evidence'))
    for name, path in paths.items():
        others = [*protected, *(p for k, p in paths.items() if k != name)]
        _require(all(not (path.is_relative_to(p) or p.is_relative_to(path)) for p in others),
                 'Electrical Silver destinations overlap inputs or other write roots')
        _require(preflight._excluded(path.relative_to(preflight._ROOT)), 'Destination is not Git-excluded')


def _mount(campaign):
    with linux._absolute_directory(campaign.root) as fd:
        return linux._mount_id(fd)


def configure(campaign, basetemp, run, parameters, *, reservations=None, child=False):
    request, grant = load_grant(campaign, parameters.approval_sha256)
    _require(grant['basetemp'] == str(basetemp) and request.publication_id == run,
             'Run or temporary path differs from electrical Silver approval')
    _require(grant['mode'] == 'standin' or child or _PARENT_GRANT.get() == parameters.approval_sha256,
             'Real electrical Silver requires the explicit parent launch entry point')
    _validate_destinations(campaign, request, grant)
    mount = _mount(campaign)
    if reservations is not None:
        _require(reservations == destination_identities(grant, mount),
                 'Electrical Silver reservations changed')
    return {
        'mode': grant['mode'], 'approval_sha256': parameters.approval_sha256,
        'output': grant['publication'], 'evidence': grant['evidence'], 'reservations': reservations,
    }


def reserve(campaign, binding, run):
    """Claim the exact two leaves once, leaving any partial reservation visible."""
    mount = _mount(campaign)
    request, grant = load_grant(campaign, binding['approval_sha256'])
    _require(run == request.publication_id and binding['output'] == grant['publication']
             and binding['evidence'] == grant['evidence'] and binding['mode'] == grant['mode'],
             'Electrical Silver reservation differs from approval')
    paths = _destinations(grant)
    # Check both leaves by descriptor before creating either one.
    for path in paths.values():
        with checked_directory(path.parent, mount) as parent:
            try:
                os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise supervisor.SupervisorError('Electrical Silver destination already exists')
    identities = {}
    for name, path in paths.items():
        with checked_directory(path.parent, mount) as parent:
            supervisor._check_io()
            os.mkdir(path.name, mode=0o700, dir_fd=parent)
            os.fsync(parent)
            fd = os.open(path.name, _DIR, dir_fd=parent)
            try:
                _check_directory(fd, mount)
                identities[name] = _identity(fd)
                if name == 'evidence':
                    supervisor._write_record(fd, 'reservation.json', {
                        'handler': supervisor._REAL_SILVER_HANDLER, 'run': run,
                        'approval_sha256': binding['approval_sha256'], 'status': 'reserved',
                    })
            finally:
                os.close(fd)
    return identities


def destination_identities(grant, mount):
    identities = {}
    for name, path in _destinations(grant).items():
        with checked_directory(path, mount) as fd:
            identities[name] = _identity(fd)
    return identities


def verify_inputs(campaign, request, grant):
    """Revalidate the thirteen pins, with weather included only as an attestation."""
    if grant['mode'] == 'real':
        preflight.verify_real_silver_inputs(request, authorized=request)
    else:
        root, _, _ = _locations(campaign.root, grant['mode'], request)
        with checked_directory(root, _mount(campaign)) as fd:
            for pin in request.inputs:
                preflight._verify_file(pin, fd, linux._mount_id(fd))


def protect_writes(campaign, grant, binding):
    """Inherit a Linux write boundary in the driver, JVM and Python workers."""
    mount = _mount(campaign)
    _require(binding['reservations'] == destination_identities(grant, mount),
             'Electrical Silver destinations moved before startup')
    # Landlock ABI 3 mediates truncation as well as writes, creation and rename.
    _require(platform.machine() in ('x86_64', 'aarch64'), 'No verified Linux write isolation on this architecture')
    libc = ctypes.CDLL(None, use_errno=True)
    create, add, restrict = 444, 445, 446
    _require(libc.syscall(create, 0, 0, 1) >= 3, 'Linux Landlock ABI 3 is required')
    handled = sum(1 << bit for bit in (1, *range(4, 15)))
    allowed = sum(1 << bit for bit in (1, 4, 5, 7, 8, 13, 14))

    class PathRule(ctypes.Structure):
        _pack_ = 1
        _fields_ = [('access', ctypes.c_uint64), ('parent', ctypes.c_int32)]

    mask = ctypes.c_uint64(handled)
    ruleset = libc.syscall(create, ctypes.byref(mask), ctypes.sizeof(mask), 0)
    if ruleset < 0:
        raise OSError(ctypes.get_errno(), 'Could not initialize Linux write isolation')
    try:
        # Git's read-only commands and the Spark launcher use the null device.
        null = os.open('/dev/null', os.O_PATH | os.O_NOFOLLOW)
        try:
            info = os.fstat(null)
            _require(stat.S_ISCHR(info.st_mode) and (os.major(info.st_rdev), os.minor(info.st_rdev)) == (1, 3),
                     'The process null device has an unexpected identity')
            rule = PathRule((1 << 1) | (1 << 14), null)
            if libc.syscall(add, ruleset, 1, ctypes.byref(rule), 0) != 0:
                raise OSError(ctypes.get_errno(), 'Could not permit the process null device')
        finally:
            os.close(null)
        for key in ('temporary', 'spark_local', 'supervisor_evidence', 'publication', 'evidence'):
            with checked_directory(Path(grant[key]), mount) as directory:
                rule = PathRule(allowed, directory)
                if libc.syscall(add, ruleset, 1, ctypes.byref(rule), 0) != 0:
                    raise OSError(ctypes.get_errno(), 'Could not restrict an authorized write directory')
        if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.syscall(restrict, ruleset, 0) != 0:
            raise OSError(ctypes.get_errno(), 'Could not enforce Linux write isolation')
    finally:
        os.close(ruleset)


def launch_real(approval_sha256):
    """Dedicated one-shot parent entry point for a later authorized real execution."""
    workspace = supervisor.workspace
    root = workspace.SCALABILITY_ROOT / CAMPAIGN
    _require(os.environ.get('TFM_REAL_CONFIG') == str(root / 'spark-conf' / GRANT),
             'TFM_REAL_CONFIG must point to the exact electrical Silver grant')
    # D39 and the isolated child must never inherit the real opt-in.
    del os.environ['TFM_REAL_CONFIG']
    token = _PARENT_GRANT.set(approval_sha256)
    try:
        campaign = workspace.authorize_campaign(CAMPAIGN, OWNER)
        return supervisor.launch_supervised_workload(
            campaign, root / 'temp' / ('pytest-' + PUBLICATION), PUBLICATION,
            handler=supervisor._REAL_SILVER_HANDLER,
            parameters=supervisor.RealSilverParameters(approval_sha256),
        )
    finally:
        _PARENT_GRANT.reset(token)
        os.environ['TFM_REAL_CONFIG'] = str(root / 'spark-conf' / GRANT)
