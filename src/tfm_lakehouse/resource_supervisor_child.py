"""Authorize a registered D44 probe in a fresh interpreter."""

from dataclasses import asdict
import os
from pathlib import Path
import socket
import struct
import sys

from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace


def _evidence_path(fd):
    # Read the descriptor's pathname, then reject foreign paths lexically.
    path = Path(os.readlink(f'/proc/self/fd/{fd}'))
    if (path.parent.name != 'evidence' or path.parent.parent.parent != workspace.SCALABILITY_ROOT):
        raise supervisor.SupervisorError('Child evidence descriptor is outside D39')
    supervisor._identifier(path.name)
    workspace._lexical_root(path.parent.parent)
    return path


def _parent_identity(channel, identity):
    credentials = struct.Struct('3i')
    peer_pid, peer_uid, peer_gid = credentials.unpack(
        channel.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, credentials.size))
    if (peer_pid != identity.parent_pid or peer_uid != os.getuid() or
            peer_gid != os.getgid() or identity.pid != identity.session_id or
            identity.pid != identity.process_group):
        raise supervisor.SupervisorError('Child is outside the launch process session')
    return supervisor.read_process_identity(peer_pid)


def _authorize(ticket, evidence, evidence_fd):
    config = ticket['configuration']
    campaign = workspace.authorize_campaign(config['campaign_id'], config['owner_id'])
    basetemp = Path(config['basetemp'])
    handler = config['handler']
    if not isinstance(handler, str) or handler not in supervisor._HANDLERS:
        raise supervisor.SupervisorError('Unknown workload handler')
    parameters = supervisor._HANDLERS[handler][0](**config['parameters'])
    expected = supervisor._configuration(campaign, basetemp, config['run_id'], handler, parameters)
    if (not supervisor._same(config, expected) or
            evidence != campaign.root / 'evidence' / config['run_id'] or
            not os.path.samestat(os.fstat(evidence_fd), evidence.lstat())):
        raise supervisor.SupervisorError('Child effective configuration differs')
    with supervisor._directory(evidence.parent / 'pytest-claims') as claim_fd:
        claim = supervisor._read_record(claim_fd, supervisor._claim_name(basetemp))
    if not supervisor._same(claim, ticket):
        raise supervisor.SupervisorError('Child basetemp claim differs')
    return handler


def main():
    if ('TFM_REAL_CONFIG' in os.environ or len(sys.argv) != 3 or
            sys.flags.isolated != 1 or sys.flags.no_site != 1 or not sys.dont_write_bytecode):
        raise supervisor.SupervisorError('Invalid child environment')
    channel_fd, evidence_fd = map(int, sys.argv[1:])
    command = supervisor._command(channel_fd, evidence_fd)
    if (sys.orig_argv != command or
            Path(__file__).absolute() != supervisor._CHILD_SOURCE / 'tfm_lakehouse'
            / 'resource_supervisor_child.py'):
        raise supervisor.SupervisorError('Child command differs')
    with socket.socket(fileno=channel_fd) as channel:
        evidence = _evidence_path(evidence_fd)
        identity = supervisor.read_process_identity(os.getpid())
        parent = _parent_identity(channel, identity)
        launch = supervisor._read_record(evidence_fd, 'launch.json')
        ticket = launch['ticket']
        if not supervisor._same(launch['command'], command) or not supervisor._same(
                ticket['parent'], asdict(parent)):
            raise supervisor.SupervisorError('Child launch evidence differs')

        # The child writes the receipt itself before it needs a message from
        # the parent. An interruption of Popen cannot authorize the payload.
        supervisor._write_record(evidence_fd, 'child-pid.json',
                                 {'identity': asdict(identity), 'parent': asdict(parent)})
        if not supervisor._same(supervisor._receive(channel), ticket):
            raise supervisor.SupervisorError('Child launch ticket differs')
        handler = _authorize(ticket, evidence, evidence_fd)
        grant = {'ticket_sha256': supervisor._digest(ticket), 'identity': asdict(identity)}
        supervisor._send(channel, grant)
        if (not supervisor._same(supervisor._receive(channel), grant) or
                not supervisor._same(supervisor._read_record(evidence_fd, 'child-identity.json'), grant)
                or _parent_identity(channel, identity) != parent):
            raise supervisor.SupervisorError('Child authorization rejected')
        _authorize(ticket, evidence, evidence_fd)
        supervisor._HANDLERS[handler][1](evidence_fd, ticket['configuration'])
        supervisor._send(channel, {'status': 'probe_passed'})


if __name__ == '__main__':
    main()
