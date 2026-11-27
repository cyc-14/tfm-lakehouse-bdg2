"""Restricted child-side handshake for the synthetic D44 probe."""
import os
from pathlib import Path
import socket
import sys

from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace


def main():
    if 'TFM_REAL_CONFIG' in os.environ or len(sys.argv) != 2:
        raise supervisor.SupervisorError('Invalid child environment')
    channel = socket.socket(fileno=int(sys.argv[1]))
    channel.settimeout(15)
    with channel:
        request = supervisor._receive(channel)
        config, nonce = request['configuration'], request['nonce']
        campaign = workspace.authorize_campaign(config['campaign_id'], config['owner_id'])
        basetemp = Path(config['basetemp'])
        supervisor.preflight(campaign, basetemp)
        expected = supervisor._configuration(campaign, basetemp, config['run_id'], channel.fileno())
        if (config != expected or config['payload'] != 'synthetic_probe_v1'
                or sys.flags.isolated != 1 or not sys.dont_write_bytecode
                or Path(__file__).absolute() != workspace.PROJECT_ROOT / 'src' / 'tfm_lakehouse'
                   / 'resource_supervisor_child.py'):
            raise supervisor.SupervisorError('Child configuration differs')
        identity = supervisor.read_process_identity(os.getpid())
        supervisor._send(channel, {'configuration_sha256': supervisor._digest(config), 'nonce': nonce,
                                   'pid': identity.pid, 'start_ticks': identity.start_ticks,
                                   'process_group': identity.process_group, 'session_id': identity.session_id})
        if supervisor._receive(channel) != {'authorized': nonce}:
            raise supervisor.SupervisorError('Child authorization rejected')
        supervisor.preflight(campaign, basetemp)
        supervisor._send(channel, {'status': 'probe_passed', 'nonce': nonce})


if __name__ == '__main__':
    main()
