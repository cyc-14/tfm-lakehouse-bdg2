"""Exercise the real electrical Silver boundary with invented inputs only."""

import csv
from copy import deepcopy
from dataclasses import asdict
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from uuid import uuid4

import pytest

from tfm_lakehouse import resource_linux, resource_sampler, resource_spark
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse.silver import real_silver_boundary as boundary
from tfm_lakehouse.silver import real_silver_preflight as preflight
from tfm_lakehouse.silver import real_silver_workload as workload

_RUN = '0' * 32
_PUBLICATION = 'electrical-' + _RUN
_BUILDINGS = preflight.electrical_cohort()
_TABLES = tuple(preflight.TableIdentity(name, version, str(uuid4()), 'part-invented.parquet')
                for name, version in zip(preflight._SOURCE, (1, 1, 0), strict=True))
_VALUES = ('0', None, 'NaN', '-0.000', '-1.2', '1e-19', '3.1234567890123456789', 'Infinity',
           '18', '99999999999999999999.999999999999999999', '1e20', '3e-2', ' 5', '1.2340',
           '7', '-.1')
_LABEL = '2016-02-29 01:02:03'


def _invented_request(base, *, materialized=False):
    provisional = preflight.RealSilverRequest(_RUN, _PUBLICATION, _TABLES, (None,) * 13)
    pins = []
    for path in sorted(preflight._required_paths(provisional)):
        target = base / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if not materialized:
            target.write_bytes(b'invented')
        data = target.read_bytes()
        pins.append({'relative_path': str(path), 'size_bytes': len(data),
                     'sha256': hashlib.sha256(data).hexdigest()})
    return {'bronze_run_id': _RUN, 'publication_id': _PUBLICATION,
            'tables': [asdict(table) for table in _TABLES], 'inputs': pins}


@pytest.fixture
def invented(tmp_path, monkeypatch):
    monkeypatch.setattr(preflight, '_ROOT', tmp_path)
    monkeypatch.setattr(preflight, '_excluded', lambda _: True)
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', tmp_path)
    root = tmp_path / 'silver-standin-unit'
    for name in ('spark-conf', 'silver-publications', 'evidence', 'temp', 'spark-local'):
        (root / name).mkdir(parents=True)
    campaign = workspace.Campaign(root, root.name, 'standin-silver')
    request_data = _invented_request(root / 'lakehouse')
    _, grant = boundary.authorization(campaign, request_data)
    return campaign, request_data, grant


def _install_grant(campaign, grant):
    with supervisor._directory(campaign.root / 'spark-conf') as fd:
        supervisor._write_record(fd, boundary.GRANT, grant)
    return supervisor.RealSilverParameters(supervisor._digest(grant))


@pytest.mark.unit
def test_registered_workload_rejects_unsupervised_and_unapproved_launch(tmp_path):
    with pytest.raises(supervisor.SupervisorError, match='requires supervised'):
        supervisor.launch_workload(None, None, 'run', handler=supervisor._REAL_SILVER_HANDLER,
                                   parameters=supervisor.RealSilverParameters('0' * 64))
    with pytest.raises(supervisor.SupervisorError, match='exact electrical Silver grant'):
        boundary.launch_real('0' * 64)
    assert not list(tmp_path.iterdir())


@pytest.mark.unit
@pytest.mark.parametrize('field,value', [
    ('profile', 'd45-electrical-synthetic-v1'), ('publication', '/other/publication'),
    ('evidence', '/other/evidence'), ('temporary', '/other/temp'), ('executable', '/usr/bin/python'),
])
def test_self_consistent_but_unauthorized_grant_fails(invented, field, value):
    campaign, _, grant = invented
    grant[field] = value
    parameters = _install_grant(campaign, grant)
    with pytest.raises(supervisor.SupervisorError, match='permitted contract'):
        boundary.load_grant(campaign, parameters.approval_sha256)


@pytest.mark.unit
@pytest.mark.parametrize('field', ('sha256', 'version', 'uuid'))
def test_real_binding_cannot_be_changed_with_a_new_approval_digest(invented, field, monkeypatch):
    _, request, _ = invented
    campaign = workspace.Campaign(preflight._ROOT / boundary.CAMPAIGN, boundary.CAMPAIGN, boundary.OWNER)
    # Invented audit pin exercises the production path without inspecting real files.
    monkeypatch.setattr(boundary, '_AUDIT_BINDING', supervisor._digest(request))
    assert boundary.authorization(campaign, request)[1]['mode'] == 'real'
    altered = deepcopy(request)
    if field == 'sha256':
        altered['inputs'][0][field] = '0' * 64
    elif field == 'version':
        altered['tables'][0][field], altered['tables'][2][field] = 0, 1
        altered['inputs'] = [
            dict(pin, relative_path=pin['relative_path'].replace('bronze/electricity/_delta_log', 'bronze/weather/_delta_log'))
            if pin['relative_path'].endswith('electricity/_delta_log/00000000000000000001.json') else pin
            for pin in altered['inputs']
        ]
    else:
        altered['tables'][0][field] = str(uuid4())
    with pytest.raises(supervisor.SupervisorError, match='audited inputs'):
        boundary.authorization(campaign, altered)


@pytest.mark.unit
def test_standin_cannot_rebind_the_campaign_directory(invented):
    campaign, request, _ = invented
    wrong = workspace.Campaign(campaign.root.parent / 'other-root', campaign.campaign_id, campaign.owner_id)
    with pytest.raises(supervisor.SupervisorError, match='campaign root differs'):
        boundary.authorization(wrong, request)


@pytest.mark.unit
def test_unapproved_digest_and_preexisting_destination_fail_closed(invented):
    campaign, _, grant = invented
    parameters = _install_grant(campaign, grant)
    with pytest.raises(supervisor.SupervisorError, match='approval SHA-256 differs'):
        boundary.load_grant(campaign, '0' * 64)
    binding = boundary.configure(campaign, Path(grant['basetemp']), _PUBLICATION, parameters)
    identities = boundary.reserve(campaign, binding, _PUBLICATION)
    assert identities == boundary.destination_identities(grant, boundary._mount(campaign))
    with pytest.raises(supervisor.SupervisorError, match='already exists'):
        boundary.reserve(campaign, binding, _PUBLICATION)


@pytest.mark.unit
def test_dangling_destination_prevents_either_reservation(invented):
    campaign, _, grant = invented
    parameters = _install_grant(campaign, grant)
    binding = boundary.configure(campaign, Path(grant['basetemp']), _PUBLICATION, parameters)
    Path(grant['evidence']).symlink_to(campaign.root / 'missing')
    with pytest.raises(supervisor.SupervisorError, match='already exists'):
        boundary.reserve(campaign, binding, _PUBLICATION)
    assert not os.path.lexists(grant['publication'])


@pytest.mark.unit
def test_storage_sampler_charges_both_external_destinations(invented):
    campaign, _, _ = invented
    output, evidence = (campaign.root.parent / name for name in ('external-output', 'external-evidence'))
    for path in (output, evidence):
        path.mkdir(mode=0o700)
        (path / 'bytes').write_bytes(b'0' * 8192)
    binding = {'mode': 'real', 'output': str(output), 'evidence': str(evidence), 'reservations': {}}
    with resource_linux.campaign_directory(campaign.root) as fd:
        mount = resource_linux._mount_id(fd)
        baseline = resource_linux.campaign_storage(campaign.root, fd)
        extra = 0
        for name, path in (('publication', output), ('evidence', evidence)):
            with boundary.checked_directory(path, mount) as external:
                binding['reservations'][name] = boundary._identity(external)
                extra += resource_linux._tree_usage(external, mount)[0] * 512
        sampler = resource_sampler.ResourceSampler(campaign.root, fd, None, real_silver=binding)
        assert sampler._campaign_storage() == {
            'allocated_bytes': baseline['allocated_bytes'] + extra, 'file_count': baseline['file_count'] + 2,
        }
        binding['reservations']['publication'] = binding['reservations']['evidence']
        with pytest.raises(resource_linux.InvalidMeasurement, match='directory changed'):
            sampler._campaign_storage()


@pytest.mark.unit
def test_launch_requires_child_grant_before_jvm(monkeypatch, invented):
    campaign, _, grant = invented
    parameters = _install_grant(campaign, grant)
    monkeypatch.setattr(workspace, 'authorize_campaign', lambda *_: campaign)
    monkeypatch.setattr(resource_spark, 'spark_profile', lambda *_: {})
    monkeypatch.setattr(resource_spark, '_load_spark', lambda: pytest.fail('JVM initialized without a grant'))
    config = {'handler': supervisor._REAL_SILVER_HANDLER, 'root': str(campaign.root),
              'campaign_id': campaign.campaign_id, 'owner_id': campaign.owner_id,
              'run_id': _PUBLICATION, 'spark': {},
              'real_silver': boundary.configure(campaign, Path(grant['basetemp']), _PUBLICATION, parameters)}
    with supervisor._directory(campaign.root / 'evidence') as fd, pytest.raises(FileNotFoundError):
        resource_spark.spark_probe(fd, config)
    assert not list((campaign.root / 'evidence').iterdir())


@pytest.mark.unit
def test_landlock_blocks_source_mutation_and_allows_only_bound_write_roots(invented):
    campaign, _, grant = invented
    parameters = _install_grant(campaign, grant)
    binding = boundary.configure(campaign, Path(grant['basetemp']), _PUBLICATION, parameters)
    binding['reservations'] = boundary.reserve(campaign, binding, _PUBLICATION)
    Path(grant['supervisor_evidence']).mkdir()
    source = campaign.root / 'lakehouse' / preflight._SOURCE['electricity']
    script = """
import json, os, sys
from pathlib import Path
from tfm_lakehouse import scalability_workspace
from tfm_lakehouse.silver import real_silver_boundary as b, real_silver_preflight
root, grant, binding, source = sys.argv[1:]
root, source = Path(root), Path(source)
real_silver_preflight._ROOT = root.parent
grant, binding = json.loads(grant), json.loads(binding)
b.protect_writes(scalability_workspace.Campaign(root, root.name, 'standin-silver'), grant, binding)
scalability_workspace._check_git_exclusion()
with open('/dev/null', 'w') as sink:
    sink.write('discarded')
for path in (source, root / 'outside.txt', root / 'spark-conf' / 'unapproved'):
    try:
        with path.open('w') as out:
            out.write('must be blocked')
    except PermissionError:
        pass
    else:
        raise AssertionError(path)
for key in ('publication', 'evidence', 'temporary', 'spark_local', 'supervisor_evidence'):
    (Path(grant[key]) / 'allowed').write_text('permitted')
assert source.read_bytes() == b'invented'
"""
    subprocess.run([sys.executable, '-B', '-c', script, str(campaign.root), json.dumps(grant),
                    json.dumps(binding), str(source)], check=True, timeout=10)


@pytest.mark.unit
def test_independent_witness_keeps_values_nulls_and_duplicate_rows():
    source, out = StringIO(), StringIO()
    csv.writer(source).writerows([('timestamp', *_BUILDINGS), (_LABEL, *_VALUES), (_LABEL, *_VALUES)])
    source.seek(0)
    counts = workload._write_expected(source, out, _BUILDINGS, ('uri', 'sha', 'synthetic', 'reference'),
                                      _RUN, 1, '/table', real=False)
    rows = [json.loads(record) for record in out.getvalue().splitlines()]
    assert counts == {'wide_rows': 2, 'positions': 32, 'r5_absent': 4, 'r5_present': 28, 'numeric_zeros': 4}
    assert rows[:16] == rows[16:] and all(tuple(row) == workload._COLUMNS for row in rows)
    assert [row['value_parse_status'] for row in rows[:11]] == [
        'finite', 'missing_null', 'missing_token', 'finite', 'finite', 'underflow',
        'scale_exceeded', 'nonfinite_literal', 'finite', 'finite', 'overflow',
    ]
    assert rows[9]['electricity_value_numeric'] == _VALUES[9]
    assert rows[-1]['nominal_second_of_day'] == 3723


@pytest.mark.unit
def test_reserved_publication_retains_primary_exception_and_releases_frame(tmp_path):
    from tfm_lakehouse.silver.silver_synthetic_publication import _persist_publication

    primary = RuntimeError('materialization failure')
    cleanup = OSError('unpersist failure')
    operations = []

    class BrokenFrame:
        def persist(self, _level):
            return self

        def count(self):
            raise primary

        def unpersist(self):
            operations.append('unpersist')
            raise cleanup

    with pytest.raises(RuntimeError) as caught:
        _persist_publication(None, tmp_path, BrokenFrame(), {'output_rows': 1}, reserved=True)
    assert caught.value is primary
    assert primary._h06_cleanup_failures == [cleanup]
    assert operations == ['unpersist'] and not list(tmp_path.iterdir())


def _seed_standin(campaign):
    """Create two tiny Parquet fixtures in a separate, owned Spark process."""
    from pyspark.sql import types as T
    from tfm_lakehouse.spark import spark_session

    base = campaign.root / 'lakehouse'
    report = {'report_contract_version': 1, 'run_id': _RUN, 'status': 'passed', 'data_kind': 'synthetic',
              'dataset_reference': workload._STANDIN_REFERENCE, 'tables': [{'name': 'weather'}]}
    with spark_session('invented-electrical-fixture', master='local[2]', campaign=campaign) as spark:
        for table in _TABLES:
            name = table.name
            root = base / preflight._BRONZE / 'bronze' / name
            source_relative = preflight._SOURCE[name]
            source = base / source_relative
            source.parent.mkdir(parents=True, exist_ok=True)
            if name == 'weather':
                source.write_bytes(b'timestamp,site_id\n2016-02-29 01:02:03,invented\n')
                (root / '_delta_log').mkdir(parents=True)
                (root / '_delta_log' / '00000000000000000000.json').write_bytes(b'{}\n')
                (root / table.active_file).write_bytes(b'weather is attestation-only')
                continue
            if name == 'electricity':
                columns = ('timestamp', *_BUILDINGS)
                rows = [(_LABEL, *_VALUES), (_LABEL, *_VALUES)]
            else:
                columns = ('building_id', 'electricity')
                rows = [(building, 'Yes') for building in _BUILDINGS]
            with source.open('w', encoding='utf-8', newline='') as out:
                csv.writer(out).writerows([columns, *rows])
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            lineage = (source.as_uri(), source_hash, 'synthetic', workload._STANDIN_REFERENCE)
            schema = T.StructType([T.StructField(c, T.StringType()) for c in (*columns, *workload._LINEAGE)])
            frame = spark.createDataFrame([(*row, *lineage) for row in rows], schema)
            spark_folder = campaign.root / 'temp' / (name + '-fixture')
            frame.coalesce(1).write.mode('errorifexists').parquet(str(spark_folder))
            (root / '_delta_log').mkdir(parents=True)
            shutil.copyfile(next(spark_folder.glob('part-*.parquet')), root / table.active_file)
            (root / ('.' + table.active_file + '.crc')).write_bytes(b'crc\x00\x00\x00\x02\x00\x00\x00\x00\x00')
            commit = {'run_id': _RUN, 'table_name': name, 'data_kind': 'synthetic',
                      'dataset_reference': workload._STANDIN_REFERENCE, 'source_sha256': source_hash}
            logs = [
                [{'metaData': {'id': table.uuid, 'schemaString': schema.json(), 'partitionColumns': []}},
                 {'add': {'path': 'part-before.parquet'}}],
                [{'commitInfo': {'userMetadata': json.dumps(commit)}},
                 {'remove': {'path': 'part-before.parquet'}}, {'add': {'path': table.active_file}}],
            ]
            for version, actions in enumerate(logs):
                (root / '_delta_log' / f'{version:020}.json').write_text(
                    ''.join(json.dumps(action) + '\n' for action in actions), encoding='utf-8')
            report['tables'].append({
                'name': name, 'status': 'verified', 'source_uri': source.as_uri(), 'source_sha256': source_hash,
                'source_bytes': source.stat().st_size, 'table_path': str(root), 'delta_version': 1,
                'output_rows': len(rows), 'output_schema': schema.jsonValue(),
            })
    manifest = base / preflight._MANIFEST
    manifest.write_text('{"description":"invented"}\n', encoding='utf-8')
    report_path = base / preflight._BRONZE / '_runs' / (_RUN + '.json')
    report_path.parent.mkdir()
    report_path.write_text(json.dumps(report), encoding='utf-8')
    _, grant = boundary.authorization(campaign, _invented_request(base, materialized=True))
    print(_install_grant(campaign, grant).approval_sha256, flush=True)


@pytest.mark.integration
def test_one_supervised_standin_publication():
    assert 'TFM_REAL_CONFIG' not in os.environ
    campaign = workspace.reserve_campaign('silver-standin-' + uuid4().hex, 'standin-silver')
    jars = campaign.root / 'ivy' / 'jars'
    jars.mkdir()
    originals = Path(os.environ['H06_EXISTING_OFFLINE_JARS'])
    for name in workspace.CAMPAIGN_JARS:
        shutil.copyfile(originals / name, jars / name)
    env = dict(os.environ, SPARK_LOCAL_IP='127.0.0.1', PYTHONDONTWRITEBYTECODE='1')
    seed = subprocess.run([sys.executable, '-B', str(Path(__file__).absolute()), campaign.campaign_id],
                          env=env, check=True, text=True, stdout=subprocess.PIPE, timeout=90)
    parameters = supervisor.RealSilverParameters(seed.stdout.strip().splitlines()[-1])
    evidence = campaign.root / 'evidence' / _PUBLICATION
    result = supervisor.launch_supervised_workload(
        campaign, campaign.root / 'temp' / ('pytest-' + _PUBLICATION), _PUBLICATION,
        handler=supervisor._REAL_SILVER_HANDLER, parameters=parameters,
    )
    assert result['status'] == 'supervised_success', (evidence, result.get('watchdog'), result['spark']['shutdown'])
    proof = result['real_silver']
    assert proof['publication']['delta_version'] == 0
    assert proof['publication']['output_rows'] == 32
    assert proof['counts']['numeric_zeros'] == 4
    assert len(proof['publication']['schema']['fields']) == 19
    assert result['spark']['heap']['latest']['maximum_bytes'] == 1024**3
    assert result['spark']['events']['status'] == 'complete'
    assert result['spark']['shutdown']['gateway_collected']
    assert result['watchdog']['unresolved_processes'] == []
    print(f'Stand-in evidence: {evidence}', flush=True)


if __name__ == '__main__':
    _seed_standin(workspace.authorize_campaign(sys.argv[1], 'standin-silver'))
