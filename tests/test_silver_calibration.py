"""Focused checks of the D48-only electrical publication boundary and verifier."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse import silver_calibration as d45
from tfm_lakehouse.silver import silver_synthetic_publication as publication


@pytest.mark.unit
def test_d45_registry_is_supervised_and_has_no_input_overrides():
    assert supervisor._HANDLERS[supervisor._SILVER_HANDLER][0] is supervisor.SilverCalibrationParameters
    with pytest.raises(TypeError):
        supervisor.SilverCalibrationParameters(buildings=('unapproved',))
    with pytest.raises(supervisor.SupervisorError, match='requires supervised'):
        supervisor.launch_workload(None, None, 'test', handler=supervisor._SILVER_HANDLER,
                                   parameters=supervisor.SilverCalibrationParameters())
    with pytest.raises(publication.SilverPublicationError, match='Unsupported transformation'):
        publication.publish_synthetic_silver(None, None, 'source', 'test', None,
                                             transform_version=d45._PROFILE)


@pytest.mark.unit
def test_d45_unapproved_campaign_is_rejected_before_inspection(monkeypatch, tmp_path):
    monkeypatch.setattr(workspace, 'validate_authorized_campaign',
                        lambda *_: pytest.fail('cohort or campaign validation accessed paths'))
    with pytest.raises(supervisor.SupervisorError, match='not approved'):
        d45.binding(workspace.Campaign(tmp_path, 'foreign', 'owner'), 'test')


@pytest.mark.unit
@pytest.mark.parametrize('fault', ['run', 'cohort', 'version', 'uuid'])
def test_d45_rejects_wrong_bronze_identities(monkeypatch, tmp_path, fault):
    from tfm_lakehouse import bronze_calibration

    campaign_id = 'd45-bronze-calibration-2ee414e7897946eeb3df6d76ac771c91'
    snapshot = d45._SNAPSHOTS[campaign_id]
    campaign = workspace.Campaign(tmp_path, campaign_id, snapshot.owner)
    config = bronze_calibration._config(campaign)
    monkeypatch.setattr(workspace, 'validate_authorized_campaign', lambda _: tmp_path)
    monkeypatch.setattr(workspace, 'validate_synthetic_config', lambda *_: tmp_path)
    monkeypatch.setattr(d45, '_fingerprint', lambda _: 'source-sha')
    target = config.table_path('electricity')
    (target / '_delta_log').mkdir(parents=True)
    commit = target / '_delta_log' / '00000000000000000000.json'
    commit.touch()
    item = {'name': 'electricity', 'status': 'verified', 'output_rows': 8,
            'delta_version': 1 if fault == 'version' else 0,
            'table_path': str(target), 'source_uri': config.electricity.as_uri(),
            'source_sha256': 'source-sha', 'output_schema': {}}
    cohort = ['timestamp', *d45._BUILDINGS]
    if fault == 'cohort':
        cohort[1] = 'unapproved'
    records = {
        'calibration-manifest.json': {'sources': {
            'electricity.csv': {'columns': cohort, 'sha256': 'source-sha'}}},
        snapshot.run + '.json': {'status': 'passed', 'data_kind': 'synthetic', 'tables': [item],
                                 'run_id': 'wrong' if fault == 'run' else snapshot.run},
        'completed.json': {'status': 'supervised_success', 'bronze': {
            'status': 'passed', 'report_run_id': snapshot.run,
            'manifest_sha256': snapshot.fixture_sha256},
            'watchdog': {'collection_complete': True, 'unresolved_processes': []}},
        commit.name: [{'metaData': {'id': 'wrong'}}, {'add': {'path': 'file.parquet'}},
                      {'commitInfo': {}}],
    }
    monkeypatch.setattr(d45, '_record', lambda path, *_args, **_kwargs: records[path.name])
    with pytest.raises(supervisor.SupervisorError, match='(?i)bronze .*differs'):
        d45.binding(campaign, 'run1')


@pytest.mark.unit
def test_ordinary_reader_cannot_resolve_d45_publication(tmp_path):
    from tfm_lakehouse.silver import silver_iqr_publication as ordinary_reader

    folder = tmp_path / d45._DIRECTORY / 'pub1'
    folder.mkdir(parents=True)
    (folder / 'intent.json').write_text(json.dumps({'transform_version': d45._PROFILE}))
    with pytest.raises(ordinary_reader.IQRPublicationError, match='directory is unavailable'):
        ordinary_reader._verify_silver(None, None, tmp_path, 'pub1')


@pytest.mark.unit
def test_d45_record_rejects_changed_manifest_or_commit(tmp_path):
    record = tmp_path / 'evidence.json'
    record.write_bytes(b'{"version":0}')
    digest = hashlib.sha256(record.read_bytes()).hexdigest()
    assert d45._record(record, digest) == {'version': 0}
    record.write_bytes(b'{"version":1}')
    with pytest.raises(supervisor.SupervisorError, match='fingerprint differs'):
        d45._record(record, digest)


@pytest.mark.unit
def test_d45_destination_must_be_exclusive(monkeypatch, tmp_path):
    monkeypatch.setattr(d45, '_bound_inputs', lambda *_: (None, None, {'source': 'verified'}))
    campaign = workspace.Campaign(tmp_path, 'fixture', 'owner')
    target = tmp_path / d45._DIRECTORY / 'already'
    target.mkdir(parents=True)
    with pytest.raises(supervisor.SupervisorError, match='already exists'):
        d45.binding(campaign, 'already', fresh=True)
    assert d45.binding(campaign, 'new', fresh=True) == {'source': 'verified'}


@pytest.mark.unit
def test_d45_execution_requires_the_owned_grant(monkeypatch, tmp_path):
    campaign = workspace.Campaign(tmp_path, 'test', 'owner')
    evidence = tmp_path / 'evidence' / 'run1'
    evidence.mkdir(parents=True)
    monkeypatch.setattr(workspace, 'validate_authorized_campaign', lambda _: tmp_path)
    monkeypatch.setattr(d45, '_bound_inputs', lambda *_: pytest.fail('source opened without grant'))
    with supervisor._directory(evidence) as fd, pytest.raises(FileNotFoundError, match='launch.json'):
        d45.execute(None, campaign, fd, 'run1', {})


@pytest.mark.integration
def test_d45_shared_engine_and_reader_exactness(spark, tmp_path, monkeypatch):
    from pyspark.sql import functions as F
    from tfm_lakehouse.silver.silver_electricity import _LINEAGE_COLUMNS
    from tfm_lakehouse.silver.silver_electricity_integrated import (
        _transform_electricity, transform_selected_electricity,
    )

    campaign = workspace.Campaign(tmp_path, 'test', 'owner')
    config = SimpleNamespace(dataset_reference='invented')
    item = {'name': 'electricity', 'table_path': '/synthetic/bronze/electricity',
            'delta_version': 0, 'source_sha256': 'a' * 64, 'output_rows': 1,
            'source_uri': 'file:///synthetic/electricity.csv'}
    source = {'source_run_id': 'source', 'manifest_sha256': 'b' * 64, 'fixture_sha256': 'c' * 64,
              'buildings': list(d45._BUILDINGS), 'source_tables': [item]}
    report = {'run_id': 'source', 'tables': [item]}
    monkeypatch.setattr(d45, '_bound_inputs', lambda _: (config, report, source))
    values = ('2016-01-01 00:00:00', *(str(i) for i in range(16)),
              item['source_uri'], item['source_sha256'], 'synthetic', config.dataset_reference)
    bronze = spark.createDataFrame([values], ['timestamp', *d45._BUILDINGS, *_LINEAGE_COLUMNS])
    with pytest.raises(ValueError, match='does not exist'):
        transform_selected_electricity(bronze, selection=report)
    frame = _transform_electricity(bronze, d45._BUILDINGS, report)
    oracle = d45._expected(spark, 1, item, config, 'source')
    intent = d45._intent('pub1', source, 1)
    with publication._lock(tmp_path, d45._DIRECTORY) as namespace:
        done = publication._publish(spark, namespace, frame, intent, expected=oracle, exclusive=True)
        with pytest.raises(publication.SilverPublicationError, match='already reserved'):
            publication._publish(spark, namespace, frame, intent, expected=oracle, exclusive=True)
    assert not (tmp_path / 'silver-publications').exists()
    assert done['output_rows'] == 16 and done['transform_version'] == d45._PROFILE
    folder = tmp_path / d45._DIRECTORY / 'pub1'
    target = folder / 'table'
    assert [p.name for p in (target / '_delta_log').glob('*.json')] == ['00000000000000000000.json']
    completed, actual = d45.read_publication(spark, campaign, 'pub1')
    assert completed == done and actual.count() == 16

    mutants = (
        ('rows differ', oracle.withColumn('electricity_value_original', F.lit(None).cast('string'))),
        ('rows differ', oracle.where(F.col('building_id') != d45._BUILDINGS[0]).unionByName(
            oracle.where(F.col('building_id') == d45._BUILDINGS[1]))),
        ('rows differ', oracle.withColumn('_bronze_snapshot_run_id', F.lit('changed'))),
        ('schema differs', oracle.withColumn('nominal_second_of_day', F.lit('changed'))),
    )
    for message, changed in mutants:
        with pytest.raises(publication.SilverPublicationError, match=message):
            publication._validate_output(spark, target, changed, intent)

    with pytest.raises(publication.SilverPublicationError, match='Delta history conflicts'):
        publication._validate_output(spark, target, oracle, {**intent, 'transform_version': 'h05.3-v1'})
    intent['profile']['buildings'][0] = 'unapproved'
    (folder / 'intent.json').write_text(json.dumps(intent))
    # Use an independent valid source binding again after simulating on-disk tampering.
    source['buildings'] = list(d45._BUILDINGS)
    with pytest.raises(supervisor.SupervisorError, match='intent differs'):
        d45.read_publication(spark, campaign, 'pub1')
