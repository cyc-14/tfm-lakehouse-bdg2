"""Synthetic boundary and fixture tests without Spark."""

import csv
import hashlib
import io
import os
import tracemalloc
from itertools import islice
from types import SimpleNamespace

import pytest

from tfm_lakehouse import bronze_calibration as calibration
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse.bronze_calibration_fixture import fixture_layout, fixture_lines, fixture_manifest


def _fixture_csv(hours=8, buildings_count=16):
    """Materialize only historical inputs for independent test oracles."""
    assert (hours, buildings_count) != (17544, 1600)
    return {
        name + '.csv': b''.join(fixture_lines(name, columns, rows, buildings_count))
        for name, (columns, rows) in fixture_layout(hours, buildings_count).items()
    }


def test_fixture_rows_sites_and_fingerprints():
    sources = _fixture_csv()
    rows = {name: list(csv.reader(io.StringIO(data.decode()))) for name, data in sources.items()}
    assert len(rows['electricity.csv']) == 9
    assert all(len(row) == 17 for row in rows['electricity.csv'])
    assert [int(cell) for row in rows['electricity.csv'][1:] for cell in row[1:]] == list(range(128))
    assert len(rows['metadata.csv']) == 17 and len(rows['weather.csv']) == 33
    assert {row[1] for row in rows['metadata.csv'][1:]} == {row[1] for row in rows['weather.csv'][1:]}
    assert fixture_manifest()['electricity.csv']['sha256'] == (
        'cfa6f8da0ab4177c194bdc1e489f7f822f69d76d2962cff9357a0303b50ae3f7')


def test_synthetic_binding_and_tamper_rejection(monkeypatch, tmp_path):
    root = tmp_path / 'scalability'
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', root)
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: None)
    monkeypatch.setattr(workspace, '_check_disk_backed', lambda _: None)
    monkeypatch.setattr(workspace, '_AUTHORIZED', {})
    campaign = workspace.reserve_campaign('campaign', 'owner')
    prepared = calibration.prepare_fixture(campaign)
    assert prepared['manifest'] == calibration.calibration_binding(campaign)
    with pytest.raises(FileExistsError):
        calibration.prepare_fixture(campaign)
    source = campaign.root / 'raw' / 'electricity.csv'
    source.write_bytes(b'invented but changed')
    with pytest.raises(supervisor.SupervisorError, match='source differs'):
        calibration.calibration_binding(campaign)


def test_bronze_handler_is_supervised_only():
    with pytest.raises(supervisor.SupervisorError, match='requires supervised'):
        supervisor.launch_workload(None, None, 'run', handler=supervisor._BRONZE_HANDLER,
                                   parameters=supervisor.BronzeCalibrationParameters())


def test_foreign_campaign_rejected_before_source_open(monkeypatch, tmp_path):
    campaign = workspace.Campaign(tmp_path / 'foreign', 'foreign', 'owner')
    monkeypatch.setattr(workspace, 'validate_authorized_campaign',
                        lambda _: (_ for _ in ()).throw(workspace.WorkspaceError('unauthorized')))
    monkeypatch.setattr('pathlib.Path.read_bytes', lambda _: pytest.fail('source inspected'))
    with pytest.raises(workspace.WorkspaceError, match='unauthorized'):
        calibration.calibration_binding(campaign)


@pytest.mark.parametrize('mutation', ['schema', 'order', 'type', 'version', 'hash', 'bytes', 'count'])
def test_exact_reconciliation_rejects_inconsistent_contract(mutation):
    from pyspark.sql.types import IntegerType, StringType

    source = fixture_manifest()['electricity.csv']
    columns, row_count = fixture_layout(8, 16)['electricity']
    item = {'name': 'electricity', 'delta_version': 0, 'source_sha256': source['sha256'],
            'source_bytes': source['bytes'], 'input_rows': row_count, 'output_rows': row_count}
    names = columns + calibration._LINEAGE_COLUMNS
    types = [StringType() for _ in names]
    if mutation == 'schema':
        names[0] = 'wrong'
    elif mutation == 'order':
        names[1], names[2] = names[2], names[1]
    elif mutation == 'type':
        types[0] = IntegerType()
    elif mutation == 'version':
        item['delta_version'] = 1
    elif mutation == 'hash':
        item['source_sha256'] = 'wrong'
    elif mutation == 'bytes':
        item['source_bytes'] += 1
    else:
        item['output_rows'] -= 1
    frame = SimpleNamespace(columns=names, schema=[SimpleNamespace(dataType=t) for t in types])
    with pytest.raises(supervisor.SupervisorError, match='content, schema or lineage'):
        calibration._reconcile_table(frame, item, columns, row_count, source, 'file:///invented', 16)


def test_execute_binds_report_versions_and_full_rows(monkeypatch, tmp_path):
    from tfm_lakehouse.ingestion import bronze, snapshot

    root = tmp_path / 'scalability'
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', root)
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: None)
    monkeypatch.setattr(workspace, '_check_disk_backed', lambda _: None)
    monkeypatch.setattr(workspace, '_AUTHORIZED', {})
    campaign = workspace.reserve_campaign('campaign', 'owner')
    binding = calibration.prepare_fixture(campaign)['manifest']
    evidence = supervisor.reserve_evidence(campaign, 'run')
    config = calibration._config(campaign)
    tables = []
    frames = {name: object() for name in config.sources}
    for name, (_, rows) in fixture_layout(8, 16).items():
        source = binding['manifest']['sources'][name + '.csv']
        tables.append({'name': name, 'delta_version': 0, 'source_sha256': source['sha256'],
                       'source_bytes': source['bytes'], 'input_rows': rows, 'output_rows': rows})

    def reconcile(frame, item, columns, rows, source, source_uri, buildings_count):
        name = item['name']
        assert frame is frames[name]
        assert (columns, rows) == fixture_layout(8, 16)[name]
        assert source == binding['manifest']['sources'][name + '.csv']
        assert source_uri == config.sources[name].as_uri()
        assert buildings_count == 16
        return {'rows': rows, 'delta_version': item['delta_version'], 'source_sha256': source['sha256']}

    monkeypatch.setattr(calibration, '_reconcile_table', reconcile)
    report = {'status': 'passed', 'run_id': 'invented', 'report_path': 'invented', 'tables': tables}
    def run(spark, config, *, phase_seconds):
        phase_seconds['source_inspection'] = .1
        (config.namespace / 'bronze').mkdir(parents=True)
        return report
    monkeypatch.setattr(bronze, 'run_bronze', run)
    selection = {'run_id': 'invented', 'using_previous_successful_run': False,
                 'report_path': 'invented', 'tables': [dict(item) for item in tables]}
    monkeypatch.setattr(snapshot, 'read_bronze_snapshot', lambda *args: (selection, frames))
    with supervisor._directory(evidence) as fd:
        result = calibration.execute(None, campaign, fd, binding)
    assert result['tables']['electricity']['rows'] == 8
    assert result['tables']['metadata']['rows'] == 16
    assert result['tables']['weather']['rows'] == 32
    assert result['ingestion_phase_seconds'] == {'source_inspection': .1}


def test_registered_handler_ticket_and_existing_output_rejection(monkeypatch, tmp_path):
    from tfm_lakehouse import resource_spark

    root = tmp_path / 'scalability'
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', root)
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: None)
    monkeypatch.setattr(workspace, '_check_disk_backed', lambda _: None)
    monkeypatch.setattr(workspace, '_AUTHORIZED', {})
    campaign = workspace.reserve_campaign('campaign', 'owner')
    calibration.prepare_fixture(campaign)
    monkeypatch.setattr(resource_spark, 'spark_profile', lambda root, run: {'fixed': str(root), 'run': run})
    base = campaign.root / 'temp' / 'pytest-new'
    config = supervisor._configuration(campaign, base, 'run', supervisor._BRONZE_HANDLER,
                                       supervisor.BronzeCalibrationParameters())
    assert config['bronze'] == calibration.calibration_binding(campaign)
    assert config['spark'] == {'fixed': str(campaign.root), 'run': 'run'}
    assert config['protected_roots'] and not os.path.lexists(base)
    (campaign.root / 'lakehouse' / 'synthetic').mkdir()
    with pytest.raises(supervisor.SupervisorError, match='destination already exists'):
        supervisor._configuration(campaign, base, 'run', supervisor._BRONZE_HANDLER,
                                  supervisor.BronzeCalibrationParameters())


def test_fixed_10000_position_fixture():
    sources = _fixture_csv(625)
    rows = {name: list(csv.reader(io.StringIO(data.decode()))) for name, data in sources.items()}
    assert len(rows['electricity.csv']) == 626
    assert all(len(row) == 17 for row in rows['electricity.csv'])
    assert [int(cell) for row in rows['electricity.csv'][1:] for cell in row[1:]] == list(range(10000))
    assert len(rows['metadata.csv']) == 17 and len(rows['weather.csv']) == 2501
    assert all(len(row) == 4 for row in rows['weather.csv'])
    with pytest.raises(ValueError):
        _fixture_csv(626)


def test_fixed_100000_position_fixture():
    sources = _fixture_csv(6250)
    rows = {name: list(csv.reader(io.StringIO(data.decode()))) for name, data in sources.items()}
    assert len(rows['electricity.csv']) == 6251
    assert all(len(row) == 17 for row in rows['electricity.csv'])
    assert [int(cell) for row in rows['electricity.csv'][1:] for cell in row[1:]] == list(range(100000))
    assert len(rows['metadata.csv']) == 17 and len(rows['weather.csv']) == 25001
    assert {row[1] for row in rows['metadata.csv'][1:]} == {row[1] for row in rows['weather.csv'][1:]}


def test_nominal_2016_2017_grid_and_cardinality():
    sources = _fixture_csv(17544)
    electricity = list(csv.reader(io.StringIO(sources['electricity.csv'].decode())))
    metadata = list(csv.reader(io.StringIO(sources['metadata.csv'].decode())))
    weather = list(csv.reader(io.StringIO(sources['weather.csv'].decode())))
    assert len(electricity) == 17545 and all(len(row) == 17 for row in electricity)
    assert electricity[1][0] == '2016-01-01 00:00:00'
    assert electricity[-1][0] == '2017-12-31 23:00:00'
    assert electricity[1][1] == '0' and electricity[-1][-1] == '280703'
    assert len(metadata) == 17 and len(weather) == 70177
    assert len({(row[0], row[1]) for row in weather[1:]}) == 70176
    assert {row[1] for row in metadata[1:]} == {row[1] for row in weather[1:]}


def test_wide_fixture_binding_and_cohort(monkeypatch, tmp_path):
    from tfm_lakehouse import resource_spark

    root = tmp_path / 'scalability'
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', root)
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: None)
    monkeypatch.setattr(workspace, '_check_disk_backed', lambda _: None)
    monkeypatch.setattr(workspace, '_AUTHORIZED', {})
    campaign = workspace.reserve_campaign('campaign', 'owner')
    prepared = calibration.prepare_fixture(campaign, hours=8, buildings_count=1600)
    manifest = prepared['manifest']['manifest']
    assert manifest['contract_version'] == 2 and manifest['electrical_positions'] == 12800
    assert manifest['building_columns'] == manifest['metadata_rows'] == 1600
    assert manifest['weather_rows'] == 32 and len(manifest['study_cohort']) == 16
    sources = _fixture_csv(8, 1600)
    electricity = list(csv.reader(io.StringIO(sources['electricity.csv'].decode())))
    metadata = list(csv.reader(io.StringIO(sources['metadata.csv'].decode())))
    weather = list(csv.reader(io.StringIO(sources['weather.csv'].decode())))
    assert len(electricity) == 9 and all(len(row) == 1601 for row in electricity)
    assert electricity[0][1:17] == manifest['study_cohort']
    assert electricity[0][-1] == 'SYNTHETIC_building_1599'
    assert [int(value) for row in electricity[1:] for value in row[1:]] == list(range(12800))
    assert len(metadata) == 1601 and len(weather) == 33
    assert [row[1] for row in metadata[1:17]] == [f'SYNTHETIC_SITE_{i // 4:02d}' for i in range(16)]
    assert manifest['sources']['electricity.csv']['header_sha256'] == hashlib.sha256(
        sources['electricity.csv'].split(b'\n', 1)[0]).hexdigest()
    monkeypatch.setattr(resource_spark, 'spark_profile', lambda root, run: {'fixed': str(root), 'run': run})
    config = supervisor._configuration(campaign, campaign.root / 'temp' / 'pytest-new', 'run',
                                       supervisor._BRONZE_HANDLER, supervisor.BronzeCalibrationParameters())
    assert config['bronze'] == prepared['manifest']
    assert len(supervisor._encode(config)) < 8192


@pytest.mark.parametrize(('hours', 'width', 'electricity', 'metadata', 'weather'), [
    (8, 16, 'cfa6f8da0ab4177c194bdc1e489f7f822f69d76d2962cff9357a0303b50ae3f7',
     '60134d39b3f354409ad327f0600f0317bb4e2f95e61694ec9ed83215937516d4',
     'cf38f2c9c3c4be6d8e00c11494459853f9efb1f4f41cad5e26b7ec052785bcd3'),
    (625, 16, '5ff6ae411345beccad2c2ae6a86cf5f98ebb76488aecfe84791e7f3121914b70',
     '60134d39b3f354409ad327f0600f0317bb4e2f95e61694ec9ed83215937516d4',
     '4782174d5573a6dfab3546fe196386dbdba4dcdcf94e27ba2cc10243f2348b4e'),
    (6250, 16, 'f55258c4001b903d0c1f89e30edf0df25205b47c75564b29a3607f0aa0055117',
     '60134d39b3f354409ad327f0600f0317bb4e2f95e61694ec9ed83215937516d4',
     '3e0140aa322a2dda7efea34f2309cae84e45002b9867838c024a5e824d3ac142'),
    (17544, 16, '6de2c8bc637ec8ef8e2c558ebedef52d9862f56e8a624e759c012944566813cb',
     '60134d39b3f354409ad327f0600f0317bb4e2f95e61694ec9ed83215937516d4',
     'feae66eba39f873197b9e590f5b01f49fe443266531a1ab1de69bd9a628e90f3'),
    (8, 1600, '9a18d834e3b29f26a2db2743c555a214c60a49bd97c2a6ec0e00cdf9cec65a83',
     '76d6b251f8ccbc47e29804ac6894c0e3185429e499f9475ff1580a0bcde922f1',
     'cf38f2c9c3c4be6d8e00c11494459853f9efb1f4f41cad5e26b7ec052785bcd3'),
], ids=['calibration', '10k', '100k', 'nominal', 'width'])
def test_historical_fixture_fingerprints(hours, width, electricity, metadata, weather):
    evidence = fixture_manifest(hours, width)
    assert {name: item['sha256'] for name, item in evidence.items()} == {
        'electricity.csv': electricity, 'metadata.csv': metadata, 'weather.csv': weather,
    }


def test_combined_layout_and_prefix_fingerprint_use_bounded_memory():
    layout = fixture_layout(17544, 1600)
    columns, rows = layout['electricity']
    assert len(columns) == 1601 and rows * (len(columns) - 1) == 28070400
    assert layout['metadata'][1] == 1600 and layout['weather'][1] == 70176
    lines = fixture_lines('electricity', columns, rows, 1600)
    assert next(lines).startswith(b'timestamp,SYNTHETIC_building_00,')
    assert next(lines).endswith(b',1599\n')

    tracemalloc.start()
    try:
        from tfm_lakehouse.bronze_calibration_fixture import source_fingerprint

        evidence = source_fingerprint(islice(lines, 256))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert evidence['bytes'] > 2 * 1024**2
    assert peak < 512 * 1024, peak


@pytest.mark.integration
def test_distributed_wide_reconciliation_with_spark(monkeypatch):
    from collections import Counter
    from pyspark.sql import DataFrame, functions as F
    from tfm_lakehouse import resource_spark
    from tfm_lakehouse.spark import spark_session

    campaign_id = os.environ.get('H06_LAUNCH_CAMPAIGN')
    if not campaign_id:
        pytest.skip('Requires an exclusive offline D39 test campaign')
    assert 'TFM_REAL_CONFIG' not in os.environ
    campaign = workspace.authorize_campaign(campaign_id, 'launcher-test')
    manifest = calibration.prepare_fixture(campaign, buildings_count=1600)['manifest']['manifest']
    config = calibration._config(campaign)

    def forbidden(*_args, **_kwargs):
        pytest.fail('The verifier materialized rows on the Python driver')

    for method in ('collect', 'toPandas', 'toLocalIterator'):
        monkeypatch.setattr(DataFrame, method, forbidden)
    context, gateway, _, command = resource_spark._load_spark()
    owned = []
    primary = None
    monkeypatch.setenv('SPARK_LOCAL_IP', '127.0.0.1')
    try:
        with resource_spark._owned_gateway(gateway, owned, command):
            with spark_session('d45-distributed-reconciliation-test', campaign=campaign) as spark:
                assert context._gateway.proc is owned[0]
                for name, (columns, rows) in fixture_layout(8, 1600).items():
                    source = manifest['sources'][name + '.csv']
                    uri = config.sources[name].as_uri()
                    frame = spark.read.csv(uri, header=True)
                    for column, value in zip(calibration._LINEAGE_COLUMNS,
                                             [uri, source['sha256'], 'synthetic', calibration._REFERENCE]):
                        frame = frame.withColumn(column, F.lit(value))
                    item = {'name': name, 'delta_version': 0, 'source_sha256': source['sha256'],
                            'source_bytes': source['bytes'], 'input_rows': rows, 'output_rows': rows}
                    args = (item, columns, rows, source, uri, 1600)
                    result = calibration._reconcile_table(frame, *args)
                    assert result['rows'] == rows
                    if name == 'electricity':
                        assert result['column_count'] == 1605
                        changed = frame.withColumn(columns[-1], F.lit('0'))
                        with pytest.raises(supervisor.SupervisorError, match='content, schema or lineage'):
                            calibration._reconcile_table(changed, *args)
                    if name == 'weather':
                        for column in calibration._LINEAGE_COLUMNS:
                            changed = frame.withColumn(column, F.lit('incorrect'))
                            with pytest.raises(supervisor.SupervisorError, match='content, schema or lineage'):
                                calibration._reconcile_table(changed, *args)

                left = [('a', 'x'), ('b', 'y'), ('a', 'x'), ('c', None)]
                comparisons = {
                    'order': left[::-1],
                    'multiplicity': [('a', 'x'), ('b', 'y'), ('b', 'y'), ('c', None)],
                    'correlation': [('a', 'y'), ('b', 'x'), ('a', 'x'), ('c', None)],
                    'null': [('a', 'x'), ('b', 'y'), ('a', 'x'), ('c', '')],
                }
                assert [Counter(col) for col in zip(*left)] == [
                    Counter(col) for col in zip(*comparisons['correlation'])]

                actual = spark.createDataFrame([(list(row),) for row in left], 'row array<string>')
                for case, values in comparisons.items():
                    expected = spark.createDataFrame([(list(row),) for row in values], 'row array<string>')
                    assert calibration._same_rows(actual, expected) == (Counter(left) == Counter(values)), case
    except BaseException as exc:
        primary = exc
        raise
    finally:
        if owned:
            failure = resource_spark._close_gateway(owned[0], primary)
            if primary is None and failure is not None:
                raise failure
    assert owned[0].returncode == 0
