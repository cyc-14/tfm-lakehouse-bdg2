"""Fixed synthetic Bronze calibration inside an authorized D39 campaign."""

import hashlib
import os
import stat
import time

from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse.bronze_calibration_fixture import fixture_layout, fixture_lines, source_fingerprint


_REFERENCE = 'd45-invented-calibration-v1'
_MANIFEST = 'calibration-manifest.json'
_LINEAGE_COLUMNS = ['_bronze_source_uri', '_bronze_source_sha256',
                    '_bronze_data_kind', '_bronze_dataset_reference']


def _manifest(sources, hours, buildings_count):
    if buildings_count == 1600:
        return {
            'contract_version': 2, 'dataset_reference': _REFERENCE,
            'electrical_rows': hours, 'building_columns': buildings_count,
            'electrical_positions': hours * buildings_count, 'metadata_rows': buildings_count,
            'weather_rows': hours * 4, 'study_cohort': [f'SYNTHETIC_building_{i:02d}' for i in range(16)],
            'site_assignment_rule': 'site = (building_index // 4) % 4',
            'sources': sources,
        }
    return {
        'contract_version': 1, 'dataset_reference': _REFERENCE,
        'electrical_rows': hours, 'building_columns': 16, 'electrical_positions': hours * 16,
        'metadata_rows': 16, 'weather_rows': hours * 4,
        'building_site_assignment': {
            f'SYNTHETIC_building_{index:02d}': f'SYNTHETIC_SITE_{index // 4:02d}'
            for index in range(16)},
        'sources': sources,
    }


def _fixture_sources(fd, hours, buildings_count, *, write=False):
    """Stream exact fixture records through campaign-relative descriptors."""
    sources = {}
    for name, (columns, rows) in fixture_layout(hours, buildings_count).items():
        filename = name + '.csv'
        lines = fixture_lines(name, columns, rows, buildings_count)
        flags = os.O_NOFOLLOW | os.O_NONBLOCK
        if write:
            flags |= os.O_WRONLY | os.O_CREAT | os.O_EXCL
        supervisor._check_io()
        with os.fdopen(os.open(filename, flags, 0o600, dir_fd=fd), 'wb' if write else 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise supervisor.SupervisorError('Invalid synthetic calibration source')

            def transfer(line):
                supervisor._check_io()
                if write:
                    if stream.write(line) != len(line):
                        raise OSError('Short synthetic calibration write')
                elif stream.read(len(line)) != line:
                    raise supervisor.SupervisorError('Synthetic calibration source differs')

            fingerprint = source_fingerprint(lines, transfer)
            supervisor._check_io()
            if write:
                stream.flush()
                os.fsync(stream.fileno())
            elif stream.read(1):
                raise supervisor.SupervisorError('Synthetic calibration source differs')
        if buildings_count == 1600:
            fingerprint['header_sha256'] = hashlib.sha256(','.join(columns).encode('utf-8')).hexdigest()
        else:
            fingerprint['columns'] = columns
        sources[filename] = fingerprint
    return sources


def prepare_fixture(campaign, hours: int = 8, buildings_count: int = 16):
    """Create the fixed inputs exclusively; retain any partial preparation."""
    root = workspace.validate_authorized_campaign(campaign)
    started = time.monotonic_ns()
    with supervisor._directory(root / 'raw') as fd:
        sources = _fixture_sources(fd, hours, buildings_count, write=True)
        supervisor._write_record(fd, _MANIFEST, _manifest(sources, hours, buildings_count))
    binding = calibration_binding(campaign)
    return {'manifest': binding, 'preparation_seconds': (time.monotonic_ns() - started) / 1e9}


def calibration_binding(campaign):
    """Revalidate the whole campaign before opening any fixed synthetic source."""
    root = workspace.validate_authorized_campaign(campaign)
    config = _config(campaign)
    workspace.validate_synthetic_config(root, config)
    with supervisor._directory(root / 'raw') as fd:
        manifest = supervisor._read_record(fd, _MANIFEST)
        hours = manifest.get('electrical_rows')
        buildings_count = manifest.get('building_columns')
        try:
            sources = _fixture_sources(fd, hours, buildings_count)
        except ValueError as exc:
            raise supervisor.SupervisorError('Unsupported fixture size') from exc
    if not supervisor._same(manifest, _manifest(sources, hours, buildings_count)):
        raise supervisor.SupervisorError('Synthetic calibration manifest differs')
    return {'manifest': manifest, 'manifest_sha256': supervisor._digest(manifest),
            'raw_root': str(config.raw_root), 'lakehouse_root': str(config.lakehouse_root)}


def _config(campaign):
    from tfm_lakehouse.config import BronzeConfig

    root = campaign.root
    return BronzeConfig(raw_root=root / 'raw', lakehouse_root=root / 'lakehouse',
                        electricity=root / 'raw' / 'electricity.csv', metadata=root / 'raw' / 'metadata.csv',
                        weather=root / 'raw' / 'weather.csv', data_kind='synthetic', dataset_reference=_REFERENCE)


def _expected_rows(spark, name, row_count, buildings_count, lineage):
    """Generate expected rows in Spark from ordinals, independently of CSV ingestion."""
    from pyspark.sql import functions as F

    index = F.col('id')
    hour = F.shiftright(index, 2) if name == 'weather' else index
    site = F.pmod(F.shiftright(index, 2) if name == 'metadata' else index, F.lit(4))
    site_id = F.format_string('SYNTHETIC_SITE_%02d', site)
    # Build nominal time labels using dates and hour numbers, with no timezone conversion.
    day = F.date_add(F.lit('2016-01-01').cast('date'), F.floor(hour / 24).cast('int'))
    timestamp = F.format_string('%s %02d:00:00', day.cast('string'), F.pmod(hour, F.lit(24)))
    if name == 'electricity':
        buildings = F.sequence(F.lit(0), F.lit(buildings_count - 1))
        values = F.concat(F.array(timestamp), F.transform(
            buildings, lambda building: (hour * buildings_count + building).cast('string')))
    elif name == 'metadata':
        values = F.array(F.format_string('SYNTHETIC_building_%02d', index), site_id,
                         F.lit('Lodging/residential'), F.lit('Synthetic'),
                         (index + 100).cast('string'), F.lit('Europe/London'))
    else:
        values = F.array(timestamp, site_id, (hour + site).cast('string'), F.lit('0'))
    return spark.range(row_count).select(
        F.concat(values, F.array(*(F.lit(value) for value in lineage))).alias('row'))


def _same_rows(actual, expected):
    """Compare full-row multiset multiplicities by exact signed counts."""
    from pyspark.sql import functions as F

    # One array key preserves positions (including nulls) without 1,605 group keys.
    signed = actual.withColumn('difference', F.lit(1)).union(
        expected.withColumn('difference', F.lit(-1)))
    differences = (signed.groupBy('row').agg(F.sum('difference').alias('difference'))
                   .where(F.col('difference') != 0))
    return differences.isEmpty()


def _reconcile_table(frame, item, columns, row_count, source, source_uri, buildings_count):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    lineage = [source_uri, source['sha256'], 'synthetic', _REFERENCE]
    if (frame.columns != columns + _LINEAGE_COLUMNS or
            any(not isinstance(field.dataType, StringType) for field in frame.schema) or
            type(item['delta_version']) is not int or item['delta_version'] != 0 or
            item['source_sha256'] != source['sha256'] or item['source_bytes'] != source['bytes'] or
            item['input_rows'] != row_count or item['output_rows'] != row_count):
        raise supervisor.SupervisorError('Bronze content, schema or lineage differs')
    expected = _expected_rows(frame.sparkSession, item['name'], row_count, buildings_count, lineage)
    actual = frame.select(F.array(*(F.col(column) for column in frame.columns)).alias('row'))
    if not _same_rows(actual, expected):
        raise supervisor.SupervisorError('Bronze content, schema or lineage differs')
    result = {'rows': row_count, 'delta_version': item['delta_version'], 'source_sha256': source['sha256']}
    if len(frame.columns) > 100:
        result.update(column_count=len(frame.columns),
                      columns_sha256=supervisor._digest(frame.columns))
    else:
        result['columns'] = frame.columns
    return result


def _delta_storage(root):
    """Count physical blocks and files without following links within the campaign."""
    from tfm_lakehouse import resource_linux

    with resource_linux._absolute_directory(root) as fd:
        blocks, files = resource_linux._tree_usage(fd, resource_linux._mount_id(fd))
    return {'allocated_bytes': blocks * 512, 'file_count': files}


def execute(spark, campaign, evidence_fd, binding):
    """Run Bronze once and reconcile its published, version-bound snapshots."""
    from tfm_lakehouse.ingestion.bronze import run_bronze
    from tfm_lakehouse.ingestion.snapshot import read_bronze_snapshot

    config = _config(campaign)
    started = time.monotonic_ns()
    if calibration_binding(campaign) != binding:
        raise supervisor.SupervisorError('Calibration binding changed')
    if os.path.lexists(config.namespace):
        raise supervisor.SupervisorError('Bronze calibration destination already exists')
    validation_seconds = (time.monotonic_ns() - started) / 1e9
    started = time.monotonic_ns()
    phase_seconds = {}
    report = run_bronze(spark, config, phase_seconds=phase_seconds)
    ingest_seconds = (time.monotonic_ns() - started) / 1e9
    if report['status'] != 'passed':
        raise supervisor.SupervisorError('Bronze did not pass')
    started = time.monotonic_ns()
    selection, frames = read_bronze_snapshot(spark, config)
    if (selection['run_id'] != report['run_id'] or selection['using_previous_successful_run'] or
            selection['report_path'] != report['report_path'] or
            not supervisor._same(selection['tables'], report['tables'])):
        raise supervisor.SupervisorError('Published Bronze snapshot differs')
    manifest = binding['manifest']
    buildings_count = manifest['building_columns']
    sources = manifest['sources']
    layout = fixture_layout(manifest['electrical_rows'], buildings_count)
    tables = {}
    for item in selection['tables']:
        name = item['name']
        columns, row_count = layout[name]
        tables[name] = _reconcile_table(
            frames[name], item, columns, row_count, sources[name + '.csv'],
            config.sources[name].as_uri(), buildings_count)
    if calibration_binding(campaign) != binding:
        raise supervisor.SupervisorError('Calibration source changed')
    evidence = {'status': 'passed', 'manifest_sha256': binding['manifest_sha256'],
                'report_run_id': report['run_id'], 'report_path': report['report_path'], 'tables': tables,
                'source_bytes': sum(source['bytes'] for source in sources.values()),
                'delta_storage': _delta_storage(config.namespace / 'bronze'),
                'ingestion_phase_seconds': phase_seconds,
                'source_validation_seconds': validation_seconds,
                'bronze_ingestion_and_writes_seconds': ingest_seconds,
                'snapshot_and_reconciliation_seconds': (time.monotonic_ns() - started) / 1e9}
    supervisor._write_record(evidence_fd, 'bronze-calibration.json', evidence)
    return evidence


def verify_result(fd, result):
    """Require an evidence record linked to this run's preauthorized manifest."""
    evidence = supervisor._read_record(fd, 'bronze-calibration.json')
    if (evidence.get('status') != 'passed' or
            evidence.get('manifest_sha256') != result['configuration']['bronze']['manifest_sha256'] or
            set(evidence.get('tables', {})) != {'electricity', 'metadata', 'weather'}):
        raise supervisor.SupervisorError('Bronze calibration evidence differs')
    return evidence
