"""Restricted D45 electrical publication and reconciliation under D48-A."""

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from time import monotonic
from types import MappingProxyType

from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse import scalability_workspace as workspace

_PROFILE = 'd45-electrical-synthetic-v1'
_DIRECTORY = 'd45-silver-publications'
_BUILDINGS = tuple(f'SYNTHETIC_building_{i:02d}' for i in range(16))


@dataclass(frozen=True)
class _Input:
    name: str
    delta_id: str
    rows: int
    commit_sha256: str
    parquet_sha256: str


@dataclass(frozen=True)
class _Snapshot:
    owner: str
    run: str
    bronze_evidence_run: str
    fixture_sha256: str
    report_sha256: str
    completion_sha256: str
    inputs: tuple[_Input, ...]


# Each subsequent authorized dataset requires an explicit attestation. Neither
# the launch parameters nor a user-editable manifest can extend this registry.
_SNAPSHOTS = MappingProxyType({
    'd45-10000-60287d4c6fc6420796f24e03e190fb38': _Snapshot(
        'benchmark-authorized', '3924afed09064067964a679fa8d75842',
        'benchmark-60287d4c6fc6420796f24e03e190fb38',
        '552f4d0b8bab636368ce78a78659fb6b651b7d0da8aa72f3c728dd843a787c1d',
        '45ce3158131e09ad814fd7b7bf5378a25355c1bee515fcb568ace469d4355dd5',
        '096adeabddec5159f271aec2f45a3872fb3e1e513ee13b014e15d5e872f5ab72',
        (
            _Input('electricity', 'db2471c6-ff6c-4271-82cb-0d02cb9eed0f', 625,
                   '0a7e28250cd0772125ef0eb8c6c9a4d7880a58a24a86b760fd1f08e74afdf96f',
                   '7f2b29336c29a6ffbfe1f0be162f39aa6400f25c169f094b59bee8ee8f4d6e4e'),
            _Input('metadata', '5658d9d9-5432-483d-b386-6b905009ed64', 16,
                   'baddbebade796ff82e4b133668ecbcc520655213a9e17c4fdeb3eef57dbddef6',
                   '8361c6bedd3c3204cd040c8fc59c3c77411d5dd8557422ba092e7904916fc291'),
        ),
    ),
    'd45-bronze-calibration-2ee414e7897946eeb3df6d76ac771c91': _Snapshot(
        'calibration-authorized', '75df603267584eb59595760b14d4d795',
        'calibration-2ee414e7897946eeb3df6d76ac771c91',
        '8dfde2759a7ced0bdddbc0a8ac8562266dfedcbf7cd769a0205c30f17cc4390f',
        '7cd1171dc70a8d90b093689c55c4803a32569d9cbd245ee3f04388e54f086080',
        '5c22c9dc0f5e7237842186f7d197930b9d63e30376d845297730f67d7024e02f',
        (
            _Input('electricity', '82dec3fb-4094-4804-9ef2-705598eda405', 8,
                   '203b0909b641ed83d268d332b8b88ed0c631904382ef4046b13471192439cc93',
                   'd9aaabb43eca8adf8a7684659e213897274ec0f95e5abd0ccf7b18c6fd7c2cd2'),
            _Input('metadata', 'ecad24ac-10ee-436b-9643-1c7c37a53044', 16,
                   '1c556475aabaed8bf8cfb33421d43219a21230d7a7b233181d94975122c9a117',
                   'bedb2112ceadadb60768073d94b5be2f373f2fbee12f9c586e7bfb480dab96bb'),
        ),
    ),
})


def _record(path, digest, *, json_lines=False):
    """Read pinned small local records after D39 has validated their tree."""
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), 'rb') as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise supervisor.SupervisorError('Invalid D45 input record')
        data = file.read(512 * 1024 + 1)
    if len(data) > 512 * 1024 or hashlib.sha256(data).hexdigest() != digest:
        raise supervisor.SupervisorError('D45 input record fingerprint differs')
    return [json.loads(line) for line in data.splitlines()] if json_lines else json.loads(data)


def _fingerprint(path):
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    digest = hashlib.sha256()
    with os.fdopen(os.open(path, flags), 'rb') as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise supervisor.SupervisorError('Invalid D45 source file')
        remaining = info.st_size
        while remaining:
            supervisor._check_io()
            block = file.read(min(remaining, 64 * 1024))
            if not block:
                raise supervisor.SupervisorError('D45 source shrank during validation')
            digest.update(block)
            remaining -= len(block)
        if file.read(1):
            raise supervisor.SupervisorError('D45 source grew during validation')
    return digest.hexdigest()


def _bound_inputs(campaign):
    from tfm_lakehouse.bronze_calibration import _config

    if not isinstance(campaign, workspace.Campaign):
        raise supervisor.SupervisorError('D45 Silver requires an authorized Bronze campaign')
    snapshot = _SNAPSHOTS.get(campaign.campaign_id)
    if snapshot is None or campaign.owner_id != snapshot.owner:
        raise supervisor.SupervisorError('Bronze snapshot is not approved for D45 Silver')
    root = workspace.validate_authorized_campaign(campaign)
    config = _config(campaign)
    workspace.validate_synthetic_config(root, config)
    fixture = _record(root / 'raw' / 'calibration-manifest.json', snapshot.fixture_sha256)
    report_path = config.namespace / '_runs' / (snapshot.run + '.json')
    report = _record(report_path, snapshot.report_sha256)
    completed = _record(root / 'evidence' / snapshot.bronze_evidence_run / 'completed.json',
                        snapshot.completion_sha256)
    if (completed['status'] != 'supervised_success' or
            completed['bronze']['status'] != 'passed' or
            completed['bronze']['report_run_id'] != snapshot.run or
            completed['bronze']['manifest_sha256'] != snapshot.fixture_sha256 or
            not completed['watchdog']['collection_complete'] or
            completed['watchdog']['unresolved_processes'] or
            report['status'] != 'passed' or report['run_id'] != snapshot.run or
            report['data_kind'] != 'synthetic'):
        raise supervisor.SupervisorError('D45 Bronze publication evidence differs')
    if tuple(fixture['sources']['electricity.csv']['columns']) != ('timestamp', *_BUILDINGS):
        raise supervisor.SupervisorError('D45 Bronze cohort differs')
    tables = []
    for source in snapshot.inputs:
        item, = (item for item in report['tables'] if item['name'] == source.name)
        path = config.table_path(source.name)
        source_path = config.sources[source.name]
        digest = _fingerprint(source_path)
        if (item['status'] != 'verified' or item['delta_version'] != 0 or
                item['output_rows'] != source.rows or item['table_path'] != str(path) or
                item['source_uri'] != source_path.as_uri() or item['source_sha256'] != digest or
                fixture['sources'][source.name + '.csv']['sha256'] != digest):
            raise supervisor.SupervisorError('D45 Bronze table binding differs')
        log = path / '_delta_log'
        commit = log / '00000000000000000000.json'
        if list(log.glob('*.json')) != [commit]:
            raise supervisor.SupervisorError('D45 Bronze snapshot Delta history differs')
        actions = _record(commit, source.commit_sha256, json_lines=True)
        delta, = (action['metaData'] for action in actions if 'metaData' in action)
        add, = (action['add'] for action in actions if 'add' in action)
        info, = (action['commitInfo'] for action in actions if 'commitInfo' in action)
        data_name = add['path']
        if ('/' in data_name or not data_name.endswith('.parquet') or delta['id'] != source.delta_id or
                json.loads(delta['schemaString']) != item['output_schema'] or
                json.loads(info['userMetadata']) != {
                    'run_id': snapshot.run, 'table_name': source.name, 'source_sha256': digest,
                    'data_kind': 'synthetic', 'dataset_reference': config.dataset_reference,
                } or _fingerprint(path / data_name) != source.parquet_sha256):
            raise supervisor.SupervisorError('D45 Bronze commit or data differs')
        tables.append({key: item[key] for key in ('name', 'table_path', 'delta_version', 'source_sha256')}
                      | {'delta_id': source.delta_id, 'commit_sha256': source.commit_sha256,
                         'parquet_sha256': source.parquet_sha256})
    return config, report, {
        'publication_profile': _PROFILE, 'buildings': list(_BUILDINGS),
        'source_run_id': snapshot.run, 'fixture_sha256': snapshot.fixture_sha256,
        'manifest_sha256': snapshot.report_sha256, 'source_tables': tables,
    }


def binding(campaign, publication_id, *, fresh=False):
    workspace._id(publication_id)
    _, _, source = _bound_inputs(campaign)
    if fresh and os.path.lexists(campaign.root / _DIRECTORY / publication_id):
        raise supervisor.SupervisorError('D45 Silver destination already exists')
    return source


def _intent(publication_id, binding, rows):
    return {
        'publication_id': publication_id, 'source_run_id': binding['source_run_id'],
        'manifest_sha256': binding['manifest_sha256'], 'source_tables': binding['source_tables'],
        'transform_version': _PROFILE, 'input_rows': rows, 'output_rows': rows * len(_BUILDINGS),
        'profile': {
            'identity': _PROFILE, 'buildings': binding['buildings'],
            'fixture_sha256': binding['fixture_sha256'],
        },
    }


def _expected(spark, rows, item, config, run):
    """Use the D45 source formula, independently of the Silver and CSV readers."""
    from pyspark.sql import functions as F

    width = len(_BUILDINGS)
    position = F.col('id')
    hour = F.floor(position / F.lit(width)).cast('int')
    building = F.pmod(position, F.lit(width))
    day = F.date_add(F.lit('2016-01-01').cast('date'), F.floor(hour / 24).cast('int'))
    nominal_hour = F.pmod(hour, F.lit(24))
    return spark.range(rows * width).select(
        F.format_string('SYNTHETIC_building_%02d', building).alias('building_id'),
        F.format_string('%s %02d:00:00', day.cast('string'), nominal_hour).alias('timestamp_original'),
        position.cast('string').alias('electricity_value_original'),
        F.lit(item['source_uri']).alias('_bronze_source_uri'),
        F.lit(item['source_sha256']).alias('_bronze_source_sha256'),
        F.lit('synthetic').alias('_bronze_data_kind'),
        F.lit(config.dataset_reference).alias('_bronze_dataset_reference'),
        F.lit('electricity').alias('_bronze_source_name'),
        F.lit(run).alias('_bronze_snapshot_run_id'),
        F.lit(0).alias('_bronze_delta_version'),
        F.lit(item['table_path']).alias('_bronze_table_path'),
        position.cast('decimal(38,18)').alias('electricity_value_numeric'),
        F.lit('finite').alias('value_parse_status'),
        F.lit(False).alias('is_missing'),
        (position == 0).alias('is_zero'),
        F.lit(False).alias('is_negative'),
        day.alias('nominal_date'),
        (nominal_hour * F.lit(3600)).alias('nominal_second_of_day'),
        F.lit('valid_nominal').alias('timestamp_parse_status'),
    )


def _assert_lineage(frames, items, config):
    from pyspark.sql import functions as F

    for item in items:
        frame = frames[item['name']]
        mismatch = (
            ~F.col('_bronze_source_uri').eqNullSafe(F.lit(item['source_uri'])) |
            ~F.col('_bronze_source_sha256').eqNullSafe(F.lit(item['source_sha256'])) |
            ~F.col('_bronze_data_kind').eqNullSafe(F.lit('synthetic')) |
            ~F.col('_bronze_dataset_reference').eqNullSafe(F.lit(config.dataset_reference))
        )
        if frame.where(mismatch).limit(1).count():
            raise supervisor.SupervisorError('D45 Bronze input lineage differs')


def _verify_grant(campaign, evidence_fd, publication_id, authorized):
    workspace._id(publication_id)
    root = workspace.validate_authorized_campaign(campaign)
    evidence = root / 'evidence' / publication_id
    if not os.path.samestat(os.fstat(evidence_fd), evidence.lstat()):
        raise supervisor.SupervisorError('D45 Silver evidence destination differs')
    launch = supervisor._read_record(evidence_fd, 'launch.json')['ticket']
    grant = supervisor._read_record(evidence_fd, 'child-identity.json')
    config = launch['configuration']
    if (config['handler'] != supervisor._SILVER_HANDLER or config['run_id'] != publication_id or
            config['campaign_id'] != campaign.campaign_id or config['owner_id'] != campaign.owner_id or
            not supervisor._same(config['silver'], authorized) or
            grant != {'supervision': 'D46', 'ticket_sha256': supervisor._digest(launch),
                      'identity': asdict(supervisor.read_process_identity(os.getpid()))}):
        raise supervisor.SupervisorError('D45 Silver D46 grant differs')


def execute(spark, campaign, evidence_fd, publication_id, authorized):
    """Verify the pinned Bronzes, publish once and validate against an independent oracle."""
    from tfm_lakehouse.bronze_calibration import _delta_storage
    from tfm_lakehouse.ingestion.snapshot import _read_verified_tables, _same_configuration, _validate_tables
    from tfm_lakehouse.silver import silver_synthetic_publication as publication
    from tfm_lakehouse.silver.silver_electricity_integrated import _transform_electricity

    started = monotonic()
    _verify_grant(campaign, evidence_fd, publication_id, authorized)
    config, report, source = _bound_inputs(campaign)
    if not supervisor._same(source, authorized):
        raise supervisor.SupervisorError('D45 Silver ticket binding differs')
    run = source['source_run_id']
    report_path = config.namespace / '_runs' / (run + '.json')
    if not _same_configuration(report, config):
        raise supervisor.SupervisorError('D45 Bronze configuration differs')
    _validate_tables(report, config, report_path)
    selection = dict(report, tables=[item for item in report['tables']
                                    if item['name'] in ('electricity', 'metadata')])
    frames = _read_verified_tables(spark, config, selection, report_path)
    _assert_lineage(frames, selection['tables'], config)
    item, = (item for item in selection['tables'] if item['name'] == 'electricity')
    rows = item['output_rows']
    phases = {'bronze_verification_seconds': monotonic() - started}
    started = monotonic()
    frame = _transform_electricity(frames['electricity'], _BUILDINGS, selection)
    expected = _expected(spark, rows, item, config, run)
    phases['transformation_plan_seconds'] = monotonic() - started
    intent = _intent(publication_id, source, rows)

    def confirm():
        if not supervisor._same(binding(campaign, publication_id), authorized):
            raise supervisor.SupervisorError('D45 Bronze source changed during Silver')

    with publication._lock(campaign.root, _DIRECTORY) as namespace:
        result = publication._publish(spark, namespace, frame, intent, expected=expected,
                                      phases=phases, exclusive=True, confirm=confirm)
    evidence = {'status': 'passed', 'binding_sha256': supervisor._digest(authorized),
                'publication': result, 'phase_seconds': phases,
                'delta_storage': _delta_storage(campaign.root / _DIRECTORY / publication_id / 'table')}
    supervisor._write_record(evidence_fd, 'silver-calibration.json', evidence)
    return evidence


def read_publication(spark, campaign, publication_id):
    """Read and independently revalidate a finalized D45-only publication."""
    from tfm_lakehouse.silver import silver_synthetic_publication as publication

    workspace._id(publication_id)
    config, report, source = _bound_inputs(campaign)
    item, = (item for item in report['tables'] if item['name'] == 'electricity')
    rows = item['output_rows']
    intent = _intent(publication_id, source, rows)
    folder = campaign.root / _DIRECTORY / publication_id
    if publication._read_json(folder / 'intent.json') != intent:
        raise supervisor.SupervisorError('D45 Silver intent differs')
    completed = publication._read_json(folder / 'completed.json')
    expected = _expected(spark, rows, item, config, source['source_run_id'])
    if publication._validate_output(spark, folder / 'table', expected, intent) != completed:
        raise supervisor.SupervisorError('D45 Silver completed evidence differs')
    frame = spark.read.format('delta').option('versionAsOf', 0).load(str(folder / 'table'))
    return completed, frame


def verify_result(fd, result):
    from tfm_lakehouse.silver import silver_synthetic_publication as publication

    evidence = supervisor._read_record(fd, 'silver-calibration.json')
    authorization = result['configuration']['silver']
    published = evidence['publication']
    if (evidence['status'] != 'passed' or published['status'] != 'passed' or
            evidence['binding_sha256'] != supervisor._digest(authorization) or
            published['publication_id'] != result['configuration']['run_id'] or
            published['transform_version'] != _PROFILE or published['delta_version'] != 0 or
            published['profile']['identity'] != _PROFILE or
            published['profile']['buildings'] != authorization['buildings'] or
            published['profile']['fixture_sha256'] != authorization['fixture_sha256'] or
            published['source_tables'] != authorization['source_tables'] or
            published['source_run_id'] != authorization['source_run_id'] or
            published['manifest_sha256'] != authorization['manifest_sha256'] or
            result['spark']['events']['status'] != 'complete'):
        raise supervisor.SupervisorError('D45 Silver terminal evidence differs')
    config = result['configuration']
    folder = Path(config['root']) / _DIRECTORY / config['run_id']
    intent = _intent(config['run_id'], authorization, published['input_rows'])
    if (published['table_path'] != str(folder / 'table') or
            publication._read_json(folder / 'intent.json') != intent or
            publication._read_json(folder / 'completed.json') != published):
        raise supervisor.SupervisorError('D45 Silver on-disk publication evidence differs')
    return evidence
