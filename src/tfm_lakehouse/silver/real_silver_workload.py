"""Run and verify the restricted real electrical Silver publication."""

import csv
from dataclasses import asdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re
import stat
import sys
from time import monotonic
from urllib.parse import unquote, urlsplit

from tfm_lakehouse import resource_linux as linux
from tfm_lakehouse import resource_supervisor as supervisor
from tfm_lakehouse.silver import real_silver_boundary as boundary
from tfm_lakehouse.silver import real_silver_preflight as preflight

_REFERENCE = 'BDG2 v1.0; https://doi.org/10.5281/zenodo.3887306'
_STANDIN_REFERENCE = 'Invented electrical Silver stand-in'
_START = datetime(2016, 1, 1)
_TIMESTAMP = '%Y-%m-%d %H:%M:%S'
_LABEL = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}')
_NUMBER = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?')
_MAXIMUM = Decimal('99999999999999999999.999999999999999999')
_MINIMUM = Decimal('1e-18')
_LINEAGE = ('_bronze_source_uri', '_bronze_source_sha256', '_bronze_data_kind',
            '_bronze_dataset_reference')
_SCHEMA = (
    ('building_id', 'string'), ('timestamp_original', 'string'), ('electricity_value_original', 'string'),
    *((name, 'string') for name in (*_LINEAGE, '_bronze_source_name', '_bronze_snapshot_run_id')),
    ('_bronze_delta_version', 'int'), ('_bronze_table_path', 'string'),
    ('electricity_value_numeric', 'decimal(38,18)'), ('value_parse_status', 'string'),
    ('is_missing', 'boolean'), ('is_zero', 'boolean'), ('is_negative', 'boolean'),
    ('nominal_date', 'date'), ('nominal_second_of_day', 'int'), ('timestamp_parse_status', 'string'),
)
_COLUMNS = tuple(name for name, _ in _SCHEMA)
_REQUIRE = boundary._require


def _numeric(value):
    """Independent Decimal oracle, without rounding or precision-context arithmetic."""
    if value is None:
        return None, 'missing_null', True, None, None
    if value in ('', 'NaN'):
        return None, 'missing_token', True, None, None
    if value in ('Infinity', '-Infinity', '+Infinity'):
        return None, 'nonfinite_literal', False, None, None
    if not _NUMBER.fullmatch(value):
        return None, 'invalid_syntax', False, None, None
    try:
        number = Decimal(value)
    except InvalidOperation:
        significand, _, exponent = value.lower().partition('e')
        if not significand.strip('+-0.'):
            return '0', 'finite', False, True, False
        status = 'underflow' if exponent.startswith('-') else 'overflow'
        return None, status, False, None, None
    if number.is_zero():
        return '0', 'finite', False, True, False
    magnitude = number.copy_abs()
    if magnitude > _MAXIMUM:
        return None, 'overflow', False, None, None
    if magnitude < _MINIMUM:
        return None, 'underflow', False, None, None
    _, digits, exponent = number.as_tuple()
    last = len(digits)
    while digits[last - 1] == 0:
        last -= 1
    if exponent + len(digits) - last < -18:
        return None, 'scale_exceeded', False, None, None
    return str(number), 'finite', False, False, number.is_signed()


def _temporal(value):
    if value is None:
        return None, None, 'missing_null'
    if not value:
        return None, None, 'missing_empty'
    if not _LABEL.fullmatch(value):
        return None, None, 'invalid_format'
    try:
        date.fromisoformat(value[:10])
    except ValueError:
        return None, None, 'invalid_calendar'
    try:
        instant = datetime.strptime(value, _TIMESTAMP)
    except ValueError:
        return None, None, 'invalid_time'
    return instant.date().isoformat(), (instant - datetime.combine(instant.date(), datetime.min.time())).seconds, 'valid_nominal'


def _pinned_text(base, pins, relative, mount):
    pin = pins[relative]
    path = base / relative
    with boundary.checked_directory(path.parent, mount) as fd:
        source = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    stream = os.fdopen(source, 'r', encoding='utf-8-sig', newline='')
    info = os.fstat(stream.fileno())
    try:
        _REQUIRE(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
                 and info.st_size == pin.size_bytes and linux._mount_id(source) == mount,
                 'Pinned electrical Silver file identity changed')
    except BaseException:
        stream.close()
        raise
    return stream


def _table_pins(base, pins, request, mode, mount):
    """Interpret only the explicitly pinned Bronze report and electricity/metadata commits."""
    kind, reference = ('real', _REFERENCE) if mode == 'real' else ('synthetic', _STANDIN_REFERENCE)
    report_path = preflight._BRONZE / '_runs' / (request.bronze_run_id + '.json')
    with _pinned_text(base, pins, report_path, mount) as stream:
        report = json.load(stream)
    _REQUIRE(report.get('status') == 'passed' and report.get('run_id') == request.bronze_run_id
             and report.get('report_contract_version') == 1 and report.get('data_kind') == kind
             and report.get('dataset_reference') == reference, 'Pinned Bronze report identity differs')
    items = {item['name']: item for item in report['tables']}
    _REQUIRE(len(report['tables']) == 3 and set(items) == set(preflight._SOURCE),
             'Pinned Bronze report table list differs')
    metadata_by_name = {}
    for table in request.tables[:2]:
        name = table.name
        item = items[name]
        root = preflight._BRONZE / 'bronze' / name
        source = preflight._SOURCE[name]
        lineage = ((base / source).as_uri(), pins[source].sha256, kind, reference)
        _REQUIRE(item.get('status') == 'verified' and item.get('source_uri') == lineage[0]
                 and item.get('source_sha256') == lineage[1] and item.get('source_bytes') == pins[source].size_bytes
                 and item.get('table_path') == str(base / root) and item.get('delta_version') == table.version
                 and type(item.get('output_rows')) is int and item['output_rows'] > 0,
                 'Pinned Bronze table report differs')
        active, delta_schema, table_id, commit = set(), None, None, None
        for version in range(table.version + 1):
            log = root / '_delta_log' / f'{version:020}.json'
            with _pinned_text(base, pins, log, mount) as stream:
                for line in stream:
                    entry = json.loads(line)
                    if 'add' in entry:
                        action = entry['add']
                        _REQUIRE(not action.get('deletionVector') and not action.get('partitionValues'),
                                 'Pinned Bronze file has an unsupported row selection')
                        active.add(action['path'])
                    if 'remove' in entry:
                        active.discard(entry['remove']['path'])
                    if 'metaData' in entry:
                        metadata = entry['metaData']
                        _REQUIRE(not metadata.get('partitionColumns') and
                                 metadata.get('configuration', {}).get('delta.columnMapping.mode', 'none') == 'none',
                                 'Pinned Bronze has unsupported physical field mapping')
                        table_id, delta_schema = metadata['id'], json.loads(metadata['schemaString'])
                    if version == table.version and 'commitInfo' in entry:
                        _REQUIRE(commit is None, 'Pinned Bronze version has multiple commit identities')
                        commit = json.loads(entry['commitInfo']['userMetadata'])
        _REQUIRE(table_id == table.uuid and delta_schema == item['output_schema'] and
                 active == {table.active_file} and commit == {
                     'run_id': request.bronze_run_id, 'table_name': name, 'source_sha256': lineage[1],
                     'data_kind': kind, 'dataset_reference': reference,
                 }, 'Pinned Bronze Delta version, active file or commit differs')
        metadata_by_name[name] = item, base / root / table.active_file, lineage
    return pins[report_path].sha256, metadata_by_name


def _write_expected(source, out, buildings, lineage, run, version, table_path, *, real):
    """Write one independent witness record at a time, keeping one CSV row in memory."""
    reader = csv.reader(source, strict=True)
    header = next(reader)
    _REQUIRE(len(header) == len(set(header)) and set(('timestamp', *buildings)).issubset(header),
             'Electrical CSV header differs from the approved cohort')
    ti = header.index('timestamp')
    indices = tuple(header.index(building) for building in buildings)
    rows = missing = zeros = 0
    for row in reader:
        if not row:
            continue
        _REQUIRE(len(row) == len(header), 'Electrical CSV row width differs')
        label = row[ti] or None
        if real:
            _REQUIRE(label == (_START + timedelta(hours=rows)).strftime(_TIMESTAMP),
                     'Audited nominal electrical grid differs')
        temporal = _temporal(label)
        for building, index in zip(buildings, indices, strict=True):
            # The audited CSV-to-Bronze equality uses empty cells as null.
            original = row[index] or None
            interpreted = _numeric(original)
            missing += original is None or original == 'NaN'
            zeros += interpreted[3] is True
            values = (building, label, original, *lineage, 'electricity', run, version, table_path,
                      *interpreted, *temporal)
            out.write(json.dumps(dict(zip(_COLUMNS, values, strict=True)), separators=(',', ':')) + '\n')
        rows += 1
    if real:
        _REQUIRE((rows, missing, zeros) == (17544, 352, 6114), 'Audited electricity R5 or zero counts differ')
    return {'wide_rows': rows, 'positions': rows * len(buildings), 'r5_absent': missing,
            'r5_present': rows * len(buildings) - missing, 'numeric_zeros': zeros}


def _read_exact(spark, item, parquet):
    from pyspark.sql.types import StructType

    # Hadoop's default LocalFileSystem can also open the unpinned checksum sidecar.
    frame = (spark.read.options(mergeSchema='false', **{
        'fs.file.impl': 'org.apache.hadoop.fs.RawLocalFileSystem', 'fs.file.impl.disable.cache': 'true',
    }).parquet(parquet.as_uri()))
    expected = StructType.fromJson(item['output_schema'])
    def columns(schema):
        return [(field.name, field.dataType, field.metadata) for field in schema]

    paths = [urlsplit(value) for value in frame.inputFiles()]
    _REQUIRE(len(paths) == 1 and paths[0].scheme == 'file' and not paths[0].netloc
             and unquote(paths[0].path) == str(parquet)
             and columns(frame.schema) == columns(expected), 'Spark read differs from the pinned Bronze file or schema')
    return frame


def _verify_cohort(frame, item, lineage, buildings):
    from pyspark.sql import functions as F

    _REQUIRE(frame.count() == item['output_rows'], 'Pinned Bronze metadata count differs')
    bad = F.lit(False)
    for name, value in zip(_LINEAGE, lineage, strict=True):
        bad = bad | ~F.col(name).eqNullSafe(F.lit(value))
    _REQUIRE(not frame.where(bad).limit(1).count(), 'Pinned Bronze metadata lineage differs')
    cohort = (frame.where(F.col('building_id').isin(*buildings))
              .groupBy('building_id').agg(F.count('*').alias('rows'),
                                         F.min('electricity').alias('meter')).limit(17).collect())
    _REQUIRE({r.building_id for r in cohort} == set(buildings)
             and all(r.rows == 1 and r.meter == 'Yes' for r in cohort),
             'Pinned Bronze metadata does not verify the approved electrical cohort')


def _verify_grant(campaign, evidence_fd, configuration):
    ticket = supervisor._read_record(evidence_fd, 'launch.json')['ticket']
    identity = asdict(supervisor.read_process_identity(os.getpid()))
    digest = supervisor._digest(ticket)
    path = Path(os.readlink(f'/proc/self/fd/{evidence_fd}'))
    _REQUIRE(sys.flags.isolated == 1 and sys.flags.no_site == 1 and
             supervisor._ACTIVE_CHILD_GRANT.get() == digest and
             path == campaign.root / 'evidence' / configuration['run_id'] and
             configuration['handler'] == supervisor._REAL_SILVER_HANDLER and
             supervisor._same(ticket['configuration'], configuration) and
             supervisor._read_record(evidence_fd, 'child-identity.json') == {
                 'supervision': 'D46', 'identity': identity, 'ticket_sha256': digest,
             }, 'Electrical Silver requires the cooperative child grant')


def prepare(campaign, evidence_fd, configuration):
    """Verify payload inputs after the watchdog grant and before JVM startup."""
    _verify_grant(campaign, evidence_fd, configuration)
    binding = configuration['real_silver']
    started = monotonic()
    request, grant = boundary.load_grant(campaign, binding['approval_sha256'])
    boundary.verify_inputs(campaign, request, grant)
    boundary.protect_writes(campaign, grant, binding)
    return request, grant, monotonic() - started


def execute(spark, campaign, evidence_fd, configuration, prepared):
    _verify_grant(campaign, evidence_fd, configuration)
    from tfm_lakehouse.bronze_calibration import _delta_storage
    from tfm_lakehouse.silver import silver_synthetic_publication as publication
    from tfm_lakehouse.silver.silver_electricity_integrated import transform_selected_electricity

    request, grant, input_seconds = prepared
    binding = configuration['real_silver']
    started = monotonic()
    base, output, evidence = boundary._locations(campaign.root, grant['mode'], request)
    mount = boundary._mount(campaign)
    pins = {pin.relative_path: pin for pin in request.inputs}
    manifest_sha, tables = _table_pins(base, pins, request, grant['mode'], mount)
    metadata_item, metadata_path, metadata_lineage = tables['metadata']
    _verify_cohort(_read_exact(spark, metadata_item, metadata_path), metadata_item, metadata_lineage, grant['cohort'])
    item, electricity_path, lineage = tables['electricity']
    bronze = _read_exact(spark, item, electricity_path)
    _REQUIRE(bronze.count() == item['output_rows'], 'Pinned Bronze electrical count differs')
    phases = {'pre_jvm_input_verification_seconds': input_seconds, 'bronze_verification_seconds': monotonic() - started}
    started = monotonic()
    frame = transform_selected_electricity(bronze, selection={'run_id': request.bronze_run_id, 'tables': [item]})
    phases['transformation_plan_seconds'] = monotonic() - started
    started = monotonic()
    witness = Path(grant['temporary']) / 'electrical-expected.jsonl'
    with (_pinned_text(base, pins, preflight._SOURCE['electricity'], mount) as source,
          witness.open('x', encoding='utf-8') as out):
        counts = _write_expected(source, out, grant['cohort'], lineage, request.bronze_run_id,
                                 request.tables[0].version, item['table_path'], real=grant['mode'] == 'real')
    _REQUIRE(counts['wide_rows'] == item['output_rows'], 'Electrical CSV and Bronze row counts differ')
    schema = ', '.join(f'{name} {dtype}' for name, dtype in _SCHEMA)
    expected = spark.read.schema(schema).option('mode', 'FAILFAST').json(witness.as_uri())
    phases['expected_generation_seconds'] = monotonic() - started
    intent = {
        'publication_id': request.publication_id, 'source_run_id': request.bronze_run_id,
        'manifest_sha256': manifest_sha, 'source_manifest_sha256': pins[preflight._MANIFEST].sha256,
        'source_tables': grant['request']['tables'], 'input_binding_sha256': supervisor._digest(grant['request']),
        'transform_version': boundary.TRANSFORM, 'input_rows': counts['wide_rows'], 'output_rows': counts['positions'],
        'profile': {'identity': boundary.PROFILE, 'data_kind': grant['mode'], 'buildings': grant['cohort']},
    }

    def confirm():
        _REQUIRE(binding['reservations'] == boundary.destination_identities(grant, mount),
                 'Electrical Silver destinations moved during publication')
        boundary.verify_inputs(campaign, request, grant)

    published = publication._persist_publication(
        spark, output, frame, intent, expected=expected, phases=phases, confirm=confirm, reserved=True,
    )
    proof = {'status': 'passed', 'approval_sha256': binding['approval_sha256'],
             'intent_sha256': supervisor._digest(intent), 'publication': published,
             'counts': counts, 'phase_seconds': phases, 'delta_storage': _delta_storage(output / 'table')}
    with boundary.checked_directory(evidence, mount) as proof_fd:
        supervisor._write_record(proof_fd, 'publication-proof.json', proof)
    supervisor._write_record(evidence_fd, 'real-silver.json', {'proof_sha256': supervisor._digest(proof)})


def verify_result(fd, result):
    """Corroborate the publication evidence after independently verified Spark shutdown."""
    config = result['configuration']
    binding = config['real_silver']
    mount = binding['reservations']['evidence'][0]
    with boundary.checked_directory(Path(binding['evidence']), mount) as proof_fd:
        _REQUIRE(boundary._identity(proof_fd) == binding['reservations']['evidence'],
                 'Electrical Silver evidence path changed')
        proof = supervisor._read_record(proof_fd, 'publication-proof.json')
    output = Path(binding['output'])
    with boundary.checked_directory(output, mount) as output_fd:
        _REQUIRE(boundary._identity(output_fd) == binding['reservations']['publication'],
                 'Electrical Silver output path changed')
        intent = supervisor._read_record(output_fd, 'intent.json')
        completed = supervisor._read_record(output_fd, 'completed.json')
    published = proof['publication']
    _REQUIRE(supervisor._read_record(fd, 'real-silver.json') == {'proof_sha256': supervisor._digest(proof)}
             and proof['status'] == published['status'] == 'passed'
             and proof['approval_sha256'] == binding['approval_sha256']
             and proof['intent_sha256'] == supervisor._digest(intent) and published == completed
             and published['publication_id'] == config['run_id'] and published['delta_version'] == 0
             and published['profile']['identity'] == boundary.PROFILE
             and published['profile']['data_kind'] == binding['mode']
             and published['profile']['buildings'] == list(preflight.electrical_cohort())
             and published['transform_version'] == boundary.TRANSFORM
             and published['output_rows'] == proof['counts']['positions']
             and published['table_path'] == str(output / 'table')
             and result['spark']['events']['status'] == 'complete', 'Electrical Silver terminal evidence differs')
    return proof
