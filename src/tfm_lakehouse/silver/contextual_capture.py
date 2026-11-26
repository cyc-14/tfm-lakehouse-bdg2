"""Durable synthetic contextual captures, independently owned from publications."""
import json
import os

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint as fingerprint

_FIELDS = ('__h06_capture_zone_status', '__h06_capture_zone_resource', '__h06_capture_zone_sha256')


def isolated_namespace(root, config, name):
    iqr._require(name in {'contextual-captures', 'contextual-capture-bindings',
                          'contextual-legacy-recoveries'}, 'Unknown contextual namespace')
    target = root / name
    for other in (config.raw_root, config.lakehouse_root, root / 'silver-publications',
                  root / 'iqr-publications', root / 'contextual-publications',
                  root / 'weather-captures', root / 'weather-publications',
                  *(root / item for item in ('contextual-captures', 'contextual-capture-bindings',
                                             'contextual-legacy-recoveries') if item != name)):
        iqr._require(not target.is_relative_to(other) and not other.is_relative_to(target),
                     'Contextual capture overlaps another product')
    silver._no_links(target)
    silver._check_tree(target)
    return target


def namespace(root, config):
    return isolated_namespace(root, config, 'contextual-captures')


def _exclusive_json(path, value):
    # Independent evidence writer: publication fault injection must not target captures.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        json.dump(value, stream, sort_keys=True, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())


def _projection(spark, measured, parent):
    schema = StructType.fromJson(parent['schema'])
    projected = measured.select(*[F.col('`' + f.name.replace('`', '``') + '`') for f in schema])
    return spark.createDataFrame(projected.rdd, schema, verifySchema=True)


def _derived(spark, measured, parent):
    missing = F.col('metadata_timezone_status').isin('missing_row', 'missing_column')
    iqr._require(not measured.where(~missing & ~F.col('metadata_timezone_status').eqNullSafe(
        F.col(_FIELDS[0]))).limit(1).count(), 'Timezone interpretation differs')
    entries = (measured.select('metadata_timezone', *[F.col(name).alias(alias) for name, alias in
        zip(_FIELDS, ('status', 'resource', 'sha256'))]).distinct().collect())
    projected = _projection(spark, measured, parent)
    derived = dict(parent)
    derived['timezone_evidence'] = identity._merge_zone_evidence(entries)
    derived['schema'] = projected.schema.jsonValue()
    derived['context_multiset_sha256'] = identity._multiset_hash(projected)
    return derived, projected


def create(spark, config, root, namespace_path, capture_id, owner_id, parent, view):
    """Reserve once; retain incomplete capture on every failure."""
    folder = namespace_path / capture_id
    silver._no_links(folder)
    iqr._require(not folder.exists(), 'Contextual capture already reserved')
    iqr._require(not set(_FIELDS).intersection(view.columns), 'Reserved capture field exists')
    zone = F.udf(identity._controlled_zone, identity._ZONE_EVIDENCE)(F.col('metadata_timezone'))
    z = view.withColumn('__h06_capture_zone', zone)
    measured = z.select(*[F.col('`' + c.replace('`', '``') + '`') for c in view.columns],
                        *[F.col('__h06_capture_zone.' + part).alias(name) for part, name in
                          zip(('status', 'resource', 'sha256'), _FIELDS)])
    intent = {'contract_version': 1, 'capture_id': capture_id, 'owner_id': owner_id,
              'contextual_intent': parent, 'logical_schema': measured.schema.jsonValue()}
    folder.mkdir()
    _exclusive_json(folder / 'intent.json', intent)
    (measured.write.format('delta').mode('errorifexists')
     .option('userMetadata', iqr._canonical(intent)).save(str(folder / 'table')))
    result, _ = verify(spark, config, root, folder, capture_id, owner_id, parent, complete=False)
    _exclusive_json(folder / 'completed.json', result)
    return result


def verify(spark, config, root, folder, capture_id, owner_id, parent, *, complete=True):
    """Verify exact capture identity and return a lazy projection of pinned Delta v0."""
    from tfm_lakehouse.silver import weather_capture as shared
    silver._check_tree(folder)
    iqr._require(folder.is_dir(), 'Contextual capture unavailable')
    wanted = {'intent.json', 'table', 'completed.json'} if complete else {'intent.json', 'table'}
    iqr._require({p.name for p in folder.iterdir()} == wanted, 'Contextual capture incomplete')
    before = fingerprint(folder)
    intent = iqr._read(folder / 'intent.json')
    iqr._require(type(intent.get('contract_version')) is int and intent['contract_version'] == 1
                 and intent.get('capture_id') == capture_id and intent.get('owner_id') == owner_id
                 and iqr._canonical(intent.get('contextual_intent')) == iqr._canonical(parent),
                 'Contextual capture identity conflicts')
    table = folder / 'table'
    tree = fingerprint(table)
    commit = shared._commit(spark, table, intent)
    physical = spark.read.format('delta').option('versionAsOf', 0).load(str(table))
    measured = shared._logical_frame(spark, physical, intent['logical_schema'])
    derived, projection = _derived(spark, measured, parent)
    iqr._require(iqr._canonical(derived) == iqr._canonical(parent), 'Captured contextual intent differs')
    identity.verify_synthetic_contextual_intent(
        spark, config, parent['silver']['publication_id'], parent['publication_id'], root, parent,
        enrichment_version=parent['enrichment_version'])
    diagnostic, expected = identity.enrich_synthetic_metadata(
        spark, config, parent['silver']['publication_id'], root)
    iqr._require(iqr._canonical(expected.schema.jsonValue()) == iqr._canonical(parent['schema']),
                 'Captured contextual schema differs')
    iqr._require(diagnostic['metadata_files_sha256'] == parent['metadata_files_sha256'],
                 'Metadata files changed')
    shared._same_multiset(projection, expected)
    electrical_identity, electrical = iqr._verify_silver(spark, config, root, parent['silver']['publication_id'])
    iqr._require(iqr._canonical(electrical_identity) == iqr._canonical(parent['silver']), 'Silver identity differs')
    shared._same_multiset(projection.select(*electrical.columns), electrical)
    iqr._stable_silver(electrical_identity, config)
    iqr._require(fingerprint(table) == tree and fingerprint(folder) == before, 'Contextual capture changed')
    result = {'contract_version': 1, 'capture_id': capture_id, 'owner_id': owner_id,
              'intent': intent, **commit, 'physical_tree_sha256': tree,
              'physical_schema': physical.schema.jsonValue(), 'logical_schema': intent['logical_schema'],
              'reconciliation': 'contextual_and_electrical_multisets_passed'}
    if complete:
        iqr._require(iqr._canonical(iqr._read(folder / 'completed.json')) == iqr._canonical(result),
                     'Contextual capture evidence differs')
    return result, projection
