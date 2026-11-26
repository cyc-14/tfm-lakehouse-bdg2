"""Focused synthetic compatibility checks for durable contextual captures."""
import pytest

from tfm_lakehouse.silver import contextual_capture as capture
from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
def test_capture_preserves_v2_and_historical_reader_after_retirement(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    original = identity.build_synthetic_contextual_intent(spark, config, 'silver', 'context', tmp_path)
    result = publication.publish_synthetic_contextual(
        spark, config, 'silver', 'context', tmp_path, capture_id='explicit', owner_id='owner')
    assert iqr._canonical(result['intent']) == iqr._canonical(original)
    folder = tmp_path / 'contextual-captures' / 'explicit'
    marker = iqr._read(folder / 'completed.json')
    assert marker['delta_version'] == 0 and marker['delta_table_id']
    assert marker['physical_tree_sha256'] == iqr._fingerprint(folder / 'table')
    checked, frame = capture.verify(spark, config, tmp_path, folder, 'explicit', 'owner', original)
    assert checked == marker
    assert identity._multiset_hash(frame) == original['context_multiset_sha256']
    (folder / 'table' / 'invented-corruption').write_text('synthetic corruption')
    with pytest.raises(iqr.IQRPublicationError, match='evidence differs'):
        capture.verify(spark, config, tmp_path, folder, 'explicit', 'owner', original)
    held = tmp_path / 'author-retired-capture'
    folder.rename(held)
    evidence, historical = reader.read_synthetic_contextual(spark, config, 'context', tmp_path)
    assert evidence == result and historical.count() == result['output_rows']
    assert held.is_dir()


@pytest.mark.unit
def test_partial_capture_ids_rejected_before_workspace_inspection(monkeypatch):
    monkeypatch.setattr(publication, '_workspace', lambda *_: pytest.fail('workspace inspected'))
    with pytest.raises(iqr.IQRPublicationError, match='paired'):
        publication.publish_synthetic_contextual(None, None, 'silver', 'context', None, capture_id='id')


@pytest.mark.unit
def test_capture_lock_rejects_cooperating_writer(tmp_path):
    from tfm_lakehouse.silver import weather_capture
    with weather_capture._lock(tmp_path):
        with pytest.raises(iqr.IQRPublicationError, match='active'):
            with weather_capture._lock(tmp_path):
                pass


@pytest.mark.unit
def test_weather_enrichment_imports_in_fresh_python_worker():
    import os
    import subprocess
    import sys
    result = subprocess.run([sys.executable, '-B', '-c',
                             'import tfm_lakehouse.silver.weather_enrichment'],
                            env=os.environ.copy(), capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.unit
@pytest.mark.parametrize('state', ['missing_capture_with_binding', 'missing_binding_with_reservation'])
def test_missing_capture_or_binding_is_not_recreated(tmp_path, monkeypatch, state):
    from types import SimpleNamespace
    intent = {'silver': {'publication_id': 'silver'}, 'metadata_files_sha256': 'invented',
              'schema': {'fields': []}}
    diagnostic = {'silver': intent['silver'], 'metadata_files_sha256': 'invented'}
    view = SimpleNamespace(schema=SimpleNamespace(jsonValue=lambda: {'fields': []}))
    monkeypatch.setattr(publication, '_workspace', lambda *_: tmp_path)
    monkeypatch.setattr(identity, 'build_synthetic_contextual_intent', lambda *a, **k: intent)
    monkeypatch.setattr(identity, 'enrich_synthetic_metadata', lambda *a, **k: (diagnostic, view))
    monkeypatch.setattr(iqr, '_fingerprint', lambda *_: 'invented')
    captures = tmp_path / 'contextual-captures'
    monkeypatch.setattr(capture, 'namespace', lambda *_: captures)
    monkeypatch.setattr(capture, 'isolated_namespace', lambda root, config, name: root / name)
    monkeypatch.setattr(capture, 'create', lambda *a, **k: pytest.fail('capture recreated'))
    if state == 'missing_capture_with_binding':
        bindings = tmp_path / 'contextual-capture-bindings'
        bindings.mkdir()
        (bindings / 'context.json').write_text('{}')
    else:
        (tmp_path / 'contextual-publications' / 'context').mkdir(parents=True)
    with pytest.raises(iqr.IQRPublicationError, match='missing'):
        config = SimpleNamespace(table_path=lambda _: tmp_path / 'invented-metadata')
        publication.publish_synthetic_contextual(None, config, 'silver', 'context', tmp_path)


@pytest.mark.unit
@pytest.mark.parametrize('name', ['contextual-captures', 'contextual-capture-bindings',
                                  'contextual-legacy-recoveries'])
def test_namespace_rejects_overlap_with_synthetic_sources(tmp_path, name, monkeypatch):
    from types import SimpleNamespace
    config = SimpleNamespace(raw_root=tmp_path / name, lakehouse_root=tmp_path / 'bronze')
    monkeypatch.setattr(capture.silver, '_no_links', lambda *_: pytest.fail('overlap was inspected'))
    with pytest.raises(iqr.IQRPublicationError, match='overlaps'):
        capture.isolated_namespace(tmp_path, config, name)


@pytest.mark.integration
def test_capture_records_valid_timezone_resource_evidence(spark, tmp_path):
    from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
    from tfm_lakehouse.ingestion.bronze import run_bronze
    from tfm_lakehouse.silver import silver_synthetic_publication as silver
    config, _, _ = _create_silver(spark, tmp_path)
    config.metadata.write_text('building_id,timezone\n' +
                               '\n'.join(f'{name},UTC' for name in APPROVED_BUILDINGS) + '\n')
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze['run_id'], 'zone-silver', tmp_path)
    result = publication.publish_synthetic_contextual(spark, config, 'zone-silver', 'zone-context', tmp_path)
    status, resource, digest = identity._controlled_zone('UTC')
    assert status == 'valid' and len(digest) == 64
    assert result['intent']['timezone_evidence'] == [
        {'timezone': 'UTC', 'status': status, 'resource': resource, 'sha256': digest}]
    marker = iqr._read(tmp_path / 'contextual-captures' / 'zone-context' / 'completed.json')
    assert marker['intent']['contextual_intent'] == result['intent']


@pytest.mark.integration
def test_substituted_delta_capture_is_rejected(spark, tmp_path):
    from delta.tables import DeltaTable
    config, _, _ = _create_silver(spark, tmp_path)
    result = publication.publish_synthetic_contextual(spark, config, 'silver', 'context', tmp_path)
    folder = tmp_path / 'contextual-captures' / 'context'
    captured_intent = iqr._read(folder / 'intent.json')
    original = spark.read.format('delta').option('versionAsOf', 0).load(str(folder / 'table'))
    replacement = tmp_path / 'synthetic-replacement-table'
    (original.write.format('delta').mode('errorifexists')
     .option('userMetadata', iqr._canonical(captured_intent)).save(str(replacement)))
    previous_id = iqr._read(folder / 'completed.json')['delta_table_id']
    replacement_id = DeltaTable.forPath(spark, str(replacement)).detail().select('id').first()['id']
    assert previous_id != replacement_id
    retained = tmp_path / 'retained-original-capture-table'
    (folder / 'table').rename(retained)
    replacement.rename(folder / 'table')
    with pytest.raises(iqr.IQRPublicationError, match='Contextual capture evidence differs|Capture table ID differs'):
        capture.verify(spark, config, tmp_path, folder, 'context', 'silver', result['intent'])
    assert retained.is_dir() and (folder / 'table').is_dir()
