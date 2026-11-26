"""Synthetic D43 provenance checks; no historical Git mutation or real inputs."""
import json
from hashlib import sha256
from types import SimpleNamespace

import pytest

from tfm_lakehouse.silver import contextual_legacy as legacy
from tfm_lakehouse.silver import silver_iqr_publication as iqr

pytestmark = pytest.mark.unit


@pytest.fixture
def intent():
    return {'contract_version': 2, 'publication_id': 'context',
            'silver': {'publication_id': 'silver'}, 'metadata_files_sha256': 'invented'}


def test_anchored_git_record_and_exact_identity(monkeypatch, intent):
    record = {'contract_version': 1, 'record_type': 'observed_contextual_reservation',
              'publication_id': 'context', 'reservation_intent_file_sha256': 'invented-bytes',
              'contextual_intent_sha256': iqr._digest(intent)}
    def fake_git(args, **kwargs):
        assert args[:3] == ['git', '--no-optional-locks', 'show']
        assert args[3] == legacy._PRE_D42_COMMIT + ':docs/h06-contextual-observed-reservations/context.json'
        assert kwargs['env']['GIT_NO_REPLACE_OBJECTS'] == '1'
        assert kwargs['env']['GIT_OPTIONAL_LOCKS'] == '0'
        assert 'TFM_REAL_CONFIG' not in kwargs['env']
        return SimpleNamespace(stdout=json.dumps(record).encode())
    monkeypatch.setattr(legacy.subprocess, 'run', fake_git)
    archived = legacy._read_archived_evidence('context')
    assert legacy._verify_evidence(archived, intent, 'context', 'invented-bytes') == record
    with pytest.raises(iqr.IQRPublicationError, match='exact'):
        legacy._verify_evidence(archived, {**intent, 'publication_id': 'other'}, 'other', 'invented-bytes')
    with pytest.raises(iqr.IQRPublicationError, match='exact'):
        legacy._verify_evidence(archived, {**intent, 'metadata_files_sha256': 'changed'}, 'context', 'invented-bytes')
    with pytest.raises(iqr.IQRPublicationError, match='exact'):
        legacy._verify_evidence(archived, intent, 'context', 'different-file-bytes')


@pytest.mark.parametrize('path', ['', '../invented', '/invented', 'docs/invented', '.'])
def test_invalid_evidence_path_rejected_before_git(monkeypatch, path):
    monkeypatch.setattr(legacy.subprocess, 'run', lambda *a, **k: pytest.fail('Git called'))
    with pytest.raises(iqr.IQRPublicationError, match='identifier'):
        legacy._read_archived_evidence(path)


def test_authorization_bound_to_id_before_workspace_access(monkeypatch):
    monkeypatch.setattr(legacy.publication, '_workspace', lambda *_: pytest.fail('workspace inspected'))
    with pytest.raises(iqr.IQRPublicationError, match='authorization'):
        legacy.recover_legacy_synthetic_contextual(None, None, 'silver', 'context', None,
            authorized_publication_id='other', authorized_intent_sha256='invented',
            justification='invented test',
            capture_id='capture', owner_id='owner')


def test_fixed_git_tree_rejects_absent_invented_record():
    with pytest.raises(iqr.IQRPublicationError, match='unavailable'):
        legacy._read_archived_evidence('invented-d43-no-record')


def test_historical_record_rejects_duplicate_json_fields(monkeypatch):
    fake = SimpleNamespace(stdout=b'{"publication_id":"a","publication_id":"b"}')
    monkeypatch.setattr(legacy.subprocess, 'run', lambda *a, **k: fake)
    with pytest.raises(iqr.IQRPublicationError, match='Duplicate'):
        legacy._read_archived_evidence('invented')


@pytest.mark.integration
@pytest.mark.parametrize('committed', [False, True])
def test_exceptional_recovery_with_synthetic_archived_record(spark, tmp_path, monkeypatch, committed):
    from tfm_lakehouse.silver import contextual_identity as identity
    from tfm_lakehouse.silver import contextual_reader as reader
    from tfm_lakehouse.silver import silver_synthetic_publication as silver
    from test_silver_iqr_publication import _create_silver

    config, _, _ = _create_silver(spark, tmp_path)
    intent = identity.build_synthetic_contextual_intent(spark, config, 'silver', 'context', tmp_path)
    _, expected = identity.enrich_synthetic_metadata(spark, config, 'silver', tmp_path)
    folder = tmp_path / 'contextual-publications' / 'context'
    folder.mkdir(parents=True)
    silver._exclusive_json(folder / 'intent.json', intent)
    if committed:
        (expected.write.format('delta').mode('errorifexists')
         .option('userMetadata', iqr._canonical(intent)).save(str(folder / 'table')))
    before_logs = list((folder / 'table' / '_delta_log').glob('*.json')) if committed else []
    record = {'contract_version': 1, 'record_type': 'observed_contextual_reservation',
              'publication_id': 'context',
              'reservation_intent_file_sha256': sha256((folder / 'intent.json').read_bytes()).hexdigest(),
              'contextual_intent_sha256': iqr._digest(intent)}
    monkeypatch.setattr(legacy, '_read_archived_evidence', lambda _: record)
    result = legacy.recover_legacy_synthetic_contextual(
        spark, config, 'silver', 'context', tmp_path,
        authorized_publication_id='context', authorized_intent_sha256=iqr._digest(intent),
        justification='invented author authorization', capture_id='capture', owner_id='owner')
    assert result['intent'] == intent and result['delta_version'] == 0
    logs = list((folder / 'table' / '_delta_log').glob('*.json'))
    assert len(logs) == 1 and (not committed or logs == before_logs)
    verified = iqr._read(tmp_path / 'contextual-legacy-recoveries' / 'context.verified.json')
    if committed:
        assert verified['prior_table_tree_sha256'] == legacy.capture.fingerprint(folder / 'table')
        assert verified['prior_table_verification'] == result
    else:
        assert verified['prior_table_tree_sha256'] is None
    assert reader.read_synthetic_contextual(spark, config, 'context', tmp_path)[0] == result


@pytest.mark.integration
def test_pre_d42_writer_publication_is_readable(spark, tmp_path):
    import os
    import subprocess
    import types
    from tfm_lakehouse.silver import contextual_reader as reader
    from test_silver_iqr_publication import _create_silver

    environment = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    environment.update(GIT_OPTIONAL_LOCKS='0', GIT_NO_REPLACE_OBJECTS='1')
    source = subprocess.run(
        ['git', '--no-optional-locks', 'show', legacy._PRE_D42_COMMIT +
         ':src/tfm_lakehouse/silver/contextual_publication.py'],
        cwd=legacy._PROJECT_ROOT, env=environment, capture_output=True, check=True).stdout
    previous = types.ModuleType('synthetic_pre_d42_contextual_publication')
    exec(compile(source, '<synthetic-pre-d42-writer>', 'exec'), previous.__dict__)
    config, _, _ = _create_silver(spark, tmp_path)
    before = previous.publish_synthetic_contextual(spark, config, 'silver', 'context', tmp_path)
    assert not (tmp_path / 'contextual-captures').exists()
    after, data = reader.read_synthetic_contextual(spark, config, 'context', tmp_path)
    assert after == before and data.count() == before['output_rows']


def test_authorization_digest_mismatch_rejected_before_archive(tmp_path, monkeypatch, intent):
    folder = tmp_path / 'contextual-publications' / 'context'
    folder.mkdir(parents=True)
    (folder / 'intent.json').write_text(json.dumps(intent))
    monkeypatch.setattr(legacy.publication, '_workspace', lambda *_: tmp_path)
    monkeypatch.setattr(legacy, '_read_archived_evidence', lambda *_: pytest.fail('archive read'))
    with pytest.raises(iqr.IQRPublicationError, match='does not bind'):
        legacy.recover_legacy_synthetic_contextual(None, None, 'silver', 'context', tmp_path,
            authorized_publication_id='context', authorized_intent_sha256='wrong',
            justification='invented author authorization', capture_id='capture', owner_id='owner')
    assert not (tmp_path / 'contextual-legacy-recoveries').exists()


def test_conflicting_d42_binding_blocks_legacy_recovery(tmp_path, monkeypatch, intent):
    folder = tmp_path / 'contextual-publications' / 'context'
    folder.mkdir(parents=True)
    (folder / 'intent.json').write_text(json.dumps(intent))
    bindings = tmp_path / 'contextual-capture-bindings'
    bindings.mkdir()
    (bindings / 'context.json').write_text('{}')
    record = {'contract_version': 1, 'record_type': 'observed_contextual_reservation',
              'publication_id': 'context',
              'reservation_intent_file_sha256': sha256((folder / 'intent.json').read_bytes()).hexdigest(),
              'contextual_intent_sha256': iqr._digest(intent)}
    monkeypatch.setattr(legacy.publication, '_workspace', lambda *_: tmp_path)
    monkeypatch.setattr(legacy, '_read_archived_evidence', lambda *_: record)
    monkeypatch.setattr(legacy.identity, 'verify_synthetic_contextual_intent', lambda *a, **k: None)
    monkeypatch.setattr(legacy.identity, 'enrich_synthetic_metadata',
                        lambda *a, **k: ({'metadata_files_sha256': 'invented'}, None))
    monkeypatch.setattr(legacy.capture, 'namespace', lambda *_: tmp_path / 'contextual-captures')
    monkeypatch.setattr(legacy.capture, 'isolated_namespace', lambda root, config, name: root / name)
    with pytest.raises(iqr.IQRPublicationError, match='Conflicting D42'):
        legacy.recover_legacy_synthetic_contextual(None, None, 'silver', 'context', tmp_path,
            authorized_publication_id='context', authorized_intent_sha256=iqr._digest(intent),
            justification='invented author authorization', capture_id='capture', owner_id='owner')
    assert (bindings / 'context.json').read_text() == '{}'
    assert not (tmp_path / 'contextual-legacy-recoveries').exists()


@pytest.mark.integration
def test_legacy_table_mutation_before_marker_is_rejected(spark, tmp_path, monkeypatch):
    from tfm_lakehouse.silver import contextual_identity as identity
    from tfm_lakehouse.silver import contextual_publication as publication
    from tfm_lakehouse.silver import silver_synthetic_publication as silver
    from test_silver_iqr_publication import _create_silver

    config, _, _ = _create_silver(spark, tmp_path)
    intent = identity.build_synthetic_contextual_intent(spark, config, 'silver', 'context', tmp_path)
    _, view = identity.enrich_synthetic_metadata(spark, config, 'silver', tmp_path)
    folder = tmp_path / 'contextual-publications' / 'context'
    folder.mkdir(parents=True)
    silver._exclusive_json(folder / 'intent.json', intent)
    (view.write.format('delta').mode('errorifexists')
     .option('userMetadata', iqr._canonical(intent)).save(str(folder / 'table')))
    record = {'contract_version': 1, 'record_type': 'observed_contextual_reservation',
              'publication_id': 'context',
              'reservation_intent_file_sha256': sha256((folder / 'intent.json').read_bytes()).hexdigest(),
              'contextual_intent_sha256': iqr._digest(intent)}
    monkeypatch.setattr(legacy, '_read_archived_evidence', lambda _: record)
    verify_output, verify_capture = publication._verify_output, legacy.capture.verify
    state = {'outputs': 0, 'injected': False}
    def output(*args, **kwargs):
        result = verify_output(*args, **kwargs)
        state['outputs'] += 1
        return result
    def verify(*args, **kwargs):
        result = verify_capture(*args, **kwargs)
        if state['outputs'] >= 2 and not state['injected']:
            (folder / 'table' / 'invented-change').write_text('changed')
            state['injected'] = True
        return result
    monkeypatch.setattr(publication, '_verify_output', output)
    monkeypatch.setattr(legacy.capture, 'verify', verify)
    with pytest.raises(iqr.IQRPublicationError, match='Historical committed table identity changed'):
        legacy.recover_legacy_synthetic_contextual(
            spark, config, 'silver', 'context', tmp_path,
            authorized_publication_id='context', authorized_intent_sha256=iqr._digest(intent),
            justification='invented author authorization', capture_id='capture', owner_id='owner')
    assert state['injected'] and not (folder / 'completed.json').exists()
    assert (folder / 'table' / 'invented-change').is_file()
