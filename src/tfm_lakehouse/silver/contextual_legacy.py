"""Explicit synthetic v2 legacy recovery anchored to a pre-D42 Git tree."""
import json
from hashlib import sha256
import os
from pathlib import Path
import subprocess

from tfm_lakehouse.silver import contextual_capture as capture
from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_capture as capture_lock

# This exact tree was the project's HEAD before the D42 materialization work.
_PRE_D42_COMMIT = '0e979b2b5ae161db7d60556363665616182df9c0'
_PROJECT_ROOT = Path(__file__).absolute().parents[3]


def _archive_path(publication_id):
    iqr._identifier(publication_id)
    return 'docs/h06-contextual-observed-reservations/' + publication_id + '.json'


def _read_archived_evidence(publication_id):
    """Read a reservation record from the fixed historical Git archive."""
    path = _archive_path(publication_id)
    env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_OPTIONAL_LOCKS='0', GIT_NO_REPLACE_OBJECTS='1')
    try:
        result = subprocess.run(['git', '--no-optional-locks', 'show',
                                 _PRE_D42_COMMIT + ':' + path],
                                cwd=_PROJECT_ROOT, env=env, check=True,
                                capture_output=True, timeout=15)
        iqr._require(len(result.stdout) <= 1024 * 1024, 'Historical evidence is too large')
        record = iqr._parse(result.stdout)
    except (OSError, subprocess.SubprocessError, UnicodeError, json.JSONDecodeError) as exc:
        raise iqr.IQRPublicationError('Historical Git evidence is unavailable') from exc
    iqr._require(isinstance(record, dict), 'Historical evidence is invalid')
    return record


def _verify_evidence(record, intent, publication_id, reservation_sha256):
    expected = {'contract_version': 1, 'record_type': 'observed_contextual_reservation',
                'publication_id': publication_id, 'contextual_intent_sha256': iqr._digest(intent),
                'reservation_intent_file_sha256': reservation_sha256}
    iqr._require(iqr._canonical(record) == iqr._canonical(expected),
                 'Historical evidence does not bind the exact contextual intent')
    return expected


def recover_legacy_synthetic_contextual(spark, config, silver_id, publication_id, workspace, *,
        authorized_publication_id, authorized_intent_sha256, justification,
        capture_id, owner_id):
    """Recover one specifically authorized legacy reservation; never infer provenance.

    The caller must have explicit author authorization for these exact values.
    Source evidence must already exist in the fixed pre-D42 Git tree; the
    authorization is not itself historical evidence. No capture is repaired.
    """
    for value in (silver_id, publication_id, authorized_publication_id, capture_id, owner_id):
        iqr._identifier(value)
    iqr._require(authorized_publication_id == publication_id and isinstance(justification, str)
                 and bool(justification.strip()), 'Explicit recovery authorization is required')
    root = publication._workspace(workspace, config)
    with publication._lock(root) as namespace:
        folder = namespace / publication_id
        silver._check_tree(folder)
        iqr._require(folder.is_dir() and not (folder / 'completed.json').exists(),
                     'Expected an unfinished historical publication')
        iqr._folder_layout(folder, 'table')
        intent = iqr._read(folder / 'intent.json')
        iqr._require(intent.get('publication_id') == publication_id
                     and intent.get('silver', {}).get('publication_id') == silver_id
                     and authorized_intent_sha256 == iqr._digest(intent),
                     'Recovery authorization does not bind the publication intent')
        reservation_sha256 = sha256(iqr._bytes(folder / 'intent.json')).hexdigest()
        archive = _verify_evidence(_read_archived_evidence(publication_id), intent, publication_id,
                                   reservation_sha256)
        identity.verify_synthetic_contextual_intent(spark, config, silver_id, publication_id, root, intent)
        diagnostic, view = identity.enrich_synthetic_metadata(spark, config, silver_id, root)
        iqr._require(diagnostic['metadata_files_sha256'] == intent['metadata_files_sha256'],
                     'Metadata evidence differs')
        prior_commit = None
        prior_table_sha256 = None
        if (folder / 'table').exists():
            prior_table_sha256 = capture.fingerprint(folder / 'table')
            prior_commit = publication._verify_output(spark, folder / 'table', view, intent)
            iqr._require(capture.fingerprint(folder / 'table') == prior_table_sha256,
                         'Historical committed table changed during verification')
        captures = capture.namespace(root, config)
        bindings = capture.isolated_namespace(root, config, 'contextual-capture-bindings')
        records = capture.isolated_namespace(root, config, 'contextual-legacy-recoveries')
        binding_path = bindings / (publication_id + '.json')
        record_path = records / (publication_id + '.json')
        evidence = {'contract_version': 1, 'publication_id': publication_id,
                    'intent_sha256': iqr._digest(intent), 'historical_commit': _PRE_D42_COMMIT,
                    'historical_path': _archive_path(publication_id), 'historical_record': archive,
                    'justification': justification, 'capture_id': capture_id, 'owner_id': owner_id}
        with capture_lock._lock(captures):
            iqr._require(not binding_path.exists(), 'Conflicting D42 capture binding')
            if captures.exists():
                for item in captures.iterdir():
                    if item.name == '.capture.lock':
                        continue
                    silver._check_tree(item)
                    other = iqr._read(item / 'intent.json')
                    iqr._require(other.get('contextual_intent', {}).get('publication_id') != publication_id,
                                 'Conflicting D42 capture evidence')
            iqr._require(not record_path.exists(), 'Legacy recovery already reserved')
            records.mkdir(exist_ok=True)
            capture._exclusive_json(record_path, evidence)
            capture.create(spark, config, root, captures, capture_id, owner_id, intent, view)
            captured, _ = capture.verify(spark, config, root, captures / capture_id,
                                          capture_id, owner_id, intent)
            tree = capture.fingerprint(captures / capture_id)
            iqr._require(sha256(iqr._bytes(folder / 'intent.json')).hexdigest() == reservation_sha256,
                         'Historical reservation evidence changed')
            if prior_table_sha256 is not None:
                iqr._require(capture.fingerprint(folder / 'table') == prior_table_sha256,
                             'Historical committed table changed during capture')
            binding = {'contract_version': 1, 'publication_id': publication_id,
                       'capture_id': capture_id, 'owner_id': owner_id,
                       'capture_evidence': captured, 'capture_tree_sha256': tree}
            verification = {'contract_version': 1, 'recovery': evidence,
                            'capture_evidence': captured, 'prior_table_verification': prior_commit,
                            'prior_table_tree_sha256': prior_table_sha256}
            capture._exclusive_json(records / (publication_id + '.verified.json'), verification)
            binding['legacy_verification_sha256'] = iqr._digest(verification)
            bindings.mkdir(exist_ok=True)
            capture._exclusive_json(binding_path, binding)
    result = publication.publish_synthetic_contextual(spark, config, silver_id, publication_id, root,
                                                       capture_id=capture_id, owner_id=owner_id)
    capture._exclusive_json(records / (publication_id + '.completed.json'),
                            {'contract_version': 1, 'publication': result})
    return result
