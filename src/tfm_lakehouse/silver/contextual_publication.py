"""Explicit, recoverable publication of synthetic contextual snapshots."""
from contextlib import contextmanager
import fcntl
import logging
import os
from pathlib import Path

from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_capture as capture
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver

_LOGGER = logging.getLogger(__name__)


def _record_cleanup_failure(primary, error):
    try:
        from tfm_lakehouse.silver.weather_capture import _record_cleanup_failure as record
        record(primary, error)
    except BaseException:
        try:
            failures = list(getattr(primary, '_h06_cleanup_failures', []))
            failures.append(error)
            BaseException.__setattr__(primary, '_h06_cleanup_failures', failures)
        except BaseException:
            pass
        try:
            primary.add_note(f'Contextual lock cleanup failed: {error!r}')
        except BaseException:
            try:
                _LOGGER.error('Contextual lock cleanup failed',
                              exc_info=(type(error), error, error.__traceback__))
            except BaseException:
                pass


@contextmanager
def _lock(root):
    namespace = root / "contextual-publications"
    silver._no_links(namespace)
    namespace.mkdir(exist_ok=True)
    handle = silver._open_regular(namespace / ".publication.lock", os.O_RDWR | os.O_CREAT)
    acquired = False
    primary = None
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as exc:
            raise iqr.IQRPublicationError("Another local contextual publication is active") from exc
        yield namespace
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures = []
        if acquired:
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            except BaseException as exc:
                failures.append(exc)
        try:
            handle.close()
        except BaseException as exc:
            failures.append(exc)
        if primary is not None:
            for exc in failures:
                _record_cleanup_failure(primary, exc)
        elif failures:
            for exc in failures[1:]:
                _record_cleanup_failure(failures[0], exc)
            raise failures[0]


def _workspace(workspace, config):
    root = iqr._workspace(workspace, config)
    target = root / "contextual-publications"
    silver._no_links(target)
    for other in (config.raw_root.resolve(), config.lakehouse_root.resolve(),
                  root / "silver-publications", root / "iqr-publications"):
        iqr._require(not target.is_relative_to(other) and not other.is_relative_to(target),
                     "Contextual destination overlaps source paths")
    silver._check_tree(target)
    return root


def _verify_output(spark, target, expected, intent):
    commit = iqr._commit(spark, target, intent)
    actual = spark.read.format("delta").option("versionAsOf", 0).load(str(target))
    # Delta makes top-level fields nullable. All other field properties must match.
    expected_schema = expected.schema.jsonValue()
    for field in expected_schema["fields"]:
        field["nullable"] = True
    iqr._require(iqr._canonical(actual.schema.jsonValue()) == iqr._canonical(expected_schema),
                 "Contextual committed schema differs")
    iqr._require(not expected.exceptAll(actual).limit(1).count()
                 and not actual.exceptAll(expected).limit(1).count(),
                 "Contextual committed multiset differs")
    iqr._require(identity._multiset_hash(actual) == intent["context_multiset_sha256"],
                 "Contextual committed fingerprint differs")
    return {"contract_version": 1, "status": "passed", "intent": intent,
            **commit, "table_path": str(target), "schema": actual.schema.jsonValue(),
            "output_rows": actual.count(), "reconciliation": "schema_multiset_passed"}


def publish_synthetic_contextual(spark, config, silver_publication_id: str,
                                 publication_id: str, workspace: Path, *,
                                 enrichment_version: str = identity.ENRICHMENT_VERSION,
                                 capture_id: str | None = None, owner_id: str | None = None) -> dict:
    """Publish/reconcile v2 from one durable capture, retaining all failure evidence.

    By default the explicitly supplied publication and Silver IDs are also the
    capture and owner IDs. The lock order is publication then capture. Completed
    legacy publications can be reconciled without any retained capture.
    """
    iqr._identifier(silver_publication_id)
    iqr._identifier(publication_id)
    iqr._require((capture_id is None) == (owner_id is None), 'Capture and owner IDs must be paired')
    explicit_capture = capture_id is not None
    capture_id = publication_id if capture_id is None else capture_id
    owner_id = silver_publication_id if owner_id is None else owner_id
    iqr._identifier(capture_id)
    iqr._identifier(owner_id)
    root = _workspace(workspace, config)
    from tfm_lakehouse.silver import weather_capture as capture_lock
    with _lock(root) as namespace:
        intent = identity.build_synthetic_contextual_intent(
            spark, config, silver_publication_id, publication_id, root,
            enrichment_version=enrichment_version)
        diagnostic, view = identity.enrich_synthetic_metadata(spark, config, silver_publication_id, root)
        iqr._require(diagnostic['silver'] == intent['silver'], 'Silver identity changed')
        iqr._require(diagnostic['metadata_files_sha256'] == intent['metadata_files_sha256'],
                     'Metadata files changed before materialization')
        iqr._require(iqr._canonical(view.schema.jsonValue()) == iqr._canonical(intent['schema']),
                     'Contextual schema differs')
        iqr._require(iqr._fingerprint(config.table_path('metadata')) == intent['metadata_files_sha256'],
                     'Metadata files changed during materialization')
        folder = namespace / publication_id
        silver._no_links(folder)
        captures = capture.namespace(root, config)
        capture_folder = captures / capture_id
        bindings = capture.isolated_namespace(root, config, 'contextual-capture-bindings')
        binding_path = bindings / (publication_id + '.json')

        def revalidate():
            identity.verify_synthetic_contextual_intent(
                spark, config, silver_publication_id, publication_id, root, intent,
                enrichment_version=enrichment_version)

        # A completed publication remains independently recoverable after retirement.
        if (not explicit_capture and folder.is_dir() and (folder / 'completed.json').is_file()
                and not capture_folder.exists()):
            silver._check_tree(folder)
            iqr._folder_layout(folder, 'table')
            iqr._require(iqr._canonical(iqr._read(folder / 'intent.json')) == iqr._canonical(intent),
                         'Contextual publication ID conflicts with intent')
            result = _verify_output(spark, folder / 'table', view, intent)
            iqr._require(iqr._canonical(iqr._read(folder / 'completed.json')) == iqr._canonical(result),
                         'Contextual completion evidence differs')
            revalidate()
            return result

        with capture_lock._lock(captures):
            if folder.exists():
                iqr._require(binding_path.is_file(), 'Contextual capture binding missing')
            if not capture_folder.exists():
                iqr._require(not folder.exists() and not binding_path.exists(),
                             'Capture missing for unfinished contextual publication')
                capture.create(spark, config, root, captures, capture_id, owner_id, intent, view)
            capture_result, output = capture.verify(
                spark, config, root, capture_folder, capture_id, owner_id, intent)
            capture_before = capture.fingerprint(capture_folder)
            binding = {'contract_version': 1, 'publication_id': publication_id,
                       'capture_id': capture_id, 'owner_id': owner_id,
                       'capture_evidence': capture_result, 'capture_tree_sha256': capture_before}
            bindings.mkdir(exist_ok=True)
            if binding_path.exists():
                saved_binding = iqr._read(binding_path)
                if 'legacy_verification_sha256' in saved_binding:
                    records = capture.isolated_namespace(root, config, 'contextual-legacy-recoveries')
                    verification = iqr._read(records / (publication_id + '.verified.json'))
                    iqr._require(iqr._digest(verification) == saved_binding['legacy_verification_sha256']
                                 and verification.get('recovery', {}).get('publication_id') == publication_id
                                 and verification.get('recovery', {}).get('intent_sha256') == iqr._digest(intent),
                                 'Legacy recovery evidence differs')
                    binding['legacy_verification_sha256'] = saved_binding['legacy_verification_sha256']
                iqr._require(iqr._canonical(saved_binding) == iqr._canonical(binding),
                             'Contextual capture binding differs')
            else:
                iqr._require(not folder.exists(), 'Contextual capture binding missing')
                capture._exclusive_json(binding_path, binding)

            def stable_capture():
                iqr._require(capture.fingerprint(capture_folder) == capture_before,
                             'Contextual capture changed during publication')
                iqr._require(iqr._canonical(iqr._read(binding_path)) == iqr._canonical(binding),
                             'Contextual capture binding changed')
                if 'legacy_verification_sha256' in binding:
                    iqr._require(iqr._digest(iqr._read(records / (publication_id + '.verified.json'))) ==
                                 binding['legacy_verification_sha256'], 'Legacy recovery evidence changed')
                actual, _ = capture.verify(spark, config, root, capture_folder, capture_id, owner_id, intent)
                iqr._require(iqr._canonical(actual) == iqr._canonical(capture_result),
                             'Contextual capture identity changed')

            revalidate()
            if not folder.exists():
                folder.mkdir()
                silver._exclusive_json(folder / 'intent.json', intent)
            else:
                silver._check_tree(folder)
                iqr._folder_layout(folder, 'table')
                iqr._require(iqr._canonical(iqr._read(folder / 'intent.json')) == iqr._canonical(intent),
                             'Contextual publication ID conflicts with intent')
            target, completed = folder / 'table', folder / 'completed.json'
            if not target.exists():
                iqr._require(not completed.exists(), 'Completion evidence exists without a table')
                stable_capture()
                revalidate()
                (output.write.format('delta').mode('errorifexists')
                 .option('userMetadata', iqr._canonical(intent)).save(str(target)))
            result = _verify_output(spark, target, output, intent)
            def stable_prior_table():
                if ('legacy_verification_sha256' in binding and
                        verification.get('prior_table_verification') is not None):
                    iqr._require(capture.fingerprint(target) == verification['prior_table_tree_sha256'],
                                 'Historical committed table identity changed')
            if 'legacy_verification_sha256' in binding and verification.get('prior_table_verification') is not None:
                iqr._require(iqr._canonical(result) == iqr._canonical(verification['prior_table_verification'])
                             and capture.fingerprint(target) == verification['prior_table_tree_sha256'],
                             'Historical committed table identity changed')
            stable_capture()
            revalidate()
            stable_prior_table()
            if completed.exists():
                iqr._require(iqr._canonical(iqr._read(completed)) == iqr._canonical(result),
                             'Contextual completion evidence differs')
            else:
                silver._exclusive_json(completed, result)
            stable_capture()
            revalidate()
            stable_prior_table()
            return result
