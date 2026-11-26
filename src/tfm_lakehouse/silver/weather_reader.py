"""Read an explicitly selected synthetic weather publication without recovery."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_capture as capture
from tfm_lakehouse.silver import weather_publication as publication
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint


@contextmanager
def _read_lock(namespace):
    """Open an existing cooperative lock read-only, preserving cleanup errors."""
    silver._check_tree(namespace)
    try:
        handle = silver._open_regular(namespace / ".capture.lock", os.O_RDONLY)
    except OSError as exc:
        raise iqr.IQRPublicationError("Weather read lock is unavailable") from exc
    primary = None
    acquired = False
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as exc:
            raise iqr.IQRPublicationError("Another weather operation is active") from exc
        yield
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
                capture._record_cleanup_failure(primary, exc)
        elif failures:
            for exc in failures[1:]:
                capture._record_cleanup_failure(failures[0], exc)
            raise failures[0]


def read_synthetic_weather(spark, config, publication_id: str, workspace: Path):
    """Verify one explicit publication and return evidence and lazy Delta v0.

    Locks are held in publication-then-capture order only during verification.
    The caller must keep the publication, capture and retained sources unchanged
    and available during every later action on the returned DataFrame.
    """
    iqr._identifier(publication_id)
    root, namespace = publication._namespace(workspace, config)
    capture_root, capture_namespace = capture._namespace(workspace, config)
    iqr._require(root == capture_root, "Capture workspace differs")
    with _read_lock(namespace):
        with _read_lock(capture_namespace):
            folder = namespace / publication_id
            silver._check_tree(folder)
            iqr._require(folder.is_dir(), "Weather publication unavailable")
            iqr._require({p.name for p in folder.iterdir()} == {"intent.json", "table", "completed.json"},
                         "Weather publication is incomplete or has unknown evidence")
            before = weather_tree_fingerprint(folder)
            intent = iqr._read(folder / "intent.json")
            marker = iqr._read(folder / "completed.json")
            iqr._require(type(intent.get("contract_version")) is int
                         and intent["contract_version"] == publication.PUBLICATION_VERSION
                         and intent.get("publication_id") == publication_id,
                         "Weather publication identity or contract differs")
            capture_id, owner_id = intent.get("capture_id"), intent.get("owner_id")
            iqr._identifier(capture_id)
            iqr._identifier(owner_id)
            capture_folder, evidence, capture_hash = publication._verified_capture(
                spark, config, root, capture_namespace, capture_id, owner_id)
            iqr._require(evidence["intent"]["weather_identity"]["publication_id"] == publication_id,
                         "Publication ID differs from capture intention")
            expected = {"contract_version": publication.PUBLICATION_VERSION, "publication_id": publication_id,
                        "capture_id": capture_id, "owner_id": owner_id,
                        "capture_folder_sha256": capture_hash, "capture": evidence}
            iqr._require(iqr._canonical(intent) == iqr._canonical(expected), "Weather publication intent differs")
            table = folder / "table"
            result = publication._verify_table(spark, config, root, table, intent, capture_folder, evidence)
            iqr._require(iqr._canonical(result) == iqr._canonical(marker), "Weather completion evidence differs")
            publication._stable_capture(spark, config, root, capture_folder, capture_hash, evidence)
            iqr._require(weather_tree_fingerprint(folder) == before, "Weather publication evidence changed")
            frame = spark.read.format("delta").option("versionAsOf", 0).load(str(table))
            iqr._require(weather_tree_fingerprint(folder) == before, "Weather publication evidence changed")
            return result, frame
