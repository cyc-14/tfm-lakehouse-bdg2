"""Read explicit synthetic contextual publications without recovery or writes."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path

from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver


@contextmanager
def _read_lock(root):
    """Share the existing writer lock without creating or changing evidence."""
    namespace = root / "contextual-publications"
    silver._check_tree(namespace)
    try:
        handle = silver._open_regular(namespace / ".publication.lock", os.O_RDONLY)
    except OSError as exc:
        raise iqr.IQRPublicationError("Contextual publication lock is unavailable") from exc
    with handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise iqr.IQRPublicationError("Another local contextual publication is active") from exc
        try:
            yield namespace
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_synthetic_contextual(spark, config, publication_id: str, workspace: Path):
    """Verify and return completion evidence and a lazy explicit Delta snapshot.

    Cooperating contextual writers are excluded while verification runs. The
    returned DataFrame remains lazy: its files and retained sources must remain
    available and unchanged after this function returns.
    """
    iqr._identifier(publication_id)
    root = publication._workspace(workspace, config)
    with _read_lock(root) as namespace:
        folder = namespace / publication_id
        silver._check_tree(folder)
        iqr._require(folder.is_dir(), "Contextual publication is unavailable")
        before = iqr._fingerprint(folder)
        iqr._folder_layout(folder, "table")
        intent = iqr._read(folder / "intent.json")
        marker = iqr._read(folder / "completed.json")
        iqr._require(type(intent.get("contract_version")) is int
                     and intent["contract_version"] == identity.INTENT_CONTRACT_VERSION,
                     "Unsupported contextual intent version")
        iqr._require(intent.get("publication_id") == publication_id,
                     "Contextual publication identity differs")
        parent = intent.get("silver")
        iqr._require(isinstance(parent, dict), "Silver identity is missing")
        silver_id = parent.get("publication_id")
        iqr._identifier(silver_id)
        identity.verify_synthetic_contextual_intent(
            spark, config, silver_id, publication_id, root, intent)
        diagnostic, expected = identity.enrich_synthetic_metadata(spark, config, silver_id, root)
        iqr._require(iqr._canonical(diagnostic["silver"]) == iqr._canonical(parent),
                     "Silver identity changed")
        iqr._require(diagnostic["metadata_files_sha256"] == intent.get("metadata_files_sha256"),
                     "Metadata evidence differs")
        iqr._require(iqr._canonical(expected.schema.jsonValue()) == iqr._canonical(intent.get("schema")),
                     "Contextual schema differs")
        result = publication._verify_output(spark, folder / "table", expected, intent)
        iqr._require(iqr._canonical(result) == iqr._canonical(marker),
                     "Contextual completion evidence differs")
        current, electrical = iqr._verify_silver(spark, config, root, silver_id)
        iqr._require(iqr._canonical(current) == iqr._canonical(parent), "Silver identity differs")
        actual = spark.read.format("delta").option("versionAsOf", 0).load(str(folder / "table"))
        projected = actual.select(*electrical.columns)
        iqr._require(not projected.exceptAll(electrical).limit(1).count()
                     and not electrical.exceptAll(projected).limit(1).count(),
                     "Electrical committed multiset differs")
        identity.verify_synthetic_contextual_intent(
            spark, config, silver_id, publication_id, root, intent)
        iqr._require(iqr._fingerprint(folder) == before, "Contextual publication changed")
        return result, actual
