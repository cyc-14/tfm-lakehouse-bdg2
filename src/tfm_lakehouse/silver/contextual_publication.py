"""Explicit, recoverable publication of synthetic contextual snapshots."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path

from pyspark.sql import Row, functions as F

from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver


@contextmanager
def _lock(root):
    namespace = root / "contextual-publications"
    silver._no_links(namespace)
    namespace.mkdir(exist_ok=True)
    with silver._open_regular(namespace / ".publication.lock", os.O_RDWR | os.O_CREAT) as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise iqr.IQRPublicationError("Another local contextual publication is active") from exc
        try:
            yield namespace
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


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
    iqr._require(actual.schema.jsonValue() == expected_schema, "Contextual committed schema differs")
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
                                 enrichment_version: str = identity.ENRICHMENT_VERSION) -> dict:
    """Publish or explicitly reconcile one retained-source synthetic identity.

    Values are collected once on the driver and recreated from those immutable
    values, so Spark cannot recompute the source plan during the Delta write.
    This deliberately limits the API to small synthetic fixtures.
    """
    iqr._identifier(silver_publication_id)
    iqr._identifier(publication_id)
    root = _workspace(workspace, config)
    with _lock(root) as namespace:
        intent = identity.build_synthetic_contextual_intent(
            spark, config, silver_publication_id, publication_id, root,
            enrichment_version=enrichment_version)
        diagnostic, view = identity.enrich_synthetic_metadata(spark, config, silver_publication_id, root)
        iqr._require(diagnostic["silver"] == intent["silver"], "Silver identity changed")
        iqr._require(view.schema.jsonValue() == intent["schema"], "Contextual schema differs")
        zone = F.udf(identity._controlled_zone, identity._ZONE_EVIDENCE)(F.col("metadata_timezone"))
        measured = view.withColumn("__h06_zone_evidence", zone).collect()
        entries = []
        rows = []
        for row in measured:
            evidence = row["__h06_zone_evidence"]
            status = row["metadata_timezone_status"]
            if status not in ("missing_row", "missing_column"):
                iqr._require(status == evidence.status, "Timezone interpretation differs")
            entries.append(Row(metadata_timezone=row["metadata_timezone"], status=evidence.status,
                               resource=evidence.resource, sha256=evidence.sha256))
            rows.append(tuple(row[name] for name in view.columns))
        iqr._require(identity._merge_zone_evidence(entries) == intent["timezone_evidence"],
                     "Materialized timezone evidence differs")
        output = spark.createDataFrame(rows, view.schema)
        iqr._require(identity._multiset_hash(output) == intent["context_multiset_sha256"],
                     "Materialized contextual multiset differs")

        def revalidate():
            identity.verify_synthetic_contextual_intent(
                spark, config, silver_publication_id, publication_id, root, intent,
                enrichment_version=enrichment_version)

        revalidate()
        folder = namespace / publication_id
        silver._no_links(folder)
        if not folder.exists():
            folder.mkdir()
            silver._exclusive_json(folder / "intent.json", intent)
        else:
            silver._check_tree(folder)
            iqr._folder_layout(folder, "table")
            iqr._require(iqr._read(folder / "intent.json") == intent,
                         "Contextual publication ID conflicts with intent")
        target, completed = folder / "table", folder / "completed.json"
        if not target.exists():
            iqr._require(not completed.exists(), "Completion evidence exists without a table")
            revalidate()
            (output.write.format("delta").mode("errorifexists")
             .option("userMetadata", iqr._canonical(intent)).save(str(target)))
        result = _verify_output(spark, target, output, intent)
        revalidate()
        if completed.exists():
            iqr._require(iqr._read(completed) == result, "Contextual completion evidence differs")
        else:
            silver._exclusive_json(completed, result)
        revalidate()
        return result
