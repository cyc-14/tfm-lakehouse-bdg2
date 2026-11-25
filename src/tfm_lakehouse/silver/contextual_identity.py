"""Verified synthetic contextual intent without publication side effects."""
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, _common, _tzpath

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver.metadata_enrichment import enrich_synthetic_metadata

ENRICHMENT_VERSION = "h06-metadata-v1"
_ZONE_EVIDENCE = StructType([StructField("status", StringType(), False),
                             StructField("resource", StringType(), True),
                             StructField("sha256", StringType(), True)])


def _controlled_zone(name):
    """Resolve using zoneinfo's precedence and construct from the hashed bytes."""
    if name is None:
        return "null", None, None
    if name == "":
        return "empty", None, None
    try:
        path = _tzpath.find_tzfile(name)
        if path is not None:
            with open(path, "rb") as stream:
                content = stream.read()
            resource = "file:" + str(Path(path))
        else:
            with _common.load_tzdata(name) as stream:
                content = stream.read()
            resource = "tzdata:" + name
    except (ZoneInfoNotFoundError, ValueError):
        return "unknown", None, None
    # Malformed or unreadable resources are not equivalent to unknown names.
    ZoneInfo.from_file(BytesIO(content), key=name)
    return "valid", resource, sha256(content).hexdigest()


def _multiset_hash(frame):
    # JSON preserves nulls; counting sorted complete rows preserves multiplicity.
    encoded = frame.select(F.to_json(F.struct(*[F.col('`' + name.replace('`', '``') + '`')
                                              for name in frame.columns]),
                                     {"ignoreNullFields": "false"}).alias("row"))
    groups = encoded.groupBy("row").count().orderBy("row")
    digest = sha256()
    for row in groups.toLocalIterator():
        payload = json.dumps([row.row, row["count"]], ensure_ascii=True,
                             separators=(",", ":")).encode()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _merge_zone_evidence(entries):
    evidence = {}
    for row in entries:
        key = row.metadata_timezone
        item = {"status": row.status, "resource": row.resource, "sha256": row.sha256}
        iqr._require(key not in evidence or evidence[key] == item, "Timezone worker evidence differs")
        evidence[key] = item
    return [{"timezone": key, **value} for key, value in
            sorted(evidence.items(), key=lambda item: (item[0] is not None, item[0] or ""))]


def build_synthetic_contextual_intent(spark, config, silver_publication_id, publication_id,
                                      workspace, *, enrichment_version=ENRICHMENT_VERSION):
    """Build verifiable intent from the existing contextual view; write nothing."""
    iqr._identifier(publication_id)
    iqr._require(enrichment_version == ENRICHMENT_VERSION, "Unsupported enrichment version")
    diagnostic, view = enrich_synthetic_metadata(spark, config, silver_publication_id, workspace)
    iqr._require("__h06_zone_evidence" not in view.columns, "Reserved evidence field exists")
    zone = F.udf(_controlled_zone, _ZONE_EVIDENCE)(F.col("metadata_timezone"))
    measured = view.withColumn("__h06_zone_evidence", zone).persist(StorageLevel.DISK_ONLY)
    controlled = None
    try:
        missing = F.col("metadata_timezone_status").isin("missing_row", "missing_column")
        status = F.when(missing, F.col("metadata_timezone_status")).otherwise(
            F.col("__h06_zone_evidence.status"))
        controlled = measured.withColumn("metadata_timezone_status", status).drop("__h06_zone_evidence")
        iqr._require([(f.name, f.dataType) for f in controlled.schema] ==
                     [(f.name, f.dataType) for f in view.schema], "Contextual schema differs")
        # Spark's projection changes nullability metadata; restore the verified
        # input schema while retaining and comparing every materialized value.
        controlled = spark.createDataFrame(controlled.rdd, view.schema)
        iqr._require(controlled.schema.jsonValue() == view.schema.jsonValue(), "Contextual schema differs")
        iqr._require(not controlled.exceptAll(view).limit(1).count()
                     and not view.exceptAll(controlled).limit(1).count(), "Timezone interpretation differs")
        entries = (measured.select("metadata_timezone", "__h06_zone_evidence.*").distinct().collect())
        ordered = _merge_zone_evidence(entries)
        result_hash = _multiset_hash(controlled)
        identity = diagnostic["silver"]
        root = iqr._workspace(workspace, config)
        iqr._stable_silver(identity, config)
        iqr._require(iqr._fingerprint(config.table_path("metadata")) == diagnostic["metadata_files_sha256"],
                     "Metadata files changed")
        return {"contract_version": 1, "publication_id": publication_id,
                "enrichment_version": enrichment_version, "silver": identity,
                "bronze_run_id": diagnostic["bronze_run_id"],
                "manifest_sha256": diagnostic["manifest_sha256"],
                "metadata_version": diagnostic["metadata_version"],
                "metadata_source_sha256": diagnostic["metadata_source_sha256"],
                "timezone_evidence": ordered, "schema": view.schema.jsonValue(),
                "context_multiset_sha256": result_hash}
    finally:
        measured.unpersist()


def verify_synthetic_contextual_intent(spark, config, silver_publication_id, publication_id,
                                       workspace, expected, *, enrichment_version=ENRICHMENT_VERSION):
    """Rebuild and compare every intent field, rejecting changed evidence."""
    actual = build_synthetic_contextual_intent(
        spark, config, silver_publication_id, publication_id, workspace,
        enrichment_version=enrichment_version)
    iqr._require(actual == expected, "Contextual intent or timezone evidence differs")
    return actual
