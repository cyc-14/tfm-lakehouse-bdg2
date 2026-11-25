"""Read-only diagnostics for metadata bound to one synthetic Silver publication."""
from pyspark import StorageLevel
from pyspark.sql import functions as F

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.ingestion.snapshot import _read_verified_tables, _same_configuration, _validate_tables
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver


_FIELDS = ("building_id", "site_id", "sqm", "timezone", "primaryspaceusage",
           "sub_primaryspaceusage")


def diagnose_synthetic_metadata(spark, config, silver_publication_id, workspace):
    """Inspect metadata from the exact Bronze publication bound to Silver.

    This reports evidence and counts without joining, normalizing or writing.
    Bronze records no historical physical table UUID; H02 commit/version,
    schema and count checks cannot authenticate externally replaced storage.
    """
    root = iqr._workspace(workspace, config)
    identity, _ = iqr._verify_silver(spark, config, root, silver_publication_id)
    run_id = identity["source_run_id"]
    report, digest = silver._manifest(config, run_id)
    iqr._require(digest == identity["manifest_sha256"] and _same_configuration(report, config)
                 and report.get("status") == "passed" and report.get("run_id") == run_id,
                 "Bronze publication identity differs")
    manifest_path = config.namespace / "_runs" / (run_id + ".json")
    _validate_tables(report, config, manifest_path)
    iqr._require(silver._table_identity(report) == identity["source_tables"],
                 "Bronze table identities differ")
    item = next((row for row in report["tables"] if row["name"] == "metadata"), None)
    iqr._require(item is not None, "Metadata table is unavailable")
    metadata_fingerprint = iqr._fingerprint(config.table_path("metadata"))
    frame = _read_verified_tables(spark, config, report, manifest_path)["metadata"]
    with iqr._frame_cleanup() as frames:
        frames.append(("metadata snapshot", frame))
        frame = frame.persist(StorageLevel.DISK_ONLY)
        lineage = {"_bronze_source_uri": item["source_uri"],
                   "_bronze_source_sha256": item["source_sha256"],
                   "_bronze_data_kind": config.data_kind,
                   "_bronze_dataset_reference": config.dataset_reference}
        iqr._require(all(name in frame.columns for name in lineage), "Metadata lineage is missing")
        mismatch = None
        for name, value in lineage.items():
            condition = ~F.col(name).eqNullSafe(F.lit(value))
            mismatch = condition if mismatch is None else mismatch | condition
        iqr._require(not frame.where(mismatch).limit(1).count(), "Metadata lineage differs")
        available = {name: name in frame.columns for name in _FIELDS}
        iqr._require(available["building_id"], "Metadata building_id is unavailable")
        selected = F.col("building_id").isin(*APPROVED_BUILDINGS)
        cohorts = {"selected": selected, "unrelated": ~selected & F.col("building_id").isNotNull()
                   & (F.col("building_id") != "")}
        counts = {}
        for label, condition in cohorts.items():
            part = frame.where(condition)
            expressions = [F.count(F.lit(1)).alias("rows")]
            for index, name in enumerate(_FIELDS):
                if available[name]:
                    expressions.extend([
                        F.count(F.when(F.col(name).isNull(), 1)).alias(f"null_{index}"),
                        F.count(F.when(F.col(name) == "", 1)).alias(f"empty_{index}"),
                    ])
            summary = part.agg(*expressions).first().asDict()
            fields = {name: {"null": summary[f"null_{index}"], "empty": summary[f"empty_{index}"]}
                      for index, name in enumerate(_FIELDS) if available[name]}
            counts[label] = {"rows": summary["rows"], "fields": fields}
        counts["null_building_id"] = frame.where(F.col("building_id").isNull()).count()
        counts["empty_building_id"] = frame.where(F.col("building_id") == "").count()
        groups = (frame.where(F.col("building_id").isNotNull() & (F.col("building_id") != ""))
                  .groupBy("building_id").agg(F.count(F.lit(1)).alias("rows"),
                     F.countDistinct(F.struct(*[F.col('`' + name.replace('`', '``') + '`')
                                               for name in frame.columns])).alias("variants")))
        duplicate_counts = {}
        for label, condition in cohorts.items():
            subset = groups.where(condition & (F.col("rows") > 1))
            duplicate_counts[label] = {"identical": subset.where(F.col("variants") == 1).count(),
                                       "conflicting": subset.where(F.col("variants") > 1).count()}
        present = {row.building_id for row in groups.where(selected).select("building_id").collect()}
        # Recheck the retained version and manifest after the Spark actions.
        current = _read_verified_tables(spark, config, report, manifest_path)["metadata"]
        iqr._require(not frame.exceptAll(current).limit(1).count()
                     and not current.exceptAll(frame).limit(1).count(), "Metadata snapshot changed")
        iqr._require(iqr._fingerprint(config.table_path("metadata")) == metadata_fingerprint,
                     "Metadata files changed")
        iqr._stable_silver(identity, config)
        return {"silver": identity, "bronze_run_id": run_id, "manifest_sha256": digest,
                "metadata_version": item["delta_version"], "metadata_source_sha256": item["source_sha256"],
                "metadata_files_sha256": metadata_fingerprint,
                "available_columns": available, "counts": counts, "duplicates": duplicate_counts,
                "missing_buildings": [name for name in APPROVED_BUILDINGS if name not in present],
                "expected_join_cardinality": "at_most_one_metadata_row_per_building",
                "historical_bronze_uuid": "not_recorded"}
