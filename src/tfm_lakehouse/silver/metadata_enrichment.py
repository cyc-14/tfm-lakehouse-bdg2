"""Non-persistent synthetic metadata enrichment bound to a Silver snapshot."""
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.ingestion.snapshot import _read_verified_tables
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.metadata_diagnostics import diagnose_synthetic_metadata
from tfm_lakehouse.silver.silver_electricity_numeric import _interpret

_CONTEXT = ("site_id", "sqm", "timezone", "primaryspaceusage", "sub_primaryspaceusage")
_OUTPUT = ("metadata_available", "metadata_site_id", "metadata_site_id_status", "metadata_sqm_original",
           "metadata_sqm_numeric", "metadata_sqm_status", "metadata_timezone",
           "metadata_timezone_status", "metadata_primaryspaceusage", "metadata_primaryspaceusage_status",
           "metadata_sub_primaryspaceusage", "metadata_sub_primaryspaceusage_status")
_AREA_TYPE = StructType([StructField("value", DecimalType(38, 18)),
                         StructField("status", StringType(), False)])


def _area(value):
    if value is None:
        return None, "null"
    if value == "":
        return None, "empty"
    if value in ("NaN", "Infinity", "+Infinity", "-Infinity"):
        return None, "nonfinite"
    number, status, zero, negative = _interpret(value)
    if status in ("overflow", "underflow", "scale_exceeded"):
        return None, "negative" if value.startswith("-") else "positive_nonrepresentable"
    if status != "finite":
        return None, "invalid"
    if zero:
        return None, "zero"
    if negative:
        return None, "negative"
    return number, "valid"


def _timezone(value):
    if value is None:
        return "null"
    if value == "":
        return "empty"
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        return "unknown"
    return "valid"


def enrich_synthetic_metadata(spark, config, silver_publication_id, workspace):
    """Return diagnostics and a lazy, verified view; retain unchanged source files."""
    diagnostic = diagnose_synthetic_metadata(spark, config, silver_publication_id, workspace)
    duplicates = diagnostic["duplicates"]["selected"]
    iqr._require(not duplicates["identical"] and not duplicates["conflicting"],
                 "Selected metadata keys are duplicated")
    root = iqr._workspace(workspace, config)
    identity, electricity = iqr._verify_silver(spark, config, root, silver_publication_id)
    iqr._require(identity == diagnostic["silver"], "Silver identity changed")
    report, digest = silver._manifest(config, identity["source_run_id"])
    iqr._require(digest == diagnostic["manifest_sha256"], "Bronze manifest changed")
    path = config.namespace / "_runs" / (identity["source_run_id"] + ".json")
    fingerprint = iqr._fingerprint(config.table_path("metadata"))
    iqr._require(fingerprint == diagnostic["metadata_files_sha256"], "Metadata files changed")
    metadata = _read_verified_tables(spark, config, report, path)["metadata"]
    item = next(row for row in report["tables"] if row["name"] == "metadata")
    lineage = {"_bronze_source_uri": item["source_uri"],
               "_bronze_source_sha256": item["source_sha256"],
               "_bronze_data_kind": config.data_kind,
               "_bronze_dataset_reference": config.dataset_reference}
    mismatch = None
    for name, value in lineage.items():
        iqr._require(name in metadata.columns, "Metadata lineage is missing")
        condition = ~F.col(name).eqNullSafe(F.lit(value))
        mismatch = condition if mismatch is None else mismatch | condition
    iqr._require(not metadata.where(mismatch).limit(1).count(), "Metadata lineage differs")
    selected = metadata.where(F.col("building_id").isin(*APPROVED_BUILDINGS))
    iqr._require(not selected.groupBy("building_id").count().where(F.col("count") > 1).limit(1).count(),
                 "Selected metadata keys are duplicated")
    iqr._require(not ({name.casefold() for name in electricity.columns} &
                      {name.casefold() for name in (*_OUTPUT, "__metadata_key", "__metadata_area",
                                                    "__metadata_zone")}), "Metadata output column collision")
    def source(name):
        return F.col(name) if name in metadata.columns else F.lit(None).cast("string")
    area = F.udf(_area, _AREA_TYPE)(source("sqm"))
    zone = F.udf(_timezone, StringType())(source("timezone"))
    context = selected.select(F.col("building_id").alias("__metadata_key"),
                              F.lit(True).alias("metadata_available"),
                              source("site_id").alias("metadata_site_id"),
                              source("sqm").alias("metadata_sqm_original"),
                              area.alias("__metadata_area"),
                              source("timezone").alias("metadata_timezone"),
                              zone.alias("__metadata_zone"),
                              source("primaryspaceusage").alias("metadata_primaryspaceusage"),
                              source("sub_primaryspaceusage").alias("metadata_sub_primaryspaceusage"))
    joined = electricity.join(context, electricity.building_id == context["__metadata_key"], "left")
    present = F.col("metadata_available").isNotNull()
    def text_status(source_name, output_name):
        status = F.when(~present, "missing_row")
        if source_name not in metadata.columns:
            return status.otherwise("missing_column").alias(output_name + "_status")
        return (status.when(F.col(output_name).isNull(), "null")
                .when(F.col(output_name) == "", "empty").otherwise("present")
                .alias(output_name + "_status"))
    area_status = (F.when(~present, "missing_row") if "sqm" in metadata.columns
                   else F.when(~present, "missing_row").otherwise("missing_column"))
    zone_status = (F.when(~present, "missing_row") if "timezone" in metadata.columns
                   else F.when(~present, "missing_row").otherwise("missing_column"))
    result = joined.select(*[F.col('`' + c.replace('`', '``') + '`') for c in electricity.columns],
        F.coalesce(F.col("metadata_available"), F.lit(False)).alias("metadata_available"),
        "metadata_site_id", text_status("site_id", "metadata_site_id"),
        "metadata_sqm_original", F.col("__metadata_area.value").alias("metadata_sqm_numeric"),
        area_status.otherwise(F.col("__metadata_area.status")).alias("metadata_sqm_status") if "sqm" in metadata.columns else area_status.alias("metadata_sqm_status"),
        "metadata_timezone", zone_status.otherwise(F.col("__metadata_zone")).alias("metadata_timezone_status") if "timezone" in metadata.columns else zone_status.alias("metadata_timezone_status"),
        "metadata_primaryspaceusage", text_status("primaryspaceusage", "metadata_primaryspaceusage"),
        "metadata_sub_primaryspaceusage", text_status("sub_primaryspaceusage", "metadata_sub_primaryspaceusage"))
    original = result.select(*[F.col('`' + c.replace('`', '``') + '`') for c in electricity.columns])
    iqr._require(not original.exceptAll(electricity).limit(1).count()
                 and not electricity.exceptAll(original).limit(1).count(), "Electricity multiset differs")
    quality = result.groupBy("building_id", "metadata_available", "metadata_sqm_status",
                             "metadata_timezone_status", "metadata_site_id_status",
                             "metadata_primaryspaceusage_status", "metadata_sub_primaryspaceusage_status").count().collect()
    diagnostic["building_quality"] = [row.asDict() for row in quality]
    iqr._require(iqr._fingerprint(config.table_path("metadata")) == fingerprint,
                 "Metadata files changed")
    iqr._stable_silver(identity, config)
    return diagnostic, result
