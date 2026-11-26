"""Distributed synthetic quality, coverage and nominal tolerance diagnostics."""
from functools import reduce

from pyspark.sql import Window, functions as F
from pyspark.sql.types import LongType

from tfm_lakehouse.ingestion.snapshot import _read_verified_tables
from tfm_lakehouse.silver import contextual_reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.weather_enrichment import _associate, _nominal_seconds
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint
from tfm_lakehouse.weather_characterization import VARIABLES

TOLERANCES = (0, 900, 1800, 3600)
QUALITY_STATES = ("column_missing", "null", "empty", "missing_token", "invalid_syntax",
                  "nonfinite", "overflow", "underflow", "scale_exceeded", "finite")


def _period(frame):
    nominal = F.udf(_nominal_seconds, LongType())(F.col("timestamp_original"))
    return frame.withColumn("__period", F.when(nominal.isNotNull(),
                                               F.substring("timestamp_original", 1, 7))
                            .otherwise("unassigned"))


def _dimensions(scope):
    return (["building_id", "metadata_site_id", "__period"] if scope == "building"
            else ["metadata_site_id", "__period"])


def _groups(frame, scope):
    status = F.col("weather_association_status")
    associated = status == "associated_unique"
    eligible = status.isin("associated_unique", "missing_observation", "duplicate_identical",
                           "duplicate_conflicting")
    duplicate = status.isin("duplicate_identical", "duplicate_conflicting")
    key = F.struct("metadata_site_id", "timestamp_original")
    weather_key = F.struct("metadata_site_id", "weather_timestamp_original")
    def count(condition):
        return F.sum(F.when(condition, 1).otherwise(0)).cast("long")
    grouped = frame.groupBy(*_dimensions(scope)).agg(
        F.count(F.lit(1)).alias("electrical_rows"), count(eligible).alias("eligible_rows"),
        count(status == "missing_site").alias("missing_site_rows"),
        count(status == "invalid_site").alias("invalid_site_rows"),
        count(status == "invalid_timestamp").alias("invalid_timestamp_rows"),
        count(associated & (F.col("weather_offset_seconds") == 0)).alias("exact_rows"),
        count(associated & (F.col("weather_offset_seconds") < 0)).alias("earlier_rows"),
        count(associated & (F.col("weather_offset_seconds") > 0)).alias("later_rows"),
        count(status == "missing_observation").alias("unmatched_rows"),
        count(status == "duplicate_identical").alias("duplicate_identical_rows"),
        count(status == "duplicate_conflicting").alias("duplicate_conflicting_rows"),
        F.countDistinct(F.when(eligible, key)).alias("eligible_electrical_keys"),
        F.countDistinct(F.when(associated, key)).alias("associated_electrical_keys"),
        F.countDistinct(F.when(associated & (F.col("weather_offset_seconds") == 0), key)).alias("exact_electrical_keys"),
        F.countDistinct(F.when(associated & (F.col("weather_offset_seconds") != 0), key)).alias("approximate_electrical_keys"),
        F.countDistinct(F.when(duplicate, key)).alias("rejected_electrical_keys"),
        F.countDistinct(F.when(status == "missing_observation", key)).alias("unmatched_electrical_keys"),
        F.countDistinct(F.when(associated, weather_key)).alias("associated_weather_keys"),
        F.countDistinct(F.when(duplicate, weather_key)).alias("rejected_weather_keys"),
        F.min(F.when(associated, F.col("weather_offset_seconds"))).alias("offset_min_seconds"),
        F.max(F.when(associated, F.col("weather_offset_seconds"))).alias("offset_max_seconds"),
        F.avg(F.when(associated, F.col("weather_offset_seconds"))).alias("offset_mean_seconds"),
        F.min(F.when(associated, F.col("weather_distance_seconds"))).alias("distance_min_seconds"),
        F.max(F.when(associated, F.col("weather_distance_seconds"))).alias("distance_max_seconds"),
        F.avg(F.when(associated, F.col("weather_distance_seconds"))).alias("distance_mean_seconds"))
    matched = F.col("exact_rows") + F.col("earlier_rows") + F.col("later_rows")
    return (grouped.withColumn("associated_rows", matched)
            .withColumn("approximate_rows", F.col("earlier_rows") + F.col("later_rows"))
            .withColumn("row_coverage", F.when(F.col("eligible_rows") > 0,
                                                 matched / F.col("eligible_rows")))
            .withColumn("overall_row_coverage", F.when(F.col("electrical_rows") > 0,
                                                         matched / F.col("electrical_rows")))
            .withColumn("label_coverage", F.when(F.col("eligible_electrical_keys") > 0,
                F.col("associated_electrical_keys") / F.col("eligible_electrical_keys")))
            .withColumn("scope", F.lit(scope))
            .withColumn("period", F.col("__period")).drop("__period")
            .withColumn("building_id", F.col("building_id") if scope == "building"
                        else F.lit(None).cast("string")))


def _coverage(frame):
    frame = _period(frame)
    building = _groups(frame, "building")
    site = _groups(frame, "site")
    return building.unionByName(site)


def _variables(frame):
    items = [F.struct(F.lit(name).alias("variable"),
                      F.col(f"weather_{name}_status").alias("quality"),
                      F.col(f"weather_{name}_potential_special_code").alias("special"))
             for name in VARIABLES]
    values = _period(frame).withColumn("__variable", F.explode(F.array(*items)))
    outputs = []
    for scope in ("building", "site"):
        state = F.col("__variable.quality")
        associated = F.col("weather_association_status") == "associated_unique"
        available = associated & ~state.isin("column_missing", "null", "empty", "missing_token")
        interpretable = associated & (state == "finite")
        ordinary_candidate = interpretable & ~F.col("__variable.special")
        key = F.struct("metadata_site_id", "weather_timestamp_original")
        electrical_key = F.struct("metadata_site_id", "timestamp_original")
        eligible = F.col("weather_association_status").isin(
            "associated_unique", "missing_observation", "duplicate_identical", "duplicate_conflicting")
        def count(condition):
            return F.sum(F.when(condition, 1).otherwise(0)).cast("long")
        output = values.groupBy(*_dimensions(scope), F.col("__variable.variable").alias("variable")).agg(
            F.count(F.lit(1)).alias("electrical_rows"), count(associated).alias("associated_rows"),
            count(~associated).alias("not_associated_rows"), count(available).alias("available_rows"),
            count(interpretable).alias("numerically_interpretable_rows"),
            count(ordinary_candidate).alias("finite_without_special_rows"),
            count(associated & F.col("__variable.special")).alias("potential_special_code_rows"),
            F.countDistinct(F.when(eligible, electrical_key)).alias("eligible_electrical_keys"),
            F.countDistinct(F.when(associated, electrical_key)).alias("associated_electrical_keys"),
            F.countDistinct(F.when(associated, key)).alias("associated_weather_keys"),
            F.countDistinct(F.when(available, key)).alias("available_weather_keys"),
            F.countDistinct(F.when(interpretable, key)).alias("interpretable_weather_keys"),
            F.countDistinct(F.when(associated & F.col("__variable.special"), key)).alias("potential_special_code_weather_keys"),
            F.countDistinct(F.when(ordinary_candidate, key)).alias("finite_without_special_weather_keys"),
            *[count(associated & (state == label)).alias(label + "_rows") for label in QUALITY_STATES])
        output = (output.withColumn("available_ratio", F.when(F.col("associated_rows") > 0,
                                   F.col("available_rows") / F.col("associated_rows")))
                  .withColumn("interpretable_ratio", F.when(F.col("associated_rows") > 0,
                                   F.col("numerically_interpretable_rows") / F.col("associated_rows")))
                  .withColumn("finite_without_special_ratio", F.when(F.col("associated_rows") > 0,
                                   F.col("finite_without_special_rows") / F.col("associated_rows")))
                  .withColumn("scope", F.lit(scope)).withColumn("period", F.col("__period")).drop("__period")
                  .withColumn("building_id", F.col("building_id") if scope == "building"
                              else F.lit(None).cast("string")))
        outputs.append(output)
    return outputs[0].unionByName(outputs[1])


def _sensitivity(context, weather):
    reports = [_coverage(_associate(context, weather, tolerance_seconds=seconds))
               .withColumn("tolerance_seconds", F.lit(seconds)) for seconds in TOLERANCES]
    combined = reduce(lambda left, right: left.unionByName(right), reports)
    partition = Window.partitionBy("scope", "building_id", "metadata_site_id", "period").orderBy("tolerance_seconds")
    rejected = F.col("duplicate_identical_rows") + F.col("duplicate_conflicting_rows")
    return (combined.withColumn("rejected_duplicate_rows", rejected)
            .withColumn("associated_rows_gain", F.col("associated_rows") - F.lag("associated_rows").over(partition))
            .withColumn("rejected_duplicate_rows_change", rejected - F.lag("rejected_duplicate_rows").over(partition))
            .withColumn("row_coverage_gain", F.col("row_coverage") - F.lag("row_coverage").over(partition))
            .withColumn("label_coverage_gain", F.col("label_coverage") - F.lag("label_coverage").over(partition)))


def diagnose_synthetic_weather_quality(spark, config, silver_publication_id, contextual_publication_id, workspace):
    """Return verified source evidence and lazy, distributed diagnostics.

    No results or examples are collected. The retained sources must remain
    unchanged while callers evaluate the returned DataFrames.
    """
    from tfm_lakehouse.silver.weather_enrichment import associate_synthetic_weather

    evidence, association = associate_synthetic_weather(
        spark, config, silver_publication_id, contextual_publication_id, workspace)
    contextual, context = contextual_reader.read_synthetic_contextual(spark, config, contextual_publication_id, workspace)
    iqr._require(contextual == evidence["contextual"], "Contextual publication changed")
    report, digest = silver._manifest(config, evidence["silver"]["source_run_id"])
    iqr._require(digest == evidence["manifest_sha256"], "Bronze manifest changed")
    weather_path = config.table_path("weather")
    iqr._require(weather_tree_fingerprint(weather_path) == evidence["weather_files_sha256"], "Weather source changed")
    report_path = config.namespace / "_runs" / (evidence["silver"]["source_run_id"] + ".json")
    weather = _read_verified_tables(spark, config, report, report_path)["weather"]
    sensitivity = _sensitivity(context, weather)
    iqr._require(weather_tree_fingerprint(weather_path) == evidence["weather_files_sha256"], "Weather source changed")
    iqr._stable_silver(evidence["silver"], config)
    return evidence, {"coverage": _coverage(association), "variables": _variables(association),
                      "sensitivity": sensitivity}
