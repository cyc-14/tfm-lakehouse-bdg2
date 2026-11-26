"""Synthetic weather association on nominal labels, without physical time inference."""
from pyspark.sql import Window, functions as F
from pyspark.sql.types import BooleanType, DecimalType, LongType, StringType, StructField, StructType

from tfm_lakehouse.ingestion.snapshot import _read_verified_tables
from tfm_lakehouse.silver import contextual_reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.silver_electricity_temporal import _interpret
from tfm_lakehouse.silver.silver_electricity_numeric import _interpret as _numeric
from tfm_lakehouse.silver.weather_diagnostics import _special
from tfm_lakehouse.weather_characterization import VARIABLES

ASSOCIATION_POLICY = "h06-nominal-nearest-1800-earlier-v1"
MAX_DISTANCE_SECONDS = 1800
_QUALITY = StructType([StructField("value", DecimalType(38, 18)),
                       StructField("status", StringType(), False),
                       StructField("special", BooleanType(), False)])


def _quality(value, variable):
    if value is None:
        return None, "null", False
    if value == "":
        return None, "empty", False
    if value == "NaN":
        return None, "missing_token", False
    number, status, _, _ = _numeric(value)
    if status == "nonfinite_literal":
        status = "nonfinite"
    return number, status, status == "finite" and _special(value, variable)


def _nominal_seconds(value):
    """Convert a valid calendar label to an ordinal, never to a physical instant."""
    day, second, status = _interpret(value)
    return day.toordinal() * 86400 + second if status == "valid_nominal" else None


def _associate(context, weather, *, tolerance_seconds=MAX_DISTANCE_SECONDS):
    """Build a lazy association; inputs must already have verified identities."""
    if tolerance_seconds not in (0, 900, 1800, 3600):
        raise ValueError("Unsupported diagnostic tolerance")
    reserved = {"weather_association_status", "weather_key_row_count",
                "weather_duplicate_kind", "weather_timestamp_original",
                "weather_offset_seconds", "weather_distance_seconds", "weather_match_type"}
    reserved.update(f"weather_{name}_original" for name in VARIABLES)
    reserved.update(f"weather_{name}_{suffix}" for name in VARIABLES
                    for suffix in ("numeric", "status", "potential_special_code"))
    iqr._require(not ({c.casefold() for c in context.columns} &
                      {c.casefold() for c in reserved}), "Weather output column collision")
    iqr._require(len({c.casefold() for c in weather.columns}) == len(weather.columns),
                 "Ambiguous weather columns")
    iqr._require({"site_id", "timestamp"}.issubset(weather.columns), "Weather keys are missing")
    iqr._require(all(isinstance(weather.schema[c].dataType, StringType)
                     for c in ("site_id", "timestamp", *[x for x in VARIABLES if x in weather.columns])),
                 "Weather columns must be strings")
    ordinal = F.udf(_nominal_seconds, LongType())
    groups = weather.groupBy("site_id", "timestamp").agg(
        F.count(F.lit(1)).alias("__count"),
        F.countDistinct(F.struct(*[F.col('`' + c.replace('`', '``') + '`')
                                   for c in weather.columns])).alias("__variants"),
        *[F.max(F.col(name)).alias("__value_" + name) for name in VARIABLES if name in weather.columns],
    ).withColumn("__weather_seconds", ordinal(F.col("timestamp")))
    groups = groups.where(F.col("__weather_seconds").isNotNull() & F.col("site_id").isNotNull()
                          & ~F.col("site_id").rlike(r"^\s*$"))
    electric = (context.select(F.col("metadata_site_id").alias("__site"),
                               F.col("timestamp_original").alias("__label"))
                .distinct().withColumn("__electric_seconds", ordinal(F.col("__label")))
                .where(F.col("__electric_seconds").isNotNull() & F.col("__site").isNotNull()
                       & ~F.col("__site").rlike(r"^\s*$")))
    candidates = electric.join(groups, (electric.__site == groups.site_id) &
        (F.abs(F.col("__weather_seconds") - F.col("__electric_seconds")) <= tolerance_seconds), "inner")
    order = Window.partitionBy("__site", "__label").orderBy(
        F.abs(F.col("__weather_seconds") - F.col("__electric_seconds")), F.col("__weather_seconds"))
    chosen = candidates.withColumn("__rank", F.row_number().over(order)).where(F.col("__rank") == 1)
    chosen = chosen.select("__site", "__label", "timestamp", "__count", "__variants",
                           (F.col("__weather_seconds") - F.col("__electric_seconds")).alias("__offset"),
                           *["__value_" + name for name in VARIABLES if name in weather.columns])
    base = context.alias("c").join(chosen.alias("w"),
        (F.col("c.metadata_site_id") == F.col("w.__site")) &
        (F.col("c.timestamp_original") == F.col("w.__label")), "left")
    missing_site = F.col("c.metadata_site_id").isNull() | (F.col("c.metadata_site_id") == "")
    invalid_site = ~missing_site & F.col("c.metadata_site_id").rlike(r"^\s*$")
    invalid_time = ordinal(F.col("c.timestamp_original")).isNull()
    duplicate = F.col("w.__count") > 1
    status = (F.when(missing_site, "missing_site").when(invalid_site, "invalid_site")
              .when(invalid_time, "invalid_timestamp").when(F.col("w.__count").isNull(), "missing_observation")
              .when(duplicate & (F.col("w.__variants") == 1), "duplicate_identical")
              .when(duplicate, "duplicate_conflicting").otherwise("associated_unique"))
    unique = status == "associated_unique"
    fields = [F.col('c.`' + c.replace('`', '``') + '`') for c in context.columns]
    fields += [status.alias("weather_association_status"), F.col("w.__count").alias("weather_key_row_count"),
        F.when(duplicate, F.when(F.col("w.__variants") == 1, "identical").otherwise("conflicting"))
         .alias("weather_duplicate_kind"), F.col("w.timestamp").alias("weather_timestamp_original"),
        F.col("w.__offset").alias("weather_offset_seconds"), F.abs(F.col("w.__offset")).alias("weather_distance_seconds"),
        F.when(F.col("w.__count").isNotNull(),
               F.when(F.col("w.__offset") == 0, "exact").otherwise("approximate"))
         .alias("weather_match_type")]
    for name in VARIABLES:
        value = F.col("w.__value_" + name) if name in weather.columns else F.lit(None).cast("string")
        fields.append(F.when(unique, value).alias(f"weather_{name}_original"))
        quality = F.udf(lambda token, variable=name: _quality(token, variable), _QUALITY)(value)
        fields.append(F.when(unique, quality.value).alias(f"weather_{name}_numeric"))
        state = F.when(~unique, "not_associated")
        if name not in weather.columns:
            state = state.otherwise("column_missing")
        else:
            state = state.otherwise(quality.status)
        fields.append(state.alias(f"weather_{name}_status"))
        fields.append(F.coalesce(F.when(unique, quality.special), F.lit(False))
                      .alias(f"weather_{name}_potential_special_code"))
    return base.select(*fields)


def associate_synthetic_weather(spark, config, silver_publication_id, contextual_publication_id, workspace):
    """Verify sources and return evidence and a lazy synthetic association.

    Returned data and sources must remain available and unchanged through every
    subsequent Spark action. This view is not a durable D36 capture.
    """
    root = iqr._workspace(workspace, config)
    contextual_evidence, context = contextual_reader.read_synthetic_contextual(
        spark, config, contextual_publication_id, root)
    identity, electrical = iqr._verify_silver(spark, config, root, silver_publication_id)
    iqr._require(contextual_evidence["intent"]["silver"] == identity, "Contextual Silver identity differs")
    report, digest = silver._manifest(config, identity["source_run_id"])
    iqr._require(digest == identity["manifest_sha256"], "Bronze manifest differs")
    items = {item["name"]: item for item in report["tables"]}
    iqr._require("weather" in items, "Weather source is unavailable")
    weather_path = config.table_path("weather")
    before = iqr._fingerprint(weather_path)
    path = config.namespace / "_runs" / (identity["source_run_id"] + ".json")
    weather = _read_verified_tables(spark, config, report, path)["weather"]
    item = items["weather"]
    for name, expected in {"_bronze_source_uri": item["source_uri"],
                           "_bronze_source_sha256": item["source_sha256"],
                           "_bronze_data_kind": config.data_kind,
                           "_bronze_dataset_reference": config.dataset_reference}.items():
        iqr._require(name in weather.columns, "Weather lineage is missing")
        iqr._require(not weather.where(~F.col(name).eqNullSafe(F.lit(expected))).limit(1).count(),
                     "Weather lineage differs")
    result = _associate(context, weather)
    def verify_multiset(left, right):
        iqr._require(not left.exceptAll(right).limit(1).count() and
                     not right.exceptAll(left).limit(1).count(), "Association multiset differs")
    verify_multiset(result.select(*context.columns), context)
    verify_multiset(result.select(*electrical.columns), electrical)
    iqr._require(iqr._fingerprint(weather_path) == before, "Weather source changed")
    iqr._stable_silver(identity, config)
    again, _ = contextual_reader.read_synthetic_contextual(spark, config, contextual_publication_id, root)
    iqr._require(again == contextual_evidence, "Contextual publication changed")
    iqr._require(iqr._fingerprint(weather_path) == before, "Weather source changed")
    return {"association_policy": ASSOCIATION_POLICY, "silver": identity,
            "contextual_publication_id": contextual_publication_id, "contextual": contextual_evidence,
            "manifest_sha256": digest, "weather_version": item["delta_version"],
            "weather_source_sha256": item["source_sha256"], "weather_files_sha256": before,
            "available_variables": {name: name in weather.columns for name in VARIABLES},
            "temporal_semantics": "nominal_not_physical"}, result
