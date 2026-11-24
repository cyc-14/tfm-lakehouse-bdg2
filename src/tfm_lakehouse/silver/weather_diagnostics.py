"""Synthetic, nominal weather diagnostics bound to one Silver publication."""
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.ingestion.snapshot import _read_verified_tables
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.metadata_diagnostics import diagnose_synthetic_metadata
from tfm_lakehouse.silver.silver_electricity_temporal import _interpret as _parse_nominal
from tfm_lakehouse.silver.metadata_enrichment import _timezone
from tfm_lakehouse.weather_characterization import VARIABLES
from tfm_lakehouse.characterization import classify_cell


def _nominal(text):
    date, second, status = _parse_nominal(text)
    return status, date.strftime("%Y-%m") if date else "unassigned", (
        second % 3600 != 0 if second is not None else None)


_NOMINAL_SCHEMA = StructType([StructField("status", StringType(), False),
                              StructField("period", StringType(), False),
                              StructField("off_grid", BooleanType(), True)])


def _cell_state(value):
    return classify_cell(value if value is not None else "", ("", "NaN"))[0]


def _special(value, variable):
    try:
        number = Decimal(value) if value is not None else None
    except InvalidOperation:
        return False
    return bool(number is not None and number.is_finite() and
                ((variable in ("precipDepth1HR", "precipDepth6HR") and number == -1) or
                 (variable == "cloudCoverage" and number == 9)))


def _transition_sensitive(label, zone):
    date, second, status = _parse_nominal(label)
    if status != "valid_nominal" or not zone:
        return None
    try:
        tz = ZoneInfo(zone)
        local = datetime(date.year, date.month, date.day, second // 3600,
                         second % 3600 // 60, second % 60)
        # These offsets are hypotheses about a label, never assigned to a row.
        return local.replace(tzinfo=tz, fold=0).utcoffset() != local.replace(tzinfo=tz, fold=1).utcoffset()
    except (ZoneInfoNotFoundError, ValueError):
        return None


def diagnose_synthetic_weather(spark, config, silver_publication_id, workspace):
    """Report nominal compatibility without associating weather to observations.

    Weather and metadata are fixed by Silver's Bronze manifest. This does not
    establish a station, physical interval, UTC instant, or historical UUID.
    """
    base = diagnose_synthetic_metadata(spark, config, silver_publication_id, workspace)
    root = iqr._workspace(workspace, config)
    identity, electricity = iqr._verify_silver(spark, config, root, silver_publication_id)
    iqr._require(identity == base["silver"], "Silver identity changed")
    report, digest = silver._manifest(config, identity["source_run_id"])
    iqr._require(digest == base["manifest_sha256"], "Bronze manifest changed")
    items = {item["name"]: item for item in report["tables"]}
    result = {"silver": identity, "bronze_run_id": identity["source_run_id"],
              "manifest_sha256": digest, "weather_status": "unavailable",
              "metadata_duplicates": base["duplicates"],
              "metadata_site_column_available": base["available_columns"]["site_id"],
              "spatial_semantics": "nominal_site_only", "temporal_semantics": "nominal_not_physical",
              "historical_bronze_uuid": "not_recorded"}
    path = config.namespace / "_runs" / (identity["source_run_id"] + ".json")
    weather_path = config.table_path("weather") if "weather" in items else None
    before_weather = iqr._fingerprint(weather_path) if weather_path else None
    before_metadata = iqr._fingerprint(config.table_path("metadata"))
    iqr._require(before_metadata == base["metadata_files_sha256"], "Metadata files changed")
    frames = _read_verified_tables(spark, config, report, path)
    metadata, weather = frames["metadata"], frames.get("weather")
    if weather is not None:
        weather = weather.persist(StorageLevel.DISK_ONLY)
    try:
        for name, frame in (("metadata", metadata),) + (("weather", weather),) * (weather is not None):
            item = items[name]
            lineage = {"_bronze_source_uri": item["source_uri"],
                       "_bronze_source_sha256": item["source_sha256"],
                       "_bronze_data_kind": config.data_kind,
                       "_bronze_dataset_reference": config.dataset_reference}
            mismatch = None
            for field, value in lineage.items():
                iqr._require(field in frame.columns, "Bronze lineage is missing")
                current = ~F.col(field).eqNullSafe(F.lit(value))
                mismatch = current if mismatch is None else mismatch | current
            iqr._require(not frame.where(mismatch).limit(1).count(), "Bronze lineage differs")
        iqr._require({"building_id"}.issubset(metadata.columns), "Metadata key is missing")
        has_site, has_zone = "site_id" in metadata.columns, "timezone" in metadata.columns
        site_value = F.col("site_id") if has_site else F.lit(None).cast("string")
        zone_value = F.col("timezone") if has_zone else F.lit(None).cast("string")
        rows = (metadata.where(F.col("building_id").isin(*APPROVED_BUILDINGS))
                .groupBy("building_id").agg(F.count(F.lit(1)).alias("row_count"),
                    F.max(site_value).alias("site_id"), F.max(zone_value).alias("timezone")).collect())
        grouped = {row.building_id: row for row in rows}
        mappings = []
        for building in APPROVED_BUILDINGS:
            value = grouped.get(building)
            if value is None:
                mappings.append({"building_id": building, "status": "missing_metadata", "site_id": None})
            elif value.row_count != 1:
                mappings.append({"building_id": building, "status": "ambiguous_metadata", "site_id": None})
            elif not has_site:
                mappings.append({"building_id": building, "status": "missing_site_column", "site_id": None})
            elif value.site_id is None or value.site_id == "":
                mappings.append({"building_id": building, "status": "missing_site", "site_id": value.site_id})
            elif not value.site_id.strip():
                mappings.append({"building_id": building, "status": "invalid_site", "site_id": value.site_id})
            else:
                zone = _timezone(value.timezone) if has_zone else "missing_column"
                mappings.append({"building_id": building, "status": "nominal_site", "site_id": value.site_id,
                                 "timezone_status": zone, "timezone_original": value.timezone,
                                 "dst_ambiguity": "unresolved_not_physical_alignment"})
        sites = sorted({row["site_id"] for row in mappings if row["status"] == "nominal_site"})
        if weather is None:
            iqr._require(iqr._fingerprint(config.table_path("metadata")) == before_metadata,
                         "Metadata files changed")
            iqr._stable_silver(identity, config)
            result.update(mappings=mappings, missing_weather_sites=sites)
            return result
        iqr._require({"site_id", "timestamp"}.issubset(weather.columns), "Weather keys are missing")
        relevant = weather.where(F.col("site_id").isin(*sites))
        duplicate_groups = (weather.groupBy("site_id", "timestamp")
                            .agg(F.count(F.lit(1)).alias("rows"),
                                 F.countDistinct(F.struct(*[F.col('`' + c.replace('`', '``') + '`')
                                                            for c in weather.columns])).alias("variants"))
                            .where(F.col("rows") > 1))
        duplicates = [row.asDict() for row in duplicate_groups.groupBy("site_id").agg(
            F.count(F.when(F.col("variants") == 1, 1)).alias("identical_keys"),
            F.count(F.when(F.col("variants") > 1, 1)).alias("conflicting_keys"),
            F.sum(F.col("rows") - F.col("variants")).alias("exact_duplicate_extra_rows")).collect()]
        available = {name: name in weather.columns for name in VARIABLES}
        nominal = F.udf(_nominal, _NOMINAL_SCHEMA)
        relevant = relevant.withColumn("__weather_nominal", nominal(F.col("timestamp")))
        state_udf = F.udf(_cell_state, StringType())
        profiles = []
        for variable in VARIABLES:
            if not available[variable]:
                continue
            token = F.col(variable)
            state = state_udf(token)
            aggregates = [F.count(F.lit(1)).alias("rows"),
                          *[F.count(F.when(state == label, 1)).alias(label)
                            for label in ("missing", "nonnumeric", "nonfinite", "finite")],
                          F.countDistinct(F.when((F.col("__weather_nominal.status") == "valid_nominal")
                                                & ~F.col("__weather_nominal.off_grid"),
                                                F.col("timestamp"))).alias("distinct_hourly_labels"),
                          F.countDistinct(F.when((F.col("__weather_nominal.status") == "valid_nominal")
                                                & ~F.col("__weather_nominal.off_grid") & (state == "finite"),
                                                F.col("timestamp"))).alias("distinct_hours_with_finite_value")]
            if variable in ("precipDepth1HR", "precipDepth6HR"):
                special = F.udf(lambda value: _special(value, variable), BooleanType())
                aggregates.append(F.count(F.when(special(token), 1)).alias("special_code_count"))
            elif variable == "cloudCoverage":
                special = F.udf(lambda value: _special(value, variable), BooleanType())
                aggregates.append(F.count(F.when(special(token), 1)).alias("special_code_count"))
            profiles.extend({"variable": variable, **row.asDict()} for row in
                            relevant.groupBy("site_id", F.col("__weather_nominal.period").alias("period")).agg(*aggregates).collect())
        present_sites = {row.site_id for row in relevant.select("site_id").distinct().collect()}
        for row in mappings:
            if row["status"] == "nominal_site":
                row["weather_site_present"] = row["site_id"] in present_sites
        # Compare sets of original nominal labels, never individual measurements.
        missing = []
        for site in sites:
            buildings = [row["building_id"] for row in mappings
                         if row["status"] == "nominal_site" and row["site_id"] == site]
            electric = (electricity.where(F.col("building_id").isin(*buildings))
                        .where(F.col("timestamp_parse_status") == "valid_nominal")
                        .select(F.col("timestamp_original").alias("label")).distinct())
            weather_labels = (relevant.where(F.col("site_id") == site)
                              .select(F.col("timestamp").alias("label")).distinct())
            by_period = F.udf(lambda text: _nominal(text)[1], StringType())
            electric_periods = electric.withColumn("period", by_period(F.col("label")))
            missing_periods = electric.join(weather_labels, "label", "left_anti").withColumn(
                "period", by_period(F.col("label")))
            periods = {row.period: row["count"] for row in electric_periods.groupBy("period").count().collect()}
            absent = {row.period: row["count"] for row in missing_periods.groupBy("period").count().collect()}
            missing.extend({"site_id": site, "period": p, "electricity_distinct_labels": n,
                            "missing_weather_labels": absent.get(p, 0)} for p, n in periods.items())
        coverage_lookup = {(row["site_id"], row["period"]): row for row in missing}
        existing = {(row["site_id"], row["period"], row["variable"]) for row in profiles}
        for (site, period), cover in coverage_lookup.items():
            for variable, is_available in available.items():
                if is_available and (site, period, variable) not in existing:
                    empty = {"site_id": site, "period": period, "variable": variable,
                             "rows": 0, "missing": 0, "nonnumeric": 0, "nonfinite": 0,
                             "finite": 0, "distinct_hourly_labels": 0,
                             "distinct_hours_with_finite_value": 0}
                    if variable in ("precipDepth1HR", "precipDepth6HR", "cloudCoverage"):
                        empty["special_code_count"] = 0
                    profiles.append(empty)
        for row in profiles:
            cover = coverage_lookup.get((row["site_id"], row["period"]))
            row["electricity_distinct_labels"] = cover["electricity_distinct_labels"] if cover else 0
            row["missing_weather_labels"] = cover["missing_weather_labels"] if cover else 0
        temporal = [row.asDict() for row in relevant.groupBy("site_id", "__weather_nominal.status",
                    "__weather_nominal.off_grid").count().collect()]
        transition_diagnostics = []
        transition_cache = {}
        for mapping in mappings:
            if mapping["status"] != "nominal_site" or mapping.get("timezone_status") != "valid":
                continue
            zone = mapping["timezone_original"]
            probe = F.udf(lambda label: _transition_sensitive(label, zone), BooleanType())
            subset = relevant.where(F.col("site_id") == mapping["site_id"])
            key = (mapping["site_id"], zone)
            if key not in transition_cache:
                transition_cache[key] = subset.where(probe(F.col("timestamp"))).select("timestamp").distinct().count()
            transition_diagnostics.append({"building_id": mapping["building_id"],
                "sensitive_labels": transition_cache[key],
                "meaning": "offset_hypotheses_only_no_physical_alignment"})
        iqr._require(iqr._fingerprint(weather_path) == before_weather, "Weather files changed")
        iqr._require(iqr._fingerprint(config.table_path("metadata")) == before_metadata, "Metadata files changed")
        iqr._stable_silver(identity, config)
        result.update(weather_status="available", weather_version=items["weather"]["delta_version"],
                      weather_source_sha256=items["weather"]["source_sha256"], mappings=mappings,
                      missing_weather_sites=[site for site in sites if site not in present_sites],
                      available_variables=available, variable_units=dict(VARIABLES),
                      duplicates=duplicates, variable_period_profiles=profiles,
                      temporal_status=temporal, nominal_label_coverage=missing,
                      transition_diagnostics=transition_diagnostics)
        return result
    finally:
        if weather is not None:
            weather.unpersist()
