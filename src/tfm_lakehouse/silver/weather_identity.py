"""Preliminary synthetic weather identity and exact distributed reconciliation."""
from tfm_lakehouse.silver import contextual_reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver.weather_enrichment import (
    ASSOCIATION_POLICY, MAX_DISTANCE_SECONDS, associate_synthetic_weather,
)
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint

IDENTITY_VERSION = 1
NUMERIC_POLICY = "h05-decimal-38-18-weather-nan-v1"
QUALITY_POLICY = "h06-weather-quality-potential-codes-v1"


def build_synthetic_weather_identity(spark, config, silver_id, contextual_id, publication_id, workspace):
    """Return preliminary source identity and a lazy view, without publication.

    This evidence is not a durable content identity; the view and retained
    sources must remain unchanged through all subsequent Spark actions.
    """
    iqr._identifier(publication_id)
    evidence, frame = associate_synthetic_weather(spark, config, silver_id, contextual_id, workspace)
    iqr._require(evidence["association_policy"] == ASSOCIATION_POLICY, "Association policy differs")
    intent = {
        "contract_version": IDENTITY_VERSION,
        "publication_id": publication_id,
        "silver": evidence["silver"],
        "contextual_publication_id": contextual_id,
        "contextual": evidence["contextual"],
        "manifest_sha256": evidence["manifest_sha256"],
        "bronze_run_id": evidence["silver"]["source_run_id"],
        "weather_delta_version": evidence["weather_version"],
        "weather_source_uri": evidence["weather_source_uri"],
        "weather_table_path": evidence["weather_table_path"],
        "weather_source_sha256": evidence["weather_source_sha256"],
        "weather_files_sha256": evidence["weather_files_sha256"],
        "available_variables": evidence["available_variables"],
        "association_policy": ASSOCIATION_POLICY,
        "numeric_policy": NUMERIC_POLICY,
        "quality_policy": QUALITY_POLICY,
        "tolerance_seconds": MAX_DISTANCE_SECONDS,
        "inclusive": True,
        "tie_break": "earlier_nominal_timestamp",
        "duplicates": "reject_selected_without_fallback",
        "schema": frame.schema.jsonValue(),
        "content_evidence": "pending_durable_capture",
    }
    iqr._require(weather_tree_fingerprint(config.table_path("weather")) == intent["weather_files_sha256"],
                 "Weather source changed")
    return intent, frame


def verify_synthetic_weather_identity(spark, config, silver_id, contextual_id, publication_id,
                                      workspace, expected, candidate):
    """Rebuild identity and compare every candidate row in both directions.

    Passing this check is evidence for the action, not a durable capture or a
    guarantee against subsequent changes to lazy inputs.
    """
    iqr._identifier(publication_id)
    iqr._workspace(workspace, config)
    if isinstance(expected, dict) and "weather_files_sha256" in expected:
        iqr._require(weather_tree_fingerprint(config.table_path("weather")) == expected["weather_files_sha256"],
                     "Weather source changed")
    actual, reference = build_synthetic_weather_identity(
        spark, config, silver_id, contextual_id, publication_id, workspace)
    try:
        same = iqr._canonical(actual) == iqr._canonical(expected)
    except (TypeError, ValueError):
        same = False
    iqr._require(same, "Weather identity differs")
    iqr._require(iqr._canonical(candidate.schema.jsonValue()) == iqr._canonical(actual["schema"]),
                 "Weather schema differs")
    iqr._require(not candidate.exceptAll(reference).limit(1).count() and
                 not reference.exceptAll(candidate).limit(1).count(), "Weather multiset differs")
    iqr._require(weather_tree_fingerprint(config.table_path("weather")) == actual["weather_files_sha256"],
                 "Weather source changed")
    iqr._stable_silver(actual["silver"], config)
    contextual, _ = contextual_reader.read_synthetic_contextual(spark, config, contextual_id, workspace)
    iqr._require(iqr._canonical(contextual) == iqr._canonical(actual["contextual"]),
                 "Contextual publication changed")
    iqr._require(weather_tree_fingerprint(config.table_path("weather")) == actual["weather_files_sha256"],
                 "Weather source changed")
    return {"status": "reconciled_for_this_action", "identity": actual,
            "durable_content_verified": False}
