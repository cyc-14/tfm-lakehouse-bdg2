"""Coordinate explicitly selected synthetic Silver and IQR publications."""
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver.silver_synthetic_publication import publish_synthetic_silver


def publish_synthetic_silver_iqr(spark, config, run_id, silver_publication_id,
                                 iqr_publication_id, workspace):
    """Publish a verified pair with at most one conservative IQR recovery attempt.

    Returned errors are exception objects, preserving their original causes.
    Successful Silver is never republished during IQR recovery.
    """
    result = {"status": "technical_failure", "silver": None, "iqr": None,
              "silver_verified": False, "iqr_attempts": 0, "error": None}
    try:
        silver = publish_synthetic_silver(spark, config, run_id, silver_publication_id, workspace)
        result["silver"] = silver
        root = iqr._workspace(workspace, config)
        identity, _ = iqr._verify_silver(spark, config, root, silver_publication_id)
        iqr._require(all(identity.get(key) == value for key, value in silver.items())
                     and identity["source_run_id"] == run_id, "Silver identity differs")
        result["silver_verified"] = True
    except Exception as exc:
        result.update(status="partial_success" if result["silver"] else "technical_failure",
                      error=exc)
        return result
    try:
        result["iqr_attempts"] = 1
        try:
            publication, _ = iqr._orchestrated_iqr(
                spark, config, root, silver_publication_id, iqr_publication_id, identity)
        except iqr._RetryCandidate:
            def mark_recovery_start():
                result["iqr_attempts"] = 2

            publication, _ = iqr._orchestrated_iqr(
                spark, config, root, silver_publication_id, iqr_publication_id, identity,
                recover=True, on_recovery_start=mark_recovery_start)
        state, _, _ = iqr.read_synthetic_iqr_pair(
            spark, config, workspace, silver_publication_id=silver_publication_id,
            iqr_publication_id=iqr_publication_id)
        iqr._require(state["silver"] == identity, "Silver identity changed")
        result.update(status="not_accepted" if state["iqr_status"] == "not_accepted" else "success",
                      iqr=publication, pair_state=state)
    except Exception as exc:
        result.update(status="partial_success", error=exc)
    return result
