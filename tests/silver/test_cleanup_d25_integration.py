"""Synthetic Spark coverage for successful metadata work with failed cleanup."""
import pytest
from pyspark.sql import DataFrame

from tfm_lakehouse.silver import metadata_diagnostics as metadata
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
def test_metadata_cleanup_failure_after_success_is_reported(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    original_read = metadata._read_verified_tables
    original_unpersist = DataFrame.unpersist
    target = []
    failure = OSError("synthetic cleanup failure")

    def capture(*args, **kwargs):
        tables = original_read(*args, **kwargs)
        if not target:
            target.append(tables["metadata"])
        return tables

    def fail_after_release(frame, *args, **kwargs):
        result = original_unpersist(frame, *args, **kwargs)
        if target and frame is target[0]:
            raise failure
        return result

    monkeypatch.setattr(metadata, "_read_verified_tables", capture)
    monkeypatch.setattr(DataFrame, "unpersist", fail_after_release)
    with pytest.raises(OSError) as caught:
        metadata.diagnose_synthetic_metadata(spark, config, "silver", tmp_path)
    assert caught.value is failure
