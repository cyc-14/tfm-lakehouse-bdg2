import pytest

from tfm_lakehouse.environment import verify_delta


@pytest.mark.environment
def test_delta_roundtrip(spark):
    report = verify_delta(spark)
    assert report["status"] == "passed"
    assert report["rows_read"] == report["rows_written"] == 3
