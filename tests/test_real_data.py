"""Optional test; fixtures never substitute for real BDG2 data."""
import os
from pathlib import Path
import pytest

from tfm_lakehouse.config import load_config
from tfm_lakehouse.ingestion.csv_source import inspect_source, read_source


@pytest.mark.real_data
def test_available_real_sources_are_readable(request):
    configured = os.environ.get("TFM_REAL_CONFIG")
    if not configured:
        pytest.skip("Sin fuentes BDG2 locales; establezca TFM_REAL_CONFIG explícitamente.")
    config = load_config(Path(configured))
    assert config.data_kind == "real"
    # Delay JVM startup until real inputs have been explicitly configured.
    spark = request.getfixturevalue("spark")
    for name, path in (("electricity", config.electricity), ("metadata", config.metadata)):
        frame = read_source(spark, inspect_source(path, name))
        assert frame.limit(1).count() == 1
