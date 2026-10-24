import os
from pathlib import Path
import pytest

from tfm_lakehouse.config import load_config
from tfm_lakehouse.spark import spark_session


@pytest.fixture(scope="session")
def spark():
    configured = os.environ.get("TFM_REAL_CONFIG")
    config = load_config(Path(configured)) if configured else None
    with spark_session(
        "tfm-lakehouse-tests",
        master=config.master if config else "local[2]",
        ivy_dir=config.ivy_dir if config else None,
    ) as session:
        yield session
