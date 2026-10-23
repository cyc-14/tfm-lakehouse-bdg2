import pytest

from tfm_lakehouse.spark import spark_session


@pytest.fixture(scope="session")
def spark():
    with spark_session("tfm-lakehouse-tests") as session:
        yield session
