import pytest
import os


def _env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


@pytest.fixture(scope="session")
def spark():
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def bronze():
    return f"{_env('DQ_CATALOG')}.{_env('DQ_SCHEMA')}_bronze"


@pytest.fixture(scope="session")
def silver():
    return f"{_env('DQ_CATALOG')}.{_env('DQ_SCHEMA')}_silver"


@pytest.fixture(scope="session")
def gold():
    return f"{_env('DQ_CATALOG')}.{_env('DQ_SCHEMA')}_gold"
