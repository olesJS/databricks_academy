import pytest
import os

CATALOG = os.getenv("DQ_CATALOG", "dbr_dev_ua5816bd")
SCHEMA = os.getenv("DQ_SCHEMA", "oles0305")


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
    return f"{CATALOG}.{SCHEMA}_bronze"


@pytest.fixture(scope="session")
def silver():
    return f"{CATALOG}.{SCHEMA}_silver"


@pytest.fixture(scope="session")
def gold():
    return f"{CATALOG}.{SCHEMA}_gold"