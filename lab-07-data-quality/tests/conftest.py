import pytest


@pytest.fixture(scope="session")
def spark():
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    return DatabricksSession.builder.getOrCreate()