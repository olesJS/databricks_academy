import os
import yaml
import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

CHECKS_PATH = os.path.join(os.path.dirname(__file__), "gold_checks.yml")

with open(CHECKS_PATH, "r") as f:
    GOLD_CHECKS = yaml.safe_load(f)


@pytest.fixture(scope="session")
def dq_engine():
    return DQEngine(WorkspaceClient())


def _criticality_checks(checks, criticality):
    return [check for check in checks if check["criticality"] == criticality]


@pytest.mark.parametrize("table", list(GOLD_CHECKS))
def test_error_checks_pass(spark, dq_engine, gold, table):
    df = spark.read.table(f"{gold}.{table}")
    _, invalid = dq_engine.apply_checks_by_metadata_and_split(df, _criticality_checks(GOLD_CHECKS[table], "error"))
    assert invalid.count() == 0, f"{table}: violating error-level checks"


@pytest.mark.parametrize("table", list(GOLD_CHECKS))
def test_warn_checks_pass(spark, dq_engine, gold, table):
    warn = _criticality_checks(GOLD_CHECKS[table], "warn")
    if not warn:
        pytest.skip("No warn checks")
    df = spark.read.table(f"{gold}.{table}")
    _, invalid = dq_engine.apply_checks_by_metadata_and_split(df, warn)
    if invalid.count() > 0:
        print(f"WARN {table}: {invalid.count()} rows flagged")


@pytest.mark.parametrize("table", list(GOLD_CHECKS))
def test_table_is_not_empty(spark, gold, table):
    assert spark.read.table(f"{gold}.{table}").count() > 0, f"{table} is empty"


def test_dqx_flags_known_bad_data(spark, dq_engine):
    bad = spark.createDataFrame(
        [(1, None, 5, 2), (1, 5, 1, 2)],
        "result_id int, grid int, wins int, races_count int",
    )
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "grid"}}},
        {"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["result_id"]}}},
        {"criticality": "error", "check": {"function": "sql_expression", "arguments": {
            "expression": "wins <= races_count", "msg": "wins exceed races_count"}}},
    ]

    _, invalid = dq_engine.apply_checks_by_metadata_and_split(bad, checks)
    assert invalid.count() >= 1

