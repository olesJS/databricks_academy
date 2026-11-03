import pytest
import json
from datetime import datetime
from chispa import assert_df_equality

from f1_pipeline.transforms.silver import (
    deduplicate_results,
    parse_results
)


BRONZE_RESULTS_SCHEMA = """
    raw_json string, _ingested_at timestamp
"""

SILVER_RESULTS_SCHEMA = """
    resultId int, raceId int, driverId int, constructorId int, grid int, position int,
    positionOrder string, points int, laps int, milliseconds bigint, fastestLapSpeed double, _ingested_at timestamp
"""

VALID_PAYLOAD = {
    "resultId": "1", "raceId": "18", "driverId": "1", "constructorId": "1", "grid": "1",
    "position": "1", "positionOrder": "1", "points": "10", "laps": "58",
    "milliseconds": "5690616", "fastestLapSpeed": "218.3"
}

VALID_ROW = {
    "resultId": 1, "raceId": 18, "driverId": 1, "constructorId": 1, "grid": 1,
    "position": 1, "positionOrder": "1", "points": 10, "laps": 58,
    "milliseconds": 5690616, "fastestLapSpeed": 218.3, "_ingested_at": None,
}

COLUMNS = [
    "resultId", "raceId", "driverId", "constructorId", "grid", "position", "positionOrder",
    "points", "laps", "milliseconds", "fastestLapSpeed", "_ingested_at",
]


def _actual(spark, raw_json):
    return parse_results(spark.createDataFrame([(raw_json, None)], BRONZE_RESULTS_SCHEMA))

def _expected(spark, base, **overrides):
    row = {**base, **overrides}
    return spark.createDataFrame([tuple(row[c] for c in COLUMNS)], SILVER_RESULTS_SCHEMA)


def test_valid_row(spark):
    actual = _actual(spark, json.dumps(VALID_PAYLOAD))
    expected = _expected(spark, VALID_ROW)
    assert_df_equality(actual, expected)


def test_non_numeric_value_becomes_null(spark):
    actual = _actual(spark, json.dumps({**VALID_PAYLOAD, "position": "abc"}))
    expected = _expected(spark, VALID_ROW, position=None)
    assert_df_equality(actual, expected)


def test_missing_fields_become_null(spark):
    actual = _actual(spark, json.dumps({**VALID_PAYLOAD, "points": None, "milliseconds": None}))
    expected = _expected(spark, VALID_ROW, points=None, milliseconds=None)
    assert_df_equality(actual, expected)


def test_numbers_instead_of_strings_in_json(spark):
    actual = _actual(spark, json.dumps({**VALID_PAYLOAD, "resultId": 1, "positionOrder": 1, "fastestLapSpeed": 218.3}))
    expected = _expected(spark, VALID_ROW)
    assert_df_equality(actual, expected)


def test_fractional_points_instead_of_whole(spark):
    actual = _actual(spark, json.dumps({**VALID_PAYLOAD, "points": "0.5"}))
    expected = _expected(spark, VALID_ROW, points=None)
    assert_df_equality(actual, expected)


@pytest.mark.xfail
def test_fractional_point_are_preserved(spark):
    actual = _actual(spark, json.dumps({**VALID_PAYLOAD, "points": "0.5"}))
    expected = _expected(spark, VALID_ROW, points=0.5)
    assert_df_equality(actual, expected)


T1, T2 = datetime(2026, 1, 1), datetime(2026, 1, 2)

def test_keeps_latest_version_of_duplicate(spark):
    source = spark.createDataFrame(
        [(1, 10, T1), (1, 25, T2), (2, 8, T1)],
        "resultId int, points int, _ingested_at timestamp"
    )
    expected = spark.createDataFrame(
        [(1, 25, T2), (2, 8, T1)],
        "resultId int, points int, _ingested_at timestamp"
    )
    assert_df_equality(deduplicate_results(source), expected)
