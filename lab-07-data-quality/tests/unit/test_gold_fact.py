import pytest
from chispa import assert_df_equality

from f1_pipeline.transforms.gold import _replace_pit_lane_start, build_fact_results


RESULTS = (
    "resultId int, raceId int, driverId int, constructorId int, grid int, position int, "
    "positionOrder string, points int, laps int, milliseconds long, fastestLapSpeed double"
)

RACES = "raceId int, circuitId int"

def _results(spark, rows):
    # rows: (resultId, raceId, grid)
    data = [(rid, race, 1, 1, grid, 1, "1", 10, 58, 1000, 200.0) for rid, race, grid in rows]
    return spark.createDataFrame(data, RESULTS)


@pytest.mark.parametrize("actual_data, expected_data", [
    (   # test_pit_lane_gets_slot_after_last_grid
        [(1, 1, 1), (2, 1, 2), (3, 1, 0), (4, 2, 1), (5, 2, 0)],
        [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 2, 1), (5, 2, 2)]
    ),
    (   # test_several_pit_lane_starters_share_the_same_slot
        [(1, 1, 1), (2, 1, 2), (3, 1, 0), (4, 1, 0)],
        [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 3)]
    ),
    (   # test_null_grid_is_treated_as_pit_lane
        [(1, 1, 1), (2, 1, None)],
        [(1, 1, 1), (2, 1, 2)]
    )
])
def test_pit_lane_start(spark, actual_data, expected_data):
    actual = spark.createDataFrame(actual_data, "resultId int, raceId int, grid int")
    expected = spark.createDataFrame(expected_data, "resultId int, raceId int, grid int")
    assert_df_equality(_replace_pit_lane_start(actual), expected)


def test_fact_results_full_row(spark):
    races = spark.createDataFrame([(1, 100)], RACES)
    actual = build_fact_results(_results(spark, [(1, 1, 1)]), races)
    expected = spark.createDataFrame(
        [(1, 1, 1, 1, 100, 1, 1, "1", 10, 58, 1000, 200.0)],
        "result_id int, race_id int, driver_id int, constructor_id int, circuit_id int, grid int, "
        "position int, position_order string, points int, laps int, milliseconds long, "
        "fastest_lap_speed double",
    )
    assert_df_equality(actual, expected)


def test_fact_circuit_id_comes_from_races(spark):
    races = spark.createDataFrame([(1, 100), (2, 200)], RACES)
    actual = build_fact_results(_results(spark, [(1, 1, 1), (2, 2, 1)]), races)
    expected = spark.createDataFrame([(1, 100), (2, 200)], "race_id int, circuit_id int")
    assert_df_equality(actual.select("race_id", "circuit_id"), expected)


def test_fact_orphan_results_dropped(spark):
    # result with absent race record disappears because of Inner Join
    races = spark.createDataFrame([(1, 100)], RACES)
    actual = build_fact_results(_results(spark, [(1, 1, 1), (2, 2, 2)]), races)
    expected = spark.createDataFrame([(1, 1)], "result_id int, race_id int")
    assert_df_equality(actual.select("result_id", "race_id"), expected)
