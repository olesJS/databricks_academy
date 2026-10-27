import pytest
from chispa import assert_df_equality

from f1_pipeline.transforms.gold import (
    build_agg_constructor_season,
    build_agg_driver_season,
    build_agg_driver_circuit
)

# (constructor_id, driver_id, race_id, circuit_id, grid, position_order, points)
FACTS = [
    (10, 1, 1, 100, 1, 1, 25),   # driver 1, race 1: win
    (10, 1, 2, 100, 2, 3, 15),   # driver 1, race 2: podium
    (10, 1, 3, 200, 4, 5, 10),   # driver 1, race 3: (different season and circuit)
    (10, 2, 1, 100, 3, 2, 18),   # driver 2 (same team), race 1
]

RACES = [(1, 2020), (2, 2020), (3, 2021)]

FACT_SCHEMA = (
    "constructor_id int, driver_id int, race_id int, circuit_id int, grid int, "
    "position_order int, points int"
)

RACES_SCHEMA = "race_id int, season_year int"

CONSTRUCTOR_SEASON_SCHEMA = (
    "constructor_id int, season_year int, races_count long, wins long, podiums long, points_sum long"
)

DRIVER_SEASON_SCHEMA = (
    "driver_id int, season_year int, races_count long, wins long, podiums long, points_sum long, "
    "grid_position_sum long, position_order_sum long"
)

DRIVER_CIRCUIT_SCHEMA = (
    "driver_id int, circuit_id int, races_count long, wins long, podiums long, points_sum long, "
    "grid_position_sum long, position_order_sum long"
)

# constructor 10: 2020 -> 2 races, 1 win, 3 podiums, 58 points
EXPECTED_CONSTRUCTOR_SEASON = [(10, 2020, 2, 1, 3, 58), (10, 2021, 1, 0, 0, 10)]


@pytest.fixture
def agg_inputs(spark):
    return (
        spark.createDataFrame(FACTS, FACT_SCHEMA),
        spark.createDataFrame(RACES, RACES_SCHEMA),
    )


def test_constructor_season(spark, agg_inputs):
    expected = spark.createDataFrame(EXPECTED_CONSTRUCTOR_SEASON, CONSTRUCTOR_SEASON_SCHEMA)
    assert_df_equality(
        build_agg_constructor_season(*agg_inputs),
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_driver_season(spark, agg_inputs):
    expected = spark.createDataFrame(
        [
            (1, 2020, 2, 1, 2, 40, 3, 4),
            (1, 2021, 1, 0, 0, 10, 4, 5),
            (2, 2020, 1, 0, 1, 18, 3, 2),
        ],
        DRIVER_SEASON_SCHEMA,
    )
    assert_df_equality(
        build_agg_driver_season(*agg_inputs),
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_driver_circuit(spark, agg_inputs):
    expected = spark.createDataFrame(
        [
            (1, 100, 2, 1, 2, 40, 3, 4),
            (1, 200, 1, 0, 0, 10, 4, 5),
            (2, 100, 1, 0, 1, 18, 3, 2),
        ],
        DRIVER_CIRCUIT_SCHEMA,
    )
    assert_df_equality(
        build_agg_driver_circuit(*agg_inputs),
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_facts_without_dim_race_are_excluded(spark):
    fact = spark.createDataFrame([(10, 1, 99, 100, 1, 1, 25)], FACT_SCHEMA)
    dim_race = spark.createDataFrame([(1, 2020)], RACES_SCHEMA)
    expected = spark.createDataFrame([], DRIVER_SEASON_SCHEMA)
    assert_df_equality(build_agg_driver_season(fact, dim_race), expected, ignore_nullable=True)