import pytest
from chispa import assert_df_equality
import datetime as dt

from f1_pipeline.transforms.gold import (
    build_dim_race,
    build_dim_circuit,
    build_dim_constructor,
    build_dim_driver
)


def test_dim_race(spark):
    source = spark.createDataFrame(
        [(1, 2009, 1, "Australian GP", dt.date(2009, 3, 29))],
        "raceId int, year int, round int, name string, date date",
    )
    expected = spark.createDataFrame(
        [(1, 2009, 1, "Australian GP", dt.date(2009, 3, 29))],
        "race_id int, season_year int, season_round int, race_name string, race_date date",
    )
    assert_df_equality(build_dim_race(source), expected)


def test_dim_circuit(spark):
    source = spark.createDataFrame(
        [(1, "albert_park", "Albert Park", "Melbourne", "Australia", -37.8, 144.9, 10)],
        "circuitId int, circuitRef string, name string, location string, country string, "
        "lat double, lng double, alt int",
    )
    expected = spark.createDataFrame(
        [(1, "albert_park", "Albert Park", "Australia", "Melbourne", -37.8, 144.9, 10)],
        "circuit_id int, circuit_ref string, circuit_name string, country string, "
        "location string, lat double, lng double, alt int",
    )
    assert_df_equality(build_dim_circuit(source), expected)


def test_dim_constructor(spark):
    source = spark.createDataFrame(
        [(1, "mclaren", "McLaren", "British")],
        "constructorId int, constructorRef string, name string, nationality string",
    )
    expected = spark.createDataFrame(
        [(1, "mclaren", "McLaren", "British")],
        "constructor_id int, constructor_ref string, constructor_name string, "
        "constructor_nationality string",
    )
    assert_df_equality(build_dim_constructor(source), expected)


GOLD_DRIVERS_SCHEMA = """
    driver_id int, driver_ref string, driver_number int, driver_code string, forename string,
    surname string, date_of_birth date, driver_nationality string, valid_from timestamp,
    valid_to timestamp, is_current int
"""

SILVER_DRIVERS_SCHEMA = """
    driverId int, driverRef string, number int, code string, forename string, surname string,
    dateOfBirth date, nationality string, __START_AT timestamp, __END_AT timestamp
"""

def test_dim_driver_full_row(spark):
    t = dt.datetime(2020, 1, 1)
    source = spark.createDataFrame(
        [(1, "ham", 44, "HAM", "Lewis", "Hamilton", dt.date(1985, 1, 7), "British", t, None)],
        SILVER_DRIVERS_SCHEMA
    )
    expected = spark.createDataFrame(
        [(1, "ham", 44, "HAM", "Lewis", "Hamilton", dt.date(1985, 1, 7), "British", t, None, 1)],
        GOLD_DRIVERS_SCHEMA
    )
    assert_df_equality(build_dim_driver(source), expected, ignore_nullable=True)


def test_dim_driver_is_current_flag(spark):
    t = dt.datetime(2026, 10, 21)
    source = spark.createDataFrame(
        [
            (1, "ham", 44, "HAM", "Lewis", "Hamilton", dt.date(1985, 1, 7), "British", t, None),
            (2, "old", 7, "OLD", "Old", "Version", dt.date(1980, 1, 1), "British", t, dt.datetime(2021, 1, 1)),
        ],
        SILVER_DRIVERS_SCHEMA
    )
    actual = build_dim_driver(source).select("driver_id", "is_current")
    expected = spark.createDataFrame([(1, 1), (2, 0)], "driver_id int, is_current int")
    assert_df_equality(actual, expected, ignore_nullable=True)
