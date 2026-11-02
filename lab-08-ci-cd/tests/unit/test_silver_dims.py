import pytest
from chispa import assert_df_equality, assert_column_equality
import datetime as dt

from f1_pipeline.transforms.silver import (
    transform_circuits,
    transform_constructors,
    transform_races,
    transform_drivers,
    _try_cast
)


def _row(base: dict, **overrides):
    return tuple({**base, **overrides}.values())


# circuits
BRONZE_CIRCUITS_SCHEMA = """
    circuitId string, circuitRef string, name string, location string, country string,
    lat string, lng string, alt string, _source_file string, _ingested_at timestamp
"""

SILVER_CIRCUITS_SCHEMA = """
    circuitId int, circuitRef string, name string, location string, country string,
    lat double, lng double, alt int, _source_file string, _ingested_at timestamp
"""

CIRCUIT = dict(circuitId="1", circuitRef="albert_park", name="Albert Park", location="Melbourne",
               country="Australia", lat="-37.8497", lng="144.968", alt="10",
               _source_file="f.csv", _ingested_at=None)


def test_circuits_cast_types(spark):
    actual = transform_circuits(spark.createDataFrame([tuple(CIRCUIT.values())], BRONZE_CIRCUITS_SCHEMA))
    expected = spark.createDataFrame(
        [(1, "albert_park", "Albert Park", "Melbourne", "Australia", -37.8497, 144.968, 10,
            "f.csv", None)],
        SILVER_CIRCUITS_SCHEMA
    )
    assert_df_equality(actual, expected)


def test_circuits_non_numeric_alt_becomes_null(spark):
    actual = transform_circuits(
        spark.createDataFrame([_row(CIRCUIT, alt="abc")], BRONZE_CIRCUITS_SCHEMA)
    ).select("circuitId", "alt")
    expected = spark.createDataFrame([(1, None)], "circuitId int, alt int")
    assert_df_equality(actual, expected)


def test_lat_type_casting(spark):
    df = spark.createDataFrame(
        [(1, "-37.8497", -37.8497)], "circuitId int, lat_bronze string, expected_lat double"
    ).withColumn("lat_silver", _try_cast("lat_bronze", "DOUBLE", "lat_silver"))
    assert_column_equality(df, "lat_silver", "expected_lat")


# constructors
BRONZE_CONSTRUCTORS_SCHEMA = (
    "constructorId string, constructorRef string, name string, nationality string, "
    "_source_file string, _ingested_at timestamp"
)
SILVER_CONSTRUCTORS_SCHEMA = (
    "constructorId int, constructorRef string, name string, nationality string, "
    "_source_file string, _ingested_at timestamp"
)


def test_constructors_casts_id(spark):
    df = spark.createDataFrame(
        [("7", "mclaren", "McLaren", "British", "f.csv", None)], BRONZE_CONSTRUCTORS_SCHEMA
    )
    expected = spark.createDataFrame(
        [(7, "mclaren", "McLaren", "British", "f.csv", None)], SILVER_CONSTRUCTORS_SCHEMA
    )
    assert_df_equality(transform_constructors(df), expected)


# drivers
BRONZE_DRIVERS_SCHEMA = (
    "driverId string, driverRef string, number string, code string, forename string, "
    "surname string, dob string, nationality string, _source_file string, _ingested_at timestamp"
)
SILVER_DRIVERS_SCHEMA = (
    "driverId int, driverRef string, number int, code string, forename string, surname string, "
    "dateOfBirth date, nationality string, _source_file string, _ingested_at timestamp"
)
DRIVER = dict(driverId="1", driverRef="hamilton", number="44", code="HAM", forename="Lewis",
              surname="Hamilton", dob="1985-01-07", nationality="British",
              _source_file="f.csv", _ingested_at=None)


def _drivers(spark, **overrides):
    return transform_drivers(spark.createDataFrame([_row(DRIVER, **overrides)], BRONZE_DRIVERS_SCHEMA))


def _expected_driver(spark, **overrides):
    base = dict(driverId=1, driverRef="hamilton", number=44, code="HAM", forename="Lewis",
                surname="Hamilton", dateOfBirth=dt.date(1985, 1, 7), nationality="British",
                _source_file="f.csv", _ingested_at=None)
    return spark.createDataFrame([_row(base, **overrides)], SILVER_DRIVERS_SCHEMA)


def test_drivers_casts_and_parses_dob(spark):
    assert_df_equality(_drivers(spark), _expected_driver(spark))


def test_drivers_missing_number_becomes_null(spark):
    assert_df_equality(_drivers(spark, number=None), _expected_driver(spark, number=None))


def test_drivers_invalid_dob_becomes_null(spark):
    assert_df_equality(_drivers(spark, dob="07/01/1985"), _expected_driver(spark, dateOfBirth=None))


def test_drivers_non_numeric_id_becomes_null(spark):
    assert_df_equality(_drivers(spark, driverId="abc"), _expected_driver(spark, driverId=None))


# races
BRONZE_RACES_SCHEMA = (
    "raceId string, year string, round string, circuitId string, name string, date string, "
    "time string, _source_file string, _ingested_at timestamp"
)

SILVER_RACES_SCHEMA = (
    "raceId int, year int, round int, circuitId int, name string, date date, "
    "_source_file string, _ingested_at timestamp"
)

RACE = dict(raceId="1", year="2009", round="1", circuitId="1", name="Australian GP",
            date="2009-03-29", time="06:00:00", _source_file="f.csv", _ingested_at=None)


def _races(spark, **overrides):
    return transform_races(spark.createDataFrame([_row(RACE, **overrides)], BRONZE_RACES_SCHEMA)).drop("time")

def test_races_casts_and_parses_date(spark):
    expected = spark.createDataFrame(
        [(1, 2009, 1, 1, "Australian GP", dt.date(2009, 3, 29), "f.csv", None)],
        SILVER_RACES_SCHEMA
    )
    assert_df_equality(_races(spark), expected)


def test_races_invalid_date_becomes_null(spark):
    expected = spark.createDataFrame(
        [(1, 2009, 1, 1, "Australian GP", None, "f.csv", None)],
        SILVER_RACES_SCHEMA
    )
    assert_df_equality(_races(spark, date="not-a-date"), expected)


@pytest.mark.xfail
def test_races_time_keeps_time_of_day(spark):
    df = spark.createDataFrame(
        [_row(RACE, raceId="1", time="06:00:00"), _row(RACE, raceId="2", time="13:00:00")],
        BRONZE_RACES_SCHEMA
    )
    assert transform_races(df).select("time").distinct().count() == 2
