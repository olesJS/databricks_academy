from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Window


RESULTS_JSON_SCHEMA = StructType([
    StructField(name, StringType()) for name in [
        "resultId", "raceId", "driverId", "constructorId", "number", "grid",
        "position", "positionText", "positionOrder", "points", "laps", "time",
        "milliseconds", "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed",
        "statusId",
    ]
])


def _try_cast(expr: str, sql_type: str, alias: str):
    return F.expr(f"try_cast({expr} AS {sql_type})").alias(alias)

def _try_date(col_name: str, fmt: str):
    return F.expr(f"try_to_timestamp(`{col_name}`, '{fmt}')").cast("date").alias(col_name)


def transform_circuits(df: DataFrame) -> DataFrame:
    return df.select(
        _try_cast("circuitId", "INT", "circuitId"),
        F.col("circuitRef"),
        F.col("name"),
        F.col("location"),
        F.col("country"),
        _try_cast("lat", "DOUBLE", "lat"),
        _try_cast("lng", "DOUBLE", "lng"),
        _try_cast("alt", "INT", "alt"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def transform_constructors(df: DataFrame) -> DataFrame:
    return df.select(
        _try_cast("constructorId", "INT", "constructorId"),
        F.col("constructorRef"),
        F.col("name"),
        F.col("nationality"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def transform_races(df: DataFrame) -> DataFrame:
    return df.select(
        _try_cast("raceId", "INT", "raceId"),
        _try_cast("year", "INT", "year"),
        _try_cast("round", "INT", "round"),
        _try_cast("circuitId", "INT", "circuitId"),
        F.col("name"),
        _try_date("date", "yyyy-MM-dd"),
        _try_date("time", "HH:mm:ss").alias("time"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def transform_drivers(df: DataFrame) -> DataFrame:
    return df.select(
        _try_cast("driverId", "INT", "driverId"),
        F.col("driverRef"),
        _try_cast("number", "INT", "number"),
        F.col("code"),
        F.col("forename"),
        F.col("surname"),
        _try_date("dob", "yyyy-MM-dd").alias("dateOfBirth"),
        F.col("nationality"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def _get_data_col(col_name, target_type=None):
    if target_type:
        return _try_cast(f"data.{col_name}", target_type, col_name)
    return F.col(f"data.{col_name}").alias(col_name)


def deduplicate_results(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("resultId").orderBy(F.col("_ingested_at").desc())
    keyed = (
        df.where(F.col("resultId").isNotNull())
          .withColumn("_rn", F.row_number().over(w))
          .where("_rn = 1")
          .drop("_rn")
    )
    return keyed.unionByName(df.where(F.col("resultId").isNull()))


def parse_results(df: DataFrame, keep_raw: bool = False) -> DataFrame:
    extra = [F.col("raw_json")] if keep_raw else []
    return (
        df
        .withColumn("data", F.from_json(F.col("raw_json"), schema=RESULTS_JSON_SCHEMA))
        .select(
            _get_data_col("resultId", "INT"),
            _get_data_col("raceId", "INT"),
            _get_data_col("driverId", "INT"),
            _get_data_col("constructorId", "INT"),
            _get_data_col("grid", "INT"),
            _get_data_col("position", "INT"),
            _get_data_col("positionOrder"),
            _get_data_col("points", "INT"),
            _get_data_col("laps", "INT"),
            _get_data_col("milliseconds", "LONG"),
            _get_data_col("fastestLapSpeed", "DOUBLE"),
            F.col("_ingested_at"),
            *extra
        )
    )

