from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

RESULTS_JSON_SCHEMA = StructType([
    StructField(name, StringType()) for name in [
        "resultId", "raceId", "driverId", "constructorId", "number", "grid",
        "position", "positionText", "positionOrder", "points", "laps", "time",
        "milliseconds", "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed",
        "statusId",
    ]
])


def transform_circuits(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("circuitId").cast(IntegerType()),
        F.col("circuitRef"),
        F.col("name"),
        F.col("location"),
        F.col("country"),
        F.col("lat").cast(DoubleType()),
        F.col("lng").cast(DoubleType()),
        F.col("alt").cast(IntegerType()),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def transform_constructors(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("constructorId").cast(IntegerType()),
        F.col("constructorRef"),
        F.col("name"),
        F.col("nationality"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def transform_races(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("raceId").cast(IntegerType()),
        F.col("year").cast(IntegerType()),
        F.col("round").cast(IntegerType()),
        F.col("circuitId").cast(IntegerType()),
        F.col("name"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.to_date(F.col("time"), "HH:mm:ss").alias("time"),
        F.col("_source_file"),
        F.col("_ingested_at")
    )


def transform_drivers(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("driverId").cast(IntegerType()),
        F.col("driverRef"),
        F.col("number").cast(IntegerType()),
        F.col("code"),
        F.col("forename"),
        F.col("surname"),
        F.to_date(F.col("dob"), "yyyy-MM-dd").alias("dateOfBirth"),
        F.col("nationality"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def _get_data_col(col_name, target_type=None):
    col = F.col(f"data.{col_name}").alias(col_name)
    if target_type:
        return col.cast(target_type)
    return col.alias(col_name)


def parse_results(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("data", F.from_json(F.col("raw_json"), schema=RESULTS_JSON_SCHEMA))
        .select(
            _get_data_col("resultId", IntegerType()),
            _get_data_col("raceId", IntegerType()),
            _get_data_col("driverId", IntegerType()),
            _get_data_col("constructorId", IntegerType()),
            _get_data_col("grid", IntegerType()),
            _get_data_col("position", IntegerType()),
            _get_data_col("positionOrder", StringType()),
            _get_data_col("points", IntegerType()),
            _get_data_col("laps", IntegerType()),
            _get_data_col("milliseconds", LongType()),
            _get_data_col("fastestLapSpeed", DoubleType()),
            F.col("_ingested_at"),
        )
    )
