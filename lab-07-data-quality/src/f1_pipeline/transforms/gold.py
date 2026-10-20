from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window


# fact table
def _replace_pit_lane_start(df: DataFrame) -> DataFrame:
    race_window = Window.partitionBy(F.col("raceId"))
    # grid = 0 (pit lane start) is replaced with the slot after the last grid position of the race
    return df.withColumn( 
        "grid", 
        F.when(F.col("grid") > 0, F.col("grid"))
            .otherwise(F.max("grid").over(race_window) + 1)
    )


def build_fact_results(df_results: DataFrame, df_races: DataFrame) -> DataFrame:
    res = _replace_pit_lane_start(df_results).alias("results")
    rac = df_races.alias("races")

    return (
        res
        .join(rac, on=F.col("races.raceId") == F.col("results.raceId"), how="inner")
        .select(
            F.col("results.resultId").alias("result_id"),
            F.col("results.raceId").alias("race_id"),
            F.col("results.driverId").alias("driver_id"),
            F.col("results.constructorId").alias("constructor_id"),
            F.col("races.circuitId").alias("circuit_id"),
            F.col("grid"),
            F.col("results.position"),
            F.col("results.positionOrder").alias("position_order"),
            F.col("results.points"),
            F.col("results.laps"),
            F.col("results.milliseconds"),
            F.col("results.fastestLapSpeed").alias("fastest_lap_speed")
        )
    )


# dimensions
def build_dim_race(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("raceId").alias("race_id"),
        F.col("year").alias("season_year"),
        F.col("round").alias("season_round"),
        F.col("name").alias("race_name"),
        F.col("date").alias("race_date"),
    )


def build_dim_circuit(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("circuitId").alias("circuit_id"),
        F.col("circuitRef").alias("circuit_ref"),
        F.col("name").alias("circuit_name"),
        F.col("country"),
        F.col("location"),
        F.col("lat"),
        F.col("lng"),
        F.col("alt")
    )


def build_dim_constructor(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("constructorId").alias("constructor_id"),
        F.col("constructorRef").alias("constructor_ref"),
        F.col("name").alias("constructor_name"),
        F.col("nationality").alias("constructor_nationality")
    )


def build_dim_driver(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("driverId").alias("driver_id"),
        F.col("driverRef").alias("driver_ref"),
        F.col("number").alias("driver_number"),
        F.col("code").alias("driver_code"),
        F.col("forename"),
        F.col("surname"),
        F.col("dateOfBirth").alias("date_of_birth"),
        F.col("nationality").alias("driver_nationality"),
        F.col("__START_AT").alias("valid_from"),
        F.col("__END_AT").alias("valid_to")
    )
    .withColumn(
        "is_current", 
        F.when(F.col("valid_to").isNull(), 1).otherwise(0)
    )


# aggregations
def _metrics():
    return [
        F.countDistinct(F.col("results.race_id")).alias("races_count"),
        F.sum(F.when(F.col("results.position_order") == 1, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("results.position_order") <= 3, 1).otherwise(0)).alias("podiums"),
        F.sum(F.col("results.points")).alias("points_sum")
    ]

def _position_sums():
    return [
        F.sum(F.col("results.grid")).alias("grid_position_sum"),
        F.sum(F.col("results.position_order")).alias("position_order_sum")
    ]


def _facts_with_season(df_results: DataFrame, df_races: DataFrame) -> DataFrame:
    return (
        df_results.alias("results")
            .join(
                df_races.alias("races"),
                on=F.col("race_id") == F.col("race_id"),
                how="inner"
            )
    )


def build_agg_constructor_season(df_results: DataFrame, df_races: DataFrame) -> DataFrame:
    return (
        _facts_with_season(df_results, df_races)
            .groupBy(F.col("results.constructor_id"), F.col("races.season_year"))
            .agg(*_metrics())
    )


def build_agg_driver_season(df_results: DataFrame, df_races: DataFrame) -> DataFrame:
    return (
        _facts_with_season(df_results, df_races)
            .groupBy(F.col("results.driver_id"), F.col("races.season_year"))
            .agg(
                *_metrics(),
                *_position_sums()
            )
    )


def build_agg_driver_circuit(df_results: DataFrame, df_races: DataFrame) -> DataFrame:
    return (
        _facts_with_season(df_results, df_races)
            .groupBy(F.col("results.driver_id"), F.col("results.circuit_id"))
            .agg(
                *_metrics(),
                *_position_sums()
            )
    )

