from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


def get_data_col(col_name):
    return F.col(f"data.{col_name}").alias(col_name)

json_results_schema = StructType([
    StructField("resultId", IntegerType()),
    StructField("raceId", IntegerType()),
    StructField("driverId", IntegerType()),
    StructField("constructorId", IntegerType()),
    StructField("number", StringType()),
    StructField("grid", IntegerType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("positionOrder", StringType()),
    StructField("points", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("time", StringType()),
    StructField("milliseconds", LongType()),
    StructField("fastestLap", IntegerType()),
    StructField("rank", StringType()),
    StructField("fastestLapTime", StringType()),
    StructField("fastestLapSpeed", DoubleType()),
    StructField("statusId", IntegerType()),
])


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.results_sdp",
    comment=f"Silver table for results"
)
def silver_results() -> DataFrame:
    df_results_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.results_sdp")

    return (
        df_results_bronze
            .withColumn("data", F.from_json(F.col("raw_json"), schema=json_results_schema))
            .select(
                get_data_col("resultId"),
                get_data_col("raceId"),
                get_data_col("driverId"),
                get_data_col("constructorId"),
                get_data_col("grid"),
                get_data_col("position"),
                get_data_col("positionOrder"),
                get_data_col("points"),
                get_data_col("laps"),
                get_data_col("milliseconds"),
                get_data_col("fastestLapSpeed"),
                F.col("_ingested_at")
            )
    )
