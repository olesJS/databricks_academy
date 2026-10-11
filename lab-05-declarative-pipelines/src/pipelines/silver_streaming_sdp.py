from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


def get_data_col(col_name, target_type=None):
    col = F.col(f"data.{col_name}").alias(col_name)
    if target_type:
        return col.cast(target_type)
    return col.alias(col_name)


json_results_schema = StructType([
    StructField("resultId", StringType()),
    StructField("raceId", StringType()),
    StructField("driverId", StringType()),
    StructField("constructorId", StringType()),
    StructField("number", StringType()),
    StructField("grid", StringType()),
    StructField("position", StringType()),
    StructField("positionText", StringType()),
    StructField("positionOrder", StringType()),
    StructField("points", StringType()),
    StructField("laps", StringType()),
    StructField("time", StringType()),
    StructField("milliseconds", StringType()),
    StructField("fastestLap", StringType()),
    StructField("rank", StringType()),
    StructField("fastestLapTime", StringType()),
    StructField("fastestLapSpeed", StringType()),
    StructField("statusId", StringType()),
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
                get_data_col("resultId", IntegerType()),
                get_data_col("raceId", IntegerType()),
                get_data_col("driverId", IntegerType()),
                get_data_col("constructorId", IntegerType()),
                get_data_col("grid", IntegerType()),
                get_data_col("position", IntegerType()),
                get_data_col("positionOrder", StringType()),
                get_data_col("points", IntegerType()),
                get_data_col("laps", IntegerType()),
                get_data_col("milliseconds", LongType()),
                get_data_col("fastestLapSpeed", DoubleType()),
                F.col("_ingested_at"),
            )
    )
