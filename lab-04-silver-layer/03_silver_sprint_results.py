# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from delta.tables import DeltaTable

# COMMAND ----------

CATALOG_NAME = "dbr_dev_ua5816bd"
BRONZE_SCHEMA = "oles0305_bronze"
SILVER_SCHEMA = "oles0305_silver"

# COMMAND ----------

def clean_nulls(col_name):
    return F.when(F.col(f"data.{col_name}") == "\\N", None).otherwise(F.col(f"data.{col_name}"))

# COMMAND ----------

json_schema = StructType([
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
    StructField("fastestLapTime", StringType()),
    StructField("statusId", StringType()),
    StructField("rank", StringType()),
    StructField("produced_at", StringType()),
])

df_bronze = spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.sprint_results")

df_clean = (
    df_bronze
        .withColumn("data", F.from_json(F.col("raw_json"), json_schema))
        .select(
            clean_nulls("resultId").cast("int").alias("resultId"),
            clean_nulls("raceId").cast("int").alias("raceId"),
            clean_nulls("driverId").cast("int").alias("driverId"),
            clean_nulls("constructorId").cast("int").alias("constructorId"),
            clean_nulls("grid").cast("int").alias("grid"),
            clean_nulls("position").cast("int").alias("position"),
            clean_nulls("points").cast("double").alias("points"),
            clean_nulls("laps").cast("int").alias("laps"),
            clean_nulls("time").cast("string").alias("time"),
            clean_nulls("milliseconds").cast("int").alias("milliseconds"),
            clean_nulls("statusId").cast("int").alias("statusId"),
            F.col("evh_timestamp").alias("evh_ingested_at")
        )
        .dropDuplicates(["resultId"])
)

# COMMAND ----------

silver_table = DeltaTable.forName(spark, f"{CATALOG_NAME}.{SILVER_SCHEMA}.sprint_results")

( # SCD Type 1
    silver_table.alias("target")
    .merge(
        df_clean.alias("source"),
        "target.resultId = source.resultId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)