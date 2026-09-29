# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

CATALOG_NAME = "dbr_dev_ua5816bd"
BRONZE_SCHEMA = "oles0305_bronze"
SILVER_SCHEMA = "oles0305_silver"

# COMMAND ----------

df_bronze = spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.results")

df_clean = (
    df_bronze
        .filter(F.col("_rescued_data").isNull())
        .select(
            F.col("resultId").cast("int"),
            F.col("raceId").cast("int"),
            F.col("driverId").cast("int"),
            F.col("constructorId").cast("int"),
            F.col("grid").cast("int"),
            F.col("position").cast("int"),
            F.col("points").cast("double"),
            F.col("laps").cast("int"),
            F.col("milliseconds").cast("int"),
            F.col("statusId").cast("int"),
            F.col("time").cast("string"),
            F.col("ingested_at").alias("bronze_ingested_at")
        )
        .dropDuplicates("resultId")
)

# COMMAND ----------

silver_table = DeltaTable.forName(spark, f"{CATALOG_NAME}.{SILVER_SCHEMA}.results")

# COMMAND ----------

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