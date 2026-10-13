# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("silver_schema", "oles0305_silver", "Silver Schema")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")

silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

SILVER = f"{CATALOG}.{silver_schema}"
GOLD = f"{CATALOG}.{gold_schema}"

# COMMAND ----------

df_silver_results = spark.read.table(f"{SILVER}.results_sdp").alias("results")
df_silver_races = spark.read.table(f"{SILVER}.races_sdp").alias("races")

df_gold_results = (
    df_silver_results
        .join(
            df_silver_races, 
            on=F.col("races.raceId") == F.col("results.raceId"),
            how="inner"
        )
        .select(
            F.col("results.resultId").alias("result_id"),
            F.col("results.raceId").alias("race_id"),
            F.col("results.driverId").alias("driver_id"),
            F.col("results.constructorId").alias("constructor_id"),
            F.col("races.circuitId").alias("circuit_id"),
            F.col("results.grid"),
            F.col("results.position"),
            F.col("results.positionOrder").alias("position_order"),
            F.col("results.points"),
            F.col("results.laps"),
            F.col("results.milliseconds"),
            F.col("results.fastestLapSpeed").alias("fastest_lap_speed")
        )
)

df_gold_results.write.mode("overwrite").saveAsTable(f"{GOLD}.fact_results")