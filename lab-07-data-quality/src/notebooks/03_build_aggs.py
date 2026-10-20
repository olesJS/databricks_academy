# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD = f"{CATALOG}.{gold_schema}"

# COMMAND ----------

df_results = spark.read.table(f"{GOLD}.fact_results").alias("results")

df_races = (
    spark.read.table(f"{GOLD}.dim_race")
        .select("race_id", "season_year")
        .alias("races")
)

# COMMAND ----------

facts_with_season = (
    df_results
        .join(
            df_races,
            on=F.col("results.race_id") == F.col("races.race_id"),
            how="inner"
        )
)

def metrics():
    return [
        F.countDistinct(F.col("results.race_id")).alias("races_count"),
        F.sum(F.when(F.col("results.position_order") == 1, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("results.position_order") <= 3, 1).otherwise(0)).alias("podiums"),
        F.sum(F.col("results.points")).alias("points_sum")
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_constructor_season

# COMMAND ----------

df_agg_constructor_season = (
    facts_with_season
        .groupBy(F.col("results.constructor_id"), F.col("races.season_year"))
        .agg(*metrics())
)

df_agg_constructor_season.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_constructor_season")

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_driver_season

# COMMAND ----------

df_agg_driver_season = (
    facts_with_season
        .groupBy(F.col("results.driver_id"), F.col("races.season_year"))
        .agg(
            *metrics(),
            F.sum(F.col("results.grid")).alias("grid_position_sum"),
            F.sum(F.col("results.position_order")).alias("position_order_sum")
        )
)

df_agg_driver_season.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_driver_season")

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_driver_circuit

# COMMAND ----------

df_agg_driver_circuit = (
    facts_with_season
        .groupBy(F.col("results.driver_id"), F.col("results.circuit_id"))
        .agg(
            *metrics(),
            F.sum(F.col("results.grid")).alias("grid_position_sum"),
            F.sum(F.col("results.position_order")).alias("position_order_sum")
        )
)

df_agg_driver_circuit.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_driver_circuit")