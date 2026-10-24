# Databricks notebook source
import sys

dbutils.widgets.text("src_path", "", "Source path")
_src = dbutils.widgets.get("src_path")
if _src and _src not in sys.path:
    sys.path.insert(0, _src)

# COMMAND ----------

from f1_pipeline.transforms.gold import (
    build_agg_constructor_season,
    build_agg_driver_season,
    build_agg_driver_circuit
)

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

# MAGIC %md
# MAGIC ### agg_constructor_season

# COMMAND ----------

df_agg_constructor_season = build_agg_constructor_season(df_results, df_races)
df_agg_constructor_season.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_constructor_season")

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_driver_season

# COMMAND ----------

df_agg_driver_season = build_agg_driver_season(df_results, df_races)
df_agg_driver_season.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_driver_season")

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_driver_circuit

# COMMAND ----------

df_agg_driver_circuit = build_agg_driver_circuit(df_results, df_races)
df_agg_driver_circuit.write.mode("overwrite").saveAsTable(f"{GOLD}.agg_driver_circuit")