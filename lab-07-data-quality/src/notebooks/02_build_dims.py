# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from f1_pipeline.transforms.gold import (
    build_dim_race,
    build_dim_circuit,
    build_dim_constructor,
    build_dim_driver
)

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

# MAGIC %md
# MAGIC ### dim_race

# COMMAND ----------

df_silver_races = spark.read.table(f"{SILVER}.races_sdp")
df_gold_races = build_dim_race(df_silver_races)
df_gold_races.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_race")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_circuit

# COMMAND ----------

df_silver_circuits = spark.read.table(f"{SILVER}.circuits_sdp")
df_gold_circuits = build_dim_circuit(df_silver_circuits)
df_gold_circuits.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_circuit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_constructor

# COMMAND ----------

df_silver_constructors = spark.read.table(f"{SILVER}.constructors_sdp")
df_gold_constructors = build_dim_constructor(df_silver_constructors)
df_gold_constructors.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_constructor")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_driver

# COMMAND ----------

df_silver_drivers = spark.read.table(f"{SILVER}.drivers_sdp")
df_gold_drivers = build_dim_driver(df_silver_drivers)
df_gold_drivers.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_driver")