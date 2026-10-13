# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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

# MAGIC %md
# MAGIC ### dim_race

# COMMAND ----------

df_silver_races = spark.read.table(f"{SILVER}.races_sdp")

df_gold_races = (
    df_silver_races.select(
        F.col("raceId").alias("race_id"),
        F.col("year").alias("season_year"),
        F.col("round").alias("season_round"),
        F.col("name").alias("race_name"),
        F.col("date").alias("race_date"),
    )
)

df_gold_races.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_race")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_circuit

# COMMAND ----------

df_silver_circuits = spark.read.table(f"{SILVER}.circuits_sdp")

df_gold_circuits = (
    df_silver_circuits.select(
        F.col("circuitId").alias("circuit_id"),
        F.col("circuitRef").alias("circuit_ref"),
        F.col("name").alias("circuit_name"),
        F.col("location"),
        F.col("lat"),
        F.col("lng"),
        F.col("alt")
    )
)

df_gold_circuits.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_circuit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_constructor

# COMMAND ----------

df_silver_constructors = spark.read.table(f"{SILVER}.constructors_sdp")

df_gold_constructors = (
    df_silver_constructors.select(
        F.col("constructorId").alias("constructor_id"),
        F.col("constructorRef").alias("constructor_ref"),
        F.col("name").alias("constructor_name"),
        F.col("nationality").alias("constructor_nationality")
    )
)

df_gold_constructors.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_constructor")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_driver

# COMMAND ----------

df_silver_drivers = spark.read.table(f"{SILVER}.drivers_sdp")

df_gold_drivers = (
    df_silver_drivers.select(
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
)

df_gold_drivers.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_driver")