# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

CATALOG_NAME = "dbr_dev_ua5816bd"
BRONZE_SCHEMA = "oles0305_bronze"
SILVER_SCHEMA = "oles0305_silver"

# COMMAND ----------

window_spec = Window.partitionBy(F.col("driverId")).orderBy(F.col("ingestion_timestamp").desc())

df_bronze = (   # new records from bronze
    spark.read.table(f"{CATALOG_NAME}.{BRONZE_SCHEMA}.drivers")
        .withColumn("rnk", F.row_number().over(window_spec))
        .filter(F.col("rnk") == 1)
        .drop("rnk", "source_filename", "load_date")
        .withColumnRenamed("ingestion_timestamp", "bronze_ingested_at")
)

# COMMAND ----------

silver_table = DeltaTable.forName(spark, f"{CATALOG_NAME}.{SILVER_SCHEMA}.drivers")

df_silver_active = (
    spark.read.table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.drivers")
        .filter(F.col("is_active") == True)
)

# COMMAND ----------

df_updates_to_existing = (
    df_bronze.alias("new")
        .join(
            df_silver_active.alias("cur"), 
            "driverId"
        )
        .filter(F.col("new.number") != F.col("cur.number"))
        .selectExpr("new.*")
        .withColumn("mergeKey", F.lit(None))
)

df_staged_updates = (
    df_bronze
        .withColumn("mergeKey", F.col("driverId"))
        .unionByName(df_updates_to_existing)
)

# COMMAND ----------

# SCD Type 2
(
    silver_table.alias("target")
        .merge(
            df_staged_updates.alias("source"), 
            "target.driverId = source.mergeKey"
        )
        .whenMatchedUpdate(
            condition="target.is_active = true AND target.number != source.number",
            set={
                "is_active": F.lit(False), 
                "valid_to": F.current_timestamp()
            }
        )
        .whenNotMatchedInsert(
            values={
                "driverId": "source.driverId", 
                "driverRef": "source.driverRef", 
                "number": "source.number",
                "code": "source.code", 
                "forename": "source.forename", 
                "surname": "source.surname",
                "dob": "source.dob", 
                "nationality": "source.nationality", 
                "url": "source.url",
                "bronze_ingested_at": "source.bronze_ingested_at",
                "valid_from": F.current_timestamp(), 
                "valid_to": F.lit(None), 
                "is_active": F.lit(True)
            }
        )
        .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbr_dev_ua5816bd.oles0305_silver.drivers