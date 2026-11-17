# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Setup: Bronze and Silver tables for Zerobus ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters and Service Principal credentials

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd")

dbutils.widgets.text("bronze_schema", "oles0305_bronze")
dbutils.widgets.text("bronze_table", "results_zerobus")

dbutils.widgets.text("silver_schema", "oles0305_silver")
dbutils.widgets.text("silver_table", "results_zerobus")

dbutils.widgets.text("secret_scope", "oles0305-kv-scope")
dbutils.widgets.text("client_id_key", "oles0305-zerobus-client-id")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")

bronze_schema = dbutils.widgets.get("bronze_schema")
bronze_table = dbutils.widgets.get("bronze_table")

silver_schema = dbutils.widgets.get("silver_schema")
silver_table = dbutils.widgets.get("silver_table")

secret_scope = dbutils.widgets.get("secret_scope")
client_id_key = dbutils.widgets.get("client_id_key")

client_id = dbutils.secrets.get(secret_scope, client_id_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze table
# MAGIC Raw F1 results (`results.csv`) pushed directly by the producer through Zerobus Ingest.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.{bronze_table} (
        result_id          INT       NOT NULL,
        race_id            INT       NOT NULL,
        driver_id          INT       NOT NULL,
        constructor_id     INT       NOT NULL,
        number             INT,
        grid               INT       NOT NULL,
        position           INT,  
        position_text      STRING    NOT NULL,
        position_order     INT       NOT NULL,
        points             DOUBLE    NOT NULL,
        laps               INT       NOT NULL,
        time               STRING,  
        milliseconds       BIGINT,
        fastest_lap        INT,
        rank               INT,
        fastest_lap_time   STRING,
        fastest_lap_speed  DOUBLE,
        status_id          INT       NOT NULL,
        _source_file       STRING    NOT NULL,
        _produced_at       TIMESTAMP NOT NULL
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permissions for the producer

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{client_id}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{bronze_schema} TO `{client_id}`")
spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog}.{bronze_schema}.{bronze_table} TO `{client_id}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver table
# MAGIC One row per `result_id`. Loaded from bronze by a streaming job with `MERGE INTO`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.{silver_table} (
        result_id          INT       NOT NULL,
        race_id            INT       NOT NULL,
        driver_id          INT       NOT NULL,           
        constructor_id     INT       NOT NULL,
        number             INT,
        grid               INT       NOT NULL,
        position           INT,
        position_text      STRING    NOT NULL,
        position_order     INT       NOT NULL,
        points             DOUBLE    NOT NULL,
        laps               INT       NOT NULL,
        time               STRING,
        milliseconds       BIGINT,
        fastest_lap        INT,
        rank               INT,
        fastest_lap_time   STRING,
        fastest_lap_speed  DOUBLE,
        status_id          INT       NOT NULL,
        _row_hash          STRING    NOT NULL,
        _produced_at       TIMESTAMP NOT NULL,
        _processed_at      TIMESTAMP NOT NULL
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoints volume
# MAGIC Stores Structured Streaming checkpoints for the bronze to silver job

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{silver_schema}.checkpoints")