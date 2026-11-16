# Databricks notebook source
dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd")
dbutils.widgets.text("bronze_schema", "oles0305_bronze")
dbutils.widgets.text("bronze_table", "results_zerobus")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
bronze_table = dbutils.widgets.get("bronze_table")

client_id = dbutils.secrets.get("oles0305-kv-scope", "oles0305-zerobus-client-id")
client_secret = dbutils.secrets.get("oles0305-kv-scope", "oles0305-zerobus-client-secret")

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

spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{client_id}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{bronze_schema} TO `{client_id}`")
spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog}.{bronze_schema}.{bronze_table} TO `{client_id}`")