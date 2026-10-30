# Databricks notebook source
dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

CATALOG = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")
GOLD = f"{CATALOG}.{gold_schema}"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.data_volume_log (
        table_name  STRING      NOT NULL,
        rows_num    BIGINT      NOT NULL,
        log_ts      TIMESTAMP   NOT NULL
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate data volume drop in `fact_results`

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {GOLD}.data_volume_log
    SELECT 'fact_results'
        , 1000
        , current_timestamp()
    FROM {GOLD}.fact_results
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Volume drop simulation result:
# MAGIC ![](../screenshots/volume_drop_alert.png)