# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

dbutils.widgets.text("delta_catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("delta_schema", "oles0305_lab10")

# COMMAND ----------

delta_catalog = dbutils.widgets.get("delta_catalog")
delta_schema = dbutils.widgets.get("delta_schema")

base = f"{delta_catalog}.{delta_schema}"
src_table = f"{base}.customer_src"
tgt_table = f"{base}.customer_scd2"
wm_table = f"{base}.cdc_watermark"

cols = ["CustomerID", "FirstName", "LastName", "CompanyName", "EmailAddress"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC source with Change Data Feed

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {src_table}
    TBLPROPERTIES(delta.enableChangeDataFeed = true)
    AS
    SELECT *
    FROM {base}.customer_snapshot
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 target and watermark
# MAGIC The target is initialised from the source state at baseline_version.
# MAGIC The watermark stores the last processed source version, so the pipeline reads only new changes.

# COMMAND ----------

baseline_version = (
    spark.sql(f"""
        DESCRIBE HISTORY {src_table}
    """)
    .orderBy("version", ascending=False).
    limit(1).collect()[0].version
)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_table}
    AS
    SELECT {", ".join(cols)}
        , _ingested_at AS valid_from
        , CAST(NULL AS TIMESTAMP) AS valid_to
    FROM {src_table} VERSION AS OF {baseline_version}
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {wm_table}
    AS
    SELECT '{src_table}' AS source_table
        , {baseline_version} AS last_version
""")