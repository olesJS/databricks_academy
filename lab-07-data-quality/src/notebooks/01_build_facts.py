# Databricks notebook source
import sys

dbutils.widgets.text("src_path", "", "Source path")
_src = dbutils.widgets.get("src_path")
if _src and _src not in sys.path:
    sys.path.insert(0, _src)

# COMMAND ----------

from f1_pipeline.transforms.gold import build_fact_results

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

df_silver_results = spark.read.table(f"{SILVER}.results_sdp")
df_silver_races = spark.read.table(f"{SILVER}.races_sdp")

df_gold_results = build_fact_results(df_silver_results, df_silver_races)

df_gold_results.write.mode("overwrite").saveAsTable(f"{GOLD}.fact_results")

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {GOLD}.data_volume_log
    SELECT 'fact_results'
        , COUNT(*)
        , current_timestamp()
    FROM {GOLD}.fact_results
""")