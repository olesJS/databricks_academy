# Databricks notebook source
import sys

dbutils.widgets.text("src_path", "", "Source path")
_src = dbutils.widgets.get("src_path")
if _src and _src not in sys.path:
    sys.path.insert(0, _src)

# COMMAND ----------

from f1_pipeline.dq.constraints import CONSTRAINTS, apply_constraints

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

GOLD = f"{dbutils.widgets.get('catalog_name')}.{dbutils.widgets.get('gold_schema')}"

# COMMAND ----------

for table, spec in CONSTRAINTS.items():
    apply_constraints(spark, f"{GOLD}.{table}", spec)
    print(f"constraints applied: {table}")