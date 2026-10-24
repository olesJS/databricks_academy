# Databricks notebook source
import os
import sys
from f1_pipeline.dq.constraints import CONSTRAINTS, apply_constraints

# COMMAND ----------

sys.path.append(os.path.abspath(".."))

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

GOLD = f"{dbutils.widgets.get('catalog_name')}.{dbutils.widgets.get('gold_schema')}"

# COMMAND ----------

for table, spec in CONSTRAINTS.items():
    apply_constraints(spark, f"{GOLD}.{table}", spec)
    print(f"constraints applied: {table}")