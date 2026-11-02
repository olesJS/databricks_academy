# Databricks notebook source
from f1_pipeline.dq.constraints import CONSTRAINTS, apply_constraints

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("gold_schema", "", "Gold Schema")

CATALOG = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD = f"{CATALOG}.{gold_schema}"

# COMMAND ----------

for table, spec in CONSTRAINTS.items():
    apply_constraints(spark, f"{GOLD}.{table}", spec)
    print(f"constraints applied: {table}")