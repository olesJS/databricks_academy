# Databricks notebook source
dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD = f"{CATALOG}.{gold_schema}"

# COMMAND ----------

gold_tables = [
    "fact_results", "dim_race", "dim_driver", "dim_circuit", "dim_constructor",
    "agg_constructor_season", "agg_driver_circuit", "agg_driver_season"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Grant permissions to objects

# COMMAND ----------

dbutils.widgets.text("admins_group", "UA_5816_BigData")
dbutils.widgets.text("analysts_group", "students")

admins_group = dbutils.widgets.get("admins_group")
analysts_group = dbutils.widgets.get("analysts_group")

# COMMAND ----------

spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {GOLD} TO {admins_group}")

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {GOLD} TO {analysts_group}")

for table in gold_tables:
    spark.sql(f"GRANT SELECT ON TABLE {GOLD}.{table} TO {analysts_group}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Access Roles

# COMMAND ----------

access_roles = f"{GOLD}.access_roles"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {access_roles} (
        user_email STRING,
        role STRING,            -- admin, analyst, constructor_viewer
        constructor_id INT      -- only for constructor_viewer
    )
""")

# COMMAND ----------

spark.sql(f"DELETE FROM {access_roles} WHERE user_email = current_user()")
spark.sql(f"INSERT INTO {access_roles} VALUES (current_user(), 'admin', NULL)")