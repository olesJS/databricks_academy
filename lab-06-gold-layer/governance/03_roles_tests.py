# Databricks notebook source
dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")
dbutils.widgets.dropdown("role", "admin", ["admin", "analyst", "constructor_viewer"])
dbutils.widgets.text("constructor_id", "6")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD = f"{CATALOG}.{gold_schema}"
ROLES = f"{GOLD}.access_roles"

curr_role = dbutils.widgets.get("role")
curr_constructor_id = dbutils.widgets.get("constructor_id")

# COMMAND ----------

cid = curr_constructor_id if curr_constructor_id else "NULL"

spark.sql(f"""
    UPDATE {ROLES}
    SET role = '{curr_role}', constructor_id = {cid}
    WHERE user_email = current_user()
""")

display(spark.table(ROLES))

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS rows
        , COUNT(DISTINCT constructor_id) AS constructors
    FROM {GOLD}.fact_results
"""))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS rows FROM {GOLD}.agg_constructor_season"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT driver_id
        , forename
        , surname
        , date_of_birth
    FROM {GOLD}.dim_driver
    LIMIT 10
"""))

# COMMAND ----------

if curr_role != "admin":
    spark.sql(f"UPDATE {ROLES} SET role = 'admin' WHERE user_email = current_user()")