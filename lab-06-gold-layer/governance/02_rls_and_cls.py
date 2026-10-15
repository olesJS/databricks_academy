# Databricks notebook source
dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd", "Catalog Name")
dbutils.widgets.text("gold_schema", "oles0305_gold", "Gold Schema")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD = f"{CATALOG}.{gold_schema}"
ROLES = f"{GOLD}.access_roles"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row-Level Filtering

# COMMAND ----------

# Shows only results for the constructor team the user belongs to (constructor_viewer)
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {GOLD}.rls_by_constructor(inp_constructor_id INT)
    RETURN EXISTS (
        SELECT 1
        FROM {ROLES}
        WHERE user_email = current_user() AND
            (role IN ('admin', 'analyst') 
            OR 
            (role = 'constructor_viewer' AND constructor_id = inp_constructor_id))
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column-Level Masking

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {GOLD}.cls_dob_masking(inp_dob DATE)
    RETURN
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {ROLES}
            WHERE user_email = current_user() AND role = 'admin'
        ) THEN inp_dob
        ELSE make_date(year(inp_dob), 1, 1)
    END
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply RLS and CLS on tables

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {GOLD}.fact_results DROP ROW FILTER")
    spark.sql(f"ALTER TABLE {GOLD}.agg_constructor_season DROP ROW FILTER")
    spark.sql(f"ALTER TABLE {GOLD}.dim_driver ALTER COLUMN date_of_birth DROP MASK")
except Exception:
    pass

spark.sql(f"""ALTER TABLE {GOLD}.fact_results 
          SET ROW FILTER {GOLD}.rls_by_constructor ON (constructor_id)""")
spark.sql(f"""ALTER TABLE {GOLD}.agg_constructor_season 
          SET ROW FILTER {GOLD}.rls_by_constructor ON (constructor_id)""")
          
spark.sql(f"ALTER TABLE {GOLD}.dim_driver ALTER COLUMN date_of_birth SET MASK {GOLD}.cls_dob_masking")