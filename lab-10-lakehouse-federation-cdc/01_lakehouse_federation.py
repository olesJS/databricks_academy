# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Federation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

dbutils.widgets.text("secret_scope", "oles0305-kv-scope")
dbutils.widgets.text("user_secret", "oles0305-sql-reader")
dbutils.widgets.text("password_secret", "oles0305-sql-password")
dbutils.widgets.text("host", "sql-oles0305-lab10.database.windows.net")
dbutils.widgets.text("database", "adventure_works")
dbutils.widgets.text("connection_name", "oles0305_sqlserver_aw")
dbutils.widgets.text("catalog_name", "oles0305_lab10_adventure_works")
dbutils.widgets.text("delta_catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("delta_schema", "oles0305_lab10")

# COMMAND ----------

scope = dbutils.widgets.get("secret_scope")
user_secret = dbutils.widgets.get("user_secret")
password_secret = dbutils.widgets.get("password_secret")
host = dbutils.widgets.get("host")
database = dbutils.widgets.get("database")
connection_name = dbutils.widgets.get("connection_name")
catalog_name = dbutils.widgets.get("catalog_name")
delta_catalog = dbutils.widgets.get("delta_catalog")
delta_schema = dbutils.widgets.get("delta_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connection and Foreign Catalog

# COMMAND ----------

spark.sql(f"""
    CREATE CONNECTION IF NOT EXISTS {connection_name} TYPE sqlserver
    OPTIONS (
        host '{host}',
        port '1433',
        user secret('{scope}', '{user_secret}'),
        password secret('{scope}', '{password_secret}')
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE FOREIGN CATALOG IF NOT EXISTS {catalog_name}
    USING CONNECTION {connection_name}
    OPTIONS(
        database '{database}'
    )  
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore the foreign catalog

# COMMAND ----------

spark.sql(f"SHOW SCHEMAS IN {catalog_name}").display()

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog_name}.saleslt").display()

# COMMAND ----------

spark.sql(f"""
    SELECT *
    FROM {catalog_name}.saleslt.customer
    LIMIT 10   
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pushdown analysis

# COMMAND ----------

spark.sql(f"""
    EXPLAIN FORMATTED
    SELECT SalesOrderID, CustomerID, TotalDue
    FROM {catalog_name}.saleslt.salesorderheader
    WHERE TotalDue > 10000
    ORDER BY TotalDue DESC
    LIMIT 5;
""").display()

# COMMAND ----------

spark.sql(f"""
    EXPLAIN FORMATTED
    SELECT CustomerID
        , COUNT(*) AS sales_cnt
        , SUM(TotalDue) AS total_due
    FROM {catalog_name}.saleslt.salesorderheader
    GROUP BY CustomerID
    HAVING SUM(TotalDue) > 10000
    ORDER BY total_due DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest to Delta

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {delta_catalog}.{delta_schema}.customer_snapshot AS
    SELECT *
        , CURRENT_TIMESTAMP() AS _ingested_at
    FROM {catalog_name}.saleslt.customer
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {delta_catalog}.{delta_schema}.salesorderheader_snapshot AS
    SELECT *
        , CURRENT_TIMESTAMP() AS _ingested_at
    FROM {catalog_name}.saleslt.salesorderheader
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Federation vs Delta

# COMMAND ----------

import time

def run_timed(query, src_table, tag):
    row = [tag, src_table]
    for i in range(2):
        start = time.perf_counter()
        spark.sql(query.format(src_table=src_table)).collect()
        if i == 1:
            row.append(time.perf_counter() - start)
    return row

# COMMAND ----------

import pandas as pd

soh_query = """
    SELECT CustomerID
        , COUNT(*) AS sales_cnt
        , SUM(TotalDue) as total_due
    FROM {src_table}
    GROUP BY CustomerID
"""

customer_query = """
    SELECT *
    FROM {src_table}
    WHERE Title = 'Mr.'
"""

results = []
results.append(run_timed(soh_query, src_table=f"{catalog_name}.saleslt.salesorderheader", tag="federation"))
results.append(run_timed(soh_query, src_table=f"{delta_catalog}.{delta_schema}.salesorderheader_snapshot", tag="delta"))

results.append(run_timed(customer_query, src_table=f"{catalog_name}.saleslt.customer", tag="federation"))
results.append(run_timed(customer_query, src_table=f"{delta_catalog}.{delta_schema}.customer_snapshot", tag="delta"))

timing_df = pd.DataFrame(results, columns=["tag", "src_table", "time"])
timing_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join an External table with a Delta table

# COMMAND ----------

join_query = f"""
    SELECT c.CustomerID
        , c.FirstName
        , c.LastName
        , c.CompanyName
        , soh.SalesOrderId
        , soh.OrderDate
        , soh.TotalDue
    FROM {catalog_name}.saleslt.salesorderheader soh
    JOIN {delta_catalog}.{delta_schema}.customer_snapshot c ON soh.CustomerID = c.CustomerID
    ORDER BY soh.TotalDue DESC
"""

spark.sql(join_query).display()

# COMMAND ----------

spark.sql(f"EXPLAIN FORMATTED {join_query}").display()