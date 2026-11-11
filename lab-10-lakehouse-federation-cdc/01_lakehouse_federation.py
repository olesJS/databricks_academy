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

# COMMAND ----------

scope = dbutils.widgets.get("secret_scope")
user_secret = dbutils.widgets.get("user_secret")
password_secret = dbutils.widgets.get("password_secret")
host = dbutils.widgets.get("host")
database = dbutils.widgets.get("database")
connection_name = dbutils.widgets.get("connection_name")
catalog_name = dbutils.widgets.get("catalog_name")

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

spark.sql("""
    SELECT *
    FROM oles0305_lab10_adventure_works.saleslt.customer
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

