# Databricks notebook source
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

last_version = (
    spark.read.table(wm_table)
        .filter(f"source_table == '{src_table}'")
        .collect()[0]["last_version"]
)

current_version = (
    spark.sql(f"""
        DESCRIBE HISTORY {src_table}
    """)
    .orderBy("version", ascending=False).
    limit(1).collect()[0].version
)

if current_version == last_version:
    dbutils.notebook.exit("No new changes")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("CustomerID")).orderBy(F.col("_commit_version").desc())

changes = (
    spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last_version + 1)
        .option("endingVersion", current_version)
        .table(src_table)
        .filter("_change_type != 'update_preimage'")
)

latest_changes = (
    changes
        .withColumn("rnk", F.row_number().over(w))
        .filter("rnk = 1")
        .drop("rnk")
)

latest_changes.createOrReplaceTempView("v_changes")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {tgt_table} AS t
    USING (
        SELECT 
            c.CustomerID AS merge_key
            , c.*
        FROM v_changes c

        UNION ALL

        SELECT 
            NULL AS merge_key
            , c.*
        FROM v_changes c
        JOIN {tgt_table} t ON (c.CustomerID = t.CustomerID AND t.valid_to IS NULL)
        WHERE c._change_type IN ('insert', 'update_postimage')
            AND NOT (
                c.FirstName <=> t.FirstName
                AND c.LastName <=> t.LastName
                AND c.CompanyName <=> t.CompanyName
                AND c.EmailAddress <=> t.EmailAddress
            )
    ) AS s
    ON (t.CustomerID = s.merge_key AND t.valid_to IS NULL)
    
    WHEN MATCHED AND (
        s._change_type = 'delete'
        OR NOT (
            s.FirstName <=> t.FirstName
            AND s.LastName <=> t.LastName
            AND s.CompanyName <=> t.CompanyName
            AND s.EmailAddress <=> t.EmailAddress
        )
    ) THEN
        UPDATE SET t.valid_to = s._commit_timestamp
    
    WHEN NOT MATCHED AND s._change_type IN ('insert', 'update_postimage') THEN
        INSERT (CustomerID, FirstName, LastName, CompanyName, EmailAddress, valid_from, valid_to)
        VALUES (s.CustomerID, s.FirstName, s.LastName, s.CompanyName, s.EmailAddress, 
            s._commit_timestamp + INTERVAL 1 SECOND, NULL)
""")

# COMMAND ----------

spark.sql(f"""
    UPDATE {wm_table}
    SET last_version = {current_version}
    WHERE source_table = '{src_table}'
""")