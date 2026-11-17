# Databricks notebook source
# MAGIC %md
# MAGIC # Idempotency checks
# MAGIC Four checks that prove the bronze → silver load is idempotent.
# MAGIC
# MAGIC Bronze may contain duplicates (at-least-once delivery, producer re-runs) — silver must not.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd")
dbutils.widgets.text("bronze_schema", "oles0305_bronze")
dbutils.widgets.text("bronze_table", "results_zerobus")
dbutils.widgets.text("silver_schema", "oles0305_silver")
dbutils.widgets.text("silver_table", "results_zerobus")
dbutils.widgets.text("expected_rows", "26759")  # results.csv contains 26759 rows

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get('bronze_schema')
bronze_table = dbutils.widgets.get('bronze_table')
silver_schema = dbutils.widgets.get('silver_schema')
silver_table = dbutils.widgets.get('silver_table')

BRONZE_TABLE = f"{catalog}.{bronze_schema}.{bronze_table}"
SILVER_TABLE = f"{catalog}.{silver_schema}.{silver_table}"
EXPECTED_ROWS = int(dbutils.widgets.get("expected_rows"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. No duplicates in silver
# MAGIC Every `result_id` must appear exactly once — expected 0.

# COMMAND ----------

silver_duplicates = spark.sql(f"""
    SELECT COUNT(*) FROM (
        SELECT result_id 
        FROM {SILVER_TABLE} 
        GROUP BY result_id 
        HAVING count(*) > 1
    )
""").first()[0]
 
print(f"Duplicate result_id in silver: {silver_duplicates}")
assert silver_duplicates == 0, f"Silver contains {silver_duplicates} duplicate result_id"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Completeness
# MAGIC  Silver must contain every row of `results.csv` — nothing lost in the producer or in `MERGE`

# COMMAND ----------

silver_rows = spark.table(SILVER_TABLE).count()

print(f"Silver rows: {silver_rows} (expected {EXPECTED_ROWS})")
assert silver_rows == EXPECTED_ROWS, f"Expected {EXPECTED_ROWS} rows in silver, got {silver_rows}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Duplicates in bronze
# MAGIC How many extra copies arrived in bronze. 
# MAGIC
# MAGIC Together with check 1:
# MAGIC *"bronze has N duplicates, silver has 0"*.

# COMMAND ----------

bronze_rows, bronze_keys = spark.sql(f"""
    SELECT COUNT(*)
        , COUNT(DISTINCT result_id) 
    FROM {BRONZE_TABLE}
""").first()
 
print(f"Bronze rows: {bronze_rows}")
print(f"Bronze duplicate rows: {bronze_rows - bronze_keys}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Last MERGE metrics
# MAGIC From the Delta transaction log. 
# MAGIC
# MAGIC After re-sending the same data both values must be 0: the re-run changed nothing.

# COMMAND ----------

last_merge = (
    spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}")
    .filter("operation = 'MERGE'")
    .orderBy("version", ascending=False)
    .first()
)
 
if last_merge is None:
    print("No MERGE in history yet")
else:
    metrics = last_merge["operationMetrics"]
    print(f"Last MERGE (version {last_merge['version']}):")
    print(f"- inserted: {metrics['numTargetRowsInserted']}")
    print(f"- updated:  {metrics['numTargetRowsUpdated']}")