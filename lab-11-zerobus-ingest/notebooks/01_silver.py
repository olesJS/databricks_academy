# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: idempotent incremental load

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dbr_dev_ua5816bd")
dbutils.widgets.text("bronze_schema", "oles0305_bronze")
dbutils.widgets.text("bronze_table", "results_zerobus")
dbutils.widgets.text("silver_schema", "oles0305_silver")
dbutils.widgets.text("silver_table", "results_zerobus")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
bronze_table = dbutils.widgets.get("bronze_table")
silver_schema = dbutils.widgets.get("silver_schema")
silver_table = dbutils.widgets.get("silver_table")

BRONZE_TABLE = f"{catalog}.{bronze_schema}.{bronze_table}"
SILVER_TABLE = f"{catalog}.{silver_schema}.{silver_table}"
CHECKPOINT = f"/Volumes/{catalog}/{silver_schema}/checkpoints/{silver_table}"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business columns
# MAGIC Columns that describe the race result itself. They are used to compute `_row_hash` (SHA-256 over a JSON of these columns)

# COMMAND ----------

BUSINESS_COLUMNS = [
    "race_id", "driver_id", "constructor_id", "number", "grid", "position", 
    "position_text", "position_order", "points", "laps", "time", "milliseconds", 
    "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "status_id"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert logic
# MAGIC Runs for every micro-batch read from bronze:
# MAGIC
# MAGIC 1. Deduplicate inside the batch — `row_number()` over `result_id`, latest `_produced_at` wins
# MAGIC 2. Compute `_row_hash` and `_processed_at`
# MAGIC 3. `MERGE INTO` silver on `result_id`:
# MAGIC    - key not in silver -> insert;
# MAGIC    - key exists, record is newer **and** its content changed -> update;
# MAGIC    - otherwise -> nothing happens.

# COMMAND ----------

def upsert_to_silver(batch_df, batch_id):
    w = Window.partitionBy("result_id").orderBy(F.col("_produced_at").desc())
    deduped = (
        batch_df
            .withColumn("rnk", F.row_number().over(w))
            .filter("rnk = 1")
            .drop("rnk", "_source_file")
            .withColumn("_row_hash", F.sha2(F.to_json(F.struct(*BUSINESS_COLUMNS)), 256))
            .withColumn("_processed_at", F.current_timestamp())
    )

    target = DeltaTable.forName(batch_df.sparkSession, SILVER_TABLE)
    (
        target.alias("t")
            .merge(
                deduped.alias("s"),
                "t.result_id = s.result_id"
            )
            .whenMatchedUpdateAll(
                "s._produced_at > t._produced_at AND s._row_hash != t._row_hash"
            )
            .whenNotMatchedInsertAll()
            .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the stream

# COMMAND ----------

(
    spark.readStream.table(BRONZE_TABLE)
        .writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
)