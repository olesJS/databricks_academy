# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest: Volume to Bronze
# MAGIC Incrementally loads raw Wikipedia JSON files into `wiki_pages` using Auto Loader

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "f1_rag_bronze")
dbutils.widgets.text("raw_volume", "raw")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
raw_volume = dbutils.widgets.get("raw_volume")

# COMMAND ----------

VOLUME_ROOT = f"/Volumes/{catalog}/{bronze_schema}/{raw_volume}"
SOURCE_PATH = f"{VOLUME_ROOT}/wiki/"
CHECKPOINT = f"{VOLUME_ROOT}/_checkpoints/bronze_wiki_pages"
BRONZE_TABLE = f"{catalog}.{bronze_schema}.wiki_pages"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read new files with Auto Loader

# COMMAND ----------

raw_schema = StructType([
    StructField("page_id", LongType(), False),
    StructField("title", StringType(), False),
    StructField("requested_title", StringType(), True),
    StructField("url", StringType(), False),
    StructField("revision_id", LongType(), False),
    StructField("text", StringType(), False),
    StructField("category", StringType(), False),
    StructField("fetched_at", StringType(), False),
])

# COMMAND ----------

pages_stream = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .schema(raw_schema)
        .load(SOURCE_PATH)
        .select(
            "page_id", "title", "requested_title", "category", "url", "revision_id",
            F.to_timestamp("fetched_at").alias("fetched_at"),
            "text",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at")
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append to Bronze

# COMMAND ----------

(
    pages_stream.writeStream
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
        .awaitTermination()
)