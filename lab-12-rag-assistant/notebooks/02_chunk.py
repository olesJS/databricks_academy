# Databricks notebook source
# MAGIC %md
# MAGIC # Chunking: Bronze to Silver

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "f1_rag_bronze")
dbutils.widgets.text("silver_schema", "f1_rag_silver")
dbutils.widgets.text("raw_volume", "raw")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
raw_volume = dbutils.widgets.get("raw_volume")

# COMMAND ----------

BRONZE_TABLE = f"{catalog}.{bronze_schema}.wiki_pages"
SILVER_TABLE = f"{catalog}.{silver_schema}.wiki_chunks"
CHECKPOINT = f"/Volumes/{catalog}/{bronze_schema}/{raw_volume}/_checkpoints/silver_wiki_chunks"

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from rag.chunking import chunk_document
from rag.config import CHUNK_SIZES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chunk and merge one micro-batch

# COMMAND ----------

CHUNK_SCHEMA = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("chunk_size", IntegerType(), False),
    StructField("page_id", LongType(), False),
    StructField("title", StringType(), False),
    StructField("category", StringType(), False),
    StructField("section", StringType(), False),
    StructField("chunk_index", IntegerType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("token_count", IntegerType(), False),
    StructField("url", StringType(), False),
    StructField("revision_id", LongType(), False),
])

# COMMAND ----------

def chunk_and_merge(batch_df, batch_id):
    w = Window.partitionBy("page_id").orderBy(F.col("revision_id").desc())
    pages = (
        batch_df
            .withColumn("rnk", F.row_number().over(w))
            .filter("rnk = 1")
            .select("page_id", "title", "category", "url", "revision_id", "text")
            .collect()
    )
    if not pages:
        return
    
    rows = []
    for page in pages:
        for size, overlap in CHUNK_SIZES.items():
            rows.extend(chunk_document(page.asDict(), size, overlap))
    
    chunks = (
        spark.createDataFrame(
            [tuple(r[field.name] for field in CHUNK_SCHEMA.fields) for r in rows],
            CHUNK_SCHEMA
        )
        .withColumn("_chunked_at", F.current_timestamp())
    )

    page_ids = ", ".join(str(p["page_id"]) for p in pages)
    (
        DeltaTable.forName(spark, SILVER_TABLE).alias("t")
            .merge(chunks.alias("s"), "t.chunk_id = s.chunk_id")
            .whenMatchedUpdateAll(condition="s.revision_id <> t.revision_id OR s.chunk_text <> t.chunk_text")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(condition=f"t.page_id IN ({page_ids})")
            .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the stream

# COMMAND ----------

(
    spark.readStream.table(BRONZE_TABLE)
        .writeStream
        .foreachBatch(chunk_and_merge)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
)