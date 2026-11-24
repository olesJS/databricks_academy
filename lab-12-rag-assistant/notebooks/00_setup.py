# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Setup
# MAGIC Creates everything the RAG pipeline needs.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "f1_rag_bronze")
dbutils.widgets.text("silver_schema", "f1_rag_silver")
dbutils.widgets.text("gold_schema", "f1_rag_gold")
dbutils.widgets.text("raw_volume", "raw")
dbutils.widgets.text("reader_group", "account users")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
raw_volume = dbutils.widgets.get("raw_volume")
reader_group = dbutils.widgets.get("reader_group")

# COMMAND ----------

BRONZE = f"{catalog}.{bronze_schema}"
SILVER = f"{catalog}.{silver_schema}"
GOLD = f"{catalog}.{gold_schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas and Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE} COMMENT 'Raw Wikipedia F1 articles'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER} COMMENT 'Cleaned and chunked documents for RAG'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD} COMMENT 'RAG monitoring (prompts and responses)'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE}.{raw_volume} COMMENT 'Raw JSON from the Wikipedia API'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze: one row per article

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {BRONZE}.wiki_pages (
        page_id          BIGINT    NOT NULL,
        title            STRING    NOT NULL,
        requested_title  STRING,
        category         STRING    NOT NULL,
        url              STRING    NOT NULL,
        revision_id      BIGINT    NOT NULL,
        fetched_at       TIMESTAMP NOT NULL,
        text             STRING    NOT NULL,
        _source_file     STRING    NOT NULL,
        _ingested_at     TIMESTAMP NOT NULL
    )
    COMMENT 'Wikipedia F1 articles as fetched from the API'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: one row per chunk

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.wiki_chunks (
        chunk_id      STRING    NOT NULL,
        chunk_size    INT       NOT NULL,
        page_id       BIGINT    NOT NULL,
        title         STRING    NOT NULL,
        category      STRING    NOT NULL,
        section       STRING    NOT NULL,
        chunk_index   INT       NOT NULL,
        chunk_text    STRING    NOT NULL,
        token_count   INT       NOT NULL,
        url           STRING    NOT NULL,
        revision_id   BIGINT    NOT NULL,
        _chunked_at   TIMESTAMP NOT NULL
    )
    COMMENT 'Wikipedia F1 articles split into chunks; chunk_size = target size in tokens (300 or 500)'
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: QA log
# MAGIC Every call of `ask()` is written here: prompt, retrieved chunks, answer, tokens, latency.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD}.qa_log (
        request_id           STRING        NOT NULL,
        requested_at         TIMESTAMP     NOT NULL,
        user_name            STRING,
        question             STRING        NOT NULL,
        category_filter      STRING,
        use_retrieval        BOOLEAN       NOT NULL,
        index_name           STRING,
        retrieved_chunk_ids  ARRAY<STRING>,
        answer               STRING,
        model                STRING        NOT NULL,
        prompt_tokens        INT,
        completion_tokens    INT,
        latency_ms           DOUBLE
    )
    COMMENT 'Log of RAG requests and responses'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access
# MAGIC Readers can query the chunks (and later the index built on them), but not raw data in bronze and not the QA log in gold, which contains user questions.

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {SILVER} TO `{reader_group}`")
spark.sql(f"GRANT SELECT ON SCHEMA {SILVER} TO `{reader_group}`")