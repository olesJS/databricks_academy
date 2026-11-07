# Databricks notebook source
# MAGIC %md
# MAGIC # Data profile of the Lab 8 layers

# COMMAND ----------

import json

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("schema", "oles0305_prod")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

def profile(full_name: str) -> dict:
    df = spark.table(full_name)
    result = {"rows": df.count(), "columns": len(df.columns)}

    null_counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).first().asDict()
    result["columns_with_nulls"] = {c: n for c, n in null_counts.items() if n}

    try:
        detail = spark.sql(f"DESCRIBE DETAIL {full_name}").first()
        result["size_mb"] = round(detail["sizeInBytes"] / 1024 / 1024, 2)
        result["last_modified"] = str(detail["lastModified"])
    except Exception:
        result["note"] = "no Delta metadata"
    return result


summary = {
    "user": spark.sql("SELECT current_user()").first()[0],
    "spark_version": spark.version,
    "layers": {},
}

problems = []
for layer in ("silver", "gold"):
    layer_schema = f"{catalog}.{schema}_{layer}"
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {layer_schema}").collect()]
    if not tables:
        problems.append(f"{layer_schema}: no tables")
    summary["layers"][layer] = {t: profile(f"{layer_schema}.{t}") for t in tables}
    problems += [
        f"{layer_schema}.{t}: empty"
        for t, p in summary["layers"][layer].items()
        if p["rows"] == 0 and "quarantine" not in t
    ]

assert not problems, "; ".join(problems)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary, default=str))