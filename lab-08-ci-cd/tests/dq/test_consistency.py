import pytest
from pyspark.sql import functions as F


@pytest.mark.parametrize("fk,dim,key", [
    ("race_id", "dim_race", "race_id"),
    ("circuit_id", "dim_circuit", "circuit_id"),
    ("constructor_id", "dim_constructor", "constructor_id"),
    ("driver_id", "dim_driver", "driver_id")
])
def test_fact_has_no_orphan_keys(spark, gold, fk, dim, key):
    fact_keys = (
        spark.read.table(f"{gold}.fact_results")
            .select(F.col(fk).alias("_fk"))
            .distinct()
    )
    dim_keys = (
        spark.read.table(f"{gold}.{dim}")
            .select(F.col(key).alias("_k"))
            .distinct()
    )

    orphans = fact_keys.join(dim_keys, F.col("_fk") == F.col("_k"), how="left_anti").count()
    assert orphans == 0, f"fact_results.{fk}: {orphans} values missing in {dim}"


def test_each_driver_has_only_one_active_record(spark, gold):
    bad = (
        spark.read.table(f"{gold}.dim_driver")
            .groupBy("driver_id")
            .agg(F.sum(F.col("is_current")).alias("n"))
            .where("n != 1")
            .count()
    )
    assert bad == 0


def test_bronze_is_fresh(spark, bronze):
    days = spark.sql(f"""
        SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(_ingested_at))) / 86400
        FROM {bronze}.results_sdp
    """).first()[0]
    limit = 14      # races are conducted once in 1-2 weeks
    assert days is not None and days <= limit, f"bronze.results_sdp is {days} days old (limit {limit}d)"
