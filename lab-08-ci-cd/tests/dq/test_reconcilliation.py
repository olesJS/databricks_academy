import pytest
from pyspark.sql import functions as F

from f1_pipeline.transforms.silver import (
    deduplicate_results,
    parse_results,
    transform_circuits,
    transform_constructors,
    transform_races,
    transform_drivers,
)


def test_bronze_results_fully_accounted(spark, bronze, silver):
    bronze_count = deduplicate_results(
        parse_results(spark.read.table(f"{bronze}.results_sdp"))
    ).count()
    silver_kept = spark.read.table(f"{silver}.results_sdp").count()
    silver_quarantined = spark.read.table(f"{silver}.results_quarantine_sdp").count()
    assert bronze_count == silver_kept + silver_quarantined, (
        f"results: bronze {bronze_count}, "
        f"silver {silver_kept} + quarantine {silver_quarantined}"
    )


@pytest.mark.parametrize("table,transform,clean_table", [
    ("circuits", transform_circuits, "circuits_sdp"),
    ("constructors", transform_constructors, "constructors_sdp"),
    ("races", transform_races, "races_sdp"),
    ("drivers", transform_drivers, "drivers_silver_prepared"),
])
def test_bronze_dims_fully_accounted(spark, bronze, silver, table, transform, clean_table):
    parsed = transform(spark.read.table(f"{bronze}.{table}_sdp")).count()
    kept = spark.read.table(f"{silver}.{clean_table}").count()
    quarantined = spark.read.table(f"{silver}.{table}_quarantine_sdp").count()
    assert parsed == kept + quarantined, (
        f"{table}: bronze after transform {parsed}, "
        f"kept {kept} + quarantine {quarantined}"
    )


@pytest.mark.parametrize("table,key", [
    ("circuits", "circuitId"),
    ("constructors", "constructorId"),
    ("races", "raceId"),
])
def test_silver_dims_do_not_exceed_valid_bronze_rows(spark, bronze, silver, table, key):
    valid_bronze = (
        spark.read.table(f"{bronze}.{table}_sdp")
        .where(F.expr(f"try_cast({key} AS INT) IS NOT NULL"))
        .count()
    )
    assert spark.read.table(f"{silver}.{table}_sdp").count() <= valid_bronze


@pytest.mark.parametrize("gold_table,silver_table", [
    ("fact_results", "results_sdp"),
    ("dim_race", "races_sdp"),
    ("dim_circuit", "circuits_sdp"),
    ("dim_constructor", "constructors_sdp"),
    ("dim_driver", "drivers_sdp"),
])
def test_gold_row_count_matches_silver(spark, silver, gold, gold_table, silver_table):
    assert spark.read.table(f"{gold}.{gold_table}").count() == spark.read.table(f"{silver}.{silver_table}").count()


@pytest.mark.parametrize("agg", ["agg_driver_season", "agg_constructor_season", "agg_driver_circuit"])
def test_aggregates_reconcile_with_fact(spark, gold, agg):
    fact = spark.read.table(f"{gold}.fact_results")
    expected_points = fact.agg(F.sum("points")).first()[0]
    expected_wins = fact.where(F.col("position_order").cast("int") == 1).count()
    got = spark.read.table(f"{gold}.{agg}").agg(F.sum("points_sum"), F.sum("wins")).first()
    assert got[0] == expected_points
    assert got[1] == expected_wins

