from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from f1_pipeline.transforms.silver import deduplicate_results, parse_results
from f1_pipeline.dq.rules import RESULTS_RULES, add_failed_rules

QUARANTINE_RULES = {
    **RESULTS_RULES,
    "race_exists": "_race_found IS NOT NULL"
}


CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


def _parsed_results() -> DataFrame:
    df_results_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.results_sdp")
    return deduplicate_results(parse_results(df_results_bronze, keep_raw=True))


def _results_with_race_check() -> DataFrame:
    results = _parsed_results()
    races = (
        spark.read.table(f"{CATALOG}.{silver_schema}.races_sdp")
            .select("raceId")
            .distinct()
            .withColumn("_race_found", F.lit(True))
    )
    return results.join(races, "raceId", "left")


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.results_sdp",
    comment=f"Silver table for results (rows that pass all DQ rules)"
)
@dp.expect_all_or_drop(RESULTS_RULES)
def silver_results() -> DataFrame:
    return (
        _results_with_race_check()
            .filter(F.col("_race_found").isNotNull())
            .drop("raw_json", "_race_found")
    )


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.results_quarantine_sdp",
    comment="Silver table for quarantined results"
)
def silver_quarantine_results() -> DataFrame:
    return add_failed_rules(
        _results_with_race_check(),
        QUARANTINE_RULES
    ).drop("_race_found")

