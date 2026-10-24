from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from f1_pipeline.transforms.silver import deduplicate_results, parse_results
from f1_pipeline.dq_rules import RESULTS_RULES, add_failed_rules


CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


def _parsed_results() -> DataFrame:
    df_results_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.results_sdp")
    return deduplicate_results(parse_results(df_results_bronze, keep_raw=True))


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.results_sdp",
    comment=f"Silver table for results (rows that pass all DQ rules)"
)
def silver_results() -> DataFrame:
    return _parsed_results().drop("raw_json")


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.results_quarantine_sdp",
    comment="Silver table for quarantined results"
)
def silver_quarantine_results() -> DataFrame:
    return add_failed_rules(_parsed_results(), RESULTS_RULES)

