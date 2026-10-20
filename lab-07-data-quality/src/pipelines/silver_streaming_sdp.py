from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from f1_pipeline.transforms.silver import parse_results

CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.results_sdp",
    comment=f"Silver table for results"
)
def silver_results() -> DataFrame:
    df_results_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.results_sdp")
    return parse_results(df_results_bronze)
