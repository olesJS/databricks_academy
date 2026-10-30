from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from f1_pipeline.transforms.silver import transform_drivers

CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


@dp.table(
    name=f"{CATALOG}.{silver_schema}.drivers_silver_prepared",
    comment="Streaming Table to prepare Drivers before ingestion to Silver"
)
@dp.expect_or_drop("validDriverId", "driverId IS NOT NULL")
def drivers_silver_prepared():
    df_drivers_bronze = spark.readStream.table(f"{CATALOG}.{bronze_schema}.drivers_sdp")
    return transform_drivers(df_drivers_bronze)


@dp.materialized_view(name=f"{CATALOG}.{silver_schema}.drivers_quarantine_sdp")
def silver_drivers_quarantine() -> DataFrame:
    df_drivers_bronze = transform_drivers(spark.read.table(f"{CATALOG}.{bronze_schema}.drivers_sdp"))
    return df_drivers_bronze.where(F.col("driverId").isNull())


dp.create_streaming_table(
    name=f"{CATALOG}.{silver_schema}.drivers_sdp",
    comment="Drivers in Silver layer",
    expect_all_or_drop={"validDriverId": "driverId IS NOT NULL"}
)


dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{silver_schema}.drivers_sdp",
    source=f"{CATALOG}.{silver_schema}.drivers_silver_prepared",
    keys=["driverId"],
    sequence_by=F.col("_ingested_at"),
    name="silver_drivers_scd2",
    stored_as_scd_type="2"
)
