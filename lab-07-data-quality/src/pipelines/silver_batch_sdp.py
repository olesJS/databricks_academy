from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from f1_pipeline.transforms.silver import (
    transform_circuits,
    transform_constructors,
    transform_races
)

CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.circuits_sdp",
    comment="Circuits in Silver layer"
)
@dp.expect_or_drop("validCircuitId", "circuitId IS NOT NULL")
@dp.expect("validCoordinates", "(lat BETWEEN -90 AND 90) AND (lng BETWEEN -180 AND 180)")
def silver_cirucits() -> DataFrame:
    df_circuits_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.circuits_sdp")
    return transform_circuits(df_circuits_bronze)
    

@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.constructors_sdp",
    comment="Constructors in Silver layer"
)
@dp.expect_or_drop("validConstructorId", "constructorId IS NOT NULL")
def silver_constructors() -> DataFrame:
    df_constructors_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.constructors_sdp")
    return transform_constructors(df_constructors_bronze)


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.races_sdp",
    comment="Races in Silver layer"
)
@dp.expect_or_drop("validRaceId", "raceId IS NOT NULL")
def silver_races() -> DataFrame:
    df_races_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.races_sdp")
    return transform_races(df_races_bronze)

