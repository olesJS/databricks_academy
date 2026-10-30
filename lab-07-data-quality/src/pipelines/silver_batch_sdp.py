from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from f1_pipeline.transforms.silver import (
    transform_circuits,
    transform_constructors,
    transform_races,
)

from f1_pipeline.dq.rules import (
    RACES_RULES,
    CIRCUITS_RULES,
    CIRCUITS_WARN,
    CONSTRUCTORS_RULES,
    add_failed_rules
)


CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

bronze_schema = f"{LOGIN}_bronze"
silver_schema = f"{LOGIN}_silver"


# silver_circuits_sdp
@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.circuits_sdp",
    comment="Circuits in Silver layer"
)
@dp.expect_all_or_drop(CIRCUITS_RULES)
@dp.expect_all(CIRCUITS_WARN)
def silver_circuits() -> DataFrame:
    df_circuits_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.circuits_sdp")
    return transform_circuits(df_circuits_bronze)


@dp.materialized_view(name=f"{CATALOG}.{silver_schema}.circuits_quarantine_sdp")
def silver_quarantine_circuit() -> DataFrame:
    df_circuits_bronze = transform_circuits(spark.read.table(f"{CATALOG}.{bronze_schema}.circuits_sdp"))
    return add_failed_rules(df_circuits_bronze, CIRCUITS_RULES)


# silver_constructors_sdp
@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.constructors_sdp",
    comment="Constructors in Silver layer"
)
@dp.expect_all_or_drop(CONSTRUCTORS_RULES)
def silver_constructors() -> DataFrame:
    df_constructors_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.constructors_sdp")
    return transform_constructors(df_constructors_bronze)


@dp.materialized_view(name=f"{CATALOG}.{silver_schema}.constructors_quarantine_sdp")
def silver_quarantine_constructors() -> DataFrame:
    df_constructors_bronze = transform_constructors(spark.read.table(f"{CATALOG}.{bronze_schema}.constructors_sdp"))
    return add_failed_rules(df_constructors_bronze, CONSTRUCTORS_RULES)


# silver_races_sdp
@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.races_sdp",
    comment="Races in Silver layer"
)
@dp.expect_all_or_drop(RACES_RULES)
def silver_races() -> DataFrame:
    df_races_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.races_sdp")
    return transform_races(df_races_bronze)


@dp.materialized_view(name=f"{CATALOG}.{silver_schema}.races_quarantine_sdp")
def silver_quarantine_races() -> DataFrame:
    df_races_bronze = transform_races(spark.read.table(f"{CATALOG}.{bronze_schema}.races_sdp"))
    return add_failed_rules(df_races_bronze, RACES_RULES)
