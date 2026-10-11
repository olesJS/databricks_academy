from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, DoubleType


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

    return (
        df_circuits_bronze.select(
            F.col("circuitId").cast(IntegerType()),
            F.col("circuitRef"),
            F.col("name"),
            F.col("location"),
            F.col("country"),
            F.col("lat").cast(DoubleType()),
            F.col("lng").cast(DoubleType()),
            F.col("alt").cast(IntegerType()),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )
    

@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.constructors_sdp",
    comment="Constructors in Silver layer"
)
@dp.expect_or_drop("validConstructorId", "constructorId IS NOT NULL")
def silver_constructors() -> DataFrame:
    df_constructors_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.constructors_sdp")

    return (
        df_constructors_bronze.select(
            F.col("constructorId").cast(IntegerType()),
            F.col("constructorRef"),
            F.col("name"),
            F.col("nationality"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{silver_schema}.races_sdp",
    comment="Races in Silver layer"
)
@dp.expect_or_drop("validRaceId", "raceId IS NOT NULL")
def silver_races() -> DataFrame:
    df_races_bronze = spark.read.table(f"{CATALOG}.{bronze_schema}.races_sdp")

    return (
        df_races_bronze.select(
            F.col("raceId").cast(IntegerType()),
            F.col("year").cast(IntegerType()),
            F.col("round").cast(IntegerType()),
            F.col("circuitId").cast(IntegerType()),
            F.col("name"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            F.to_date(F.col("time"), "HH:mm:ss").alias("time"),
            F.col("_source_file"),
            F.col("_ingested_at")
        )
    )
