from datetime import datetime
from chispa import assert_df_equality
from f1_pipeline.transforms.silver import transform_circuits

BRONZE_CIRCUITS = (
    "circuitId STRING, circuitRef STRING, name STRING, location STRING, "
    "country STRING, lat STRING, lng STRING, alt STRING, "
    "_source_file STRING, _ingested_at TIMESTAMP"
)

SILVER_CIRCUITS = (
    "circuitId INT, circuitRef STRING, name STRING, location STRING, "
    "country STRING, lat DOUBLE, lng DOUBLE, alt INT, "
    "_source_file STRING, _ingested_at TIMESTAMP"
)


def test_transform_circuits(spark):
    ts = datetime(2026, 1, 1)

    bronze_row = (
        "1", "albert_park", "Albert Park Grand Prix Circuit", "Melbourne",
        "Australia", "-37.8497", "144.968", "10", "f.csv", ts,
    )
    silver_row = (
        1, "albert_park", "Albert Park Grand Prix Circuit", "Melbourne",
        "Australia", -37.8497, 144.968, 10, "f.csv", ts,
    )

    df_bronze = spark.createDataFrame([bronze_row], BRONZE_CIRCUITS)
    df_silver = spark.createDataFrame([silver_row], SILVER_CIRCUITS)

    assert_df_equality(
        transform_circuits(df_bronze), df_silver, ignore_nullable=True
    )