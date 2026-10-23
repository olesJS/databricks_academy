from f1_pipeline.transforms.silver import transform_circuits
from chispa import assert_df_equality, assert_column_equality

BRONZE_CIRCUITS = [
    "circuitId STRING, circuitRef STRING, name STRING, location STRING, country STRING, lat STRING, lng STRING, alt STRING"
]

SILVER_CIRCUITS = [
    "circuitId INT, circuitRef STRING, name STRING, location STRING, country STRING, lat DOUBLE, lng DOUBLE, alt INT"
]

def test_transform_circuits(spark):
    values = [
        "1", "albert_park", "Albert Park Grand Prix Circuit", "Melbourne",
        "Australia", "-37.8497", "144.968", "10"
    ]

    df_bronze = spark.createDataFrame(values, BRONZE_CIRCUITS)
    df_silver = spark.createDataFrame(values, SILVER_CIRCUITS)

    assert_df_equality(transform_circuits(df_bronze), df_silver, ignore_columns=["_source_file", "_ingested_at"])

    

