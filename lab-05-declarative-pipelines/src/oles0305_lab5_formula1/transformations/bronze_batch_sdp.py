from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import ast


CATALOG = spark.conf.get("catalog_name")
LOGIN = spark.conf.get("base_schema_name")

tables_list = spark.conf.get("tables")
tables = ast.literal_eval(tables_list)

landing_path = f"/Volumes/{CATALOG}/{LOGIN}/landing"
bronze_schema = f"{LOGIN}_bronze"


def ingest_to_bronze(table_name):
    table_landing_path = f"{landing_path}/{table_name}"
    
    @dp.table(
        name=f"{bronze_schema}.{table_name}_sdp",
        comment=f"Raw {table_name} data from Landing zone"
    )
    def create_table() -> DataFrame:
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("nullValue", r"\N")
                .load(table_landing_path)
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_file", F.col("_metadata.file_path"))
        )


for table in tables:
    ingest_to_bronze(table)
