from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


LOGIN = spark.conf.get("base_schema_name")
bronze_schema = f"{LOGIN}_bronze"

EVENT_HUB_NAME = spark.conf.get("event_hub_name")
EVENT_HUB_NAMESPACE = spark.conf.get("event_hub_namespace")
EVENT_HUB_CONN_STR = dbutils.secrets.get(
    scope=spark.conf.get("kv_scope"), 
    key=spark.conf.get("kv_conn_str_key")
)


@dp.table(
    name=f"{bronze_schema}.results_sdp",
    comment=f"Raw results streaming data from EventHub"
)
def bronze_results() -> DataFrame:
    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password=\"{EVENT_HUB_CONN_STR}\";'

    kafka_options = {
        "kafka.bootstrap.servers": f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093",
        "subscribe": EVENT_HUB_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "30000",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "10000"
    }

    df_raw = (
        spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
    )

    return (
        df_raw.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("_evh_name"),
            F.col("partition").alias("_evh_partition"),
            F.col("offset").alias("_evh_offset"),
            F.col("timestamp").alias("_evh_timestamp"),
            F.current_timestamp().alias("_ingested_at")
        )
    )
