from clickhouse_connect import get_client
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_DATABASE = "analytics"
CLICKHOUSE_TABLE = "raw_events"
CHECKPOINT_DIR = "checkpoints/raw_events"

schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", LongType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("url", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("event_time", StringType(), False),
    ]
)


def write_batch_to_clickhouse(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    rows = pdf.to_dict("records")

    client = get_client(host=CLICKHOUSE_HOST, port=8123)
    client.insert(
        f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}",
        rows,
        column_names=[
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "url",
            "device_type",
            "event_time",
        ],
    )
    client.close()

    print(f"batch {batch_id}: inserted {len(rows)} rows")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("clickstream-processor")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    "org.apache.kafka:kafka-clients:3.5.1",
                ]
            ),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "clickstream")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df.selectExpr("cast(value as string) as payload")
        .select(from_json(col("payload"), schema).alias("event"))
        .select("event.*")
        .withColumn("event_time", to_timestamp("event_time"))
    )

    query = (
        parsed_df.writeStream.outputMode("append")
        .foreachBatch(write_batch_to_clickhouse)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()
