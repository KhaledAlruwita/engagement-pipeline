import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, round as f_round
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.engagement_events")

PG_URL = "jdbc:postgresql://pg:5432/engagement"
PG_PROPS = {"user": "app", "password": "app", "driver": "org.postgresql.Driver"}

CH_URL = "jdbc:clickhouse://clickhouse:8123/engagement"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "spark",
    "password": "spark"
}

CHECKPOINT = "/opt/checkpoints/kafka_to_clickhouse"

after_schema = StructType([
    StructField("id", LongType(), False),
    StructField("content_id", LongType(), False),
    StructField("user_id", LongType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("duration_ms", IntegerType(), True),
    StructField("device", StringType(), True),
])

value_schema = StructType([
    StructField("before", StringType(), True),
    StructField("after", after_schema, True),
    StructField("op", StringType(), True),
])

spark = SparkSession.builder.appName("kafka-to-clickhouse").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read content dimension once (simple for evaluation)
content_df = (
    spark.read.jdbc(PG_URL, "content", properties=PG_PROPS)
    .select(
        col("content_id").cast("long").alias("c_content_id"),
        col("content_type").alias("content_type"),
        col("length_seconds").cast("int").alias("length_seconds"),
    )
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS value_str")
    .select(from_json(col("value_str"), value_schema).alias("j"))
    .select(col("j.after").alias("a"), col("j.op").alias("op"))
    .filter(col("op") == "c")
    .filter(col("a").isNotNull())
)

events = (
    parsed.select(
        col("a.id").alias("event_id"),
        col("a.content_id").alias("content_id"),
        col("a.user_id").alias("user_id"),
        col("a.event_type").alias("event_type"),
        expr("to_timestamp(a.event_ts)").cast("timestamp").alias("event_ts"),

        col("a.duration_ms").alias("duration_ms"),
        col("a.device").alias("device"),
    )
)

enriched = (
    events.join(content_df, events.content_id == content_df.c_content_id, "left")
    .drop("c_content_id")
    .withColumn("engagement_seconds", expr("CASE WHEN duration_ms IS NULL THEN NULL ELSE duration_ms/1000.0 END"))
    .withColumn(
        "engagement_pct",
        expr("""
            CASE
              WHEN duration_ms IS NULL OR length_seconds IS NULL OR length_seconds = 0 THEN NULL
              ELSE (duration_ms/1000.0) / length_seconds
            END
        """)
    )
    .withColumn("engagement_pct", f_round(col("engagement_pct"), 2))
)

def write_clickhouse(batch_df, batch_id: int):
    out = batch_df.coalesce(1)
    out.write.mode("append").jdbc(CH_URL, "events_enriched", properties=CH_PROPS)

query = (
    enriched.writeStream
    .foreachBatch(write_clickhouse)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="1 second")
    .start()
)

query.awaitTermination()
