import sys
sys.path.insert(0, "/tmp/pip")
import os, json
import requests
import redis
from decimal import Decimal
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, round as f_round, window
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "dbserver1.public.engagement_events"

PG_URL = "jdbc:postgresql://pg:5432/engagement"
PG_PROPS = {"user": "app", "password": "app", "driver": "org.postgresql.Driver"}

CH_URL = "jdbc:clickhouse://clickhouse:8123/engagement"
CH_PROPS = {"driver": "com.clickhouse.jdbc.ClickHouseDriver", "user": "spark", "password": "spark"}

REDIS_HOST = "redis"
REDIS_PORT = 6379

EXTERNAL_URL = "http://external-api:8000/events"

CHECKPOINT = "/opt/checkpoints/fanout_job"

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

spark = SparkSession.builder.appName("fanout-clickhouse-redis-external").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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
    .option("startingOffsets", "latest")  #real time
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


def json_safe(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v

def write_fanout(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    # 1) ClickHouse (analytics)
    batch_df.coalesce(1).write.mode("append").jdbc(CH_URL, "events_enriched", properties=CH_PROPS)

    # 2) to Redis 
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline(transaction=False)

    rows = batch_df.select(
        "event_id","content_id","user_id","event_type","event_ts",
        "duration_ms","device","content_type","length_seconds","engagement_seconds","engagement_pct"
    ).collect()

    to_external = []
    for row in rows:
        eid = int(row["event_id"])
        cid = int(row["content_id"])

       
        ok = r.set(f"event:seen:{eid}", "1", nx=True, ex=86400)
        if not ok:
            continue

        payload = {
            "event_id": int(row["event_id"]),
            "content_id": int(row["content_id"]),
            "user_id": int(row["user_id"]),
            "event_type": row["event_type"],
            "event_ts": str(row["event_ts"]),
            "duration_ms": json_safe(row["duration_ms"]),
            "device": json_safe(row["device"]),
            "content_type": json_safe(row["content_type"]),
            "length_seconds": json_safe(row["length_seconds"]),
            "engagement_seconds": json_safe(row["engagement_seconds"]),
            "engagement_pct": json_safe(row["engagement_pct"]),
        }


        pipe.set(f"content:last:{cid}", json.dumps(payload), ex=3600)
        pipe.incr(f"content:count:{cid}")
        ts = row["event_ts"]  
        bucket = ts.strftime("%Y%m%d%H%M")  
        pipe.zincrby(f"top:content:bucket:{bucket}", 1, str(cid))
        pipe.expire(f"top:content:bucket:{bucket}", 15 * 60)  # 15 minute
        to_external.append(payload)
        bucket = row["event_ts"].strftime("%Y%m%d%H%M")  # minute bucket
        pipe.zincrby(f"top:content:bucket:{bucket}", 1, str(cid))
        pipe.expire(f"top:content:bucket:{bucket}", 15 * 60)


    pipe.execute()

    # 3) External system (mock)
    if to_external:
        try:
            requests.post(EXTERNAL_URL, json=to_external, timeout=2)
        except Exception:
            # for evaluation: don't crash the stream
            pass

def write_top10m(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline(transaction=False)

    rows = batch_df.select("content_id", "cnt").collect()
    for row in rows:
        pipe.zadd("top:content:10m", {str(int(row["content_id"])): int(row["cnt"])})
    pipe.expire("top:content:10m", 120)
    pipe.execute()

q1 = (
    enriched.writeStream
    .foreachBatch(write_fanout)
    .option("checkpointLocation", CHECKPOINT + "/enriched")
    .trigger(processingTime="1 second")
    .start()
)

def send_to_external(df):
    import requests

    for row in df.collect():
        event_id = row.event_id

        # idempotency
        if redis.get(f"ext:sent:{event_id}"):
            continue

        r = requests.post(
            "http://external-api:8000/events",
            json=row.asDict(),
            timeout=3
        )

        if r.status_code == 200:
            redis.setex(f"ext:sent:{event_id}", 86400, 1)
        else:
            raise Exception("external failed")


q1.awaitTermination()

