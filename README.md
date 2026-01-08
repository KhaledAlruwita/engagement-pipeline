ENGAGEMENT PIPELINE – STREAMING SYSTEM
====================================

1. OVERVIEW
-----------
This project implements a real-time data streaming pipeline that captures new
rows from a PostgreSQL table (engagement_events), enriches each event, and
fan-outs the enriched data to three different destinations:

1) ClickHouse (analytics / reporting – BigQuery alternative)
2) Redis (real-time & last-10-minutes aggregations)
3) External HTTP API (system outside our control)

The system is designed to be practical, fault-tolerant, reproducible, and easy
to run locally using Docker Compose.


2. BUSINESS GOAL
----------------
The pipeline enables:
- Real-time engagement analytics
- Near real-time updates (< 5 seconds) for Redis
- Historical analytics via a columnar database
- Safe integration with external systems
- Reprocessing (backfill) of historical data


3. HIGH-LEVEL ARCHITECTURE
--------------------------
Postgres
  └─(CDC via Debezium)
      └─ Kafka
          └─ Spark Structured Streaming
              ├─ ClickHouse (analytics)
              ├─ Redis (real-time & aggregations)
              └─ External API (HTTP)


4. DATA FLOW – STEP BY STEP
---------------------------

STEP 0 – SOURCE (Postgres)
--------------------------
Tables:
- engagement_events (fact table)
- content (dimension table)

Example insert:
INSERT INTO engagement_events
(content_id, user_id, event_type, duration_ms, device)
VALUES (1, 999, 'play', 7000, 'web');


STEP 1 – CDC (Debezium)
----------------------
- Debezium monitors Postgres WAL.
- Every INSERT becomes a JSON event.
- Only CREATE operations (op = "c") are processed.

Output: Kafka message (JSON).


STEP 2 – TRANSPORT (Kafka)
--------------------------
Topic:
- dbserver1.public.engagement_events

Kafka acts as:
- Buffer
- Decoupling layer
- Replay mechanism (offset-based)


STEP 3 – STREAM INGESTION (Spark)
--------------------------------
Spark Structured Streaming:
- Reads Kafka topic
- Parses Debezium JSON
- Extracts the "after" payload
- Filters only INSERT events


STEP 4 – ENRICHMENT & TRANSFORMATION
------------------------------------
Spark joins each event with the content table:

Join:
- engagement_events.content_id = content.content_id

Derived fields:
- engagement_seconds = duration_ms / 1000
- engagement_pct = round(engagement_seconds / length_seconds, 2)

Null handling:
- If duration_ms or length_seconds is missing:
  engagement_seconds = NULL
  engagement_pct = NULL


STEP 5 – MULTI-SINK FAN-OUT
---------------------------
Using foreachBatch, Spark writes each micro-batch to:

A) ClickHouse
B) Redis
C) External HTTP API

All sinks receive the same enriched dataset.


5. DATA MODEL (ENRICHED EVENT)
------------------------------
Example enriched record:

{
  "event_id": 16,
  "content_id": 1,
  "user_id": 999,
  "event_type": "play",
  "event_ts": "2026-01-08 14:21:52",
  "duration_ms": 7000,
  "device": "web",
  "content_type": "video",
  "length_seconds": 300,
  "engagement_seconds": 7.0,
  "engagement_pct": 0.02
}


6. SINK DETAILS
---------------

6.1 ClickHouse (Analytics Store)
--------------------------------
Purpose:
- Reporting & analytics (BigQuery alternative)

Table:
- engagement.events_enriched

Write mode:
- Append only

Use cases:
- Time-based analytics
- Aggregations
- Dashboards


6.2 Redis (Real-Time Store)
---------------------------
Redis supports low-latency (< 5s) use cases.

Keys used:

1) Last event per content
   Key: content:last:{content_id}
   Value: JSON enriched event
   TTL: 1 hour

2) Event count per content
   Key: content:count:{content_id}
   Value: integer (INCR)

3) Per-minute aggregation buckets
   Key: top:content:bucket:{YYYYMMDDHHMM}
   Type: ZSET
   Member: content_id
   Score: number of events in that minute
   TTL: 15 minutes

4) Last 10 minutes aggregation
   Key: top:content:10m
   Type: ZSET
   Built by merging the last 10 minute buckets
   TTL: 120 seconds


6.3 External API (Uncontrolled System)
--------------------------------------
Purpose:
- Demonstrate integration with a system we do not control

Implementation:
- FastAPI mock service
- Endpoint: POST /events

Behavior:
- Receives enriched events as JSON
- Returns HTTP 200 OK
- Can be extended to simulate failures or delays


7. PARTITIONING STRATEGY
------------------------
Kafka:
- Logical partition key: content_id
- Ensures ordering per content if needed

Spark:
- Controlled shuffle partitions to reduce overhead

ClickHouse:
- ORDER BY (event_ts, content_id, event_id)
- Optimized for time-based queries


8. FAULT TOLERANCE & EXACTLY-ONCE
---------------------------------
Spark:
- Uses checkpointing to track offsets and state
- On restart, resumes from last committed offsets

Kafka:
- Guarantees at-least-once delivery
- Spark checkpointing ensures exactly-once processing

Redis & External API:
- Idempotency handled via event_id
- Duplicate events can be safely ignored


9. BACKFILL (HISTORICAL REPROCESSING)
-------------------------------------
Tool:
- tools/backfill.py

Behavior:
- Reads historical data from Postgres within a time range
- Applies the same transformations as streaming
- Writes to ClickHouse and Redis
- Uses idempotency keys to avoid duplicates

Example:
python backfill.py --start 2026-01-08T09:00:00 --end 2026-01-08T10:00:00


10. RUNNING THE SYSTEM
---------------------
1) Start all services:
   docker compose up -d

2) Start Spark streaming jobs:
   - Kafka → ClickHouse
   - ClickHouse → Redis & External API

3) Insert data into Postgres:
   INSERT INTO engagement_events (...)

4) Observe:
   - ClickHouse tables
   - Redis keys
   - External API logs


11. REQUIREMENTS COVERAGE
-------------------------
✔ Streaming from engagement_events
✔ Enrichment via content table
✔ Derived metrics (engagement_seconds, engagement_pct)
✔ Multi-sink fan-out
✔ Redis updates < 5 seconds
✔ 10-minute rolling aggregations
✔ Backfill support
✔ Exactly-once processing (practical)
✔ Reproducible environment (Docker)


12. FUTURE IMPROVEMENTS
-----------------------
- Dead Letter Queue (DLQ) for failed external calls
- Schema Registry (Avro/Protobuf)
- Real BigQuery sink
- Monitoring & alerting (Prometheus / Grafana)
- Rate limiting & retries for external API


END OF DOCUMENT
====================================
