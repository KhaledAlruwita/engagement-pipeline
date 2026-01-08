ENGAGEMENT STREAMING PIPELINE
=============================

This project implements a real-time data streaming system that captures
new rows from a PostgreSQL table, enriches them, and fans out the results
to multiple destinations with low latency and high reliability.

--------------------------------------------------
1. WHAT THIS SYSTEM DOES
--------------------------------------------------

The system performs the following:

1) Streams new rows from PostgreSQL table: engagement_events
2) Enriches each event by joining with the content table
3) Derives new metrics:
   - engagement_seconds = duration_ms / 1000
   - engagement_pct = engagement_seconds / length_seconds (rounded to 2 decimals)
4) Sends enriched events to THREE destinations in real time:
   - ClickHouse (analytics / reporting – BigQuery alternative)
   - Redis (real-time use cases, < 5 seconds latency)
   - External HTTP API (system we do not control)
5) Maintains rolling aggregations in Redis:
   - Top content in the last 10 minutes
6) Supports backfilling historical data
7) Uses a streaming engine with checkpointing for reliable processing

--------------------------------------------------
2. HIGH-LEVEL ARCHITECTURE
--------------------------------------------------

PostgreSQL
  |
  | (CDC via Debezium)
  v
Kafka
  |
  | (Structured Streaming)
  v
Spark Structured Streaming
  |
  |---> ClickHouse (Analytics Store)
  |
  |---> Redis (Real-time Store + Aggregations)
  |
  |---> External API (HTTP)

--------------------------------------------------
3. DATA FLOW EXPLAINED STEP BY STEP
--------------------------------------------------

STEP 1 – SOURCE (PostgreSQL)
----------------------------
- New events are inserted into:
  public.engagement_events
- Reference data lives in:
  public.content

Example insert:
INSERT INTO engagement_events
(content_id, user_id, event_type, duration_ms, device)
VALUES (1, 999, 'play', 7000, 'web');

--------------------------------------------------

STEP 2 – CDC (Debezium)
-----------------------
- Debezium reads PostgreSQL WAL (write-ahead log)
- Every INSERT is converted into a JSON event
- No polling, no triggers, no application changes

--------------------------------------------------

STEP 3 – MESSAGE TRANSPORT (Kafka)
----------------------------------
- Debezium publishes events to Kafka topics
- Kafka provides:
  - Buffering
  - Replay capability
  - Fault tolerance

--------------------------------------------------

STEP 4 – STREAM PROCESSING (Spark Structured Streaming)
-------------------------------------------------------
Spark reads events from Kafka and performs:

1) JSON parsing
2) Filtering only INSERT operations
3) Joining with content table
4) Deriving new fields

Derived fields:
- engagement_seconds = duration_ms / 1000
- engagement_pct = round(engagement_seconds / length_seconds, 2)

If duration_ms or length_seconds is missing:
- engagement_seconds = NULL
- engagement_pct = NULL

--------------------------------------------------

STEP 5 – MULTI-SINK FAN-OUT
---------------------------
Each micro-batch is written to THREE sinks:

A) ClickHouse
--------------
- Column-oriented analytics database
- Stores enriched events for reporting and analysis
- Acts as a BigQuery alternative

B) Redis
---------
Used for real-time use cases:
- content:last:{content_id}    -> last event per content
- content:count:{content_id}   -> total events counter
- top:content:bucket:{minute}  -> per-minute aggregation
- top:content:10m              -> merged top content for last 10 minutes

Redis updates arrive within < 5 seconds of the event.

C) External API
---------------
- HTTP POST endpoint
- Represents a system outside our control
- Demonstrates integration with unreliable external services

--------------------------------------------------
4. EXACTLY-ONCE & FAILURE HANDLING
--------------------------------------------------

- Spark uses checkpointing to track Kafka offsets
- On restart, processing resumes from the last committed state
- Idempotency is enforced using event_id to avoid duplicates
- External API failures can be retried or isolated depending on policy

--------------------------------------------------
5. BACKFILL SUPPORT
--------------------------------------------------

A dedicated backfill script allows reprocessing historical data:

- Reads events from PostgreSQL for a given time range
- Applies the SAME transformations as the streaming job
- Writes to ClickHouse and Redis

This ensures consistency between real-time and historical data.

--------------------------------------------------
6. HOW TO RUN THE SYSTEM
--------------------------------------------------

PREREQUISITES
-------------
- Docker
- Docker Compose

--------------------------------------------------

STEP 1 – Start all services
---------------------------
docker compose up -d

This starts:
- PostgreSQL
- Kafka + Zookeeper
- Debezium Connect
- Spark (master + workers)
- ClickHouse
- Redis
- External API

--------------------------------------------------

STEP 2 – Register Debezium Connector
------------------------------------
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/engagement-events-connector.json

--------------------------------------------------

STEP 3 – Start the Spark Streaming Job
--------------------------------------
docker exec -it spark-client bash -lc "
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/01_kafka_to_clickhouse.py
"

--------------------------------------------------

STEP 4 – Insert Test Data
-------------------------
docker exec -it pg psql -U app -d engagement -c "
INSERT INTO engagement_events
(content_id, user_id, event_type, duration_ms, device)
VALUES (1, 999, 'play', 7000, 'web');
"

--------------------------------------------------

STEP 5 – Verify Outputs
-----------------------

ClickHouse:
docker exec -it clickhouse clickhouse-client -q "
SELECT * FROM engagement.events_enriched ORDER BY event_id DESC LIMIT 5;
"

Redis:
docker exec -it redis redis-cli GET content:last:1
docker exec -it redis redis-cli ZREVRANGE top:content:10m 0 10 WITHSCORES

External API:
docker logs -f external-api

--------------------------------------------------
7. WHY THIS DESIGN
--------------------------------------------------

- Debezium avoids polling and ensures low-latency CDC
- Kafka decouples producers and consumers
- Spark Structured Streaming provides:
  - Exactly-once processing
  - Checkpointing
  - Multi-sink fan-out
- Redis serves real-time, low-latency use cases
- ClickHouse enables fast analytical queries
- Docker Compose ensures reproducibility

--------------------------------------------------
8. POSSIBLE FUTURE IMPROVEMENTS
--------------------------------------------------

- Dead-letter queue for failed external API calls
- Schema Registry for Kafka
- Alerting and monitoring (Prometheus + Grafana)
- Stateful deduplication using event-time watermarks
- Deployment on Kubernetes

--------------------------------------------------
END
--------------------------------------------------
