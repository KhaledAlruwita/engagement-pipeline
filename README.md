
# Engagement streaming pipeline

a real-time data streaming system that captures
new rows from a PostgreSQL table, enriches them, and fans out the results
to multiple destinations with low latency and high reliability.



## What it does


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
## Deployment

To deploy this project run

```bash
  docker compose up -d
```
This starts:
- PostgreSQL
- Kafka + Zookeeper
- Debezium Connect
- Spark (master + workers)
- ClickHouse
- Redis
- External API

Register Debezium Connector
------------------------------------
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/engagement-events-connector.json
```
Start the Spark Streaming Job
--------------------------------------
```bash
docker exec -it spark-client bash -lc "
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/01_kafka_to_clickhouse.py
"
```

Insert Test Data
-------------------------
```bash
docker exec -it pg psql -U app -d engagement -c "
INSERT INTO engagement_events
(content_id, user_id, event_type, duration_ms, device)
VALUES (1, 999, 'play', 7000, 'web');
"
```
Verify Outputs
-----------------------

ClickHouse:
```bash
docker exec -it clickhouse clickhouse-client -q "
SELECT * FROM engagement.events_enriched ORDER BY event_id DESC LIMIT 5;
"
```

Redis:
```bash
docker exec -it redis redis-cli GET content:last:1
docker exec -it redis redis-cli ZREVRANGE top:content:10m 0 10 WITHSCORES
```
External API:
```bash
docker logs -f external-api
```
Backfill:
```bash
python backfill.py --start 2026-01-08T09:00:00 --end 2026-01-08T10:00:00
```
