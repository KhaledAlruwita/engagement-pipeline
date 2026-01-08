import os, json
import psycopg2
import redis
from datetime import datetime
from clickhouse_connect import get_client
PG_DSN = "dbname=engagement user=app password=app host=pg port=5432"
REDIS_HOST = "redis"
CH_HOST = "clickhouse"


def main(start: str, end: str):
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)

    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    ch = get_client(host=CH_HOST, port=8123, username="spark", password="spark", database="engagement")

    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute("""
      SELECT e.id, e.content_id, e.user_id, e.event_type, e.event_ts, e.duration_ms, e.device,
             c.content_type, c.length_seconds
      FROM engagement_events e
      LEFT JOIN content c ON c.content_id = e.content_id
      WHERE e.event_ts >= %s AND e.event_ts < %s
      ORDER BY e.id
    """, (start_dt, end_dt))

    rows = cur.fetchall()

    out_rows = []
    for (eid, cid, uid, etype, ets, dur, dev, ctype, length) in rows:
        # idempotency
        if not r.set(f"event:seen:{eid}", "1", nx=True, ex=86400):
            continue

        eng_s = None if dur is None else dur / 1000.0
        eng_pct = None
        if dur is not None and length not in (None, 0):
            eng_pct = round((dur / 1000.0) / length, 2)

        payload = {
            "event_id": int(eid),
            "content_id": int(cid),
            "user_id": int(uid),
            "event_type": etype,
            "event_ts": ets.isoformat(sep=" "),
            "duration_ms": dur,
            "device": dev,
            "content_type": ctype,
            "length_seconds": length,
            "engagement_seconds": float(eng_s) if eng_s is not None else None,
            "engagement_pct": float(eng_pct) if eng_pct is not None else None,
        }

        r.set(f"content:last:{cid}", json.dumps(payload), ex=3600)
        r.incr(f"content:count:{cid}")

        bucket = ets.strftime("%Y%m%d%H%M")
        r.zincrby(f"top:content:bucket:{bucket}", 1, str(cid))
        r.expire(f"top:content:bucket:{bucket}", 15 * 60)

        out_rows.append((
            int(eid), int(cid), int(uid), etype, ets, dur, dev,
            ctype, length,
            eng_s, eng_pct
        ))

    if out_rows:
        ch.insert(
            "events_enriched",
            out_rows,
            column_names=[
                "event_id","content_id","user_id","event_type","event_ts","duration_ms","device",
                "content_type","length_seconds","engagement_seconds","engagement_pct"
            ]
        )

    print(f"Backfill done. inserted={len(out_rows)}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="ISO datetime, e.g. 2026-01-08T09:00:00")
    p.add_argument("--end", required=True, help="ISO datetime, e.g. 2026-01-08T10:00:00")
    args = p.parse_args()
    main(args.start, args.end)
