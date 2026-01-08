import redis
from datetime import datetime, timedelta

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

now = datetime.utcnow()
buckets = [(now - timedelta(minutes=i)).strftime("%Y%m%d%H%M") for i in range(10)]

tmp_key = "top:content:10m"
r.delete(tmp_key)

for b in buckets:
    key = f"top:content:bucket:{b}"
 
    for member, score in r.zscan_iter(key):
        r.zincrby(tmp_key, float(score), member)

r.expire(tmp_key, 120)

print("Top 10m:")
print(r.zrevrange(tmp_key, 0, 10, withscores=True))
