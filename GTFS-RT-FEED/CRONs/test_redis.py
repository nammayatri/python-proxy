import os
import redis

REDIS_HOST = os.getenv('REDIS_HOST', 'beckn-lts-redis.fdmlwb.ng.0001.aps1.cache.amazonaws.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 3))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB
)

# Test connection
try:
    keys = r.keys('*')
    print(f"Found {len(keys)} keys in DB {REDIS_DB}:")
    for key in keys:
        print(key.decode() if isinstance(key, bytes) else key)
except redis.ConnectionError as e:
    print("Redis connection failed:", e)
