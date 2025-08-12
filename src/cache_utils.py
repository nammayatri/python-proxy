"""
Cache utilities and Redis operations for the GPS tracking system.
Contains the SimpleCache class and vehicle location history management.
"""

import json
import logging
import time
from typing import Optional, List, Dict, Any

logger = logging.getLogger('amnex-data-server')


class SimpleCache:
    """Simple in-memory cache with Redis fallback"""
    
    def __init__(self, redis_client):
        self.cache = {}
        self.redis_client = redis_client

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache, checking in-memory first, then Redis"""
        res = self.cache.get(key)
        if res:
            value, expiry_timestamp = res
            if expiry_timestamp is not None and expiry_timestamp < time.time():
                del self.cache[key]  # Expired
                res = None
            else:
                return value

        if res is None:
            res_from_redis = self.redis_client.get(f"simpleCache:{key}")
            if res_from_redis:
                parsed_res = json.loads(res_from_redis)
                # When loading from Redis, get the TTL from Redis and apply it to the in-memory cache
                redis_ttl = self.redis_client.ttl(f"simpleCache:{key}")
                in_memory_expiry_timestamp = None
                if redis_ttl is not None and redis_ttl > -1:  # -1 means no expire, -2 means key doesn't exist
                    in_memory_expiry_timestamp = time.time() + redis_ttl

                self.cache[key] = (parsed_res, in_memory_expiry_timestamp)
                return parsed_res
            else:
                return None
        return res

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in both in-memory cache and Redis"""
        expiry_timestamp = None
        if ttl is not None:
            expiry_timestamp = time.time() + ttl
        self.cache[key] = (value, expiry_timestamp)
        
        if ttl is None:
            self.redis_client.set(f"simpleCache:{key}", json.dumps(value))
        else:
            self.redis_client.setex(f"simpleCache:{key}", ttl, json.dumps(value))


def store_vehicle_location_history(redis_client, device_id: str, lat: float, lon: float, 
                                 timestamp: int, max_points: int = 25) -> None:
    """Store vehicle location history in Redis with TTL"""
    from .geometry_utils import calculate_distance
    
    history = None
    try:
        history_key = f"vehicle_history:{device_id}"
        point = {
            "lat": lat,
            "lon": lon,
            "timestamp": int(timestamp if timestamp else time.time())
        }
        
        # Get existing history
        history = redis_client.get(history_key)
        if history:
            points = json.loads(history) or []
        else:
            points = []
        
        if len(points) > 0:
            last_point = points[-1]
            if calculate_distance(last_point['lat'], last_point['lon'], point['lat'], point['lon']) < 0.002:
                return
            
        # Add new point
        points.append(point)
        
        # Keep only last max_points
        if len(points) > max_points:
            points = points[-max_points:]
        
        points.sort(key=lambda x: x['timestamp'])
        # Store updated history with 1 hour TTL
        redis_client.setex(history_key, 3600, json.dumps(points))
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Error storing vehicle history for {device_id}: {e}\nHistory value: {history}\nTraceback: {error_details}")


def get_vehicle_location_history(redis_client, device_id: str) -> List[Dict[str, Any]]:
    """Get vehicle location history from Redis"""
    try:
        history_key = f"vehicle_history:{device_id}"
        history = redis_client.get(history_key)
        if history:
            value = json.loads(history)
            if value:
                return value
        return []
    except Exception as e:
        logger.error(f"Error getting vehicle history for {device_id}: {e}")
        return []