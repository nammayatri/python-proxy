from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from datetime import datetime
import time
import redis
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
import json
import os
import logging
from logging.handlers import RotatingFileHandler
import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('app.log', maxBytes=10485760, backupCount=5),  # 10MB per file, keep 5 backups
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="GTFS Realtime Feed API", 
              description="A FastAPI service that serves GTFS realtime data from Redis",
              version="1.0.0")

REDIS_HOST = os.getenv("REDIS_HOST", "host.docker.internal")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
APP_PORT = int(os.getenv("APP_PORT", 8004))
REDIS_KEY = os.getenv("REDIS_KEY", "gtfs-rt-tripupdates:bus")
TRAIN_REDIS_KEY = os.getenv("TRAIN_REDIS_KEY", "gtfs-rt-tripupdates:bus")

DEV_MODE = os.getenv("DEV_MODE", "false").lower() == "true"

logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} (DB: {REDIS_DB})")

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD or None,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
        decode_responses=False
    )
    redis_client.ping()
    logger.info("Successfully connected to Redis")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {str(e)}")
    raise
except Exception as e:
    logger.error(f"Unexpected error connecting to Redis: {str(e)}")
    raise

def get_trip_updates_from_redis(redis_key):
    try:
        logger.info(f"Fetching trip updates from Redis key: {redis_key}")
        trip_updates_str = redis_client.get(redis_key)
        trip_updates = json.loads(trip_updates_str)
        if not trip_updates:
            logger.warning(f"No trip updates found in Redis key {redis_key}")
            raise HTTPException(status_code=404, detail=f"No trip updates found in Redis")
        
        logger.debug(f"Retrieved trip data: {trip_updates}")
        
        feed = gtfs_realtime_pb2.FeedMessage()
        
        header_timestamp = trip_updates.get('header', {}).get('timestamp')
        if isinstance(header_timestamp, str):
            header_timestamp = int(header_timestamp)
        
        feed.header.gtfs_realtime_version = trip_updates.get('header', {}).get('gtfsRealtimeVersion', '2.0')
        feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed.header.timestamp = header_timestamp or int(time.time())
        
        for entity_data in trip_updates.get('entity', []):
            entity = feed.entity.add()
            entity.id = entity_data.get('id', '1')
            
            if 'tripUpdate' in entity_data:
                trip_update_data = entity_data['tripUpdate']
                
                if 'trip' in trip_update_data:
                    trip_data = trip_update_data['trip']
                    entity.trip_update.trip.trip_id = trip_data.get('tripId', '')
                    entity.trip_update.trip.start_time = trip_data.get('startTime', '')
                    entity.trip_update.trip.start_date = trip_data.get('startDate', '')
                    entity.trip_update.trip.route_id = trip_data.get('routeId', '')
                    entity.trip_update.trip.direction_id = trip_data.get('directionId', 0)
                
                if 'vehicle' in trip_update_data:
                    vehicle_data = trip_update_data['vehicle']
                    entity.trip_update.vehicle.id = vehicle_data.get('id', '')
                    entity.trip_update.vehicle.label = vehicle_data.get('label', '')
                
                for stop_time_update in trip_update_data.get('stopTimeUpdate', []):
                    update = entity.trip_update.stop_time_update.add()
                    update.stop_sequence = stop_time_update.get('stopSequence', 0)
                    update.stop_id = stop_time_update.get('stopId', '')
                    
                    if 'arrival' in stop_time_update:
                        arrival_time = stop_time_update['arrival'].get('time', 0)
                        if isinstance(arrival_time, str):
                            arrival_time = int(arrival_time)
                        update.arrival.time = arrival_time
                    
                    if 'departure' in stop_time_update:
                        departure_time = stop_time_update['departure'].get('time', 0)
                        if isinstance(departure_time, str):
                            departure_time = int(departure_time)
                        update.departure.time = departure_time
                
                timestamp = trip_update_data.get('timestamp')
                if isinstance(timestamp, str):
                    timestamp = int(timestamp)
                entity.trip_update.timestamp = timestamp or int(time.time())
        
        logger.info(f"Successfully generated feed with {len(feed.entity)} entities")
        return feed
        
    except redis.RedisError as e:
        logger.error(f"Redis error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing trip updates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing trip updates: {str(e)}")


@app.get("/")
async def root():
    """API root with information about available endpoints"""
    return {
        "api": "GTFS Realtime Feed API",
        "version": "1.0.0",
        "endpoints": {
            "bus_trip_updates": "/gtfs-rt-tripupdates/bus",
            "train_trip_updates": "/gtfs-rt-tripupdates/train",
            "vehicle_positions": "/gtfs-rt/vehicle-positions (coming soon)",
            "service_alerts": "/gtfs-rt/service-alerts (coming soon)"
        }
    }


@app.get("/gtfs-rt-tripupdates/bus", tags=["Bus Trip Updates"])
async def bus_trip_updates_info():
    """Information about bus trip updates endpoints"""
    return {
        "description": "GTFS-RT Bus Trip Updates feed",
        "formats": {
            "protobuf": "/gtfs-rt-tripupdates/bus/pb",
            "json": "/gtfs-rt-tripupdates/bus/json"
        }
    }

@app.get("/gtfs-rt-tripupdates/bus/pb", tags=["Bus Trip Updates"])
async def get_bus_trip_updates_protobuf():
    """Get bus trip updates in protobuf format"""
    logger.info("Received request for protobuf bus trip updates")
    feed = get_trip_updates_from_redis(REDIS_KEY)
    return Response(
        content=feed.SerializeToString(),
        media_type="application/x-protobuf"
    )

@app.get("/gtfs-rt-tripupdates/bus/json", tags=["Bus Trip Updates"])
async def get_bus_trip_updates_json():
    """Get bus trip updates in JSON format"""
    logger.info("Received request for JSON bus trip updates")
    feed = get_trip_updates_from_redis(REDIS_KEY)
    return json_format.MessageToDict(feed)

@app.get("/gtfs-rt-tripupdates/train", tags=["Train Trip Updates"])
async def train_trip_updates_info():
    """Information about train trip updates endpoints"""
    return {
        "description": "GTFS-RT Train Trip Updates feed",
        "formats": {
            "protobuf": "/gtfs-rt-tripupdates/train/pb",
            "json": "/gtfs-rt-tripupdates/train/json"
        }
    }

@app.get("/gtfs-rt-tripupdates/train/pb", tags=["Train Trip Updates"])
async def get_train_trip_updates_protobuf():
    """Get train trip updates in protobuf format"""
    logger.info("Received request for protobuf train trip updates")
    feed = get_trip_updates_from_redis(TRAIN_REDIS_KEY)
    return Response(
        content=feed.SerializeToString(),
        media_type="application/x-protobuf"
    )

@app.get("/gtfs-rt-tripupdates/train/json", tags=["Train Trip Updates"])
async def get_train_trip_updates_json():
    """Get train trip updates in JSON format"""
    logger.info("Received request for JSON train trip updates")
    feed = get_trip_updates_from_redis(TRAIN_REDIS_KEY)
    return json_format.MessageToDict(feed)

# Placeholder endpoints for future implementation
@app.get("/gtfs-rt/vehicle-positions", tags=["Coming Soon"])
async def vehicle_positions_info():
    """Information about vehicle positions endpoints (coming soon)"""
    return {
        "status": "Not implemented yet",
        "description": "GTFS-RT Vehicle Positions feed",
        "coming_soon": True
    }

@app.get("/gtfs-rt/service-alerts", tags=["Coming Soon"])
async def service_alerts_info():
    """Information about service alerts endpoints (coming soon)"""
    return {
        "status": "Not implemented yet",
        "description": "GTFS-RT Service Alerts feed",
        "coming_soon": True
    }

@app.on_event("startup")
async def startup_event():
    logger.info("Application startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated")

if __name__ == "__main__":
    logger.info(f"Starting application on port {APP_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT) 