import socket
from confluent_kafka import Producer, KafkaError, KafkaException
import os
import json
from datetime import datetime, date, timedelta
import threading
from rediscluster import RedisCluster
import redis
import time
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from pydantic import BaseModel
from typing import Optional, List
import requests
from concurrent.futures import ThreadPoolExecutor
import math
import traceback
import logging
import atexit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('amnex-data-server')

HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 8080        # Port 443 (normally used for HTTPS, but this is plaintext)

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amnex_direct_live')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9096')

# Redis connection setup
REDIS_NODES = os.getenv('REDIS_NODES', 'localhost:6379').split(',')
IS_CLUSTER_REDIS = os.getenv('IS_CLUSTER_REDIS', 'false').lower() == 'true'

# TCP forwarding configuration
CHALO_URL = os.getenv('CHALO_URL', "chennai-gps.chalo.com")
CHALO_PORT = int(os.getenv('CHALO_PORT', '1544'))
FORWARD_TCP = os.getenv('FORWARD_TCP', 'true').lower() == 'true'
TCP_FORWARD_TIMEOUT = int(os.getenv('TCP_FORWARD_TIMEOUT', '5'))  # Socket timeout in seconds
TCP_MAX_RETRIES = int(os.getenv('TCP_MAX_RETRIES', '3'))  # Maximum retry attempts
TCP_RECONNECT_INTERVAL = int(os.getenv('TCP_RECONNECT_INTERVAL', '2555'))  # Seconds between reconnection attempts

# Setup Kafka producer with better config for high load
producer_config = {
    'bootstrap.servers': KAFKA_SERVER,
    'queue.buffering.max.messages': 1000000,  # Increase buffer size (default is 100,000)
    'queue.buffering.max.ms': 100,  # Batch more frequently
    'compression.type': 'snappy',  # Add compression to reduce bandwidth
    'retry.backoff.ms': 250,  # Shorter backoff for retries
    'message.max.bytes': 1000000,  # Allow larger messages
    'request.timeout.ms': 30000,  # Longer timeout
    'delivery.timeout.ms': 120000,  # Allow more time for delivery
    'message.send.max.retries': 5  # More retries before giving up
}

producer = Producer(producer_config)

# Redis connection setup
if IS_CLUSTER_REDIS:
    # Redis Cluster setup
    startup_nodes = [{"host": node.split(":")[0], "port": int(node.split(":")[1])} for node in REDIS_NODES]
    redis_client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)
    print("✅ Connected to Redis Cluster")
else:
    # Redis Standalone setup (assume first node for standalone)
    STANDALONE_REDIS_DATABASE = int(os.getenv('STANDALONE_REDIS_DATABASE', '1'))
    host, port = REDIS_NODES[0].split(":")
    redis_client = redis.StrictRedis(host=host, port=int(port), db=STANDALONE_REDIS_DATABASE, decode_responses=True)
    print(f"✅ Connected to Redis Standalone at {host}:{port} (DB={STANDALONE_REDIS_DATABASE})")

# Database configuration
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'gps_tracking')

# SQLAlchemy setup
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"DATABASE_URL: {DATABASE_URL}")
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=30,
    pool_timeout=30,
    pool_recycle=1800,
    connect_args={
        "options": "-c search_path=atlas_app"
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Pydantic model for type validation
class RouteDeviceMapping(BaseModel):
    id: int
    route_id: str
    device_id: str
    created_at: datetime
    updated_at: Optional[datetime]
    is_active: bool

    class Config:
        orm_mode = True

# Update the SQLAlchemy models
class DeviceVehicleMapping(Base):
    __tablename__ = "device_vehicle_mapping"
    __table_args__ = {'schema': 'atlas_app'}
    vehicle_no = Column(Text, index=True)
    device_id = Column(Text, index=True, primary_key=True)

class VehicleRouteMapping(Base):
    __tablename__ = "vehicle_route_mapping"
    __table_args__ = {'schema': 'atlas_app'}
    vehicle_no = Column(Text, index=True, primary_key=True)
    route_id = Column(Text, index=True)

class RouteStopMapping(Base):
    __tablename__ = "route_stop_mapping"
    __table_args__ = {'schema': 'atlas_app'}
    
    stop_code = Column(Integer, primary_key=True)
    route_code = Column(Text, index=True)
    sequence_num = Column(Integer)
    stop_lat = Column(Text)
    stop_lon = Column(Text)
    stop_name = Column(Text)

# Don't create tables since we're using existing table
# Base.metadata.create_all(bind=engine)

# Environment variables for route data configuration
USE_OSRM = os.getenv('USE_OSRM', 'true').lower() == 'true'
OSRM_URL = os.getenv('OSRM_URL', 'http://router.project-osrm.org')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', '')
ROUTE_CACHE_TTL = int(os.getenv('ROUTE_CACHE_TTL', '3600'))  # 1 hour default

class StopTracker:
    def __init__(self, db_engine, redis_client, use_osrm=USE_OSRM, 
                 osrm_url=OSRM_URL, google_api_key=GOOGLE_API_KEY, 
                 cache_ttl=ROUTE_CACHE_TTL):
        self.db_engine = db_engine
        self.redis_client = redis_client
        self.use_osrm = use_osrm
        self.osrm_url = osrm_url
        self.google_api_key = google_api_key
        self.cache_ttl = cache_ttl
        self.stop_visit_radius = float(os.getenv('STOP_VISIT_RADIUS', '0.05'))  # 50 meters in km
        print(f"StopTracker initialized with {'OSRM' if use_osrm else 'Google Maps'}")
        
    def get_route_stops(self, route_id):
        """Get all stops for a route ordered by sequence"""
        cache_key = f"route_stops:{route_id}"
        
        # Check cache
        cached = cache.get(cache_key)
        if cached:
            return cached
            
        # Get from DB
        try:
            with SessionLocal() as db:
                stops = db.query(RouteStopMapping)\
                    .filter(RouteStopMapping.route_code == route_id)\
                    .order_by(RouteStopMapping.sequence_num)\
                    .all()
                
                if not stops:
                    return []
                    
                # Format results
                result = [
                    {
                        'stop_id': stop.stop_code,
                        'sequence': stop.sequence_num,
                        'name': stop.stop_name,
                        'stop_lat': float(stop.stop_lat),
                        'stop_lon': float(stop.stop_lon)
                    }
                    for stop in stops
                ]
                
                # Cache result
                cache.set(cache_key, result)
                return result
        except Exception as e:
            print(f"Error getting stops for route {route_id}: {e}")
            return []
    
    def get_visited_stops(self, route_id, vehicle_id):
        """Get list of stops already visited by this vehicle on this route"""
        visit_key = f"visited_stops:{route_id}:{vehicle_id}"
        try:
            visited_stops = self.redis_client.get(visit_key)
            if visited_stops:
                return json.loads(visited_stops)
            return []
        except Exception as e:
            logger.error(f"Error getting visited stops: {e}")
            return []
    
    def update_visited_stops(self, route_id, vehicle_id, stop_id):
        """Add a stop to the visited stops list"""
        visit_key = f"visited_stops:{route_id}:{vehicle_id}"
        try:
            visited_stops = self.get_visited_stops(route_id, vehicle_id)
            if stop_id not in visited_stops:
                visited_stops.append(stop_id)
                self.redis_client.setex(
                    visit_key, 
                    86400,  # 24 hour TTL
                    json.dumps(visited_stops)
                )
                logger.info(f"Added stop {stop_id} to visited stops for vehicle {vehicle_id} on route {route_id}")
            return visited_stops
        except Exception as e:
            logger.error(f"Error updating visited stops: {e}")
            return []
    
    def reset_visited_stops(self, route_id, vehicle_id):
        """Reset the visited stops list for a vehicle"""
        visit_key = f"visited_stops:{route_id}:{vehicle_id}"
        try:
            self.redis_client.delete(visit_key)
            logger.info(f"Reset visited stops for vehicle {vehicle_id} on route {route_id}")
            return True
        except Exception as e:
            logger.error(f"Error resetting visited stops: {e}")
            return False
    
    def check_if_at_stop(self, stop, vehicle_lat, vehicle_lon):
        """Check if vehicle is within radius of a stop"""
        # Calculate distance using haversine formula
        lat1, lon1 = math.radians(vehicle_lat), math.radians(vehicle_lon)
        lat2, lon2 = math.radians(float(stop['stop_lat'])), math.radians(float(stop['stop_lon']))
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        distance = 6371 * c  # Radius of earth in kilometers
        
        return distance <= self.stop_visit_radius, distance
    
    def find_next_stop(self, stops, visited_stops):
        """Find the next stop in sequence after the last visited stop"""
        if not visited_stops:
            # If no stops visited yet, the next stop is the first in sequence
            return stops[0] if stops else None
        
        # Get the last visited stop ID
        last_visited_id = visited_stops[-1]
        
        # Find its index in the stops list
        last_index = -1
        for i, stop in enumerate(stops):
            if stop['stop_id'] == last_visited_id:
                last_index = i
                break
                
        # If we found the last stop and it's not the last in the route
        if last_index >= 0 and last_index < len(stops) - 1:
            return stops[last_index + 1]
        elif last_index == len(stops) - 1:
            # We're at the last stop of the route
            return None
            
        # If we couldn't find the last visited stop in the list
        # (this shouldn't happen but just in case)
        return stops[0] if stops else None
        
    def find_closest_stop(self, stops, vehicle_lat, vehicle_lon):
        """Find the closest stop to the given coordinates"""
        if not stops:
            return None, float('inf')
            
        closest_stop = None
        min_distance = float('inf')
        
        for stop in stops:
            # Calculate distance using haversine formula
            lat1, lon1 = math.radians(vehicle_lat), math.radians(vehicle_lon)
            lat2, lon2 = math.radians(float(stop['stop_lat'])), math.radians(float(stop['stop_lon']))
            
            # Haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            distance = 6371 * c  # Radius of earth in kilometers
            
            if distance < min_distance:
                min_distance = distance
                closest_stop = stop
                
        return closest_stop, min_distance
    
    def get_travel_duration(self, origin_id, dest_id, origin_lat, origin_lon, dest_lat, dest_lon):
        """Get travel duration between two stops with caching"""
        # Try to get from cache
        cache_key = f"route_segment:{origin_id}:{dest_id}"
        try:
            cached = self.redis_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data.get('duration')
        except Exception as e:
            print(f"Redis error: {e}")
        
        # Not in cache, calculate using routing API
        try:
            duration = None    
            # Fallback to simple estimation (30 km/h)
            # Calculate distance using haversine
            lat1, lon1 = math.radians(origin_lat), math.radians(origin_lon)
            lat2, lon2 = math.radians(dest_lat), math.radians(dest_lon)
            
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            distance = 6371000 * c  # Radius of earth in meters
            
            # Estimate duration: distance / speed (30 km/h = 8.33 m/s)
            duration = distance / 8.33
            
            # Cache the fallback estimation
            cache_data = {
                'duration': duration,
                'timestamp': datetime.now().isoformat(),
                'estimated': True
            }
            self.redis_client.setex(cache_key, self.cache_ttl, json.dumps(cache_data))
            
            return duration
        except Exception as e:
            print(f"Error calculating travel duration: {e}")
            return None
    
    def calculate_eta(self, route_id, vehicle_lat, vehicle_lon, current_time, vehicle_id=None):
        """Calculate ETA for all upcoming stops from current position"""
        # Get all stops for the route
        stops = self.get_route_stops(route_id)
        if not stops:
            return None
        
        visited_stops = []
        next_stop = None
        closest_stop = None
        distance = float('inf')
        calculation_method = "realtime"
        
        # If we have a vehicle ID, use visited stops logic
        if vehicle_id:
            visited_stops = self.get_visited_stops(route_id, vehicle_id)
            
            # Check if the vehicle is at a stop now
            for stop in stops:
                is_at_stop, stop_distance = self.check_if_at_stop(stop, vehicle_lat, vehicle_lon)
                if is_at_stop:
                    # Vehicle is at this stop
                    if stop['stop_id'] not in visited_stops:
                        # Add to visited stops if not already there
                        logger.info(f"Vehicle {vehicle_id} is at stop {stop['stop_id']}")
                        visited_stops = self.update_visited_stops(route_id, vehicle_id, stop['stop_id'])
                        calculation_method = "visited_stops"
                    break
                        
            # Find next stop based on visited stops
            next_stop = self.find_next_stop(stops, visited_stops)
            
            if next_stop:
                # Calculate distance to next stop
                _, distance = self.check_if_at_stop(next_stop, vehicle_lat, vehicle_lon)
                closest_stop = next_stop
                calculation_method = "sequence_based"
            else:
                # We're at the end of the route, reset visited stops
                self.reset_visited_stops(route_id, vehicle_id)
                # Fall back to closest stop method
                closest_stop, distance = self.find_closest_stop(stops, vehicle_lat, vehicle_lon)
                calculation_method = "distance_based_fallback"
        
        # If no vehicle ID or no next stop found, fall back to closest stop method
        if not vehicle_id or not closest_stop:
            closest_stop, distance = self.find_closest_stop(stops, vehicle_lat, vehicle_lon)
            calculation_method = "distance_based"
            
        if not closest_stop:
            return None
            
        # Find the index of the closest/next stop in the route
        closest_index = -1
        for i, stop in enumerate(stops):
            if stop['stop_id'] == closest_stop['stop_id']:
                closest_index = i
                break
                
        if closest_index == -1:
            # Something went wrong, stop not found in the list
            return None
            
        # Calculate ETAs for the closest stop and all upcoming stops
        eta_list = []
        cumulative_time = 0
        current_lat, current_lon = vehicle_lat, vehicle_lon
        
        # First, calculate ETA for the closest/next stop
        if distance <= 0.01:  # 10 meters in km - we're practically at the stop
            arrival_time = current_time
            calculation_method = "immediate"
        else:
            # Calculate time to reach the stop
            duration = self.get_travel_duration(
                0, closest_stop['stop_id'],
                current_lat, current_lon,
                closest_stop['stop_lat'], closest_stop['stop_lon']
            )
            
            if duration:
                arrival_time = current_time + timedelta(seconds=duration)
                cumulative_time = duration
                calculation_method = "osrm" if self.use_osrm else "google_maps"
            else:
                # Fallback estimation
                duration = distance / 8.33  # distance / (30 km/h in m/s)
                arrival_time = current_time + timedelta(seconds=duration)
                cumulative_time = duration
                calculation_method = "estimated"
        
        # Add closest/next stop to the ETA list
        eta_list.append({
            'stop_id': closest_stop['stop_id'],
            'stop_seq': closest_stop['sequence'],
            'stop_name': closest_stop['name'],
            'stop_lat': closest_stop['stop_lat'],
            'stop_lon': closest_stop['stop_lon'],
            'arrival_time': int(arrival_time.timestamp()),
            'calculation_method': calculation_method
        })
        
        # Then calculate ETAs for all remaining stops (everything after closest_index)
        for i in range(closest_index + 1, len(stops)):
            prev_stop = stops[i-1]
            current_stop = stops[i]
            
            # Calculate duration between stops
            duration = self.get_travel_duration(
                prev_stop['stop_id'], current_stop['stop_id'],
                prev_stop['stop_lat'], prev_stop['stop_lon'],
                current_stop['stop_lat'], current_stop['stop_lon']
            )
            
            if duration:
                cumulative_time += duration
                arrival_time = current_time + timedelta(seconds=cumulative_time)
                
                # Determine calculation method
                if self.use_osrm:
                    calculation_method = "osrm"
                else:
                    calculation_method = "google_maps"
                
                eta_list.append({
                    'stop_id': current_stop['stop_id'],
                    'stop_seq': current_stop['sequence'],
                    'stop_name': current_stop['name'],
                    'stop_lat': current_stop['stop_lat'],
                    'stop_lon': current_stop['stop_lon'],
                    'arrival_time': int(arrival_time.timestamp()),
                    'calculation_method': calculation_method
                })
            else:
                # If we couldn't calculate duration, use estimated method
                calculation_method = "estimated"
        
        return {
            'route_id': route_id,
            'current_time': int(current_time.timestamp()),
            'closest_stop': {
                'stop_id': closest_stop['stop_id'],
                'stop_name': closest_stop['name'],
                'distance': distance
            },
            'calculation_method': calculation_method,
            'eta': eta_list
        }

# Create instance
stop_tracker = StopTracker(engine, redis_client)

class SimpleCache:
    def __init__(self):
        self.cache = {}

    def get(self, key: str):
        res = self.cache.get(key)
        if res == None:
            res_from_redis = redis_client.get(f"simpleCache:{key}")
            if res_from_redis:
                parsed_res = json.loads(res_from_redis)
                self.cache[key] = parsed_res
                return parsed_res
            else:
                return None
        return res

    def set(self, key: str, value):
        self.cache[key] = value
        redis_client.set(f"simpleCache:{key}", json.dumps(value))
# Create single cache instance
cache = SimpleCache()

def get_route_for_device(device_id: str) -> Optional[str]:
    """Get the active route ID for a device with caching"""
    cache_key = f"device:{device_id}"
    
    # Check cache first
    cached_route = cache.get(cache_key)
    if cached_route is not None:
        if cached_route == "NOT_AVAILABLE":
            return None
        return cached_route

    try:
        with SessionLocal() as db:
            # First get the fleet number for the device
            fleet_mapping = db.query(DeviceVehicleMapping)\
                .filter(DeviceVehicleMapping.device_id == device_id)\
                .first()
            
            if not fleet_mapping:
                cache.set(cache_key, "NOT_AVAILABLE")
                return None

            # Then get the route for that fleet
            route_mapping = db.query(VehicleRouteMapping)\
                .filter(VehicleRouteMapping.vehicle_no == fleet_mapping.vehicle_no)\
                .first()
            
            route_id = route_mapping.route_id if route_mapping else "NOT_AVAILABLE"
            cache.set(cache_key, route_id)
            return route_id

    except Exception as e:
        print(f"Error querying route for device {device_id}: {e}")
        return None

def get_devices_for_route(route_id: str) -> List[str]:
    """Get all active devices for a route with caching"""
    cache_key = f"route:{route_id}"
    
    # Check cache first
    cached_devices = cache.get(cache_key)
    if cached_devices is not None:
        return cached_devices

    try:
        with SessionLocal() as db:
            # First get all fleet numbers for this route
            fleet_numbers = db.query(VehicleRouteMapping.vehicle_no)\
                .filter(VehicleRouteMapping.route_id == route_id)\
                .all()
            
            if not fleet_numbers:
                cache.set(cache_key, [])
                return []

            # Then get all devices for these fleet numbers
            vehicle_nos = [f[0] for f in fleet_numbers]  # Extract fleet numbers from result tuples
            devices = db.query(DeviceVehicleMapping.device_id)\
                .filter(DeviceVehicleMapping.vehicle_no.in_(vehicle_nos))\
                .all()
            
            device_list = [d[0] for d in devices]  # Extract device IDs from result tuples
            cache.set(cache_key, device_list)
            return device_list

    except Exception as e:
        print(f"Error querying devices for route {route_id}: {e}")
        return []

def get_fleet_info(device_id: str) -> dict:
    """Get both fleet number and route ID for a device"""
    cache_key = f"fleetInfo:{device_id}"
    
    # Check cache first
    fleet_info = cache.get(cache_key)
    if fleet_info is not None:
        return fleet_info

    try:
        with SessionLocal() as db:
            # Get fleet number for device
            fleet_mapping = db.query(DeviceVehicleMapping)\
                .filter(DeviceVehicleMapping.device_id == device_id)\
                .first()
            
            if not fleet_mapping:
                cache.set(cache_key, {})
                return {}

            # Get route for fleet
            route_mapping = db.query(VehicleRouteMapping)\
                .filter(VehicleRouteMapping.vehicle_no == fleet_mapping.vehicle_no)\
                .first()
            
            val = {
                'vehicle_no': fleet_mapping.vehicle_no,
                'route_id': route_mapping.route_id if route_mapping else None
            }
            cache.set(cache_key, val)
            return val

    except Exception as e:
        print(f"Error querying fleet info for device {device_id}: {e}")
        return {}

def date_to_unix(d: date) -> int:
    return int(d.timestamp())

def parse_coordinate(coord_str, dir_char, is_latitude):
    # Split the coordinate string and direction
    coord, direction = coord_str.strip(), dir_char.strip().upper()
    
    # Determine degrees and minutes based on coordinate type
    if is_latitude:
        degrees = int(coord[:2])
        minutes = float(coord[2:])
    else:
        degrees = int(coord[:3])
        minutes = float(coord[3:])
    
    # Convert to decimal degrees
    decimal_deg = degrees + minutes / 60
    
    # Apply direction sign
    if direction in ['S', 'W']:
        decimal_deg *= -1
    
    return decimal_deg

def dd_mm_ss_to_date(date_str: str) -> datetime.date:
    try:
        return datetime.strptime(date_str, "%d/%m/%Y-%H:%M:%S")
    except:
        return datetime.strptime(date_str, "%d/%m/%y-%H:%M:%S")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def parse_chalo_payload(payload, serverTime, client_ip):
    """
    Parse the payload from Chalo format.
    
    Format example:
    $Header,iTriangle,1_36T02B0164MAIS_6,NR,16,L,868728039301806,KA01G1234,1,19032025,143947,12.831032,N,80.225189,E,28.0,269,17,30.0,0.00,0.68,CellOne,1,1,26.9,4.3,0,C,9,404,64,091D,8107,33,8267,091d,25,8107,091d,20,8194,091d,17,8195,091d,0101,01,492430,0.008,0.008,86,()*29
    """ 
    try:
        # Extract required fields from payload
        dataState = payload[5]  # Data state
        deviceId = payload[6]  # IMEI number
        vehicleNumber = payload[7]  # Vehicle registration number
        dateStr = payload[9]  # Date in DDMMYYYY format
        timeStr = payload[10]  # Time in HHMMSS format
        latitude = float(payload[11])  # Direct decimal degrees
        latDir = payload[12]  # 'N' or 'S'
        longitude = float(payload[13])  # Direct decimal degrees
        longDir = payload[14]  # 'E' or 'W'
        speed = float(payload[15])  # Speed in km/h
        
        # Format date and time
        dateFormatted = datetime.strptime(dateStr, "%d%m%Y")
        timeFormatted = datetime.strptime(timeStr, "%H%M%S").time()
        timestamp = datetime.combine(dateFormatted.date(), timeFormatted)
        
        # Apply direction sign
        if latDir == 'S':
            latitude *= -1
        if longDir == 'W':
            longitude *= -1
            
        entity = {
            "lat": latitude,
            "long": longitude,
            "deviceId": deviceId,
            "version": None,
            "timestamp": date_to_unix(timestamp),
            "vehicleNumber": vehicleNumber,
            "speed": speed,
            "pushedToKafkaAt": date_to_unix(datetime.now()),
            "dataState": dataState,
            "serverTime": date_to_unix(serverTime),
            "provider": "chalo",
            "raw": payload,
            "client_ip": client_ip
        }
        
        return entity
    except Exception as e:
        print(f"Error parsing Chalo payload: {e}")
        return None

def parse_amnex_payload(payload, serverTime, client_ip):
    """Parse the payload from Amnex format."""
    try:
        if len(payload) >= 14 and payload[0] == "&PEIS" and payload[1] == "N" and payload[2] == "VTS" and payload[10] == 'A':
            latitude = parse_coordinate(payload[11], payload[12], True)
            longitude = parse_coordinate(payload[13], payload[14], False)
            version = payload[4]
            deviceId = payload[5]
            timestamp = payload[8]
            date = payload[9]
            routeNumber = payload[17]
            date = dd_mm_ss_to_date(date + "-" + timestamp)
            dataState = payload[3]
            raw = payload
            entity = {
                "lat": latitude,
                "long": longitude,
                "version": version,
                "deviceId": deviceId,
                "timestamp": date_to_unix(date),
                "dataState": dataState,
                "routeNumber": routeNumber,
                "pushedToKafkaAt": date_to_unix(datetime.now()),
                "serverTime": date_to_unix(serverTime),
                "raw": raw,
                "provider": "amnex",
                "client_ip": client_ip
            }
            return entity
        return None
    except Exception as e:
        print(f"Error parsing Amnex payload: {e}")
        return None

def parse_payload(data_decoded, client_ip, serverTime):
    """Parse payload data by determining the format"""
    try:
        payload = data_decoded.split(",")
        
        # Parse payload based on format
        if len(payload) > 0 and payload[0].endswith("$Header"):
            return parse_chalo_payload(payload, serverTime, client_ip)
        elif len(payload) >= 14 and payload[0] == "&PEIS":
            return parse_amnex_payload(payload, serverTime, client_ip)
        
        return None
    except Exception as e:
        print(f"Error parsing payload: {e}")
        return None

# Persistent TCP connection handler
class TCPClient:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = TCPClient(CHALO_URL, CHALO_PORT)
        return cls._instance
    
    def __init__(self, host, port, reconnect_interval=5):
        self.host = host
        self.port = port
        self.reconnect_interval = reconnect_interval
        self.socket = None
        self.connected = False
        self.lock = threading.Lock()
        self.connect_thread = None
        self._stop_event = threading.Event()
        self._message_queue = []
        self._queue_lock = threading.Lock()
        
    def start(self):
        """Start connection manager and message sender threads"""
        if self.connect_thread and self.connect_thread.is_alive():
            return  # Already running
            
        self._stop_event.clear()
        self.connect_thread = threading.Thread(target=self._connection_manager, daemon=True)
        self.connect_thread.start()
        
        # Start message processor thread
        self.message_thread = threading.Thread(target=self._process_message_queue, daemon=True)
        self.message_thread.start()
        
        logger.info(f"TCP Client started for {self.host}:{self.port}")
        
    def stop(self):
        """Stop connection manager gracefully"""
        self._stop_event.set()
        if self.connect_thread and self.connect_thread.is_alive():
            self.connect_thread.join(timeout=5)
        self._close_socket()
        
    def _connection_manager(self):
        """Maintains persistent TCP connection, reconnecting as needed"""
        while not self._stop_event.is_set():
            if not self.connected:
                self._establish_connection()
            time.sleep(0.1)  # Small delay to avoid tight loop
                
    def _establish_connection(self):
        """Establish connection with retry logic"""
        try:
            self._close_socket()  # Close any existing socket
            
            # Create new socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)  # Connection timeout
            logger.info(f"Connecting to {self.host}:{self.port}...")
            self.socket.connect((self.host, self.port))
            self.socket.settimeout(None)  # Remove timeout for normal operation
            
            # Set keepalive options
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                # These options may not be available on all systems
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
            except (AttributeError, OSError):
                pass  # Ignore if these options are not available
                
            logger.info(f"✅ Successfully connected to {self.host}:{self.port}")
            self.connected = True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.host}:{self.port}: {str(e)}")
            self.connected = False
            time.sleep(self.reconnect_interval)
    
    def _close_socket(self):
        """Close the socket connection"""
        with self.lock:
            if self.socket:
                try:
                    self.socket.close()
                except Exception as e:
                    logger.error(f"Error closing socket: {str(e)}")
                finally:
                    self.socket = None
                    self.connected = False
    
    def queue_message(self, message):
        """Add message to queue for sending"""
        message = message.strip()
        message = message +'#'
        with self._queue_lock:
            logger.info(f"Queueing message: {message}")
            self._message_queue.append(message)
            
    def _process_message_queue(self):
        """Process queued messages in background"""
        logger.info("Starting message processing thread...")
        while not self._stop_event.is_set():
            messages_to_send = []
            
            # Get all queued messages
            with self._queue_lock:
                if self._message_queue:
                    messages_to_send = self._message_queue.copy()
                    self._message_queue.clear()

            logger.info(f"Processing {len(messages_to_send)} queued messages")
                    
            # Send all queued messages
            if messages_to_send and self.connected:
                for message in messages_to_send:
                    self._send_message(message)
            logger.info(f"Processed {len(messages_to_send)} messages")
            time.sleep(0.1)  # Small delay
    
    def _send_message(self, data):
        """Send a single message over TCP connection"""
        with self.lock:
            if not self.connected or not self.socket:
                logger.error("Not connected, queuing message for later")
                with self._queue_lock:
                    self._message_queue.append(data)
                return False
                
            try:
                # Make sure data ends with newline
                if not data.endswith('\n'):
                    data += '\n'
                
                self.socket.sendall(data.encode())
                return True
            except Exception as e:
                logger.error(f"Error sending data: {str(e)}")
                self.connected = False  # Mark as disconnected for reconnection
                
                # Re-queue the message
                with self._queue_lock:
                    self._message_queue.append(data)
                return False

# Create and start singleton TCP client
tcp_client = None
if FORWARD_TCP:
    tcp_client = TCPClient.get_instance()
    tcp_client.start()
    
    # Register shutdown handler
    def shutdown_tcp_client():
        if tcp_client:
            logger.info("Shutting down TCP client...")
            tcp_client.stop()
            
    atexit.register(shutdown_tcp_client)

def forward_to_tcp(data_str):
    """Forward data using persistent TCP connection"""
    if not FORWARD_TCP or not tcp_client:
        return False
        
    # Queue the message for sending
    tcp_client.queue_message(data_str)
    return True

def handle_client_data(payload, client_ip, serverTime,session=None):
    """Handle client data and send it to Kafka"""
    try:
        entity = parse_payload(payload, client_ip, serverTime)
        print(f"entity: {entity}")
        if not entity:
            return
            
        deviceId = entity.get("deviceId")
        # Get route information for this vehicle
        fleet_info = get_fleet_info(deviceId)
        if fleet_info and 'route_id' in fleet_info and fleet_info["route_id"] != None:
            route_id = fleet_info['route_id']
            vehicle_lat = float(entity['lat'])
            vehicle_lon = float(entity['long'])
            
            # Use the timestamp from the entity instead of current_time
            entity_timestamp = datetime.fromtimestamp(entity['timestamp'])
            
            # Pass vehicle_id (deviceId) to track visited stops
            eta_data = stop_tracker.calculate_eta(
                route_id, 
                vehicle_lat, 
                vehicle_lon, 
                entity_timestamp,
                vehicle_id=deviceId
            )
            
            if eta_data:
                entity['closest_stop'] = eta_data['closest_stop']
                entity['distance_to_stop'] = eta_data['closest_stop']['distance']
                entity['eta_list'] = eta_data['eta']
                entity['calculation_method'] = eta_data['calculation_method']        
        # Try to send to Kafka with retries
        max_retries = 3
        retries = 0
        success = False
        
        while retries < max_retries and not success:
            try:
                # For confluent_kafka.Producer, we need to provide the data as a string
                producer.produce(KAFKA_TOPIC, json.dumps(entity).encode('utf-8'), callback=delivery_report)
                producer.poll(0)  # Trigger any callbacks
                success = True
            except BufferError as e:
                logger.error(f"Kafka buffer full, waiting before retry: {str(e)}")
                # Wait for buffer space to free up
                producer.poll(1)
                retries += 1
            except Exception as e:
                logger.error(f"Failed to send to Kafka (attempt {retries+1}): {str(e)}")
                retries += 1
                time.sleep(1)
        
        # Flush to ensure delivery        
        if success:
            try:
                producer.flush(timeout=5.0)
            except Exception as e:
                logger.error(f"Error flushing Kafka producer: {str(e)}")
        if FORWARD_TCP:
            forward_to_tcp(payload)
        # Store in Redis
        if fleet_info and 'route_id' in fleet_info and fleet_info["route_id"] != None:
            route_id = fleet_info['route_id']
            redis_key = f"route:{route_id}"
            
            # Get vehicle number
            vehicle_number = fleet_info.get('vehicle_no', deviceId)
            
            # Create vehicle data
            vehicle_data = json.dumps({
                "latitude": entity["lat"],
                "longitude": entity["long"],
                "timestamp": entity["timestamp"],
                "speed": entity.get("speed", 0),
                "device_id": deviceId,
                "route_id": route_id
            })
            
            # Add ETA data if available
            if 'eta_list' in entity:
                vehicle_data_obj = json.loads(vehicle_data)
                vehicle_data_obj['eta_data'] = entity['eta_list']
                vehicle_data = json.dumps(vehicle_data_obj)
            
            try:
                # Store vehicle data in hash
                redis_client.hset(redis_key, vehicle_number, vehicle_data)
                redis_client.expire(redis_key, 86400)  # Expire after 24 hours
                
                # Store location in Redis Geo set
                geo_key = "bus_locations"  # Single key for all bus locations
                if vehicle_lon is not None and vehicle_lat is not None and vehicle_number:
                    redis_client.geoadd(geo_key, vehicle_lon, vehicle_lat, vehicle_number)
                else:
                    logger.error(f"Invalid location data: lon={vehicle_lon}, lat={vehicle_lat}, member={vehicle_number}")
                redis_client.expire(geo_key, 86400)  # Expire after 24 hours
                
                logger.info(f"Success")
            except Exception as e:
                logger.error(f"Error storing data in Redis: {str(e)}")
    except Exception as e:
        logger.error(f"Error handling client data: {str(e)}")
        traceback.print_exc()

def handle_connection(conn, addr):
    """Handle a persistent client connection"""
    print(f"New connection from {addr}")
    
    # Set socket options for keep-alive if using Linux
    # These settings might not work on all platforms
    try:
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # The following options may not be available on all systems
        try:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)  # Start sending keepalive after 60 seconds
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)  # Send keepalive every 10 seconds
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)     # Drop connection after 5 failed keepalives
        except AttributeError:
            # These options might not be available on some systems
            pass
    except Exception as e:
        print(f"Warning: Could not set keep-alive options: {e}")
    
    # Set a generous timeout (5 minutes) 
    conn.settimeout(300)
    
    try:
        # Keep reading from the connection as long as it's open
        while True:
            try:
                data = conn.recv(4096)
                if not data:
                    # Client closed the connection
                    print(f"Client {addr} closed connection")
                    break
                
                # Respond to the client immediately
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                
                # Process the data
                data_decoded = data.decode(errors='ignore')
                
                # Clean up the data (remove any trailing characters like #)
                data_decoded = data_decoded.rstrip('#\r\n')
                
                # If data contains HTTP headers, extract just the payload
                if '\r\n\r\n' in data_decoded:
                    data_decoded = data_decoded.split('\r\n\r\n')[-1]
                
                serverTime = datetime.now()
                
                executor.submit(handle_client_data, data_decoded, addr, serverTime)
                # Use the thread pool instead of creating a new thread for each message
                # executor.submit(handle_client_data, data_decoded, addr, serverTime)
                
                # Reset the timeout after each successful read
                conn.settimeout(300)
                
            except socket.timeout:
                # Just log the timeout and continue - don't close the connection
                print(f"Connection from {addr} idle for 5 minutes, keeping open")
                conn.settimeout(300)  # Reset the timeout
                continue
                
            except ConnectionResetError:
                print(f"Connection reset by peer: {addr}")
                break
                
            except Exception as e:
                print(f"Error handling data from {addr}: {e}")
                break
    except Exception as e:
        print(f"Connection handler error for {addr}: {e}")
    finally:
        # Only close the connection if we've exited the loop
        try:
            conn.close()
            print(f"Connection from {addr} closed")
        except:
            pass

def periodic_flush():
    """Periodically flush the Kafka producer"""
    while True:
        try:
            time.sleep(5)  # Flush every 5 seconds
            producer.flush(timeout=1.0)
            print("Performed periodic Kafka flush")
        except Exception as e:
            print(f"Error during periodic flush: {e}")

# Start the Kafka flush thread
flush_thread = threading.Thread(target=periodic_flush, daemon=True)
flush_thread.start()

# Create a thread pool with a reasonable number of worker threads
MAX_WORKER_THREADS = int(os.getenv('MAX_WORKER_THREADS', '1000'))  # Default to 50 worker threads
logger.info(f"Initializing thread pool with {MAX_WORKER_THREADS} worker threads")
executor = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)

# Register a shutdown function to clean up the executor
def shutdown_executor():
    logger.info("Shutting down thread pool executor...")
    executor.shutdown(wait=False)
    logger.info("Thread pool executor shutdown complete")

atexit.register(shutdown_executor)

# We can also add monitoring for the thread pool
def monitor_thread_pool():
    """Monitor the thread pool and log its status"""
    while True:
        try:
            time.sleep(60)  # Check every minute
            # Get approximate queue size (only in Python 3.9+)
            try:
                queue_size = executor._work_queue.qsize()
                logger.info(f"Thread pool status: {len(executor._threads)} active threads, ~{queue_size} queued tasks")
            except (NotImplementedError, AttributeError):
                # If qsize() is not available
                logger.info(f"Thread pool status: {len(executor._threads)} active threads")
        except Exception as e:
            logger.error(f"Error monitoring thread pool: {e}")

# Start the thread pool monitor thread
monitor_thread = threading.Thread(target=monitor_thread_pool, daemon=True)
monitor_thread.start()

# Main server loop
def main_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Avoid "Address already in use" error
        server.bind((HOST, PORT))
        server.listen(100)  # Increase backlog for more pending connections
        
        print(f"Listening for connections on {HOST}:{PORT}...")
        
        # Track active connection threads
        connection_threads = []
        
        while True:
            try:
                # Accept new connection
                conn, addr = server.accept()
                
                # Start a new thread to handle this connection
                thread = threading.Thread(target=handle_connection, args=(conn, addr))
                thread.daemon = True  # Allow program to exit even if threads are running
                thread.start()
                
                # Keep track of the thread
                connection_threads.append((thread, addr))
                
                # Clean up completed connection threads
                connection_threads = [(t, a) for t, a in connection_threads if t.is_alive()]
                
                print(f"Active connections: {len(connection_threads)}")
                
            except Exception as e:
                print(f"Error accepting connection: {e}")
                time.sleep(1)  # Avoid tight loop if accept is failing

if __name__ == "__main__":
    main_server()

