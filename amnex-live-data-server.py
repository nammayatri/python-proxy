import atexit
import json
import logging
import math
import os
import socket
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from typing import Optional, List

import paho.mqtt.client as mqtt
import redis
import requests
from confluent_kafka import Producer
from rediscluster import RedisCluster
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

# Import refactored modules from src package
from src.cache_utils import SimpleCache, get_vehicle_location_history
from src.fleet_management import (
    get_fleet_info, clean_redis_key_for_route_info, load_device_vehicle_mappings,
    clean_outdated_vehicle_mappings, start_vehicle_cleanup_thread
)
from src.models import Base, WaybillsBase
from src.stop_tracker import StopTracker

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
PROD_REDIS_NODES = os.getenv('PROD_REDIS_NODES', 'localhost:6379').split(',')
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
    prod_startup_nodes = [{"host": node.split(":")[0], "port": int(node.split(":")[1])} for node in PROD_REDIS_NODES]
    redis_client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)
    prod_redis_client = RedisCluster(startup_nodes=prod_startup_nodes, decode_responses=True, skip_full_coverage_check=True)
    print("✅ Connected to Redis Cluster")
else:
    # Redis Standalone setup (assume first node for standalone)
    STANDALONE_REDIS_DATABASE = int(os.getenv('STANDALONE_REDIS_DATABASE', '1'))
    host, port = REDIS_NODES[0].split(":")
    prodHost, prodPort = PROD_REDIS_NODES[0].split(":")
    redis_client = redis.StrictRedis(host=host, port=int(port), db=STANDALONE_REDIS_DATABASE, decode_responses=True)
    prod_redis_client = redis.StrictRedis(host=prodHost, port=int(prodPort), db=STANDALONE_REDIS_DATABASE, decode_responses=True)
    print(f"✅ Connected to Redis Standalone at {host}:{port} (DB={STANDALONE_REDIS_DATABASE})")

# Database configuration
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'gps_tracking')

# Waybills database configuration
WAYBILLS_DB_USER = os.getenv('WAYBILLS_DB_USER', 'postgres')
WAYBILLS_DB_PASS = os.getenv('WAYBILLS_DB_PASS', 'postgres')
WAYBILLS_DB_HOST = os.getenv('WAYBILLS_DB_HOST', 'localhost')
WAYBILLS_DB_PORT = os.getenv('WAYBILLS_DB_PORT', '5432')
WAYBILLS_DB_NAME = os.getenv('WAYBILLS_DB_NAME', 'waybills')
INTEGRATED_BPP_CONFIG_ID = os.getenv('INTEGRATED_BPP_CONFIG_ID_HD', 'b0454b15-9755-470d-a16a-71e87695e003')
MERCHANT_OPERATING_CITY_ID = os.getenv('MERCHANT_OPERATING_CITY_ID', 'fc87c15e-29aa-492b-835f-bda8ff00c840')
GTFS_ID = os.getenv('GTFS_ID', 'chennai_data')
ROUTE_STOP_MAPPING_API_URL = os.getenv('ROUTE_STOP_MAPPING_API_URL', 'http://gtfs-inmemory-data-server.nandi.svc.cluster.local:8000')

# SQLAlchemy setup for main database
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

# SQLAlchemy setup for waybills database
WAYBILLS_DATABASE_URL = f"postgresql://{WAYBILLS_DB_USER}:{WAYBILLS_DB_PASS}@{WAYBILLS_DB_HOST}:{WAYBILLS_DB_PORT}/{WAYBILLS_DB_NAME}"
print(f"WAYBILLS_DATABASE_URL: {WAYBILLS_DATABASE_URL}")
waybills_engine = create_engine(
    WAYBILLS_DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800
)
WaybillsSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=waybills_engine)

# Models are now imported from models.py

# get_route_ids_from_waybills function moved to route_matching.py

# Don't create tables since we're using existing tables
# Base.metadata.create_all(bind=engine)
# WaybillsBase.metadata.create_all(bind=waybills_engine)

# Environment variables for route data configuration
USE_OSRM = os.getenv('USE_OSRM', 'true').lower() == 'true'
OSRM_URL = os.getenv('OSRM_URL', 'http://router.project-osrm.org')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', '')
ROUTE_CACHE_TTL = int(os.getenv('ROUTE_CACHE_TTL', '3600'))  # 1 hour default
BUS_LOCATION_MAX_AGE = int(os.getenv('BUS_LOCATION_MAX_AGE', '120'))  # 2 minutes default
BUS_CLEANUP_INTERVAL = int(os.getenv('BUS_CLEANUP_INTERVAL', '180'))  # 3 minute default
CLEANUP_LOCK_TTL = 30  # 30 seconds lock TTL to prevent multiple cleanups
ENABLE_TIMESTAMP_VALIDATION = os.getenv('ENABLE_TIMESTAMP_VALIDATION', 'false').lower() == 'true'  # Feature flag for timestamp validation
FUTURE_TIMESTAMP_TOLERANCE = int(os.getenv('FUTURE_TIMESTAMP_TOLERANCE', '300'))  # 5 minutes tolerance for future timestamps

# StopTracker class moved to stop_tracker.py


# Create instance with updated parameters
stop_tracker = StopTracker(
    db_engine=engine, 
    redis_client=redis_client, 
    use_osrm=USE_OSRM,
    osrm_url=OSRM_URL, 
    google_api_key=GOOGLE_API_KEY, 
    cache_ttl=ROUTE_CACHE_TTL,
    route_stop_mapping_api_url=ROUTE_STOP_MAPPING_API_URL,
    gtfs_id=GTFS_ID,
    merchant_operating_city_id=MERCHANT_OPERATING_CITY_ID
)

# Create single cache instance
cache = SimpleCache(redis_client)

# FleetInfo model moved to models.py

# get_fleet_info function moved to fleet_management.py

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
            ign_status = payload[6]
            timestamp = payload[8]
            date = payload[9]
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
                "pushedToKafkaAt": date_to_unix(datetime.now()),
                "serverTime": date_to_unix(serverTime),
                "raw": raw,
                "provider": "amnex",
                "ign_status": ign_status,
                "client_ip": client_ip
            }
            return entity
        return None
    except Exception as e:
        print(f"Error parsing Amnex payload: {e}")
        return None

def parse_mqtt_payload(data_str, serverTime, client_ip):
    """Parse MQTT GPS data format"""
    # Payload format: "data,<device_id>,<lat>,<long>,<speed_from_gps>,<signal_quality>,<busname>"
    try:
        parts = data_str.split(',')
        
        if len(parts) != 7 or parts[0] != "data":
            raise Exception(f"Unknown format of payload {data_str}")
            
        deviceId = parts[1].replace("CD", "")
        lat = float(parts[2])
        lon = float(parts[3])
        speed = float(parts[4])
        signalQuality = parts[5]
        busName = parts[6]
        
        entity = {
            "lat": lat,
            "long": lon,
            "deviceId": deviceId,
            "version": None,
            "timestamp": date_to_unix(serverTime),
            "vehicleNumber": busName,
            "speed": speed,
            "pushedToKafkaAt": int(time.time()),
            "dataState": "L",  # Live data
            "serverTime": date_to_unix(serverTime),
            "provider": "nammayatri-gps-devices",
            "raw": data_str,
            "client_ip": client_ip,
            "routeNumber": None,
            "signalQuality": signalQuality
        }
        
        return entity
    except Exception as e:
        print(f"Error parsing MQTT payload: {e} for payload: {data_str}")
        return None

def parse_payload(data_decoded, client_ip, serverTime, isNYGpsDevice):
    """Parse payload data by determining the format"""
    try:
        # First check if it's NY GPS device mqtt server data
        if isNYGpsDevice:
            return parse_mqtt_payload(data_decoded, serverTime, client_ip)
        
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
            self._message_queue.append(message)
            
    def _process_message_queue(self):
        """Process queued messages in background"""
        while not self._stop_event.is_set():
            messages_to_send = []
            
            # Get all queued messages
            with self._queue_lock:
                if self._message_queue:
                    messages_to_send = self._message_queue.copy()
                    self._message_queue.clear()

                    
            # Send all queued messages
            if messages_to_send and self.connected:
                for message in messages_to_send:
                    self._send_message(message)
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

# Geometry utilities and vehicle history functions moved to geometry_utils.py and cache_utils.py

# clean_redis_key_for_route_info function moved to fleet_management.py

# Vehicle cleanup functions moved to fleet_management.py

# calculate_route_match_score function moved to route_matching.py

def push_to_kafka(entity):
    max_retries = 3
    retries = 0
    success = False

    while retries < max_retries and not success:
        try:
            # Use a specific key (e.g., deviceId) to select partition
            kafka_key = str(entity['deviceId']) if entity.get('deviceId') is not None else None

            producer.produce(
                KAFKA_TOPIC,
                json.dumps(entity).encode('utf-8'),
                key=kafka_key.encode('utf-8') if kafka_key else None,
                callback=delivery_report
            )
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

WHITELISTED_NY_GPS_DEVICE_IDS_CACHE_KEY = "gps-server:whitelisted_ny_gps_device_ids"
def get_whitelisted_ny_gps_deviceIds():
    return ["3087764365", "3087855693", "3087813297", "3087744289", "3087810997", "1428387553", "301219289"]
    deviceIds = redis_client.lrange(WHITELISTED_NY_GPS_DEVICE_IDS_CACHE_KEY, 0, -1)
    if type(deviceIds) != list[str]:
        return []
    print("deviceIds white", deviceIds)    
    return deviceIds

BLACKLISTED_AMNEX_DEVICE_IDS_CACHE_KEY = "gps-server:blacklisted_amnex_device_ids"
def get_blacklisted_amnex_deviceIds():
    return ["866041042256377", "865860041737124", "865501043873992", "864337056440093", "866758048846386", "861107033954232", "862607055624299"]
    deviceIds = redis_client.lrange(BLACKLISTED_AMNEX_DEVICE_IDS_CACHE_KEY, 0, -1)
    if type(deviceIds) != list[str]:
        return []
    print("deviceIds black", deviceIds)
    return deviceIds

def validate_and_update_timestamp(entity: dict, vehicle_number: str) -> bool:
    """
    Check if the entity's timestamp is valid and update Redis with the latest timestamp.
    
    Args:
        entity: The parsed GPS entity
        vehicle_number: The vehicle number
    
    Returns:
        True if timestamp is valid and should proceed, False if outdated or from future
    """
    if not ENABLE_TIMESTAMP_VALIDATION:
        return True
    
    try:
        entity_timestamp = entity.get('timestamp')
        if not entity_timestamp:
            return True  # Allow if no timestamp
        
        entity_timestamp = int(entity_timestamp)
        current_timestamp = int(time.time())
        
        # Check if timestamp is from the future (allow tolerance for clock skew)
        if entity_timestamp > current_timestamp + FUTURE_TIMESTAMP_TOLERANCE:
            logger.info(f"Future timestamp detected for vehicle {vehicle_number}: entity={entity_timestamp}, current={current_timestamp}, diff={entity_timestamp - current_timestamp}s")
            return False
        
        # Use dedicated key for vehicle timestamps
        timestamp_key = f"vehicle_timestamp:{vehicle_number}"
        stored_timestamp = redis_client.get(timestamp_key)
        
        if stored_timestamp is None:
            # No previous timestamp found, store current and allow
            redis_client.setex(timestamp_key, 86400, str(entity_timestamp))  # 24 hour TTL
            return True
        
        stored_timestamp = int(stored_timestamp)
        
        # Compare timestamps
        if entity_timestamp >= stored_timestamp:
            # Entity timestamp is newer or equal, update Redis and allow
            redis_client.setex(timestamp_key, 86400, str(entity_timestamp))  # 24 hour TTL
            return True
        else:
            # Entity timestamp is older, don't allow processing
            logger.info(f"Outdated data detected for vehicle {vehicle_number}: entity={entity_timestamp}, stored={stored_timestamp}")
            return False
        
    except Exception as e:
        logger.error(f"Error validating timestamp for vehicle {vehicle_number}: {e}")
        return True  # Allow on error to avoid blocking valid data

# load_device_vehicle_mappings function moved to fleet_management.py

def handle_client_data(payload, client_ip, serverTime, isNYGpsDevice = False, session=None):
    """Handle client data and send it to Kafka"""
    try:
         # Try to send to Kafka with retries
        entity = parse_payload(payload, client_ip, serverTime, isNYGpsDevice)

        if not entity:
            return

        if FORWARD_TCP and not isNYGpsDevice:
            forward_to_tcp(payload)
        
        deviceId = entity.get("deviceId")

        if isNYGpsDevice:
            push_to_kafka(entity)
            ny_whitelisted_device_ids = get_whitelisted_ny_gps_deviceIds()
            if deviceId not in ny_whitelisted_device_ids:
                return

        # Check for timestamp validation
        vehicle_number = None
        is_timestamp_valid = True
        
        if not isNYGpsDevice and deviceId in device_vehicle_map:
            vehicle_number = device_vehicle_map[deviceId]
            is_timestamp_valid = validate_and_update_timestamp(entity, vehicle_number)
        
        if not isNYGpsDevice and ('dataState' not in entity or entity.get('dataState') not in ['L', 'LP', 'LO'] or deviceId not in device_vehicle_map or not is_timestamp_valid):
            push_to_kafka(entity)
            if not is_timestamp_valid:
                print(f"Skipping invalid timestamp data for vehicle {vehicle_number}, device {deviceId}")
            else:
                print(f"Skipping non-live data or unknown device {deviceId}")
            return

        vehicle_lat = float(entity['lat'])
        vehicle_lon = float(entity['long'])
            
        if entity.get('provider') == 'amnex':
            amnex_blacklisted_device_ids = get_blacklisted_amnex_deviceIds()
            if deviceId in amnex_blacklisted_device_ids:
                push_to_kafka(entity)
                logger.info(f"Skipping blacklisted amnex device: {deviceId}")
                return
        
        # Get route information for this vehicle
        fleet_infos = get_fleet_info(redis_client, device_vehicle_map, WaybillsSessionLocal, deviceId, vehicle_lat, vehicle_lon, entity.get('timestamp'), entity.get('provider'), stop_tracker, BUS_LOCATION_MAX_AGE, BUS_CLEANUP_INTERVAL)
        if not fleet_infos:
            push_to_kafka(entity)
        for fleet_info in fleet_infos:
            entity['routeNumber'] = fleet_info.route_id
            if fleet_info and fleet_info.route_id is not None:
                route_id = fleet_info.route_id
                
                stopsInfo = stop_tracker.get_route_stops(route_id)
                
                # Pass vehicle_id (deviceId) to track visited stops
                if deviceId:
                    visited_stops = stop_tracker.get_visited_stops(route_id, deviceId)
                else:
                    visited_stops = []
                before_curr_point_visited_stops = [x for x in visited_stops]
                eta_data = stop_tracker.calculate_eta(
                    stopsInfo,
                    route_id, 
                    vehicle_lat, 
                    vehicle_lon, 
                    serverTime,
                    vehicle_id=deviceId,
                    visited_stops=visited_stops,
                    vehicle_no=fleet_info.vehicle_no
                )
                if len(visited_stops) > len(before_curr_point_visited_stops):
                    entity['stopId'] = visited_stops[-1]
                push_to_kafka(entity)
                
                if eta_data:
                    entity['closest_stop'] = eta_data['closest_stop']
                    entity['distance_to_stop'] = eta_data['closest_stop']['distance']
                    entity['eta_list'] = eta_data['eta']
                    entity['calculation_method'] = eta_data['calculation_method']
                    entity['visited_stops'] = visited_stops
            else: 
                push_to_kafka(entity)
            # Store in Redis
            if fleet_info and 'route_id' in fleet_info and fleet_info["route_id"] != None:
                route_id = fleet_info['route_id']
                redis_key = f"route:{route_id}"
                
                # Get vehicle number
                vehicle_number = fleet_info.get('vehicle_no', deviceId)
                
                # Create vehicle data
                vehicle_data_obj = {
                    "latitude": entity["lat"],
                    "longitude": entity["long"],
                    "timestamp": entity["timestamp"],
                    "speed": entity.get("speed", 0),
                    "device_id": deviceId,
                    "vehicle_number": vehicle_number,
                    "route_id": str(route_id),
                    "serverTime": int(time.time())  # Add current server time
                }

                min_vehicle_data = json.dumps(vehicle_data_obj)
                
                
                # Add ETA data if available
                if 'eta_list' in entity:
                    vehicle_data_obj['eta_data'] = entity['eta_list']
                    vehicle_data_obj['visited_stops'] = entity['visited_stops']
                vehicle_data = json.dumps(vehicle_data_obj)
                
                try:
                    # Store vehicle data in hash
                    logger.info(f"Route ID: Bus vehicle {vehicle_number} is on route, {route_id}")
                    prod_redis_client.hset(redis_key, vehicle_number, vehicle_data)
                    prod_redis_client.expire(redis_key, 86400)  # Expire after 24 hours
                    redis_client.hset(redis_key, vehicle_number, vehicle_data)
                    redis_client.expire(redis_key, 86400)  # Expire after 24 hours
                    
                    geo_key = "bus_locations"
                    vehicle_meta_key = "bus_metadata"
                    if vehicle_lon is not None and vehicle_lat is not None and vehicle_number:
                        prod_redis_client.geoadd(geo_key, vehicle_lon, vehicle_lat, vehicle_number)
                        prod_redis_client.hset(vehicle_meta_key, vehicle_number, min_vehicle_data)
                        redis_client.geoadd(geo_key, vehicle_lon, vehicle_lat, vehicle_number)
                        redis_client.hset(vehicle_meta_key, vehicle_number, min_vehicle_data)
                    else:
                        logger.error(f"Invalid location data: lon={vehicle_lon}, lat={vehicle_lat}, member={vehicle_number}")
                    prod_redis_client.expire(geo_key, 86400)
                    prod_redis_client.expire(vehicle_meta_key, 86400)
                    redis_client.expire(geo_key, 86400)
                    redis_client.expire(vehicle_meta_key, 86400)
                    
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
            except (NotImplementedError, AttributeError):
                # If qsize() is not available
                pass
        except Exception as e:
            logger.error(f"Error monitoring thread pool: {e}")

# Start the thread pool monitor thread
monitor_thread = threading.Thread(target=monitor_thread_pool, daemon=True)
monitor_thread.start()

MQTT_HOST = os.getenv('MQTT_HOST', 'localhost')
MQTT_PORT = os.getenv('MQTT_PORT', '1883')
MQTT_USER = os.getenv('MQTT_USER', 'user123')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', 'abc123')
# In MQTTv5, we can use $share to share a topic between multiple clients 
# so that one message is consumed by one client in a group (It is load balanced automatically by the broker)
MQTT_TOPIC = '$share/prod-gps-server/' + os.getenv('MQTT_TOPIC', 'gps-data')
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'local-gps-fetch-server') # Pod name in Production

def mqtt_client():
    """MQTT client to consume GPS data and forward to Kafka"""
    def on_connect(client, _userdata, _flags, rc, _properties):
        if rc == 0:
            logger.info("✅ Connected to MQTT broker")
            client.subscribe(MQTT_TOPIC)
        else:
            logger.error(f"❌ Failed to connect to MQTT broker with code {rc}")
    
    def on_message(_client, _userdata, msg):
        try:
            # Parse the message payload
            payload = msg.payload.decode('utf-8')
            
            # Use the existing handle_client_data function
            serverTime = datetime.now()
            executor.submit(handle_client_data, payload, None, serverTime, True)
            
        except Exception as e:
            logger.error(f"❌ Error processing MQTT message: {str(e)}")
            traceback.print_exc()
    
    # Create MQTT client
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, protocol=mqtt.MQTTv5)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_HOST, int(MQTT_PORT), 60)
        client.loop_start()
        logger.info(f"✅ MQTT client started and connected to {MQTT_HOST}:{MQTT_PORT}")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to start MQTT client: {str(e)}")
        return None

mqtt_client_obj = None

# Register shutdown function for MQTT client
def shutdown_mqtt_client():
    if mqtt_client_obj and mqtt_client_obj.is_connected():
        mqtt_client_obj.disconnect()
        logger.info("✅ MQTT client disconnected")
        time.sleep(0.5)

atexit.register(shutdown_mqtt_client)

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
                
            except Exception as e:
                print(f"Error accepting connection: {e}")
                time.sleep(1)  # Avoid tight loop if accept is failing

device_vehicle_map = {}

if __name__ == "__main__":
    # Start MQTT client, no separate thread required 
    # as we already called loop_start() and we already registered a shutdown function
    mqtt_client_obj = mqtt_client()
    device_vehicle_map = load_device_vehicle_mappings(SessionLocal)
    start_vehicle_cleanup_thread(redis_client, prod_redis_client, CLEANUP_LOCK_TTL, BUS_CLEANUP_INTERVAL, BUS_LOCATION_MAX_AGE)
    main_server()