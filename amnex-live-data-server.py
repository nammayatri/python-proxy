import socket
from confluent_kafka import Producer, KafkaError, KafkaException
import os
import json
from datetime import datetime, date
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

HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 443        # Port 443 (normally used for HTTPS, but this is plaintext)

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amnex_direct_live')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9096')

# Redis connection setup
REDIS_NODES = os.getenv('REDIS_NODES', 'localhost:6379').split(',')
IS_CLUSTER_REDIS = os.getenv('IS_CLUSTER_REDIS', 'false').lower() == 'true'

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
class FleetDeviceMapping(Base):
    __tablename__ = "fleet_device_mapping"  
    id = Column(Integer, primary_key=True)
    fleet_no = Column(Text, index=True)
    device_id = Column(Text, index=True)

class FleetRouteMapping(Base):
    __tablename__ = "fleet_route_mapping"  
    id = Column(Integer, primary_key=True)
    fleet_no = Column(Text, index=True)
    route_id = Column(Text, index=True)

# Don't create tables since we're using existing table
# Base.metadata.create_all(bind=engine)

class SimpleCache:
    def __init__(self):
        self.cache = {}

    def get(self, key: str):
        return self.cache.get(key)

    def set(self, key: str, value):
        self.cache[key] = value

# Create single cache instance
cache = SimpleCache()

def get_route_for_device(device_id: str) -> Optional[str]:
    """Get the active route ID for a device with caching"""
    cache_key = f"device:{device_id}"
    
    # Check cache first
    cached_route = cache.get(cache_key)
    if cached_route is not None:
        return cached_route

    try:
        with SessionLocal() as db:
            # First get the fleet number for the device
            fleet_mapping = db.query(FleetDeviceMapping)\
                .filter(FleetDeviceMapping.device_id == device_id)\
                .first()
            
            if not fleet_mapping:
                cache.set(cache_key, None)
                return None

            # Then get the route for that fleet
            route_mapping = db.query(FleetRouteMapping)\
                .filter(FleetRouteMapping.fleet_no == fleet_mapping.fleet_no)\
                .first()
            
            route_id = route_mapping.route_id if route_mapping else None
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
            fleet_numbers = db.query(FleetRouteMapping.fleet_no)\
                .filter(FleetRouteMapping.route_id == route_id)\
                .all()
            
            if not fleet_numbers:
                cache.set(cache_key, [])
                return []

            # Then get all devices for these fleet numbers
            fleet_nos = [f[0] for f in fleet_numbers]  # Extract fleet numbers from result tuples
            devices = db.query(FleetDeviceMapping.device_id)\
                .filter(FleetDeviceMapping.fleet_no.in_(fleet_nos))\
                .all()
            
            device_list = [d[0] for d in devices]  # Extract device IDs from result tuples
            cache.set(cache_key, device_list)
            return device_list

    except Exception as e:
        print(f"Error querying devices for route {route_id}: {e}")
        return []

def get_fleet_info(device_id: str) -> dict:
    """Get both fleet number and route ID for a device"""
    try:
        with SessionLocal() as db:
            # Get fleet number for device
            fleet_mapping = db.query(FleetDeviceMapping)\
                .filter(FleetDeviceMapping.device_id == device_id)\
                .first()
            
            if not fleet_mapping:
                return {}

            # Get route for fleet
            route_mapping = db.query(FleetRouteMapping)\
                .filter(FleetRouteMapping.fleet_no == fleet_mapping.fleet_no)\
                .first()
            
            return {
                'fleet_no': fleet_mapping.fleet_no,
                'route_id': route_mapping.route_id if route_mapping else None
            }

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

def parse_chalo_payload(payload, serverTime):
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
            "timestamp": date_to_unix(timestamp),
            "vehicleNumber": vehicleNumber,
            "speed": speed,
            "dataState": dataState,
            "serverTime": date_to_unix(serverTime),
            "provider": "chalo"
        }

        print(f"chalo entity: {entity}")
        
        return entity
    except Exception as e:
        print(f"Error parsing Chalo payload: {e}")
        return None

def parse_amnex_payload(payload, serverTime):
    """Parse the payload from Amnex format."""
    try:
        if len(payload) >= 14 and payload[0] == "&PEIS" and payload[1] == "N" and payload[2] == "VTS" and payload[10] == 'A':
            latitude = parse_coordinate(payload[11], payload[12], True)
            longitude = parse_coordinate(payload[13], payload[14], False)
            deviceId = payload[5]
            timestamp = payload[8]
            date = payload[9]
            routeNumber = payload[17]
            date = dd_mm_ss_to_date(date + "-" + timestamp)
            dataState = payload[3]
            entity = {
                "lat": latitude,
                "long": longitude,
                "deviceId": deviceId,
                "timestamp": date_to_unix(date),
                "dataState": dataState,
                "routeNumber": routeNumber,
                "serverTime": date_to_unix(serverTime),
                "provider": "amnex"
            }
            return entity
        return None
    except Exception as e:
        print(f"Error parsing Amnex payload: {e}")
        return None

def handle_client_data(data_decoded, serverTime, addr):
    try:
        payload = data_decoded.split(",")
        print(f"data fromAddress: {addr} -> {data_decoded}")
        
        entity = None
        
        # Try to parse as Chalo format
        if len(payload) > 0 and payload[0].endswith("$Header"):
            print(f"chalo payload: {payload}")
            entity = parse_chalo_payload(payload, serverTime)
        # Try to parse as Amnex format
        elif len(payload) >= 14 and payload[0] == "&PEIS":
            entity = parse_amnex_payload(payload, serverTime)
        
        if entity:
            deviceId = entity["deviceId"]
            
            # Get route_id from database
            fleet_info = get_fleet_info(deviceId)
            route_id = fleet_info.get("route_id")
            vehicle_number = fleet_info.get("fleet_no")
            if route_id:
                entity["routeNumber"] = route_id
            
            # Send to Kafka with improved error handling and flushing logic
            kafka_sent = False
            max_retries = 3
            retry_count = 0
            
            while not kafka_sent and retry_count < max_retries:
                try:
                    producer.produce(KAFKA_TOPIC, key=deviceId, value=json.dumps(entity), callback=delivery_report)
                    producer.poll(0)
                    kafka_sent = True
                except BufferError:
                    print(f"Kafka queue full, flushing producer - {addr} (retry {retry_count+1}/{max_retries})")
                    producer.flush(timeout=2.0)
                    retry_count += 1
                    if retry_count == max_retries - 1:
                        print(f"Performing extended poll to clear queue - {addr}")
                        producer.poll(5.0)
                except Exception as kafka_err:
                    print(f"Error producing to Kafka: {kafka_err}")
                    retry_count = max_retries
            
            if not kafka_sent:
                print(f"Failed to send message to Kafka after {max_retries} retries - {addr}")
            
            # Store in Redis if we have a route_id
            if route_id:
                try:
                    reqData = {
                        "latitude": entity["lat"],
                        "longitude": entity["long"],
                        "timestamp": entity["timestamp"],
                        "speed": str(entity.get("speed", "0.0"))
                    }
                    redis_client.hset(f"route:{route_id}", mapping={vehicle_number: json.dumps(reqData)})
                    print(f"Stored in Redis with route:{route_id}, device:{deviceId}, vehicle_number:{vehicle_number}")
                except Exception as e:
                    print(f"Redis storage error: {e}")
            else:
                print(f"No active route mapping found for device: {deviceId}")
            
            print(f"device id: {entity}")
    except Exception as e:
        print(f"An error occurred while processing data from {addr}: {e}")

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
                
                # Process in a separate thread to avoid blocking
                threading.Thread(
                    target=handle_client_data,
                    args=(data_decoded, serverTime, addr)
                ).start()
                
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

