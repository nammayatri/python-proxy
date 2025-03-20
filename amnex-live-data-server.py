import socket
from confluent_kafka import Producer
import os
import json
from datetime import datetime, date
import threading
from rediscluster import RedisCluster
import redis

HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 443        # Port 443 (normally used for HTTPS, but this is plaintext)

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amnex_direct_live')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'eks:9096')

# Redis connection setup
REDIS_NODES = os.getenv('REDIS_NODES', 'localhost:7000').split(',')
IS_CLUSTER_REDIS = os.getenv('IS_CLUSTER_REDIS', 'true').lower() == 'true'

# Setup Kafka producer
producer_config = {
            'bootstrap.servers': KAFKA_SERVER
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
    STANDALONE_REDIS_DATABASE = int(os.getenv('STANDALONE_REDIS_DATABASE', '0'))
    host, port = REDIS_NODES[0].split(":")
    redis_client = redis.StrictRedis(host=host, port=int(port), db=STANDALONE_REDIS_DATABASE, decode_responses=True)
    print(f"✅ Connected to Redis Standalone at {host}:{port} (DB={STANDALONE_REDIS_DATABASE})")

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

def parse_chalo_payload(payload):
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
            "provider": "chalo"
        }

        print(f"chalo entity: {entity}")
        
        return entity
    except Exception as e:
        print(f"Error parsing Chalo payload: {e}")
        return None

def parse_amnex_payload(payload):
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
                "provider": "amnex"
            }
            return entity
        return None
    except Exception as e:
        print(f"Error parsing Amnex payload: {e}")
        return None

def handle_client_data(addr, data):
    try:
        decodedData = data.decode(errors='ignore')
        payload = decodedData.split(",")
        print(f"data fromAddress: {addr} -> {decodedData}")
        
        entity = None
        
        # Try to parse as Chalo format
        if len(payload) > 0 and payload[0].endswith("$Header"):
            print(f"chalo payload: {payload}")
            entity = parse_chalo_payload(payload)
        # Try to parse as Amnex format
        elif len(payload) >= 14 and payload[0] == "&PEIS":
            entity = parse_amnex_payload(payload)
        
        if entity:
            deviceId = entity["deviceId"]
            
            # Send to Kafka
            producer.produce(KAFKA_TOPIC, key=deviceId, value=json.dumps(entity), callback=delivery_report)
            
            # Store in Redis similar to push-to-kafka.py
            try:
                # Extract route or trip ID
                routeId = entity.get("routeNumber", None)
                
                if routeId:
                    # Format data for Redis
                    reqData = {
                        "latitude": entity["lat"],
                        "longitude": entity["long"],
                        "tripId": deviceId,  # Using deviceId as tripId since we don't have specific tripId
                        "speed": str(entity.get("speed", "0.0"))
                    }
                    
                    # Store in Redis
                    redis_client.hset(f"route:{routeId}", mapping={deviceId: json.dumps(reqData)})
                    print(f"Stored in Redis with route:{routeId}, device:{deviceId}")
                else:
                    print("Not storing in Redis: No routeId found")
            except Exception as e:
                print(f"Redis storage error: {e}")
            
            print(f"device id: {entity}")
    except Exception as e:
        print(f"An error occurred while processing data from {addr}: {e}")

# Example Usage
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Avoid "Address already in use" error
    server.bind((HOST, PORT))
    server.listen(5)

    print(f"Listening for connections on {HOST}:{PORT}...")

    while True:
        conn, addr = server.accept()
        with conn:
            try:
                data = conn.recv(4096)  # Adjust buffer size as needed
                if data:
                    # Respond to the client immediately
                    conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                    
                    # Process the data in a separate thread
                    threading.Thread(target=handle_client_data, args=(addr, data)).start()
            except ConnectionResetError:
                print(f"Connection reset by peer: {addr}")
            except Exception as e:
                print(f"An error occurred while receiving data from {addr}: {e}")
