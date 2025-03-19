import socket
from confluent_kafka import Producer
import os
import json
from datetime import datetime, date

HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 443        # Port 443 (normally used for HTTPS, but this is plaintext)

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amnex_direct_live')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'eks:9096')

producer_config = {
            'bootstrap.servers': KAFKA_SERVER
            }

producer = Producer(producer_config)

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
    $Header,iTriangle1,1_36T02B0164MAIS_6,NR,16,L,869244044490666,KA01G1234,1,18032025,200705,13.090713,N,80.291023,E,0.0,0,9,-19.0,2.00,0.90,CellOne,1,1,25.5,4.3,0,C,25,404,64,090F,6A92,29,72d9,090f,21,6a4c,090f,20,6a60,090f,17,6a9c,090f,0101,01,383149,0.000,0.000,15,()*7C
    """
    if payload[0] != "$Header":
        return None
        
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

# Example Usage
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Avoid "Address already in use" error
    server.bind((HOST, PORT))
    server.listen(5)

    print(f"Listening for connections on {HOST}:{PORT}...")

    while True:
        conn, addr = server.accept()
        with conn:
            data = conn.recv(4096)  # Adjust buffer size as needed
            if data:
                decodedData = data.decode(errors='ignore')
                payload = decodedData.split(",")
                print(f"data fromAddress: {addr} -> {decodedData}")
                
                entity = None
                
                # Try to parse as Chalo format
                if len(payload) > 0 and payload[0] == "$Header":
                    entity = parse_chalo_payload(payload)
                # Try to parse as Amnex format
                elif len(payload) >= 14 and payload[0] == "&PEIS":
                    entity = parse_amnex_payload(payload)
                
                if entity:
                    deviceId = entity["deviceId"]
                    producer.produce(KAFKA_TOPIC, key=deviceId, value=json.dumps(entity), callback=delivery_report)
                    print(f"device id: {entity}")
                
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
