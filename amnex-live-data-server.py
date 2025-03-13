import socket
from confluent_kafka import Producer
import os
import json
from datetime import datetime, date

HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 80        # Port (normally used for HTTPS, but this is plaintext)

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'default')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'test')

producer_config = {
            'bootstrap.servers': KAFKA_SERVER
            }

producer = Producer(producer_config)

def date_to_unix(d: date) -> int:
    return int(datetime.combine(d, datetime.min.time()).timestamp())

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
    return datetime.strptime(date_str, "%d/%m/%Y").date()


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

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
                if len(payload) >= 14 and payload[0] == "&PEIS" and payload[1] == "N" and payload[2] == "VTS" and payload[10] == 'A':
                    latitude = parse_coordinate(payload[11], payload[12], True)
                    longitude = parse_coordinate(payload[13], payload[14], False)
                    deviceId = payload[5]
                    timestamp = payload[8]
                    date = payload[9]
                    routeNumber = payload[17]
                    date = dd_mm_ss_to_date(date)
                    dataState = payload[3]
                    entity = {
                            "lat": latitude,
                            "long": longitude,
                            "deviceId": deviceId,
                            "timestamp": timestamp,
                            "dataState": dataState,
                            "routeNumber": routeNumber,
                            "date": date_to_unix(date)
                    }
                    producer.produce(KAFKA_TOPIC, key=deviceId, value=json.dumps(entity), callback=delivery_report)                                                
                    print(f"device id: {entity}")
                
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
