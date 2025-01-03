from datetime import datetime
import json

# Sample input entity data
input_data = {
    "entity": [
        {
            "id": "K0134LF",
            "vehicle": {
                "trip": {
                    "tripId": "-O8c4nV_HIFaac6fK4zk",
                    "startTime": "23:16:45",
                    "startDate": "20241007",
                    "scheduleRelationship": "SCHEDULED",
                    "routeId": "TCzBOEDh"
                },
                "position": {
                    "latitude": 13.073745,
                    "longitude": 80.199974,
                    "speed": 4.3744693
                },
                "timestamp": "1728323243",
                "vehicle": {
                    "id": "K0134LF"
                }
            }
        },
        {
            "id": "K0134LA",
            "vehicle": {
                "trip": {
                    "tripId": "-O8c4nV_HIFaac6fK4zk",
                    "startTime": "23:16:45",
                    "startDate": "20241007",
                    "scheduleRelationship": "SCHEDULED",
                    "routeId": "TCzBOEDh"
                },
                "position": {
                    "latitude": 13.073745,
                    "longitude": 80.199974,
                    "speed": 4.3744693
                },
                "timestamp": "1728323243",
                "vehicle": {
                    "id": "K0134LA"
                }
            }
        }
    ]
}

# Hyderabad API sample input data
input_data_hyderabad = {
    "data": [
        {
            "id": 993774,
            "active": False,
            "status": 5,
            "vehicleBattery": 0.0,
            "location": {
                "gpsTime": 1672687854,
                "gprsTime": 1672687940,
                "latitude": 17.4470633,
                "longitude": 78.4983483,
                "altitude": 524.0,
                "heading": 3.0,
                "speedKph": 0.0,
                "address": "Shriraghava Garden Road",
                "odometer": 52904.582,
                "gpsSignal": 32
            },
            "deviceDetails": {
                "registrationNumber": "TG10T1559"
            },
            "canInfo": {
                "engineRPM": 0,
                "chargingStatus": 0
            },
            "alerts": {
                "deviceId": 993774,
                "timestamp": 1672686953
            }
        }
    ]
}

# Function to convert timestamp to UTC time
# def convert_timestamp_to_utc(timestamp):
#     # Convert UNIX timestamp to UTC time in ISO format
#     return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ')

def convert_timestamp_to_utc(timestamp):
    try:
        return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            print(f"Unknown timestamp format: {timestamp}")
            return None

# POSIX timestamp: 1700000000 → 2024-11-13T12:30:00Z
# ISO 8601 timestamp: 2024-11-13T14:25:36Z → 2024-11-13T14:25:36Z

# Transform the entity data
def transform_entity(input_entity, api_type="bangalore"):
    transformed_entities = []
    for entity in input_entity['entity']:
        vehicle_info = entity.get('vehicle', {})
        id_ = entity.get('id', None)
        trip_info = vehicle_info.get('trip', {})
        position_info = vehicle_info.get('position', {})
        timestamp = convert_timestamp_to_utc(vehicle_info.get('timestamp', None))

        transformed_entity = {
            "vehicleid": vehicle_info.get('vehicle', {}).get('id', id_),  
            "routeId": trip_info.get('routeId', None),
            "startTime": trip_info.get('startTime', None),
            "startDate": trip_info.get('startDate', None),
            "scheduleRelationship": trip_info.get('scheduleRelationship', None),
            "tripId": trip_info.get('tripId', None),
            "latitude": position_info.get('latitude', None),
            "longitude": position_info.get('longitude', None),
            "speed": position_info.get('speed', None),
            "timestamp": timestamp
        }

    return {"entity": transformed_entities}

def transform_hyderabad_entity(hyderabad_data):
    transformed_entities = []
    for item in hyderabad_data.get('data', []):
        location_info = item.get('location', {})
        device_details = item.get('deviceDetails', {})

        transformed_entity = {
            "id": item['id'],
            "vehicleNum": device_details.get("registrationNumber", None),
            "timestamp": convert_timestamp_to_utc(location_info.get("gpsTime")),
            "latitude": location_info.get("latitude", None),
            "longitude": location_info.get("longitude", None),
            "speed": location_info.get("speedKph", None)
        }
        transformed_entities.append(transformed_entity)
    return {"entity": transformed_entities}

def transform_amx_entity(amx_data):
    transformed_entities = []
    for item in amx_data.get('Data', []):
        transformed_entity = {
            "vehicleid": item.get("fleet_no"),
            "vehicleNum": item.get("vehicle_reg_no", None),
            "routeId": None,
            "startTime": None,
            "startDate": None,
            "scheduleRelationship": None,
            "tripId": item.get("trip_no"),
            "latitude": item.get("lat"),
            "longitude": item.get("lon"),
            "speed": item.get("speed"),
            "timestamp": item.get("gps_timestamp")
        }
        transformed_entities.append(transformed_entity)

    return {"entity": transformed_entities}

