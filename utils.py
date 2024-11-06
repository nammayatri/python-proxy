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

# Function to convert timestamp to UTC time
def convert_timestamp_to_utc(timestamp):
    # Convert UNIX timestamp to UTC time in ISO format
    return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%dT%H:%M:%SZ')

# Transform the entity data
def transform_entity(input_entity):
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
        transformed_entities.append(transformed_entity)
    
    return {"entity": transformed_entities}

# Transform the entity data
# def transform_entity(input_entity):
#     transformed_entities = []
#     for entity in input_entity['entity']:
#         vehicle_info = entity['vehicle']
#         id_ = entity['id']
#         trip_info = vehicle_info['trip']
#         position_info = vehicle_info['position']
#         timestamp = convert_timestamp_to_utc(vehicle_info['timestamp'])

#         # Create the new format
#         make all the things which are not coming as maybe
#         transformed_entity = {
#             "vehicleid": vehicle_info['vehicle']['id'] || id_,
#             "routeId": trip_info['routeId'],
#             "startTime": trip_info.get('startTime', None),
#             "startDate": trip_info['startDate'],
#             "scheduleRelationship": trip_info['scheduleRelationship'],
#             "tripId": trip_info['tripId'],
#             "latitude": position_info['latitude'],
#             "longitude": position_info['longitude'],
#             "speed": position_info['speed'],
#             "timestamp": timestamp
#         }
#         transformed_entities.append(transformed_entity)
    
#     return {"entity": transformed_entities}

