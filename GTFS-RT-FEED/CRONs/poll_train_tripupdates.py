import requests
import redis
import json
import time
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
from pathlib import Path
import pytz
import psycopg2
from psycopg2.extras import RealDictCursor

ist = pytz.timezone("Asia/Kolkata")

SCRIPT_DIR = Path(__file__).resolve().parent
log_file = SCRIPT_DIR / 'train_updates.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_DB = os.getenv('REDIS_DB')
TRAIN_REDIS_KEY = os.getenv('TRAIN_REDIS_KEY')

# New environment variables for grouped train data Redis
GROUPED_REDIS_HOST = os.getenv('GROUPED_REDIS_HOST', REDIS_HOST)
GROUPED_REDIS_PORT = os.getenv('GROUPED_REDIS_PORT', REDIS_PORT)
GROUPED_REDIS_DB = os.getenv('GROUPED_REDIS_DB', '1')  # Default to DB 1 for grouped data

# New environment variables for API and auth
TRAIN_API_URL = os.getenv('TRAIN_API_URL', 'https://enquiry.indianrail.gov.in/ntesagent/get-train-running')
AUTH_API_URL = os.getenv('AUTH_API_URL')
AUTH_TOKEN = os.getenv('AUTH_TOKEN')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# Bus API configuration
BUS_REDIS_KEY = os.getenv('BUS_REDIS_KEY', 'gtfs-rt-tripupdates:bus')

# PostgreSQL configuration for bus data
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Redis expiration configuration
GROUPED_REDIS_EXPIRY = int(os.getenv('GROUPED_REDIS_EXPIRY', '70'))  # Default 70 seconds

# In-memory token storage
bearer_token = None
token_expiry = None

logger.info(f"Logging to file: {log_file}")
logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# New Redis client for grouped train data
grouped_redis_client = redis.Redis(
    host=GROUPED_REDIS_HOST,
    port=GROUPED_REDIS_PORT,
    db=GROUPED_REDIS_DB,
    decode_responses=True
)

def construct_trip_id(route_id, service_type_code, route_direction, schedule_trip_detail_id):
    """
    Construct trip ID using the standardized format.
    
    Args:
        route_id: The route ID
        service_type_code: The service type code
        route_direction: The route direction
        schedule_trip_detail_id: The schedule trip detail ID (trip_uniq_identifier)
    
    Returns:
        str: Constructed trip ID in format: {normalized_route_id}-{service_type_code}-{route_direction}-{schedule_trip_detail_id}
    """
    # Normalize route_id by replacing spaces with underscores
    normalized_route_id = str(route_id).replace(' ', '_')
    
    # Construct trip ID using the specified format
    trip_id = f"{normalized_route_id}-{service_type_code}-{route_direction}-{schedule_trip_detail_id}"
    
    return trip_id

def get_auth_token():
    """Get authentication token from the auth API"""
    global bearer_token, token_expiry
    
    if not AUTH_API_URL:
        logger.error("AUTH_API_URL not configured")
        return None
    
    if not CLIENT_ID or not CLIENT_SECRET:
        logger.error("CLIENT_ID and CLIENT_SECRET must be configured")
        return None
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }
    
    # Add auth token header if provided
    if AUTH_TOKEN:
        headers['authToken'] = AUTH_TOKEN
    
    # Form data for OAuth2 client credentials flow
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'cumta'
    }
    
    try:
        response = requests.post(AUTH_API_URL, headers=headers, data=data)
        
        # Handle auth failure
        if response.status_code == 401 or response.status_code == 400:
            try:
                error_data = response.json()
                if error_data.get('error') == 'invalid_client':
                    logger.error(f"Authentication failed: {error_data.get('error_description', 'Invalid credentials')}")
                else:
                    logger.error(f"Authentication failed: {error_data}")
            except:
                logger.error(f"Authentication failed with status {response.status_code}")
            return None
        
        response.raise_for_status()
        auth_data = response.json()
        
        # Extract bearer token from response - using access_token field
        if 'access_token' in auth_data:
            bearer_token = auth_data['access_token']
            # Set token expiry using expires_in field
            expires_in = auth_data.get('expires_in', 3600)  # Default to 1 hour if not provided
            token_expiry = time.time() + expires_in
            logger.info(f"Successfully obtained new bearer token, expires in {expires_in} seconds")
            return bearer_token
        else:
            logger.error("No access_token found in auth response")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making get auth token API request: {e}")
        return None

def is_token_valid():
    """Check if current bearer token is still valid"""
    global bearer_token, token_expiry
    
    if not bearer_token or not token_expiry:
        return False
    
    # Check if token has expired
    if time.time() > token_expiry:
        logger.info("Bearer token has expired")
        return False
    
    return True

def get_train_status():
    """Get train status with authentication"""
    global bearer_token
    
    # Check if we need to get a new token
    if not is_token_valid():
        logger.info("Getting new authentication token")
        bearer_token = get_auth_token()
        if not bearer_token:
            logger.error("Failed to obtain authentication token")
            return None
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'authToken': AUTH_TOKEN,
        'Authorization': f'Bearer {bearer_token}'
    }
    
    data = {
        "trainDate": datetime.now().strftime("%Y-%m-%d")
    }
    
    try:
        response = requests.post(TRAIN_API_URL, headers=headers, json=data)
        
        # Log response details for debugging
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        
        # Handle authentication errors from train API
        if response.status_code in [401, 403]:
            try:
                error_data = response.json()
                if 'errors' in error_data:
                    error_info = error_data['errors']
                    if error_info.get('code') == 900902:
                        logger.warning("Authentication information missing or invalid, refreshing token")
                        bearer_token = get_auth_token()
                        if bearer_token:
                            headers['Authorization'] = f'Bearer {bearer_token}'
                            response = requests.post(TRAIN_API_URL, headers=headers, json=data)
                        else:
                            logger.error("Failed to refresh authentication token")
                            return None
                    else:
                        logger.error(f"Authentication error: {error_info}")
                        return None
                else:
                    logger.warning("Authentication failed, trying to refresh token")
                    bearer_token = get_auth_token()
                    if bearer_token:
                        headers['Authorization'] = f'Bearer {bearer_token}'
                        response = requests.post(TRAIN_API_URL, headers=headers, json=data)
            except:
                logger.warning("Authentication failed, trying to refresh token")
                bearer_token = get_auth_token()
                if bearer_token:
                    headers['Authorization'] = f'Bearer {bearer_token}'
                    response = requests.post(TRAIN_API_URL, headers=headers, json=data)
        
        response.raise_for_status()
        
        # Check if response has content
        if not response.text.strip():
            logger.error("Empty response received from API")
            return None
            
        try:
            response_data = response.json()
            logger.info(f"Successfully parsed response JSON")
            logger.info(f"Response data type: {type(response_data)}")
            logger.info(f"Response data: {str(response_data)[:500]}...")
            return response_data
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            logger.error(f"Response content: {response.text[:500]}...")  # Log first 500 chars
            return None
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making get train status API request: {e}")
        return None

def get_bus_status():
    """Get bus trip updates from PostgreSQL database"""
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        logger.warning("PostgreSQL configuration not complete, skipping bus data fetch")
        return None
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Query 1: Get all trips (without waybills)
            all_trips_query = """
            SELECT
              "public"."bus_route"."route_id" AS "route_id",
              "public"."bus_route"."route_name" AS "route_name",
              "public"."bus_route"."route_number" AS "schedule_route_code",
              "Bus Schedule Trip Detail - Route"."schedule_number" AS "schedule_number",
              "Bus Schedule Trip Detail - Route"."schedule_trip_detail_id" AS "trip_uniq_identifier",
              "public"."bus_route"."route_direction" AS "route_direction",
              "Bus Schedule Trip Detail - Route"."start_time" AS "schedule_trip_start_time",
              split_part(schedule_number, '-', 1) as "service_type_code"
            FROM
              "public"."bus_route"
              INNER JOIN "public"."bus_schedule_trip_detail" AS "Bus Schedule Trip Detail - Route" ON "public"."bus_route"."route_id" = "Bus Schedule Trip Detail - Route"."route_number_id"
              INNER JOIN "public"."bus_schedule_trip" AS "bus_schedule_trip__via__schedule_trip_id" ON "Bus Schedule Trip Detail - Route"."schedule_trip_id" = "bus_schedule_trip__via__schedule_trip_id"."schedule_trip_id"
            WHERE
              ("public"."bus_route"."status" = 'Active')
              AND ("public"."bus_route"."deleted" = FALSE)
              AND ("Bus Schedule Trip Detail - Route"."deleted" = FALSE)
              AND ("bus_schedule_trip__via__schedule_trip_id"."deleted" = FALSE)
              AND ("bus_schedule_trip__via__schedule_trip_id"."status" = 'Active')
            """
            
            cursor.execute(all_trips_query)
            all_trips = cursor.fetchall()
            logger.info(f"Fetched {len(all_trips)} total trips from database")
            
            # Query 2: Get trips happening today (with waybills)
            today_trips_query = """
            SELECT
              "public"."bus_route"."route_id" AS "route_id",
              "public"."bus_route"."route_name" AS "route_name",
              "public"."bus_route"."route_number" AS "schedule_route_code",
              "Bus Schedule Trip Detail - Route"."schedule_number" AS "schedule_number",
              "Bus Schedule Trip Detail - Route"."schedule_trip_detail_id" AS "trip_uniq_identifier",
              "public"."bus_route"."route_direction" AS "route_direction",
              "Bus Schedule Trip Detail - Route"."start_time" AS "schedule_trip_start_time",
              split_part(schedule_number, '-', 1) as "service_type_code"
            FROM
              "public"."bus_route"
              INNER JOIN "public"."bus_schedule_trip_detail" AS "Bus Schedule Trip Detail - Route" ON "public"."bus_route"."route_id" = "Bus Schedule Trip Detail - Route"."route_number_id"
              INNER JOIN "public"."waybills"  ON "public"."waybills"."schedule_trip_id" = "Bus Schedule Trip Detail - Route"."schedule_trip_id"
              INNER JOIN "public"."bus_schedule_trip" AS "bus_schedule_trip__via__schedule_trip_id" ON "Bus Schedule Trip Detail - Route"."schedule_trip_id" = "bus_schedule_trip__via__schedule_trip_id"."schedule_trip_id"
            WHERE
              ("public"."bus_route"."status" = 'Active')
              AND ("public"."bus_route"."deleted" = FALSE)
              AND ("Bus Schedule Trip Detail - Route"."deleted" = FALSE)
              AND ("bus_schedule_trip__via__schedule_trip_id"."deleted" = FALSE)
              AND ("bus_schedule_trip__via__schedule_trip_id"."status" = 'Active')
              AND "public"."waybills"."duty_date" = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
            """
            
            cursor.execute(today_trips_query)
            today_trips = cursor.fetchall()
            logger.info(f"Fetched {len(today_trips)} trips happening today from database")
            
            # Convert to sets for comparison using constructed trip IDs
            all_trip_ids = set()
            today_trip_ids = set()
            
            # Construct trip IDs for all trips
            for trip in all_trips:
                trip_id = construct_trip_id(
                    trip['route_id'], 
                    trip['service_type_code'], 
                    trip['route_direction'], 
                    trip['trip_uniq_identifier']
                )
                all_trip_ids.add(trip_id)
            
            # Construct trip IDs for today's trips
            for trip in today_trips:
                trip_id = construct_trip_id(
                    trip['route_id'], 
                    trip['service_type_code'], 
                    trip['route_direction'], 
                    trip['trip_uniq_identifier']
                )
                today_trip_ids.add(trip_id)
            
            # Find cancelled trips (trips in all_trips but not in today_trips)
            cancelled_trip_ids = all_trip_ids - today_trip_ids
            logger.info(f"Found {len(cancelled_trip_ids)} cancelled trips")
            
            # Create the response data structure
            response_data = {
                'all_trips': [dict(trip) for trip in all_trips],
                'today_trips': [dict(trip) for trip in today_trips],
                'cancelled_trip_ids': cancelled_trip_ids  # Keep as set for O(1) lookup
            }
            
            logger.info(f"Successfully fetched bus data from PostgreSQL")
            return response_data
            
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching bus data: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def transform_to_gtfs_rt(data):
    """Transform railway API data to GTFS-RT format"""
    if not data:
        return None, None, []

    # Check if data is a string (error message or HTML)
    if isinstance(data, str):
        logger.error(f"API returned string instead of JSON object: {data[:200]}...")
        return None, None, []
    
    # Handle the new API response structure
    if isinstance(data, dict):
        if 'vTrainRunningList' in data:
            train_list = data['vTrainRunningList']
            logger.info(f"Found vTrainRunningList with {len(train_list)} items")
            data = train_list
        else:
            logger.error(f"API returned dict but no vTrainRunningList found. Keys: {list(data.keys())}")
            return None, None, []
    elif not isinstance(data, list):
        logger.error(f"API returned unexpected data type: {type(data)}. Data: {str(data)[:200]}...")
        return None, None, []

    logger.info(f"Processing {len(data)} items from API response")
    
    current_timestamp = int(time.time())
    
    gtfs_rt = {
        "header": {
            "gtfsRealtimeVersion": "2.0",
            "incrementality": "FULL_DATASET",
            "timestamp": str(current_timestamp)
        },
        "entity": []
    }

    trains_data = {}
    cancelled_train_ids = []
    for i, station in enumerate(data):
        logger.info(f"Processing item {i}: type={type(station)}, value={str(station)[:100]}...")
        
        # Check if station is a dictionary
        if not isinstance(station, dict):
            logger.warning(f"Skipping non-dict station data at index {i}: {type(station)}")
            continue
            
        train_no = station.get('trainNo')
        if not train_no:
            logger.warning(f"No trainNo found in station at index {i}")
            continue

        # Convert MASS to MAS if station code is MASS
        if station.get('stationCode') == 'MASS':
            station = station.copy()  # Create a copy to avoid modifying the original
            station['stationCode'] = 'MAS'
        
        if train_no not in trains_data:
            trains_data[train_no] = []
        trains_data[train_no].append(station)

    for train_no, stations in trains_data.items():
        if not stations:
            continue

        first_station = stations[0]
        
        if first_station.get('exceptionFlag') == 1 and first_station.get('trainRunStatus') == 0:
            trip_update = {
                "id": f"{train_no}_T1",
                "tripUpdate": {
                    "trip": {
                        "tripId": f"{train_no}_T1",
                        "scheduleRelationship": "CANCELED"
                    }
                }
            }
            gtfs_rt["entity"].append(trip_update)
            cancelled_train_ids.append(f"{train_no}_T1")
            logger.info(f"Successfully cancelled train {train_no}_T1")
            continue
        
        train_start_date = datetime.strptime(first_station['trainStartDate'], "%Y/%m/%d %H:%M:%S")
        
        # Get today's date in IST for start_date
        today_ist = datetime.now(ist).strftime("%Y%m%d")
        
        trip_update = {
            "id": f"{train_no}_T1",
            "tripUpdate": {
                "trip": {
                    "tripId": f"{train_no}_T1",
                    "startTime": train_start_date.strftime("%H:%M:%S"),
                    "startDate": today_ist,
                    "routeId": train_no,
                    "directionId": 0
                },
                "stopTimeUpdate": [],
                "vehicle": {
                    "id": f"vehicle_{train_no}",
                    "label": train_no
                },
                "timestamp": str(current_timestamp)
            }
        }

        for station in stations:
            if station.get('arrCancelFlag') == 1:
                stop_update = {
                    "stopSequence": station['sequence'],
                    "stopId": station['stationCode'],
                    "scheduleRelationship": "SKIPPED"
                }
                trip_update["tripUpdate"]["stopTimeUpdate"].append(stop_update)
                logger.info(f"Successfully skipped station {station['stationCode']} for train {train_no}")
                continue

            sched_arrival_time = datetime.strptime(station['schedArrivalTime'], "%H:%M:%S").time()
            sched_departure_time = datetime.strptime(station['schedDepartureTime'], "%H:%M:%S").time()
            
            sched_arrival = datetime.combine(train_start_date.date(), sched_arrival_time)
            sched_departure = datetime.combine(train_start_date.date(), sched_departure_time)

            sched_arrival_ist = ist.localize(sched_arrival)
            sched_departure_ist = ist.localize(sched_departure)
            
            actual_arrival = sched_arrival_ist.timestamp() + station['delayArrival']
            actual_departure = sched_departure_ist.timestamp() + station['delayDeparture']
            
            # Ensure departure time is not less than arrival time
            if actual_departure < actual_arrival:
                logger.warning(f"Departure time ({actual_departure}) is less than arrival time ({actual_arrival}) for train {train_no} at station {station['stationCode']}. Setting departure time to arrival time.")
                actual_departure = actual_arrival

            stop_update = {
                "stopSequence": station['sequence'],
                "arrival": {
                    "time": str(int(actual_arrival))
                },
                "departure": {
                    "time": str(int(actual_departure))
                },
                "stopId": station['stationCode']
            }
            
            platform_code = station.get('platformNo', '')
            logger.info(f"Platform code for {station['stationCode']} is {platform_code}")
            if platform_code and platform_code.strip():
                stop_update["stopTimeProperties"] = {
                    "assignedStopId": f"{station['stationCode']}_P_{platform_code}"
                }
                logger.info(f"Successfully changed platform number for {station['stationCode']} to {platform_code}")
            
            trip_update["tripUpdate"]["stopTimeUpdate"].append(stop_update)

        gtfs_rt["entity"].append(trip_update)

    return gtfs_rt, trains_data, cancelled_train_ids

def transform_bus_to_gtfs_rt(data):
    """Transform bus database data to GTFS-RT format"""
    if not data:
        return None

    # Check if data is a string (error message or HTML)
    if isinstance(data, str):
        logger.error(f"Bus data returned string instead of dict: {data[:200]}...")
        return None
    
    # Handle database response structure
    if not isinstance(data, dict):
        logger.error(f"Bus data returned unexpected data type: {type(data)}. Data: {str(data)[:200]}...")
        return None

    # Extract data from database response
    all_trips = data.get('all_trips', [])
    today_trips = data.get('today_trips', [])
    cancelled_trip_ids = set(data.get('cancelled_trip_ids', []))

    logger.info(f"Processing {len(all_trips)} total trips, {len(today_trips)} today trips, {len(cancelled_trip_ids)} cancelled trips")
    
    current_timestamp = int(time.time())
    
    gtfs_rt = {
        "header": {
            "gtfsRealtimeVersion": "2.0",
            "incrementality": "FULL_DATASET",
            "timestamp": str(current_timestamp)
        },
        "entity": []
    }

    # Process all trips to create GTFS-RT entities
    for i, trip in enumerate(all_trips):
        logger.info(f"Processing bus trip {i}: {trip.get('trip_uniq_identifier', 'unknown')}")
        
        # Check if trip is a dictionary
        if not isinstance(trip, dict):
            logger.warning(f"Skipping non-dict trip data at index {i}: {type(trip)}")
            continue
            
        # Extract trip information from database
        schedule_trip_detail_id = trip.get('trip_uniq_identifier')
        route_id = trip.get('route_id')
        route_name = trip.get('route_name')
        schedule_route_code = trip.get('schedule_route_code')
        route_direction = trip.get('route_direction', 0)
        schedule_number = trip.get('schedule_number')
        start_time = trip.get('schedule_trip_start_time')
        service_type_code = trip.get('service_type_code')
        
        if not schedule_trip_detail_id:
            logger.warning(f"No schedule trip detail ID found in bus trip at index {i}")
            continue
            
        # Construct trip ID using the standardized format
        trip_id = construct_trip_id(route_id, service_type_code, route_direction, schedule_trip_detail_id)

        # Check if this trip is cancelled
        is_cancelled = trip_id in cancelled_trip_ids
        
        # Get today's date in IST for start_date
        today_ist = datetime.now(ist).strftime("%Y%m%d")
        
        # Create trip update entity
        trip_update = {
            "id": f"{trip_id}",
            "tripUpdate": {
                "trip": {
                    "tripId": f"{trip_id}",
                    "startDate": today_ist
                },
                "timestamp": str(current_timestamp)
            }
        }

        # Only include cancelled trips in the final JSON
        if is_cancelled:
            trip_update["tripUpdate"]["trip"]["scheduleRelationship"] = "CANCELED"
            logger.info(f"Marked bus trip {trip_id} as CANCELLED")
            gtfs_rt["entity"].append(trip_update)
        else:
            # Skip non-cancelled trips - don't add them to the final JSON
            logger.debug(f"Skipping non-cancelled bus trip {trip_id}")

    logger.info(f"Successfully transformed {len(gtfs_rt['entity'])} bus trips to GTFS-RT format")
    return gtfs_rt

def store_gtfs_rt_in_redis(gtfs_rt_data, cancelled_train_ids=None):
    """Store the GTFS-RT data in Redis"""
    if not gtfs_rt_data:
        logger.warning("No GTFS-RT data to store")
        return

    try:
        redis_client.set(TRAIN_REDIS_KEY, json.dumps(gtfs_rt_data))
        
        redis_client.expire(TRAIN_REDIS_KEY, 86400)
        
        logger.info(f"Successfully stored GTFS-RT feed with {len(gtfs_rt_data['entity'])} trip updates")
        
        # Store cancelled train IDs if provided
        if cancelled_train_ids:
            redis_client.set("trains:cancelled", json.dumps(cancelled_train_ids))
            redis_client.expire("trains:cancelled", 43200)
            logger.info(f"Successfully stored {len(cancelled_train_ids)} cancelled train IDs: {cancelled_train_ids}")
        else:
            logger.info("No cancelled trains to store")

    except redis.RedisError as e:
        logger.error(f"Error storing data in Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while storing data: {e}")
        raise

def store_grouped_train_data_in_redis(trains_data):
    """Store grouped train data in the second Redis instance"""
    if not trains_data:
        logger.warning("No grouped train data to store")
        return

    try:
        for train_no, stations in trains_data.items():
            # Create key name in format "suburban:<train_no>"
            key_name = f"suburban:{train_no}"
            
            # Store the grouped data as JSON
            grouped_redis_client.set(key_name, json.dumps(stations))
            
            # Set expiry using environment variable
            grouped_redis_client.expire(key_name, GROUPED_REDIS_EXPIRY)
            
            logger.info(f"Successfully stored grouped data for train {train_no} with {len(stations)} stations")
        
        logger.info(f"Successfully stored grouped data for {len(trains_data)} trains")

    except redis.RedisError as e:
        logger.error(f"Error storing grouped data in Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while storing grouped data: {e}")
        raise

def store_bus_gtfs_rt_in_redis(gtfs_rt_data):
    """Store the bus GTFS-RT data in Redis"""
    if not gtfs_rt_data:
        logger.warning("No bus GTFS-RT data to store")
        return

    try:
        redis_client.set(BUS_REDIS_KEY, json.dumps(gtfs_rt_data))
        
        redis_client.expire(BUS_REDIS_KEY, 86400)  # 24 hours expiry
        
        logger.info(f"Successfully stored bus GTFS-RT feed with {len(gtfs_rt_data['entity'])} trip updates")

    except redis.RedisError as e:
        logger.error(f"Error storing bus data in Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while storing bus data: {e}")
        raise

def store_platform_codes_in_redis(trains_data):
    """Store platform codes in Redis hashmap for Haskell code to consume"""
    if not trains_data:
        logger.warning("No train data to extract platform codes from")
        return

    try:
        platform_hash_key = "platform-codes-hashmap"
        platform_codes = {}
        
        # Extract platform codes from train data
        for train_no, stations in trains_data.items():
            trip_id = f"{train_no}_T1"  # Match the trip ID format used in GTFS-RT
            
            # Extract platform codes for all stations with platform information
            for station in stations:
                station_code = station.get('stationCode', '')
                platform_code = station.get('platformNo', '')
                
                if station_code and platform_code and platform_code.strip():
                    # Use the format: tripId:stopCode -> platformCode
                    redis_key = f"{trip_id}:{station_code}"
                    # JSON encode the platform code to make it work with hmGet
                    platform_codes[redis_key] = json.dumps(platform_code)
                    logger.info(f"Found platform code {platform_code} for trip {trip_id} at station {station_code}")
        
        # Store platform codes in Redis hashmap
        if platform_codes:
            # Clear existing hashmap and set new values
            redis_client.delete(platform_hash_key)
            redis_client.hset(platform_hash_key, mapping=platform_codes)
            
            # Set expiry for the hashmap (same as train data)
            redis_client.expire(platform_hash_key, 86400)  # 24 hours expiry
            
            logger.info(f"Successfully stored {len(platform_codes)} platform codes in Redis hashmap '{platform_hash_key}'")
            logger.info(f"Platform codes stored: {list(platform_codes.keys())[:10]}...")  # Show first 10 keys
        else:
            logger.info("No platform codes found to store")
            
    except redis.RedisError as e:
        logger.error(f"Error storing platform codes in Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while storing platform codes: {e}")
        raise
def main():
    """Main function to fetch and store train and bus status data"""
    logger.info("Starting train and bus status data fetch")
    
    # Process train data
    try:
        logger.info("Processing train data...")
        train_status_data = get_train_status()
        
        # Log the type and content of train_status_data for debugging
        if train_status_data is not None:
            logger.info(f"Received train status_data type: {type(train_status_data)}")
            if isinstance(train_status_data, list):
                logger.info(f"Received {len(train_status_data)} train stations")
            elif isinstance(train_status_data, dict):
                logger.info(f"Received train dict with keys: {list(train_status_data.keys())}")
            else:
                logger.info(f"Received train data: {str(train_status_data)[:200]}...")
        else:
            logger.warning("No train status data received")
        
        train_gtfs_rt_data, grouped_trains_data, cancelled_train_ids = transform_to_gtfs_rt(train_status_data)

        store_gtfs_rt_in_redis(train_gtfs_rt_data, cancelled_train_ids)
        store_grouped_train_data_in_redis(grouped_trains_data) # This caused time table issues so disabling it for now
        
        # Store platform codes in Redis hashmap for Haskell code
        store_platform_codes_in_redis(grouped_trains_data)
        
        logger.info("Successfully completed train status data fetch and storage")
            
    except Exception as e:
        logger.error(f"Unexpected error in train processing: {e}")
        # Don't raise here - continue with bus processing
    
    # Process bus data
    try:
        logger.info("Processing bus data...")
        bus_status_data = get_bus_status()
        
        # Log the type and content of bus_status_data for debugging
        if bus_status_data is not None:
            logger.info(f"Received bus status_data type: {type(bus_status_data)}")
            if isinstance(bus_status_data, list):
                logger.info(f"Received {len(bus_status_data)} bus trips")
            elif isinstance(bus_status_data, dict):
                logger.info(f"Received bus dict with keys: {list(bus_status_data.keys())}")
            else:
                logger.info(f"Received bus data: {str(bus_status_data)[:200]}...")
        else:
            logger.warning("No bus status data received")
        
        bus_gtfs_rt_data = transform_bus_to_gtfs_rt(bus_status_data)
        store_bus_gtfs_rt_in_redis(bus_gtfs_rt_data)
        
        logger.info("Successfully completed bus status data fetch and storage")
            
    except Exception as e:
        logger.error(f"Unexpected error in bus processing: {e}")
        # Don't raise here - bus processing failure shouldn't affect train processing
    
    logger.info("Completed train and bus status data processing")

if __name__ == "__main__":
    main()