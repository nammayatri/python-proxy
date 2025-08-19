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

def transform_to_gtfs_rt(data):
    """Transform railway API data to GTFS-RT format"""
    if not data:
        return None, None

    # Check if data is a string (error message or HTML)
    if isinstance(data, str):
        logger.error(f"API returned string instead of JSON object: {data[:200]}...")
        return None, None
    
    # Handle the new API response structure
    if isinstance(data, dict):
        if 'vTrainRunningList' in data:
            train_list = data['vTrainRunningList']
            logger.info(f"Found vTrainRunningList with {len(train_list)} items")
            data = train_list
        else:
            logger.error(f"API returned dict but no vTrainRunningList found. Keys: {list(data.keys())}")
            return None, None
    elif not isinstance(data, list):
        logger.error(f"API returned unexpected data type: {type(data)}. Data: {str(data)[:200]}...")
        return None, None

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
            continue
        
        train_start_date = datetime.strptime(first_station['trainStartDate'], "%Y/%m/%d %H:%M:%S")
        
        trip_update = {
            "id": f"{train_no}_T1",
            "tripUpdate": {
                "trip": {
                    "tripId": f"{train_no}_T1",
                    "startTime": train_start_date.strftime("%H:%M:%S"),
                    "startDate": train_start_date.strftime("%Y%m%d"),
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
                continue

            sched_arrival_time = datetime.strptime(station['schedArrivalTime'], "%H:%M:%S").time()
            sched_departure_time = datetime.strptime(station['schedDepartureTime'], "%H:%M:%S").time()
            
            sched_arrival = datetime.combine(train_start_date.date(), sched_arrival_time)
            sched_departure = datetime.combine(train_start_date.date(), sched_departure_time)

            sched_arrival_ist = ist.localize(sched_arrival)
            sched_departure_ist = ist.localize(sched_departure)
            
            actual_arrival = sched_arrival_ist.timestamp() + station['delayArrival']
            actual_departure = sched_departure_ist.timestamp() + station['delayDeparture']
            

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
            
            trip_update["tripUpdate"]["stopTimeUpdate"].append(stop_update)

        gtfs_rt["entity"].append(trip_update)

    return gtfs_rt, trains_data

def store_gtfs_rt_in_redis(gtfs_rt_data):
    """Store the GTFS-RT data in Redis"""
    if not gtfs_rt_data:
        logger.warning("No GTFS-RT data to store")
        return

    try:
        redis_client.set(TRAIN_REDIS_KEY, json.dumps(gtfs_rt_data))
        
        redis_client.expire(TRAIN_REDIS_KEY, 86400)
        
        logger.info(f"Successfully stored GTFS-RT feed with {len(gtfs_rt_data['entity'])} trip updates")

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

def main():
    """Main function to fetch and store train status data"""
    logger.info("Starting train status data fetch")
    
    try:
        status_data = get_train_status()
        
        # Log the type and content of status_data for debugging
        if status_data is not None:
            logger.info(f"Received status_data type: {type(status_data)}")
            if isinstance(status_data, list):
                logger.info(f"Received {len(status_data)} stations")
            elif isinstance(status_data, dict):
                logger.info(f"Received dict with keys: {list(status_data.keys())}")
            else:
                logger.info(f"Received data: {str(status_data)[:200]}...")
        else:
            logger.warning("No status data received")
        
        gtfs_rt_data, grouped_trains_data = transform_to_gtfs_rt(status_data)

        store_gtfs_rt_in_redis(gtfs_rt_data)
        store_grouped_train_data_in_redis(grouped_trains_data)
        
        logger.info("Successfully completed train status data fetch and storage")
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()