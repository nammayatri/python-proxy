import requests
import redis
import json
import time
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis configuration from environment variables
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL', 5))
CLIENT_ID = os.getenv('CLIENT_ID', 'client1')
CLIENT_SECRET = os.getenv('CLIENT_SECRET', 'secret')
SCOPE = os.getenv('SCOPE', 'cumta')
AUTH_TOKEN = os.getenv('AUTH_TOKEN', 'token')
AUTH_URL = os.getenv('AUTH_URL', 'auth_url')
TRAIN_RUNNING_URL = os.getenv('TRAIN_RUNNING_URL', 'train_running_url')

access_token_global = None

logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
logger.info(f"Using sleep interval of {SLEEP_INTERVAL} seconds")

# Redis configuration
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

def get_access_token(force_refresh=False):
    global access_token_global
    if not force_refresh and access_token_global:
        return access_token_global
    url = AUTH_URL
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': SCOPE
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        token_data = response.json()
        access_token_global = token_data.get('access_token')
        return access_token_global
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting access token: {e}")
        return None

def get_train_status():
    """Make API call to get train running status"""
    global access_token_global
    access_token = get_access_token()
    if not access_token:
        logger.error("Failed to get access token")
        return None

    url = TRAIN_RUNNING_URL
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'authToken': AUTH_TOKEN,
        'Authorization': f'Bearer {access_token}'
    }
    
    data = {
        "trainDate": datetime.now().strftime("%Y-%m-%d")
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        # If authentication error, refresh token and retry once
        if (
            result
            and isinstance(result, dict)
            and "errors" in result
            and result["errors"].get("code") == 900901
        ):
            logger.info("Access token expired or invalid, refreshing token and retrying...")
            access_token = get_access_token(force_refresh=True)
            if not access_token:
                logger.error("Failed to refresh access token")
                return None
            headers['Authorization'] = f'Bearer {access_token}'
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")
        return None

def store_in_redis(data):
    """Store the train status data in Redis"""
    if not data:
        return

    try:
        # Group stations by train number
        trains_data = {}
        for station in data:
            train_no = station.get('trainNo')
            if not train_no:
                continue

            if train_no not in trains_data:
                trains_data[train_no] = []
            trains_data[train_no].append(station)

        # Store each train's data in Redis
        for train_no, stations in trains_data.items():
            key = f"suburban:{train_no}"
            
            # Store the stations list as JSON string
            redis_client.set(key, json.dumps(stations))
            
            # Set expiry for 24 hours (86400 seconds)
            redis_client.expire(key, 86400)
            
            logger.info(f"Successfully stored data for train {train_no} with {len(stations)} stations")

    except redis.RedisError as e:
        logger.error(f"Error storing data in Redis: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while processing data: {e}")

def main():
    """Main function to run the script continuously"""
    logger.info("Starting train status monitoring service")
    
    while True:
        try:
            status_data = get_train_status()
            # print("get_train_status() response:", status_data)  # Debug print
            if status_data and 'vTrainRunningList' in status_data:
                store_in_redis(status_data['vTrainRunningList'])
            else:
                logger.error("API response missing 'vTrainRunningList'")
            time.sleep(SLEEP_INTERVAL)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(SLEEP_INTERVAL)  # Wait before retrying

if __name__ == "__main__":
    main()