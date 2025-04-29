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

logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
logger.info(f"Using sleep interval of {SLEEP_INTERVAL} seconds")

# Redis configuration
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

def get_train_status():
    """Make API call to get train running status"""
    url = 'https://enquiry.indianrail.gov.in/ntesagent/get-train-running'
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }
    
    data = {
        "trainDate": datetime.now().strftime("%Y-%m-%d")
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
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
            # Get train status
            status_data = get_train_status()
            
            # Store in Redis
            store_in_redis(status_data)
            
            # Wait for the configured interval before next call
            time.sleep(SLEEP_INTERVAL)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(SLEEP_INTERVAL)  # Wait before retrying

if __name__ == "__main__":
    main()