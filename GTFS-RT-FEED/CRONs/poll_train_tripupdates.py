import requests
import redis
import json
import time
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
from pathlib import Path

# Get script directory and set up log file
SCRIPT_DIR = Path(__file__).resolve().parent
log_file = SCRIPT_DIR / 'train_updates.log'

# Configure logging to write to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Redis configuration from environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_DB = os.getenv('REDIS_DB')
TRAIN_REDIS_KEY = os.getenv('TRAIN_REDIS_KEY')

logger.info(f"Logging to file: {log_file}")
logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")

# Redis configuration
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

def get_train_status():
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

def transform_to_gtfs_rt(data):
    """Transform railway API data to GTFS-RT format"""
    if not data:
        return None

    current_timestamp = int(time.time())
    
    # Initialize GTFS-RT structure
    gtfs_rt = {
        "header": {
            "gtfsRealtimeVersion": "2.0",
            "incrementality": "FULL_DATASET",
            "timestamp": str(current_timestamp)
        },
        "entity": []
    }

    # Group stations by train number
    trains_data = {}
    for station in data:
        train_no = station.get('trainNo')
        if not train_no:
            continue

        if train_no not in trains_data:
            trains_data[train_no] = []
        trains_data[train_no].append(station)

    # Process each train
    for train_no, stations in trains_data.items():
        if not stations:
            continue

        # Get the first station to extract common trip information
        first_station = stations[0]
        train_start_date = datetime.strptime(first_station['trainStartDate'], "%Y/%m/%d %H:%M:%S")
        
        # Create trip update entity
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

        # Add stop time updates
        for station in stations:
            # Parse scheduled times
            sched_arrival_time = datetime.strptime(station['schedArrivalTime'], "%H:%M:%S").time()
            sched_departure_time = datetime.strptime(station['schedDepartureTime'], "%H:%M:%S").time()
            
            # Combine with train start date
            sched_arrival = datetime.combine(train_start_date.date(), sched_arrival_time)
            sched_departure = datetime.combine(train_start_date.date(), sched_departure_time)
            
            # Add delays to scheduled times
            actual_arrival = sched_arrival.timestamp() + station['delayArrival']
            actual_departure = sched_departure.timestamp() + station['delayDeparture']

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

    return gtfs_rt

def store_gtfs_rt_in_redis(gtfs_rt_data):
    """Store the GTFS-RT data in Redis"""
    if not gtfs_rt_data:
        logger.warning("No GTFS-RT data to store")
        return

    try:
        # Store the complete GTFS-RT feed
        redis_client.set(TRAIN_REDIS_KEY, json.dumps(gtfs_rt_data))
        
        # Set expiry for 24 hours (86400 seconds)
        redis_client.expire(TRAIN_REDIS_KEY, 86400)
        
        logger.info(f"Successfully stored GTFS-RT feed with {len(gtfs_rt_data['entity'])} trip updates")

    except redis.RedisError as e:
        logger.error(f"Error storing data in Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while storing data: {e}")
        raise

def main():
    """Main function to fetch and store train status data"""
    logger.info("Starting train status data fetch")
    
    try:
        # Get train status
        status_data = get_train_status()
        
        # Transform to GTFS-RT format
        gtfs_rt_data = transform_to_gtfs_rt(status_data)

        print(gtfs_rt_data)
        
        # Store in Redis
        store_gtfs_rt_in_redis(gtfs_rt_data)
        
        logger.info("Successfully completed train status data fetch and storage")
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise  # Re-raise the exception so Kubernetes knows the job failed

if __name__ == "__main__":
    main()