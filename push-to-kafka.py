import os
import requests
import time
import json
from rediscluster import RedisCluster
import redis
from confluent_kafka import Producer

# Read configurations from environment variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'default_topic')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
ENDPOINT_URL = os.getenv('ENDPOINT_URL', 'https://example.com/api/vehicle_data')
INTERVAL = int(os.getenv('INTERVAL', '10'))

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


# Function to deliver messages to Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to scrape and send to Kafka
def scrape_and_send_to_kafka():
    while True:
        try:
            # Make the request to the endpoint with SSL verification disabled
            response = requests.get(ENDPOINT_URL, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if 'entity' exists in the response
                if 'entity' in data:
                    for entity in data['entity']:
                        # Send each entity as a message to Kafka
                        route_id = entity.get('routeId', None)
                        trip_id = entity.get('tripId', None)
                        if route_id != None or trip_id != None:
                            producer.produce(KAFKA_TOPIC, key=str(entity['vehicleid']), value=json.dumps(entity), callback=delivery_report)
                            vehicle_id = str(entity['vehicleid'])
                            entity_data = {k: v for k, v in entity.items() if k not in ('vehicleid', 'routeId')}
                            reqData = {}
                            reqData['latitude'] = entity_data['latitude']
                            reqData['longitude'] = entity_data['longitude']
                            reqData['tripId'] = entity_data['tripId']
                            reqData['speed'] = str(entity_data['speed'])
                            if route_id == None:
                                print("NOT PUSHING AGAINST TRIP ID", json.dumps(reqData))
                                # redis_client.hset(f"trip:{trip_id}", mapping={vehicle_id: json.dumps(reqData)})
                            else:
                                redis_client.hset(f"route:{route_id}", mapping={vehicle_id: json.dumps(reqData)})
                        else:
                            print("got null routeId")
                else:
                    print(f"No 'entity' found in the response: {data}")
            else:
                print(f"Failed to fetch data from the endpoint. Status code: {response.status_code}")

        except Exception as e:
            print(f"An error occurred: {e}")

        # Wait for the specified interval before scraping again
        producer.flush()
        time.sleep(INTERVAL)

# Run the scraper
if __name__ == '__main__':
    print(f"Starting scraper with endpoint: {ENDPOINT_URL}, Kafka server: {KAFKA_SERVER}, topic: {KAFKA_TOPIC}")
    scrape_and_send_to_kafka()
