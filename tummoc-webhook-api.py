import logging
import threading
import time
from flask import Flask, request, jsonify
from confluent_kafka import Producer
import os

app = Flask(__name__)

ACCESS_TOKEN = "+QTB8QjMJbRT+ajMbreDrg=="

KAFKA_TOPIC_WEBHOOK = os.getenv('KAFKA_TOPIC_WEBHOOK', 'tummoc_logs')
KAFKA_BROKER_WEBHOOK = os.getenv('KAFKA_BROKER_WEBHOOK', 'localhost:9096')

# Setup Kafka producer with better config for high load
producer_config = {
    'bootstrap.servers': KAFKA_BROKER_WEBHOOK,
    'queue.buffering.max.messages': 1000000,  # Increase buffer size (default is 100,000)
    'queue.buffering.max.ms': 100,  # Batch more frequently
    'compression.type': 'snappy',  # Add compression to reduce bandwidth
    'retry.backoff.ms': 250,  # Shorter backoff for retries
    'message.max.bytes': 1000000,  # Allow larger messages
    'request.timeout.ms': 30000,  # Longer timeout
    'delivery.timeout.ms': 120000,  # Allow more time for delivery
    'message.send.max.retries': 5  # More retries before giving up
}

producer = Producer(producer_config)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

logger = logging.getLogger('tummoc_logger')

class KafkaLogHandler(logging.Handler):
    def emit(self, record):
        max_retries = 3
        retries = 0
        success = False
        log_entry = self.format(record)
        while retries < max_retries and not success:
            try:
                logger.info(f"Sending data to Kafka topic {KAFKA_TOPIC_WEBHOOK}")
                producer.produce(KAFKA_TOPIC_WEBHOOK, log_entry.encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                success = True
            except BufferError as e:
                logger.error(f"Kafka buffer full, waiting before retry: {str(e)}")
                producer.poll(1)
                retries += 1
            except Exception as e:
                logger.error(f"Failed to send to Kafka (attempt {retries+1}): {str(e)}")
                retries += 1
                time.sleep(1)
        if success:
            try:
                producer.flush(timeout=5.0)
                logger.info(f"Successfully sent data to Kafka topic {KAFKA_TOPIC_WEBHOOK}")
            except Exception as e:
                logger.error(f"Error flushing Kafka producer: {str(e)}")

def periodic_flush():
    """Periodically flush the Kafka producer"""
    while True:
        try:
            time.sleep(5)  # Flush every 5 seconds
            producer.flush(timeout=1.0)
            logger.info("Performed periodic Kafka flush")
        except Exception as e:
            logger.warning(f"Error during periodic flush: {e}")

log_handler = KafkaLogHandler()
log_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

app.logger.addHandler(log_handler)
app.logger.setLevel(logging.INFO)
app.logger.propagate = False

flush_thread = threading.Thread(target=periodic_flush, daemon=True)
flush_thread.start()

def validate_token(req):
    token = req.headers.get("Access-Token")
    return token == ACCESS_TOKEN

@app.route("/schedule-fleet-sync", methods=["POST"])
def schedule_fleet_sync():
    if not validate_token(request):
        app.logger.warning("Unauthorized access attempt to /schedule-fleet-sync")
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    
    data = request.get_json()
    app.logger.info(f"Received /schedule-fleet-sync request: {data}")
    if not data or not all(k in data for k in ["requestId", "fleetNo", "scheduleNo", "timestamp"]):
        return jsonify({"status": "error", "message": "Invalid request payload"}), 400
    
    return jsonify({"status": "success", "data": data["requestId"]})

@app.route("/schedule-trip-sync", methods=["POST"])
def schedule_trip_sync():
    if not validate_token(request):
        app.logger.warning("Unauthorized access attempt to /schedule-trip-sync")
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    
    data = request.get_json()
    app.logger.info(f"Received /schedule-trip-sync request: {data}")
    if not data or not all(k in data for k in ["requestId", "tripEvent", "tripNo", "routeNo", "scheduleNo", "timestamp"]):
        return jsonify({"status": "error", "message": "Invalid request payload"}), 400
    
    return jsonify({"status": "success", "data": data["requestId"]})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
