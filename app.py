from flask import Flask, jsonify
from google.transit import gtfs_realtime_pb2
import requests

# Create a Flask application instance
app = Flask(__name__)

feed = gtfs_realtime_pb2.FeedMessage()
# Define a GET endpoint
@app.route('/api/cumta-bro', methods=['GET'])
def get_data():
    # Return a JSON response
    response = requests.get('https://external.chalo.com/dashboard/gtfs/realtime/chennai/mtc/bus', headers={'externalauth': 'KeDtr50KHF8kHULmzn5y0SOCKC6AhbUgZXj8cjHr'})
    feed.ParseFromString(response.content)
    return jsonify(feed.entity)

# Run the server
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
