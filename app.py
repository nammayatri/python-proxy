from flask import Flask, Response
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import requests
import utils
import json

# Create a Flask application instance
app = Flask(__name__)

feed = gtfs_realtime_pb2.FeedMessage()

# Define a GET endpoint
@app.route('/api/cumta-bro', methods=['GET'])
def get_data():
    # Fetch the GTFS feed data
    response = requests.get(
        'https://external.chalo.com/dashboard/gtfs/realtime/chennai/mtc/bus',
        headers={'externalauth': 'KeDtr50KHF8kHULmzn5y0SOCKC6AhbUgZXj8cjHr'}
    )

    # Parse the GTFS feed data into a FeedMessage object
    feed.ParseFromString(response.content)
    
    # Convert the FeedMessage protobuf object to a JSON string
    serialized = MessageToJson(feed)
    res = utils.transform_entity(json.loads(serialized))
    
    # Return the serialized JSON data as a proper JSON response
    return Response(json.dumps(res), mimetype='application/json')

# Run the server
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
