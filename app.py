from flask import Flask, Response
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import requests
import utils
import json
import os

# Create a Flask application instance
app = Flask(__name__)

hyderabad_api_url = "https://intouch.mappls.com/iot/api/devices"
auth_url = "https://outpost.mapmyindia.com/api/security/v2.2.3/oauth/token"
client_id = "33OkryzDZsLbfW4m63d4-eoyiwlQql5QPACUeEoe_hEvlOTtExIhi4jeAGxVoC4tlaKVqVmvRUd8ajuN4ux-hTpCZ96mzY1v"
client_secret = "lrFxI-iSEg9N0tNwzycDQIUwb1sXy69yurtwEOcMO2OJDKSTp7mMlVbrZ4ORHViNx4DJgJoqCF4DEV6WocvCZmLfrAGvCiug50CngOHm3eU%3D"

amx_chennai_api_url = "https://api.mtcbusits.in/avls/LiveData/GetLiveData"
amx_chennai_auth_header = "Basic QW1uZXhfdXNlcjpBbW5leEAkJiEjJQ=="

bearer_token = None

feed = gtfs_realtime_pb2.FeedMessage()
feed2 = gtfs_realtime_pb2.FeedMessage()
feed3 = gtfs_realtime_pb2.FeedMessage()

# Define a GET endpoint
@app.route('/api/cumta-bro', methods=['GET'])
def get_data():
    try:
        response = requests.get(
            'https://rt.dult-karnataka.com/',
            headers={'x-gtfs-api-key': 'bce99e0b3c6d9a56b403dc545973c6a9'}
        )
        print("response1", response.status_code)
        feed.ParseFromString(response.content)
        serialized = MessageToJson(feed)
        res = utils.transform_entity(json.loads(serialized))
    except Exception as e:
        print(f"Error fetching or processing DULT Karnataka feed: {e}")
        res = {"entity": []}

    try:
        response2 = requests.get(
            'https://external.chalo.com/dashboard/gtfs/realtime/chennai/mtc/bus',
            headers={'externalauth': 'KeDtr50KHF8kHULmzn5y0SOCKC6AhbUgZXj8cjHr'}
        )
        print("response2", response2.status_code)
        feed2.ParseFromString(response2.content)
        serialized2 = MessageToJson(feed2)
        res2 = utils.transform_entity(json.loads(serialized2))
    except Exception as e:
        print(f"Error fetching or processing Chalo Chennai feed: {e}")
        res2 = {"entity": []}

    try:
        amx_response = requests.post(
            amx_chennai_api_url,
            headers={'Content-Type': 'application/json', 'Authorization': amx_chennai_auth_header},
            json={}
        )
        print("response3", amx_response.status_code)
        amx_data = amx_response.json()
        res4 = utils.transform_amx_entity(amx_data)
    except Exception as e:
        print(f"Error fetching or processing Amnex Chennai data: {e}")
        res4 = {"entity": []}

    headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}
    try:
        hyderabad_response = requests.get(hyderabad_api_url, headers=headers)
        print("hyderabad_response.status_code", hyderabad_response.status_code)

        if hyderabad_response.status_code == 401:  
            auth_data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
            token_response = requests.post(auth_url, data=auth_data)
            if token_response.status_code == 200:
                token_json = token_response.json()
                bearer_token = token_json["access_token"]
                headers["Authorization"] = f"Bearer {bearer_token}"
                hyderabad_response = requests.get(hyderabad_api_url, headers=headers)

        if hyderabad_response.status_code == 200:
            hyderabad_data = hyderabad_response.json()
            res3 = utils.transform_entity(hyderabad_data)
        else:
            print(f"Failed to fetch Hyderabad data: {hyderabad_response.status_code}")
            res3 = {"entity": []}
    except Exception as e:
        print(f"Error fetching or processing Hyderabad data: {e}")
        res3 = {"entity": []}

    resFinal = {"entity": res["entity"] + res2["entity"] + res3["entity"] + res4["entity"]}

    return Response(json.dumps(resFinal), mimetype='application/json')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  
    app.run(port=port)
