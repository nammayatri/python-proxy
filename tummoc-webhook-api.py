from flask import Flask, request, jsonify

app = Flask(__name__)

ACCESS_TOKEN = "+QTB8QjMJbRT+ajMbreDrg=="

def validate_token(req):
    token = req.headers.get("Access-Token")
    return token == ACCESS_TOKEN

@app.route("/schedule-fleet-sync", methods=["POST"])
def schedule_fleet_sync():
    if not validate_token(request):
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    
    data = request.get_json()
    print("data from /schedule-fleet-sync:\n")
    print(data)
    # validation
    if not data or not all(k in data for k in ["requestId", "fleetNo", "scheduleNo", "timestamp"]):
        return jsonify({"status": "error", "message": "Invalid request payload"}), 400
    # fleetNo = should be Vehicle No
    # scheduleNo = O-26-R-PM something like this 
    # process the data here
    
    return jsonify({"status": "success", "data": data["requestId"]})

@app.route("/schedule-trip-sync", methods=["POST"])
def schedule_trip_sync():
    if not validate_token(request):
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    
    data = request.get_json()
    print("data from /schedule-trip-sync:\n")
    print(data)
    # validation
    if not data or not all(k in data for k in ["requestId", "tripEvent", "tripNo", "routeNo", "scheduleNo", "timestamp"]):
        return jsonify({"status": "error", "message": "Invalid request payload"}), 400
    # process the data here
    
    return jsonify({"status": "success", "data": data["requestId"]})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
