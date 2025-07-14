import math
import json
from typing import List, Tuple, Dict, Any, Optional

try:
    import polyline as gpolyline
except ImportError:
    gpolyline = None


def decode_polyline(polyline_str: str) -> List[Tuple[float, float]]:
    """Wrapper for polyline library's decoder"""
    if not polyline_str or not gpolyline:
        return []
    try:
        return gpolyline.decode(polyline_str)
    except Exception as e:
        print(f"Error decoding polyline: {e}")
        return []

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points using the haversine formula (in kilometers)
    """
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371
    return c * r

def is_point_near_polyline(point_lat: float, point_lon: float, polyline_points: List[Tuple[float, float]], max_distance_meter: float = 50) -> Tuple[bool, float, Optional[int]]:
    """
    Check if a point is within max_distance_meter of any segment of the polyline.
    Returns (is_near, min_distance, min_segment_index)
    """
    if not polyline_points or len(polyline_points) < 2:
        return False, float('inf'), None
    min_distance = float('inf')
    min_segment = None
    for i in range(len(polyline_points) - 1):
        p1_lat, p1_lon = polyline_points[i]
        p2_lat, p2_lon = polyline_points[i + 1]
        d1 = calculate_distance(point_lat, point_lon, p1_lat, p1_lon)
        d2 = calculate_distance(point_lat, point_lon, p2_lat, p2_lon)
        segment_length = calculate_distance(p1_lat, p1_lon, p2_lat, p2_lon)
        if segment_length > 0:
            v1x = p2_lon - p1_lon
            v1y = p2_lat - p1_lat
            v2x = point_lon - p1_lon
            v2y = point_lat - p1_lat
            dot = v1x * v2x + v1y * v2y
            len_sq = v1x * v1x + v1y * v1y
            t = max(0, min(1, dot / len_sq))
            proj_x = p1_lon + t * v1x
            proj_y = p1_lat + t * v1y
            distance = calculate_distance(point_lat, point_lon, proj_y, proj_x)
        else:
            distance = d1
        if distance < min_distance:
            min_segment = i
            min_distance = distance
    max_distance_km = max_distance_meter / 1000
    return min_distance <= max_distance_km, min_distance, min_segment

def calculate_route_match_score(
    route_id: Any,
    vehicle_no: Any,
    stops: Dict[str, Any],
    vehicle_points: List[Dict[str, Any]],
    max_distance_meter: float = 100,
    debug: bool = False
) -> float:
    """
    Calculate how well a route matches a series of vehicle_points, considering direction.
    Uses polyline for more accurate route matching when available.
    Returns a score between 0 and 1, where 1 is a perfect match.
    """
    try:
        if isinstance(stops, dict) and 'stops' in stops and 'polyline' in stops:
            route_polyline = stops.get('polyline')
            polyline_points = decode_polyline(route_polyline)
            min_points_required = 4
        else:
            route_polyline = ""
            stopsInfo = stops.get('stops')
            polyline_points = list(map(lambda x: (x['stop_lat'], x['stop_lon']), stopsInfo))
            min_points_required = 10
        if not vehicle_points or len(vehicle_points) < min_points_required:
            if debug:
                print(f"Not enough vehicle points: {len(vehicle_points)} < {min_points_required}")
            return 0.0
        vehicle_points = sorted(vehicle_points, key=lambda x: x.get('timestamp', 0))
        if polyline_points:
            near_points = []
            min_segments_list = []
            for point in vehicle_points:
                try:
                    is_near, distance, min_segment_start = is_point_near_polyline(
                        point['lat'], point['lon'], polyline_points, max_distance_meter
                    )
                    if is_near:
                        if min_segment_start is not None:
                            min_segments_list.append(min_segment_start)
                        near_points.append(point)
                        if debug:
                            print(f"Point {point} is near polyline (distance {distance:.2f} km, segment {min_segment_start})")
                    elif debug:
                        print(f"Point {point} is NOT near polyline (distance {distance:.2f} km)")
                except (KeyError, ValueError, TypeError) as e:
                    if debug:
                        print(f"Error checking if point is near polyline: {e}, point: {point}")
                    continue
            proximity_ratio = len(near_points) / len(vehicle_points) if len(vehicle_points) > 0 else 0
            if debug:
                print(f"Proximity ratio: {proximity_ratio:.2f} ({len(near_points)}/{len(vehicle_points)})")
            if proximity_ratio >= 0.0:
                if len(min_segments_list) >= 2 and min(min_segments_list) == min_segments_list[0]:
                    if debug:
                        print(f"Route ID: {vehicle_no} {len(near_points)}/{len(vehicle_points)}, Score: {proximity_ratio:.2f}")
                    return proximity_ratio
            if debug:
                print("Route match failed: not enough points near polyline or direction check failed.")
            return 0.0
    except Exception as e:
        if debug:
            import traceback
            print(f"Error calculating route match score: {stops} {e}\nTraceback: {traceback.format_exc()}")
        return 0.0

if __name__ == "__main__":
    import argparse
    import csv
    import json
    from datetime import datetime
    try:
        from dateutil import parser as dateutil_parser
    except ImportError:
        dateutil_parser = None
    parser = argparse.ArgumentParser(description="Route Matcher Standalone Tool")
    parser.add_argument('--gps', type=str, required=True, help='Path to GPS CSV file')
    parser.add_argument('--routegeojson', type=str, required=True, help='Path to route GeoJSON file (with polyline and stops)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logs')
    args = parser.parse_args()

    # Load GPS points
    gps_points = []
    with open(args.gps, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse ISO8601 timestamp to Unix time (seconds since epoch)
            ts = row['timestamp']
            if ts:
                try:
                    if dateutil_parser:
                        dt = dateutil_parser.parse(ts)
                    else:
                        try:
                            dt = datetime.fromisoformat(ts)
                        except Exception:
                            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
                    unix_ts = int(dt.timestamp())
                except Exception:
                    unix_ts = 0
            else:
                unix_ts = 0
            gps_points.append({
                'lat': float(row['lat']),
                'lon': float(row['long']),
                'timestamp': unix_ts
            })

    # Load route polyline and stops from GeoJSON
    # Expected: one LineString feature for the route polyline, Point features for stops
    with open(args.routegeojson, 'r') as f:
        geojson = json.load(f)
    polyline_points = None
    stops = []
    for feature in geojson['features']:
        geom = feature.get('geometry', {})
        props = feature.get('properties', {})
        if geom.get('type') == 'LineString':
            # Polyline: coordinates are [lon, lat] pairs
            polyline_points = [(lat, lon) for lon, lat in geom['coordinates']]
        elif geom.get('type') == 'Point':
            lon, lat = geom['coordinates']
            stop = {
                'stop_lat': lat,
                'stop_lon': lon,
                'stop_id': props.get('Stop Code') or props.get('Stop ID') or '',
                'name': props.get('Stop Name') or '',
                'sequence': int(props.get('Stage No') or props.get('Sequence') or 0)
            }
            stops.append(stop)
    stops_dict = {'stops': stops, 'polyline': None}
    if polyline_points:
        # Encode polyline_points to a polyline string if polyline lib is available
        if gpolyline:
            stops_dict['polyline'] = gpolyline.encode([(s['stop_lat'], s['stop_lon']) for s in stops])
        else:
            # Or just pass the points directly (decode_polyline will handle None)
            stops_dict['polyline'] = None
    score = calculate_route_match_score(
        route_id='standalone',
        vehicle_no='standalone',
        stops=stops_dict,
        vehicle_points=gps_points,
        debug=args.debug
    )
    print(f"Route match score: {score:.2f}")