"""
Route matching algorithm and related components.
Contains the main route matching logic and helper functions.
"""

import json
import logging
import traceback
from typing import List, Dict, Any, Optional

from .geometry_utils import decode_polyline, is_point_near_polyline

logger = logging.getLogger('amnex-data-server')


def calculate_route_match_score(route_id: str, vehicle_no: str, stops: dict, 
                              vehicle_points: List[dict], max_distance_meter: float = 100) -> float:
    """
    Calculate how well a route matches a series of vehicle_points, considering direction.
    Uses polyline for more accurate route matching when available.
    Returns a score between 0 and 1, where 1 is a perfect match.
    """
    try:
        # Check if stops is a dict with polyline and stops keys
        if isinstance(stops, dict) and 'stops' in stops and 'polyline' in stops:
            route_polyline = stops.get('polyline')
            polyline_points = decode_polyline(route_polyline)
            min_points_required = 4
        else:
            route_polyline = ""
            stops_info = stops.get('stops')
            polyline_points = list(map(lambda x: (x['stop_lat'], x['stop_lon']), stops_info))
            min_points_required = 10

        if not vehicle_points or len(vehicle_points) < min_points_required:
            return 0.0

        # Sort vehicle_points by timestamp to ensure they're in chronological order
        vehicle_points = sorted(vehicle_points, key=lambda x: x.get('timestamp', 0))
        
        if polyline_points:
            # Count how many vehicle_points are near the polyline
            near_points = []
            total_distance = 0.0
            
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
                        total_distance += distance
                except (KeyError, ValueError, TypeError) as e:
                    logger.debug(f"Error checking if point is near polyline: {e}, point: {point}")
                    continue
            
            # Calculate proximity score (0-1)
            proximity_ratio = len(near_points) / len(vehicle_points) if len(vehicle_points) > 0 else 0
            
            # Only proceed if enough vehicle_points are near the polyline
            if proximity_ratio >= 0.3:
                # Convert set to list and sort to check direction
                if len(min_segments_list) >= 2 and min(min_segments_list) == min_segments_list[0]:
                    print(f"Route ID: {vehicle_no} {len(near_points)}/{len(vehicle_points)}, Score: {proximity_ratio:.2f}")
                    return proximity_ratio
            return 0.0
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error calculating route match score: {stops} {e}\nTraceback: {error_details}")
        return 0.0


def get_route_ids_from_waybills(waybills_session_local, vehicle_no: str, current_lat: float = None, 
                               current_lon: float = None, timestamp: int = None, 
                               provider: str = None, stop_tracker=None) -> List[str]:
    """Get the route_id from waybills database for a given vehicle number"""
    from .models import Waybill, BusScheduleTripDetail
    from .cache_utils import store_vehicle_location_history, get_vehicle_location_history
    
    try:
        with waybills_session_local() as db:
            # First get the active waybill for the vehicle
            waybill = db.query(Waybill)\
                .filter(
                    Waybill.vehicle_no == vehicle_no,
                    Waybill.deleted == False,
                    Waybill.status == 'Online'
                )\
                .order_by(Waybill.updated_at.desc())\
                .first()
            
            if not waybill:
                return []
                
            if current_lat is not None and current_lon is not None and stop_tracker:
                store_vehicle_location_history(stop_tracker.redis_client, vehicle_no, current_lat, current_lon, timestamp)
            
            # Add current location to history if provided
            location_history = get_vehicle_location_history(stop_tracker.redis_client, vehicle_no) if stop_tracker else []
            if len(location_history) < 5:
                return []

            # Then get all possible routes from bus_schedule
            schedules = db.query(BusScheduleTripDetail)\
                .filter(
                    BusScheduleTripDetail.schedule_trip_id == waybill.schedule_trip_id,
                    BusScheduleTripDetail.deleted == False
                )\
                .all()  # Execute the query to get results
            
            if len(schedules) == 0:
                return []
            
            print(f"Route ID: Bus schedule len {len(schedules)}")

            best_route_ids = []
            routes_match_score = {}
            for schedule in schedules:
                if schedule.route_number_id not in routes_match_score:
                    route_stops = stop_tracker.get_route_stops(str(schedule.route_number_id))
                    # Calculate match score using location history
                    score = calculate_route_match_score(schedule.route_number_id, vehicle_no, route_stops, location_history)
                    # Ensure score is not None
                    if score is None:
                        score = 0.0
                    print(f"Route ID: Bus score {vehicle_no} Score for route {schedule.route_number_id}: {score} (Provider: {provider})")
                    if score > 0.8:
                        best_route_ids.append(schedule.route_number_id)
                    routes_match_score[schedule.route_number_id] = score
            return best_route_ids
            
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error querying waybills database for vehicle {vehicle_no} (Provider: {provider}): {e}\nTraceback: {error_details}")
        return []