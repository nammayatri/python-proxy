"""
Stop tracking and ETA calculation functionality.
Contains the StopTracker class that handles route stops and ETA calculations.
"""

import json
import logging
import math
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from .cache_utils import get_vehicle_location_history
from .geometry_utils import calculate_distance, check_if_crossed_stop

logger = logging.getLogger('amnex-data-server')


class StopTracker:
    """Handles stop tracking, route information, and ETA calculations"""
    
    def __init__(self, db_engine, redis_client, use_osrm=True, 
                 osrm_url='http://router.project-osrm.org', google_api_key='', 
                 cache_ttl=3600, route_stop_mapping_api_url='', gtfs_id='', 
                 merchant_operating_city_id=''):
        self.db_engine = db_engine
        self.redis_client = redis_client
        self.use_osrm = use_osrm
        self.osrm_url = osrm_url
        self.google_api_key = google_api_key
        self.cache_ttl = cache_ttl
        self.stop_visit_radius = 0.05  # 50 meters in km
        self.route_stop_mapping_api_url = route_stop_mapping_api_url
        self.gtfs_id = gtfs_id
        self.merchant_operating_city_id = merchant_operating_city_id
        print(f"StopTracker initialized with {'OSRM' if use_osrm else 'Google Maps'}")
        
    def get_route_stops(self, route_id: str) -> Dict[str, Any]:
        """Get all stops for a route ordered by sequence, including the route polyline if available"""
        from .models import RoutePolyline
        from sqlalchemy.orm import sessionmaker
        
        cache_key = f"route_stops_info:{route_id}"
        
        # Check cache
        cached = self.redis_client.get(f"simpleCache:{cache_key}")
        if cached:
            return json.loads(cached)
            
        try:
            # Get stops for the route from API
            stops_api_url = f"{self.route_stop_mapping_api_url}/route-stop-mapping/{self.gtfs_id}/route/{route_id}"
            response = requests.get(stops_api_url)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            stops_data = response.json()

            # Get the route polyline from DB
            route_polyline = None
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.db_engine)
            with SessionLocal() as db:
                polyline_info = db.query(RoutePolyline)\
                    .filter(RoutePolyline.route_id == str(route_id), 
                           RoutePolyline.merchant_operating_city_id == self.merchant_operating_city_id)\
                    .first()
                if polyline_info and polyline_info.polyline:
                    route_polyline = polyline_info.polyline
                
            if not stops_data:
                return {
                    'stops': [],
                    'polyline': None
                }
            
            # Format results
            result_stops = [
                {
                    'stop_id': stop['stopCode'],
                    'sequence': stop['sequenceNum'],
                    'name': stop['stopName'],
                    'stop_lat': float(stop['stopPoint']['lat']),
                    'stop_lon': float(stop['stopPoint']['lon'])
                }
                for stop in stops_data
            ]
            result = {
                'stops': result_stops,
                'polyline': route_polyline
            }
            # Cache result
            self.redis_client.setex(f"simpleCache:{cache_key}", 3600, json.dumps(result))
            return result
        except requests.exceptions.RequestException as e:
            print(f"Error fetching route stops or polyline from API for route {route_id}: {e}")
            return {
                'stops': [],
                'polyline': None
            }
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response for route {route_id}: {e}")
            return {
                'stops': [],
                'polyline': None
            }
        except Exception as e:
            print(f"An unexpected error occurred getting stops for route {route_id}: {e}")
            return {
                'stops': [],
                'polyline': None
            }
    
    def get_visited_stops(self, route_id: str, vehicle_id: str) -> List[str]:
        """Get list of stops already visited by this vehicle on this route"""
        visit_key = f"visited_stops:{route_id}:{vehicle_id}"
        try:
            visited_stops = self.redis_client.get(visit_key)
            if visited_stops:
                return json.loads(visited_stops)
            return []
        except Exception as e:
            logger.error(f"Error getting visited stops: {e}")
            return []
    
    def update_visited_stops(self, route_id: str, vehicle_id: str, stop_id: str) -> List[str]:
        """Add a stop to the visited stops list"""
        visit_key = f"visited_stops:{route_id}:{vehicle_id}"
        try:
            visited_stops = self.get_visited_stops(route_id, vehicle_id)
            if stop_id not in visited_stops:
                visited_stops.append(stop_id)
                self.redis_client.setex(
                    visit_key, 
                    7200,  # 2 hour TTL
                    json.dumps(visited_stops)
                )
            return visited_stops
        except Exception as e:
            logger.error(f"Error updating visited stops: {e}")
            return []
    
    def reset_visited_stops(self, route_id: str, vehicle_id: str, vehicle_no: str) -> bool:
        """Reset the visited stops list for a vehicle"""
        visit_key = f"visited_stops:{route_id}:{vehicle_id}"
        history_key = f"vehicle_history:{vehicle_no}"
        try:
            self.redis_client.delete(visit_key)
            self.redis_client.delete(history_key)
            logger.info(f"Reset visited stops for vehicle {vehicle_id} on route {route_id}")
            return True
        except Exception as e:
            logger.error(f"Error resetting visited stops: {e}")
            return False
    
    def check_if_at_stop(self, stop: Dict[str, Any], vehicle_lat: float, vehicle_lon: float) -> Tuple[bool, float]:
        """Check if vehicle is within radius of a stop"""
        # Calculate distance using haversine formula
        lat1, lon1 = math.radians(vehicle_lat), math.radians(vehicle_lon)
        lat2, lon2 = math.radians(float(stop['stop_lat'])), math.radians(float(stop['stop_lon']))
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        distance = 6371 * c  # Radius of earth in kilometers
        
        return distance <= self.stop_visit_radius, distance
    
    def find_next_stop(self, stops: List[Dict[str, Any]], visited_stops: List[str], 
                      vehicle_lat: float, vehicle_lon: float) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
        """Find the next stop in sequence after the last visited stop"""
        if not visited_stops:
            # If no stops visited yet, find the nearest stop as the next stop
            nearest_stop = None
            min_distance = float('inf')
            for stop in stops:
                is_at_stop, distance = self.check_if_at_stop(stop, vehicle_lat, vehicle_lon)
                if distance < min_distance:
                    min_distance = distance
                    nearest_stop = stop
            return (nearest_stop, min_distance)
        
        # Get the last visited stop ID
        last_visited_id = visited_stops[-1]
        
        # Find its index in the stops list
        last_index = -1
        for i, stop in enumerate(stops):
            if stop['stop_id'] == last_visited_id:
                last_index = i
                break
                
        # If we found the last stop and it's not the last in the route
        if last_index >= 0 and last_index < len(stops) - 1:
            return (stops[last_index + 1], None)
        elif last_index == len(stops) - 1:
            # We're at the last stop of the route
            return (None, None)
            
        # If we couldn't find the last visited stop in the list
        # (this shouldn't happen but just in case)
        return (stops[0] if stops else None, None)
    
    def find_closest_stop(self, stops: List[Dict[str, Any]], vehicle_lat: float, 
                         vehicle_lon: float) -> Tuple[Optional[Dict[str, Any]], float]:
        """Find the closest stop to the given coordinates"""
        if not stops:
            return None, float('inf')
            
        closest_stop = None
        min_distance = float('inf')
        
        for stop in stops:
            # Calculate distance using haversine formula
            lat1, lon1 = math.radians(vehicle_lat), math.radians(vehicle_lon)
            lat2, lon2 = math.radians(float(stop['stop_lat'])), math.radians(float(stop['stop_lon']))
            
            # Haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            distance = 6371 * c  # Radius of earth in kilometers
            
            if distance < min_distance:
                min_distance = distance
                closest_stop = stop
                
        return closest_stop, min_distance
    
    def get_travel_duration(self, origin_id: str, dest_id: str, origin_lat: float, origin_lon: float, 
                           dest_lat: float, dest_lon: float) -> Optional[float]:
        """Get travel duration between two stops with caching"""
        # Try to get from cache
        cache_key = f"route_segment:{origin_id}:{dest_id}"
        try:
            if origin_id != 0:
                cached = self.redis_client.get(cache_key)
                if cached:
                    data = json.loads(cached)
                    return data.get('duration')
        except Exception as e:
            print(f"Redis error: {e}")
        
        # Not in cache, calculate using routing API
        try:
            duration = None
            # Fallback to simple estimation (30 km/h)
            # Calculate distance using haversine
            lat1, lon1 = math.radians(origin_lat), math.radians(origin_lon)
            lat2, lon2 = math.radians(dest_lat), math.radians(dest_lon)
            
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            distance = 6371000 * c  # Radius of earth in meters
            
            # Estimate duration: distance / speed (30 km/h = 8.33 m/s)
            duration = distance / 8.33
            
            # Cache the fallback estimation
            cache_data = {
                'duration': duration,
                'timestamp': datetime.now().isoformat(),
                'estimated': True
            }
            if origin_id != 0:
                self.redis_client.setex(cache_key, self.cache_ttl, json.dumps(cache_data))
            
            return duration
        except Exception as e:
            print(f"Error calculating travel duration: {e}")
            return None
        
    def calculate_eta(self, stops_info: Dict[str, Any], route_id: str, vehicle_lat: float, 
                     vehicle_lon: float, current_time: datetime, vehicle_id: str, 
                     visited_stops: List[str] = None, vehicle_no: str = None) -> Optional[Dict[str, Any]]:
        """Calculate ETA for all upcoming stops from current position"""
        if visited_stops is None:
            visited_stops = []
            
        # Get all stops for the route
        stops = stops_info.get('stops')
        if not stops:
            return None
            
        next_stop = None
        closest_stop = None
        distance = float('inf')
        calculation_method = "realtime"
        
        # Check if the vehicle is at a stop now
        for stop in stops:
            # Check if vehicle is at the stop based on current position
            is_at_stop, _ = self.check_if_at_stop(stop, vehicle_lat, vehicle_lon)
            
            # Get the vehicle's previous location from history                
            # Check if we crossed the stop between last position and current position
            if not is_at_stop:
                location_history = get_vehicle_location_history(self.redis_client, vehicle_no)
                if len(location_history) > 0:
                    last_point = location_history[-1]  # Most recent point in history
                    # Check if the stop is between the last point and current point
                    crossed_stop = check_if_crossed_stop( 
                        (last_point['lat'], last_point['lon']),
                        (vehicle_lat, vehicle_lon),
                        (float(stop['stop_lat']), float(stop['stop_lon']))
                    )
                    if crossed_stop:
                        is_at_stop = True
            
            if is_at_stop:
                # Vehicle is at this stop
                if stop['stop_id'] not in visited_stops:
                    # Add to visited stops if not already there
                    self.update_visited_stops(route_id, vehicle_id, stop['stop_id'])
                    visited_stops.append(stop['stop_id'])
                    calculation_method = "visited_stops"
                break
                    
        # Find next stop based on visited stops
        (next_stop, distance) = self.find_next_stop(stops, visited_stops, vehicle_lat, vehicle_lon)
        if next_stop:
            if not distance:
                _, distance = self.check_if_at_stop(next_stop, vehicle_lat, vehicle_lon)
            closest_stop = next_stop
            calculation_method = "sequence_based"
        else:
            # We're at the end of the route, reset visited stops
            self.reset_visited_stops(route_id, vehicle_id, vehicle_no)
            # Fall back to closest stop method
            closest_stop, distance = self.find_closest_stop(stops, vehicle_lat, vehicle_lon)
            calculation_method = "distance_based_fallback"
            
        if not closest_stop:
            return None
            
        # Find the index of the closest/next stop in the route
        closest_index = -1
        for i, stop in enumerate(stops):
            if stop['stop_id'] == closest_stop['stop_id']:
                closest_index = i
                break
                
        if closest_index == -1:
            # Something went wrong, stop not found in the list
            return None
            
        # Calculate ETAs for the closest stop and all upcoming stops
        eta_list = []
        cumulative_time = 0
        current_lat, current_lon = vehicle_lat, vehicle_lon
        
        # First, calculate ETA for the closest/next stop
        if distance <= 0.01:  # 10 meters in km - we're practically at the stop
            arrival_time = current_time
            calculation_method = "immediate"
        else:
            # Calculate time to reach the stop
            duration = self.get_travel_duration(
                0, closest_stop['stop_id'],
                current_lat, current_lon,
                closest_stop['stop_lat'], closest_stop['stop_lon']
            )
            
            if duration:
                arrival_time = current_time + timedelta(seconds=duration)
                cumulative_time = duration
                calculation_method = "estimated"
            else:
                # Fallback estimation
                duration = distance / 8.33  # distance / (30 km/h in m/s)
                arrival_time = current_time + timedelta(seconds=duration)
                cumulative_time = duration
                calculation_method = "estimated"
        
        # Add closest/next stop to the ETA list
        eta_list.append({
            'stop_id': closest_stop['stop_id'],
            'stop_seq': closest_stop['sequence'],
            'stop_name': closest_stop['name'],
            'stop_lat': closest_stop['stop_lat'],
            'stop_lon': closest_stop['stop_lon'],
            'arrival_time': int(arrival_time.timestamp()),
            'calculation_method': calculation_method
        })
        
        # Then calculate ETAs for all remaining stops (everything after closest_index)
        for i in range(closest_index + 1, len(stops)):
            prev_stop = stops[i-1]
            current_stop = stops[i]
            
            # Calculate duration between stops
            duration = self.get_travel_duration(
                prev_stop['stop_id'], current_stop['stop_id'],
                prev_stop['stop_lat'], prev_stop['stop_lon'],
                current_stop['stop_lat'], current_stop['stop_lon']
            )
            
            if duration:
                cumulative_time += duration
                arrival_time = current_time + timedelta(seconds=cumulative_time)
                
                calculation_method = "estimated"
                
                eta_list.append({
                    'stop_id': current_stop['stop_id'],
                    'stop_seq': current_stop['sequence'],
                    'stop_name': current_stop['name'],
                    'stop_lat': current_stop['stop_lat'],
                    'stop_lon': current_stop['stop_lon'],
                    'arrival_time': int(arrival_time.timestamp()),
                    'calculation_method': calculation_method
                })
            else:
                # If we couldn't calculate duration, use estimated method
                calculation_method = "estimated"
        
        return {
            'route_id': route_id,
            'current_time': int(current_time.timestamp()),
            'closest_stop': {
                'stop_id': closest_stop['stop_id'],
                'stop_name': closest_stop['name'],
                'distance': distance
            },
            'calculation_method': calculation_method,
            'eta': eta_list
        }