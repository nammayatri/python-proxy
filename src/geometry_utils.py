"""
Geometry and distance calculation utilities for GPS tracking.
Contains polyline decoding, distance calculations, and spatial analysis functions.
"""

import math
import polyline as gpolyline
from geopy.distance import geodesic
from typing import List, Tuple, Optional


def decode_polyline(polyline_str: str) -> List[Tuple[float, float]]:
    """Wrapper for polyline library's decoder"""
    if not polyline_str:
        return []
    try:
        return gpolyline.decode(polyline_str)
    except Exception as e:
        print(f"Error decoding polyline: {e}")
        return []


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points 
    using the haversine formula
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r


def is_point_near_polyline(point_lat: float, point_lon: float, polyline_points: List[Tuple[float, float]], 
                          max_distance_meter: float = 50) -> Tuple[bool, float, Optional[int]]:
    """
    Simpler function to check if a point is within max_distance_meter of any 
    segment of the polyline.
    
    Returns:
        Tuple of (is_near, min_distance, min_segment_index)
    """
    if not polyline_points or len(polyline_points) < 2:
        return False, float('inf'), None
        
    min_distance = float('inf')
    min_segment = None
    
    # Check each segment of the polyline
    for i in range(len(polyline_points) - 1):
        # Start and end points of current segment
        p1_lat, p1_lon = polyline_points[i]
        p2_lat, p2_lon = polyline_points[i + 1]
        
        # Calculate distance to this segment using a simple approximation
        # For short segments, this is reasonable and much simpler
        
        # Calculate distances to segment endpoints
        d1 = calculate_distance(point_lat, point_lon, p1_lat, p1_lon)
        d2 = calculate_distance(point_lat, point_lon, p2_lat, p2_lon)
        
        # Calculate length of segment
        segment_length = calculate_distance(p1_lat, p1_lon, p2_lat, p2_lon)
        
        # Use the simplified distance formula (works well for short segments)
        if segment_length > 0:
            # Projection calculation
            # Vector from p1 to p2
            v1x = p2_lon - p1_lon
            v1y = p2_lat - p1_lat
            
            # Vector from p1 to point
            v2x = point_lon - p1_lon
            v2y = point_lat - p1_lat
            
            # Dot product
            dot = v1x * v2x + v1y * v2y
            
            # Squared length of segment
            len_sq = v1x * v1x + v1y * v1y
            
            # Projection parameter (t)
            t = max(0, min(1, dot / len_sq))
            
            # Projected point
            proj_x = p1_lon + t * v1x
            proj_y = p1_lat + t * v1y
            
            # Distance to projection
            distance = calculate_distance(point_lat, point_lon, proj_y, proj_x)
        else:
            # If segment is very short, just use distance to p1
            distance = d1
            
        # Update minimum distance
        if distance < min_distance:
            min_segment = i
            min_distance = distance
            
    # Check if within threshold (convert meters to kilometers)
    max_distance_km = max_distance_meter / 1000
    return min_distance <= max_distance_km, min_distance, min_segment


def check_if_crossed_stop(prev_location: Tuple[float, float], current_location: Tuple[float, float], 
                         stop_location: Tuple[float, float], threshold_meters: float = 20) -> bool:
    """
    Check if a vehicle has crossed a stop between its previous and current location.
    
    This function determines if a stop was passed by checking if the stop is near 
    the path between the vehicle's previous and current positions.
    
    Args:
        prev_location: (lat, lon) of previous vehicle location
        current_location: (lat, lon) of current vehicle location
        stop_location: (lat, lon) of the stop
        threshold_meters: Maximum distance in meters from the path to consider the stop crossed
        
    Returns:
        bool: True if the stop was crossed, False otherwise
    """
    # If any of the locations are None, return False
    if any(loc is None for loc in [prev_location, current_location, stop_location]):
        return False
    
    # 1. First check: Is the stop close enough to either the current or previous position?
    # This handles the case where the vehicle might have temporarily stopped at the bus stop
    dist_to_prev = geodesic(prev_location, stop_location).meters
    dist_to_curr = geodesic(current_location, stop_location).meters
    
    if dist_to_prev < threshold_meters or dist_to_curr < threshold_meters:
        return True
    
    path_distance = geodesic(prev_location, current_location).meters
    
    if path_distance < 5:  # 5 meters threshold for significant movement
        return False
    
    # Calculate distances from prev to stop and from stop to current
    dist_prev_to_stop = geodesic(prev_location, stop_location).meters
    dist_stop_to_curr = geodesic(stop_location, current_location).meters
    
    # Check if the stop is roughly on the path (within reasonable error margin)
    # due to GPS inaccuracy and road curvature
    is_on_path = abs(dist_prev_to_stop + dist_stop_to_curr - path_distance) < threshold_meters
    
    # 3. Third check: Direction verification
    # We need to verify the vehicle is moving toward the stop and then away from it
    
    # Calculate bearings
    def calculate_bearing(point1: Tuple[float, float], point2: Tuple[float, float]) -> float:
        """Calculate the bearing between two points."""
        lat1, lon1 = math.radians(point1[0]), math.radians(point1[1])
        lat2, lon2 = math.radians(point2[0]), math.radians(point2[1])
        
        dlon = lon2 - lon1
        
        y = math.sin(dlon) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        
        bearing = math.atan2(y, x)
        # Convert to degrees
        bearing = math.degrees(bearing)
        # Normalize to 0-360
        bearing = (bearing + 360) % 360
        
        return bearing
    
    # Get bearings
    bearing_prev_to_curr = calculate_bearing(prev_location, current_location)
    bearing_prev_to_stop = calculate_bearing(prev_location, stop_location)
    bearing_stop_to_curr = calculate_bearing(stop_location, current_location)
    
    # Check if the bearings are roughly aligned
    def angle_diff(a: float, b: float) -> float:
        """Calculate the absolute difference between two angles in degrees."""
        return min(abs(a - b), 360 - abs(a - b))
    
    alignment_prev_to_stop = angle_diff(bearing_prev_to_curr, bearing_prev_to_stop) < 60
    alignment_stop_to_curr = angle_diff(bearing_prev_to_curr, bearing_stop_to_curr) < 60
    
    # 4. Combine all checks:
    # - The stop should be roughly on the path
    # - The bearings should be aligned
    # - The distance from prev to stop and then to curr should be in increasing order of sequence
    return (is_on_path and 
            alignment_prev_to_stop and 
            alignment_stop_to_curr and
            dist_prev_to_stop < path_distance and 
            dist_stop_to_curr < path_distance)