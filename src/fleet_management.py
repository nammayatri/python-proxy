"""
Fleet management and device mapping functionality.
Contains fleet information retrieval and device-vehicle mapping logic.
"""

import json
import logging
from typing import List, Optional

from .cache_utils import store_vehicle_location_history
from .models import FleetInfo
from .route_matching import get_route_ids_from_waybills

logger = logging.getLogger('amnex-data-server')


def get_fleet_info(redis_client, device_vehicle_map: dict, waybills_session_local, 
                  device_id: str, current_lat: float = None, current_lon: float = None, 
                  timestamp: int = None, provider: str = None, stop_tracker=None,
                  bus_location_max_age: int = 120, bus_cleanup_interval: int = 180) -> List[FleetInfo]:
    """Get both fleet number and route ID for a device"""
    cache_key = f"fleetInfo:{device_id}"
    cache_key_saved = cache_key + ":saved"

    fleet_mapping_values = []  # response values
    
    # Check cache first
    fleet_info_str = redis_client.get(cache_key)
    if fleet_info_str is not None:
        fleet_infos_data = json.loads(fleet_info_str)
        fleet_infos = [FleetInfo(**fleet_info) for fleet_info in fleet_infos_data]
        for fleet_info in fleet_infos:
            if current_lat is not None and current_lon is not None:
                store_vehicle_location_history(redis_client, fleet_info.vehicle_no, current_lat, current_lon, timestamp)
        return fleet_infos
    
    try:
        vehicle_no = device_vehicle_map.get(device_id)
        if not vehicle_no:
            return []

        # Get route for fleet
        route_ids = get_route_ids_from_waybills(waybills_session_local, vehicle_no, current_lat, current_lon, timestamp, provider, stop_tracker)
        for route_id in route_ids:
            fleet_info = FleetInfo(
                vehicle_no=vehicle_no,
                device_id=device_id,
                route_id=route_id
            )
            try:
                fleet_info_saved = redis_client.get(cache_key_saved)
                if fleet_info_saved is not None:
                    fleet_info_saved = json.loads(fleet_info_saved)
                    print("going to delete route info")
                    if ('route_id' in fleet_info_saved and 
                        fleet_info_saved['route_id'] is not None and 
                        route_id != fleet_info_saved['route_id']):
                        route_key = "route:" + fleet_info_saved['route_id']
                        clean_redis_key_for_route_info(redis_client, fleet_info_saved['route_id'], route_key, bus_location_max_age)
            except Exception as e:
                logger.error(f"Error cleaning redis key for route info: {e}")
            fleet_mapping_values.append(fleet_info)
        
        if len(route_ids) > 0:
            # Convert FleetInfo objects to dicts for JSON serialization to Redis
            fleet_mapping_dicts = [fleet_info.model_dump() for fleet_info in fleet_mapping_values]
            redis_client.setex(cache_key_saved, bus_location_max_age + bus_cleanup_interval, json.dumps(fleet_mapping_dicts))  # hack for cleanup if route changes
            redis_client.setex(cache_key, bus_cleanup_interval, json.dumps(fleet_mapping_dicts))
        return fleet_mapping_values
    except Exception as e:
        print(f"Error querying fleet info for device {device_id}: {e}")
        return fleet_mapping_values


def clean_redis_key_for_route_info(redis_client, prod_redis_client, route_id: str, redis_key: str, 
                                  bus_location_max_age: int) -> int:
    """Clean outdated vehicle data from a specific route key"""
    import time
    
    current_time = int(time.time())
    prod_vehicle_data = prod_redis_client.hgetall(redis_key)
    vehicle_data = redis_client.hgetall(redis_key)
    
    # Merge prod_vehicle_data and vehicle_data so that all vehicles from both are considered.
    # If a vehicle_id exists in both, prefer the one from prod_vehicle_data.
    merged_vehicle_data = dict(vehicle_data) if vehicle_data else {}
    if prod_vehicle_data:
        merged_vehicle_data.update(prod_vehicle_data)
    vehicle_data = merged_vehicle_data
    
    if not vehicle_data:
        return 0
    
    vehicles_to_remove = []
    removed_count = 0
    
    # Check each vehicle's timestamp
    for vehicle_id, data_json in merged_vehicle_data.items():
        try:
            data = json.loads(data_json)
            # First check serverTime if available
            if 'serverTime' in data:
                timestamp = data.get('serverTime')
            # Otherwise use timestamp
            else:
                timestamp = data.get('timestamp')
            
            # If no valid timestamp, skip
            if not timestamp:
                continue
                
            age = current_time - int(timestamp)
            print("Error age", vehicle_id, route_id, age, current_time, int(timestamp), current_time - int(timestamp))
            
            # If older than threshold, mark for removal
            if age > bus_location_max_age:
                vehicles_to_remove.append(vehicle_id)
                logger.debug(f"Vehicle {vehicle_id} on route {route_id} outdated by {age}s, marking for removal")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.error(f"Error parsing data for vehicle {vehicle_id}: {e}")
            # Mark invalid entries for removal
            vehicles_to_remove.append(vehicle_id)
    
    # Remove outdated vehicles
    if vehicles_to_remove:
        redis_client.hdel(redis_key, *vehicles_to_remove)
        prod_redis_client.hdel(redis_key, *vehicles_to_remove)
        removed_count = len(vehicles_to_remove)
        logger.info(f"Removed {removed_count} outdated vehicles from route {route_id}")
    
    return removed_count


def load_device_vehicle_mappings(session_local) -> dict:
    """Load device to vehicle mappings from database"""
    from .models import DeviceVehicleMapping
    
    device_vehicle_map = {}
    try:
        with session_local() as db:
            mappings = db.query(DeviceVehicleMapping).all()
            for mapping in mappings:
                device_vehicle_map[mapping.device_id] = mapping.vehicle_no
        logger.info(f"Loaded {len(device_vehicle_map)} device-vehicle mappings at startup.")
    except Exception as e:
        logger.error(f"Error loading device-vehicle mappings at startup: {e}")
    
    return device_vehicle_map