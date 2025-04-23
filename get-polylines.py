#!/usr/bin/env python3
import pandas as pd
import numpy as np
import json
import polyline
import sqlite3
from sklearn.cluster import DBSCAN
from datetime import datetime, timedelta
from collections import defaultdict
import os
import requests
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points
from haversine import haversine
import logging
import itertools
from functools import lru_cache
import clickhouse_driver

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RoutePolylineGenerator:
    def __init__(self, date='2025-04-03', output_dir='route_polylines'):
        """Initialize the route polyline generator.
        
        Args:
            date (str): The date for which to generate route polylines in YYYY-MM-DD format
            output_dir (str): Directory to save the output GeoJSON files
        """
        self.date = date
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Load the route-stop mapping
        self.route_stops = self._load_route_stops()
        
        # Load the vehicle-device mapping
        self.vehicle_device_map = self._load_vehicle_device_mapping()
        
        # Fetch vehicle-route mapping once at initialization
        self.vehicle_route_map = {}
        
        # Cache for GPS data
        self.gps_data_cache = {}
        
        # Cache for cleaned GPS data
        self.cleaned_gps_cache = {}
        
        # For route-to-vehicles lookup
        self.route_to_vehicles = defaultdict(list)
        
        # DB connection parameters - need to be set
        self.clickhouse_conn_params = {
            'host': 'your_clickhouse_host',
            'port': 'your_clickhouse_port',
            'user': 'your_username',
            'password': 'your_password',
            'database': 'atlas_kafka'
        }
        
        # ClickHouse client
        self.clickhouse_client = None
        
        # Initialize ClickHouse client
        self._initialize_clickhouse_client()
        
        # Initialize vehicle and route mappings
        self._initialize_vehicle_route_mapping()
    
    def _initialize_clickhouse_client(self):
        """Initialize ClickHouse client connection."""
        try:
            self.clickhouse_client = clickhouse_driver.Client(
                host=self.clickhouse_conn_params['host'],
                port=self.clickhouse_conn_params['port'],
                user=self.clickhouse_conn_params['user'],
                password=self.clickhouse_conn_params['password'],
                database=self.clickhouse_conn_params['database']
            )
            logger.info("Successfully initialized ClickHouse client.")
        except Exception as e:
            logger.error(f"Error initializing ClickHouse client: {e}")
            self.clickhouse_client = None

    def _load_route_stops(self):
        """Load route stops from CSV file."""
        logger.info("Loading route-stop mapping from CSV...")
        try:
            df = pd.read_csv('route-stop-mapping.csv')
            # Group by route ID and create a dictionary of stops
            route_stops = {}
            for route_id, group in df.groupby('Route ID'):
                # Sort by sequence
                sorted_stops = group.sort_values('Sequence')
                stops = []
                for _, row in sorted_stops.iterrows():
                    stops.append({
                        'stop_id': row['Stop ID'],
                        'name': row['Name'],
                        'lat': row['LAT'],
                        'lon': row['LON'],
                        'sequence': row['Sequence']
                    })
                route_stops[route_id] = stops
            logger.info(f"Loaded {len(route_stops)} routes from the route-stop mapping.")
            return route_stops
        except Exception as e:
            logger.error(f"Error loading route-stop mapping: {e}")
            return {}

    def _load_vehicle_device_mapping(self):
        """Load vehicle to device mapping from CSV file."""
        logger.info("Loading vehicle-device mapping from CSV...")
        try:
            df = pd.read_csv('vehicle_device_mapping.csv')
            # Convert to dictionary
            mapping = dict(zip(df['vehicle_no'], df['device_id']))
            logger.info(f"Loaded {len(mapping)} vehicle-device mappings.")
            return mapping
        except Exception as e:
            logger.error(f"Error loading vehicle-device mapping: {e}")
            return {}

    def _initialize_vehicle_route_mapping(self):
        """Load vehicle to route mapping from CSV file."""
        logger.info(f"Loading vehicle-route mapping from CSV...")
        try:
            df = pd.read_csv('vehicle_route_mapping.csv')
            
            # Filter by date if needed
            # if 'date' column exists in the CSV, uncomment the line below
            # df = df[df['date'] == self.date]
            
            # Create mappings
            for _, row in df.iterrows():
                vehicle_no = row['vehicle_no']
                route_id = row['route_id']
                self.vehicle_route_map[vehicle_no] = route_id
                self.route_to_vehicles[route_id].append(vehicle_no)
            
            logger.info(f"Found {len(self.vehicle_route_map)} vehicle-route mappings for {len(self.route_to_vehicles)} routes.")
            
            # Pre-calculate device IDs for each route
            self.route_to_devices = {}
            for route_id, vehicles in self.route_to_vehicles.items():
                device_ids = [self.vehicle_device_map.get(v) for v in vehicles]
                self.route_to_devices[route_id] = [d for d in device_ids if d]
                
        except Exception as e:
            logger.error(f"Error loading vehicle-route mapping: {e}")

    def _get_gps_data_batch(self, all_device_ids):
        """Get GPS data for all device IDs in a single query."""
        if not all_device_ids:
            logger.warning("No device IDs provided for GPS data retrieval.")
            return pd.DataFrame()
            
        if not self.clickhouse_client:
            logger.error("ClickHouse client not initialized.")
            return pd.DataFrame()
            
        logger.info(f"Fetching GPS data for {len(all_device_ids)} devices in a batch...")
        try:
            # Construct the query
            device_ids_str = ", ".join(f"'{device_id}'" for device_id in all_device_ids if device_id)
            
            # Construct the query with a limit to avoid memory issues
            query = f"""
            SELECT 
                `atlas_kafka`.`amnex_direct_data`.`lat` AS `lat`, 
                `atlas_kafka`.`amnex_direct_data`.`timestamp` AS `timestamp`, 
                `atlas_kafka`.`amnex_direct_data`.`long` AS `long`, 
                `atlas_kafka`.`amnex_direct_data`.`deviceId` AS `device_id`
            FROM `atlas_kafka`.`amnex_direct_data`
            WHERE 
                (`atlas_kafka`.`amnex_direct_data`.`timestamp` >= parseDateTimeBestEffort('{self.date} 00:00:00.000')) 
                AND (`atlas_kafka`.`amnex_direct_data`.`timestamp` < parseDateTimeBestEffort('{self.date} 23:59:59.999'))
                AND (`atlas_kafka`.`amnex_direct_data`.`deviceId` IN ({device_ids_str}))
            ORDER BY `device_id`, `timestamp`
            """
            
            # Execute query with ClickHouse client
            result = self.clickhouse_client.execute(query, with_column_types=True)
            
            # Parse result data and column names
            data, columns = result
            column_names = [col[0] for col in columns]
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=column_names)
            
            logger.info(f"Retrieved {len(df)} GPS points for {len(all_device_ids)} devices.")
            return df
        except Exception as e:
            logger.error(f"Error fetching GPS data: {e}")
            return pd.DataFrame()

    def _get_gps_data_for_devices(self, device_ids):
        """Get GPS data for specified device IDs, using cache when possible."""
        if not device_ids:
            return pd.DataFrame()
        
        # Create a unique cache key
        cache_key = ','.join(sorted(filter(None, device_ids)))
        
        # Check if already in cache
        if cache_key in self.gps_data_cache:
            return self.gps_data_cache[cache_key]
        
        # Get data for any devices not in cache
        missing_devices = [d for d in device_ids if d and not any(d in k for k in self.gps_data_cache.keys())]
        
        if missing_devices:
            # Fetch data for missing devices
            new_data = self._get_gps_data_batch(missing_devices)
            
            # Add to cache (individual device data)
            for device_id, group in new_data.groupby('device_id'):
                self.gps_data_cache[device_id] = group
        
        # Combine cached data for requested devices
        result_data = pd.concat([self.gps_data_cache[d] for d in device_ids if d and d in self.gps_data_cache])
        
        # Cache the combined result
        self.gps_data_cache[cache_key] = result_data
        
        return result_data

    def _clean_gps_data(self, gps_data):
        """Clean GPS data by removing outliers and invalid points."""
        if gps_data.empty:
            return gps_data
            
        # Generate a cache key based on the data
        cache_key = hash(tuple(gps_data.index.tolist()))
        
        # Check if already in cache
        if cache_key in self.cleaned_gps_cache:
            return self.cleaned_gps_cache[cache_key]
            
        logger.info("Cleaning GPS data...")
        # Convert to numeric
        gps_data['lat'] = pd.to_numeric(gps_data['lat'], errors='coerce')
        gps_data['long'] = pd.to_numeric(gps_data['long'], errors='coerce')
        
        # Remove invalid coordinates
        gps_data = gps_data.dropna(subset=['lat', 'long'])
        
        # Filter out points with impossible coordinates
        gps_data = gps_data[(gps_data['lat'] >= -90) & (gps_data['lat'] <= 90) & 
                            (gps_data['long'] >= -180) & (gps_data['long'] <= 180)]
        
        # Vectorized operations for speed improvements
        device_groups = []
        for device_id, group in gps_data.groupby('device_id'):
            # Sort by timestamp
            group = group.sort_values('timestamp')
            
            # Calculate shifts at once
            group['next_lat'] = group['lat'].shift(-1)
            group['next_long'] = group['long'].shift(-1)
            group['next_timestamp'] = group['timestamp'].shift(-1)
            
            # Calculate time difference in seconds (vectorized)
            if isinstance(group['timestamp'].iloc[0], str):
                group['time_diff'] = (pd.to_datetime(group['next_timestamp']) - 
                                     pd.to_datetime(group['timestamp'])).dt.total_seconds()
            else:
                group['time_diff'] = (group['next_timestamp'] - group['timestamp']).dt.total_seconds()
            
            # Prepare coordinates for haversine (vectorized)
            coords1 = np.column_stack([group['lat'], group['long']])
            coords2 = np.column_stack([group['next_lat'], group['next_long']])
            
            # Apply haversine with broadcasting
            distances = []
            valid_indices = ~np.isnan(coords2[:, 0])
            for i in range(len(coords1)):
                if i < len(coords2) and valid_indices[i]:
                    distances.append(haversine((coords1[i][0], coords1[i][1]), 
                                              (coords2[i][0], coords2[i][1]), unit='m'))
                else:
                    distances.append(np.nan)
            
            group['distance'] = distances
            
            # Calculate speed (vectorized)
            group['speed'] = group['distance'] / group['time_diff']
            
            # Filter out impossibly high speeds (e.g., > 100 km/h or ~30 m/s for urban buses)
            group = group[group['speed'] < 30]
            
            # Drop helper columns
            group = group.drop(['next_lat', 'next_long', 'next_timestamp', 'time_diff', 'distance', 'speed'], axis=1)
            
            device_groups.append(group)
        
        if device_groups:
            cleaned_df = pd.concat(device_groups)
            logger.info(f"Cleaned GPS data: {len(cleaned_df)} points remaining.")
            
            # Cache the result
            self.cleaned_gps_cache[cache_key] = cleaned_df
            
            return cleaned_df
        else:
            logger.warning("No GPS data remained after cleaning.")
            return pd.DataFrame()

    def _segment_trips(self, gps_data, route_id):
        """Segment GPS data into individual trips based on time gaps and proximity to stops."""
        if gps_data.empty:
            return []
            
        logger.info(f"Segmenting trips for route {route_id}...")
        
        # Get stops for this route
        stops = self.route_stops.get(route_id, [])
        if not stops:
            logger.warning(f"No stops found for route {route_id}.")
            return []
        
        # Create stop points (do this once)
        stop_points = [Point(stop['lon'], stop['lat']) for stop in stops]
        
        # Pre-calculate stop information
        stop_info = []
        for stop_idx, stop_point in enumerate(stop_points):
            stop_info.append({
                'idx': stop_idx,
                'point': stop_point,
                'sequence': stops[stop_idx]['sequence']
            })
        
        # Process each device's data in parallel using vectorized operations where possible
        trips = []
        for device_id, group in gps_data.groupby('device_id'):
            # Sort by timestamp
            group = group.sort_values('timestamp')
            
            # Convert timestamp to datetime if it's not already
            if isinstance(group['timestamp'].iloc[0], str):
                group['timestamp'] = pd.to_datetime(group['timestamp'])
            
            # Find time gaps greater than 10 minutes to separate trips
            group['time_diff'] = group['timestamp'].diff().dt.total_seconds()
            trip_breaks = group[group['time_diff'] > 600].index.tolist()
            
            # Split into trips based on breaks
            trip_indices = [0] + trip_breaks + [len(group)]
            
            # Pre-calculate all trip points at once
            all_lat = group['lat'].values
            all_lon = group['long'].values
            all_timestamps = group['timestamp'].values
            
            for i in range(len(trip_indices) - 1):
                start_idx = trip_indices[i]
                end_idx = trip_indices[i+1]
                
                # Only consider trips with enough points
                if end_idx - start_idx < 10:
                    continue
                
                # Extract trip data
                trip_lat = all_lat[start_idx:end_idx]
                trip_lon = all_lon[start_idx:end_idx]
                trip_timestamps = all_timestamps[start_idx:end_idx]
                
                # Create points only once
                trip_points = [Point(lon, lat) for lon, lat in zip(trip_lon, trip_lat)]
                
                # Check if trip passes near the stops in sequence
                stops_passed = []
                
                # Calculate distance matrix between all trip points and all stops
                for stop in stop_info:
                    # Find closest point in the trip to this stop
                    distances = [point.distance(stop['point']) for point in trip_points]
                    min_dist_idx = np.argmin(distances)
                    min_dist = distances[min_dist_idx]
                    
                    # If closest point is within 100 meters, consider the stop passed
                    if min_dist < 0.001:  # approximately 100 meters in decimal degrees
                        stops_passed.append({
                            'stop_idx': stop['idx'],
                            'sequence': stop['sequence'],
                            'trip_point_idx': min_dist_idx,
                            'distance': min_dist
                        })
                
                # Sort stops by the trip point index to check if they are in sequence
                stops_passed.sort(key=lambda x: x['trip_point_idx'])
                
                # Check if at least 50% of stops are passed in sequence
                if len(stops_passed) >= len(stops) * 0.5:
                    # Check if stops are in correct sequence - use vectorized comparison
                    sequences = [s['sequence'] for s in stops_passed]
                    is_sequence_correct = all(sequences[i] <= sequences[i+1] for i in range(len(sequences)-1)) if sequences else False
                    
                    if is_sequence_correct:
                        trips.append({
                            'device_id': device_id,
                            'route_id': route_id,
                            'start_time': trip_timestamps[0],
                            'end_time': trip_timestamps[-1],
                            'points': list(zip(trip_lat, trip_lon)),
                            'stops_passed': stops_passed,
                            'confidence': len(stops_passed) / len(stops)
                        })
                        
        logger.info(f"Segmented {len(trips)} valid trips for route {route_id}.")
        return trips

    def _cluster_route_points(self, trips):
        """Cluster GPS points from multiple trips to find the consensus route."""
        if not trips:
            return []
            
        logger.info(f"Clustering route points from {len(trips)} trips...")
        
        # Extract all points from all trips - use list comprehension for better performance
        all_points = list(itertools.chain.from_iterable(trip['points'] for trip in trips))
        
        # Convert to numpy array for DBSCAN
        points_array = np.array(all_points)
        
        # Cluster points using DBSCAN
        # eps is the maximum distance between two samples to be considered in the same cluster
        # min_samples is the number of samples in a neighborhood for a point to be considered a core point
        dbscan = DBSCAN(eps=0.0001, min_samples=5)  # Adjust parameters as needed
        clusters = dbscan.fit_predict(points_array)
        
        # Filter out noise points (cluster label -1)
        valid_indices = clusters != -1
        valid_points = points_array[valid_indices]
        valid_clusters = clusters[valid_indices]
        
        # Group points by cluster using numpy operations
        unique_clusters = np.unique(valid_clusters)
        centroids = []
        
        for cluster_id in unique_clusters:
            # Get points belonging to this cluster
            cluster_mask = valid_clusters == cluster_id
            cluster_points = valid_points[cluster_mask]
            
            # Calculate centroid
            centroid = np.mean(cluster_points, axis=0)
            centroids.append(centroid)
        
        # Sort centroids to form a continuous path
        if not centroids:
            return []
            
        # Start with the first centroid
        sorted_centroids = [centroids[0]]
        remaining_centroids = centroids[1:]
        
        # Use numpy for distance calculations
        while remaining_centroids:
            last_centroid = sorted_centroids[-1]
            
            # Calculate distances using numpy for vectorization
            remaining_array = np.array(remaining_centroids)
            # Prepare coordinates for haversine
            last_coords = np.array([last_centroid[0], last_centroid[1]])
            
            # Calculate distances
            distances = []
            for point in remaining_array:
                distances.append(haversine((last_coords[0], last_coords[1]), (point[0], point[1]), unit='m'))
            
            # Find closest
            closest_idx = np.argmin(distances)
            
            # Add the closest centroid to sorted list and remove from remaining
            sorted_centroids.append(remaining_centroids[closest_idx])
            remaining_centroids.pop(closest_idx)
        
        logger.info(f"Generated consensus route with {len(sorted_centroids)} points.")
        return sorted_centroids

    def _generate_route_polyline(self, route_id):
        """Generate a polyline for a single route."""
        logger.info(f"Generating polyline for route {route_id}...")
        
        # Check if route has stops
        if route_id not in self.route_stops:
            logger.warning(f"No stops found for route {route_id}.")
            return None
        
        # Get device IDs for this route (pre-calculated)
        device_ids = self.route_to_devices.get(route_id, [])
        
        if not device_ids:
            logger.warning(f"No device IDs found for vehicles assigned to route {route_id}.")
            return None
        
        # Get GPS data for these devices
        gps_data = self._get_gps_data_for_devices(device_ids)
        
        if gps_data.empty:
            logger.warning(f"No GPS data found for devices assigned to route {route_id}.")
            return None
        
        # Clean GPS data
        cleaned_gps_data = self._clean_gps_data(gps_data)
        
        if cleaned_gps_data.empty:
            logger.warning(f"No valid GPS data found for route {route_id} after cleaning.")
            return None
        
        # Segment trips
        trips = self._segment_trips(cleaned_gps_data, route_id)
        
        if not trips:
            logger.warning(f"No valid trips found for route {route_id}.")
            return None
        
        # Cluster route points to find consensus route
        route_points = self._cluster_route_points(trips)
        
        if not route_points:
            logger.warning(f"Could not generate consensus route for route {route_id}.")
            return None
        
        # Create GeoJSON Feature
        route_feature = {
            "type": "Feature",
            "properties": {
                "route_id": route_id,
                "name": f"Route {route_id}",
                "num_trips": len(trips),
                "confidence": sum(trip['confidence'] for trip in trips) / len(trips),
                "stops": [{"name": stop['name'], "lat": stop['lat'], "lon": stop['lon'], 
                          "sequence": stop['sequence']} for stop in self.route_stops[route_id]]
            },
            "geometry": {
                "type": "LineString",
                "coordinates": [[point[1], point[0]] for point in route_points]  # [lon, lat] format for GeoJSON
            }
        }
        
        return route_feature

    def generate_all_route_polylines(self):
        """Generate polylines for all routes."""
        logger.info("Starting generation of polylines for all routes...")
        
        # Get all unique device IDs for batch fetching
        all_device_ids = set()
        for devices in self.route_to_devices.values():
            all_device_ids.update(devices)
        
        # Fetch all GPS data at once
        logger.info(f"Prefetching GPS data for {len(all_device_ids)} devices across all routes...")
        all_gps_data = self._get_gps_data_batch(list(all_device_ids))
        
        # Populate the cache
        for device_id, group in all_gps_data.groupby('device_id'):
            self.gps_data_cache[device_id] = group
        
        route_ids = list(self.route_stops.keys())
        logger.info(f"Found {len(route_ids)} routes to process.")
        
        successful_routes = 0
        all_features = []
        
        for route_id in route_ids:
            try:
                route_feature = self._generate_route_polyline(route_id)
                
                if route_feature:
                    # Save to GeoJSON file
                    output_path = os.path.join(self.output_dir, f"route_{route_id}.geojson")
                    with open(output_path, 'w') as f:
                        json.dump(route_feature, f)
                    
                    all_features.append(route_feature)
                    logger.info(f"Successfully generated polyline for route {route_id}.")
                    successful_routes += 1
                else:
                    logger.warning(f"Failed to generate polyline for route {route_id}.")
            except Exception as e:
                logger.error(f"Error generating polyline for route {route_id}: {e}")
        
        logger.info(f"Completed polyline generation. Successfully processed {successful_routes}/{len(route_ids)} routes.")
        
        # Create a summary GeoJSON file with all routes
        try:
            if all_features:
                collection = {
                    "type": "FeatureCollection",
                    "features": all_features
                }
                
                with open(os.path.join(self.output_dir, "all_routes.geojson"), 'w') as f:
                    json.dump(collection, f)
                
                logger.info(f"Created summary GeoJSON with {len(all_features)} routes.")
        except Exception as e:
            logger.error(f"Error creating summary GeoJSON: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate route polylines from GPS traces')
    parser.add_argument('--date', type=str, default='2025-04-03', 
                        help='Date for which to generate route polylines (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, default='route_polylines',
                        help='Directory to save the output GeoJSON files')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Number of devices to fetch GPS data for in a single batch')
    
    args = parser.parse_args()
    
    generator = RoutePolylineGenerator(date=args.date, output_dir=args.output_dir)
    generator.generate_all_route_polylines()
