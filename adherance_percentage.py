import pandas as pd
import datetime
from geopy.distance import geodesic
import multiprocessing as mp
from functools import partial
import numpy as np
import time
import os

# Define the date and radius for stop adherence
date = '2025-04-11'
STOP_RADIUS = 100  # meters - adjust this value as needed

# Global counter variable that will be inherited by worker processes
counter = None

def init_worker(shared_counter):
    """Initialize the global counter in each worker process."""
    global counter
    counter = shared_counter

def process_trip_chunk(trips_chunk, device_location_data, vehicle_to_device, route_stop_mapping, total_trips):
    """Process a chunk of trips and return adherence results."""
    global counter
    chunk_results = {}
    
    for i, (_, trip) in enumerate(trips_chunk.iterrows()):
        vehicle_no = trip['vehicle_no']
        route_id = trip['route_id']
        
        # Skip if no device is mapped to this vehicle
        if vehicle_no not in vehicle_to_device or pd.isna(vehicle_to_device[vehicle_no]):
            with counter.get_lock():
                counter.value += 1
            continue
        
        device_id = vehicle_to_device[vehicle_no]
        
        # Create a key for this vehicle-route combination
        vehicle_route_key = f"{vehicle_no}_{route_id}"
        
        # Initialize the entry in results if not already present
        if vehicle_route_key not in chunk_results:
            chunk_results[vehicle_route_key] = {
                'vehicle_no': vehicle_no,
                'route_id': route_id,
                'correct_trips': 0,
                'incorrect_trips': 0
            }
        
        # Parse start and end times
        # Add a 5-minute buffer before the start time to account for early buses
        start_time = datetime.datetime.strptime(f"{date} {trip['start_time']}", "%Y-%m-%d %H:%M") - datetime.timedelta(minutes=30)
        end_time = datetime.datetime.strptime(f"{date} {trip['end_time']}", "%Y-%m-%d %H:%M")
        
        # Handle times that cross midnight
        if end_time < start_time:
            end_time = end_time + datetime.timedelta(days=1)
            
        route_stops = route_stop_mapping[route_stop_mapping['Route ID'] == route_id].sort_values('Sequence')
        
        # Calculate the total route distance using straight-line distances between consecutive stops
        total_route_distance = 0
        for i in range(len(route_stops) - 1):
            current_stop = route_stops.iloc[i]
            next_stop = route_stops.iloc[i + 1]
            current_point = (current_stop['LAT'], current_stop['LON'])
            next_point = (next_stop['LAT'], next_stop['LON'])
            segment_distance = geodesic(current_point, next_point).meters
            total_route_distance += segment_distance
        
        # Calculate expected trip duration based on average speed of 20 km/h
        # Convert distance from meters to kilometers and calculate hours needed
        average_speed_kmph = 20
        expected_duration_hours = (total_route_distance / 1000) / average_speed_kmph
        expected_duration_minutes = expected_duration_hours * 60
        
        # Adjust end_time based on calculated duration if it's more realistic
        calculated_end_time = start_time + datetime.timedelta(minutes=expected_duration_minutes)
        if calculated_end_time > end_time:
            end_time = calculated_end_time + datetime.timedelta(minutes=30) # added buffer of 30 minutes in end time
        
        # Get the GPS data for this device during the trip time window
        device_locations = device_location_data[
            (device_location_data['device_id'] == device_id) & 
            (device_location_data['timestamp'] >= start_time) & 
            (device_location_data['timestamp'] <= end_time)
        ]
        
        if len(device_locations) == 0:
            with counter.get_lock():
                counter.value += 1
            continue
        
        if len(route_stops) < 2:
            with counter.get_lock():
                counter.value += 1
            continue
        
        # Get the first and last stops of the route
        start_stop = route_stops.iloc[0]
        end_stop = route_stops.iloc[-1]
        
        # Find all points that are close to the start stop
        start_candidate = None
        for idx, location in device_locations.iterrows():
            loc_point = (location['lat'], location['long'])
            start_stop_point = (start_stop['LAT'], start_stop['LON'])
            distance = geodesic(loc_point, start_stop_point).meters
            if distance <= STOP_RADIUS:
                start_candidate = location['timestamp']
                break
        
        # Skip if no start candidate found
        if start_candidate is None:
            with counter.get_lock():
                counter.value += 1
            chunk_results[vehicle_route_key]['incorrect_trips'] += 1
            continue
            
        # Filter locations after start candidate
        filtered_locations = device_locations[device_locations['timestamp'] >= start_candidate]
        
        if len(filtered_locations) == 0:
            with counter.get_lock():
                counter.value += 1
            chunk_results[vehicle_route_key]['incorrect_trips'] += 1
            continue
        
        # Find all points that are close to the end stop
        end_candidate = None
        for idx, location in filtered_locations.iterrows():
            loc_point = (location['lat'], location['long'])
            end_stop_point = (end_stop['LAT'], end_stop['LON'])
            distance = geodesic(loc_point, end_stop_point).meters
            if distance <= STOP_RADIUS:
                end_candidate = location['timestamp']
                break
        
        # Check if we have valid start and end points
        if start_candidate is not None and end_candidate is not None:
            # Make sure end_candidate is after start_candidate
            if end_candidate <= start_candidate:
                chunk_results[vehicle_route_key]['incorrect_trips'] += 1
                with counter.get_lock():
                    counter.value += 1
                continue
                
            # Calculate the actual trip duration
            actual_duration = (end_candidate - start_candidate).total_seconds() / 60  # in minutes
            
            # Compare with expected duration (with some tolerance, e.g., ±30%)
            duration_tolerance = 0.3  # 30%
            min_acceptable_duration = expected_duration_minutes * (1 - duration_tolerance)
            max_acceptable_duration = expected_duration_minutes * (1 + duration_tolerance)
            
            if (actual_duration >= min_acceptable_duration and 
                actual_duration <= max_acceptable_duration):
                chunk_results[vehicle_route_key]['correct_trips'] += 1
            else:
                chunk_results[vehicle_route_key]['incorrect_trips'] += 1
        else:
            chunk_results[vehicle_route_key]['incorrect_trips'] += 1
        
        # Update progress counter
        with counter.get_lock():
            counter.value += 1
    
    return chunk_results

def update_progress(counter, total, start_time):
    """Display progress percentage."""
    while counter.value < total:
        completed = counter.value
        percent = (completed / total) * 100
        elapsed = time.time() - start_time
        
        if completed > 0 and elapsed > 0:
            items_per_sec = completed / elapsed
            remaining_items = total - completed
            eta_seconds = remaining_items / items_per_sec if items_per_sec > 0 else 0
            
            # Convert to minutes and seconds
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            
            # Clear line and print progress
            print(f"\rProgress: {percent:.1f}% ({completed}/{total}) | ETA: {eta_min}m {eta_sec}s", end="")
        else:
            print(f"\rProgress: {percent:.1f}% ({completed}/{total})", end="")
            
        time.sleep(1)
    
    # Final update
    print(f"\rProgress: 100.0% ({total}/{total}) | Completed!", end="\n\n")

def main():
    # Load all the necessary data
    print("Loading data files...")
    device_location_data = pd.read_csv(f'{date}-location.csv')
    vehicle_route_mapping = pd.read_csv("vehicle_route_mapping-11-april.csv")
    vehicle_device_mapping = pd.read_csv("vehicle_device_mapping.csv")
    route_stop_mapping = pd.read_csv("route-stop-mapping.csv")

    # Convert timestamp to datetime format for easier comparison
    device_location_data['timestamp'] = pd.to_datetime(device_location_data['timestamp'])

    # Create a lookup dictionary for vehicle to device mapping
    vehicle_to_device = dict(zip(vehicle_device_mapping['vehicle_no'], vehicle_device_mapping['device_id']))

    # Determine number of processes to use (usually number of CPU cores)
    num_processes = mp.cpu_count()
    
    # Split data into chunks for parallel processing
    chunk_size = len(vehicle_route_mapping) // num_processes
    if chunk_size == 0:
        chunk_size = 1  # Handle case with very few records
    
    # Create chunks
    trip_chunks = [vehicle_route_mapping.iloc[i:i+chunk_size] for i in range(0, len(vehicle_route_mapping), chunk_size)]
    
    # Calculate total trips for progress tracking
    total_trips = len(vehicle_route_mapping)
    
    print(f"Processing {total_trips} trips in parallel using {num_processes} processes...")
    
    # Create a shared counter for progress tracking
    shared_counter = mp.Value('i', 0)
    
    # Track start time for ETA calculation
    start_time = time.time()
    
    # Create a process to display progress
    progress_process = mp.Process(target=update_progress, args=(shared_counter, total_trips, start_time))
    progress_process.start()
    
    # Create a pool of worker processes with proper initialization
    with mp.Pool(processes=num_processes, initializer=init_worker, initargs=(shared_counter,)) as pool:
        # Create a partial function with fixed arguments (excluding the shared counter)
        process_chunk_partial = partial(
            process_trip_chunk, 
            device_location_data=device_location_data,
            vehicle_to_device=vehicle_to_device,
            route_stop_mapping=route_stop_mapping,
            total_trips=total_trips
        )
        
        # Map the function to chunks
        chunk_results_list = pool.map(process_chunk_partial, trip_chunks)
    
    # Wait for the progress process to finish
    progress_process.join()
    
    # Calculate processing time
    processing_time = time.time() - start_time
    processing_minutes = int(processing_time // 60)
    processing_seconds = int(processing_time % 60)
    
    print(f"Processing completed in {processing_minutes}m {processing_seconds}s")
    
    # Merge results from all chunks
    results = {}
    for chunk_result in chunk_results_list:
        for key, data in chunk_result.items():
            if key not in results:
                results[key] = data
            else:
                results[key]['correct_trips'] += data['correct_trips']
                results[key]['incorrect_trips'] += data['incorrect_trips']

    # Calculate adherence percentages
    print("\nAdherence Results:")
    print("-" * 80)
    print(f"{'Vehicle':<10} {'Route':<10} {'Adherence %':<15} {'Correct':<10} {'Incorrect':<10}")
    print("-" * 80)

    for key, data in results.items():
        total_trips = data['correct_trips'] + data['incorrect_trips']
        if total_trips > 0:
            adherence_percentage = (data['correct_trips'] / total_trips) * 100
        else:
            adherence_percentage = 0
        
        print(f"{data['vehicle_no']:<10} {data['route_id']:<10} {adherence_percentage:.2f}%{' ':<10} {data['correct_trips']:<10} {data['incorrect_trips']:<10}")

    # Calculate overall adherence
    total_correct = sum(data['correct_trips'] for data in results.values())
    total_incorrect = sum(data['incorrect_trips'] for data in results.values())
    total_trips = total_correct + total_incorrect

    if total_trips > 0:
        overall_adherence = (total_correct / total_trips) * 100
    else:
        overall_adherence = 0

    print("-" * 80)
    print(f"Overall Adherence: {overall_adherence:.2f}% ({total_correct} correct out of {total_trips} trips)")

if __name__ == "__main__":
    main()







