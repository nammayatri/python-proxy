from fastapi import FastAPI, HTTPException
from typing import Dict, List, Optional, Union, Set
import aiohttp
import asyncio
import os
from datetime import datetime
import logging
from pydantic import BaseModel, field_validator, RootModel
import uvicorn
from contextlib import asynccontextmanager
import gc
import threading
import time
from urllib.parse import unquote
import psutil
import weakref

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
BASE_URL = os.getenv("GTFS_BASE_URL", "http://localhost:8080")
POLLING_INTERVAL = int(os.getenv("GTFS_POLLING_INTERVAL", "30"))  # in seconds
PROCESS_BATCH_SIZE = int(os.getenv("GTFS_PROCESS_BATCH_SIZE", "50"))
API_HOST = os.getenv("GTFS_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("GTFS_API_PORT", "8000"))
GC_INTERVAL = int(os.getenv("GTFS_GC_INTERVAL", "300"))  # 5 minutes default


class LatLong(BaseModel):
    lat: float
    lon: float

# Pydantic models for data validation
class NandiStop(BaseModel):
    id: str
    code: str
    name: str
    lat: float
    lon: float

    @field_validator('id', 'code', 'name', mode='before')
    @classmethod
    def convert_to_str(cls, v):
        return str(v)

class NandiTrip(BaseModel):
    id: str
    direction: Optional[str] = None

    @field_validator('id', 'direction', mode='before')
    @classmethod
    def convert_to_str(cls, v):
        return str(v) if v is not None else None

class NandiPattern(BaseModel):
    id: str
    desc: str
    routeId: str

    @field_validator('id', 'desc', 'routeId', mode='before')
    @classmethod
    def convert_to_str(cls, v):
        return str(v)

class NandiPatternDetails(BaseModel):
    id: str
    desc: Optional[str] = None
    routeId: str
    stops: List[NandiStop]
    trips: List[NandiTrip]

    @field_validator('id', 'desc', 'routeId', mode='before')
    @classmethod
    def convert_to_str(cls, v):
        return str(v) if v is not None else None

class NandiRoutesRes(BaseModel):
    id: str
    shortName: Optional[str] = None
    longName: Optional[str] = None
    mode: str
    agencyName: Optional[str] = None
    tripCount: Optional[int] = None
    stopCount: Optional[int] = None
    startPoint: LatLong
    endPoint: LatLong

    @field_validator('id', 'shortName', 'longName', 'mode', 'agencyName', mode='before')
    @classmethod
    def convert_to_str(cls, v):
        return str(v) if v is not None else None

class NandiPatternsRes(RootModel):
    root: List[NandiPattern]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

    def __len__(self):
        return len(self.root)

class RouteStopMapping(BaseModel):
    estimatedTravelTimeFromPreviousStop: Optional[int] = None
    providerCode: str
    routeCode: str
    sequenceNum: int
    stopCode: str
    stopName: str
    stopPoint: LatLong
    vehicleType: str

def castVehicleType(vehicle_type:str) -> str:
    if vehicle_type == "RAIL":
        return "METRO"
    else:
        return vehicle_type

class GTFSData:
    def __init__(self):
        self.routes: Dict[str, NandiRoutesRes] = {}
        self.route_stop_map: Dict[str, Dict[str, List[RouteStopMapping]]] = {}
        self.stop_route_map: Dict[str, Dict[str, List[RouteStopMapping]]] = {}
        self.routes_by_gtfs: Dict[str, Dict[str, NandiRoutesRes]] = {}
        self.last_update: Dict[str, datetime] = {
            "routes": datetime.min
        }
        self._lock = threading.Lock()

    def update_data(self, temp_data: 'GTFSData'):
        """Atomically update all data structures"""
        with self._lock:
            self.routes = temp_data.routes
            self.route_stop_map = temp_data.route_stop_map
            self.stop_route_map = temp_data.stop_route_map
            self.last_update = temp_data.last_update

# At the top of your file
is_ready = False

async def initial_data_load():
    """Load initial data before server starts"""
    logger.info("Starting initial data load...")
    try:
        async with aiohttp.ClientSession() as session:
            # Initialize route_trip_counts
            route_trip_counts = {}
            
            # Fetch all patterns
            patterns = await fetch_patterns(session)
            route_stop_map = {}
            stop_route_map = {}
            total_patterns = len(patterns)
            # Fetch and process all pattern details in a single pass (no batching)
            pattern_details_list = []
            for pattern in patterns:
                try:
                    details = await fetch_pattern_details(session, pattern.id)
                    pattern_details_list.append(details)
                except Exception as e:
                    logger.error(f"Error fetching pattern details for {pattern.id}: {str(e)}")

            # After pattern_details_list is populated
            for pattern in pattern_details_list:
                route_code = pattern.routeId.split(':')[-1]
                trip_count = len(pattern.trips) if pattern.trips else 0
                if route_code not in route_trip_counts:
                    route_trip_counts[route_code] = 0
                route_trip_counts[route_code] += trip_count
            
            # Calculate stop counts for each route
            route_stop_counts = {}
            for pattern in pattern_details_list:
                gtfs_id = pattern.routeId.split(':')[0]
                route_code = pattern.routeId.split(':')[-1]
                if gtfs_id not in route_stop_counts:
                    route_stop_counts[gtfs_id] = {}
                if route_code not in route_stop_counts[gtfs_id]:
                    route_stop_counts[gtfs_id][route_code] = set()
                # Add all stop codes for this pattern to the set
                route_stop_counts[gtfs_id][route_code].update(stop.code for stop in pattern.stops)
            
            # Fetch routes
            routes = await fetch_routes(session)
            routes_by_gtfs = {}
            for route in routes:
                gtfs_id = route.id.split(':')[0]
                route_code = route.id.split(':')[-1]
                if gtfs_id not in routes_by_gtfs:
                    routes_by_gtfs[gtfs_id] = {}
                routes_by_gtfs[gtfs_id][route_code] = NandiRoutesRes(
                    id=route_code,
                    shortName=route.shortName,
                    longName=route.longName,
                    mode=castVehicleType(route.mode),
                    agencyName=route.agencyName,
                    tripCount=route_trip_counts.get(route_code, 0),
                    stopCount=len(route_stop_counts.get(gtfs_id, {}).get(route_code, set())),
                    startPoint=LatLong(lat=0.0, lon=0.0),
                    endPoint=LatLong(lat=0.0, lon=0.0)
                )

            for pattern in pattern_details_list:
                gtfs_id = pattern.routeId.split(':')[0]
                route_code = pattern.routeId.split(':')[-1]
                if gtfs_id not in route_stop_map:
                    route_stop_map[gtfs_id] = {}
                if route_code not in route_stop_map[gtfs_id]:
                    route_stop_map[gtfs_id][route_code] = []
                if gtfs_id not in stop_route_map:
                    stop_route_map[gtfs_id] = {}
                for seq, stop in enumerate(pattern.stops):
                    mapping = RouteStopMapping(
                        estimatedTravelTimeFromPreviousStop=None,  # Not available in GTFS
                        providerCode="GTFS",
                        routeCode=route_code,
                        sequenceNum=seq,
                        stopCode=stop.code,
                        stopName=stop.name,
                        stopPoint=LatLong(lat=stop.lat, lon=stop.lon),
                        vehicleType=castVehicleType(routes_by_gtfs.get(gtfs_id, {}).get(pattern.routeId, NandiRoutesRes(
                            id=pattern.routeId,
                            mode="UNKNOWN",
                            startPoint=LatLong(lat=0.0, lon=0.0),
                            endPoint=LatLong(lat=0.0, lon=0.0)
                        )).mode)
                    )
                    route_stop_map[gtfs_id][route_code].append(mapping)
                    if stop.code not in stop_route_map[gtfs_id]:
                        stop_route_map[gtfs_id][stop.code] = []
                    stop_route_map[gtfs_id][stop.code].append(mapping)
                    
                    # Set startPoint and endPoint for the route
                    if seq == 0:  # First stop
                        routes_by_gtfs[gtfs_id][route_code].startPoint = LatLong(lat=stop.lat, lon=stop.lon)
                    if seq == len(pattern.stops) - 1:  # Last stop
                        routes_by_gtfs[gtfs_id][route_code].endPoint = LatLong(lat=stop.lat, lon=stop.lon)
            # Free memory
            del pattern_details_list
            gc.collect()
            # Update all data structures atomically
            gtfs_data.routes = {route.id: route for route in routes}
            gtfs_data.route_stop_map = route_stop_map
            gtfs_data.stop_route_map = stop_route_map
            gtfs_data.routes_by_gtfs = routes_by_gtfs

            # Update timestamp
            current_time = datetime.now()
            gtfs_data.last_update["routes"] = current_time
            logger.info(f"Initial data load complete: {len(routes)} routes, {total_patterns} patterns")
            # Set global is_ready to True
            global is_ready
            is_ready = True
    except Exception as e:
        logger.error(f"Error during initial data load: {str(e)}")
        raise

def garbage_collector():
    """Background thread for periodic garbage collection"""
    while True:
        try:
            # Force garbage collection
            collected = gc.collect()
            logger.debug(f"Garbage collector ran, collected {collected} objects")
            
            # Log memory usage
            process = psutil.Process()
            memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB
            logger.info(f"Current memory usage: {memory_usage:.2f} MB")
            
            # If memory usage is too high, try to free more memory
            if memory_usage > 500:  # 500MB threshold
                gc.collect(2)  # Force full collection
                logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
                
        except Exception as e:
            logger.error(f"Error in garbage collector: {str(e)}")
        time.sleep(GC_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application"""
    # Start garbage collector thread
    gc_thread = threading.Thread(target=garbage_collector, daemon=True)
    gc_thread.start()
    
    # Startup: Load initial data and start polling task
    try:
        await initial_data_load()
        polling_task_instance = asyncio.create_task(polling_task())
        yield
        # Shutdown: Cancel the polling task
        polling_task_instance.cancel()
        try:
            await polling_task_instance
        except asyncio.CancelledError:
            pass
    except Exception as e:
        logger.error(f"Failed to start service: {str(e)}")
        raise

# Initialize FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)
gtfs_data = GTFSData()

async def fetch_patterns(session: aiohttp.ClientSession) -> List[NandiPattern]:
    """Fetch all patterns from the API"""
    async with session.get(f"{BASE_URL}/otp/routers/default/index/patterns") as response:
        if response.status == 200:
            data = await response.json()
            return [NandiPattern(**item) for item in data]
        raise HTTPException(status_code=response.status, detail="Failed to fetch patterns")

async def fetch_pattern_details(session: aiohttp.ClientSession, pattern_id: str) -> NandiPatternDetails:
    """Fetch specific pattern details from the API"""
    async with session.get(f"{BASE_URL}/otp/routers/default/index/patterns/{pattern_id}") as response:
        if response.status == 200:
            data = await response.json()
            return NandiPatternDetails(**data)
        raise HTTPException(status_code=response.status, detail=f"Failed to fetch pattern details for {pattern_id}")

async def fetch_routes(session: aiohttp.ClientSession) -> List[NandiRoutesRes]:
    """Fetch all routes from the API"""
    async with session.get(f"{BASE_URL}/otp/routers/default/index/routes") as response:
        if response.status == 200:
            data = await response.json()
            return [NandiRoutesRes(
                **item,
                startPoint=LatLong(lat=0.0, lon=0.0),
                endPoint=LatLong(lat=0.0, lon=0.0)
            ) for item in data]
        raise HTTPException(status_code=response.status, detail="Failed to fetch routes")

async def process_pattern_batch(session: aiohttp.ClientSession, patterns: List[NandiPattern], 
                              batch_size: int = 100) -> Dict[str, NandiPatternDetails]:
    """Process patterns in batches to manage memory"""
    pattern_details = {}
    for i in range(0, len(patterns), batch_size):
        batch = patterns[i:i + batch_size]
        tasks = []
        for pattern in batch:
            if pattern.id not in gtfs_data._processed_patterns:
                tasks.append(fetch_pattern_details(session, pattern.id))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for pattern, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching pattern details for {pattern.id}: {str(result)}")
                    continue
                pattern_details[pattern.id] = result
                gtfs_data._processed_patterns.add(pattern.id)
        
        # Force garbage collection after each batch
        gc.collect()
    
    return pattern_details

async def polling_task():
    """Main polling task that updates all data periodically"""
    while True:
        session = None
        try:
            session = aiohttp.ClientSession()
            # Fetch routes
            routes = await fetch_routes(session)
            routes_by_gtfs = {}
            for route in routes:
                gtfs_id = route.id.split(':')[0]
                if gtfs_id not in routes_by_gtfs:
                    routes_by_gtfs[gtfs_id] = {}
                routes_by_gtfs[gtfs_id][route.id] = route

            # Fetch all patterns
            patterns = await fetch_patterns(session)
            batch_size = PROCESS_BATCH_SIZE
            route_stop_map = {}
            stop_route_map = {}
            total_patterns = len(patterns)
            for i in range(0, total_patterns, batch_size):
                batch = patterns[i:i+batch_size]
                batch_details = []
                for pattern in batch:
                    try:
                        details = await fetch_pattern_details(session, pattern.id)
                        batch_details.append(details)
                    except Exception as e:
                        logger.error(f"Error fetching pattern details for {pattern.id}: {str(e)}")
                # Process batch
                for pattern in batch_details:
                    gtfs_id = pattern.routeId.split(':')[0]
                    route_code = pattern.routeId.split(':')[-1]
                    if gtfs_id not in route_stop_map:
                        route_stop_map[gtfs_id] = {}
                    if route_code not in route_stop_map[gtfs_id]:
                        route_stop_map[gtfs_id][route_code] = []
                    if gtfs_id not in stop_route_map:
                        stop_route_map[gtfs_id] = {}
                    for seq, stop in enumerate(pattern.stops):
                        mapping = RouteStopMapping(
                            estimatedTravelTimeFromPreviousStop=None,  # Not available in GTFS
                            providerCode="GTFS",
                            routeCode=route_code,
                            sequenceNum=seq,
                            stopCode=stop.code,
                            stopName=stop.name,
                            stopPoint=LatLong(lat=stop.lat, lon=stop.lon),
                            vehicleType=castVehicleType(routes_by_gtfs.get(gtfs_id, {}).get(pattern.routeId, NandiRoutesRes(
                                id=pattern.routeId,
                                mode="UNKNOWN",
                                startPoint=LatLong(lat=0.0, lon=0.0),
                                endPoint=LatLong(lat=0.0, lon=0.0)
                            )).mode)
                        )
                        route_stop_map[gtfs_id][route_code].append(mapping)
                        if stop.code not in stop_route_map[gtfs_id]:
                            stop_route_map[gtfs_id][stop.code] = []
                        stop_route_map[gtfs_id][stop.code].append(mapping)
                # Free memory after batch
                del batch_details
                gc.collect()

            if routes:
                temp_data = GTFSData()
                temp_data.routes = {route.id: route for route in routes}
                temp_data.route_stop_map = route_stop_map
                temp_data.stop_route_map = stop_route_map
                current_time = datetime.now()
                temp_data.last_update["routes"] = current_time
                gtfs_data.update_data(temp_data)
                logger.info(f"Successfully updated all data: {len(routes)} routes, {total_patterns} patterns")
                # Clear references to help garbage collection
                del routes
                del routes_by_gtfs
                del temp_data
                del route_stop_map
                del stop_route_map
                # Force garbage collection
                gc.collect()
                # Log memory usage
                process = psutil.Process()
                memory_usage = process.memory_info().rss / 1024 / 1024
                logger.info(f"Current memory usage: {memory_usage:.2f} MB")
            else:
                logger.error("Failed to fetch complete data set, skipping update")
        except Exception as e:
            logger.error(f"Error in polling task: {str(e)}")
        finally:
            if session:
                await session.close()
        await asyncio.sleep(POLLING_INTERVAL)

# API endpoints
@app.get("/route/{gtfs_id}/{route_id}", response_model=NandiRoutesRes)
async def get_route(gtfs_id: str, route_id: str):
    """Get specific route"""
    gtfs_id = unquote(gtfs_id)
    route_id = unquote(route_id)
    if gtfs_id not in gtfs_data.routes_by_gtfs or route_id not in gtfs_data.routes_by_gtfs[gtfs_id]:
        raise HTTPException(status_code=404, detail="Route not found")
    return gtfs_data.routes_by_gtfs[gtfs_id][route_id]

@app.get("/routes/{gtfs_id}", response_model=List[NandiRoutesRes])
async def get_routes(gtfs_id: str):
    gtfs_id = unquote(gtfs_id)
    if gtfs_id not in gtfs_data.routes_by_gtfs:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    return list(gtfs_data.routes_by_gtfs[gtfs_id].values())

@app.get("/route-stop-mapping/{gtfs_id}/route/{route_code}", response_model=List[RouteStopMapping])
async def get_route_stop_mapping_by_route(gtfs_id: str, route_code: str):
    gtfs_id = unquote(gtfs_id)
    route_code = unquote(route_code)
    if gtfs_id not in gtfs_data.route_stop_map or route_code not in gtfs_data.route_stop_map[gtfs_id]:
        raise HTTPException(status_code=404, detail="Route code not found for GTFS ID")
    return gtfs_data.route_stop_map[gtfs_id][route_code]

@app.get("/route-stop-mapping/{gtfs_id}/stop/{stop_code}", response_model=List[RouteStopMapping])
async def get_route_stop_mapping_by_stop(gtfs_id: str, stop_code: str):
    gtfs_id = unquote(gtfs_id)
    stop_code = unquote(stop_code)
    if gtfs_id not in gtfs_data.stop_route_map or stop_code not in gtfs_data.stop_route_map[gtfs_id]:
        raise HTTPException(status_code=404, detail="Stop code not found for GTFS ID")
    return gtfs_data.stop_route_map[gtfs_id][stop_code]

@app.get("/routes/{gtfs_id}/fuzzy/{query}", response_model=List[NandiRoutesRes])
async def get_routes_fuzzy(gtfs_id: str, query: str, limit: Optional[int] = None):
    gtfs_id = unquote(gtfs_id)
    query = unquote(query)
    if gtfs_id not in gtfs_data.routes_by_gtfs:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    
    # Use a dictionary to track unique routes
    unique_routes = {}
    
    for route in gtfs_data.routes_by_gtfs[gtfs_id].values():
        if query in route.longName or query in route.shortName or query in route.id:
            # Use route.id as key to ensure uniqueness
            unique_routes[route.id] = route
            if limit is not None and len(unique_routes) >= limit:
                break
    
    return list(unique_routes.values())

@app.get("/stops/{gtfs_id}", response_model=List[RouteStopMapping])
async def get_stops(gtfs_id: str):
    gtfs_id = unquote(gtfs_id)
    if gtfs_id not in gtfs_data.stop_route_map:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    # Flatten the list of lists into a single list of RouteStopMapping objects
    all_stops = []
    for stop_list in gtfs_data.stop_route_map[gtfs_id].values():
        all_stops.extend(stop_list)
    return all_stops

@app.get("/stop/{gtfs_id}/{stop_code}", response_model=List[RouteStopMapping])
async def get_stop(gtfs_id: str, stop_code: str):
    gtfs_id = unquote(gtfs_id)
    stop_code = unquote(stop_code)
    if gtfs_id not in gtfs_data.stop_route_map or stop_code not in gtfs_data.stop_route_map[gtfs_id]:
        raise HTTPException(status_code=404, detail="Stop code not found for GTFS ID")
    return gtfs_data.stop_route_map[gtfs_id][stop_code]

@app.get("/stops/{gtfs_id}/fuzzy/{query}", response_model=List[RouteStopMapping])
async def get_stops_fuzzy(gtfs_id: str, query: str, limit: Optional[int] = None):
    gtfs_id = unquote(gtfs_id)
    query = unquote(query).lower()
    if gtfs_id not in gtfs_data.stop_route_map:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    
    # Use a dictionary to track unique stops while preserving all matching RouteStopMapping objects
    unique_stops = {}
    
    for stop_code, stop_mappings in gtfs_data.stop_route_map[gtfs_id].items():
        # Check if any of the stop mappings match the query
        for mapping in stop_mappings:
            if (query in mapping.stopName.lower() or 
                query in mapping.stopCode.lower()):
                # Use stop_code as key to ensure uniqueness while preserving all matching mappings
                unique_stops[stop_code] = mapping
                if limit is not None and len(unique_stops) >= limit:
                    return list(unique_stops.values())
    
    return list(unique_stops.values())

@app.get("/ready")
async def readiness_probe():
    """Readiness probe endpoint that checks if the service is ready to handle requests"""
    if not is_ready:
        raise HTTPException(status_code=503, detail="Service not ready - still loading initial data")
    return {"status": "ok", "message": "Service is ready to handle requests"}

if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
