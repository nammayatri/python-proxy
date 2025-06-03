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
POLLING_INTERVAL = int(os.getenv("GTFS_POLLING_INTERVAL", "60"))  # in seconds
API_HOST = os.getenv("GTFS_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("GTFS_API_PORT", "8000"))
GC_INTERVAL = int(os.getenv("GTFS_GC_INTERVAL", "300"))  # 5 minutes default

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

class LatLong(BaseModel):
    lat: float
    lon: float

class RouteStopMapping(BaseModel):
    estimatedTravelTimeFromPreviousStop: Optional[int] = None
    providerCode: str
    routeCode: str
    sequenceNum: int
    stopCode: str
    stopName: str
    stopPoint: LatLong
    vehicleType: str

class GTFSData:
    def __init__(self):
        self.patterns_by_gtfs: Dict[str, Dict[str, NandiPattern]] = {}
        self.pattern_details: Dict[str, NandiPatternDetails] = {}
        self.routes_by_gtfs: Dict[str, Dict[str, NandiRoutesRes]] = {}
        self.routes: Dict[str, NandiRoutesRes] = {}
        self.route_stop_map: Dict[str, Dict[str, List[RouteStopMapping]]] = {}
        self.stop_route_map: Dict[str, Dict[str, List[RouteStopMapping]]] = {}
        self.last_update: Dict[str, datetime] = {
            "patterns": datetime.min,
            "pattern_details": datetime.min,
            "routes": datetime.min
        }
        self._lock = threading.Lock()
        self._processed_patterns: Set[str] = set()  # Track processed patterns

    def update_data(self, temp_data: 'GTFSData'):
        """Atomically update all data structures"""
        with self._lock:
            self.patterns_by_gtfs = temp_data.patterns_by_gtfs
            self.pattern_details = temp_data.pattern_details
            self.routes = temp_data.routes
            self.routes_by_gtfs = temp_data.routes_by_gtfs
            self.route_stop_map = temp_data.route_stop_map
            self.stop_route_map = temp_data.stop_route_map
            self.last_update = temp_data.last_update
            self._processed_patterns = temp_data._processed_patterns

async def initial_data_load():
    """Load initial data before server starts"""
    logger.info("Starting initial data load...")
    try:
        async with aiohttp.ClientSession() as session:
            # Fetch all data first
            patterns = await fetch_patterns(session)
            routes = await fetch_routes(session)
            
            # Create temporary storage for pattern details
            pattern_details = {}
            
            # Organize patterns by GTFS ID using dictionaries for O(1) operations
            patterns_by_gtfs = {}
            for pattern in patterns:
                gtfs_id = pattern.id.split(':')[0]  # Extract GTFS ID from pattern ID
                if gtfs_id not in patterns_by_gtfs:
                    patterns_by_gtfs[gtfs_id] = {}
                patterns_by_gtfs[gtfs_id][pattern.id] = pattern
                
                try:
                    details = await fetch_pattern_details(session, pattern.id)
                    pattern_details[pattern.id] = details
                except Exception as e:
                    logger.error(f"Error fetching pattern details for {pattern.id}: {str(e)}")
                    continue

            # Organize routes by GTFS ID using dictionaries for O(1) operations
            routes_by_gtfs = {}
            for route in routes:
                gtfs_id = route.id.split(':')[0]
                if gtfs_id not in routes_by_gtfs:
                    routes_by_gtfs[gtfs_id] = {}
                routes_by_gtfs[gtfs_id][route.id] = route
            
            # Build route_stop_map and stop_route_map (reference-based)
            route_stop_map = {}
            stop_route_map = {}
            route_lookup = {route.id: route for route in routes}
            for gtfs_id in patterns_by_gtfs:
                route_stop_map[gtfs_id] = {}
                stop_route_map[gtfs_id] = {}
            for pattern_detail in pattern_details.values():
                route_id = pattern_detail.routeId
                route = route_lookup.get(route_id)
                if not route:
                    continue
                gtfs_id = route_id.split(":")[0]
                route_code = route.id
                if route_code not in route_stop_map[gtfs_id]:
                    route_stop_map[gtfs_id][route_code] = []
                for seq, stop in enumerate(pattern_detail.stops):
                    route_obj = routes_by_gtfs[gtfs_id].get(route_id)
                    vehicle_type = route_obj.mode if route_obj and hasattr(route_obj, 'mode') else "UNKNOWN"
                    mapping = RouteStopMapping(
                        estimatedTravelTimeFromPreviousStop=None,  # Placeholder
                        providerCode="GTFS",  # Placeholder
                        routeCode=route_code.split(':')[-1],
                        sequenceNum=seq,
                        stopCode=stop.code.split(':')[-1],
                        stopName=stop.name,
                        stopPoint=LatLong(lat=stop.lat, lon=stop.lon),
                        vehicleType=vehicle_type
                    )
                    route_stop_map[gtfs_id][route_code].append(mapping)
                    if stop.code not in stop_route_map[gtfs_id]:
                        stop_route_map[gtfs_id][stop.code] = []
                    stop_route_map[gtfs_id][stop.code].append(mapping)
            
            # Update all data structures atomically
            gtfs_data.patterns_by_gtfs = patterns_by_gtfs
            gtfs_data.pattern_details = pattern_details
            gtfs_data.routes = {route.id: route for route in routes}
            gtfs_data.routes_by_gtfs = routes_by_gtfs
            gtfs_data.route_stop_map = route_stop_map
            gtfs_data.stop_route_map = stop_route_map
            
            # Update timestamps
            current_time = datetime.now()
            gtfs_data.last_update["patterns"] = current_time
            gtfs_data.last_update["pattern_details"] = current_time
            gtfs_data.last_update["routes"] = current_time
            
            logger.info(f"Initial data load complete: {len(patterns)} patterns across {len(patterns_by_gtfs)} GTFS IDs, {len(pattern_details)} pattern details, {len(routes)} routes")
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
    await initial_data_load()
    polling_task_instance = asyncio.create_task(polling_task())
    yield
    # Shutdown: Cancel the polling task
    polling_task_instance.cancel()
    try:
        await polling_task_instance
    except asyncio.CancelledError:
        pass

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
            return [NandiRoutesRes(**item) for item in data]
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
            
            # Fetch all data first
            patterns = await fetch_patterns(session)
            routes = await fetch_routes(session)
            
            # Process patterns in batches
            pattern_details = await process_pattern_batch(session, patterns)
            
            # Organize patterns by GTFS ID
            patterns_by_gtfs = {}
            for pattern in patterns:
                gtfs_id = pattern.id.split(':')[0]
                if gtfs_id not in patterns_by_gtfs:
                    patterns_by_gtfs[gtfs_id] = {}
                patterns_by_gtfs[gtfs_id][pattern.id] = pattern

            # Organize routes by GTFS ID
            routes_by_gtfs = {}
            for route in routes:
                gtfs_id = route.id.split(':')[0]
                if gtfs_id not in routes_by_gtfs:
                    routes_by_gtfs[gtfs_id] = {}
                routes_by_gtfs[gtfs_id][route.id] = route

            # Build route_stop_map and stop_route_map
            route_stop_map = {}
            stop_route_map = {}
            
            # Pre-allocate dictionaries
            for gtfs_id in patterns_by_gtfs:
                route_stop_map[gtfs_id] = {}
                stop_route_map[gtfs_id] = {}

            # Process pattern details in batches
            batch_size = 100
            for i in range(0, len(pattern_details), batch_size):
                batch = list(pattern_details.items())[i:i + batch_size]
                for pattern_id, pattern_detail in batch:
                    route_id = pattern_detail.routeId
                    gtfs_id = route_id.split(':')[0]
                    route_obj = routes_by_gtfs.get(gtfs_id, {}).get(route_id)
                    
                    if not route_obj:
                        continue

                    route_code = route_id
                    if route_code not in route_stop_map[gtfs_id]:
                        route_stop_map[gtfs_id][route_code] = []

                    for seq, stop in enumerate(pattern_detail.stops):
                        route_obj = routes_by_gtfs[gtfs_id].get(route_id)
                        vehicle_type = route_obj.mode if route_obj and hasattr(route_obj, 'mode') else "UNKNOWN"
                        
                        mapping = RouteStopMapping(
                            estimatedTravelTimeFromPreviousStop=None,
                            providerCode="GTFS",
                            routeCode=route_code.split(':')[-1],
                            sequenceNum=seq,
                            stopCode=stop.code.split(':')[-1],
                            stopName=stop.name,
                            stopPoint=LatLong(lat=stop.lat, lon=stop.lon),
                            vehicleType=vehicle_type
                        )

                        route_stop_map[gtfs_id][route_code].append(mapping)
                        stop_code = stop.code.split(':')[-1]
                        if stop_code not in stop_route_map[gtfs_id]:
                            stop_route_map[gtfs_id][stop_code] = []
                        stop_route_map[gtfs_id][stop_code].append(mapping)

                # Force garbage collection after each batch
                gc.collect()

            if patterns and routes:
                temp_data = GTFSData()
                temp_data.patterns_by_gtfs = patterns_by_gtfs
                temp_data.pattern_details = pattern_details
                temp_data.routes = {route.id: route for route in routes}
                temp_data.routes_by_gtfs = routes_by_gtfs
                temp_data.route_stop_map = route_stop_map
                temp_data.stop_route_map = stop_route_map
                temp_data._processed_patterns = gtfs_data._processed_patterns.copy()
                
                current_time = datetime.now()
                temp_data.last_update["patterns"] = current_time
                temp_data.last_update["pattern_details"] = current_time
                temp_data.last_update["routes"] = current_time
                
                gtfs_data.update_data(temp_data)
                
                # Log the update before clearing variables
                logger.info(f"Successfully updated all data: {len(patterns)} patterns across {len(patterns_by_gtfs)} GTFS IDs, {len(pattern_details)} pattern details, {len(routes)} routes")
                
                # Clear references to help garbage collection
                del patterns
                del routes
                del patterns_by_gtfs
                del routes_by_gtfs
                del pattern_details
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
@app.get("/patterns/{gtfs_id}", response_model=NandiPatternsRes)
async def get_patterns(gtfs_id: str):
    """Get patterns filtered by GTFS ID"""
    gtfs_id = unquote(gtfs_id)
    if gtfs_id not in gtfs_data.patterns_by_gtfs:
        raise HTTPException(status_code=404, detail=f"No patterns found for GTFS ID: {gtfs_id}")
    return NandiPatternsRes(root=list(gtfs_data.patterns_by_gtfs[gtfs_id].values()))

@app.get("/pattern/{pattern_id}", response_model=NandiPatternDetails)
async def get_pattern_details(pattern_id: str):
    """Get specific pattern details"""
    pattern_id = unquote(pattern_id)
    if pattern_id not in gtfs_data.pattern_details:
        raise HTTPException(status_code=404, detail="Pattern not found")
    return gtfs_data.pattern_details[pattern_id]

@app.get("/routes/{gtfs_id}", response_model=List[NandiRoutesRes])
async def get_routes(gtfs_id: str):
    """Get routes filtered by GTFS ID"""
    gtfs_id = unquote(gtfs_id)
    if gtfs_id not in gtfs_data.routes_by_gtfs:
        raise HTTPException(status_code=404, detail=f"No routes found for GTFS ID: {gtfs_id}")
    return list(gtfs_data.routes_by_gtfs[gtfs_id].values())

@app.get("/routes/{route_id}", response_model=NandiRoutesRes)
async def get_route(route_id: str):
    """Get specific route"""
    route_id = unquote(route_id)
    if route_id not in gtfs_data.routes:
        raise HTTPException(status_code=404, detail="Route not found")
    return gtfs_data.routes[route_id]

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

if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
