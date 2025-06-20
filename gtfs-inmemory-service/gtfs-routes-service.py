from fastapi import FastAPI, HTTPException
from typing import Dict, List, Optional, Union, Set
import aiohttp
import asyncio
import os
from datetime import datetime
import logging
from pydantic import BaseModel, field_validator, RootModel, Field
import uvicorn
from contextlib import asynccontextmanager
import gc
import threading
import time
from urllib.parse import unquote
import psutil
from aiohttp import TCPConnector
import traceback
import hashlib
import json
from db_vehicle_reader import db_vehicle_reader, initialize_db_vehicle_reader

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
MAX_RETRIES = int(os.getenv("GTFS_MAX_RETRIES", "3"))  # Maximum number of retries for API calls
RETRY_DELAY = int(os.getenv("GTFS_RETRY_DELAY", "5"))  # Delay between retries in seconds
RATE_LIMIT_DELAY = float(os.getenv("GTFS_RATE_LIMIT_DELAY", "0.1"))  # Delay between API calls in seconds
CPU_THRESHOLD = float(os.getenv("GTFS_CPU_THRESHOLD", "80.0"))  # CPU usage threshold percentage
CONNECTION_LIMIT = int(os.getenv("GTFS_CONNECTION_LIMIT", "100"))  # Maximum number of concurrent connections
DNS_TTL = int(os.getenv("GTFS_DNS_TTL", "300"))  # DNS cache TTL in seconds
MEMORY_THRESHOLD = int(os.getenv("GTFS_MEMORY_THRESHOLD", "5000"))  # Memory usage threshold in MB

# Add rate limiting semaphore
api_semaphore = asyncio.Semaphore(5)  # Limit concurrent API calls

def clean_identifier(identifier: str) -> str:
    """
    Clean an identifier by:
    1. URL decoding (e.g., %3A -> :)
    2. Removing GTFS ID prefix if present (e.g., chennai_data:12345 -> 12345)
    
    Args:
        identifier: The identifier to clean (could be route code, stop code, etc.)
    
    Returns:
        Cleaned identifier without GTFS ID prefix
    """
    # First decode URL encoding
    decoded = unquote(identifier).split(':')[-1]
    
    # Remove GTFS ID prefix if present (format: gtfs_id:code)
    
    
    return decoded

# Create a shared session with optimized settings
class OptimizedClientSession:
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    async def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    # Configure TCP connector with connection pooling and memory optimizations
                    connector = TCPConnector(
                        limit=CONNECTION_LIMIT,
                        ttl_dns_cache=DNS_TTL,
                        use_dns_cache=True,
                        force_close=False,  # Changed to False to allow keepalive
                        enable_cleanup_closed=True,
                        keepalive_timeout=10  # Keep connections alive for 10 seconds
                    )
                    
                    # Create session with optimized settings
                    cls._instance = aiohttp.ClientSession(
                        connector=connector,
                        timeout=aiohttp.ClientTimeout(
                            total=30,
                            connect=10,
                            sock_read=10
                        ),
                        headers={
                            'Connection': 'keep-alive',
                            'Accept-Encoding': 'gzip, deflate'
                        },
                        auto_decompress=True,  # Enable automatic decompression
                        raise_for_status=True
                    )
        return cls._instance

    @classmethod
    async def close_instance(cls):
        if cls._instance is not None:
            with cls._lock:
                if cls._instance is not None:
                    await cls._instance.close()
                    cls._instance = None

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

# Response models for vehicle endpoints
class VehicleServiceTypeResponse(BaseModel):
    vehicle_no: str
    service_type: str
    waybill_id: Optional[str] = None
    schedule_no: Optional[str] = None
    last_updated: Optional[datetime] = None

def castVehicleType(vehicle_type:str) -> str:
    if vehicle_type == "RAIL":
        return "METRO"
    else:
        return vehicle_type

# Pydantic model for GTFS stops
class GTFSStop(BaseModel):
    id: str
    code: str
    name: str
    lat: float
    lon: float
    stationId: Optional[str] = None
    cluster: Optional[str] = None

class GTFSData:
    def __init__(self):
        self.routes: Dict[str, NandiRoutesRes] = {}
        self.route_stop_map: Dict[str, Dict[str, List[RouteStopMapping]]] = {}
        self.stop_route_map: Dict[str, Dict[str, List[RouteStopMapping]]] = {}
        self.routes_by_gtfs: Dict[str, Dict[str, NandiRoutesRes]] = {}
        self.last_update: Dict[str, datetime] = {
            "routes": datetime.min
        }
        self.data_hash: Dict[str, str] = {}  # Store hash per gtfs_id
        self._lock = threading.Lock()
        self.children_by_parent: Dict[str, Dict[str, List[str]]] = {}  # gtfs_id -> parent stop_code -> [child stop_code]

    def update_data(self, temp_data: 'GTFSData'):
        """Atomically update all data structures"""
        with self._lock:
            self.routes = temp_data.routes
            self.route_stop_map = temp_data.route_stop_map
            self.stop_route_map = temp_data.stop_route_map
            self.last_update = temp_data.last_update
            self.routes_by_gtfs = temp_data.routes_by_gtfs
            self.data_hash = temp_data.data_hash
            self.children_by_parent = temp_data.children_by_parent

# At the top of your file
is_ready = False

# Helper function to compute a hash from a data structure
def compute_data_hash(data) -> str:
    # Use json.dumps with sort_keys for deterministic output
    data_str = json.dumps(data, sort_keys=True, default=lambda o: o.__dict__ if hasattr(o, '__dict__') else str(o))
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()


async def initial_data_load():
    """Load initial data before server starts"""
    logger.info("Starting initial data load...")
    try:
        session = await OptimizedClientSession.get_instance()
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
                    vehicleType=castVehicleType(routes_by_gtfs[gtfs_id][route_code].mode if routes_by_gtfs[gtfs_id][route_code].mode != None else "UNKNOWN")
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

        # Compute and store hash for each gtfs_id
        gtfs_data.data_hash = {}
        for gtfs_id in routes_by_gtfs:
            # Only hash the relevant data for this gtfs_id
            relevant = {
                'routes': {k: v.model_dump() if hasattr(v, 'model_dump') else v for k, v in routes_by_gtfs[gtfs_id].items()},
                'route_stop_map': {k: [m.model_dump() if hasattr(m, 'model_dump') else m for m in v] for k, v in route_stop_map.get(gtfs_id, {}).items()},
                'stop_route_map': {k: [m.model_dump() if hasattr(m, 'model_dump') else m for m in v] for k, v in stop_route_map.get(gtfs_id, {}).items()},
            }
            gtfs_data.data_hash[gtfs_id] = compute_data_hash(relevant)
        logger.info(f"Initial data load complete: {len(routes)} routes, {total_patterns} patterns")
        # Set global is_ready to True
        global is_ready
        is_ready = True

        # Fetch stops and build children mapping
        stops = await fetch_stops(session)
        gtfs_data.children_by_parent = {}
        stops_by_gtfs_temp = {}
        for stop in stops:
            gtfs_id = stop.id.split(':')[0]
            if gtfs_id not in stops_by_gtfs_temp:
                stops_by_gtfs_temp[gtfs_id] = []
            stops_by_gtfs_temp[gtfs_id].append(stop)
        for gtfs_id, stops_list in stops_by_gtfs_temp.items():
            children_map = {}
            for stop in stops_list:
                if stop.stationId:
                    parent_code = stop.stationId.split(':')[-1]
                    if parent_code not in children_map:
                        children_map[parent_code] = []
                    children_map[parent_code].append(stop.id.split(':')[-1])
            gtfs_data.children_by_parent[gtfs_id] = children_map
    except Exception as e:
        logger.error(f"Error during initial data load: {str(e)}")
        raise

def garbage_collector():
    """Background thread for periodic garbage collection"""
    while True:
        try:
            # Check CPU usage before running GC
            process = psutil.Process()
            cpu_percent = process.cpu_percent(interval=1.0)
            memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB
            
            logger.info(f"Current CPU usage: {cpu_percent:.1f}%, Memory usage: {memory_usage:.2f} MB")
            
            # Only run GC if CPU usage is below threshold or memory usage is high
            if cpu_percent < CPU_THRESHOLD or memory_usage > 500:
                collected = gc.collect()
                logger.debug(f"Garbage collector ran, collected {collected} objects")
                
                if memory_usage > 500:  # 500MB threshold
                    gc.collect(2)  # Force full collection
                    logger.warning(f"High memory usage detected: {memory_usage:.2f} MB")
            else:
                logger.debug(f"Skipping GC due to high CPU usage: {cpu_percent:.1f}%")
                
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
        
        # Initialize database vehicle reader (with graceful failure handling)
        try:
            await initialize_db_vehicle_reader()
        except Exception as e:
            logger.error(f"Failed to initialize vehicle reader: {str(e)}")
            logger.warning("Service will continue without vehicle data functionality")
        
        yield
        # Shutdown: Cancel the polling tasks and close session
        polling_task_instance.cancel()
        try:
            await polling_task_instance
        except asyncio.CancelledError:
            pass
        await OptimizedClientSession.close_instance()
    except Exception as e:
        logger.error(f"Failed to start service: {str(e)}")
        raise

# Initialize FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)
gtfs_data = GTFSData()

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, retries: int = MAX_RETRIES) -> dict:
    """Fetch data from API with retries and rate limiting"""
    start_time = time.time()
    for attempt in range(retries):
        try:
            async with api_semaphore:  # Rate limit API calls
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        # elapsed = time.time() - start_time
                        # logger.info(f"API call to {url} completed in {elapsed:.3f}s")
                        return data
                    elif response.status == 429:  # Too Many Requests
                        wait_time = int(response.headers.get('Retry-After', RETRY_DELAY))
                        logger.warning(f"Rate limited, waiting {wait_time} seconds")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise HTTPException(status_code=response.status, detail=f"API request failed: {url}")
        except Exception as e:
            if attempt == retries - 1:
                raise
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
    raise HTTPException(status_code=500, detail=f"All retry attempts failed for {url}")

async def fetch_patterns(session: aiohttp.ClientSession) -> List[NandiPattern]:
    """Fetch all patterns from the API"""
    data = await fetch_with_retry(session, f"{BASE_URL}/otp/routers/default/index/patterns")
    return [NandiPattern(**item) for item in data]

async def fetch_pattern_details(session: aiohttp.ClientSession, pattern_id: str) -> NandiPatternDetails:
    """Fetch specific pattern details from the API"""
    data = await fetch_with_retry(session, f"{BASE_URL}/otp/routers/default/index/patterns/{pattern_id}")
    return NandiPatternDetails(**data)

async def fetch_routes(session: aiohttp.ClientSession) -> List[NandiRoutesRes]:
    """Fetch all routes from the API"""
    data = await fetch_with_retry(session, f"{BASE_URL}/otp/routers/default/index/routes")
    return [NandiRoutesRes(
        **item,
        startPoint=LatLong(lat=0.0, lon=0.0),
        endPoint=LatLong(lat=0.0, lon=0.0)
    ) for item in data]

async def fetch_stops(session: aiohttp.ClientSession) -> List[GTFSStop]:
    """Fetch all stops from the API and parse as GTFSStop models"""
    data = await fetch_with_retry(session, f"{BASE_URL}/otp/routers/default/index/stops")
    return [GTFSStop(**item) for item in data]

async def process_pattern_batch(session: aiohttp.ClientSession, patterns: List[NandiPattern], 
                              batch_size: int = 50) -> Dict[str, NandiPatternDetails]:  # Reduced batch size
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
        # Clear batch data
        del batch
        del tasks
        del results
    
    return pattern_details

async def polling_task():
    """Main polling task that updates all data periodically"""
    session = None
    while True:
        try:
            if session is None:
                session = await OptimizedClientSession.get_instance()
            # Check CPU usage before starting update
            process = psutil.Process()
            cpu_percent = process.cpu_percent(interval=1.0)
            memory_usage = process.memory_info().rss / 1024 / 1024
            
            if cpu_percent > CPU_THRESHOLD or memory_usage > MEMORY_THRESHOLD:  # Added memory threshold
                logger.warning(f"High resource usage detected (CPU: {cpu_percent:.1f}%, Memory: {memory_usage:.1f}MB), delaying update")
                await asyncio.sleep(POLLING_INTERVAL)
                continue
            try:
                # Fetch routes
                routes = await fetch_routes(session)
                routes_by_gtfs = {}
                for route in routes:
                    try:
                        gtfs_id = str(route.id.split(':')[0])
                        route_code = str(route.id.split(':')[-1])
                        if gtfs_id not in routes_by_gtfs:
                            routes_by_gtfs[gtfs_id] = {}
                        routes_by_gtfs[gtfs_id][route_code] = route
                    except Exception as e:
                        logger.error(f"Error processing route {route.id}: {str(e)}")
                        continue

                # Fetch all patterns
                patterns = await fetch_patterns(session)
                batch_size = 25  # Reduced batch size for better memory management
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
                        try:
                            gtfs_id = str(pattern.routeId.split(':')[0])
                            route_code = str(pattern.routeId.split(':')[-1])
                            if gtfs_id not in route_stop_map:
                                route_stop_map[gtfs_id] = {}
                            if route_code not in route_stop_map[gtfs_id]:
                                route_stop_map[gtfs_id][route_code] = []
                            if gtfs_id not in stop_route_map:
                                stop_route_map[gtfs_id] = {}
                            for seq, stop in enumerate(pattern.stops):
                                try:
                                    mapping = RouteStopMapping(
                                        estimatedTravelTimeFromPreviousStop=None,
                                        providerCode="GTFS",
                                        routeCode=route_code,
                                        sequenceNum=seq,
                                        stopCode=str(stop.code),
                                        stopName=str(stop.name),
                                        stopPoint=LatLong(lat=float(stop.lat), lon=float(stop.lon)),
                                        vehicleType=castVehicleType(routes_by_gtfs[gtfs_id][route_code].mode if routes_by_gtfs[gtfs_id][route_code].mode != None else "UNKNOWN")
                                    )
                                    route_stop_map[gtfs_id][route_code].append(mapping)
                                    if stop.code not in stop_route_map[gtfs_id]:
                                        stop_route_map[gtfs_id][stop.code] = []
                                    stop_route_map[gtfs_id][stop.code].append(mapping)
                                except Exception as e:
                                    logger.error(f"Error processing stop {stop.code} for route {route_code}: {str(e)}")
                                    continue
                        except Exception as e:
                            logger.error(f"Error processing pattern {pattern.id}: {str(e)}")
                            continue
                    
                    # Free memory after batch
                    del batch_details
                    del batch
                    gc.collect()

                if routes:
                    temp_data = GTFSData()
                    temp_data.routes = {str(route.id): route for route in routes}
                    temp_data.route_stop_map = route_stop_map
                    temp_data.stop_route_map = stop_route_map
                    current_time = datetime.now()
                    temp_data.last_update["routes"] = current_time
                    temp_data.routes_by_gtfs = routes_by_gtfs
                    # Compute and store hash for each gtfs_id
                    temp_data.data_hash = {}
                    for gtfs_id in routes_by_gtfs:
                        relevant = {
                            'routes': {k: v.model_dump() if hasattr(v, 'model_dump') else v for k, v in routes_by_gtfs[gtfs_id].items()},
                            'route_stop_map': {k: [m.model_dump() if hasattr(m, 'model_dump') else m for m in v] for k, v in route_stop_map.get(gtfs_id, {}).items()},
                            'stop_route_map': {k: [m.model_dump() if hasattr(m, 'model_dump') else m for m in v] for k, v in stop_route_map.get(gtfs_id, {}).items()},
                        }
                        temp_data.data_hash[gtfs_id] = compute_data_hash(relevant)
                    # Fetch stops and build children mapping
                    stops = await fetch_stops(session)
                    temp_data.children_by_parent = {}
                    stops_by_gtfs_temp = {}
                    for stop in stops:
                        gtfs_id = stop.id.split(':')[0]
                        if gtfs_id not in stops_by_gtfs_temp:
                            stops_by_gtfs_temp[gtfs_id] = []
                        stops_by_gtfs_temp[gtfs_id].append(stop)
                    for gtfs_id, stops_list in stops_by_gtfs_temp.items():
                        children_map = {}
                        for stop in stops_list:
                            if stop.stationId:
                                parent_code = stop.stationId.split(':')[-1]
                                if parent_code not in children_map:
                                    children_map[parent_code] = []
                                children_map[parent_code].append(stop.id.split(':')[-1])
                        temp_data.children_by_parent[gtfs_id] = children_map
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
                logger.error(f"Error in polling task iteration: {str(e)}")
                logger.error(f"Error details: {traceback.format_exc()}")
            await asyncio.sleep(POLLING_INTERVAL)
        except Exception as e:
            logger.error(f"Fatal error in polling task: {str(e)}")
            logger.error(f"Error details: {traceback.format_exc()}")
            await asyncio.sleep(POLLING_INTERVAL)
    if session:
        await session.close()

# API endpoints
@app.get("/route/{gtfs_id}/{route_id}", response_model=NandiRoutesRes)
async def get_route(gtfs_id: str, route_id: str):
    """Get specific route"""
    gtfs_id = clean_identifier(gtfs_id)
    route_id = clean_identifier(route_id)
    if gtfs_id not in gtfs_data.routes_by_gtfs or route_id not in gtfs_data.routes_by_gtfs[gtfs_id]:
        raise HTTPException(status_code=404, detail="Route not found")
    return gtfs_data.routes_by_gtfs[gtfs_id][route_id]

@app.get("/routes/{gtfs_id}", response_model=List[NandiRoutesRes])
async def get_routes(gtfs_id: str):
    gtfs_id = clean_identifier(gtfs_id)
    if gtfs_id not in gtfs_data.routes_by_gtfs:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    return list(gtfs_data.routes_by_gtfs[gtfs_id].values())

@app.get("/route-stop-mapping/{gtfs_id}/route/{route_code}", response_model=List[RouteStopMapping])
async def get_route_stop_mapping_by_route(gtfs_id: str, route_code: str):
    gtfs_id = clean_identifier(gtfs_id)
    route_code = clean_identifier(route_code)
    if gtfs_id not in gtfs_data.route_stop_map or route_code not in gtfs_data.route_stop_map[gtfs_id]:
        raise HTTPException(status_code=404, detail="Route code not found for GTFS ID")
    return gtfs_data.route_stop_map[gtfs_id][route_code]

@app.get("/route-stop-mapping/{gtfs_id}/stop/{stop_code}", response_model=List[RouteStopMapping])
async def get_route_stop_mapping_by_stop(gtfs_id: str, stop_code: str):
    gtfs_id = clean_identifier(gtfs_id)
    stop_code = clean_identifier(stop_code)
    if gtfs_id not in gtfs_data.stop_route_map or stop_code not in gtfs_data.stop_route_map[gtfs_id]:
        raise HTTPException(status_code=404, detail="Stop code not found for GTFS ID")
    return gtfs_data.stop_route_map[gtfs_id][stop_code]

@app.get("/routes/{gtfs_id}/fuzzy/{query}", response_model=List[NandiRoutesRes])
async def get_routes_fuzzy(gtfs_id: str, query: str, limit: Optional[int] = None):
    gtfs_id = clean_identifier(gtfs_id)
    query = clean_identifier(query)
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
    gtfs_id = clean_identifier(gtfs_id)
    if gtfs_id not in gtfs_data.stop_route_map:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    
    # Return the first mapping for each stop since they are already deduplicated
    return [mappings[0] for mappings in gtfs_data.stop_route_map[gtfs_id].values()]

@app.get("/stop/{gtfs_id}/{stop_code}", response_model=RouteStopMapping)
async def get_stop(gtfs_id: str, stop_code: str):
    gtfs_id = clean_identifier(gtfs_id)
    stop_code = clean_identifier(stop_code)
    if gtfs_id not in gtfs_data.stop_route_map or stop_code not in gtfs_data.stop_route_map[gtfs_id]:
        raise HTTPException(status_code=404, detail="Stop code not found for GTFS ID")
    result = gtfs_data.stop_route_map[gtfs_id][stop_code]
    if len(result) > 0:
        return result[0]
    else:
        raise HTTPException(status_code=404, detail="Stop code not found for GTFS ID")

@app.get("/stops/{gtfs_id}/fuzzy/{query}", response_model=List[RouteStopMapping])
async def get_stops_fuzzy(gtfs_id: str, query: str, limit: Optional[int] = None):
    gtfs_id = clean_identifier(gtfs_id)
    query = clean_identifier(query).lower()
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

# API endpoint to get the current data hash (version) for a GTFS ID
@app.get("/version/{gtfs_id}")
async def get_version(gtfs_id: str):
    gtfs_id = clean_identifier(gtfs_id)
    if gtfs_id not in gtfs_data.data_hash:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    return {"gtfs_id": gtfs_id, "version": gtfs_data.data_hash[gtfs_id]}

# API endpoint to get children station codes for a given gtfs_id and stop_code
@app.get("/station-children/{gtfs_id}/{stop_code}", response_model=List[str])
async def get_station_children(gtfs_id: str, stop_code: str):
    gtfs_id = clean_identifier(gtfs_id)
    stop_code = clean_identifier(stop_code)
    if gtfs_id not in gtfs_data.children_by_parent:
        raise HTTPException(status_code=404, detail="GTFS ID not found")
    children = gtfs_data.children_by_parent[gtfs_id].get(stop_code, [])
    return children

# Vehicle data endpoints from database
@app.get("/vehicle/{vehicle_no}/service-type", response_model=VehicleServiceTypeResponse)
async def get_service_type_by_vehicle(vehicle_no: str):
    """Get service type for a specific vehicle number"""
    try:
        vehicle_no = clean_identifier(vehicle_no)
        vehicle_data = await db_vehicle_reader.get_vehicle_data(vehicle_no)
        if not vehicle_data:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        return {"vehicle_no": vehicle_no, "service_type": vehicle_data.service_type, "waybill_id": vehicle_data.waybill_id, "schedule_no": vehicle_data.schedule_no, "last_updated": vehicle_data.last_updated}
    except HTTPException:
        # Re-raise HTTP exceptions as they are intentional
        raise
    except Exception as e:
        logger.error(f"Error in vehicle service type endpoint: {str(e)}")
        raise HTTPException(status_code=503, detail="Vehicle service temporarily unavailable")

@app.get("/vehicle/cache/stats")
async def get_vehicle_cache_stats():
    """Get vehicle cache statistics"""
    try:
        stats = db_vehicle_reader.get_cache_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        raise HTTPException(status_code=503, detail="Cache stats temporarily unavailable")

@app.post("/vehicle/cache/clear")
async def clear_vehicle_cache():
    """Clear the vehicle cache"""
    try:
        db_vehicle_reader.clear_cache()
        return {"message": "Vehicle cache cleared successfully"}
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        raise HTTPException(status_code=503, detail="Failed to clear cache")

@app.get("/vehicle/status")
async def get_vehicle_reader_status():
    """Get vehicle reader status and cache information"""
    try:
        status = db_vehicle_reader.get_status()
        return status
    except Exception as e:
        logger.error(f"Error getting vehicle reader status: {str(e)}")
        raise HTTPException(status_code=503, detail="Status temporarily unavailable")

if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)