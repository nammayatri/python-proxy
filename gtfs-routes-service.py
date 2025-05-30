from fastapi import FastAPI, HTTPException
from typing import Dict, List, Optional, Union
import aiohttp
import asyncio
import os
from datetime import datetime
import logging
from pydantic import BaseModel, field_validator
import uvicorn
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
BASE_URL = os.getenv("GTFS_BASE_URL", "http://localhost:8080")
POLLING_INTERVAL = int(os.getenv("GTFS_POLLING_INTERVAL", "60"))  # in seconds
API_HOST = os.getenv("GTFS_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("GTFS_API_PORT", "8000"))

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

class NandiPatternsRes(BaseModel):
    patterns: List[NandiPattern]

class GTFSData:
    def __init__(self):
        self.patterns_by_gtfs: Dict[str, List[NandiPattern]] = {}
        self.pattern_details: Dict[str, NandiPatternDetails] = {}
        self.routes: Dict[str, NandiRoutesRes] = {}
        self.last_update: Dict[str, datetime] = {
            "patterns": datetime.min,
            "pattern_details": datetime.min,
            "routes": datetime.min
        }

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
            
            # Organize patterns by GTFS ID
            patterns_by_gtfs = {}
            for pattern in patterns:
                gtfs_id = pattern.id.split(':')[0]  # Extract GTFS ID from pattern ID
                if gtfs_id not in patterns_by_gtfs:
                    patterns_by_gtfs[gtfs_id] = []
                patterns_by_gtfs[gtfs_id].append(pattern)
                
                try:
                    details = await fetch_pattern_details(session, pattern.id)
                    pattern_details[pattern.id] = details
                except Exception as e:
                    logger.error(f"Error fetching pattern details for {pattern.id}: {str(e)}")
                    continue
            
            # Update all data structures atomically
            gtfs_data.patterns_by_gtfs = patterns_by_gtfs
            gtfs_data.pattern_details = pattern_details
            gtfs_data.routes = {route.id: route for route in routes}
            
            # Update timestamps
            current_time = datetime.now()
            gtfs_data.last_update["patterns"] = current_time
            gtfs_data.last_update["pattern_details"] = current_time
            gtfs_data.last_update["routes"] = current_time
            
            logger.info(f"Initial data load complete: {len(patterns)} patterns across {len(patterns_by_gtfs)} GTFS IDs, {len(pattern_details)} pattern details, {len(routes)} routes")
    except Exception as e:
        logger.error(f"Error during initial data load: {str(e)}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application"""
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

async def polling_task():
    """Main polling task that updates all data periodically"""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # Fetch all data first
                patterns = await fetch_patterns(session)
                routes = await fetch_routes(session)
                
                # Create temporary storage for pattern details
                pattern_details = {}
                
                # Organize patterns by GTFS ID
                patterns_by_gtfs = {}
                for pattern in patterns:
                    gtfs_id = pattern.id.split(':')[0]  # Extract GTFS ID from pattern ID
                    if gtfs_id not in patterns_by_gtfs:
                        patterns_by_gtfs[gtfs_id] = []
                    patterns_by_gtfs[gtfs_id].append(pattern)
                    
                    try:
                        details = await fetch_pattern_details(session, pattern.id)
                        pattern_details[pattern.id] = details
                    except Exception as e:
                        logger.error(f"Error fetching pattern details for {pattern.id}: {str(e)}")
                        continue
                
                # Only update if we have all the data
                if patterns and routes:
                    # Update all data structures atomically
                    gtfs_data.patterns_by_gtfs = patterns_by_gtfs
                    gtfs_data.pattern_details = pattern_details
                    gtfs_data.routes = {route.id: route for route in routes}
                    
                    # Update timestamps
                    current_time = datetime.now()
                    gtfs_data.last_update["patterns"] = current_time
                    gtfs_data.last_update["pattern_details"] = current_time
                    gtfs_data.last_update["routes"] = current_time
                    
                    logger.info(f"Successfully updated all data: {len(patterns)} patterns across {len(patterns_by_gtfs)} GTFS IDs, {len(pattern_details)} pattern details, {len(routes)} routes")
                else:
                    logger.error("Failed to fetch complete data set, skipping update")
                    
        except Exception as e:
            logger.error(f"Error in polling task: {str(e)}")
            
        await asyncio.sleep(POLLING_INTERVAL)  # Poll at configured interval

# API endpoints
@app.get("/patterns", response_model=NandiPatternsRes)
async def get_patterns(gtfs_id: str):
    """Get patterns filtered by GTFS ID"""
    if gtfs_id not in gtfs_data.patterns_by_gtfs:
        raise HTTPException(status_code=404, detail=f"No patterns found for GTFS ID: {gtfs_id}")
    return NandiPatternsRes(patterns=gtfs_data.patterns_by_gtfs[gtfs_id])

@app.get("/patterns/{pattern_id}", response_model=NandiPatternDetails)
async def get_pattern_details(pattern_id: str):
    """Get specific pattern details"""
    if pattern_id not in gtfs_data.pattern_details:
        raise HTTPException(status_code=404, detail="Pattern not found")
    return gtfs_data.pattern_details[pattern_id]

@app.get("/routes", response_model=List[NandiRoutesRes])
async def get_routes():
    """Get all routes"""
    return list(gtfs_data.routes.values())

@app.get("/routes/{route_id}", response_model=NandiRoutesRes)
async def get_route(route_id: str):
    """Get specific route"""
    if route_id not in gtfs_data.routes:
        raise HTTPException(status_code=404, detail="Route not found")
    return gtfs_data.routes[route_id]

if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
