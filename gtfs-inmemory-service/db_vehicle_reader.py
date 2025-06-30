import asyncio
import logging
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
from pydantic import BaseModel, field_validator
import asyncpg
import traceback

# Configure logging
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Test log message to verify logging is working
logger.info("db_vehicle_reader module loaded successfully")
logger.critical("CRITICAL: db_vehicle_reader module logging test - this should always appear")
print("PRINT: db_vehicle_reader module loaded - print statement fallback")

# Environment Variables
WAYBILLS_DB_USER = os.getenv("WAYBILLS_DB_USER", "postgres")
WAYBILLS_DB_PASS = os.getenv("WAYBILLS_DB_PASS", "postgres")
WAYBILLS_DB_HOST = os.getenv("WAYBILLS_DB_HOST", "localhost")
WAYBILLS_DB_PORT = int(os.getenv("WAYBILLS_DB_PORT", "5432"))
WAYBILLS_DB_NAME = os.getenv("WAYBILLS_DB_NAME", "waybills")
WAYBILLS_DB_MAX_CONNECTIONS = int(os.getenv("WAYBILLS_DB_MAX_CONNECTIONS", "10"))

class VehicleData(BaseModel):
    waybill_id: str
    service_type: str
    vehicle_no: str
    schedule_no: str
    last_updated: Optional[datetime] = None
    duty_date: Optional[date] = None

    @field_validator('waybill_id', 'schedule_no', mode='before')
    @classmethod
    def convert_to_str(cls, v):
        """Convert any value to string"""
        return str(v) if v is not None else None

class DBVehicleReader:
    def __init__(self):
        self.db_pool = None
        self.is_initialized = False
        self.last_error = None
        self.error_count = 0
        self.max_errors = 5  # Max consecutive errors before giving up
        self._initialization_lock = asyncio.Lock()  # Prevent multiple simultaneous initializations
        
        # Date-based in-memory cache: {vehicle_no: (vehicle_data, cache_date)}
        self.vehicle_cache: Dict[str, Tuple[VehicleData, date]] = {}
        self.cache_hits = 0
        self.cache_misses = 0

    async def initialize_db_connection(self) -> bool:
        """Initialize database connection with error handling - only creates one pool"""
        async with self._initialization_lock:
            # If already initialized and pool is healthy, return True
            if self.is_initialized and self.db_pool and not self.db_pool.is_closed():
                try:
                    # Test the connection to make sure it's still working
                    async with self.db_pool.acquire() as conn:
                        await conn.execute("SELECT 1")
                    logger.debug("Database connection already initialized and healthy")
                    return True
                except Exception as e:
                    logger.warning(f"Existing pool is unhealthy, recreating: {str(e)}")
                    # Fall through to recreate the pool
            
            try:
                logger.debug("Starting database connection initialization...")
                
                # Close existing pool if it exists
                if self.db_pool and not self.db_pool.is_closed():
                    await self.db_pool.close()
                
                logger.debug(f"Connecting to database: {WAYBILLS_DB_HOST}:{WAYBILLS_DB_PORT}/{WAYBILLS_DB_NAME}")
                logger.debug(f"Max connections: {WAYBILLS_DB_MAX_CONNECTIONS}")
                
                self.db_pool = await asyncpg.create_pool(
                    user=WAYBILLS_DB_USER,
                    password=WAYBILLS_DB_PASS,
                    host=WAYBILLS_DB_HOST,
                    port=WAYBILLS_DB_PORT,
                    database=WAYBILLS_DB_NAME,
                    min_size=1,
                    max_size=WAYBILLS_DB_MAX_CONNECTIONS,
                    command_timeout=60,
                    server_settings={
                        'application_name': 'gtfs_vehicle_reader'
                    }
                )
                
                # Test the connection
                logger.debug("Testing database connection...")
                async with self.db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                
                self.is_initialized = True
                self.error_count = 0
                self.last_error = None
                logger.info(f"Database connection initialized successfully with max {WAYBILLS_DB_MAX_CONNECTIONS} connections")
                return True
                
            except Exception as e:
                self.is_initialized = False
                self.last_error = str(e)
                self.error_count += 1
                logger.error(f"Failed to initialize database connection: {str(e)}")
                logger.error(f"Error count: {self.error_count}/{self.max_errors}")
                logger.error(f"Exception details: {traceback.format_exc()}")
                return False

    async def _ensure_connection(self) -> bool:
        """Ensure database connection is available, initialize if needed"""
        if not self.is_initialized or not self.db_pool or self.db_pool.is_closed():
            return await self.initialize_db_connection()
        return True

    def _get_cached_vehicle_data(self, vehicle_no: str) -> Optional[VehicleData]:
        """Get vehicle data from cache if it exists for today's date"""
        try:
            if vehicle_no in self.vehicle_cache:
                vehicle_data, cache_date = self.vehicle_cache[vehicle_no]
                today = date.today()
                
                if cache_date == today:
                    self.cache_hits += 1
                    logger.debug(f"Cache HIT for vehicle {vehicle_no}")
                    return vehicle_data
                else:
                    # Cache is stale, remove it
                    del self.vehicle_cache[vehicle_no]
                    logger.debug(f"Cache STALE for vehicle {vehicle_no}, removed")
            
            self.cache_misses += 1
            logger.debug(f"Cache MISS for vehicle {vehicle_no}")
            return None
            
        except Exception as e:
            logger.error(f"Error checking cache for vehicle {vehicle_no}: {str(e)}")
            return None

    def _cache_vehicle_data(self, vehicle_no: str, vehicle_data: VehicleData) -> None:
        """Cache vehicle data with today's date"""
        try:
            today = date.today()
            self.vehicle_cache[vehicle_no] = (vehicle_data, today)
            logger.debug(f"Cached vehicle data for {vehicle_no}")
        except Exception as e:
            logger.error(f"Error caching vehicle data for {vehicle_no}: {str(e)}")

    async def get_vehicle_data(self, vehicle_no: str) -> Optional[VehicleData]:
        """Get vehicle data with date-based caching"""
        try:
            # First check cache
            cached_data = self._get_cached_vehicle_data(vehicle_no)
            if cached_data:
                return cached_data
            
            # Ensure connection is available
            if not await self._ensure_connection():
                logger.warning("Could not establish database connection, returning None for vehicle data")
                return None
            
            # Check if we've had too many consecutive errors
            if self.error_count >= self.max_errors:
                logger.warning(f"Too many consecutive errors ({self.error_count}), attempting to reinitialize")
                if not await self.initialize_db_connection():
                    logger.error("Failed to reinitialize database after multiple errors")
                    return None
            
            async with self.db_pool.acquire() as conn:
                # First try to get current date data
                query = """
                SELECT 
                  waybill_id,
                  service_type,
                  vehicle_no,
                  schedule_no,
                  updated_at as last_updated,
                  duty_date
                FROM waybills 
                WHERE vehicle_no = $1 AND duty_date = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
                ORDER BY updated_at DESC 
                LIMIT 1
                """
                
                row = await conn.fetchrow(query, vehicle_no)
                
                # If no current date data, get the latest data for this vehicle
                if not row:
                    query = """
                    SELECT 
                      waybill_id,
                      service_type,
                      vehicle_no,
                      schedule_no,
                      updated_at as last_updated,
                      duty_date
                    FROM waybills 
                    WHERE vehicle_no = $1
                    ORDER BY updated_at DESC 
                    LIMIT 1
                    """
                    row = await conn.fetchrow(query, vehicle_no)
                
                if row:
                    logger.info(f"Raw database row for vehicle {vehicle_no}: {dict(row)}")
                    logger.info(f"Data types: waybill_id={type(row['waybill_id'])}, schedule_no={type(row['schedule_no'])}")
                    
                    vehicle = VehicleData(
                        waybill_id=row['waybill_id'],
                        service_type=row['service_type'],
                        vehicle_no=row['vehicle_no'],
                        schedule_no=row['schedule_no'],
                        last_updated=row['last_updated'],
                        duty_date=row['duty_date']
                    )
                    logger.info(f"Found vehicle data for {vehicle_no}: {vehicle.service_type}")
                    
                    # Cache the data
                    self._cache_vehicle_data(vehicle_no, vehicle)
                    
                    return vehicle
                else:
                    logger.debug(f"No vehicle data found for {vehicle_no}")
                    return None
                    
        except Exception as e:
            self.last_error = str(e)
            self.error_count += 1
            logger.error(f"Error getting vehicle data for {vehicle_no}: {str(e)}")
            logger.error(f"Error count: {self.error_count}/{self.max_errors}")
            return None

    async def get_all_vehicles(self) -> List[VehicleData]:
        """Get all vehicles directly from database with error handling"""
        try:
            if not await self._ensure_connection():
                logger.warning("Could not establish database connection, returning empty list")
                return []
            
            async with self.db_pool.acquire() as conn:
                query = """
                SELECT DISTINCT ON (vehicle_no)
                  waybill_id,
                  service_type,
                  vehicle_no,
                  schedule_no,
                  updated_at as last_updated,
                  duty_date
                FROM waybills
                ORDER BY vehicle_no, updated_at DESC
                """
                
                rows = await conn.fetch(query)
                vehicles = []
                for row in rows:
                    vehicle = VehicleData(
                        waybill_id=row['waybill_id'],
                        service_type=row['service_type'],
                        vehicle_no=row['vehicle_no'],
                        schedule_no=row['schedule_no'],
                        last_updated=row['last_updated'],
                        duty_date=row['duty_date']
                    )
                    vehicles.append(vehicle)
                
                logger.info(f"Fetched {len(vehicles)} vehicles from database")
                return vehicles
                
        except Exception as e:
            self.last_error = str(e)
            self.error_count += 1
            logger.error(f"Error getting all vehicles: {str(e)}")
            logger.error(f"Error count: {self.error_count}/{self.max_errors}")
            return []

    async def get_vehicles_by_service_type(self, service_type: str) -> List[VehicleData]:
        """Get vehicles by service type directly from database with error handling"""
        try:
            if not await self._ensure_connection():
                logger.warning("Could not establish database connection, returning empty list")
                return []
            
            async with self.db_pool.acquire() as conn:
                query = """
                SELECT DISTINCT ON (vehicle_no)
                  waybill_id,
                  service_type,
                  vehicle_no,
                  schedule_no,
                  updated_at as last_updated,
                  duty_date
                FROM waybills
                WHERE service_type = $1
                ORDER BY vehicle_no, updated_at DESC
                """
                
                rows = await conn.fetch(query, service_type)
                vehicles = []
                for row in rows:
                    vehicle = VehicleData(
                        waybill_id=row['waybill_id'],
                        service_type=row['service_type'],
                        vehicle_no=row['vehicle_no'],
                        schedule_no=row['schedule_no'],
                        last_updated=row['last_updated'],
                        duty_date=row['duty_date']
                    )
                    vehicles.append(vehicle)
                
                logger.info(f"Fetched {len(vehicles)} vehicles with service type {service_type}")
                return vehicles
                
        except Exception as e:
            self.last_error = str(e)
            self.error_count += 1
            logger.error(f"Error getting vehicles by service type {service_type}: {str(e)}")
            logger.error(f"Error count: {self.error_count}/{self.max_errors}")
            return []

    async def search_vehicles(self, query: str) -> List[VehicleData]:
        """Search vehicles directly from database with error handling"""
        try:
            if not await self._ensure_connection():
                logger.warning("Could not establish database connection, returning empty list")
                return []
            
            async with self.db_pool.acquire() as conn:
                search_pattern = f"%{query}%"
                query_sql = """
                SELECT DISTINCT ON (vehicle_no)
                  waybill_id,
                  service_type,
                  vehicle_no,
                  schedule_no,
                  updated_at as last_updated,
                  duty_date
                FROM waybills
                WHERE vehicle_no ILIKE $1 OR waybill_id ILIKE $1 OR schedule_no ILIKE $1
                ORDER BY vehicle_no, updated_at DESC
                """
                
                rows = await conn.fetch(query_sql, search_pattern)
                vehicles = []
                for row in rows:
                    vehicle = VehicleData(
                        waybill_id=row['waybill_id'],
                        service_type=row['service_type'],
                        vehicle_no=row['vehicle_no'],
                        schedule_no=row['schedule_no'],
                        last_updated=row['last_updated'],
                        duty_date=row['duty_date']
                    )
                    vehicles.append(vehicle)
                
                logger.info(f"Found {len(vehicles)} vehicles matching query '{query}'")
                return vehicles
                
        except Exception as e:
            self.last_error = str(e)
            self.error_count += 1
            logger.error(f"Error searching vehicles with query {query}: {str(e)}")
            logger.error(f"Error count: {self.error_count}/{self.max_errors}")
            return []

    async def get_vehicle_count(self) -> int:
        """Get vehicle count directly from database with error handling"""
        try:
            if not await self._ensure_connection():
                logger.warning("Could not establish database connection, returning 0")
                return 0
            
            async with self.db_pool.acquire() as conn:
                query = "SELECT COUNT(DISTINCT vehicle_no) as count FROM waybills"
                row = await conn.fetchrow(query)
                count = row['count'] if row else 0
                logger.debug(f"Vehicle count: {count}")
                return count
                
        except Exception as e:
            self.last_error = str(e)
            self.error_count += 1
            logger.error(f"Error getting vehicle count: {str(e)}")
            logger.error(f"Error count: {self.error_count}/{self.max_errors}")
            return 0

    def clear_cache(self) -> None:
        """Clear the vehicle cache"""
        try:
            cache_size = len(self.vehicle_cache)
            self.vehicle_cache.clear()
            logger.info(f"Cleared vehicle cache ({cache_size} entries)")
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            total_requests = self.cache_hits + self.cache_misses
            hit_rate = (self.cache_hits / total_requests * 100) if total_requests > 0 else 0
            
            return {
                "cache_size": len(self.vehicle_cache),
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "total_requests": total_requests,
                "hit_rate_percent": round(hit_rate, 2)
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {}

    def get_db_connection(self):
        """Get database connection for testing"""
        return self.db_pool

    def get_status(self) -> Dict[str, Any]:
        """Get status information"""
        cache_stats = self.get_cache_stats()
        pool_info = {}
        if self.db_pool and not self.db_pool.is_closed():
            pool_info = {
                "pool_size": self.db_pool.get_size(),
                "free_size": self.db_pool.get_free_size(),
                "is_closed": self.db_pool.is_closed()
            }
        
        return {
            "is_initialized": self.is_initialized,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "db_host": WAYBILLS_DB_HOST,
            "db_name": WAYBILLS_DB_NAME,
            "max_connections": WAYBILLS_DB_MAX_CONNECTIONS,
            "pool_info": pool_info,
            "cache_stats": cache_stats
        }

    async def close(self):
        """Close the database connection pool"""
        try:
            if self.db_pool and not self.db_pool.is_closed():
                await self.db_pool.close()
                logger.info("Database connection pool closed")
            self.is_initialized = False
        except Exception as e:
            logger.error(f"Error closing database pool: {str(e)}")

# Global instance
db_vehicle_reader = DBVehicleReader()

async def initialize_db_vehicle_reader():
    """Initialize the database vehicle reader with error handling"""
    try:
        logger.info("Starting database vehicle reader initialization...")
        success = await db_vehicle_reader.initialize_db_connection()
        if success:
            logger.info("Database vehicle reader initialized successfully")
        else:
            logger.warning("Database vehicle reader initialization failed, but service will continue")
            logger.warning(f"Last error: {db_vehicle_reader.last_error}")
    except Exception as e:
        logger.error(f"Failed to initialize database vehicle reader: {str(e)}")
        logger.error(f"Exception details: {traceback.format_exc()}")
        logger.warning("Service will continue without vehicle data functionality")

async def cleanup_db_vehicle_reader():
    """Cleanup the database vehicle reader"""
    try:
        await db_vehicle_reader.close()
        logger.info("Database vehicle reader cleanup completed")
    except Exception as e:
        logger.error(f"Error during database vehicle reader cleanup: {str(e)}") 