import boto3
import pandas as pd
import logging
import os
from typing import Dict, Optional, List
from pydantic import BaseModel
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

# Configure logging
logger = logging.getLogger(__name__)

# Environment Variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "nandi-data")
S3_CSV_KEY = os.getenv("S3_CSV_KEY", "currentWaybill/waybills.csv")
USE_IAM_ROLE = os.getenv("USE_IAM_ROLE", "false").lower() == "true"
AWS_PROFILE = os.getenv("AWS_PROFILE")

class VehicleData(BaseModel):
    """Pydantic model for vehicle data"""
    waybill_id: str
    service_type: str
    vehicle_no: str
    schedule_no: str
    last_updated: Optional[datetime] = None

class S3CSVReader:
    """Class to handle reading CSV data from S3 and creating vehicle hashmap"""
    
    def __init__(self):
        self.s3_client = None
        self.vehicle_hashmap: Dict[str, VehicleData] = {}
        self.last_etag: Optional[str] = None
        self._lock = asyncio.Lock()
        
    async def initialize_s3_client(self):
        """Initialize S3 client with credentials or IAM role"""
        try:
            if not S3_BUCKET_NAME or not S3_CSV_KEY:
                logger.error("Missing required S3 configuration: S3_BUCKET_NAME and S3_CSV_KEY")
                raise ValueError("Missing required S3 configuration")
            
            # Log S3 configuration (without sensitive data)
            logger.info(f"S3 Configuration - Bucket: {S3_BUCKET_NAME}, Region: {AWS_REGION}")
            logger.info(f"AWS Access Key ID length: {len(AWS_ACCESS_KEY_ID) if AWS_ACCESS_KEY_ID else 0}")
            logger.info(f"AWS Secret Key length: {len(AWS_SECRET_ACCESS_KEY) if AWS_SECRET_ACCESS_KEY else 0}")
            
            # Get session token from environment variable
            session_token = os.getenv('AWS_SESSION_TOKEN')
            if session_token:
                logger.info("AWS Session Token is present")
            
            # Verify credentials are not empty
            if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
                logger.error("AWS credentials are empty. Please check your environment variables:")
                logger.error("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
                raise ValueError("AWS credentials are empty")
            
            # Initialize S3 client
            try:
                self.s3_client = boto3.client(
                    's3',
                    region_name=AWS_REGION,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    aws_session_token=session_token 
                )
                logger.info("Successfully initialized S3 client")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {e}")
                raise
            
            # Test the connection by listing the bucket
            try:
                self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
                logger.info(f"Successfully connected to S3 bucket: {S3_BUCKET_NAME}")
            except Exception as e:
                logger.error(f"Failed to access S3 bucket {S3_BUCKET_NAME}: {str(e)}")
                raise
                
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise
    
    async def check_file_updated(self) -> bool:
        """Check if the CSV file has been updated by comparing ETags"""
        try:
            if not self.s3_client:
                await self.initialize_s3_client()
            
            response = self.s3_client.head_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_CSV_KEY
            )
            
            current_etag = response.get('ETag', '').strip('"')
            
            if self.last_etag != current_etag:
                self.last_etag = current_etag
                logger.info(f"CSV file updated. New ETag: {current_etag}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking file update: {str(e)}")
            return False
    
    async def read_csv_from_s3(self) -> pd.DataFrame:
        """Read CSV file from S3 bucket"""
        try:
            if not self.s3_client:
                await self.initialize_s3_client()
            
            logger.info(f"Reading CSV from s3://{S3_BUCKET_NAME}/{S3_CSV_KEY}")
            
            # Get the CSV object from S3
            response = self.s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_CSV_KEY
            )
            
            # Read CSV data
            df = pd.read_csv(response['Body'])
            
            # Validate required columns
            required_columns = ['waybill_id', 'service_type', 'vehicle_no', 'schedule_no']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            logger.info(f"Successfully read CSV with {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV from S3: {str(e)}")
            raise
    
    async def create_vehicle_hashmap(self, df: pd.DataFrame) -> Dict[str, VehicleData]:
        """Create hashmap from DataFrame mapping vehicle_no to VehicleData objects"""
        try:
            vehicle_hashmap = {}
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                try:
                    # Create VehicleData object
                    vehicle_data = VehicleData(
                        waybill_id=str(row['waybill_id']),
                        service_type=str(row['service_type'].upper()),
                        vehicle_no=str(row['vehicle_no']),
                        schedule_no=str(row['schedule_no']),
                        last_updated=current_time
                    )
                    
                    # Use vehicle_no as key
                    vehicle_hashmap[vehicle_data.vehicle_no] = vehicle_data
                    
                except Exception as e:
                    logger.warning(f"Error processing row {row}: {str(e)}")
                    continue
            
            logger.info(f"Created vehicle hashmap with {len(vehicle_hashmap)} entries")
            return vehicle_hashmap
            
        except Exception as e:
            logger.error(f"Error creating vehicle hashmap: {str(e)}")
            raise
    
    async def update_vehicle_data(self):
        """Update vehicle data from S3 CSV"""
        try:
            async with self._lock:
                # Check if file has been updated
                if not await self.check_file_updated():
                    logger.debug("CSV file not updated, skipping data refresh")
                    return
                
                # Read CSV from S3
                df = await self.read_csv_from_s3()
                
                # Create new hashmap
                new_hashmap = await self.create_vehicle_hashmap(df)
                
                # Update the hashmap atomically
                self.vehicle_hashmap = new_hashmap
                
                logger.info(f"Successfully updated vehicle data. Total vehicles: {len(self.vehicle_hashmap)}")
                
        except Exception as e:
            logger.error(f"Error updating vehicle data: {str(e)}")
            raise
    
    def get_vehicle_data(self, vehicle_no: str) -> Optional[VehicleData]:
        """Get vehicle data by vehicle number"""
        return self.vehicle_hashmap.get(vehicle_no)
    
    def get_all_vehicles(self) -> Dict[str, VehicleData]:
        """Get all vehicle data"""
        return self.vehicle_hashmap.copy()
    
    def get_vehicles_by_service_type(self, service_type: str) -> Dict[str, VehicleData]:
        """Get vehicles filtered by service type"""
        return {
            vehicle_no: vehicle_data 
            for vehicle_no, vehicle_data in self.vehicle_hashmap.items()
            if vehicle_data.service_type == service_type
        }
    
    def get_vehicle_count(self) -> int:
        """Get total number of vehicles"""
        return len(self.vehicle_hashmap)
    
    def search_vehicles(self, query: str) -> Dict[str, VehicleData]:
        """Search vehicles by vehicle number, waybill ID, or schedule number"""
        query_lower = query.lower()
        results = {}
        
        for vehicle_no, vehicle_data in self.vehicle_hashmap.items():
            if (query_lower in vehicle_no.lower() or
                query_lower in vehicle_data.waybill_id.lower() or
                query_lower in vehicle_data.schedule_no.lower()):
                results[vehicle_no] = vehicle_data
        
        return results

# Global instance
s3_csv_reader = S3CSVReader()

async def initialize_s3_csv_reader():
    """Initialize the S3 CSV reader"""
    try:
        await s3_csv_reader.initialize_s3_client()
        await s3_csv_reader.update_vehicle_data()
        logger.info("S3 CSV reader initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize S3 CSV reader: {str(e)}")
        raise

async def s3_csv_polling_task(polling_interval: int = 300):
    """Background task to periodically update vehicle data from S3"""
    while True:
        try:
            await s3_csv_reader.update_vehicle_data()
        except Exception as e:
            logger.error(f"Error in S3 CSV polling task: {str(e)}")
        
        await asyncio.sleep(polling_interval)

@asynccontextmanager
async def s3_csv_lifespan():
    """Lifespan context manager for S3 CSV reader"""
    try:
        # Initialize S3 CSV reader
        await initialize_s3_csv_reader()
        
        # Start polling task
        polling_task = asyncio.create_task(s3_csv_polling_task())
        
        yield
        
        # Cleanup
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
            
    except Exception as e:
        logger.error(f"Failed to start S3 CSV reader: {str(e)}")
        raise 