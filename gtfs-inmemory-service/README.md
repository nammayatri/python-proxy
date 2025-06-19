# GTFS Routes Service

A FastAPI-based service that provides GTFS (General Transit Feed Specification) routes, stops, and vehicle data through RESTful APIs.

## Features

- **GTFS Routes API**: Fetch routes, patterns, and stop mappings
- **Vehicle Data Integration**: S3 CSV integration for vehicle service type data
- **Real-time Data Polling**: Automatic data refresh from GTFS sources
- **Memory Optimization**: Efficient memory management with garbage collection
- **Health Checks**: Built-in readiness and health check endpoints

## API Endpoints

### Routes
- `GET /route/{gtfs_id}/{route_id}` - Get specific route
- `GET /routes/{gtfs_id}` - Get all routes for a GTFS ID
- `GET /routes/{gtfs_id}/fuzzy/{query}` - Fuzzy search routes

### Stops
- `GET /stops/{gtfs_id}` - Get all stops for a GTFS ID
- `GET /stop/{gtfs_id}/{stop_code}` - Get specific stop
- `GET /stops/{gtfs_id}/fuzzy/{query}` - Fuzzy search stops

### Route-Stop Mappings
- `GET /route-stop-mapping/{gtfs_id}/route/{route_code}` - Get stops for a route
- `GET /route-stop-mapping/{gtfs_id}/stop/{stop_code}` - Get routes for a stop

### Vehicle Data
- `GET /vehicle/{vehicle_no}/service-type` - Get vehicle service type

### System
- `GET /ready` - Readiness probe
- `GET /version/{gtfs_id}` - Get data version hash

## Environment Variables

### GTFS Configuration
- `GTFS_BASE_URL` - Base URL for GTFS API (default: http://localhost:8080)
- `GTFS_POLLING_INTERVAL` - Data refresh interval in seconds (default: 30)
- `GTFS_PROCESS_BATCH_SIZE` - Batch size for processing (default: 50)
- `GTFS_API_HOST` - API host (default: 0.0.0.0)
- `GTFS_API_PORT` - API port (default: 8000)

### Performance Tuning
- `GTFS_GC_INTERVAL` - Garbage collection interval (default: 300)
- `GTFS_MAX_RETRIES` - Maximum API retries (default: 3)
- `GTFS_RETRY_DELAY` - Retry delay in seconds (default: 5)
- `GTFS_RATE_LIMIT_DELAY` - Rate limiting delay (default: 0.1)
- `GTFS_CPU_THRESHOLD` - CPU usage threshold (default: 80.0)
- `GTFS_CONNECTION_LIMIT` - Connection limit (default: 100)
- `GTFS_DNS_TTL` - DNS cache TTL (default: 300)
- `GTFS_MEMORY_THRESHOLD` - Memory threshold in MB (default: 5000)

### AWS/S3 Configuration
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_REGION` - AWS region (default: ap-south-1)
- `S3_BUCKET_NAME` - S3 bucket name (default: nandi-data)
- `S3_CSV_KEY` - S3 CSV file key (default: currentWaybill/waybills.csv)
- `USE_IAM_ROLE` - Use IAM role (default: false)
- `AWS_PROFILE` - AWS profile name

## Setup

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export GTFS_BASE_URL="your_gtfs_api_url"
export AWS_ACCESS_KEY_ID="your_aws_key"
export AWS_SECRET_ACCESS_KEY="your_aws_secret"
```

3. Run the service:
```bash
python gtfs-routes-service.py
```

### Docker

1. Build and run with Docker Compose:
```bash
docker-compose up --build
```

2. Or build and run manually:
```bash
docker build -t gtfs-service .
docker run -p 8000:8000 --env-file .env gtfs-service
```

## API Documentation

Once the service is running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Health Checks

- Readiness: `GET /ready`
- Health check: The service includes a health check endpoint for container orchestration

## Data Sources

- **GTFS Data**: Fetched from the configured GTFS API endpoint
- **Vehicle Data**: S3 CSV file containing vehicle service type mappings

## Performance Features

- Connection pooling and keep-alive
- Rate limiting for API calls
- Memory usage monitoring
- Automatic garbage collection
- Batch processing for large datasets 