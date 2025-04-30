# GTFS Realtime Feed API

A FastAPI application that serves GTFS realtime data from Redis.

## API Endpoints

The API follows a structured approach to provide access to different types of GTFS Realtime data:

### Main Endpoints

- **API Root**: `/` - Information about all available endpoints
- **Trip Updates**: `/gtfs-rt/trip-updates` - Information about trip updates endpoints
  - **Protobuf format**: `/gtfs-rt/trip-updates/pb`
  - **JSON format**: `/gtfs-rt/trip-updates/json`
- **Original Trip Updates Endpoints**:
  - **Protobuf format**: `/tripupdates.pb`
  - **JSON format**: `/tripupdates.json`
- **Vehicle Positions**: `/gtfs-rt/vehicle-positions` - Coming soon
- **Service Alerts**: `/gtfs-rt/service-alerts` - Coming soon

### Legacy Endpoints (Deprecated)

These endpoints are maintained for backward compatibility but will be removed in future versions:

- **Protobuf format**: `/tripupdates.pb`
- **JSON format**: `/tripupdates.json`

## Setup

### Using Docker Compose

1. Create a `.env` file in the project root with the following variables:

```bash
# Redis Configuration
REDIS_HOST=host.docker.internal
REDIS_PORT=6379
REDIS_DB=0
REDIS_KEY=gtfs_realtime_data:tripupdates
# REDIS_PASSWORD=your_password_here  # Uncomment and set if Redis requires authentication

# Application Configuration
APP_PORT=8004
DEV_MODE=true  # Set to true for hot reloading # Set to false in production
```

2. Start the services:
```bash
docker compose up -d
```

3. To stop the services:
```bash
docker compose down
```

4. To view logs:
```bash
docker compose logs -f
```

## Configuration

The following environment variables can be set:

| Variable | Description | Default |
|----------|-------------|---------|
| REDIS_HOST | Redis host | host.docker.internal |
| REDIS_PORT | Redis port | 6379 |
| REDIS_DB | Redis database number | 0 |
| REDIS_KEY | Redis key for trip updates | gtfs_realtime_data:tripupdates |
| REDIS_PASSWORD | Redis password | None |
| APP_PORT | Application port | 8004 |

These can be set in `.env` file for development or as environment variables in Docker.

## Redis Data Format

The application expects trip update data to be stored in Redis under the key specified by `REDIS_KEY` (default: `gtfs_realtime_data:tripupdates`). The data should be a JSON object with the following structure:

```json
{
  "header": {
    "gtfsRealtimeVersion": "2.0",
    "incrementality": "FULL_DATASET",
    "timestamp": "1745591074"
  },
  "entity": [
    {
      "id": "1",
      "tripUpdate": {
        "trip": {
          "tripId": "4796-S-UP-53717",
          "startTime": "05:40:00",
          "startDate": "20250611",
          "routeId": "4796",
          "directionId": 0
        },
        "stopTimeUpdate": [
          {
            "stopSequence": 2,
            "arrival": {
              "time": "1749600920"
            },
            "departure": {
              "time": "1749605700"
            },
            "stopId": "KwaxxmSs"
          }
        ],
        "vehicle": {
          "id": "vehicle_4796",
          "label": "PB07"
        },
        "timestamp": "1745591074"
      }
    }
  ]
}
```

### Setting up Test Data in Redis (For testing locally)

1. Access Redis CLI:
```bash
docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
```

2. Connect with redis-cli:
```bash
docker exec -it redis-stack redis-cli
```

3. Set test data:
```bash
JSON.SET gtfs_realtime_data:tripupdates $ '{"header":{"gtfsRealtimeVersion":"2.0","incrementality":"FULL_DATASET","timestamp":"1745591074"},"entity":[{"id":"1","tripUpdate":{"trip":{"tripId":"4796-S-UP-53717","startTime":"05:40:00","startDate":"20250611","routeId":"4796","directionId":0},"stopTimeUpdate":[{"stopSequence":2,"arrival":{"time":"1749600920"},"departure":{"time":"1749605700"},"stopId":"KwaxxmSs"}],"vehicle":{"id":"vehicle_4796","label":"PB07"},"timestamp":"1745591074"}}]}'
```

4. Check the data in Redis GUI:
```bash
http://localhost:8001/redis-stack/browser
```

5. Test the API:
```bash
curl http://localhost:8004/gtfs-rt/trip-updates/json
```