# SAPPHIRE Services

This folder contains the microservices architecture for SAPPHIRE database APIs. These services provide REST APIs for storing and retrieving forecast data, replacing direct CSV file I/O with a database-backed solution.

## Architecture Overview

```
sapphire/
├── docker-compose.yml      # Orchestrates all services and databases
└── services/
    ├── api-gateway/        # Routes requests to appropriate services (port 8000)
    ├── preprocessing/      # Runoff, hydrograph, meteo, snow data (port 8002)
    ├── postprocessing/     # Forecasts, skill metrics (port 8003)
    ├── user/               # User management (port 8004)
    ├── auth/               # Authentication service (port 8005)
    └── task-service/       # Task queue (currently disabled)
```

## Services

| Service | Port | Database | Description |
|---------|------|----------|-------------|
| api-gateway | 8000 | - | Routes requests to backend services |
| preprocessing-api | 8002 | preprocessing-db (5433) | Runoff, hydrograph, meteo, snow data |
| postprocessing-api | 8003 | postprocessing-db (5434) | Forecasts, LR forecasts, skill metrics |
| user-api | 8004 | user-db (5435) | User management |
| auth-api | 8005 | auth-db (5436) | Authentication and authorization |

## Running Services

### Start all services with Docker Compose

```bash
cd sapphire
docker-compose up -d
```

### Check service health

```bash
# API Gateway
curl http://localhost:8000/health

# Preprocessing service
curl http://localhost:8000/health/ready
```

### View logs

```bash
docker-compose logs -f preprocessing-api
```

### Stop services

```bash
docker-compose down
```

## Running Tests

### Preprocessing Service

```bash
cd sapphire/services/preprocessing

# Install dependencies (using uv)
uv pip install -r requirements.txt

# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_data_migrator.py -v

# Run with coverage
python -m pytest tests/ --cov=app --cov-report=term-missing
```

### Postprocessing Service

```bash
cd sapphire/services/postprocessing

# Install dependencies (using uv)
uv pip install -r requirements.txt

# Run all tests
python -m pytest tests/ -v
```

### Test Structure

```
services/preprocessing/
├── app/
│   ├── data_migrator.py    # CSV to API migration logic
│   ├── models.py           # SQLAlchemy models
│   ├── schemas.py          # Pydantic schemas
│   ├── crud.py             # Database operations
│   └── main.py             # FastAPI endpoints
└── tests/
    ├── conftest.py         # Pytest configuration
    └── test_data_migrator.py  # Transformation tests (39 tests)

services/postprocessing/
├── app/
│   ├── data_migrator.py    # Forecast migration logic
│   ├── models.py           # SQLAlchemy models
│   └── ...
└── tests/
    ├── conftest.py         # Pytest configuration
    └── test_data_migrator.py  # Forecast transformation tests (23 tests)
```

## API Usage

See the [sapphire-api-client](https://github.com/hydrosolutions/sapphire-api-client) package for a Python client library.

### Example: Write runoff data

```python
from sapphire_api_client import SapphirePreprocessingClient

client = SapphirePreprocessingClient(base_url="http://localhost:8000")

# Prepare records from DataFrame
records = client.prepare_runoff_records(df, horizon_type="day", code="12345")

# Write to API
client.write_runoff(records)
```

### Example: Read data via curl

```bash
# Get runoff data
curl "http://localhost:8000/runoff/?code=12345&horizon=day"

# Get hydrograph data
curl "http://localhost:8000/hydrograph/?code=12345"
```

## Development

### Adding a new endpoint

1. Define model in `app/models.py`
2. Create schema in `app/schemas.py`
3. Add CRUD operations in `app/crud.py`
4. Create endpoint in `app/main.py`
5. Write tests in `tests/`

### Database migrations

Using Alembic (when schema changes):

```bash
cd services/preprocessing
alembic revision --autogenerate -m "Description"
alembic upgrade head
```

## Related Documentation

- [API Integration Plan](../doc/plans/sapphire_api_integration_plan.md)
- [sapphire-api-client](https://github.com/hydrosolutions/sapphire-api-client)
