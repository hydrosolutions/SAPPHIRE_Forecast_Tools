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

## Developer Database Access

The SAPPHIRE services use PostgreSQL databases running in Docker containers. You can connect directly to these databases for debugging, data exploration, and development.

### Connecting to Preprocessing Database

```bash
docker exec -it sapphire-preprocessing-db psql -U postgres -d preprocessing_db
```

Once connected, useful commands:

```sql
-- List all tables
\dt

-- Describe a table structure
\d runoffs

-- Exit psql
\q
```

### Connecting to Postprocessing Database

```bash
docker exec -it sapphire-postprocessing-db psql -U postgres -d postprocessing_db
```

### Connecting to User/Auth Databases

```bash
# User database
docker exec -it sapphire-user-db psql -U postgres -d user_db

# Auth database
docker exec -it sapphire-auth-db psql -U postgres -d auth_db
```

## Data Migration

The data migrator scripts load historical data from CSV files into the database. This is useful for:
- Initial database population
- Recreating tables after schema changes
- Refreshing data after database issues

### Preprocessing Data Migration

Connect to the preprocessing API container and run migrations:

```bash
# Connect to the container
docker exec -it sapphire-preprocessing-api /bin/bash

# Inside the container, run migrations for each data type:
python app/data_migrator.py --type runoff
python app/data_migrator.py --type hydrograph
python app/data_migrator.py --type meteo
python app/data_migrator.py --type snow
```

### Postprocessing Data Migration

Connect to the postprocessing API container and run migrations:

```bash
# Connect to the container
docker exec -it sapphire-postprocessing-api /bin/bash

# Inside the container, run migrations for each data type:
python app/data_migrator.py --type skillmetric --batch-size 1
python app/data_migrator.py --type lrforecast
python app/data_migrator.py --type combinedforecast
python app/data_migrator.py --type forecast
python app/data_migrator.py --type longforecast
```

**Note:** The `--batch-size 1` flag for skill metrics is needed due to duplicate entry handling.
**Note 2** `python app/data_migrator.py --type longforecast --modes month_1 --model-filter LR_Base,GBT` only migrates month_1 and filter for the selected models.  

### Migration Options

| Option | Description |
|--------|-------------|
| `--type TYPE` | Data type to migrate (required) |
| `--batch-size N` | Number of records per batch (default: 1000) |
| `--dry-run` | Validate without writing to database |

---

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
records = client.prepare_runoff_records(df, horizon_type="day", code="15013")

# Write to API
client.write_runoff(records)
```

### Example: Read data via curl

```bash
# Get runoff data (via API gateway)
curl "http://localhost:8000/api/preprocessing/runoff/?code=15013&horizon=day&limit=5"

# Get hydrograph data
curl "http://localhost:8000/api/preprocessing/hydrograph/?code=15013&horizon=day&limit=5"

# Get meteo data
curl "http://localhost:8000/api/preprocessing/meteo/?code=38457&meteo_type=T&limit=5"

# Get snow data
curl "http://localhost:8000/api/preprocessing/snow/?code=15013&snow_type=SWE&limit=5"
```

## Development

### Adding a new endpoint

1. Define model in `app/models.py`
2. Create schema in `app/schemas.py`
3. Add CRUD operations in `app/crud.py`
4. Create endpoint in `app/main.py`
5. Write tests in `tests/`


## Database Schema Reference

### Enum Types

The databases use several enumeration types for categorization:

#### HorizonType
Forecast time horizon granularity.

| Value | Description |
|-------|-------------|
| `DAY` | Daily forecasts |
| `PENTAD` | 5-day period forecasts |
| `DECADE` | 10-day period forecasts |
| `MONTH` | Monthly forecasts |
| `SEASON` | Seasonal forecasts |
| `YEAR` | Annual forecasts |

#### MeteoType
Meteorological variable types.

| Value | Description |
|-------|-------------|
| `T` | Temperature |
| `P` | Precipitation |

#### SnowType
Snow measurement types.

| Value | Description |
|-------|-------------|
| `HS` | Snow height |
| `ROF` | Snow melt plus rainfall runoff |
| `SWE` | Snow water equivalent |

#### ModelType
Forecast model types (postprocessing database).

| Value | Description |
|-------|-------------|
| `TSMixer` | Time-Series Mixer |
| `TiDE` | Time-Series Dense Encoder |
| `TFT` | Temporal Fusion Transformer |
| `EM` | Ensemble Mean with LR, TFT, TIDE |
| `NE` | Neural Ensemble with TIDE, TFT, TSMixer |
| `RRAM` | Rainfall runoff assimilation model |
| `LR` | Linear Regression |

---

### Preprocessing Database Tables

#### `runoffs`
Historical runoff/discharge observations used as input for forecasting models.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `horizon_type` | ENUM(HorizonType) | NO | Time horizon granularity |
| `code` | VARCHAR(10) | NO | Station/catchment identifier |
| `date` | DATE | NO | Observation date |
| `discharge` | FLOAT | YES | Measured discharge value (m³/s) |
| `predictor` | FLOAT | YES | Predictor variable for regression models |
| `horizon_value` | INTEGER | NO | Horizon number (e.g., pentad 1-6) |
| `horizon_in_year` | INTEGER | NO | Absolute horizon within year (e.g., pentad 1-73) |

**Unique constraint:** `(horizon_type, code, date)`

---

#### `hydrographs`
Statistical hydrograph data showing historical flow patterns and percentiles.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `horizon_type` | ENUM(HorizonType) | NO | Time horizon granularity |
| `code` | VARCHAR(10) | NO | Station/catchment identifier |
| `date` | DATE | NO | Reference date |
| `horizon_value` | INTEGER | NO | Horizon number |
| `horizon_in_year` | INTEGER | NO | Absolute horizon within year |
| `day_of_year` | INTEGER | NO | Day of year (1-366) |
| `count` | INTEGER | YES | Number of observations in statistics |
| `mean` | FLOAT | YES | Mean discharge |
| `std` | FLOAT | YES | Standard deviation |
| `min` | FLOAT | YES | Minimum observed value |
| `max` | FLOAT | YES | Maximum observed value |
| `q05` | FLOAT | YES | 5th percentile |
| `q25` | FLOAT | YES | 25th percentile |
| `q50` | FLOAT | YES | 50th percentile (median) |
| `q75` | FLOAT | YES | 75th percentile |
| `q95` | FLOAT | YES | 95th percentile |
| `norm` | FLOAT | YES | Long-term normal value |
| `previous` | FLOAT | YES | Previous period's value |
| `current` | FLOAT | YES | Current period's value |

**Unique constraint:** `(horizon_type, code, date)`

---

#### `meteo`
Meteorological observations (temperature, precipitation).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `meteo_type` | ENUM(MeteoType) | NO | Variable type (T=temperature, P=precipitation) |
| `code` | VARCHAR(10) | NO | Station identifier |
| `date` | DATE | NO | Observation date |
| `value` | FLOAT | YES | Measured value |
| `norm` | FLOAT | YES | Long-term normal for this date |
| `day_of_year` | INTEGER | NO | Day of year (1-366) |

**Unique constraint:** `(meteo_type, code, date)`

---

#### `snow`
Snow measurements from multiple elevation zones.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `snow_type` | ENUM(SnowType) | NO | Measurement type (HS/ROF/SWE) |
| `code` | VARCHAR(10) | NO | Station/catchment identifier |
| `date` | DATE | NO | Observation date |
| `value` | FLOAT | YES | Aggregated/main value |
| `norm` | FLOAT | YES | Long-term normal |
| `value1` - `value14` | FLOAT | YES | Values for elevation zones 1-14 |

**Unique constraint:** `(snow_type, code, date)`

---

### Postprocessing Database Tables

#### `forecasts`
Model forecast outputs with uncertainty quantiles.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `horizon_type` | ENUM(HorizonType) | NO | Time horizon granularity |
| `code` | VARCHAR(10) | NO | Station/catchment identifier |
| `model_type` | ENUM(ModelType) | NO | Forecasting model used |
| `date` | DATE | NO | Date when forecast was made |
| `target` | DATE | YES | Forecast target date |
| `flag` | INTEGER | YES | Quality/status flag |
| `horizon_value` | INTEGER | NO | Horizon number |
| `horizon_in_year` | INTEGER | NO | Absolute horizon within year |
| `q05` | FLOAT | YES | 5th percentile forecast |
| `q25` | FLOAT | YES | 25th percentile forecast |
| `q50` | FLOAT | YES | 50th percentile forecast (median) |
| `q75` | FLOAT | YES | 75th percentile forecast |
| `q95` | FLOAT | YES | 95th percentile forecast |
| `forecasted_discharge` | FLOAT | YES | Point forecast value |

**Unique constraint:** `(horizon_type, code, model_type, date, target)`

---

#### `lr_forecasts`
Linear regression model forecasts with model parameters.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `horizon_type` | ENUM(HorizonType) | NO | Time horizon granularity |
| `code` | VARCHAR(10) | NO | Station/catchment identifier |
| `date` | DATE | NO | Forecast date |
| `horizon_value` | INTEGER | NO | Horizon number |
| `horizon_in_year` | INTEGER | NO | Absolute horizon within year |
| `discharge_avg` | FLOAT | YES | Average discharge used in model |
| `predictor` | FLOAT | YES | Predictor variable value |
| `slope` | FLOAT | YES | Regression slope coefficient |
| `intercept` | FLOAT | YES | Regression intercept |
| `forecasted_discharge` | FLOAT | YES | Forecasted discharge value |
| `q_mean` | FLOAT | YES | Mean of forecast distribution |
| `q_std_sigma` | FLOAT | YES | Standard deviation (sigma) |
| `delta` | FLOAT | YES | Forecast uncertainty delta |
| `rsquared` | FLOAT | YES | R² goodness of fit |

**Unique constraint:** `(horizon_type, code, date)`

---

#### `skill_metrics`
Model performance metrics for forecast verification.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | INTEGER | NO | Primary key (auto-increment) |
| `horizon_type` | ENUM(HorizonType) | NO | Time horizon granularity |
| `code` | VARCHAR(10) | NO | Station/catchment identifier |
| `model_type` | ENUM(ModelType) | NO | Model being evaluated |
| `date` | DATE | NO | Evaluation date |
| `horizon_in_year` | INTEGER | NO | Absolute horizon within year |
| `sdivsigma` | FLOAT | YES | S/σ ratio (skill score) |
| `nse` | FLOAT | YES | Nash-Sutcliffe Efficiency |
| `delta` | FLOAT | YES | Forecast error delta |
| `accuracy` | FLOAT | YES | Accuracy percentage |
| `mae` | FLOAT | YES | Mean Absolute Error |
| `n_pairs` | INTEGER | YES | Number of forecast-observation pairs |

**Unique constraint:** `(horizon_type, code, model_type, date)`

---

## Useful Query Examples

### Preprocessing Database Queries

```sql
-- Get latest runoff data for a station
SELECT * FROM runoffs
WHERE code = '15013'
ORDER BY date DESC
LIMIT 10;

-- Get all pentad runoffs for a station in a year
SELECT * FROM runoffs
WHERE code = '15013'
  AND horizon_type = 'PENTAD'
  AND date >= '2024-01-01'
ORDER BY date;

-- Get hydrograph percentiles for current period
SELECT code, date, q05, q25, q50, q75, q95, current
FROM hydrographs
WHERE horizon_type = 'PENTAD'
  AND date = CURRENT_DATE;

-- Get temperature data for a station
SELECT date, value, norm, value - norm as anomaly
FROM meteo
WHERE code = '15013'
  AND meteo_type = 'T'
ORDER BY date DESC
LIMIT 30;

-- Get snow water equivalent across elevation zones
SELECT date, value, value1, value2, value3, value4, value5
FROM snow
WHERE code = '15013'
  AND snow_type = 'SWE'
ORDER BY date DESC
LIMIT 10;

-- Count records per station
SELECT code, COUNT(*) as records
FROM runoffs
GROUP BY code
ORDER BY records DESC;
```

### Postprocessing Database Queries

```sql
-- Get latest forecasts for a station
SELECT date, target, model_type, q50, forecasted_discharge
FROM forecasts
WHERE code = '15013'
ORDER BY date DESC, target
LIMIT 20;

-- Compare forecasts across models for a specific date
SELECT model_type, q05, q25, q50, q75, q95
FROM forecasts
WHERE code = '15013'
  AND date = '2024-06-01'
  AND horizon_type = 'PENTAD';

-- Get linear regression forecast with model parameters
SELECT date, predictor, slope, intercept, forecasted_discharge, rsquared
FROM lr_forecasts
WHERE code = '15013'
ORDER BY date DESC
LIMIT 10;

-- Compare skill metrics across models
SELECT model_type, AVG(nse) as avg_nse, AVG(accuracy) as avg_accuracy, AVG(mae) as avg_mae
FROM skill_metrics
WHERE code = '15013'
  AND horizon_type = 'PENTAD'
GROUP BY model_type
ORDER BY avg_nse DESC;

-- Find best performing model per station
SELECT DISTINCT ON (code) code, model_type, nse, accuracy
FROM skill_metrics
WHERE horizon_type = 'PENTAD'
ORDER BY code, nse DESC;

-- Get skill metrics trend over time
SELECT date, model_type, nse, accuracy
FROM skill_metrics
WHERE code = '15013'
  AND model_type = 'TFT'
ORDER BY date;
```

## Related Documentation

- [API Integration Plan](../doc/plans/sapphire_api_integration_plan.md)
- [sapphire-api-client](https://github.com/hydrosolutions/sapphire-api-client)
