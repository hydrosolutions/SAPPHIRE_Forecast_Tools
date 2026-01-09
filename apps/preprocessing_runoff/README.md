# Preprocessing Runoff Module

Downloads and processes historical and operational runoff (discharge) data from iEasyHydro HF or local files (Excel/CSV), calculates statistics for forecast models, and prepares hydrograph data for the dashboard.

## Overview

This module retrieves daily discharge data and prepares it for use by the forecast models. It runs as part of the daily forecast pipeline (typically at 11:00 local time) after runoff observations have been reviewed and entered into the database.

Key functions:
1. Download discharge time series from iEasyHydro HF SDK (or read from Excel/CSV files)
2. Calculate runoff for virtual stations (aggregated catchments)
3. Filter outliers using IQR-based thresholds
4. Interpolate small gaps (max 2 days)
5. Calculate hydrograph statistics (percentiles, mean, std)
6. Export data for forecast models and dashboard

## Scripts

| Script | Purpose |
|--------|---------|
| `preprocessing_runoff.py` | Main script - orchestrates the preprocessing workflow |
| `src/src.py` | Core functions for data processing and statistics |

## Quick Start

```bash
# Set the path to your environment configuration file
export ieasyhydroforecast_env_file_path=/path/to/config/.env

# Run in operational mode (default) - fast daily updates
uv run preprocessing_runoff.py

# Run in maintenance mode - full lookback window, gap filling
uv run preprocessing_runoff.py --maintenance
```

## Docker Usage

### Building the Images

From the repository root:

```bash
# 1. Build the base image (if not already available)
docker build -f apps/docker_base_image/Dockerfile.py312 -t mabesa/sapphire-pythonbaseimage-local:py312 .

# 2. Temporarily change the Dockerfile to use the local base image
# Edit the first line of apps/preprocessing_runoff/Dockerfile.py312 to:
FROM mabesa/sapphire-pythonbaseimage-local:py312 AS base

# 3. Build the preprocessing_runoff image
docker build -f apps/preprocessing_runoff/Dockerfile.py312 -t mabesa/sapphire-preprunoff:py312-test .
```

### Running Locally (macOS)

**Prerequisites:**
- SSH tunnel to iEasyHydro HF server running on the host machine
- Start tunnel with: `ssh -L 5555:localhost:5555 <server>`

**Operational mode** (default - fast daily updates):

```bash
docker run --rm --network host \
  -e ieasyhydroforecast_data_root_dir=/kyg_data_forecast_tools \
  -e ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env_develop_kghm \
  -e SAPPHIRE_OPDEV_ENV=True \
  -e IN_DOCKER=True \
  -e IEASYHYDROHF_HOST=http://host.docker.internal:5555/api/v1/ \
  -v /path/to/kyg_data_forecast_tools/config:/kyg_data_forecast_tools/config \
  -v /path/to/kyg_data_forecast_tools/daily_runoff:/kyg_data_forecast_tools/daily_runoff \
  -v /path/to/kyg_data_forecast_tools/intermediate_data:/kyg_data_forecast_tools/intermediate_data \
  -v /path/to/kyg_data_forecast_tools/bin:/kyg_data_forecast_tools/bin \
  mabesa/sapphire-preprunoff:py312-test
```

**Maintenance mode** (full lookback window, gap filling):

```bash
docker run --rm --network host \
  -e ieasyhydroforecast_data_root_dir=/kyg_data_forecast_tools \
  -e ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env_develop_kghm \
  -e SAPPHIRE_OPDEV_ENV=True \
  -e IN_DOCKER=True \
  -e IEASYHYDROHF_HOST=http://host.docker.internal:5555/api/v1/ \
  -e PREPROCESSING_MODE=maintenance \
  -v /path/to/kyg_data_forecast_tools/config:/kyg_data_forecast_tools/config \
  -v /path/to/kyg_data_forecast_tools/daily_runoff:/kyg_data_forecast_tools/daily_runoff \
  -v /path/to/kyg_data_forecast_tools/intermediate_data:/kyg_data_forecast_tools/intermediate_data \
  -v /path/to/kyg_data_forecast_tools/bin:/kyg_data_forecast_tools/bin \
  mabesa/sapphire-preprunoff:py312-test
```

**Important notes:**
- Replace `/path/to/kyg_data_forecast_tools` with your actual data directory path
- On macOS, `host.docker.internal` resolves to the host machine, allowing the container to reach the SSH tunnel
- The `IEASYHYDROHF_HOST` environment variable overrides the value in the .env file

## Data Flow

```
iEasyHydro HF Database
   (or Excel/CSV files)
         │
         ▼
┌─────────────────────────────┐
│  preprocessing_runoff.py    │
│  - Download discharge data  │
│  - Virtual station calc     │
│  - Outlier filtering        │
│  - Gap interpolation        │
│  - Statistics calculation   │
└─────────────────────────────┘
         │
         ├──► runoff_day.csv (time series)
         │
         └──► hydrograph_day.csv (statistics)
                    │
                    ▼
            Forecast Models &
            Dashboard
```

## Input Data Sources

### iEasyHydro HF (Primary)
Access to iEasyHydro HF database via SDK for operational data.

### Excel Files (Alternative)
Excel files with daily river runoff data, one file per measurement site:
- 2 header lines; first header contains station code and name (space-separated)
- Columns: date (`%d.%m.%Y` format) and discharge (float, m³/s)
- Some files use code ID in the filename instead

### CSV Files (Alternative)
CSV files following the same format as the output.

## Output

### runoff_day.csv
Daily discharge time series:
- `code` - Station code (5-digit integer)
- `date` - Date (YYYY-MM-DD)
- `discharge` - Daily average discharge (m³/s)

### hydrograph_day.csv
Hydrograph statistics by day of year:
- `code` - Station code
- `day_of_year` - Day of year (1-366)
- `count`, `mean`, `std`, `min`, `max` - Basic statistics
- `5%`, `25%`, `50%`, `75%`, `95%` - Percentiles
- Recent years (e.g., `2024`, `2025`) - Values for comparison

### SAPPHIRE API (when enabled)
- Database records written to the SAPPHIRE preprocessing API

## Outlier Filtering

The module uses IQR-based thresholds to identify and remove outliers:
- **Lower threshold:** Q25 - 2.5 × IQR
- **Upper threshold:** Q75 + IQR

Where IQR = Q75 - Q25 (interquartile range)

Values outside these thresholds are set to NaN and interpolated if the gap is ≤ 2 days.

## SAPPHIRE API Integration

This module supports writing runoff data to the SAPPHIRE preprocessing API in addition to CSV files. The API integration uses **incremental sync** - only newly fetched data is sent to the API, while the complete dataset is always written to CSV.

### API Data Flow

```
┌─────────────────────┐     ┌──────────────────────┐
│  Historical Data    │     │  iEasyHydro HF SDK   │
│  (CSV/Excel files)  │     │  (new records only)  │
└─────────┬───────────┘     └──────────┬───────────┘
          │                            │
          ▼                            ▼
    ┌─────────────────────────────────────────┐
    │         Combined DataFrame              │
    │    (full_data + new_data tracked)       │
    │    + Virtual station calculations       │
    └─────────────────┬───────────────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
    ┌───────────┐           ┌───────────┐
    │  CSV File │           │ SAPPHIRE  │
    │ (all data)│           │    API    │
    └───────────┘           │(new only) │
                            └───────────┘
```

### Virtual Stations

Virtual stations (e.g., reservoir inflows calculated from weighted sums of contributing gauges) are handled specially:

1. **Calculation**: Virtual station discharge is calculated from the weighted sum of contributing stations' data, as configured in `config_virtual_stations.json`.

2. **API Sync**: After calculating virtual stations for the full dataset, the module extracts virtual station records for the same date range as the new data and includes them in the API sync.

3. **Consistency Checking**: The `verify_runoff_data_consistency()` function separates virtual station issues from regular station issues, since virtual stations may have sync delays due to their calculation dependencies.

### Why Incremental Sync?

- **Performance**: Avoids re-uploading 500k+ historical records on every run
- **Efficiency**: Only today's discharge measurements need to sync daily
- **Scalability**: API writes complete in seconds instead of hours

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SAPPHIRE_API_URL` | `http://localhost:8000` | Base URL of the SAPPHIRE API gateway |
| `SAPPHIRE_API_ENABLED` | `true` | Set to `false` to disable API writes entirely |
| `SAPPHIRE_DEBUG_VERIFY` | `false` | Set to `true` to verify CSV and API data consistency after writes |

### Data Consistency Verification

When `SAPPHIRE_DEBUG_VERIFY=true`, the module compares CSV and API data after writing to ensure they match. This is useful during the transition period to verify the API integration is working correctly.

The verification checks:
- **Runoff data**: Compares `runoff_day.csv` with the `runoffs` database table
- **Hydrograph data**: Compares `hydrograph_day.csv` with the `hydrographs` database table

Each verification reports:
- Record counts in CSV vs API
- Missing records (in CSV but not API)
- Extra records (in API but not CSV)
- Value mismatches (same key, different values)

Example output:
```
✓ Runoff: Data consistent: 585606 CSV records match 585606 API records
✓ Hydrograph: Hydrograph data consistent: 14600 CSV records, 14600 API records
```

### Operating Modes (Future)

The incremental sync is designed to support different operating modes:

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Operational** | Sync only today's data | Daily forecast runs |
| **Maintenance** | Sync last N days | Backfill after outages, corrections |
| **Initial** | Sync all historical data | First-time setup, database rebuild |

Currently, the module automatically determines what data is "new" based on the latest date in existing data. Future versions will allow explicit configuration of the sync window.

### Verifying API Data

To check data in the preprocessing database:

```bash
# Connect to the database
docker exec -it sapphire-preprocessing-db psql -U postgres -d preprocessing_db

# Count records
SELECT COUNT(*) FROM runoffs;

# Check date range
SELECT MIN(date), MAX(date) FROM runoffs;

# View recent records
SELECT * FROM runoffs ORDER BY date DESC LIMIT 10;
```

## Date Handling

This module standardizes on normalized pandas Timestamp objects for all date values:
- All datetime objects have their time components set to midnight (00:00:00)
- All date comparisons are consistent
- DataFrame merging on date columns works reliably
- Timezone information is preserved when relevant

**Important:** Never use Python native `datetime.date` objects for date comparisons or merges as this causes incompatibilities with pandas Timestamp objects.

## Operating Modes

The module supports two operating modes:

### Operational Mode (Default)
- **Purpose:** Fast daily updates for operational forecasting
- **Behavior:** Uses cached data, fetches only yesterday's daily average + today's morning discharge
- **When to use:** Daily automated pipeline runs

```bash
uv run preprocessing_runoff.py
```

### Maintenance Mode
- **Purpose:** Gap filling and data quality updates
- **Behavior:** Always rereads input Excel/CSV files, fetches configurable lookback window from database (default 120 days), merges and updates values
- **When to use:** Weekly scheduled maintenance, after data corrections, when data issues are suspected, or when rows were accidentally deleted from output files

```bash
uv run preprocessing_runoff.py --maintenance
# Or via environment variable
PREPROCESSING_MODE=maintenance uv run preprocessing_runoff.py
```

### Configuration

Module behavior is controlled by `config.yaml`. This is particularly important for the **lookback window** which determines how far back maintenance mode fetches data to fill gaps.

```yaml
# config.yaml
maintenance:
  # Number of days to look back when fetching data in maintenance mode.
  # This determines how old a gap can be and still get filled from the database.
  # Default: 120 days. Override with: PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS
  lookback_days: 120

operational:
  # Fetch yesterday's daily average discharge (WDDA variable)
  fetch_yesterday: true
  # Fetch today's morning discharge (WDD variable)
  fetch_morning: true

api:
  # Number of records per page when fetching from iEasyHydro HF API.
  # Higher values = fewer API calls but larger responses.
  # Override with: PREPROCESSING_API_PAGE_SIZE
  page_size: 1000
```

**Key parameters:**

| Parameter | Default | Env Override | Description |
|-----------|---------|--------------|-------------|
| `maintenance.lookback_days` | 120 | `PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS` | Days of historical data to fetch in maintenance mode. Gaps older than this cannot be filled from the database. |
| `operational.fetch_yesterday` | true | - | Whether to fetch yesterday's daily average discharge |
| `operational.fetch_morning` | true | - | Whether to fetch today's morning discharge |
| `api.page_size` | 1000 | `PREPROCESSING_API_PAGE_SIZE` | Records per API page (reduces round-trips) |

**Note:** In maintenance mode, the module always rereads input Excel/CSV files to ensure data completeness before merging with database data. This ensures that gaps in the output file (e.g., accidentally deleted rows) are restored from the original source data.

## Development

### Run Locally

From the `preprocessing_runoff` directory:
```bash
ieasyhydroforecast_env_file_path=/path/to/.env uv run preprocessing_runoff.py
```

### Run Tests

From the `apps` directory:
```bash
uv run --project preprocessing_runoff pytest preprocessing_runoff/test/ -v
```

From the `preprocessing_runoff` directory:
```bash
uv run pytest test/ -v
```

## Key Environment Variables

| Variable | Description | Note |
|----------|-------------|------------|
| `ieasyhydroforecast_env_file_path` | Path to the .env configuration file | Required |
| `IEASYHYDROHF_HOST` | iEasyHydro HF API endpoint URL | .env file |
| `IEASYHYDROHF_USERNAME` | API username | .env file |
| `IEASYHYDROHF_PASSWORD` | API password | .env file |
| `PREPROCESSING_MODE` | Operating mode: `operational` (default) or `maintenance` | optional |
| `PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS` | Number of days to fetch in maintenance mode (default: 120) | optional |
| `PREPROCESSING_API_PAGE_SIZE` | Records per API page for iEasyHydro HF (default: 1000) | optional |

See [doc/configuration.md](../../doc/configuration.md) for the complete list.

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [User Guide](../../doc/user_guide.md) - How to use the forecast system
