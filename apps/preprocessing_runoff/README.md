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

## Outlier Filtering

The module uses IQR-based thresholds to identify and remove outliers:
- **Lower threshold:** Q25 - 2.5 × IQR
- **Upper threshold:** Q75 + IQR

Where IQR = Q75 - Q25 (interquartile range)

Values outside these thresholds are set to NaN and interpolated if the gap is ≤ 2 days.

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
- **Behavior:** Checks for Excel file changes, fetches configurable lookback window (default 30 days), updates differing values
- **When to use:** Weekly scheduled maintenance, after data corrections, or when data issues are suspected

```bash
uv run preprocessing_runoff.py --maintenance
# Or via environment variable
PREPROCESSING_MODE=maintenance uv run preprocessing_runoff.py
```

### Configuration

Module configuration is in `config.yaml`:
```yaml
maintenance:
  lookback_days: 30  # Override with PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS env var

operational:
  fetch_yesterday: true
  fetch_morning: true
```

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
| `PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS` | Number of days to fetch in maintenance mode (default: 30) | optional |

See [doc/configuration.md](../../doc/configuration.md) for the complete list.

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [User Guide](../../doc/user_guide.md) - How to use the forecast system
