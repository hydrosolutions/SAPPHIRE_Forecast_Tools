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
| `src/config.py` | Configuration loading (log level, validation, spot-check, site cache settings) |
| `src/profiling.py` | Performance profiling utilities (enabled via `PREPROCESSING_PROFILING=true`) |

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

## Limitations

### iEasyHydro HF Integration

When using iEasyHydro HF as the data source, the following limitations apply:

**Supported Station Types:**
- **Manual stations only** - Forecasts are generated only for stations marked as `site_type: manual` in iEasyHydro HF
- Automatic stations with the same site code are filtered out to avoid duplicates
- This is by design, as manual stations contain quality-controlled data suitable for forecasting

**API Constraints:**
- **Page size limit:** The iEasyHydro HF API supports up to 1000 records per page
- The module handles pagination automatically with parallel fetching for performance
- Records with null/missing values are automatically filtered out

**Data Availability:**
- Data must be available in iEasyHydro HF before the preprocessing run (typically 11:00 local time)
- Yesterday's daily average (`WDDA` variable) and today's morning discharge (`WDD` variable) are fetched
- Older data gaps can only be filled in maintenance mode within the configured lookback window (default: 50 days)

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

### forecast_sites_cache.json
Cached list of forecast sites (updated in maintenance mode):
- `site_codes` - List of station codes
- `site_ids` - List of site IDs (for iEasyHydro HF)
- `created_at` - Cache creation timestamp

Used by operational mode to skip slow SDK site-listing calls.

### reliability_stats.json
Site reliability tracking (updated in maintenance mode):
- Tracks data availability percentages per site
- Used by validation to flag unreliable sites

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
- **Behavior:** Uses cached site list (skips slow SDK site-listing calls), fetches only yesterday's daily average + today's morning discharge
- **When to use:** Daily automated pipeline runs
- **Site caching:** Uses `forecast_sites_cache.json` to avoid slow SDK calls. Cache is refreshed in maintenance mode.

```bash
uv run preprocessing_runoff.py
```

### Maintenance Mode
- **Purpose:** Gap filling, data quality updates, and cache refresh
- **Behavior:** Always rereads input Excel/CSV files, fetches configurable lookback window from database (default 50 days), merges and updates values, runs validation checks, updates site cache
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
logging:
  # Log level for this module: DEBUG, INFO, WARNING, ERROR
  # If not set, falls back to env variable 'log_level', then INFO
  log_level: INFO

maintenance:
  # Number of days to look back when fetching data in maintenance mode.
  # This determines how old a gap can be and still get filled from the database.
  # Default: 50 days. Override with: PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS
  lookback_days: 50

operational:
  # Fetch yesterday's daily average discharge (WDDA variable)
  fetch_yesterday: true
  # Fetch today's morning discharge (WDD variable)
  fetch_morning: true

# Post-write validation (maintenance mode only)
validation:
  enabled: true
  max_age_days: 3           # Sites without data this recent are flagged
  reliability_threshold: 80.0  # Sites below this % are flagged
  stats_file: "reliability_stats.json"

# Spot-check validation (maintenance mode only)
spot_check:
  enabled: true
  # Sites configured via IEASYHYDRO_SPOTCHECK_SITES env var

# Site caching (operational mode optimization)
site_cache:
  enabled: true
  cache_file: "forecast_sites_cache.json"
  max_age_days: 7
```

**Key parameters:**

| Parameter | Default | Env Override | Description |
|-----------|---------|--------------|-------------|
| `logging.log_level` | INFO | `log_level` | Log level for this module |
| `maintenance.lookback_days` | 50 | `PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS` | Days of historical data to fetch in maintenance mode. Gaps older than this cannot be filled from the database. |
| `operational.fetch_yesterday` | true | - | Whether to fetch yesterday's daily average discharge |
| `operational.fetch_morning` | true | - | Whether to fetch today's morning discharge |
| `validation.enabled` | true | - | Enable post-write validation in maintenance mode |
| `validation.max_age_days` | 3 | - | Sites without data this recent are flagged |
| `validation.reliability_threshold` | 80.0 | - | Sites below this % are flagged as unreliable |
| `spot_check.enabled` | true | - | Enable spot-check validation in maintenance mode |
| `site_cache.enabled` | true | - | Enable site caching for faster operational mode |
| `site_cache.max_age_days` | 7 | - | Days before cache is considered stale |

**Note:** In maintenance mode, the module always rereads input Excel/CSV files to ensure data completeness before merging with database data. This ensures that gaps in the output file (e.g., accidentally deleted rows) are restored from the original source data.

## Validation Features (Maintenance Mode)

Maintenance mode includes automated validation checks to ensure data quality.

### Post-Write Validation
Checks data completeness after writing output files:
- Verifies all expected sites have recent data (within `max_age_days`)
- Tracks site reliability over time in `reliability_stats.json`
- Flags sites below the `reliability_threshold` percentage

### Spot-Check Validation
Validates output data against direct API queries for selected sites:
- Configure sites via `IEASYHYDRO_SPOTCHECK_SITES` environment variable
- Compares values in output file against fresh API responses
- Helps detect data pipeline issues or SDK bugs

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
| `PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS` | Number of days to fetch in maintenance mode (default: 50) | optional |
| `IEASYHYDRO_SPOTCHECK_SITES` | Comma-separated site codes for spot-check validation (e.g., `15166,16159,15189`) | optional |
| `PREPROCESSING_PROFILING` | Set to `true` to enable performance profiling output | optional |
| `log_level` | System-wide log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` | optional, default: INFO |

See [doc/configuration.md](../../doc/configuration.md) for the complete list.

## Logging

The module uses structured logging with stage tags for easy filtering and debugging.

### Log Levels

| Level | Use Case |
|-------|----------|
| **DEBUG** | Detailed data for debugging (DataFrame info, intermediate values) |
| **INFO** | Normal operation milestones, counts, summaries |
| **WARNING** | Potential issues (e.g., >20% of sites missing data) |
| **ERROR** | Failures that affect results |

### Stage Tags

Log messages are prefixed with stage tags for filtering:

- `[CONFIG]` - Configuration loading, mode, timezone
- `[API]` - iEasyHydro HF requests and responses
- `[DATA]` - DataFrame transformations, outlier filtering
- `[MERGE]` - Combining data sources
- `[OUTPUT]` - Writing results, final summaries
- `[TIMING]` - Performance metrics

### Configuration

Log level can be configured at two levels (higher priority first):

1. **Module-specific** (`config.yaml`):
   ```yaml
   logging:
     log_level: DEBUG  # Override for this module only
   ```

2. **System-wide** (`.env` file):
   ```bash
   log_level=INFO  # Default for all modules
   ```

If neither is set, defaults to `INFO`.

### Example Output

```
2025-01-12 08:00:01 - INFO - [CONFIG] Mode: MAINTENANCE
2025-01-12 08:00:01 - INFO - [CONFIG] Timezone: Asia/Bishkek
2025-01-12 08:00:02 - INFO - [API] Request: 62 sites, WDDA, 2024-09-14 to 2025-01-12
2025-01-12 08:00:15 - INFO - [API] Response WDDA: 3780 records from 53/62 sites (85.5%)
2025-01-12 08:00:15 - WARNING - [API] Sites without WDDA data: ['15020', '15025']
2025-01-12 08:00:16 - INFO - [MERGE] Complete: 50000 existing + 1200 new = 51200 total
2025-01-12 08:00:17 - INFO - [OUTPUT] Final: 51200 records, 62 sites, 2020-01-01 to 2025-01-12
2025-01-12 08:00:17 - INFO - [TIMING] Total: 16.2s (config: 0.1s, sites: 2.0s, data: 13.1s, process: 0.8s, write: 0.2s)
```

### Log Files

Logs are written to `logs/log` with daily rotation (kept for 30 days).

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [User Guide](../../doc/user_guide.md) - How to use the forecast system
