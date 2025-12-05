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

# Run the preprocessing script
python preprocessing_runoff.py
```

## Docker Usage

```bash
# Build the image (from repository root)
docker build -f apps/preprocessing_runoff/Dockerfile.py312 -t mabesa/sapphire-preprunoff:py312-test .

# Run the container (requires SSH tunnel to iEasyHydro HF)
docker run --rm \
    --network host \
    -e ieasyhydroforecast_data_root_dir=/data \
    -e ieasyhydroforecast_env_file_path=/data/config/.env \
    -e IEASYHYDROHF_HOST=http://host.docker.internal:5555/api/v1/ \
    -v /path/to/data:/data \
    mabesa/sapphire-preprunoff:py312-test
```

**Note:** On macOS, use `host.docker.internal` to connect to an SSH tunnel running on the host.

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

## Development

### Run Locally

From the `preprocessing_runoff` directory:
```bash
ieasyhydroforecast_env_file_path=/path/to/.env python preprocessing_runoff.py
```

### Run Tests

From the `apps` directory:
```bash
SAPPHIRE_TEST_ENV=True python -m pytest preprocessing_runoff/tests -v
```

## Key Environment Variables

| Variable | Description |
|----------|-------------|
| `ieasyhydroforecast_env_file_path` | Path to the .env configuration file |
| `IEASYHYDROHF_HOST` | iEasyHydro HF API endpoint URL |
| `IEASYHYDROHF_USERNAME` | API username |
| `IEASYHYDROHF_PASSWORD` | API password |

See [doc/configuration.md](../../doc/configuration.md) for the complete list.

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [User Guide](../../doc/user_guide.md) - How to use the forecast system
