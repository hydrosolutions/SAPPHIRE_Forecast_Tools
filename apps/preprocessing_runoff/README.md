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
