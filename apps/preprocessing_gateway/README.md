# Preprocessing Gateway Module

Downloads and processes weather data (ECMWF ERA5 Land and IFS HRES open data) and snow data (SnowMapper) from the SAPPHIRE Data Gateway, performs quantile mapping on ERA5/ECMWF forecast data, and prepares forcing data for forecast models.

## Overview

This module is the first step in the operational forecast pipeline. It runs daily (typically at 04:00 UTC) to:

1. Download operational weather forecasts from the SAPPHIRE Data Gateway
2. Apply bias correction (quantile mapping) to temperature and precipitation
3. Extend ERA5 reanalysis data with the latest observations
4. Process snow data from the Snowmapper model for snow-influenced catchments

## Scripts

| Script | Purpose |
|--------|---------|
| `Quantile_Mapping_OP.py` | Downloads and bias-corrects operational ECMWF IFS forecasts (control member + ensemble) |
| `extend_era5_reanalysis.py` | Extends historical ERA5-Land reanalysis with recent operational data |
| `snow_data_operational.py` | Processes operational snow data (SWE, snow depth, snow melt and direct precipitation runoff) |
| `dg_utils.py` | Utility functions for data gateway interactions and quantile mapping |

### Supporting Scripts (not run operationally)

| Script | Purpose |
|--------|---------|
| `get_era5_reanalysis_data.py` | Downloads historical ERA5-Land reanalysis data (initial setup) |
| `snow_data_renalysis.py` | Downloads historical snow reanalysis data (initial setup) |

## Quick Start

```bash
# Set the path to your environment configuration file
export ieasyhydroforecast_env_file_path=/path/to/config/.env

# Run the main quantile mapping script
python Quantile_Mapping_OP.py

# Or run all three operational scripts in sequence (as done in Docker)
python Quantile_Mapping_OP.py && python extend_era5_reanalysis.py && python snow_data_operational.py
```

## Docker Usage

```bash
# Build the image (from repository root)
docker build -f apps/preprocessing_gateway/Dockerfile.py312 -t mabesa/sapphire-prepgateway:py312-test .

# Run the container
docker run --rm \
    -e ieasyhydroforecast_data_root_dir=/data \
    -e ieasyhydroforecast_env_file_path=/data/config/.env \
    -v /path/to/data:/data \
    mabesa/sapphire-prepgateway:py312-test
```

## Data Flow

```
SAPPHIRE Data Gateway (ECMWF IFS + Snowmapper)
         │
         ▼
┌─────────────────────────────┐
│  Quantile_Mapping_OP.py     │
│  - Control member forecast  │
│  - Ensemble forecasts       │
│  - Bias correction          │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  extend_era5_reanalysis.py  │
│  - Merge with ERA5 history  │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  snow_data_operational.py   │
│  - Snow water equivalent    │
│  - Snow depth               │
│  - Runoff fraction          │
└─────────────────────────────┘
         │
         ▼
    Output CSVs for
    forecast models
```

## Key Environment Variables

| Variable | Description |
|----------|-------------|
| `ieasyhydroforecast_env_file_path` | Path to the .env configuration file |
| `ieasyhydroforecast_API_KEY_GATEAWAY` | API key for SAPPHIRE Data Gateway access |
| `ieasyhydroforecast_HRU_CONTROL_MEMBER` | HRU codes for control member forecasts |
| `ieasyhydroforecast_HRU_ENSEMBLE` | HRU codes for ensemble forecasts |
| `ieasyhydroforecast_Q_MAP_PARAM_PATH` | Path to quantile mapping parameter files |
| `ieasyhydroforecast_OUTPUT_PATH_CM` | Output path for control member data |
| `ieasyhydroforecast_OUTPUT_PATH_ENS` | Output path for ensemble data |

See [doc/configuration.md](../../doc/configuration.md) for the complete list of environment variables.

## Output Data Formats

### Control Member Output
CSV files with columns: `date`, `P` or `T`, `code`

### Ensemble Output
CSV files with columns: `date`, `P` or `T`, `code`, `ensemble_member`

### Quantile Mapping Parameters
Parameter files named `HRU{code}_P_params.csv` and `HRU{code}_T_params.csv` with columns: `code`, `a`, `b`, `wet_day`

The quantile mapping formula: `y_corrected = a × y_raw^b`

## Requirements

- Access to SAPPHIRE Data Gateway (API key required)
- `sapphire-dg-client` package (private repository - contact hydrosolutions for access)
- Python 3.11+ with dependencies listed in `pyproject.toml`

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [Deployment Guide](../../doc/deployment.md) - Production deployment instructions