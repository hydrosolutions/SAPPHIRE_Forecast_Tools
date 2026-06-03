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
| `recalculate_snow_norms.py` | Recalculates yearly snow norms and dashboard-facing snow statistics from historical reanalysis CSVs |
| `backfill_new_stations.py` | Detects new/stale stations and backfills their history to the API |
| `bin/backfill_snow_stats_history.sh` | One-time wrapper that backfills historical snow statistics year by year |
| `dg_utils.py` | Utility functions for data gateway interactions and quantile mapping |

### Supporting Scripts (not run operationally)

| Script | Purpose |
|--------|---------|
| `get_era5_reanalysis_data.py` | Downloads historical ERA5-Land reanalysis data (initial setup) |
| `snow_data_renalysis.py` | Downloads historical snow reanalysis data (initial setup) |

## Running the Pipeline

The three main scripts run sequentially: QM -> extend ERA5 -> snow. The `SAPPHIRE_SYNC_MODE` environment variable controls how much data each script writes to the API. CSVs are always written in full regardless of sync mode.

### Operational mode (daily)

Writes **yesterday's and today's** data to the API (2-day window guards against data lag from the Data Gateway). Reanalysis API write is skipped entirely. This is the default.

```bash
# Local (via run_locally.sh)
ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh preprocessing_gateway

# Local (manual, from apps/preprocessing_gateway/)
uv run Quantile_Mapping_OP.py
uv run extend_era5_reanalysis.py
uv run snow_data_operational.py

# Docker
docker run mabesa/sapphire-prepgateway:latest
```

### Maintenance mode

Writes a **lookback window** to the API: 30 days for meteo and snow, 365 days for reanalysis. Use for weekly/monthly gap-filling.

```bash
# Local (via run_locally.sh — runs extend_era5_reanalysis.py only)
ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh maintenance:preprocessing_gateway

# Local (manual, all 3 scripts)
SAPPHIRE_SYNC_MODE=maintenance uv run Quantile_Mapping_OP.py
SAPPHIRE_SYNC_MODE=maintenance uv run extend_era5_reanalysis.py
SAPPHIRE_SYNC_MODE=maintenance uv run snow_data_operational.py

# Docker
docker run -e SAPPHIRE_SYNC_MODE=maintenance mabesa/sapphire-prepgateway:latest
```

### Initial mode

Writes **all historical data** to the API. Use for first-time database population or bulk recovery after a database reset.

```bash
# Local
SAPPHIRE_SYNC_MODE=initial uv run Quantile_Mapping_OP.py
SAPPHIRE_SYNC_MODE=initial uv run extend_era5_reanalysis.py
SAPPHIRE_SYNC_MODE=initial uv run snow_data_operational.py

# Docker
docker run -e SAPPHIRE_SYNC_MODE=initial mabesa/sapphire-prepgateway:latest
```

### Backfill (new/stale stations)

A separate entry point that queries the API coverage endpoints, detects stations present in CSVs but missing or stale in the API, and writes only the gap. Requires `sapphire_api_client` and a running API.

```bash
# Local
SAPPHIRE_SYNC_MODE=initial uv run backfill_new_stations.py

# Docker (overrides the default CMD)
docker run mabesa/sapphire-prepgateway:latest uv run backfill_new_stations.py
```

### Yearly snow norms and statistics

Run the yearly snow recalculation after the snow reanalysis files have been updated for the previous season, normally at the end of August. The yearly wrapper now writes both the climatological norm fields and the dashboard-facing snow statistic fields in one run; no separate statistics invocation is needed.

```bash
bash bin/yearly_snow_norm_recalculation.sh /path/to/config/.env
```

The command requires a valid env file path and the SAPPHIRE API stack to be running. Logs are written to `${ieasyhydroforecast_data_root_dir}/logs/snow_norm_recalc/`; `LOG_DIR` and the `prepgw-snow-norm-recalc` container name are historical names and now cover both norm and statistic recalculation. Expect about 15-20 minutes per year (P3 measured 954 seconds for 2026 with `SWE`, `HS`, and `RoF`).

Run the historical snow-stat backfill once after this work is deployed to populate historical years that predate the unified yearly recalculation. It uses the same env file and running API stack, skips the current year, can resume from `${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill/backfill_progress.txt`, and writes logs under `${ieasyhydroforecast_data_root_dir}/logs/snow_stat_backfill/`.

```bash
ieasyhydroforecast_env_file_path=/path/to/config/.env \
  bash bin/backfill_snow_stats_history.sh --start-year 2010
```

The snow tab depends on these statistic columns for the percentile, last-year, and current-year bands. Dashboard work on `develop_dashboard_snow_display` is paused until the merged preprocessing output is available.

### Sync mode summary

| Mode | `SAPPHIRE_SYNC_MODE` | Meteo API writes | Reanalysis API writes | Snow API writes |
|------|----------------------|------------------|-----------------------|-----------------|
| Operational | _(default)_ | yesterday + today | skipped | yesterday + today |
| Maintenance | `maintenance` | last 30 days | last 365 days | last 30 days |
| Initial | `initial` | all rows | all rows | all rows |
| Backfill | `initial` | per-station gap | _N/A_ | per-station gap |

## Data Flow

```
SAPPHIRE Data Gateway (ECMWF IFS + Snowmapper)
         |
         v
+---------------------------------+
|  Quantile_Mapping_OP.py         |
|  - Control member forecast      |     --> CSV + API (meteo)
|  - Ensemble forecasts           |     --> CSV only
|  - Bias correction              |
+---------------------------------+
         |
         v
+---------------------------------+
|  extend_era5_reanalysis.py      |
|  - Merge with ERA5 history      |     --> CSV (reanalysis)
|  - Calculate daily norms        |     --> CSV + API (dashboard)
|  - Write reanalysis to API      |     --> API (maintenance/initial only)
+---------------------------------+
         |
         v
+---------------------------------+
|  snow_data_operational.py       |
|  - Snow water equivalent (SWE)  |     --> CSV + API (snow)
|  - Snow depth (HS)              |
|  - Runoff fraction (RoF)        |
+---------------------------------+
         |
         v
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
| `ieasyhydroforecast_HRU_SNOW_DATA` | HRU codes for snow data |
| `ieasyhydroforecast_SNOW_VARS` | Snow variables to download (e.g. `SWE,HS,RoF`) |
| `ieasyhydroforecast_Q_MAP_PARAM_PATH` | Path to quantile mapping parameter files |
| `ieasyhydroforecast_OUTPUT_PATH_CM` | Output path for control member data |
| `ieasyhydroforecast_OUTPUT_PATH_ENS` | Output path for ensemble data |
| `ieasyhydroforecast_OUTPUT_PATH_REANALYSIS` | Output path for reanalysis data |
| `ieasyhydroforecast_OUTPUT_PATH_SNOW` | Output path for snow data |
| `SAPPHIRE_SYNC_MODE` | API write scope: `operational` (default), `maintenance`, or `initial` |
| `SAPPHIRE_API_ENABLED` | Enable/disable API writes (`true`/`false`, default `true`) |
| `SAPPHIRE_API_URL` | API base URL (default `http://localhost:8000`) |
| `SAPPHIRE_CONSISTENCY_CHECK` | Enable post-write consistency check (`true`/`false`, default `false`) |

See [doc/configuration.md](../../doc/configuration.md) for the complete list of environment variables.

## Output Data Formats

### Control Member Output
CSV files with columns: `date`, `P` or `T`, `code`

### Ensemble Output
CSV files with columns: `date`, `P` or `T`, `code`, `ensemble_member`

### Snow Output
CSV files with columns: `date`, `<var>`, `<var>_1` ... `<var>_N`, `code`
where `<var>` is `SWE`, `HS`, or `RoF` and `_1` ... `_N` are elevation band values.

### Reanalysis Dashboard Output
CSV files with columns: `code`, `<var>_norm`, `date`, `<var>`
where `<var>_norm` is the climatological daily mean.

### Quantile Mapping Parameters
Parameter files named `HRU{code}_P_params.csv` and `HRU{code}_T_params.csv` with columns: `code`, `a`, `b`, `wet_day`

The quantile mapping formula: `y_corrected = a * y_raw^b`

## Testing

```bash
# Run all preprocessing_gateway tests (275 tests)
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway

# Run only the integration tests (22 tests)
SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py -v

# Run a single test class
SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_integration_preprocessing_gateway.py::TestCrossScriptDataFlow -v
```

Test files:

| File | What it covers |
|------|---------------|
| `test_integration_preprocessing_gateway.py` | End-to-end pipeline: cross-script CSV handoff, sync modes, snow pipeline, error propagation, backfill |
| `test_api_integration.py` | API write functions: sync modes, elevation bands, NaN handling, consistency checks |
| `test_backfill_new_stations.py` | Gap detection, CSV extraction, backfill writers |
| `test_data_transforms.py` | Quantile mapping, DG data transformation |
| `test_edge_cases.py` | Edge cases for data processing |
| `test_reanalysis_processing.py` | Stable data selection, reanalysis extension, norm calculation |
| `test_reanalysis_staleness.py` | Reanalysis lag, growth, stability window tuning |

## Requirements

- Access to SAPPHIRE Data Gateway (API key required)
- `sapphire-dg-client` package (private repository - contact hydrosolutions for access)
- `sapphire-api-client` package (optional - required for API writes and backfill)
- Python 3.12+ with dependencies listed in `pyproject.toml`

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [Deployment Guide](../../doc/deployment.md) - Production deployment instructions
- [Testing Workflow](../../doc/dev/testing_workflow.md) - Full testing workflow across all stages
