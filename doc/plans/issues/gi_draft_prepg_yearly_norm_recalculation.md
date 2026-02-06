# Yearly Norm Recalculation for Snow and Meteo Data

**Status**: Draft
**Module**: preprocessing_gateway
**Priority**: Medium
**Labels**: `enhancement`, `cron`, `snow-data`, `meteo-data`, `maintenance`, `refactor`

---

## Summary

Extract norm calculation from daily processing and implement a yearly maintenance task (mid-August) that calculates climatological norms for both snow (SWE, HS, RoF) and meteo (T, P) data using a configurable averaging window (default: 20 years).

## Context

SAPPHIRE uses climatological norms (long-term daily averages) to contextualize current observations. Currently, temperature (T) and precipitation (P) norms are calculated **daily** in `extend_era5_reanalysis.py` using all available historical data.

This approach has limitations:
- Norms are recalculated every day unnecessarily (they change slowly)
- The averaging window is implicit (all available data) rather than explicit
- Snow data (SWE, HS, RoF) does not have norm calculations at all
- No user control over the averaging period

The new approach will:
- Calculate norms **once per year** in mid-August
- Use a **configurable window** (e.g., 20 years) specified in `config.yaml`
- Apply consistently to both snow and meteo data
- Run via a dedicated cron job

Mid-August timing is appropriate because:
1. By August, the full hydrological year (Oct-Sep) data is complete
2. Provides time before the next snow season begins (Oct-Nov)
3. Any issues can be resolved before forecasts depend on updated norms

## Problem

1. **Snow norms missing**: No climatological norms exist for SWE, HS, RoF variables
2. **Inefficient calculation**: T/P norms are recalculated daily despite rarely changing
3. **No user control**: The averaging window cannot be configured
4. **Inconsistent approach**: Snow and meteo data are handled differently

## Desired Outcome

- A cron job (`yearly_maintain_meteo_norms.sh`) runs annually in mid-August
- Norms are calculated for all variables: SWE, HS, RoF, T, P
- Averaging window is configurable via `config.yaml` (default: 20 years)
- Daily `extend_era5_reanalysis.py` no longer calculates norms (it only extends data)
- Results are written to both CSV files and the SAPPHIRE API

---

## Technical Analysis

### Current Implementation

**Current T/P norm calculation** - `apps/preprocessing_gateway/extend_era5_reanalysis.py:414-448`:
```python
# CALCULATE DAILY NORM VALUES
logger.debug('Calculating Daily Norm Values for later display in dashboard')
era5_reanalysis_P['dayofyear'] = era5_reanalysis_P['date'].dt.dayofyear
era5_reanalysis_T['dayofyear'] = era5_reanalysis_T['date'].dt.dayofyear

# Group by code and dayofyear and calculate the mean
daily_norm_P = era5_reanalysis_P.groupby(['code', 'dayofyear'])['P'].mean().round(2).reset_index()
daily_norm_T = era5_reanalysis_T.groupby(['code', 'dayofyear'])['T'].mean().round(2).reset_index()
# ... date handling and saving ...
```

This logic will be:
1. **Removed** from `extend_era5_reanalysis.py`
2. **Moved** to a new `calculate_norms.py` script
3. **Extended** to support snow variables
4. **Enhanced** with configurable time window

**Snow data retrieval** - `apps/preprocessing_gateway/snow_data_renalysis.py`:
- `get_snow_data_reanalysis()` fetches historical snow data from the Data Gateway
- Variables: SWE, HS, RoF
- Data stored in `{save_path}/{variable}/{HRU}_{variable}.csv`

**Maintenance script pattern** - `bin/daily_linreg_maintenance.sh`:
- Sources `bin/utils/common_functions.sh`
- Reads configuration from `.env`
- Creates timestamped log files
- Runs Docker containers with appropriate mounts

### Key Files

| File | Current Purpose |
|------|-----------------|
| `apps/preprocessing_gateway/extend_era5_reanalysis.py` | Extends ERA5 data + calculates T/P norms (daily) |
| `apps/preprocessing_gateway/snow_data_renalysis.py` | Fetches snow reanalysis data |
| `bin/daily_ml_maintenance.sh` | Template for maintenance scripts |
| `apps/config/.env` | Current configuration location |

---

## Implementation Plan

### Approach

1. Create new script `calculate_norms.py` with configurable window
2. Create shell script `yearly_maintain_meteo_norms.sh` for cron
3. Add configuration to a new `config.yaml` file
4. Remove norm calculation from `extend_era5_reanalysis.py`
5. Update dashboard data pipeline to use pre-calculated norms

### Files to Create

| File | Purpose |
|------|---------|
| `bin/yearly_maintain_meteo_norms.sh` | Cron-triggered shell script |
| `apps/preprocessing_gateway/calculate_norms.py` | Python script for all norm calculations |
| `apps/preprocessing_gateway/config.yaml` | Configuration file with `norm_averaging_years` setting |

### Files to Modify

| File | Changes |
|------|---------|
| `apps/preprocessing_gateway/extend_era5_reanalysis.py` | Remove lines 414-448 (norm calculation), remove norm-related API writes |

### Implementation Steps

- [ ] Step 1: Create `apps/preprocessing_gateway/config.yaml` with norm configuration
  ```yaml
  norms:
    averaging_years: 20  # Number of years to use for norm calculation
    variables:
      meteo: ["T", "P"]
      snow: ["SWE", "HS", "RoF"]
  ```

- [ ] Step 2: Create `apps/preprocessing_gateway/calculate_norms.py`
  - Load configuration from `config.yaml`
  - Function to calculate meteo norms (T, P) from ERA5 reanalysis files
  - Function to calculate snow norms (SWE, HS, RoF) from snow reanalysis files
  - Handle leap year day 366 (map to day 365)
  - Write results to dashboard CSV files
  - Write results to SAPPHIRE API
  - Add `main()` with argument parsing for standalone execution

- [ ] Step 3: Create `bin/yearly_maintain_meteo_norms.sh`
  - Follow pattern from `daily_linreg_maintenance.sh`
  - Source common functions
  - Create log directory (`logs/yearly_norms/`)
  - Run Docker container with `calculate_norms.py`
  - Document recommended crontab entry

- [ ] Step 4: Modify `apps/preprocessing_gateway/extend_era5_reanalysis.py`
  - Remove norm calculation block (lines 414-448)
  - Remove calls to `_write_meteo_to_api(daily_norm_P, 'P')` and similar
  - Update logging to reflect removed functionality
  - Ensure the extended reanalysis data is still saved (keep lines 457-459)

- [ ] Step 5: Update Docker image to include new script
  - Ensure `calculate_norms.py` is included in `sapphire-preprocessing-gateway` image
  - Ensure `config.yaml` is mounted or copied

- [ ] Step 6: Add documentation
  - Crontab entry example in `bin/README.md`
  - Document `config.yaml` settings

### Code Examples

**config.yaml:**
```yaml
# Climatological norm calculation settings
norms:
  # Number of years of historical data to use for calculating norms
  # Norms are calculated as the mean of each day-of-year across this window
  averaging_years: 20

  # Variables to calculate norms for
  variables:
    meteo:
      - T   # Temperature
      - P   # Precipitation
    snow:
      - SWE  # Snow Water Equivalent
      - HS   # Snow Height (depth)
      - RoF  # Runoff
```

**calculate_norms.py structure:**
```python
"""
Yearly norm calculation for snow and meteo data.

Calculates climatological norms (daily averages) using a configurable
time window. Run once per year in mid-August.

Usage:
    python calculate_norms.py [--config /path/to/config.yaml]
"""

import yaml
import pandas as pd
from datetime import datetime

def load_config(config_path: str = None) -> dict:
    """Load configuration from config.yaml."""
    ...

def calculate_meteo_norms(
    reanalysis_path: str,
    output_path: str,
    hru_codes: list[str],
    averaging_years: int = 20
) -> dict[str, pd.DataFrame]:
    """
    Calculate norms for T and P from ERA5 reanalysis data.

    Args:
        reanalysis_path: Path to reanalysis CSV files
        output_path: Path to write dashboard CSV files
        hru_codes: List of HRU codes to process
        averaging_years: Number of years to include in average

    Returns:
        Dict mapping variable name to norm DataFrame
    """
    current_year = datetime.now().year
    cutoff_date = f"{current_year - averaging_years}-01-01"

    for hru in hru_codes:
        for var in ['T', 'P']:
            # Read reanalysis file
            df = pd.read_csv(f"{reanalysis_path}/{hru}_{var}_reanalysis.csv")
            df['date'] = pd.to_datetime(df['date'])

            # Filter to averaging window
            df = df[df['date'] >= cutoff_date]

            # Calculate norms
            df['dayofyear'] = df['date'].dt.dayofyear
            norms = df.groupby(['code', 'dayofyear'])[var].mean().round(2).reset_index()

            # Handle leap year...
            # Write to dashboard file...
    ...

def calculate_snow_norms(
    snow_data_path: str,
    output_path: str,
    hru_codes: list[str],
    variables: list[str],
    averaging_years: int = 20
) -> dict[str, pd.DataFrame]:
    """
    Calculate norms for snow variables from reanalysis data.

    Similar logic to meteo norms but for SWE, HS, RoF.
    """
    ...

def main():
    """Main entry point for yearly norm calculation."""
    config = load_config()
    averaging_years = config['norms']['averaging_years']

    # Calculate meteo norms
    calculate_meteo_norms(...)

    # Calculate snow norms
    calculate_snow_norms(...)

    # Write to API
    ...

if __name__ == '__main__':
    main()
```

**Shell script structure:**
```bash
#!/bin/bash
# Yearly Norm Recalculation Script
#
# Calculates climatological norms for snow and meteo data.
# Should be run once per year in mid-August.
#
# Recommended crontab entry:
#   0 2 15 8 * /path/to/yearly_maintain_meteo_norms.sh /path/to/.env
#
# This runs at 02:00 on August 15th each year.

source "$(dirname "$0")/utils/common_functions.sh"

print_banner
echo "| Running Yearly Norm Recalculation"
echo "| Calculating norms for: T, P, SWE, HS, RoF"

read_configuration $1

# Validate environment
if [ -z "$ieasyhydroforecast_data_root_dir" ]; then
    echo "Error: Required environment variables not set"
    exit 1
fi

# Create log directory
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/yearly_norms"
mkdir -p ${LOG_DIR}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting yearly norm calculation"

# Run Docker container
docker run --rm \
    --network host \
    -e ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir} \
    -e ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path} \
    -v ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config \
    -v ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data \
    mabesa/sapphire-preprocessing-gateway:latest \
    python calculate_norms.py \
    2>&1 | tee -a "$log_file"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    log_message "Yearly norm calculation completed successfully"
else
    log_message "ERROR: Norm calculation failed with exit code $EXIT_CODE"
fi

# Clean up old logs (keep 2 years worth)
find $LOG_DIR -type f -mtime +730 -delete

log_message "Done"
```

---

## Testing

### Test Cases

- [ ] Verify averaging window calculation is correct (e.g., in 2026 with 20-year window, should use 2006-2025 data)
- [ ] Verify leap year handling (day 366 mapped to day 365)
- [ ] Verify output CSV format matches existing dashboard file format
- [ ] Verify API writes succeed for all variables
- [ ] Verify script runs successfully in Docker container
- [ ] Verify `extend_era5_reanalysis.py` still works without norm calculation
- [ ] Verify norms match between old and new calculation (for T/P, first run)
- [ ] Verify idempotency (running twice produces same results)

### Testing Commands

```bash
# Local testing (without Docker)
cd apps/preprocessing_gateway
python calculate_norms.py --config config.yaml --dry-run

# Full test via shell script
bash bin/yearly_maintain_meteo_norms.sh /path/to/.env

# Verify extend_era5_reanalysis.py still works
cd apps/preprocessing_gateway
SAPPHIRE_OPDEV_ENV=True python extend_era5_reanalysis.py
```

### Manual Verification

1. Check output CSV files exist in expected locations
2. Compare T/P norms from new script against previous daily-calculated norms
3. Verify snow norm files are created (new!)
4. Verify API endpoints have data for all variables

---

## Out of Scope

- Creating new API endpoints (assume existing endpoints support all variables)
- Historical backfill of norms for years before implementation
- Automation of cron job deployment (manual server setup required)
- Changing the dashboard to display snow norms (separate issue)

## Dependencies

- Data Gateway must have sufficient historical data for the averaging window
- SAPPHIRE API snow endpoints must support norm data (verify or create as prerequisite)
- Docker image must be rebuilt to include new script

## Acceptance Criteria

- [ ] Shell script `bin/yearly_maintain_meteo_norms.sh` exists and runs successfully
- [ ] Python script `apps/preprocessing_gateway/calculate_norms.py` calculates norms correctly
- [ ] Configuration file `apps/preprocessing_gateway/config.yaml` exists with `norms.averaging_years` setting
- [ ] Norms are calculated for: T, P, SWE, HS, RoF
- [ ] `extend_era5_reanalysis.py` no longer calculates norms
- [ ] `extend_era5_reanalysis.py` still extends reanalysis data correctly
- [ ] Output CSV files match expected dashboard format
- [ ] Script can be scheduled via cron to run mid-August
- [ ] Logging captures execution details for troubleshooting
- [ ] Documentation includes crontab entry example
- [ ] All existing tests pass
- [ ] New tests cover norm calculation logic

---

## References

- Current meteo norm calculation: `apps/preprocessing_gateway/extend_era5_reanalysis.py:414-448`
- Snow data retrieval: `apps/preprocessing_gateway/snow_data_renalysis.py`
- Maintenance script pattern: `bin/daily_linreg_maintenance.sh`
- API write pattern: `apps/preprocessing_gateway/extend_era5_reanalysis.py:88-171`
