# Yearly Snow Norm Recalculation

**Status**: Draft
**Module**: preprocessing_gateway
**Priority**: Medium
**Labels**: `enhancement`, `cron`, `snow-data`, `maintenance`

---

## Summary

Implement yearly snow norm calculation (SWE, HS, RoF) and integrate norms
into the daily operational pipeline so the forecast dashboard can display
climatological reference lines for snow variables.

## Context

SAPPHIRE uses climatological norms (long-term daily averages) to give
operational hydrologists context for current observations. Temperature and
precipitation norms are already calculated daily in
`extend_era5_reanalysis.py:500-558` using `calculate_daily_norm()`. The
dashboard plots these as reference lines.

**Snow norms are missing entirely.** The infrastructure is ready — the
Snow database model has a `norm` column, the API write functions pass
`norm` through, and the dashboard visualization reads `norm` from current-
year records (`vizualization.py:2186`) — but no code actually computes
snow norms. The `norm` column is always `None`.

Snow data comes from the SAPPHIRE Data Gateway (SnowMapper reanalysis) and
is stored as CSV files at `{OUTPUT_PATH_SNOW}/{variable}/{HRU}_{variable}.csv`
with historical data from 2000 to present.

## Problem

1. **Snow norms not calculated**: No climatological norms exist for SWE,
   HS, or RoF variables
2. **Dashboard shows no reference**: The snow plots lack the norm line that
   T and P plots have, making it harder for hydrologists to assess current
   conditions
3. **API records have null norm**: Every snow record written to the API has
   `norm: None`

## Desired Outcome

- A standalone script calculates snow norms from all historical data
- A shell script (`bin/yearly_snow_norm_recalculation.sh`) runs the
  calculation in Docker, scheduled for end of August each year
- Norms are written to the API for the full current year
- The daily operational script preserves norms when writing new records
- The dashboard displays snow norm reference lines (no dashboard changes
  needed — it already reads the `norm` column)

---

## Technical Analysis

### Current Implementation

**Snow data flow (operational):**
1. `snow_data_operational.py:main()` — fetches today's snow from Data
   Gateway for each HRU and variable
2. `dg_utils.transform_snow_data()` — parses columns, handles elevation
   bands (value1-value14), computes mean across bands
3. CSV written to `{OUTPUT_PATH_SNOW}/{var}/{HRU}_{var}.csv`
4. `_write_snow_to_api()` — writes to preprocessing API with upsert on
   `(snow_type, code, date)`. The `norm` field is included but always
   `None` since no code populates it.

**Snow data flow (reanalysis):**
1. `snow_data_renalysis.py:main()` — fetches historical data from Data
   Gateway in 5-year batches (2000 to today-180d)
2. Same transform, CSV write, and API write pattern as operational
3. Historical CSVs accumulate all data: `{HRU}_{var}.csv` with columns
   `[date, {var}, code, {var}_1, {var}_2, ...]`

**Reference: Meteo norm calculation** —
`extend_era5_reanalysis.py:500-558`:
```python
def calculate_daily_norm(reanalysis, operational, value_col, current_year):
    reanalysis['dayofyear'] = reanalysis['date'].dt.dayofyear
    daily_norm = (
        reanalysis
        .groupby(['code', 'dayofyear'])[value_col]
        .mean().round(2).reset_index()
    )
    # Handle leap year, create dates, merge with operational...
```

**Snow DB model** — `sapphire/services/preprocessing/app/models.py:117-148`:
- Fields: `snow_type`, `code`, `date`, `value`, `norm`, `value1-value14`
- Unique constraint: `(snow_type, code, date)`
- **No `day_of_year` column** (unlike Meteo which has one)

**API upsert behavior** — `crud.py:205-267`: Full-record replacement on
`(snow_type, code, date)`. Cannot update norm alone — must provide all
fields. This means writing a record with `norm=None` **overwrites** any
existing norm. The daily script must preserve norms when writing.

**Dashboard consumption** — `vizualization.py:2186`:
```python
norm_snow = current_year[['doy', 'norm', 'date']].copy()
norm_snow.rename(columns={"norm": variable}, inplace=True)
```
Already reads `norm` from the API response. Will work as soon as norms
are populated.

### Key Files

| File | Role |
|------|------|
| `apps/preprocessing_gateway/dg_utils.py:213-284` | `transform_snow_data()` — snow data parsing |
| `apps/preprocessing_gateway/snow_data_operational.py` | Daily snow data fetch + CSV/API write |
| `apps/preprocessing_gateway/snow_data_renalysis.py` | Historical snow data fetch |
| `apps/preprocessing_gateway/extend_era5_reanalysis.py:500-558` | Reference: `calculate_daily_norm()` for meteo |
| `sapphire/services/preprocessing/app/models.py:117-148` | Snow DB model (has `norm` field) |
| `sapphire/services/preprocessing/app/crud.py:205-267` | Snow upsert logic |
| `apps/forecast_dashboard/src/vizualization.py:2186` | Dashboard reads `norm` column |
| `apps/forecast_dashboard/src/db.py:194-238` | Dashboard fetches snow from API |
| `bin/yearly_skill_metrics_recalculation.sh` | Pattern for yearly shell scripts |
| `bin/utils/common_functions.sh` | Shared shell utilities |

### Upsert Conflict & Resolution

The API uses full-record upsert on `(snow_type, code, date)`. This
creates a conflict:

1. Yearly script writes record with `norm=45.2, value=None` for a future
   date
2. Daily script later writes the same record with `value=120.5, norm=None`
3. The upsert replaces the entire record → norm is lost

**Resolution**: The shared `write_snow_to_api()` function (see Step 3)
reads existing records for the dates being written and preserves their
`norm` values. This adds one API read per (code, snow_type) per write
call — negligible overhead for the ~1-2 dates being written daily.

This also fixes the reanalysis script, which has the same problem: its
own copy of `_write_snow_to_api()` also writes `norm=None`, so running
reanalysis after norms have been set would erase them.

---

## Implementation Plan

### Approach

**API-first design — the API is the single source of truth for norms:**

1. **Yearly script** (end of August): Read all historical snow CSVs,
   calculate day-of-year means, write norm records to the API for the
   full current year.

2. **Shared write function**: Both the daily operational and reanalysis
   scripts use a single `write_snow_to_api()` in `dg_utils.py` that
   preserves existing norms when writing. This eliminates the duplicated
   `_write_snow_to_api()` and ensures neither script can erase norms.

No intermediate CSV lookup file needed. The API stores norms, the shared
write function preserves them.

### Duplication to eliminate

Both `snow_data_operational.py` and `snow_data_renalysis.py` have their
own `_write_snow_to_api()`. They differ only in date windowing:

| | Operational (`snow_data_operational.py:85-280`) | Reanalysis (`snow_data_renalysis.py:95-240`) |
|---|---|---|
| Date window | mode-based: operational (yesterday+today), maintenance (last 30d from today), initial (all) | last 30 days relative to `data['date'].max()` |
| API checks, record building, norm handling | identical | identical |
| Consistency check | `_check_snow_consistency()` — checks yesterday+today window | `_check_snow_consistency()` — checks 30d from data max |

The shared function unifies these via a `reference_date` parameter:
- Operational calls with `reference_date=None` → defaults to today
- Reanalysis calls with `reference_date=data['date'].max()`

### Files to Create

| File | Purpose |
|------|---------|
| `apps/preprocessing_gateway/recalculate_snow_norms.py` | Standalone Python script for yearly norm calculation |
| `bin/yearly_snow_norm_recalculation.sh` | Shell script to run in Docker via cron |

### Files to Modify

| File | Changes |
|------|---------|
| `apps/preprocessing_gateway/dg_utils.py` | Add `calculate_snow_norms()` and `write_snow_to_api()` |
| `apps/preprocessing_gateway/snow_data_operational.py` | Remove `_write_snow_to_api()`, call `dg_utils.write_snow_to_api()` |
| `apps/preprocessing_gateway/snow_data_renalysis.py` | Remove `_write_snow_to_api()`, call `dg_utils.write_snow_to_api()` |

### Implementation Steps

#### Step 1: Add `calculate_snow_norms()` to `dg_utils.py`

Add a function analogous to `calculate_daily_norm()` for meteo, but
adapted for snow data.

```python
def calculate_snow_norms(
    snow_data_path: str,
    hru_codes: list[str],
    variables: list[str],
) -> pd.DataFrame:
    """Calculate climatological daily norms for snow variables.

    Reads historical snow CSVs, groups by (snow_type, code, dayofyear),
    and computes the mean value across all years.

    Args:
        snow_data_path: Base path containing snow CSVs, structured as
            {snow_data_path}/{variable}/{HRU}_{variable}.csv
        hru_codes: List of HRU codes to process (e.g., ['15013', '15029'])
        variables: List of snow variables (e.g., ['SWE', 'HS', 'RoF'])

    Returns:
        DataFrame with columns [snow_type, code, dayofyear, norm]
        containing the climatological mean for each (type, code, day)
        combination.
    """
    all_norms = []
    for variable in variables:
        for hru in hru_codes:
            file_path = os.path.join(
                snow_data_path, variable, f"{hru}_{variable}.csv"
            )
            if not os.path.exists(file_path):
                logger.warning(
                    "Snow CSV not found: %s, skipping", file_path
                )
                continue

            df = pd.read_csv(file_path)
            df['date'] = pd.to_datetime(df['date'])

            # Use the main variable column (mean across elevation bands)
            if variable not in df.columns:
                logger.warning(
                    "Column '%s' not found in %s, skipping",
                    variable, file_path
                )
                continue

            df['dayofyear'] = df['date'].dt.dayofyear

            norm = (
                df.groupby(['code', 'dayofyear'])[variable]
                .mean()
                .round(2)
                .reset_index()
            )
            norm = norm.rename(columns={variable: 'norm'})
            norm['snow_type'] = variable
            all_norms.append(norm)

    if not all_norms:
        return pd.DataFrame(
            columns=['snow_type', 'code', 'dayofyear', 'norm']
        )

    result = pd.concat(all_norms, ignore_index=True)
    result = result[['snow_type', 'code', 'dayofyear', 'norm']]
    return result
```

#### Step 2: Create `recalculate_snow_norms.py`

Standalone script that:
1. Loads environment via `setup_library`
2. Reads HRU codes and snow variables from env vars
3. Calls `calculate_snow_norms()` to get norm lookup table
4. Writes norms to the API for the full current year

For each (snow_type, code, dayofyear):
- Map dayofyear to a date in the current year
- Read existing API record for that date (to preserve `value` and
  elevation bands if the record already exists)
- Write record with updated `norm` via upsert

```python
"""
Yearly snow norm recalculation.

Calculates climatological norms (daily averages across all years) for
snow variables (SWE, HS, RoF) and writes them to the SAPPHIRE API
for the full current year.

Run once per year in late August, after the hydrological year's snow
data is complete.

Usage:
    SAPPHIRE_OPDEV_ENV=True python recalculate_snow_norms.py
"""

import os
import sys
import pandas as pd
import logging

import dg_utils

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, '..', 'iEasyHydroForecast'))
import setup_library as sl

try:
    from sapphire_api_client import SapphirePreprocessingClient
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False

logger = logging.getLogger(__name__)


def write_norms_to_api(
    norms: pd.DataFrame,
    current_year: int,
) -> int:
    """Write norm records to the API for the full current year.

    For each (snow_type, code, dayofyear) in the norms DataFrame:
    1. Map dayofyear to a date in the current year
    2. Read any existing API record for that date (to preserve value
       and elevation band data)
    3. Write back with the norm attached

    Args:
        norms: DataFrame with [snow_type, code, dayofyear, norm].
        current_year: Year to write norms for.

    Returns:
        Total number of records written.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, skipping")
        return 0

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.warning("API not ready at %s", api_url)
        return 0

    total_written = 0

    # Process one (snow_type, code) group at a time to batch API reads
    for (snow_type, code), group in norms.groupby(
        ['snow_type', 'code']
    ):
        # Map dayofyear → date for the current year
        group = group.copy()
        group['date'] = pd.Timestamp(str(current_year)) + pd.to_timedelta(
            group['dayofyear'] - 1, unit='D'
        )
        # Filter out day 366 for non-leap years
        if not _is_leap_year(current_year):
            group = group[group['dayofyear'] <= 365]

        # Read existing records for this (snow_type, code) to
        # preserve values and elevation bands
        start_date = f"{current_year}-01-01"
        end_date = f"{current_year}-12-31"
        try:
            existing = client.read_snow(
                snow_type=snow_type.upper(),
                code=str(code),
                start_date=start_date,
                end_date=end_date,
                limit=400,
            )
        except Exception:
            existing = pd.DataFrame()

        # Build lookup of existing records by date
        existing_by_date = {}
        if not existing.empty:
            existing['date'] = pd.to_datetime(existing['date'])
            for _, row in existing.iterrows():
                existing_by_date[row['date'].date()] = row

        # Build records: norm + preserved existing data
        records = []
        for _, norm_row in group.iterrows():
            date_val = norm_row['date']
            existing_row = existing_by_date.get(date_val.date())

            record = {
                "snow_type": snow_type.upper(),
                "code": str(code),
                "date": date_val.strftime('%Y-%m-%d'),
                "norm": round(float(norm_row['norm']), 2),
                "value": (
                    float(existing_row['value'])
                    if existing_row is not None
                    and pd.notna(existing_row.get('value'))
                    else None
                ),
            }

            # Preserve elevation bands from existing record
            if existing_row is not None:
                for i in range(1, 15):
                    key = f"value{i}"
                    if key in existing_row and pd.notna(
                        existing_row.get(key)
                    ):
                        record[key] = float(existing_row[key])

            records.append(record)

        # Write batch
        if records:
            count = client.write_snow(records)
            total_written += count
            logger.info(
                "Wrote %d norm records for %s, code %s",
                count, snow_type, code,
            )

    return total_written


def main():
    logging.basicConfig(level=logging.INFO)
    sl.load_environment()

    intermediate_data_path = os.getenv(
        'ieasyforecast_intermediate_data_path'
    )
    snow_data_path = os.path.join(
        intermediate_data_path,
        os.getenv('ieasyhydroforecast_OUTPUT_PATH_SNOW'),
    )
    hru_codes = os.getenv(
        'ieasyhydroforecast_HRU_SNOW_DATA'
    ).split(',')
    snow_vars = os.getenv(
        'ieasyhydroforecast_SNOW_VARS'
    ).split(',')

    # Calculate norms from historical CSVs
    norms = dg_utils.calculate_snow_norms(
        snow_data_path, hru_codes, snow_vars
    )
    logger.info(
        "Calculated %d norm entries for %d variables, %d HRUs",
        len(norms), len(snow_vars), len(hru_codes),
    )

    if norms.empty:
        logger.warning("No norms calculated, exiting")
        return

    # Write norms to API for the current year
    current_year = pd.Timestamp.today().year
    count = write_norms_to_api(norms, current_year)
    logger.info("Wrote %d total norm records to API", count)


if __name__ == '__main__':
    main()
```

#### Step 3: Extract shared `write_snow_to_api()` into `dg_utils.py`

Both `snow_data_operational.py:85-280` and `snow_data_renalysis.py:95-240`
have their own `_write_snow_to_api()` with identical logic except date
windowing. Extract into a single function that:

1. Handles all sync modes (operational, maintenance, initial)
2. Accepts a `reference_date` parameter so reanalysis can use data-relative
   dates instead of today
3. **Preserves existing norms** from the API before writing

The `sapphire_api_client` import goes in `dg_utils.py` with the standard
try/except pattern (already used by both calling scripts).

```python
# In dg_utils.py — add near the top, after existing imports:

try:
    from sapphire_api_client import (
        SapphirePreprocessingClient,
        SapphireAPIError,
    )
    SAPPHIRE_API_AVAILABLE = True
except ImportError:
    SAPPHIRE_API_AVAILABLE = False
    SapphirePreprocessingClient = None
    SapphireAPIError = Exception


def _read_existing_norms(
    client,
    snow_type: str,
    codes: list[str],
    start_date: str,
    end_date: str,
) -> dict[tuple[str, str], float]:
    """Read existing norm values from the API.

    Returns:
        Dict mapping (code, date_str) → norm value for records that
        have a non-null norm.
    """
    norm_lookup = {}
    for code in codes:
        try:
            existing = client.read_snow(
                snow_type=snow_type.upper(),
                code=str(code),
                start_date=start_date,
                end_date=end_date,
                limit=1000,
            )
            if not existing.empty and 'norm' in existing.columns:
                for _, row in existing.iterrows():
                    if pd.notna(row.get('norm')):
                        date_str = pd.to_datetime(
                            row['date']
                        ).strftime('%Y-%m-%d')
                        norm_lookup[
                            (str(row['code']), date_str)
                        ] = float(row['norm'])
        except Exception as e:
            logger.debug(
                "Could not read existing norms for %s/%s: %s",
                snow_type, code, e,
            )
    return norm_lookup


def write_snow_to_api(
    data: pd.DataFrame,
    snow_type: str,
    hru_code: str,
    mode: str | None = None,
    reference_date: pd.Timestamp | None = None,
) -> bool:
    """Write snow data to SAPPHIRE preprocessing API.

    Supports different sync modes:
    - operational (default): write yesterday's and today's data
    - maintenance: write the last 30 days of data
    - initial: write all data in the DataFrame

    Preserves existing norm values from the API — if a record already
    has a norm and the incoming data does not, the existing norm is
    kept.

    Args:
        data: DataFrame with snow data. Expected columns:
            date, code, {snow_type}, optional {snow_type}_1..14
        snow_type: Type of snow data (SWE, HS, RoF).
        hru_code: HRU code for logging context.
        mode: Sync mode override. If None, reads SAPPHIRE_SYNC_MODE
            env var, defaulting to 'operational'.
        reference_date: Date to use as "today" for date windowing.
            If None, uses pd.Timestamp.today(). Reanalysis passes
            data['date'].max() here so maintenance mode uses a
            data-relative window instead of a wall-clock window.

    Returns:
        True if successful, False otherwise.
    """
    if not SAPPHIRE_API_AVAILABLE:
        logger.warning(
            "sapphire-api-client not installed, skipping snow API "
            "write"
        )
        return False

    api_enabled = os.getenv(
        "SAPPHIRE_API_ENABLED", "true"
    ).lower() == "true"
    if not api_enabled:
        logger.info(
            "SAPPHIRE API writing disabled via "
            "SAPPHIRE_API_ENABLED=false"
        )
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.warning(
            "SAPPHIRE API at %s is not ready, skipping snow write "
            "(HRU %s, %s)", api_url, hru_code, snow_type
        )
        return False

    if data.empty:
        logger.info(
            "No snow data to write to API (%s, HRU %s)",
            snow_type, hru_code,
        )
        return False

    data = data.copy()
    data['date'] = pd.to_datetime(data['date'])

    # --- Date windowing ---
    if mode is not None:
        sync_mode = mode.lower()
    else:
        sync_mode = os.getenv(
            "SAPPHIRE_SYNC_MODE", "operational"
        ).lower()

    ref = (
        reference_date
        if reference_date is not None
        else pd.Timestamp.today().normalize()
    )

    if sync_mode == "operational":
        yesterday = ref - pd.Timedelta(days=1)
        data_to_write = data[
            (data['date'] >= yesterday) & (data['date'] <= ref)
        ]
    elif sync_mode == "maintenance":
        cutoff = ref - pd.Timedelta(days=30)
        data_to_write = data[data['date'] >= cutoff]
    elif sync_mode == "initial":
        data_to_write = data
    else:
        logger.warning(
            "Unknown sync mode '%s', defaulting to operational",
            sync_mode,
        )
        yesterday = ref - pd.Timedelta(days=1)
        data_to_write = data[
            (data['date'] >= yesterday) & (data['date'] <= ref)
        ]

    if data_to_write.empty:
        logger.info(
            "No snow data to write after %s filtering "
            "(%s, HRU %s)", sync_mode, snow_type, hru_code,
        )
        return False

    logger.info(
        "%s mode: writing %d snow records (HRU %s, %s)",
        sync_mode, len(data_to_write), hru_code, snow_type,
    )

    # --- Read existing norms so we don't overwrite them ---
    codes = [str(c) for c in data_to_write['code'].unique()]
    min_date = data_to_write['date'].min().strftime('%Y-%m-%d')
    max_date = data_to_write['date'].max().strftime('%Y-%m-%d')
    existing_norms = _read_existing_norms(
        client, snow_type, codes, min_date, max_date
    )

    # --- Build records ---
    value_columns = {}
    main_value_col = (
        snow_type if snow_type in data_to_write.columns else None
    )
    for col in data_to_write.columns:
        if col.startswith(f"{snow_type}_") and col != snow_type:
            try:
                band_num = int(col.split("_")[-1])
                value_columns[band_num] = col
            except ValueError:
                pass

    records = []
    for _, row in data_to_write.iterrows():
        date_obj = pd.to_datetime(row['date'])
        if pd.isna(date_obj):
            continue
        date_str = date_obj.strftime('%Y-%m-%d')

        # Norm: prefer local value, fall back to existing API norm
        local_norm = (
            float(row['norm'])
            if 'norm' in row and pd.notna(row.get('norm'))
            else None
        )
        api_norm = existing_norms.get((str(row['code']), date_str))
        norm = (
            round(local_norm, 3) if local_norm is not None
            else round(api_norm, 3) if api_norm is not None
            else None
        )

        record = {
            "snow_type": snow_type.upper(),
            "code": str(row['code']),
            "date": date_str,
            "value": (
                round(float(row[main_value_col]), 3)
                if main_value_col
                and pd.notna(row.get(main_value_col))
                else None
            ),
            "norm": norm,
        }

        for band_num, col_name in value_columns.items():
            if band_num <= 14:
                record[f"value{band_num}"] = (
                    round(float(row[col_name]), 3)
                    if pd.notna(row.get(col_name))
                    else None
                )

        records.append(record)

    if records:
        count = client.write_snow(records)
        logger.info(
            "SAPPHIRE API: Wrote %d snow records (%s, HRU %s)",
            count, snow_type, hru_code,
        )
        return True

    logger.info(
        "No snow records to write to API (%s, HRU %s)",
        snow_type, hru_code,
    )
    return False
```

#### Step 3b: Update calling scripts

**`snow_data_operational.py`** — remove `_write_snow_to_api()` (lines
85-280), replace calls:

```python
# Before (line 627):
written = _write_snow_to_api(df_combined, variable, hru)

# After:
written = dg_utils.write_snow_to_api(df_combined, variable, hru)
```

The operational script uses the default `reference_date=None` (today)
and default `mode` (reads `SAPPHIRE_SYNC_MODE` env var).

**`snow_data_renalysis.py`** — remove `_write_snow_to_api()` (lines
95-240), replace calls:

```python
# Before (line 557):
written = _write_snow_to_api(df_combined, variable, hru)

# After:
written = dg_utils.write_snow_to_api(
    df_combined, variable, hru,
    mode="maintenance",
    reference_date=df_combined['date'].max(),
)
```

The reanalysis script passes `mode="maintenance"` and
`reference_date=data max` so the 30-day window is relative to the
data's own timeframe (historical), not today.

#### Step 4: Create `bin/yearly_snow_norm_recalculation.sh`

Follow the pattern from `yearly_skill_metrics_recalculation.sh`:

```bash
#!/bin/bash
# Yearly Snow Norm Recalculation Script
#
# Calculates climatological norms for snow data (SWE, HS, RoF) using
# all historical reanalysis data. Run once per year in late August.
#
# Usage:
#   bash bin/yearly_snow_norm_recalculation.sh <env_file_path>
#
# Recommended crontab entry (runs August 25 at 02:00):
#   0 2 25 8 * /path/to/bin/yearly_snow_norm_recalculation.sh /path/to/.env
#
# Author: Beatrice Marti

source "$(dirname "$0")/utils/common_functions.sh"

print_banner
echo "| Running Yearly Snow Norm Recalculation"

read_configuration $1

# Validate required environment variables
if [ -z "$ieasyhydroforecast_data_root_dir" ] || \
   [ -z "$ieasyhydroforecast_env_file_path" ] || \
   [ -z "$ieasyhydroforecast_data_ref_dir" ] || \
   [ -z "$ieasyhydroforecast_container_data_ref_dir" ]; then
    echo "| Error: Required environment variables are not set."
    exit 1
fi

# Create log directory
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/snow_norm_recalc"
mkdir -p ${LOG_DIR}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

log_message "Starting Yearly Snow Norm Recalculation"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
    log_message "ERROR: Docker is not running."
    exit 1
fi

IMAGE_ID="mabesa/sapphire-pipeline:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
if ! docker image inspect $IMAGE_ID > /dev/null 2>&1; then
    log_message "Image $IMAGE_ID not found locally, pulling..."
    docker pull $IMAGE_ID
    if [ $? -ne 0 ]; then
        log_message "ERROR: Failed to pull Docker image $IMAGE_ID"
        exit 1
    fi
fi

establish_ssh_tunnel
trap cleanup EXIT

CONTAINER_NAME="snow-norm-recalc"
SERVICE_LOG="${LOG_DIR}/${CONTAINER_NAME}_${TIMESTAMP}.log"

log_message "Container: $CONTAINER_NAME"
log_message "Service log: $SERVICE_LOG"

# Remove existing container if any
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    docker rm -f $CONTAINER_NAME
fi

# Run recalculation
docker run \
    --name $CONTAINER_NAME \
    --network host \
    -e ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir} \
    -e ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path} \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    -v ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config \
    -v ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data \
    ${IMAGE_ID} \
    uv run python preprocessing_gateway/recalculate_snow_norms.py \
    2>&1 | tee "$SERVICE_LOG"

CONTAINER_EXIT_CODE=$(docker inspect $CONTAINER_NAME --format='{{.State.ExitCode}}' 2>/dev/null || echo "1")

if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
    log_message "Snow norm recalculation completed successfully"
else
    log_message "WARNING: Completed with exit code: $CONTAINER_EXIT_CODE"
fi

docker rm -f $CONTAINER_NAME 2>/dev/null

# Keep logs for 2 years (runs once/year, want history)
find $LOG_DIR -type f -mtime +730 -delete

log_message "Done"
```

#### Step 5: Write tests

Tests for `calculate_snow_norms()` in `dg_utils`:

| Test | Description |
|------|-------------|
| `test_basic_norm_calculation` | 3 years of data, verify mean is correct |
| `test_multiple_hrus_and_variables` | Multiple HRUs and snow types produce correct groupings |
| `test_missing_csv_file` | Missing file logged and skipped, no crash |
| `test_missing_variable_column` | Column not in CSV, logged and skipped |
| `test_empty_csv` | Empty DataFrame handled gracefully |
| `test_leap_year_day_366` | Day 366 present only in leap years, norm calculated correctly |
| `test_single_year_of_data` | Norm equals the single year's values |
| `test_nan_values_excluded` | NaN values don't contribute to mean |
| `test_output_format` | Columns are exactly `[snow_type, code, dayofyear, norm]` |

Tests for shared `write_snow_to_api()` in `dg_utils`:

| Test | Description |
|------|-------------|
| `test_existing_norm_preserved` | When API has norm and incoming data has `norm=None`, the existing norm is kept |
| `test_local_norm_takes_precedence` | When incoming data has a norm value, it overrides the API norm |
| `test_no_existing_record_norm_is_none` | When no API record exists, norm stays None |
| `test_api_read_failure_does_not_block_write` | If norm read fails, write still succeeds with `norm=None` |
| `test_operational_mode_filters_yesterday_today` | Default mode writes only yesterday+today |
| `test_maintenance_mode_with_reference_date` | Maintenance mode uses `reference_date` for 30-day window (reanalysis case) |
| `test_maintenance_mode_defaults_to_today` | Without `reference_date`, maintenance uses today |
| `test_initial_mode_writes_all` | Initial mode writes entire DataFrame |

Tests for `write_norms_to_api()` (yearly script):

| Test | Description |
|------|-------------|
| `test_writes_365_records_per_code_variable` | Full year written for non-leap year |
| `test_writes_366_records_for_leap_year` | Day 366 included for leap years |
| `test_preserves_existing_values` | Existing value and elevation bands not overwritten |
| `test_api_unavailable_returns_zero` | Graceful degradation when API down |

Tests for `recalculate_snow_norms.py` (integration):

| Test | Description |
|------|-------------|
| `test_end_to_end_norm_to_api` | From historical CSVs → norms calculated → API write (mocked client) |
| `test_missing_env_vars_exits_gracefully` | Missing env vars produce clear error |

Behavioral tests for calling scripts (verify shared function integration):

| Test | Description |
|------|-------------|
| `test_operational_calls_shared_write` | `snow_data_operational` uses `dg_utils.write_snow_to_api()` |
| `test_reanalysis_passes_reference_date` | `snow_data_renalysis` passes `reference_date=data.max()` |

---

## Data Flow Diagram

```
YEARLY (August)
                                       recalculate_snow_norms.py
Historical Snow CSVs                           │
{OUTPUT_PATH_SNOW}/                            │
  SWE/15013_SWE.csv  ─────────┐               │
  HS/15013_HS.csv    ─────────┼── calculate_snow_norms()   [dg_utils]
  RoF/15013_RoF.csv  ─────────┘               │
                                               │
                                        write_norms_to_api()
                                               │
                         ┌─── read existing ───┤
                         │    records (to       │
                    Preprocessing   preserve    │
                       API          values)     │
                         │                      │
                         └─── write records ────┘
                              with norm

DAILY (operational)                 PERIODIC (reanalysis)
snow_data_operational.py            snow_data_renalysis.py
         │                                   │
  Data Gateway ──> transform          Data Gateway ──> transform
         │                                   │
         └────────────┐         ┌────────────┘
                      ▼         ▼
              dg_utils.write_snow_to_api()    ← shared function
                      │
               ┌── read existing norms ──┐
               │     from API            │
          Preprocessing                  │
             API                         │
               │                         │
               └── write record ─────────┘
                   with value + preserved norm

DASHBOARD
Preprocessing API ──> db.py:get_snow_data()
                        │
                  vizualization.py:2186
                  reads norm column ──> plots norm line
```

---

## Out of Scope

- **Meteo norm refactoring**: Moving T/P norm calculation from daily to
  yearly is a separate optimization (existing daily approach works fine)
- **Per-elevation-band norms**: Only the main value (mean across bands)
  gets a norm. Per-band norms can be added later if needed.
- **config.yaml**: No new config file — uses existing .env variables for
  HRU codes and snow variables
- **Dashboard changes**: The dashboard already reads the `norm` column.
  No visualization code changes needed.
- **Cron deployment**: The shell script is created but crontab setup on
  the production server is a manual sysadmin task.

## Dependencies

- Historical snow CSVs must exist (populated by `snow_data_renalysis.py`)
- The preprocessing API snow endpoints must be operational (they are)
- Docker image `mabesa/sapphire-pipeline` must include the new script

## Acceptance Criteria

- [ ] `dg_utils.calculate_snow_norms()` computes correct day-of-year means
      from historical CSVs
- [ ] `dg_utils.write_snow_to_api()` is the single shared write function
      used by both operational and reanalysis scripts
- [ ] `dg_utils.write_snow_to_api()` preserves existing norms from the API
      (does not overwrite norm with None)
- [ ] `snow_data_operational.py` no longer has its own `_write_snow_to_api()`
- [ ] `snow_data_renalysis.py` no longer has its own `_write_snow_to_api()`
- [ ] Reanalysis script passes `reference_date=data['date'].max()` so
      maintenance mode uses a data-relative window
- [ ] `recalculate_snow_norms.py` writes norms to the API for all 365/366
      days of the current year
- [ ] Existing API records (value, elevation bands) are preserved when
      norms are written
- [ ] API records for current year have non-null `norm` after yearly run
- [ ] `bin/yearly_snow_norm_recalculation.sh` runs successfully in Docker
- [ ] Dashboard snow plots display norm reference line
- [ ] All existing tests pass (`SAPPHIRE_TEST_ENV=True bash run_tests.sh`)
- [ ] New tests cover norm calculation, shared write function, and norm
      preservation
- [ ] No sensitive data (station codes, API keys) in committed files

---

## References

- Meteo norm reference: `apps/preprocessing_gateway/extend_era5_reanalysis.py:500-558`
- Snow data transform: `apps/preprocessing_gateway/dg_utils.py:213-284`
- Snow operational flow: `apps/preprocessing_gateway/snow_data_operational.py:517-638`
- Snow operational API write (to be replaced): `apps/preprocessing_gateway/snow_data_operational.py:85-280`
- Snow reanalysis flow: `apps/preprocessing_gateway/snow_data_renalysis.py:460-566`
- Snow reanalysis API write (to be replaced): `apps/preprocessing_gateway/snow_data_renalysis.py:95-240`
- Snow DB model: `sapphire/services/preprocessing/app/models.py:117-148`
- Snow CRUD upsert: `sapphire/services/preprocessing/app/crud.py:205-267`
- Dashboard snow viz: `apps/forecast_dashboard/src/vizualization.py:2170-2300`
- Yearly script pattern: `bin/yearly_skill_metrics_recalculation.sh`
- Shell utilities: `bin/utils/common_functions.sh`
