# SAPPHIRE API Integration Plan

## Overview

Adapt all SAPPHIRE modules to write data to the new PostgreSQL databases via the SAPPHIRE API services, replacing or supplementing the current CSV file output.

## Background

The `sapphire/services/` folder contains new FastAPI microservices:
- **preprocessing-api** (port 8002): Runoff, hydrograph, meteo, snow data
- **postprocessing-api** (port 8003): Forecasts, LR forecasts, skill metrics
- **api-gateway** (port 8000): Routes requests to appropriate services
- **auth-api** (port 8005): Authentication service (future)
- **user-api** (port 8004): User management (future)

## Design Decisions

### Transition Strategy
- **Write BOTH CSV and API** during transition period
- Maintain CSV output until API writing is fully tested and verified
- Phased rollout: runoff → hydrograph → pentad → decad

### API Client Location
- **Separate repository**: `hydrosolutions/sapphire-api-client`
- Installable via: `pip install git+https://github.com/hydrosolutions/sapphire-api-client.git`
- MIT License (permissive, easy for external orgs to adopt)

### Error Handling
- **Strict failure handling**: Retry with exponential backoff, then FAIL
- No silent fallbacks - robustness is a priority
- Clear error messages for auth errors (401, 403)

### Authentication (Future-Proofed)
- Client supports optional `auth_token` parameter (Bearer token)
- Server-side access control will define:
  - **Hydromet operators**: Full read/write access
  - **External organizations**: Read-only access to specific sites/horizons
- Auth service implementation deferred until API is exposed externally

## Implementation Phases

### Phase 1: API Client Package (COMPLETE)
**Location**: `hydrosolutions/sapphire-api-client` (copied from `_temp_sapphire_api_client/`)

Files created:
- `pyproject.toml` - Package config (requests, pandas, tenacity deps)
- `README.md` - Documentation with usage examples
- `LICENSE` - Update to MIT
- `.gitignore`
- `src/sapphire_api_client/__init__.py`
- `src/sapphire_api_client/client.py` - Base client with retry logic, auth support
- `src/sapphire_api_client/preprocessing.py` - Runoff, hydrograph, meteo, snow
- `src/sapphire_api_client/postprocessing.py` - Forecasts, LR forecasts, skill metrics
- `tests/test_client.py` - Base client tests
- `tests/test_preprocessing.py` - Preprocessing client tests
- `tests/test_postprocessing.py` - Postprocessing client tests

### Phase 2: Copy to New Repository (COMPLETE)
1. ~~Copy contents of `_temp_sapphire_api_client/` to `hydrosolutions/sapphire-api-client`~~
2. Update LICENSE to MIT
3. Initial commit and push
4. Verify tests pass: `pip install -e ".[dev]" && pytest`

### Phase 2.5: Test Existing Data Migrators (COMPLETE)
**Purpose**: Preserve status quo behavior before integrating new client.

Write tests for `sapphire/services/preprocessing/app/data_migrator.py`:

**Test file**: `sapphire/services/preprocessing/tests/test_data_migrator.py`

**Classes to test**:

1. **RunoffDataMigrator**
   - `prepare_day_data()` - transforms daily CSV to API records
   - `prepare_pentad_data()` - transforms pentad CSV to API records
   - `prepare_decade_data()` - transforms decade CSV to API records

2. **HydrographDataMigrator**
   - `prepare_day_data()` - handles percentile columns (5%, 25%, etc.)
   - `prepare_pentad_data()` - handles different column naming (q05, q25, etc.)
   - `prepare_decade_data()` - handles decade-specific calculations

3. **MeteoDataMigrator**
   - `prepare_day_data()` - temperature and precipitation
   - `load_and_merge_data()` - merges norm values

4. **SnowDataMigrator**
   - `prepare_day_data()` - zone values (value1-value14)
   - `load_and_merge_data()` - merges norm values

5. **DataMigrator base class**
   - `send_batch()` - mock API calls, verify payload structure
   - `migrate_csv()` - end-to-end with mocked API

**Test approach**:
- Create minimal sample DataFrames mimicking real CSV structure
- Verify output record structure matches API schema
- Verify null/NaN handling
- Mock `requests.Session` for API tests

**Results - Preprocessing**: 39 tests created and passing:
- `TestMigrationStats` (1 test)
- `TestRunoffDataMigrator` (9 tests)
- `TestHydrographDataMigrator` (5 tests)
- `TestMeteoDataMigrator` (8 tests)
- `TestSnowDataMigrator` (9 tests)
- `TestDataMigratorBase` (4 tests)
- `TestRecordSchemaCompliance` (4 tests)

### Phase 2.5b: Test Postprocessing Data Migrators (COMPLETE)
**Test file**: `sapphire/services/postprocessing/tests/test_data_migrator.py`

**Classes tested**:
- CombinedForecastDataMigrator - pentad/decade forecasts, LR filtering
- LRForecastDataMigrator - linear regression forecasts
- SkillMetricDataMigrator - skill metrics with horizon-to-date mapping

**Results - Postprocessing**: 23 tests created and passing:
- `TestMigrationStats` (1 test)
- `TestCombinedForecastDataMigrator` (7 tests)
- `TestLRForecastDataMigrator` (4 tests)
- `TestSkillMetricDataMigrator` (5 tests)
- `TestDataMigratorBase` (4 tests)
- `TestRecordSchemaCompliance` (3 tests)

### Phase 3: Integrate into preprocessing_runoff (IN PROGRESS)

#### 3.1 Daily Runoff API Integration (COMPLETE)
- [x] Add `sapphire-api-client` as dependency
- [x] Update `write_daily_time_series_data_to_csv()` to also write to API
- [x] Add configuration for API URL (env var `SAPPHIRE_API_URL`)
- [x] Add configuration to disable API writes (env var `SAPPHIRE_API_ENABLED`)
- [x] Test with Docker services running
- [x] **Implement incremental sync** - only new data sent to API (not all 500k+ records)

**Incremental Sync Implementation**:
- `get_runoff_data_for_sites_HF()` now returns `RunoffDataResult(full_data, new_data)`
- `write_daily_time_series_data_to_csv()` accepts optional `api_data` parameter
- Full dataset written to CSV, only new records sent to API
- Dramatically improves performance for daily operational runs

#### 3.2 Daily Hydrograph API Integration (COMPLETE)
- [x] Add `_write_hydrograph_to_api()` function with column mapping (5% → q05, etc.)
- [x] Update `write_daily_hydrograph_data_to_csv()` with `api_data` parameter
- [x] Update caller to filter hydrograph to today's date for API sync

**Hydrograph API Sync Strategy**:
- Full hydrograph (365/366 days × stations) written to CSV
- Only today's hydrograph rows sent to API (one row per station)
- Statistics recalculated from full historical data but only today is synced

#### 3.3 Operating Modes Integration (PENDING)
- [ ] Add `SAPPHIRE_SYNC_MODE` env var (operational/maintenance/initial)
- [ ] Operational mode: sync only today's data
- [ ] Maintenance mode: sync last N days (configurable)
- [ ] Initial mode: sync all historical data (for first-time setup)

### Phase 4: Integrate into Other Modules (IN PROGRESS)
Order of integration:
1. **preprocessing_gateway** - Similar pattern to preprocessing_runoff
2. **linear_regression** - Pentad/decad aggregations, forecasts
3. **machine_learning** - ML forecasts
4. **postprocessing_forecasts** - Final forecast output, skill metrics

#### 4.2 Linear Regression LR Forecasts (COMPLETE)
- [x] Add `_write_lr_forecast_to_api()` helper in forecast_library.py
- [x] Modify `write_linreg_pentad_forecast_data()` with api_data parameter
- [x] Modify `write_linreg_decad_forecast_data()` with api_data parameter
- [x] Add sapphire-api-client dependency to linear_regression/requirements.txt
- [x] Add tests for API integration

**Column Mapping (LR Forecasts):**
| CSV Column | API Field |
|------------|-----------|
| code | code (str) |
| date | date (YYYY-MM-DD) |
| pentad_in_month/decad_in_month | horizon_value |
| pentad_in_year/decad_in_year | horizon_in_year |
| discharge_avg | discharge_avg |
| predictor | predictor |
| slope | slope |
| intercept | intercept |
| forecasted_discharge | forecasted_discharge |
| q_mean | q_mean |
| q_std_sigma | q_std_sigma |
| delta | delta |
| rsquared | rsquared |
| (constant) | horizon_type: "pentad"/"decade" |

**Incremental Sync Strategy:**
- The existing `last_line` variable contains newest forecast per station
- This is the "new data" sent to API by default
- Caller can override with `api_data` parameter for custom sync logic
- Full combined data still written to CSV for backward compatibility
- Server deduplicates by `(horizon_type, code, date)` - matches CSV behavior

**Files Modified:**
- `apps/iEasyHydroForecast/forecast_library.py` - Added import, helper function, modified write functions
- `apps/linear_regression/requirements.txt` - Added sapphire-api-client dependency
- `apps/linear_regression/test/test_forecast_library_api.py` - New test file for API integration

#### 4.2b Linear Regression Hydrograph (COMPLETE)
- [x] Add `_write_hydrograph_to_api()` helper function in forecast_library.py
- [x] Update `write_pentad_hydrograph_data()` to write to API before CSV
- [x] Update `write_decad_hydrograph_data()` to write to API before CSV

**Hydrograph API Integration:**
- Uses `SapphirePreprocessingClient.write_hydrograph()` method
- Writes to `/api/preprocessing/hydrograph/` endpoint
- Supports both pentad and decade horizon types
- Column mapping handles year columns dynamically (e.g., "2025", "2026" → previous, current)

**Column Mapping (Hydrograph):**
| CSV Column | API Field |
|------------|-----------|
| code | code (str) |
| date | date (YYYY-MM-DD) |
| pentad/decad | horizon_value |
| pentad_in_year/decad_in_year | horizon_in_year |
| day_of_year | day_of_year |
| mean, min, max | mean, min, max |
| q05, q25, q75, q95 | q05, q25, q75, q95 |
| norm | norm |
| <previous_year> | previous |
| <current_year> | current |

#### 4.2c Linear Regression Discharge (COMPLETE)
- [x] Add `_write_runoff_to_api()` helper function in forecast_library.py
- [x] Update `write_pentad_time_series_data()` to write to API before CSV
- [x] Update `write_decad_time_series_data()` to write to API before CSV
- [x] Implement sync modes: operational (latest only), maintenance (30 days), initial (all)

**Runoff API Integration:**
- Uses `SapphirePreprocessingClient.write_runoff()` method
- Writes to `/api/preprocessing/runoff/` endpoint
- Supports both pentad and decade horizon types

**Sync Modes (via SAPPHIRE_SYNC_MODE env var):**
| Mode | Behavior | Use Case |
|------|----------|----------|
| operational (default) | Write only latest date's data | Daily forecast runs |
| maintenance | Write last 30 days | Backfill after outages |
| initial | Write all data | First-time setup |

**Column Mapping (Runoff):**
| CSV Column | API Field |
|------------|-----------|
| code | code (str) |
| date | date (YYYY-MM-DD) |
| pentad/decad_in_month | horizon_value |
| pentad_in_year/decad_in_year | horizon_in_year |
| discharge_avg | discharge |
| predictor | predictor |

#### 4.2d Linear Regression Daily Discharge Read (COMPLETE)
- [x] Add `SapphirePreprocessingClient` import to forecast_library.py
- [x] Add `_read_daily_discharge_from_api()` function with pagination support
- [x] Add `read_daily_discharge_data()` unified function (API default, CSV fallback via env var)
- [x] Update `get_pentadal_and_decadal_data()` to use new unified function
- [x] Add tests for read functionality

**Read Integration Design:**
- **Default behavior**: Read from SAPPHIRE API (`SAPPHIRE_API_ENABLED=true`)
- **Fail fast**: If API unavailable, raises `SapphireAPIError` immediately (no silent CSV fallback)
- **Local dev mode**: Set `SAPPHIRE_API_ENABLED=false` to use CSV files
- **Pagination**: Handles large datasets with configurable page size (default 10,000 records)
- **Filtering**: Supports optional `site_codes`, `start_date`, `end_date` parameters

**Functions Added:**
| Function | Purpose |
|----------|---------|
| `_read_daily_discharge_from_api()` | Low-level API read with pagination and health check |
| `read_daily_discharge_data()` | Unified entry point (API or CSV based on env var) |

**API Endpoint Used:**
- `GET /runoff/?horizon=day` - Returns daily discharge records

**Bug Fix - crud.py Enum Handling:**
The preprocessing API's `get_runoff()`, `get_hydrograph()`, `get_meteo()`, and `get_snow()` functions were fixed to properly convert query parameters to enum types. Without this fix, queries like `horizon=day` wouldn't match records stored as `HorizonType.DAY` in the database.

```python
# Before (broken - string comparison against enum)
if horizon:
    query = query.filter(Runoff.horizon_type == horizon)

# After (fixed - convert to enum)
if horizon:
    horizon_enum = HorizonType(horizon)
    query = query.filter(Runoff.horizon_type == horizon_enum)
```

**DataFrame Format:**
| Column | Type | Description |
|--------|------|-------------|
| code | str | Station code |
| date | datetime | Measurement date |
| discharge | float | Discharge in m³/s |

**Migration Path:**
1. Deploy with `SAPPHIRE_API_ENABLED=false` (current CSV behavior)
2. Verify API has data: `SELECT COUNT(*) FROM runoffs WHERE horizon_type='day';`
3. Enable `SAPPHIRE_API_ENABLED=true` once API is verified
4. Rollback: Set `SAPPHIRE_API_ENABLED=false` if issues arise

#### 4.2e Runtime Consistency Checking (COMPLETE)
- [x] Add `_check_dataframe_consistency()` helper function with strict/lenient modes
- [x] Add `_verify_write_consistency()` helper function
- [x] Add consistency check to `read_daily_discharge_data()` for reads
- [x] Add consistency check to `write_linreg_pentad_forecast_data()` for writes
- [x] Add consistency check to `write_linreg_decad_forecast_data()` for writes
- [x] Add unit tests for consistency checking
- [x] Add lenient mode (default) to handle historical data differences from outlier filtering

**Consistency Check Feature:**

Enable runtime verification that API and CSV data match by setting:
```bash
SAPPHIRE_CONSISTENCY_CHECK=true
```

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SAPPHIRE_CONSISTENCY_CHECK` | `false` | Enable data consistency verification |
| `SAPPHIRE_CONSISTENCY_STRICT` | `false` | Strict mode: fail on value/NaN mismatches. Lenient mode (default): log as warnings |

**Lenient vs Strict Mode:**
- **Lenient (default)**: NaN and value mismatches are logged as warnings but don't cause failure. This is expected when historical data has different outlier filtering between API and CSV sources.
- **Strict**: Any mismatch causes failure. Use for validation after fresh migrations.

**For Reads:**
- Reads from BOTH API and CSV
- Compares key columns (`code`, `date`, `discharge`)
- Raises `ValueError` if data doesn't match
- Returns API data if consistent

**For Writes:**
- After writing to both destinations
- Verifies CSV contains data matching what was sent to API
- Compares forecast columns (`discharge_avg`, `forecasted_discharge`, `rsquared`, etc.)
- Raises `ValueError` with details if inconsistencies found

**Use Cases:**
- Post-migration validation: Verify API was populated correctly
- Debugging: Identify data transformation issues
- CI/CD: Add to integration tests

### Phase 5: Remove CSV Fallback (FUTURE)
Once all modules are verified:
1. Make API writing the primary method
2. Keep CSV as optional backup (configurable)
3. Update documentation

## API Endpoints Reference

### Preprocessing Service (via gateway at :8000)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/runoff/` | POST | Bulk create/update runoff records |
| `/runoff/` | GET | Read runoff with filters |
| `/hydrograph/` | POST | Bulk create/update hydrograph records |
| `/hydrograph/` | GET | Read hydrograph with filters |
| `/meteo/` | POST | Bulk create/update meteo records |
| `/meteo/` | GET | Read meteo with filters |
| `/snow/` | POST | Bulk create/update snow records |
| `/snow/` | GET | Read snow with filters |

### Postprocessing Service (via gateway at :8000)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/forecasts/` | POST | Bulk create/update forecasts |
| `/forecasts/` | GET | Read forecasts with filters |
| `/lr-forecasts/` | POST | Bulk create/update LR forecasts |
| `/lr-forecasts/` | GET | Read LR forecasts with filters |
| `/skill-metrics/` | POST | Bulk create/update skill metrics |
| `/skill-metrics/` | GET | Read skill metrics with filters |

## Client Usage Example

```python
from sapphire_api_client import SapphirePreprocessingClient

# Initialize (no auth for internal use)
client = SapphirePreprocessingClient(base_url="http://localhost:8000")

# Prepare records from DataFrame
records = SapphirePreprocessingClient.prepare_runoff_records(
    df=daily_discharge_df,
    horizon_type="day",
    code=station_code,
)

# Write to API (with retry and batching)
count = client.write_runoff(records)
print(f"Wrote {count} runoff records")
```

## Related Files

- `sapphire/services/preprocessing/app/models.py` - SQLAlchemy models
- `sapphire/services/preprocessing/app/schemas.py` - Pydantic schemas
- `sapphire/services/preprocessing/app/main.py` - FastAPI endpoints
- `sapphire/docker-compose.yml` - Service definitions
- `apps/forecast_dashboard/src/db.py` - Reference for API reading pattern
- `apps/iEasyHydroForecast/forecast_library.py` - LR forecast API integration (Phase 4.2), Daily discharge read (Phase 4.2d)
- `apps/linear_regression/requirements.txt` - Dependencies including sapphire-api-client
- `apps/linear_regression/test/test_forecast_library_api.py` - API integration tests (write and read)

## Design Decisions (Resolved)

### Health Check Before Writes
**Decision**: Yes, perform health check before write operations.
- Robustness and correctness are first priority, speed is second
- Health check cost is negligible compared to data integrity benefits
- Fail fast if API is unavailable rather than attempting writes

### Partial Batch Failure Strategy
**Decision**: Retry failed batch, then hard fail the entire process.
- On batch failure: retry with exponential backoff (already implemented in client)
- If retry exhausted: raise `SapphireAPIError` and halt
- No partial success - either all batches succeed or process fails
- This ensures data consistency (no partial writes to investigate)

### Write Order Strategy
**Decision**: API first, CSV as backup.
1. Attempt API write first
2. If API write succeeds → write CSV (for transition period redundancy)
3. If API write fails → write CSV as fallback, then raise error
4. This ensures CSV always has the data even if API fails
5. Future: Once API is proven reliable, CSV becomes optional

```python
# Pseudocode for write strategy
def write_data(df, station_code, ...):
    client = SapphirePreprocessingClient()

    # Health check first
    if not client.readiness_check():
        logger.error("API not ready, falling back to CSV only")
        write_to_csv(df, ...)
        raise SapphireAPIError("API not ready")

    try:
        # API first
        records = client.prepare_runoff_records(df, ...)
        client.write_runoff(records)
        logger.info("API write successful")
    except SapphireAPIError as e:
        logger.error(f"API write failed: {e}")
        # CSV as backup
        write_to_csv(df, ...)
        raise  # Re-raise to fail the process

    # Both succeeded - write CSV for redundancy (transition period)
    write_to_csv(df, ...)
```

---
#### 4.2f API Client Endpoint Naming Fix (COMPLETE)
- [x] Fixed endpoint naming in `sapphire-api-client` to use singular form (`/lr-forecast/` instead of `/lr-forecasts/`)
- [x] All endpoints now match the SAPPHIRE API convention (singular form)

**Endpoint Naming Convention:**
| Service | Endpoint | Form |
|---------|----------|------|
| Preprocessing | `/runoff/` | singular |
| Preprocessing | `/hydrograph/` | singular |
| Preprocessing | `/meteo/` | singular |
| Preprocessing | `/snow/` | singular |
| Postprocessing | `/forecast/` | singular |
| Postprocessing | `/lr-forecast/` | singular |
| Postprocessing | `/skill-metric/` | singular |

#### 4.1 Preprocessing Gateway Snow/Meteo API Integration (COMPLETE)
- [x] Add `sapphire-api-client` import to snow_data_operational.py
- [x] Add `_write_snow_to_api()` function (writes latest date only - operational behavior)
- [x] Update `get_snow_data_operational()` to call API write after CSV
- [x] Add `sapphire-api-client` import to extend_era5_reanalysis.py
- [x] Add `_write_meteo_to_api()` function (writes all data passed - maintenance behavior)
- [x] Update `main()` in extend_era5_reanalysis.py to call API write after CSV
- [x] Add tests for snow and meteo API integration (17 tests)

**Module Behavior:**
The preprocessing_gateway modules have fixed behavior based on their purpose:
- `snow_data_operational.py` → **Operational mode**: Writes only the latest date's data
- `extend_era5_reanalysis.py` → **Maintenance mode**: Writes all data passed (extends historical data)

Note: These modules do NOT use the `SAPPHIRE_SYNC_MODE` environment variable. The behavior is determined by the module's purpose.

**Snow Data API Integration:**
- Uses `SapphirePreprocessingClient.write_snow()` method
- Writes to `/api/preprocessing/snow/` endpoint
- Supports SWE, HS, RoF snow types
- Handles elevation band values (value1-value14)

**Snow Column Mapping:**
| CSV Column | API Field |
|------------|-----------|
| date | date (YYYY-MM-DD) |
| code | code (str) |
| {snow_type} (e.g., SWE) | value |
| {snow_type}_1, _2, ... | value1, value2, ... |
| norm (if present) | norm |
| (constant) | snow_type: "SWE"/"HS"/"ROF" |

**Meteo Data API Integration:**
- Uses `SapphirePreprocessingClient.write_meteo()` method
- Writes to `/api/preprocessing/meteo/` endpoint
- Supports T (temperature) and P (precipitation)
- Handles norm values and day_of_year

**Meteo Column Mapping:**
| CSV Column | API Field |
|------------|-----------|
| date | date (YYYY-MM-DD) |
| code | code (str) |
| T or P | value |
| T_norm or P_norm | norm |
| dayofyear | day_of_year |
| (constant) | meteo_type: "T"/"P" |

**Consistency Checking (Temporary):**
Enable runtime verification that API and CSV data match by setting:
```bash
SAPPHIRE_CONSISTENCY_CHECK=true
```

When enabled:
- After writing to API, reads back from API and compares with CSV data
- Logs warnings for row count mismatches, value mismatches, missing rows
- Does not fail on mismatches (logs warnings only)
- Will be removed once API is verified working correctly

**Files Modified:**
- `apps/preprocessing_gateway/snow_data_operational.py` - Added imports, API write function (latest only), consistency check, integration
- `apps/preprocessing_gateway/extend_era5_reanalysis.py` - Added imports, API write function (all data), consistency check, integration
- `apps/preprocessing_gateway/test/test_api_integration.py` - New test file (23 tests)

*Last updated: 2026-01-20 (added consistency checking to Phase 4.1)*
