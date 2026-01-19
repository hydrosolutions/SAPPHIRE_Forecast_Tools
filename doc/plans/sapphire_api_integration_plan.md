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

### Phase 4: Integrate into Other Modules (PENDING)
Order of integration:
1. **preprocessing_gateway** - Similar pattern to preprocessing_runoff
2. **linear_regression** - Pentad/decad aggregations, forecasts
3. **machine_learning** - ML forecasts
4. **postprocessing_forecasts** - Final forecast output, skill metrics

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
*Last updated: 2026-01-19*
