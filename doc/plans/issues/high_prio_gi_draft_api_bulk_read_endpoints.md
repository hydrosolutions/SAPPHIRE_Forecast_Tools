> **NOTE**: This plan was auto-extracted from `bulk_read_endpoints_instructions.md`.
> It needs review and refining before it can be implemented.

# Add bulk-read endpoints to SAPPHIRE API services

**Status**: Draft
**Module**: infra
**Priority**: High
**Labels**: `api`, `performance`, `long-term-forecasting`

---

## Summary

Add `/bulk/` endpoints to the preprocessing and postprocessing FastAPI services that return all matching records in a single response (no pagination). Then add corresponding methods to the `sapphire-api-client`. This lets the `long_term_forecasting` module replace its direct SQL with API calls.

## Context

The `long_term_forecasting` module currently uses direct SQL queries (via `data_interface.py`) to fetch large training datasets. This bypasses API validation, exposes DB credentials, and creates a parallel code path. The detailed specification is in `doc/plans/bulk_read_endpoints_instructions.md`.

## Problem

- `long_term_forecasting` uses direct SQL instead of the API client
- Paginated API reads are too slow for large training datasets (millions of records)
- No bulk-read capability exists in the API services or client

## Implementation Steps

### Step 1: Add safeguards (Part 0 of spec)

Every bulk endpoint must:
- Require at least one filter (reject unfiltered requests with HTTP 400)
- Implement row count safety limit (default 500,000, HTTP 413 if exceeded)
- Set statement timeout (30 seconds default)

### Step 2: Add preprocessing bulk endpoints

**Service**: `sapphire/services/preprocessing/`

| Endpoint | Method | Returns |
|----------|--------|---------|
| `GET /bulk/runoff/` | All runoff matching filters | Single JSON array |
| `GET /bulk/meteo/` | All meteo matching filters | Single JSON array |
| `GET /bulk/snow/` | All snow matching filters | Single JSON array |

**Files to modify**:
- `sapphire/services/preprocessing/app/main.py` (add routes)
- `sapphire/services/preprocessing/app/crud.py` (add `get_all_*` functions)

### Step 3: Add postprocessing bulk endpoint

**Service**: `sapphire/services/postprocessing/`

| Endpoint | Method | Returns |
|----------|--------|---------|
| `GET /bulk/long-forecast/` | All long-term forecasts matching filters | Single JSON array |

**Files to modify**:
- `sapphire/services/postprocessing/app/main.py` (add route)
- `sapphire/services/postprocessing/app/crud.py` (add `get_all_long_forecasts`)

### Step 4: Add bulk_read methods to sapphire-api-client

**Repository**: `hydrosolutions/sapphire-api-client`

Add methods:
- `SapphirePreprocessingClient.bulk_read_runoff()`
- `SapphirePreprocessingClient.bulk_read_meteo()`
- `SapphirePreprocessingClient.bulk_read_snow()`
- `SapphirePostprocessingClient.bulk_read_long_forecasts()`

### Step 5: Migrate long_term_forecasting to use API client

**File to modify**: `apps/long_term_forecasting/data_interface.py`

Replace direct SQL queries with API client bulk reads.

### Step 6: Tests

- Safeguard tests (no-filter rejection, row limit, timeout)
- Unit tests per endpoint
- Integration test: long_term_forecasting reads from API instead of SQL

## Acceptance Criteria

- [ ] All bulk endpoints reject unfiltered requests (HTTP 400)
- [ ] Row count safety limit returns HTTP 413 when exceeded
- [ ] Statement timeout prevents long-running queries
- [ ] `long_term_forecasting` works with API client instead of direct SQL
- [ ] No direct SQL imports remain in `long_term_forecasting`
- [ ] All existing tests pass

## Detailed Specification

See `doc/plans/bulk_read_endpoints_instructions.md` for complete implementation details including code examples.

## Related

- `doc/plans/bulk_read_endpoints_instructions.md` (full spec)
- `doc/plans/sapphire_api_integration_plan.md` (overall API integration)
