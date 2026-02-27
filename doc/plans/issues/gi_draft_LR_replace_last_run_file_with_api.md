# Replace `last_successful_run` File with API Query

**Module:** linear_regression, iEasyHydroForecast (setup_library)
**Priority:** Medium
**Depends on:** API write working correctly for linear_regression

## Problem

The linear regression module tracks its last successful run date via a text file
(`linreg_last_successful_run_PENTAD.txt` / `_DECAD.txt`). This causes friction:

1. **Hindcast → operational conflict:** After running hindcast (which doesn't
   update the file), the file may already contain today's date from a previous
   operational run, causing the next operational run to exit with "already
   produced." The only recovery is manually editing the file or using
   `rerun_forecast.py`.

2. **File drift:** The file can desync from what's actually in the database.
   A failed API write still updates the file, or a successful API write after
   a crash leaves the file stale.

3. **Deployment friction:** The file must exist on disk with the right path
   configured via two environment variables (`ieasyforecast_intermediate_data_path`
   + `ieasyforecast_last_successful_run_file`). On fresh deployments or new
   servers, this is easy to misconfigure.

## Solution

Replace file reads with a single API query: "what is the most recent LR
forecast date for this horizon?" The database already has this information —
every LR forecast written via the API has a `date` column.

### Key insight

`store_last_successful_run_date()` becomes unnecessary. The act of writing
forecasts to the API **is** the store. The read side just queries for
`max(date)` from existing LR forecasts.

## Files to Modify

| File | Change |
|------|--------|
| `apps/iEasyHydroForecast/setup_library.py` | Replace `get_last_run_date()` internals, deprecate `store_last_successful_run_date()` |
| `apps/linear_regression/linear_regression.py` | Remove `store_last_successful_run_date()` call (line ~843) |
| `apps/reset_forecast_run_date/rerun_forecast.py` | Update to work without the file |

## Design

### Phase 1: API-first `get_last_run_date()` with file fallback

Replace the internals of `get_last_run_date()`. Keep the same signature and
return type so callers don't change.

```python
def get_last_run_date(prediction_mode='BOTH'):
    """Read the most recent LR forecast date from the API.

    Falls back to the file if the API is unavailable, then to yesterday.
    """
    horizon = _prediction_mode_to_horizon(prediction_mode)

    # Try API first
    if SAPPHIRE_API_AVAILABLE:
        try:
            client = _get_postprocessing_client()
            if client and client.readiness_check():
                df = client.read_lr_forecasts(
                    horizon=horizon, limit=1,
                    # Sort by date desc is the API default; limit=1 gives max
                )
                if df is not None and not df.empty:
                    max_date = pd.to_datetime(df["date"]).max().date()
                    logger.debug(f"Last LR forecast date from API: {max_date}")
                    return max_date
        except Exception as exc:
            logger.warning(f"API query for last run date failed: {exc}")

    # Fallback: read from file (existing logic)
    return _get_last_run_date_from_file(prediction_mode)
```

A helper `_prediction_mode_to_horizon()` maps `PENTAD` → `"pentad"`,
`DECAD` → `"decade"`, `BOTH` → `"pentad"` (default).

### Phase 2: Remove `store_last_successful_run_date()` call

In `linear_regression.py` (line ~843), the call is only made in non-hindcast
mode. Since the API write of forecast data already happened by this point,
the store call is redundant when the API is available.

**Keep the file write as fallback** for the transition period (API might be
disabled). Guard it:

```python
# In linear_regression.py, after forecast loop body:
if not args.hindcast:
    if not SAPPHIRE_API_AVAILABLE or not api_enabled:
        # File fallback only when API is not in use
        sl.store_last_successful_run_date(
            current_day, prediction_mode=prediction_mode
        )
```

### Phase 3: Update `rerun_forecast.py`

Current behavior: reads the file, calculates the previous forecast date,
writes it back. This triggers a re-run on the next operational invocation.

New behavior with API-first reads:
- **Option A (simple):** Add a `--force` flag to `linear_regression.py` that
  skips the "already produced" check. `rerun_forecast.py` becomes a thin
  wrapper that calls `linear_regression.py --force`.
- **Option B (file compat):** Keep `rerun_forecast.py` as-is for the
  transition period. It still works because `get_last_run_date()` falls back
  to the file.

Recommend **Option A** for simplicity, but implement in a later PR.

### Phase 4: Remove file and env vars (future cleanup)

Once API-first is proven stable in production:
- Remove `store_last_successful_run_date()` entirely
- Remove `_get_last_run_date_from_file()` fallback
- Remove env vars `ieasyforecast_last_successful_run_file`
- Remove the `linreg_last_successful_run*.txt` files
- Simplify or remove `rerun_forecast.py`

## Affected Environment Variables

- `ieasyforecast_last_successful_run_file` — kept for fallback in Phase 1–3, removed in Phase 4
- `ieasyforecast_intermediate_data_path` — used by other things too, not removed

## Edge Cases

| Case | Current behavior | New behavior |
|------|-----------------|--------------|
| First-ever run (no data) | File not found → default yesterday | API returns empty → file fallback → default yesterday |
| API down | N/A (always reads file) | API fails → file fallback (same result) |
| Hindcast then operational | File has today → "already produced" | API has today's forecast → same, BUT `--force` flag provides escape hatch |
| Run twice same day | File has today → "already produced" | API has today → "already produced" (same) |
| Midnight crossing | File written before midnight, read after | API date is from forecast write time (same semantics) |

## Testing

### Unit tests (setup_library)
- `test_get_last_run_date_api_success` — mock API returns df with date, verify return
- `test_get_last_run_date_api_empty` — mock API returns empty df, verify file fallback
- `test_get_last_run_date_api_unavailable` — mock API raises, verify file fallback
- `test_get_last_run_date_no_api_no_file` — both fail, verify returns yesterday

### Integration tests (linear_regression)
- `test_operational_skips_store_when_api_enabled` — verify no file write
- `test_operational_writes_file_when_api_disabled` — verify file write as fallback
- Existing hindcast tests remain unchanged (hindcast never writes file or API state)

### Edge case tests
- `test_first_run_no_data_anywhere` — returns yesterday
- `test_prediction_mode_to_horizon_mapping` — PENTAD/DECAD/BOTH all map correctly

## Verification

1. Run `linear_regression` in operational mode → verify it queries API for last date
2. Run twice → second run says "already produced" (same as today)
3. Run with `SAPPHIRE_API_ENABLED=false` → falls back to file behavior
4. Run hindcast then operational → operational correctly sees today's forecasts
5. Full test suite passes: `SAPPHIRE_TEST_ENV=True bash run_tests.sh`

## Out of Scope

- Adding a dedicated "run metadata" API endpoint (overkill for this)
- Changing how other modules track state (only linear_regression uses this)
- The `--force` flag on `linear_regression.py` (separate small PR)
