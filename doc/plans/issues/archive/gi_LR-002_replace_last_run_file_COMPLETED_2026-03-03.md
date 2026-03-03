# Replace `last_successful_run` File with API Query

**Module:** linear_regression, iEasyHydroForecast (setup_library)
**Priority:** Medium
**Status:** Phases 1-2 superseded by "just run for today" simplification

## Problem

The linear regression module tracked its last successful run date via a text file
(`linreg_last_successful_run_PENTAD.txt` / `_DECAD.txt`). This caused friction:

1. **Hindcast -> operational conflict:** After running hindcast (which doesn't
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

## Solution: "Just Run for Today"

Instead of replacing the file read with an API query (original plan), the
simpler approach is to **remove the concept entirely**. The operational
catch-up loop was redundant with hindcast mode:

- **Operational mode** now runs for today only (`forecast_date = date.today()`).
  No file read, no "already produced" guard. Upsert semantics make re-runs safe.
- **Hindcast mode** (`--hindcast`) handles catch-up for missed days. It already
  auto-detects missing dates and backfills them.

This matches how all other forecast modules (machine_learning,
postprocessing, long_term_forecasting) work.

### What was done

- `linear_regression.py` no longer calls `define_run_dates()` or
  `store_last_successful_run_date()` in operational mode.
- The operational loop runs exactly once (for today).
- The in-loop `define_run_dates()` re-call was removed.
- Exit code simplified to `sys.exit(0)` (errors propagate as exceptions).

### What remains (follow-up cleanup PR)

| Task | Description |
|------|-------------|
| Remove dead code in setup_library | `store_last_successful_run_date()`, `get_last_run_date()`, `define_run_dates()` are no longer called by LR. Check if any other module still uses them; if not, remove. |
| Remove env vars | `ieasyforecast_last_successful_run_file` can be removed from `.env` files once the functions are removed. |
| Remove state files | Delete `linreg_last_successful_run_PENTAD.txt` / `_DECAD.txt` from deployment servers. |
| Simplify `rerun_forecast.py` | Module is no longer needed for LR (just re-run `linear_regression.py`). Check if other modules use it; if not, deprecate. |

## Original Design (superseded)

The original plan proposed replacing file reads with API queries:

- Phase 1: API-first `get_last_run_date()` with file fallback
- Phase 2: Remove `store_last_successful_run_date()` call (guarded by API availability)
- Phase 3: Update `rerun_forecast.py` with `--force` flag
- Phase 4: Remove file and env vars

Phases 1-2 are no longer needed — the simpler "just run for today" approach
eliminates the need for any state tracking. Phase 3 becomes trivial (the
module is idempotent, just re-run it). Phase 4 cleanup is the same.

## Testing

All existing tests pass. New tests added:
- `test_operational_runs_for_today` — verify `forecast_date = date.today()`
- `test_operational_no_define_run_dates` — verify no `define_run_dates` call
- `test_operational_no_store_call` — verify no `store_last_successful_run_date` call
- `test_operational_idempotent` — running twice produces same result
