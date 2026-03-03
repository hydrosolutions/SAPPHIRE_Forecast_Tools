# Clean Up Dead `last_successful_run` Code and State

**Module:** linear_regression, iEasyHydroForecast (setup_library), reset_forecast_run_date
**Priority:** Low
**Status:** Draft
**Parent:** Cleanup from LR-002 (archived as complete)

## Context

LR-002 replaced the file-based `last_successful_run` state with a "just run
for today" approach. The core change is implemented and tested. This issue
tracks the remaining dead-code removal and env var cleanup.

## Tasks

### 1. Remove dead functions from `setup_library.py`

**File:** `apps/iEasyHydroForecast/setup_library.py` (lines ~54-143+)

Remove the following functions that are no longer called by any active module:

- `store_last_successful_run_date()`
- `get_last_run_date()`
- `define_run_dates()`

**Verification:** `grep -r` across `apps/` confirms no caller except
`rerun_forecast.py` (see task 3).

### 2. Remove `ieasyforecast_last_successful_run_file` from `.env` files

Remove the env var from these 4 files (all in the external config repo):

- `.env_develop`
- `.env_develop_kghm`
- `.env`
- `.env_develop_test`

This var is only read by the functions removed in task 1.

### 3. Deprecate/remove `rerun_forecast.py`

**File:** `apps/reset_forecast_run_date/rerun_forecast.py` (~209 lines)

This module is the only remaining consumer of `define_run_dates()` and
`store_last_successful_run_date()`. Since LR is now idempotent (upsert
semantics), "rerun" is simply "run `linear_regression.py` again." The module
can be removed entirely.

**Before removing:** Verify no cron job or deployment script references
`rerun_forecast.py`.

### 4. Remove state files from deployment servers (manual)

Delete these files from all deployment servers:

- `linreg_last_successful_run_PENTAD.txt`
- `linreg_last_successful_run_DECAD.txt`

These are in the intermediate data directory
(`ieasyforecast_intermediate_data_path`). They are no longer read or written
by any code after tasks 1-3.

### 5. Update documentation

After tasks 1-3, several docs reference the removed module and state files.
Update or remove these references:

**`README.md`:**
- Line 152: Remove `latest_successful_run.txt` from the folder structure listing
- Lines 153-156: Remove the `reset_forecast_run_date` module entry, or replace
  with a note that LR is idempotent and can simply be re-run

**`CLAUDE.md`:**
- Line 38: Remove `reset_forecast_run_date` from the Application Modules table

**`doc/development.md`:**
- Lines ~1147-1159: Rewrite the "re-run a forecast" instructions — no longer
  requires `reset_forecast_run_date`, just re-run the module directly
- Line ~1297: Remove instructions about editing `last_successful_run.txt` for
  hindcasts — hindcast mode (`--hindcast`) handles this now

**`doc/configuration.md`:**
- Lines 45-48: Remove the `ieasyforecast_last_successful_run_file` env var
  description

**`doc/deployment.md`:**
- Line ~241: Remove reference to editing `last_successful_run.txt` for running
  hindcasts; replace with `--hindcast` flag instructions

**`doc/prod/update_deployment_checklist.md`:**
- Line ~229: Remove `last_successful_run.txt` from the backup file list

## Testing

- After task 1: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — all
  tests pass (no module calls these functions).
- After task 3: Verify `run_tests.sh` still passes; update or remove any tests
  in `reset_forecast_run_date/` if they exist.
- After task 5: Verify no remaining references with
  `grep -r "reset_forecast_run_date\|last_successful_run\|rerun_forecast" README.md CLAUDE.md doc/ --include="*.md" | grep -v doc/plans`
