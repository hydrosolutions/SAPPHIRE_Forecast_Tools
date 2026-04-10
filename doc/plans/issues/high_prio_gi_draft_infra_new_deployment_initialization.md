# Add deployment initialization workflow to run_locally.sh and Docker pipeline

**Status**: Draft (revised 2026-04-10 — technical analysis + implementation plan corrected after code review; reviewed against codebase 2026-04-10)
**Module**: infrastructure (run_locally.sh, pipeline_docker.py)
**Priority**: High
**Labels**: `uzhm`, `deployment`, `infrastructure`, `initialization`

---

## Summary

Add an `initialize` target to `run_locally.sh` (and Docker equivalent) that performs first-time database population for a new SAPPHIRE deployment: full historical data sync to the API, hindcast from a configurable start date, and skill metrics computation.

## Context

Deploying SAPPHIRE for a new organization (uzhm) revealed that there is no streamlined initialization workflow. The current process requires manually running 6+ commands in the correct order, passing specific env vars (`SAPPHIRE_SYNC_MODE=initial`, `--start-date`), and using ad-hoc utility scripts. For the AWS deployment, a sysadmin needs a single documented command sequence.

The current manual process discovered during uzhm setup:
1. `maintenance:preprocessing_runoff` — reads Excel files, writes CSV cache (but only syncs last 30 days to API)
2. Manual `initial_api_sync.py` script — pushes full CSV history to API with `SAPPHIRE_SYNC_MODE=initial`
3. `linear_regression --hindcast --start-date 2000-01-06` for PENTAD — but `run_locally.sh` doesn't pass `--start-date`
4. Same for DECAD — must be run separately with explicit start date to avoid auto-detection picking up stale records
5. `recalculate_skill_metrics` for PENTAD
6. `recalculate_skill_metrics` for DECAD
7. ~~`bootstrap_stations.py`~~ — no longer needed since FD-001 Bug 2 integrates manual stations into the dashboard's loading path

## Problem

- `run_locally.sh` has no `initialize` target; sysadmins must know the exact sequence and flags
- `--start-date` cannot be passed to the hindcast via `run_locally.sh`
- Two separate preprocessing steps are required (maintenance run + initial API sync) because the mode system conflates data-fetch behavior with API-write behavior (see Technical Analysis)
- One ad-hoc utility script (`initial_api_sync.py`) is still needed as a workaround

## Desired Outcome

A sysadmin can initialize a new deployment with:
```bash
ieasyhydroforecast_env_file_path=/path/to/.env bash apps/run_locally.sh initialize
```

This single target runs all initialization steps in the correct order. The `ieasyhydroforecast_START_DATE` env var in the `.env` file controls the hindcast start date.

---

## Technical Analysis

### Corrected understanding of mode system (revised 2026-04-10)

The original draft incorrectly stated that `get_mode()` only returns `"operational"` or `"maintenance"`. The actual implementation:

```python
# preprocessing_runoff.py:176-187
def get_mode(args) -> str:
    if args.maintenance:
        return "maintenance"
    return os.getenv("SAPPHIRE_SYNC_MODE", "operational").lower()
```

`get_mode()` returns whatever `SAPPHIRE_SYNC_MODE` contains, including `"initial"`. The `--maintenance` flag takes precedence when present.

### The real problem: mode conflation

The same `mode` string controls **two different behaviors**:

1. **Data fetch** — `get_runoff_data_for_sites_HF(mode=mode)` at `src.py:3520-3524`:
   ```python
   if mode not in ("operational", "maintenance"):
       logger.warning(f"Unknown mode '{mode}', defaulting to 'operational'")
       mode = "operational"
   ```
   Only `"operational"` and `"maintenance"` are recognized. `"initial"` is demoted to `"operational"` with a `logger.warning()` (fetches only yesterday's data, no gap detection, no Excel re-read).

2. **API write** — `_write_runoff_to_api(mode=mode)` at `src.py:4270-4284`:
   ```python
   if mode is not None:
       sync_mode = mode.lower()
   # ...
   elif sync_mode == "initial":
       data_to_write = data  # Write ALL data
   ```
   Recognizes `"initial"` and writes all data to the API.

**For initialization, we need:**
- Fetch: `"maintenance"` behavior (full Excel read, gap detection, 30-day lookback)
- Write: `"initial"` behavior (push all data to API, no date filter)

These are mutually exclusive under the current single-mode design. Running with `--maintenance` gives correct fetch but 30-day API write. Running with `SAPPHIRE_SYNC_MODE=initial` (no `--maintenance`) gives full API write but shallow fetch (yesterday only).

### Why the two-step approach is correct

The existing manual process already solves this by running two steps:
1. `--maintenance` → correct fetch, populates CSV cache, writes 30 days to API
2. `initial_api_sync.py` → reads CSV cache, pushes ALL to API with `mode=None` + `SAPPHIRE_SYNC_MODE=initial` env var

Step 2 works because `initial_api_sync.py` calls the write functions with `mode=None`, so they fall back to reading `SAPPHIRE_SYNC_MODE=initial` from the environment.

### Coverage of initial_api_sync.py

The script syncs three data types:
- **Daily runoff** → `src._write_runoff_to_api` (also done by `preprocessing_runoff.py`)
- **Pentadal observations** → `fl._write_runoff_to_api` (NOT done by `preprocessing_runoff.py`)
- **Decadal observations** → `fl._write_runoff_to_api` (NOT done by `preprocessing_runoff.py`)

The pentadal/decadal observation write is also triggered indirectly by `linear_regression --hindcast`: the pre-loop calls at lines 798 and 820 (`fl.write_pentad_time_series_data` / `fl.write_decad_time_series_data`) internally call `fl._write_runoff_to_api` without a `mode` parameter, so it falls back to `SAPPHIRE_SYNC_MODE` from the environment. Without `SAPPHIRE_SYNC_MODE=initial`, these writes default to `"operational"` mode and only write **today's** observations — not the full history. The full pentadal/decadal observation sync is performed by Step 2 (`initial_api_sync.py`), which is therefore a **hard prerequisite** for the hindcast, not just a convenience for dashboard consumers.

### `--start-date` in linear_regression

- `linear_regression.py:240-248` (arg definition), `:262-266` (parsing) — accepts `--start-date YYYY-MM-DD`
- `run_locally.sh:740` — calls `--hindcast` without `--start-date`
- `ieasyhydroforecast_START_DATE` (env) is only used for new gauges with zero records; for a shared DB with other org's data, auto-detection picks up stale records

The `--start-date` passthrough must ONLY be in `run_initialize_deployment()`, NOT in `run_maintenance_linear_regression()`. Adding it to maintenance would force full re-hindcasting on every maintenance cycle for any deployment with `ieasyhydroforecast_START_DATE` in their `.env`.

### Hindcast data source — API, not CSV

With `SAPPHIRE_API_ENABLED=true` (the default), `forecast_library.py:read_daily_discharge_data()` (line 2365) reads observations exclusively from the preprocessing API — **there is no CSV fallback** (line 2467: fail-fast if API unavailable). This means Step 2 (`initial_api_sync.py`) is a hard prerequisite for Step 3 (hindcast): if the API is not populated with the full observation history, the hindcast will either fail or produce forecasts based on incomplete data.

### `SAPPHIRE_PREDICTION_MODE` casing

All Python consumers (`linear_regression.py:646-647`, `recalculate_skill_metrics.py:85-94`) compare **case-sensitively** against uppercase values `PENTAD`, `DECAD`, `BOTH`. There is no `.lower()` normalization. Lowercase values like `pentad` or `decade` cause silent no-ops or hard errors. The correct values are **`PENTAD`** and **`DECAD`** (not `DECADE`).

### bootstrap_stations.py — already redundant

FD-001 Bug 2 (committed in `b4546a9`) integrated `_create_manual_sites()` into `processing.py`'s `get_all_stations_from_iehhf()` and `get_all_stations_from_file()`. The dashboard automatically populates manual stations on startup. `bootstrap_stations.py` can be deleted.

---

## Implementation Plan

### Approach

Codify the existing manual process as a single `run_locally.sh` target. **No changes to `preprocessing_runoff.py` or `src.py`.** Keep `initial_api_sync.py` as the tool for full CSV-to-API sync — it correctly handles the mode=None/env-var pattern. Add Docker equivalent.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/run_locally.sh` | Add `initialize` target and `run_initialize_deployment()` function |
| `apps/pipeline/pipeline_docker.py` | Add `InitialApiSync`, `LinRegInitial`, `SkillMetricsInitial`, `RunInitializeWorkflow` task classes (reuse `PrepRunoffMaintenance`) |
| `apps/preprocessing_runoff/initial_api_sync.py` | Commit to git (currently untracked) |
| `apps/forecast_dashboard/bootstrap_stations.py` | Delete |

### Implementation Steps

- [ ] **Step 0**: Commit `apps/preprocessing_runoff/initial_api_sync.py` to git

  This file is currently untracked (`??` in git status). It must be committed before the `initialize` target can work on any other machine or in Docker. Review the file for any hardcoded paths or sensitive data before committing.

- [ ] **Step 1**: Add `run_initialize_deployment()` function to `run_locally.sh`

  ```bash
  run_initialize_deployment() {
      # Read start_date: shell env takes precedence, then grep from .env file
      # (run_locally.sh never sources .env — mirrors the resolve_org() pattern)
      local start_date="${ieasyhydroforecast_START_DATE:-}"
      if [ -z "$start_date" ]; then
          local env_file="${ieasyhydroforecast_env_file_path:-}"
          if [ -n "$env_file" ] && [ -f "$env_file" ]; then
              start_date=$(grep -m1 '^ieasyhydroforecast_START_DATE=' "$env_file" \
                           | cut -d'=' -f2 | tr -d '[:space:]"'"'")
          fi
      fi
      if [ -z "$start_date" ]; then
          echo "ERROR: ieasyhydroforecast_START_DATE must be set in .env (or shell env) for initialization"
          exit 1
      fi

      echo "=== SAPPHIRE Initialization ==="
      echo "Start date: $start_date"

      # Step 1: Read data sources and populate CSV cache (maintenance fetch + 30-day API sync)
      echo "--- Step 1/5: Preprocessing runoff (maintenance) ---"
      run_maintenance_preprocessing_runoff

      # Step 2: Push full CSV history to API (initial sync)
      # CRITICAL: This is a hard prerequisite for Step 3 — the hindcast reads
      # observations from the preprocessing API (no CSV fallback when
      # SAPPHIRE_API_ENABLED=true). The script sets SAPPHIRE_SYNC_MODE=initial
      # internally; the env var below is a safety net.
      echo "--- Step 2/5: Initial API sync (full history) ---"
      run_in_venv preprocessing_runoff initial_api_sync.py \
          "SAPPHIRE_SYNC_MODE=initial"

      # Step 3: Hindcast for each horizon
      # Values must be uppercase PENTAD/DECAD — linear_regression.py compares
      # case-sensitively (lines 646-647), lowercase causes silent no-op.
      for mode in PENTAD DECAD; do
          echo "--- Step 3/5: Hindcast ($mode) ---"
          run_in_venv linear_regression linear_regression.py \
              "SAPPHIRE_PREDICTION_MODE=$mode" -- \
              --hindcast --start-date "$start_date"
      done

      # Step 4: Skill metrics for each horizon
      # Values must be uppercase PENTAD/DECAD — recalculate_skill_metrics.py
      # validates against VALID_MODES and exits on unrecognized values.
      for mode in PENTAD DECAD; do
          echo "--- Step 4/5: Skill metrics ($mode) ---"
          run_in_venv postprocessing_forecasts recalculate_skill_metrics.py \
              "SAPPHIRE_PREDICTION_MODE=$mode"
      done

      echo "--- Step 5/5: Verification ---"
      echo "Initialization complete. Start the dashboard to verify data."
  }
  ```

  Add `initialize)` case to the dispatch and `initialize` to `valid_targets`.

- [ ] **Step 2**: Add Docker initialization tasks to `pipeline_docker.py`

  Follow existing patterns:
  - **No `PrepRunoffInitial`** — reuse `PrepRunoffMaintenance` directly. It already sets `SAPPHIRE_SYNC_MODE=maintenance` (correct fetch behavior). Creating a duplicate task that does the exact same thing introduces maintenance burden and divergence risk.
  - `InitialApiSync` — runs `initial_api_sync.py` with `SAPPHIRE_SYNC_MODE=initial`; requires `PrepRunoffMaintenance`. Must use a `command` override: `["uv", "run", "initial_api_sync.py"]` (the script sets `SAPPHIRE_SYNC_MODE=initial` internally). Image: `"sapphire-preprunoff"` (same image, different script).
  - `LinRegInitial(prediction_mode)` — requires `InitialApiSync`. Must use a `command` override to pass `--start-date` (unlike `LinRegMaintenance` which uses the image's default CMD):
    ```python
    start_date = os.environ.get("ieasyhydroforecast_START_DATE", "")
    command = ["uv", "run", "linear_regression.py", "--hindcast", "--start-date", start_date]
    ```
    Environment must include `SAPPHIRE_PREDICTION_MODE={self.prediction_mode}` with values `PENTAD` or `DECAD` (uppercase, case-sensitive). Image: `"sapphire-linreg"`.
  - `SkillMetricsInitial(prediction_mode)` — runs `recalculate_skill_metrics.py`; requires `LinRegInitial(prediction_mode)`. Environment must include `SAPPHIRE_PREDICTION_MODE={self.prediction_mode}` with values `PENTAD` or `DECAD` (uppercase, case-sensitive).
  - `RunInitializeWorkflow` — top-level task requiring `SkillMetricsInitial(prediction_mode="PENTAD")` and `SkillMetricsInitial(prediction_mode="DECAD")`

  Use distinct marker prefix: `get_marker_filepath("initial_<task>")` for tasks that differ from their maintenance equivalents (`InitialApiSync`, `LinRegInitial`, `SkillMetricsInitial`). `PrepRunoffMaintenance` keeps its existing marker.

- [ ] **Step 3**: Delete `apps/forecast_dashboard/bootstrap_stations.py`

  FD-001 Bug 2 handles manual stations natively. Verify by checking that the dashboard loads manual stations without `bootstrap_stations.py` on a fresh deployment.

- [ ] **Step 4**: Document the initialization process

  - Add `initialize` to `run_locally.sh` help text
  - Add `ieasyhydroforecast_START_DATE=YYYY-MM-DD` to `sapphire/.env.example` with a comment explaining it is only needed for initialization
  - Document required `.env` vars: `ieasyhydroforecast_START_DATE=YYYY-MM-DD`
  - Note: initialization can take hours for long hindcast periods

### Phases

```json
{
  "phases": {
    "P0": { "goal": "Commit initial_api_sync.py to git", "depends_on": [], "parallel_agents": 1 },
    "P1": { "goal": "run_locally.sh initialize target", "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "goal": "Docker initialization tasks (reuse PrepRunoffMaintenance, add InitialApiSync/LinRegInitial/SkillMetricsInitial/RunInitializeWorkflow)", "depends_on": ["P0"], "parallel_agents": 1 },
    "P3": { "goal": "Delete bootstrap_stations.py + documentation", "depends_on": ["P1"], "parallel_agents": 1 },
    "P4": { "goal": "Tests", "depends_on": ["P1", "P2", "P3"], "parallel_agents": 1 }
  }
}
```

---

## Testing

### Test Cases

- [ ] `bash apps/run_locally.sh initialize` completes without error for a fresh uzhm deployment
- [ ] API contains full historical daily, pentad, and decad runoff data after initialization
- [ ] LR forecasts exist for both pentad and decad horizons spanning the full hindcast period
- [ ] Skill metrics are populated for all stations and horizons
- [ ] Dashboard loads and shows manual stations without `bootstrap_stations.py`
- [ ] Existing `maintenance` and `short-term` targets still work unchanged
- [ ] Docker `RunInitializeWorkflow` task runs to completion

### Testing Commands

```bash
cd apps
ieasyhydroforecast_env_file_path=/path/to/.env bash run_locally.sh initialize
```

### Manual Verification

1. After initialization, start the dashboard and verify data is visible
2. Run `short-term` operational pipeline and verify it produces a forecast
3. Verify API contains data: `curl http://localhost:8000/api/preprocessing/runoff/?horizon=day&code=16022&limit=1`

---

## Risks

- **R1**: `initial_api_sync.py` is kept as a production tool (not deleted). This is intentional — refactoring `preprocessing_runoff.py` to natively support `--initial` requires modifying the mode conflation in `src.py:get_runoff_data_for_sites_HF` (which demotes unknown modes to "operational" with a warning). That refactor should be a separate issue.
- **R2**: `--start-date` is only passed in `run_initialize_deployment()`, never in `run_maintenance_linear_regression()`. If a user runs `maintenance:linear_regression` with `ieasyhydroforecast_START_DATE` in their .env, the env var is only used for new gauges with zero records — existing behavior is unchanged.
- **R3**: Docker initialization tasks use distinct marker prefixes (`initial_*`) for tasks that differ from maintenance equivalents. `PrepRunoffMaintenance` is reused directly (no duplicate task).
- **R4**: Hindcast for long periods (e.g., 2000-2026) can take hours. The initialization function should print progress.
- **R5**: **Step 2 (initial_api_sync) is a hard prerequisite for Step 3 (hindcast).** With `SAPPHIRE_API_ENABLED=true` (the default), `forecast_library.py:read_daily_discharge_data()` reads observations exclusively from the preprocessing API with no CSV fallback (line 2467). If Step 2 fails silently or partially, the hindcast will fail or produce forecasts based on incomplete data.
- **R6**: `SAPPHIRE_PREDICTION_MODE` values are case-sensitive throughout the codebase. Must always use uppercase `PENTAD`/`DECAD` — lowercase causes silent no-ops (`linear_regression.py:646-647`) or hard errors (`recalculate_skill_metrics.py:85-94`).
- **R7**: `ieasyhydroforecast_START_DATE` is a **new `.env` variable** — it does not exist in any current `.env` file or `.env.example`. It must be added to `sapphire/.env.example` and documented. The `run_initialize_deployment()` function reads it via `grep` from the `.env` file (mirroring the `resolve_org()` pattern at `run_locally.sh:378-397`), since `run_locally.sh` never sources the `.env` file.
- **R8**: `initial_api_sync.py` silently skips missing CSV files (no error, no warning). If preprocessing didn't produce a CSV, that data type won't be synced and the hindcast will run against incomplete API data. The script also has no `try/except` around `pd.read_csv` — a malformed CSV crashes the entire script. These are pre-existing weaknesses, not introduced by this plan.

## Out of Scope

- Refactoring the mode conflation in `preprocessing_runoff.py` / `src.py` (separate issue — would require `get_runoff_data_for_sites_HF` to understand `"initial"`)
- Automating `.env` file creation for new orgs
- Migrating data between organizations
- Adding org-level isolation to the database schema

## Dependencies

- FD-001 Bug 2 (manual stations) — **resolved** (committed `b4546a9`)
- `initial_api_sync.py` must be committed to git and remain in the repo (it is the tool for step 2; currently untracked)

## Acceptance Criteria

- [ ] `run_locally.sh initialize` is a documented, tested target that performs full first-time setup
- [ ] `--start-date` is passed to hindcast from `ieasyhydroforecast_START_DATE` in the initialize path only
- [ ] `bootstrap_stations.py` deleted (manual stations handled by dashboard natively)
- [ ] Docker pipeline has equivalent initialization workflow
- [ ] All existing targets continue to work (zero regressions)
- [ ] A sysadmin following the deployment docs can initialize a new org without developer assistance

---

## Future improvement (separate issue)

Refactor mode handling to separate "fetch mode" from "write mode":
- `get_runoff_data_for_sites_HF` should accept `"initial"` as a fetch mode (equivalent to maintenance fetch)
- `_write_runoff_to_api` should accept sync_mode independently
- This would allow `preprocessing_runoff.py --initial` to do both fetch and write in a single pass
- Would eliminate the need for `initial_api_sync.py`

## References

- uzhm adapter: `doc/plans/issues/archive/high_prio_gi_draft_prepq_uzhm_wide_matrix_adapter.md` (complete)
- Dashboard LR-only fixes: `doc/plans/issues/archive/high_prio_gi_draft_fd_lr_only_deployment_fixes.md` (review)
- `SAPPHIRE_SYNC_MODE` handling: `apps/preprocessing_runoff/src/src.py:4270-4287`, `apps/iEasyHydroForecast/forecast_library.py:3529-3687` (mode logic at `:3594-3598`)
- Mode demotion: `apps/preprocessing_runoff/src/src.py:3520-3524` — `get_runoff_data_for_sites_HF` demotes `"initial"` to `"operational"` (with `logger.warning`)
- Ad-hoc sync script: `apps/preprocessing_runoff/initial_api_sync.py` — kept as production tool
