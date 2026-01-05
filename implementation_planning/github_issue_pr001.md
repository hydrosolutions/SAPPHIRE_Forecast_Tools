# GitHub Issue: PR-001

**Title**: `feat(preprocessing_runoff): Add operational and maintenance modes for data fetching`

**Labels**: `enhancement`, `preprocessing`, `py312-migration`

**Status**: Implementation Complete (pending server verification with recent data)

---

## Summary

Add two operating modes to the preprocessing_runoff module to fix data update issues in Docker containers and improve performance for daily operations.

## Context

The preprocessing_runoff module currently does not update runoff data when running in Docker containers on the server, even though it works correctly locally. This is blocking the py312 migration (Phase 6).

**Root Cause**: The `should_reprocess_input_files()` function only checks if local Excel/CSV files have been modified. In Docker with stable file timestamps, it always returns `False`, causing the module to read stale cached data instead of fetching new data from iEasyHydro HF.

## Problem

- Module runs without errors but output files are not updated
- Log shows: "No changes in the daily_discharge directory, using previous data."
- Works locally but fails in Docker due to stable file timestamps
- Current behavior is all-or-nothing: either full reprocessing or cached data

## Desired Outcome

Two distinct operating modes:

### Mode 1: Operational (Default)
- **Purpose**: Fast daily updates for operational forecasting
- **Behavior**: Fetch only yesterday's daily average (`WDDA`) + today's morning discharge (`WDD`)
- **Performance**: ~5-10 seconds (expected) / ~100 seconds (actual - see PR-002)
- **Trigger**: Default for daily pipeline runs

### Mode 2: Maintenance
- **Purpose**: Gap filling and data quality updates
- **Behavior**: Re-read Excel files if modified, fetch configurable lookback window (default 30 days), update differing values
- **Performance**: ~30-60 seconds (expected) / ~850 seconds (actual - see PR-002)
- **Trigger**: Weekly cron job via `bin/daily_preprunoff_maintenance.sh`, manual trigger

## Implementation

### Files Created

| File | Purpose |
|------|---------|
| `apps/preprocessing_runoff/config.yaml` | Module configuration for modes |
| `apps/preprocessing_runoff/src/config.py` | Config loader with defaults and env var overrides |
| `bin/daily_preprunoff_maintenance.sh` | Maintenance mode orchestration script |

### Files Modified

| File | Changes |
|------|---------|
| `apps/preprocessing_runoff/preprocessing_runoff.py` | Added `parse_args()` and `get_mode()` for CLI/env handling |
| `apps/preprocessing_runoff/src/src.py` | Added `_load_cached_data()`, `_merge_with_update()`, refactored `get_runoff_data_for_sites_HF()` with mode parameter |
| `apps/preprocessing_runoff/pyproject.toml` | Added `pyyaml>=6.0` dependency |
| `apps/preprocessing_runoff/Dockerfile.py312` | Updated to use `uv sync` + `uv run` |
| `apps/preprocessing_runoff/README.md` | Added Docker usage documentation |
| `apps/preprocessing_runoff/test/test_src.py` | Fixed relative paths, added tests for new functions |

### Configuration

`apps/preprocessing_runoff/config.yaml`:
```yaml
maintenance:
  lookback_days: 30

operational:
  fetch_yesterday: true
  fetch_morning: true
```

### Usage

```bash
# Operational mode (default) - fast daily updates
uv run preprocessing_runoff.py

# Maintenance mode - full lookback, gap filling
uv run preprocessing_runoff.py --maintenance
# Or via environment variable
PREPROCESSING_MODE=maintenance uv run preprocessing_runoff.py

# Maintenance via orchestration script
bash bin/daily_preprunoff_maintenance.sh /path/to/config/.env
```

## Tasks

- [x] Create `apps/preprocessing_runoff/config.yaml`
- [x] Create `apps/preprocessing_runoff/src/config.py` (config loader)
- [x] Add mode parameter handling to `preprocessing_runoff.py`
- [x] Refactor `get_runoff_data_for_sites_HF()` for operational mode
- [x] Refactor `get_runoff_data_for_sites_HF()` for maintenance mode
- [x] Update `Dockerfile.py312` to use `uv sync` + `uv run`
- [x] Add unit tests for config loading
- [x] Add unit tests for operational mode
- [x] Add unit tests for maintenance mode
- [x] Test operational mode locally (uv run) - ~101 seconds
- [x] Test maintenance mode locally (uv run) - ~857 seconds
- [x] Test operational mode in Docker - working
- [x] Test maintenance mode in Docker - working
- [x] Create `bin/daily_preprunoff_maintenance.sh` orchestration script
- [x] Update README with Docker usage documentation
- [ ] Re-test with recent data on server (pending data availability)

## Out of Scope

- Dashboard UI for triggering maintenance mode (separate issue)
- Changes to other preprocessing modules
- Performance optimization (tracked as PR-002)

## Dependencies

- Requires access to iEasyHydro HF for testing
- Blocks: Phase 6 of py312 migration (flipping `:latest` tag)

## Acceptance Criteria

- [x] Operational mode fetches only yesterday's daily average + today's morning data
- [x] Maintenance mode fetches configurable lookback window and fills gaps
- [x] Mode is controlled via `--maintenance` CLI flag or `PREPROCESSING_MODE` environment variable
- [x] Config file allows customization of lookback days
- [x] Both modes work correctly in Docker containers
- [x] Existing tests continue to pass (27 tests passing)
- [x] New tests cover both modes
- [ ] Verified with recent data on server

## Test Results

```
27 passed in 2.53s
```

Tests run from `apps/` directory:
```bash
uv run --project preprocessing_runoff pytest preprocessing_runoff/test/ -v
```

## Performance Notes

Actual performance is significantly slower than expected due to iEasyHydro HF API latency. This is tracked as a separate issue (PR-002) in `implementation_planning/module_issues.md`.

| Mode | Expected | Actual |
|------|----------|--------|
| Operational | ~5-10s | ~100s |
| Maintenance | ~30-60s | ~850s |

## References

- Issue tracking: `implementation_planning/module_issues.md` (PR-001, PR-002)
- Related: py312 migration plan in `implementation_planning/uv_migration_plan.md`
- Maintenance script pattern: `bin/daily_linreg_maintenance.sh`
