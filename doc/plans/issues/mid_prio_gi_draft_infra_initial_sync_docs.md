# Document database initialization for fresh deployments

**Priority:** Mid
**Category:** Infrastructure / Documentation
**Discovered:** 2026-04-01 — Maxat's local DB had no historical runoff data,
causing `linear_regression` DECAD mode to crash with
`ValueError: Cannot write empty runoff_stats DataFrame to CSV`.

---

## Problem

A fresh SAPPHIRE deployment (empty preprocessing DB) cannot run the forecast
pipeline because `linear_regression` needs historical daily runoff data
(multiple years) to compute hydrograph statistics. Without initialization:

- **DECAD mode** crashes in `write_decad_hydrograph_data` (explicit empty
  check at `forecast_library.py:5102`)
- **PENTAD mode** silently writes an empty hydrograph CSV (no guard — same
  root cause, different symptom)

The `SAPPHIRE_SYNC_MODE=initial` environment variable exists and works, but:

1. It's not mentioned in the deployment plan (now fixed in Phase 4.3)
2. Module READMEs document it inconsistently
3. `run_locally.sh` has no `initialize` target
4. There's no central "first-time setup" guide that tells deployers they
   need to run it

## Current state of documentation

| Location | Coverage |
|----------|----------|
| `apps/preprocessing_gateway/README.md` | Good — explicit "Initial mode" section with examples |
| `apps/preprocessing_runoff/README.md` | Minimal — listed in env var table, no "when to use" guidance |
| `apps/linear_regression/README.md` | Minimal — config table only |
| `apps/iEasyHydroForecast/` | None |
| `doc/deployment.md` | None |
| `doc/configuration.md` | None |
| `run_locally.sh` | No `initialize` target; no comments about initial mode |
| `sapphire/services/preprocessing/app/run_data_migrator.sh` | Runoff line commented out (line 24) |

## Two initialization paths

### Path A: Data migrator (CSV → API, inside Docker)

```bash
docker exec -it sapphire-preprocessing-api python app/data_migrator.py --type runoff
```

- Reads from `runoff_day.csv`, `runoff_pentad.csv`, `runoff_decad.csv`
- Requires CSV files mounted in the container
- Best for: migrating from CSV-based SAPPHIRE, restoring backups

### Path B: Initial sync mode (pipeline → API)

```bash
SAPPHIRE_SYNC_MODE=initial \
  ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh preprocessing_runoff
```

- `preprocessing_runoff` reads from Excel files + iEH HF API, writes ALL
  records to the preprocessing API (not just today's)
- Same pattern works for `preprocessing_gateway` (meteo/snow data)
- Best for: fresh deployment where data source is Excel + iEH HF

## Implementation plan

### Phase 1: Fix `run_data_migrator.sh`

**File:** `sapphire/services/preprocessing/app/run_data_migrator.sh`

- Uncomment line 24 (`run "$SCRIPT --type runoff"`) OR remove it since
  line 21 (`run "$SCRIPT"`) already runs `--type all` which includes runoff
- Clean up the redundant individual runs (lines 25-27) that duplicate
  the `--type all` on line 21

### Phase 2: Add `initialize` target to `run_locally.sh`

**File:** `apps/run_locally.sh`

Add a new target that runs preprocessing modules with `SAPPHIRE_SYNC_MODE=initial`:

```bash
# Target: initialize
#   Populate empty databases with historical data from Excel/iEH HF sources.
#   Run this ONCE on a fresh deployment before any daily/operational runs.
initialize)
    export SAPPHIRE_SYNC_MODE=initial
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    # After preprocessing, run LR to populate hydrograph + runoff pentad/decad tables
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done
    ;;
```

### Phase 3: Update module READMEs

For each module that supports `SAPPHIRE_SYNC_MODE`, add an "Initial Setup"
section (following the pattern in `preprocessing_gateway/README.md`):

| Module README | What to add |
|---------------|-------------|
| `preprocessing_runoff/README.md` | "Initial Setup" section with command example, explanation of what data sources are read, expected record count |
| `linear_regression/README.md` | Note that LR requires historical data in preprocessing DB; link to initialization |
| `iEasyHydroForecast/` | No README exists — out of scope for this issue |

### Phase 4: Update central docs

| Doc | What to add |
|-----|-------------|
| `doc/deployment.md` | "Database Initialization" section between services startup and pipeline testing |
| `doc/configuration.md` | Add `SAPPHIRE_SYNC_MODE` to the env var reference with description of all three modes |

## Acceptance criteria

- [ ] `run_locally.sh initialize` target exists and works on a fresh DB
- [ ] `preprocessing_runoff/README.md` has an "Initial Setup" section
- [ ] `linear_regression/README.md` explains the historical data requirement
- [ ] `doc/deployment.md` references initialization as a required step
- [ ] `run_data_migrator.sh` is cleaned up (no commented-out runoff line)

## Out of scope

- Fixing the pentad silent-empty-write (separate bug — should add the same
  empty guard as decad has). Track separately.
- Adding `SAPPHIRE_SYNC_MODE` to `.env` template files (nice-to-have,
  not blocking).
