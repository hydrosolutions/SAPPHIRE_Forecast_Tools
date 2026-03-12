# Organization-Aware Module Filtering in run_locally.sh

**Status**: Draft
**Module**: Infrastructure (`apps/run_locally.sh`)
**Branch**: `develop_long_term_fix_api_postprocessing_forecasts`

## Problem

`run_locally.sh` runs all modules for every organization, but the **demo** org
only needs a subset:

| Module | Demo needs? | Why not? |
|--------|-------------|----------|
| `preprocessing_runoff` | Yes | Core discharge data |
| `preprocessing_gateway` | **No** | ERA5/SnowMapper data not used by demo LR models |
| `linear_regression` | Yes | Primary forecast model for demo |
| `machine_learning` | **No** | ML models disabled by default for demo |
| `postprocessing_forecasts` | Yes | Ensemble creation + API write |
| `long_term_forecasting` | **No** | Not part of demo workflow |

This is already reflected in Docker image pulls (`bin/utils/pull_docker_images.sh`
lines 20-28: demo pulls only base, preprunoff, linreg, postprocessing, rerun).

## Design

### Approach: Module Skip Lists per Organization

Read `ieasyhydroforecast_organization` from the environment (already sourced from
`.env` by the caller). Define a skip list for each known org. Before running any
module, check if it should be skipped for the current org.

**Why skip-list instead of allow-list?** Production orgs (kghm, tjhm) run
everything. Only demo has exclusions. A skip-list keeps the default behavior
unchanged for production and is additive-safe when new modules are added.

### Organization Variable

The variable `ieasyhydroforecast_organization` is already set by the `.env` file
and exported by `run_locally.sh` (line 1484). It is available in the shell
environment. We just need to read it and default to empty (= run everything) if
unset.

### Modules to Skip for Demo

```bash
DEMO_SKIP_MODULES=(preprocessing_gateway machine_learning long_term_forecasting)
```

### Skip Function

```bash
should_skip_module() {
    local module="$1"
    local org="${ieasyhydroforecast_organization:-}"
    if [ "$org" = "demo" ]; then
        for skip in "${DEMO_SKIP_MODULES[@]}"; do
            [ "$module" = "$skip" ] && return 0
        done
    fi
    return 1
}
```

## Changes Required

### File: `apps/run_locally.sh`

#### Change 1: Add org-aware skip infrastructure (Configuration section, ~line 95)

Add after the `ML_MAINTENANCE_SCRIPTS` array (line 151):

```bash
# Organization-aware module skip lists
# Demo org only needs: preprocessing_runoff, linear_regression, postprocessing_forecasts
ORG="${ieasyhydroforecast_organization:-}"
DEMO_SKIP_MODULES=(preprocessing_gateway machine_learning long_term_forecasting)
```

Add a utility function after `check_venv()` (line 289):

```bash
should_skip_module() {
    local module="$1"
    if [ "$ORG" = "demo" ]; then
        for skip in "${DEMO_SKIP_MODULES[@]}"; do
            [ "$module" = "$skip" ] && return 0
        done
    fi
    return 1
}
```

#### Change 2: Guard pipeline orchestrators

**`run_short_term_pipeline()`** (line 871): Wrap `run_preprocessing_gateway`,
`run_machine_learning` calls with skip guards.

**`run_daily_pipeline()`** (line 1018): Same — skip gateway, ML, and long-term
sections when org=demo.

**`run_maintenance_pipeline()`** (line 981): Skip gateway and ML maintenance.

**`run_long_term_pipeline()`** (line 909): Skip entirely for demo.

**`run_long_term_operational_pipeline()`** (line 927): Skip entirely for demo.

**`run_all()`** (line 966): Skip long-term pipeline for demo.

#### Change 3: Guard individual module runners when called as single targets

In the `main()` dispatch (line 1492), for single-module targets like
`preprocessing_gateway`, `machine_learning`, `long_term_forecasting` — add a
skip check that logs a message and exits cleanly.

#### Change 4: Guard maintenance targets

For `maintenance:preprocessing_gateway`, `maintenance:machine_learning`, and
`maintenance:postprocessing_long_term` — same skip-and-log pattern.

#### Change 5: Guard validation targets

For `calibrate_long_term` — skip for demo.

#### Change 6: Log the organization at startup

In `main()` after the banner (line 1467), add:
```bash
log INFO "Organization: ${ORG:-<not set, running all modules>}"
```

And if any modules will be skipped, log which ones:
```bash
if [ "$ORG" = "demo" ]; then
    log INFO "Demo org: skipping ${DEMO_SKIP_MODULES[*]}"
fi
```

#### Change 7: Update help text

Add `ieasyhydroforecast_organization` to the Environment variables section in
`print_usage()`, noting that demo org skips gateway/ML/long-term modules.

## Behavioral Matrix

| Target | Demo behavior | Other orgs |
|--------|---------------|------------|
| `daily` | preproc_runoff → LR (PENTAD+DECAD) → PP (PENTAD+DECAD) → maintenance (runoff, LR, PP) | Full pipeline (unchanged) |
| `short-term` | preproc_runoff → LR → PP | Full pipeline (unchanged) |
| `long-term` | Skip with log message | Full pipeline (unchanged) |
| `long-term-operational` | Skip with log message | Full pipeline (unchanged) |
| `all` | Short-term only (skip LT) | Full pipeline (unchanged) |
| `maintenance` | runoff + LR + PP maintenance only | Full pipeline (unchanged) |
| `maintenance:preprocessing_gateway` | Skip with log message | Runs (unchanged) |
| `maintenance:machine_learning` | Skip with log message | Runs (unchanged) |
| `maintenance:postprocessing_long_term` | Skip with log message | Runs (unchanged) |
| `calibrate_long_term` | Skip with log message | Runs (unchanged) |
| `preprocessing_gateway` (single) | Skip with log message | Runs (unchanged) |
| `machine_learning` (single) | Skip with log message | Runs (unchanged) |
| `long_term_forecasting` (single) | Skip with log message | Runs (unchanged) |
| `yearly` | snow norms skip (gateway), skill metrics runs | Full (unchanged) |
| `recalculate_snow_norms` | Skip (requires gateway) | Runs (unchanged) |

## Testing

1. **Dry-run with demo org**:
   ```bash
   ieasyhydroforecast_organization=demo bash apps/run_locally.sh --dry-run daily
   ```
   Should show skipped modules in validation output.

2. **Dry-run with kghm org** (or unset):
   ```bash
   ieasyhydroforecast_organization=kghm bash apps/run_locally.sh --dry-run daily
   ```
   Should show all modules validated (unchanged behavior).

3. **Single skipped module**:
   ```bash
   ieasyhydroforecast_organization=demo bash apps/run_locally.sh --dry-run machine_learning
   ```
   Should log "Skipping machine_learning (not required for demo org)" and exit 0.

## Implementation Phases

### Phase 1: Add skip infrastructure
- Add `ORG`, `DEMO_SKIP_MODULES`, `should_skip_module()` to configuration section
- Add org logging to `main()`
- Update `validate_env()` to only check venvs for non-skipped modules

### Phase 2: Guard pipeline orchestrators
- `run_short_term_pipeline()` — skip gateway + ML calls
- `run_daily_pipeline()` — skip gateway, ML, long-term phases
- `run_maintenance_pipeline()` — skip gateway + ML maintenance
- `run_long_term_pipeline()` — skip entirely for demo
- `run_long_term_operational_pipeline()` — skip entirely for demo
- `run_all()` — skip long-term for demo
- `run_yearly_pipeline()` — skip snow norms for demo

### Phase 3: Guard dispatch targets in main()
- Single-module targets: add skip check before dispatch
- Maintenance targets: add skip check before dispatch
- Calibration/recalculation targets: add skip check

### Phase 4: Update help text and validation
- Update `print_usage()` with org documentation
- Update `validate_env()` to filter modules_to_check by org

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "name": "Add skip infrastructure",
      "depends_on": [],
      "description": "Add ORG variable, DEMO_SKIP_MODULES array, should_skip_module() function, org logging in main()",
      "files": ["apps/run_locally.sh"],
      "lines": "Configuration section (~95-160), utility functions (~282-289), main() banner (~1466-1470)"
    },
    "phase_2": {
      "name": "Guard pipeline orchestrators",
      "depends_on": ["phase_1"],
      "description": "Add should_skip_module guards in run_short_term_pipeline, run_daily_pipeline, run_maintenance_pipeline, run_long_term_pipeline, run_long_term_operational_pipeline, run_all, run_yearly_pipeline",
      "files": ["apps/run_locally.sh"],
      "lines": "Pipeline orchestrators (~871-1078)"
    },
    "phase_3": {
      "name": "Guard dispatch targets in main()",
      "depends_on": ["phase_1"],
      "description": "Add skip checks for single-module and maintenance dispatch cases in main()",
      "files": ["apps/run_locally.sh"],
      "lines": "Main dispatch (~1492-1585)"
    },
    "phase_4": {
      "name": "Update help text and validation",
      "depends_on": ["phase_1"],
      "description": "Update print_usage() env vars section, update validate_env() to filter by org",
      "files": ["apps/run_locally.sh"],
      "lines": "print_usage (~1290-1403), validate_env (~1084-1154)"
    }
  },
  "dependency_graph": {
    "phase_1": [],
    "phase_2": ["phase_1"],
    "phase_3": ["phase_1"],
    "phase_4": ["phase_1"]
  },
  "parallelizable": [
    ["phase_2", "phase_3", "phase_4"]
  ],
  "execution_order": [
    {"step": 1, "phases": ["phase_1"]},
    {"step": 2, "phases": ["phase_2", "phase_3", "phase_4"]}
  ]
}
```
