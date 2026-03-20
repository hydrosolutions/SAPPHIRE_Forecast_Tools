# Plan: Upstream Module Org-Scoped API Reads (INFRA-011)

**Status**: In Progress — Phase 1 audit complete; Phases 2-3 deferred
**Branch**: TBD
**Module**: linear_regression, preprocessing_gateway, preprocessing_runoff, iEasyHydroForecast, validate_pipeline
**Depends on**: PP-025 (establishes the pattern), INFRA-009 (station filtering)
**Priority**: Low (production is single-org; these modules are already implicitly scoped by config)

## Context

PP-025 scopes all postprocessing and ML reads by station code. However, several
upstream modules also make unscoped API reads/writes. In the current single-org
production deployment this is not a problem because:

1. **Writes** are scoped by the station loop (each module iterates over its
   configured stations from `config_station_selection.json`)
2. **Reads** that fetch all data are used within a per-station loop, so extra
   data is filtered downstream

This issue tracks the remaining unscoped calls for future multi-org deployments
or shared-DB scenarios.

## Inventory

### linear_regression

| Call | File | Scoped? | Risk |
|------|------|---------|------|
| `client.read_runoff()` | `linear_regression.py` | No `code=` in some calls | Low — used within station loop |
| `client.write_lr_forecasts()` | `linear_regression.py` | Per-station (writes current station) | None |
| `client.write_hydrograph()` | `linear_regression.py` | Per-station | None |
| `client.write_runoff()` | `linear_regression.py` | Per-station | None |

### preprocessing_gateway

| Call | File | Scoped? | Risk |
|------|------|---------|------|
| `client.read_meteo(code=code)` | `gateway.py` | Yes | None |
| `client.write_meteo()` | `gateway.py` | Per-station | None |
| `client.read_snow(code=code)` | `gateway.py` | Yes | None |
| `client.write_snow()` | `gateway.py` | Per-station | None |

### preprocessing_runoff

| Call | File | Scoped? | Risk |
|------|------|---------|------|
| `client.write_runoff()` | `preprocessing_runoff.py` | Per-station (from HF SDK) | None |
| `client.write_hydrograph()` | `preprocessing_runoff.py` | Per-station | None |

### iEasyHydroForecast

| Call | File | Scoped? | Risk |
|------|------|---------|------|
| `client.read_runoff(code=code)` | `forecast_library.py` | Yes | None |
| `client.read_meteo(code=code)` | `forecast_library.py` | Yes | None |
| `client.read_hydrograph(code=code)` | `forecast_library.py` | Yes | None |
| `client.read_lr_forecasts(code=code)` | `forecast_library.py` | Yes | None |
| `client.read_short_term_forecasts(code=code)` | `forecast_library.py` | Yes | None |
| `client.write_lr_forecasts()` | `setup_library.py` | Per-station | None |
| `client.write_hydrograph()` | `setup_library.py` | Per-station | None |
| `client.write_runoff()` | `setup_library.py` | Per-station | None |

### validate_pipeline

| Call | File | Scoped? | Risk |
|------|------|---------|------|
| All `client.read_*()` | `validate_pipeline.py` | No — reads everything | **Medium** — gives false positives in multi-org DB (Org A validation passes if Org B has data for the same horizon/model/date) |

### Out-of-Scope Modules

| Module | Unscoped calls | Why out of scope |
|--------|----------------|------------------|
| `long_term_forecasting` | `get_meteo_data()`, `get_runoff_data()` in `data_interface.py` — raw SQL without `code` filter | Uses a separate data access layer (`DataInterfaceDB` with raw SQL), not the `sapphire_api_client`. Scoping requires changes to SQL query construction in `data_interface.py`. Tracked separately if needed. |
| `forecast_dashboard` | `get_all_stations_from_file()` loads `all_stations.pkl` without org filter | Display-only dashboard. Station list dropdown shows all orgs' stations but individual data fetches are per-station (`code=` in URL params). Low risk — no data mutation. |

## Assessment

Most upstream modules are already effectively org-scoped because they iterate
over stations from the org's config file. The only truly unscoped reads are in
`validate_pipeline`, which is intentionally global but produces **misleading
results** in a shared-DB multi-org scenario.

**Recommendation**: No immediate action needed for most modules. However,
`validate_pipeline` should gain an optional `--org` flag (Phase 2) so that
validation results are reliable when the database contains multiple orgs' data.

## Phases

### Phase 1: Audit Complete

This plan IS the audit. Mark complete when reviewed.

### Phase 2: Add `--org` Flag to validate_pipeline

**Goal**: When `--org` is passed (or `ieasyhydroforecast_organization` env var
is set), validate_pipeline reads station codes from `config_station_selection.json`
and passes `code=` to all API reads. Without the flag, behavior is unchanged
(reads everything — backward compatible).

**Depends on**: INFRA-009 Phase 3 (for `config_station_selection.json` pattern)

**File to modify**: `apps/validate_pipeline/validate_pipeline.py`

1. Add `--org` CLI argument (argparse). If not passed, check
   `ieasyhydroforecast_organization` env var. If neither, run unscoped (current
   behavior).
2. When org is set, read station codes from `config_station_selection.json`
   (same pattern as INFRA-009 Phase 3).
3. Pass `code=code` to each `client.read_*()` call in `check_presence()`, looping
   over codes. Concatenate results per table.
4. Log clearly whether running in org-scoped or global mode.

**Test requirements**:
- Test with `--org` set: only scoped data returned
- Test without `--org`: all data returned (backward compat)
- Test with empty station list: graceful warning, not crash

### Phase 3: Add `code=` to Unscoped linear_regression Reads

**Depends on**: Phase 1
**Note**: Only needed if shared-DB multi-org goes to production.

**File to modify**: `apps/linear_regression/linear_regression.py`

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "name": "Audit and document all upstream API calls",
      "depends_on": [],
      "note": "This plan IS the audit. Mark complete when reviewed."
    },
    "phase_2": {
      "name": "Add --org flag to validate_pipeline",
      "depends_on": ["phase_1", "INFRA-009 Phase 3"],
      "note": "Org-scoped validation to prevent false positives in multi-org DB",
      "files": [
        "apps/validate_pipeline/validate_pipeline.py",
        "apps/validate_pipeline/test/test_validate_pipeline.py"
      ]
    },
    "phase_3": {
      "name": "Add code= to unscoped linear_regression reads",
      "depends_on": ["phase_1"],
      "note": "Deferred — only needed if shared-DB multi-org goes to production",
      "files": ["apps/linear_regression/linear_regression.py"]
    }
  },
  "execution_order": [
    "phase_1",
    {"parallel": ["phase_2", "phase_3 (deferred)"]}
  ]
}
```
