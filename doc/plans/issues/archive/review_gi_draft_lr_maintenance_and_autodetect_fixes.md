# LR-006: Fix maintenance sync mode and hindcast auto-detect (API-first)

**Status**: Review
**Module**: `linear_regression`
**Priority**: High
**Labels**: `bug`, `maintenance`, `hindcast`, `data-integrity`

---

## Summary

Two bugs in the linear regression maintenance and hindcast infrastructure:

1. **Missing `SAPPHIRE_SYNC_MODE=maintenance`** in both
   `bin/daily_linreg_maintenance.sh` (line 130) and
   `apps/pipeline/pipeline_docker.py` `LinRegMaintenance` (line 1758).
   The LR container is the **sole writer** of `horizon_type="pentad"` and
   `horizon_type="decade"` runoff records to the preprocessing API.
   Without the sync-mode flag, `_write_runoff_to_api()` in
   `forecast_library.py` filters to today-only, so pentadal/decadal
   aggregates for historical dates produced during hindcast are silently
   discarded.

2. **Hindcast auto-detect reads CSV instead of the API.**
   `get_last_forecast_dates_per_gauge()` (lines 311, 335) reads hardcoded
   CSV filenames to determine where to resume a hindcast. The postprocessing
   API already stores every LR forecast with `date`, `code`, and
   `horizon_type` — auto-detect should query the API as the primary source,
   with CSV as a fallback for deployments where the API is unavailable.
   The CSV path also has a secondary bug: it hardcodes filenames that don't
   match non-default deployments (e.g., KGHM uses `analysis_pentad_kghm.csv`).

---

## Problem Statement

### Bug 1: Missing SAPPHIRE_SYNC_MODE in LR maintenance orchestration

`SAPPHIRE_SYNC_MODE` controls the date-range filter in
`forecast_library._write_runoff_to_api()` (line 3553):

- `operational` (default): writes only records where `date == today`
- `maintenance`: writes records from the last 30 days (will be extended to
  90 days in Phase 1 — see Change 1c)
- `initial`: writes all records

The preprocessing `runoffs` table stores all temporal resolutions in one table,
discriminated by the `horizon_type` column (`"day"`, `"pentad"`, `"decade"`,
etc.), with unique constraint `(horizon_type, code, date)`.

| Writer | `horizon_type` | Module |
|--------|----------------|--------|
| `preprocessing_runoff/src/src.py` | `"day"` | preprunoff container |
| `forecast_library.py` (pentad path) | `"pentad"` | LR container |
| `forecast_library.py` (decade path) | `"decade"` | LR container |

`PrepRunoffMaintenance` correctly sets `SAPPHIRE_SYNC_MODE=maintenance` —
but it only writes `horizon_type="day"` records. The LR container is the
**only** writer for pentad and decade runoff. Two orchestration paths omit
the flag:

- `bin/daily_linreg_maintenance.sh` line 130: sets `-e RUN_MODE=maintenance`
  but not `-e SAPPHIRE_SYNC_MODE=maintenance`
- `apps/pipeline/pipeline_docker.py` `LinRegMaintenance.run()` line 1758:
  sets `"RUN_MODE=maintenance"` but not `"SAPPHIRE_SYNC_MODE=maintenance"`

Both `daily_preprunoff_maintenance.sh` (line 125), `daily_gateway_maintenance.sh`
(line 111), `GatewayMaintenance` (line 1679), and `PrepRunoffMaintenance`
(line 1716) all set this correctly.

### Bug 2: Hindcast auto-detect reads CSV instead of API

`get_last_forecast_dates_per_gauge()` determines where to resume a hindcast.
It currently reads CSV files only. The postprocessing API stores LR forecast
records with `date`, `code`, and `horizon_type` — queried via
`client.read_lr_forecasts(horizon=..., code=..., start_date=..., limit=...)`.

The function should query the API first (the authoritative data store), then
fall back to CSV for deployments where the API is unavailable.

The CSV fallback path itself has a secondary bug: lines 311 and 335 hardcode
`"forecast_pentad_linreg.csv"` / `"forecast_decad_linreg.csv"` instead of
reading from `ieasyforecast_analysis_pentad_file` /
`ieasyforecast_analysis_decad_file`. These env vars are already used by 6
production call sites in `forecast_library.py` and `setup_library.py` for
writing and reading these same files. The auto-detect function is the sole
place that doesn't use them.

---

## Root Cause

**Bug 1**: The flag was not carried over when the linreg maintenance script
was authored. The same omission was replicated in the Luigi `LinRegMaintenance`
task.

**Bug 2**: The function was written before the API integration existed and was
never updated. The hardcoded CSV filenames matched the default `.env` values
but not deployment-specific overrides like `.env_develop_kghm`.

---

## Impact

- **Bug 1**: After a maintenance hindcast, the preprocessing API has gaps in
  `horizon_type="pentad"` and `"decade"` runoff records. Any downstream
  consumer querying `GET /runoff/?horizon=pentad` or `horizon=decade` (e.g.,
  dashboards, skill metric calculations) won't see aggregated data for the
  backfilled dates. The LR **forecasts** themselves are unaffected — they are
  computed from daily data in the same run and written to the postprocessing
  API without sync-mode filtering.

- **Bug 2**: In all deployments, auto-detect ignores LR forecasts already
  stored in the API. In non-default deployments (e.g., KGHM), the CSV
  fallback also fails silently because the hardcoded filenames don't match
  the actual files. The result: every maintenance run re-runs the full
  hindcast from `START_DATE`, wasting compute and re-writing records.

---

## Implementation Plan

### Phase 1: Fix maintenance sync mode (shell script + Luigi task)

**Goal**: Add `SAPPHIRE_SYNC_MODE=maintenance` to both orchestration paths
for the LR maintenance container.

**Files allowed to modify**:
- `bin/daily_linreg_maintenance.sh`
- `apps/pipeline/pipeline_docker.py`
- `apps/iEasyHydroForecast/forecast_library.py` (Change 1c only)

**Change 1a** — `daily_linreg_maintenance.sh`: Add
`-e SAPPHIRE_SYNC_MODE=maintenance \` after line 130
(`-e RUN_MODE=maintenance \`). Extend the comment block at lines 119–121:

```bash
# Run the linear regression container in maintenance (hindcast) mode
# RUN_MODE=maintenance triggers --hindcast flag via the container's CMD
# SAPPHIRE_SYNC_MODE=maintenance triggers 90-day lookback for pentad/decad
#   runoff writes to the preprocessing API (same as daily_preprunoff_maintenance.sh)
# DOCKER_HOST_OVERRIDE is set on macOS to replace localhost with host.docker.internal
```

**Change 1b** — `pipeline_docker.py`: In `LinRegMaintenance.run()` (line 1756),
add `"SAPPHIRE_SYNC_MODE=maintenance"` to the environment list:

```python
environment = _common_maintenance_env() + [
    f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
    "RUN_MODE=maintenance",
    "SAPPHIRE_SYNC_MODE=maintenance",
]
```

**Change 1c** — `forecast_library.py`: In `_write_runoff_to_api()`, extend
the maintenance window from 30 days to 90 days (line 3572):

```python
# Before:
cutoff = today - pd.Timedelta(days=30)

# After:
cutoff = today - pd.Timedelta(days=90)
```

Update the docstring at line 3495 and the inline comment at line 3571 to say
"90 days" instead of "30 days". This change affects only the
`forecast_library.py` copy of `_write_runoff_to_api()`, which writes
pentad/decade runoff from the LR container. Note: `preprocessing_runoff` has
its own independent copy in `src/src.py:4036` with a separate `days=30`
cutoff at line 4089 — that copy is **not** modified here (it writes daily
runoff only and is outside the scope of this issue). The broader 90-day
window covers typical LR maintenance hindcast durations without requiring
`SAPPHIRE_SYNC_MODE=initial`.

**Do NOT change any existing function signatures, data flow logic, or control
flow. These changes are purely additive (1a, 1b) or change a single constant
(1c).**

**Acceptance criteria**:
- `daily_linreg_maintenance.sh` docker run block includes
  `-e SAPPHIRE_SYNC_MODE=maintenance \`
- `LinRegMaintenance.run()` environment list includes
  `"SAPPHIRE_SYNC_MODE=maintenance"`
- `_write_runoff_to_api()` maintenance cutoff is 90 days
- No other changes to these files

---

### Phase 2: API-first hindcast auto-detect

**Goal**: Rewrite `get_last_forecast_dates_per_gauge()` to query the
postprocessing API as the primary source, with the existing CSV logic as
fallback.

**Files allowed to modify**:
- `apps/linear_regression/linear_regression.py`

**Depends on**: Nothing (independent of Phase 1)

**Design**:

The function currently takes `prediction_mode` (`"PENTAD"`, `"DECAD"`, or
`"BOTH"`) and returns `dict[str, datetime.date]` mapping gauge code → last
forecast date. The new implementation preserves this signature and contract.

```python
def get_last_forecast_dates_per_gauge(prediction_mode="BOTH"):
    """..."""
    gauge_dates = {}

    # --- Primary path: query postprocessing API ---
    try:
        gauge_dates = _get_last_dates_from_api(prediction_mode)
        if gauge_dates:
            logger.info(
                f"Auto-detect: found last forecast dates for "
                f"{len(gauge_dates)} gauges from API"
            )
            return gauge_dates
        logger.info("Auto-detect: API returned no LR forecasts, trying CSV fallback")
    except Exception as e:
        logger.warning(f"Auto-detect: API query failed ({e}), trying CSV fallback")

    # --- Fallback: read CSV files ---
    gauge_dates = _get_last_dates_from_csv(prediction_mode)
    return gauge_dates
```

**New helper `_get_last_dates_from_api(prediction_mode)`**:

```python
def _get_last_dates_from_api(prediction_mode):
    """Query postprocessing API for the last LR forecast date per gauge.

    Uses the module-level ``fl`` binding (``import forecast_library as fl``
    at the top of ``linear_regression.py``).  Do NOT add a local import —
    ``iEasyHydroForecast.forecast_library`` is not importable as a package
    path (the directory is added to ``sys.path`` directly).
    """
    client = fl._get_postprocessing_client()
    if client is None:
        return {}

    gauge_dates = {}
    # Map prediction_mode to API horizon values
    horizons = []
    if prediction_mode in ("PENTAD", "BOTH"):
        horizons.append("pentad")
    if prediction_mode in ("DECAD", "BOTH"):
        horizons.append("decade")

    page_size = 10000  # match setup_library pagination convention

    for horizon in horizons:
        # Use a 2-year lookback to bound the query; any gauge without
        # forecasts in the last 2 years is effectively a new gauge.
        two_years_ago = (dt.date.today() - dt.timedelta(days=730)).isoformat()

        # Paginate to avoid silent truncation on large deployments
        skip = 0
        while True:
            df = client.read_lr_forecasts(
                horizon=horizon,
                start_date=two_years_ago,
                skip=skip,
                limit=page_size,
            )
            if df.empty or "date" not in df.columns or "code" not in df.columns:
                break

            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["code"] = df["code"].astype(str)
            for code, group in df.groupby("code"):
                max_date = group["date"].max()
                if pd.notna(max_date) and (
                    code not in gauge_dates
                    or max_date.date() > gauge_dates[code]
                ):
                    gauge_dates[code] = max_date.date()

            if len(df) < page_size:
                break
            skip += page_size

    return gauge_dates
```

**Refactored `_get_last_dates_from_csv(prediction_mode)`**: Extract the
existing CSV logic into this helper, fixing the hardcoded filenames to use
env vars with fallback defaults:

```python
def _get_last_dates_from_csv(prediction_mode):
    """Read last LR forecast dates per gauge from CSV files (fallback)."""
    intermediate_path = os.getenv("ieasyforecast_intermediate_data_path", "")
    gauge_dates = {}

    if prediction_mode in ("PENTAD", "BOTH"):
        pentad_filename = os.getenv(
            "ieasyforecast_analysis_pentad_file", "forecast_pentad_linreg.csv"
        )
        pentad_file = os.path.join(intermediate_path, pentad_filename)
        # ... existing CSV read logic (unchanged) ...

    if prediction_mode in ("DECAD", "BOTH"):
        decad_filename = os.getenv(
            "ieasyforecast_analysis_decad_file", "forecast_decad_linreg.csv"
        )
        decad_file = os.path.join(intermediate_path, decad_filename)
        # ... existing CSV read logic (unchanged) ...

    return gauge_dates
```

**Constraints**:
- Do NOT change the function signature of `get_last_forecast_dates_per_gauge()`
- Do NOT change `get_hindcast_start_date_from_output()` or any other caller
- Do NOT modify any code outside the three functions listed above
- The API query uses `start_date` (2-year lookback) and paginated reads
  with `page_size=10000` (matching `setup_library._read_lr_forecasts_from_api`).
  This avoids unbounded queries and silent truncation on large deployments.
- The mapping `"DECAD"` → `"decade"` is necessary because the API uses
  `HorizonType` enum values (`"decade"`) while the LR module uses
  `prediction_mode` values (`"DECAD"`)

**Acceptance criteria**:
- When the API is available and has LR forecasts, the function returns
  dates from the API without touching CSV files
- When the API is unavailable or returns no data, the function falls back
  to CSV and reads from the env-var-named files
- When neither API nor CSV has data, returns empty dict (same as before)
- No changes to function signature or return type

---

### Phase 3: Tests

**Goal**: Test API-first auto-detect with API mock, CSV fallback with
env-var filenames, and the interaction between them.

**Files allowed to modify**:
- `apps/linear_regression/test/test_hindcast_autodetect.py` (new file)

**Depends on**: Phase 2

**Tests**:

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | API returns LR forecast records for 3 gauges | Returns max date per gauge from API; CSV never read |
| 2 | API returns records for pentad only; mode=BOTH | Returns pentad dates (decade returns empty from API) |
| 3 | API raises exception (e.g., connection error) | Falls back to CSV; returns dates from CSV files |
| 4 | API returns empty DataFrame | Falls back to CSV; returns dates from CSV files |
| 5 | `_get_postprocessing_client()` returns None (API not configured) | Falls back to CSV; distinct from exception (Test 3) and empty DF (Test 4) |
| 6 | API unavailable, CSV with env-var custom filename exists | Returns dates from the env-var-named file |
| 7 | API unavailable, CSV env var not set, default CSV exists | Returns dates from default file (backward compat) |
| 8 | API unavailable, CSV env-var-named file does not exist | Returns empty dict without raising |
| 9 | API returns data for some gauges, CSV has additional gauges | API result is used (CSV not consulted when API succeeds) |
| 10 | API returns records; mode=DECAD only | Returns decade dates only; pentad API not queried |
| 11 | CSV fallback with `ieasyforecast_analysis_decad_file` env var | Returns dates from env-var-named decad file (covers both env vars) |

**Test patterns**:
- Mock `fl._get_postprocessing_client()` to return a fake client with
  controlled `read_lr_forecasts()` responses (use `unittest.mock.MagicMock`)
- Use `tmp_path` + `monkeypatch.setenv("ieasyforecast_intermediate_data_path", ...)`
  for CSV fallback tests
- Use `monkeypatch.setenv("ieasyforecast_analysis_pentad_file", ...)` for
  env-var filename tests
- No real API calls or model runs

**Note**: Tests 5–7 for the CSV path are distinct from the existing tests
in `test_edge_cases.py` and `test_integration_hindcast.py`, which all use
the hardcoded default filenames and don't exercise env-var resolution.

**Acceptance criteria**:
- All 11 tests pass with `SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression`
- Zero unexpected skips
- Full existing LR test suite still passes

---

## Acceptance Criteria (overall)

- [ ] `daily_linreg_maintenance.sh` docker run block includes
      `-e SAPPHIRE_SYNC_MODE=maintenance`
- [ ] `LinRegMaintenance.run()` in `pipeline_docker.py` includes
      `"SAPPHIRE_SYNC_MODE=maintenance"` in environment list
- [ ] `get_last_forecast_dates_per_gauge()` queries postprocessing API first
- [ ] Falls back to CSV when API is unavailable or returns no data
- [ ] CSV fallback reads filenames from env vars with hardcoded defaults
- [ ] All 11 new tests pass with zero skips
- [ ] Full LR test suite passes:
      `SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression`
- [ ] No changes to `sapphire/services/` (ownership boundary respected)

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Paginated API query is slow for large deployments | Low — uses `page_size=10000` with pagination loop, matching `setup_library` convention; each page is a bounded DB query | Monitor query time in logs; reduce lookback window if needed |
| `client.read_lr_forecasts()` returns records without `date` or `code` columns | Very low — schema is fixed | Defensive check: `if df.empty or "date" not in df.columns` |
| `_get_postprocessing_client()` returns None when API is not configured | Expected — this is the CSV fallback trigger | Handled in `_get_last_dates_from_api`: returns `{}` if client is None |
| Mapping `"DECAD"` → `"decade"` introduces a string mismatch if the API enum changes | Very low — the enum is stable and tested | Use the string `"decade"` explicitly, matching `HorizonType` enum |
| `SAPPHIRE_SYNC_MODE=maintenance` causes unexpected pentad/decad writes on servers already running without it | Low — the writes use upsert semantics (Python-level read-then-write with DB unique constraint), so existing records are updated not duplicated | The 90-day window bounds the blast radius; for full historical backfill use `SAPPHIRE_SYNC_MODE=initial` |

---

## Out of Scope

- Adding `SAPPHIRE_SYNC_MODE=maintenance` to `daily_ml_maintenance.sh` and
  `MLMaintenance` in `pipeline_docker.py`. Note: this is **not** the same bug
  — the ML container never calls `_write_runoff_to_api()` (it does not write
  pentad/decade runoff), so `SAPPHIRE_SYNC_MODE` has no effect there. Listed
  here only for completeness, not as a required fix.
- Adding a dedicated `GET /lr-forecast/last-date/` aggregation endpoint to
  the postprocessing service (would be cleaner but requires service change;
  coordinate with colleague if query performance becomes an issue)
- Backfilling pentad/decad runoff records that were lost due to Bug 1
  (operational cleanup — re-run maintenance after deploying the fix)
- Per-gauge date ranges in the hindcast loop (the TODO at lines 473–487 in
  `get_hindcast_start_date_from_output` — separate optimization)

---

## Related Issues

- **LR-005**: No missing LR forecasts for KGH (archived) — context for why
  auto-detect is on the critical path for the KGHM deployment
- `daily_preprunoff_maintenance.sh` line 125 / `PrepRunoffMaintenance`
  line 1716: reference implementations for `SAPPHIRE_SYNC_MODE=maintenance`
- `daily_gateway_maintenance.sh` line 111 / `GatewayMaintenance` line 1679:
  second reference implementations
- **LR-007**: Silent API write failures — both LR-006 and LR-007 modify
  `bin/daily_linreg_maintenance.sh`. Implement LR-006 P1 first (sync mode
  line ~130) before LR-007 P4 (warning improvement ~line 148)
- **LR-008**: LR pentad horizon offset — recommended execution order:
  LR-006 → LR-007 → LR-008

---

## Source

Identified during maintenance script review on 2026-03-25. Root cause
confirmed by:
- Comparing `daily_linreg_maintenance.sh` and `LinRegMaintenance` against
  the preprunoff/gateway equivalents
- Tracing `_write_runoff_to_api()` and confirming the LR container is the
  sole writer for `horizon_type="pentad"` and `"decade"` in the
  preprocessing `runoffs` table
- Verifying that `get_last_forecast_dates_per_gauge()` has no API path and
  hardcodes CSV filenames that don't match non-default deployments
- Confirming the postprocessing API stores LR forecasts queryable via
  `client.read_lr_forecasts(horizon=..., start_date=..., limit=...)`

---

## Dependency Graph

**Cross-plan execution order**: LR-006 → LR-007 → LR-008.

```json
{
  "phases": {
    "phase_1": {
      "title": "Add SAPPHIRE_SYNC_MODE=maintenance and extend maintenance window to 90 days",
      "files": [
        "bin/daily_linreg_maintenance.sh",
        "apps/pipeline/pipeline_docker.py",
        "apps/iEasyHydroForecast/forecast_library.py"
      ],
      "depends_on": [],
      "parallel_with": ["phase_2"]
    },
    "phase_2": {
      "title": "API-first hindcast auto-detect with CSV fallback",
      "files": [
        "apps/linear_regression/linear_regression.py"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    },
    "phase_3": {
      "title": "Tests for API auto-detect and CSV fallback",
      "files": [
        "apps/linear_regression/test/test_hindcast_autodetect.py"
      ],
      "depends_on": ["phase_2"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": true,
      "agents": [
        {
          "id": "agent_sync_mode",
          "phases": ["phase_1"],
          "reason": "Additive env-var insertion in two files — independent of Python change"
        },
        {
          "id": "agent_autodetect",
          "phases": ["phase_2"],
          "reason": "Refactor get_last_forecast_dates_per_gauge() to API-first with CSV fallback"
        }
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {
          "id": "agent_tests",
          "phases": ["phase_3"],
          "reason": "Tests for API and CSV paths — depends on phase_2 code being in place"
        }
      ]
    }
  ]
}
```
