# FD-009: Pass explicit forecast date to containers in dashboard retrigger flows

**Status**: Review
**Module**: forecast_dashboard, linear_regression, postprocessing_forecasts
**Priority**: Medium
**Labels**: `forecast_dashboard`, `linear_regression`, `postprocessing`, `docker`

---

## Summary

The dashboard "Save Changes" and "Trigger Forecasts" flows run linreg and postprocessing containers that hardcode `date.today()` as the forecast date. On non-boundary days (any day not in {5, 10, 15, 20, 25, last-of-month}), both modules skip all computation. The dashboard knows which pentad the user is editing but has no way to communicate the target date to the containers.

Add a `SAPPHIRE_FORECAST_DATE` environment variable to `linear_regression` and `postprocessing_forecasts`. When set, use that date instead of `date.today()` and skip the boundary-day guard. Update the dashboard to compute and pass the correct boundary date.

## Context

Discovered during FD-007 server testing. The `sapphire-rerun` container (now removed) was intended to solve this by backdating `last_successful_run_file`, but linreg never read that file in operational mode — so it was always broken on non-boundary days.

**Current behavior**: User edits visibility checkboxes on the regression tab, clicks "Save Changes". Visibility changes are saved to DB (via `lr-visibility` API). Then linreg + postprocessing containers run but skip computation because today is not a boundary day. The user sees no updated forecasts.

**Desired behavior**: The containers recompute forecasts for the pentad the user is editing, regardless of what day it is.

## Design Decision: Env Var vs Hindcast CLI

Linreg already has a hindcast mechanism (`--hindcast --start-date X --end-date X`) that runs for arbitrary past dates. We considered reusing it for the dashboard retrigger but chose the env var approach because:

1. **Docker CMD doesn't support date args.** The Dockerfile CMD is hardcoded to `--hindcast` (maintenance) or no args (operational). Passing `--start-date` requires CMD override or expansion.
2. **Hindcast has a date-snapping hazard.** Non-boundary dates are silently snapped forward to the next boundary day, which may overshoot `--end-date` and produce `sys.exit(0)` with zero output. The operational path doesn't snap.
3. **The loop body is identical.** Both paths enter the same `while` loop with the same write functions. The env var on the operational path gives equivalent computation with less machinery.
4. **No conflict.** The env var only applies in the operational `else` branch (line 725). Hindcast is a separate `if` branch (line 688) reached only with `--hindcast` flag (maintenance mode). The two mechanisms never interact.

---

## Problem

### Linear regression (`linear_regression.py`)

Operational mode (line 728): `forecast_date = dt.date.today()`. No env var override exists.

The boundary guard (line 802–812):
```python
forecast_flags = sl.ForecastFlags.from_forecast_date_get_flags(current_day)  # line 802
...
if run_pentad and forecast_flags.pentad:  # line 812 — False on non-boundary days
```

No mechanism to bypass this from outside the process.

### Postprocessing (`postprocessing_operational.py`)

Same pattern (line 189): `today = dt.date.today()`. No env var override.

Two boundary guards, one per forecast horizon:

**Pentad guard** (line 191–192):
```python
if prediction_mode in ["PENTAD", "BOTH", "ALL"]:
    if not is_pentad_boundary(today):
        logger.info("Skipping pentad postprocessing: %s is not a pentad boundary day ...")
```

**Decad guard** (line 201–202):
```python
if prediction_mode in ["DECAD", "BOTH", "ALL"]:
    if not is_decad_boundary(today):
        logger.info("Skipping decad postprocessing: %s is not a decad boundary day ...")
```

Both `is_pentad_boundary` and `is_decad_boundary` are defined locally in the file (lines 54–63). `is_pentad_boundary` checks `d.day in (5, 10, 15, 20, 25, last_day)`. `is_decad_boundary` checks `d.day in (10, 20, last_day)`.

**No risk from dual guards**: The dashboard always passes single-mode via `SAPPHIRE_PREDICTION_MODE={horizon.upper()}` (either PENTAD or DECAD, never BOTH/ALL). The computed boundary date matches the mode: pentad boundaries for PENTAD mode, decad boundaries for DECAD mode. Each guard passes for its respective mode.

### Dashboard

**Save Changes** (`save_to_database`): The dashboard knows the exact pentad from `horizon_value` (pentad_in_year, 1-72) and can compute the boundary date. But the container environment (line 3829) contains only `IN_DOCKER_CONTAINER`, `SAPPHIRE_PREDICTION_MODE`, and `ieasyhydroforecast_env_file_path` — no date.

**Trigger Forecasts** (`run_docker_pipeline`): Same gap — no date is passed. This flow runs the full pipeline and is also non-functional on non-boundary days.

---

## Technical Analysis

### Date computation in the dashboard

#### Boundary date tables

**Pentad** (pentad_in_year 1–72, 6 per month):

| pentad_in_month | days covered | boundary day |
|-----------------|-------------|--------------|
| 1 | 1-5 | 5 |
| 2 | 6-10 | 10 |
| 3 | 11-15 | 15 |
| 4 | 16-20 | 20 |
| 5 | 21-25 | 25 |
| 6 | 26-last | last day of month |

**Decad** (decad_in_year 1–36, 3 per month):

| decad_in_month | days covered | boundary day |
|----------------|-------------|--------------|
| 1 | 1-10 | 10 |
| 2 | 11-20 | 20 |
| 3 | 21-last | last day of month |

#### Save Changes — boundary date from `horizon_value`

`horizon_value` and `horizon` (string `"pentad"` or `"decad"`) are both available in the `save_to_database` closure. The existing code already computes `periods_per_month` (6 for pentad, 3 for decad) at line 3763. Compute boundary date:

```python
year = dt.date.today().year

if horizon == "pentad":
    periods_per_month = 6
    month = (horizon_value - 1) // periods_per_month + 1
    period_in_month = (horizon_value - 1) % periods_per_month + 1
    if period_in_month == periods_per_month:
        boundary_day = calendar.monthrange(year, month)[1]
    else:
        boundary_day = period_in_month * 5
else:  # decad
    periods_per_month = 3
    month = (horizon_value - 1) // periods_per_month + 1
    period_in_month = (horizon_value - 1) % periods_per_month + 1
    if period_in_month == periods_per_month:
        boundary_day = calendar.monthrange(year, month)[1]
    else:
        boundary_day = period_in_month * 10

forecast_date = dt.date(year, month, boundary_day)

# Year guard: if the computed date is in the future, the user is viewing
# the previous year's data (e.g. pentad 72 = Dec 31, viewed in January).
if forecast_date > dt.date.today():
    year -= 1
    # Recompute boundary_day for last-of-month periods (Feb 28 vs 29)
    if period_in_month == periods_per_month:
        boundary_day = calendar.monthrange(year, month)[1]
    forecast_date = dt.date(year, month, boundary_day)
```

#### Trigger Forecasts — most recent boundary date from today

`run_docker_pipeline` has no `horizon_value` (its closure only captures `horizon`). Compute the most recent boundary date by walking backward from today:

```python
def get_previous_boundary_date(today, horizon):
    """Return the most recent boundary date <= today for the given horizon."""
    if horizon == "pentad":
        boundaries = [5, 10, 15, 20, 25]
    else:  # decad
        boundaries = [10, 20]
    last_of_month = calendar.monthrange(today.year, today.month)[1]
    boundaries.append(last_of_month)

    # Check current month boundaries in descending order
    for b in sorted(boundaries, reverse=True):
        if today.day >= b:
            return dt.date(today.year, today.month, b)

    # No boundary reached yet this month — use last day of previous month
    first_of_month = dt.date(today.year, today.month, 1)
    last_day_prev = first_of_month - dt.timedelta(days=1)
    return last_day_prev  # last day of previous month is always a boundary
```

This function should be defined as a module-level helper in `vizualization.py` (or in a shared utility if preferred). It is small and self-contained.

### Linreg changes

Add to `main()` at line 728 (the operational `else` branch), replacing `forecast_date = dt.date.today()`:
```python
_env_date = os.getenv('SAPPHIRE_FORECAST_DATE', '').strip()
if _env_date:
    try:
        forecast_date = dt.datetime.strptime(_env_date, '%Y-%m-%d').date()
        logger.info("Using explicit forecast date from SAPPHIRE_FORECAST_DATE: %s", forecast_date)
    except ValueError:
        logger.error(
            "Invalid SAPPHIRE_FORECAST_DATE=%r — expected YYYY-MM-DD. Falling back to today.",
            _env_date,
        )
        forecast_date = dt.date.today()
else:
    forecast_date = dt.date.today()
```

**Error handling**: `.strip()` guards against whitespace-only values. `try/except ValueError` catches empty strings that passed the truthy check, invalid dates like `2026-02-30`, and malformed formats. On any parse failure, the container logs a clear error and falls back to `date.today()` — no crash, existing behavior preserved.

**Variable flow**: `forecast_date` (line 728) → `date_end` (line 729) → `current_day` (line 795, `current_day = forecast_date`) → all downstream computation. After line 795, only `current_day` is used in the loop body. No hidden `date.today()` calls after line 728 in the operational path.

**Pre-loop code is safe**: The pre-loop `ForecastFlags` at line 733 uses `forecast_date`, but the result is immediately overwritten by lines 734–735 (`forecast_flags.pentad = run_pentad`) and then again inside `get_pentadal_and_decadal_data` (line 741). The pre-loop writes (`write_pentad_hydrograph_data`, `write_pentad_time_series_data`, lines 762–775) do not use `forecast_date` at all — they operate on the full historical DataFrame. No side effects from the override.

**Boundary guard**: The guard at line 802 calls `ForecastFlags.from_forecast_date_get_flags(current_day)`, and line 812 checks `if run_pentad and forecast_flags.pentad:`. If `forecast_date` is a boundary day (which it will be — the dashboard computes it that way), the flags will be `True` and the guard passes naturally. No bypass needed.

**No Dockerfile change needed**: `os.getenv` reads the env var directly from the container environment. The Docker CMD stays as-is.

**Precedence**: The env var only applies in the operational code path (the `else` branch at line 725). The hindcast path (`if args.hindcast:`, line 688) is a separate branch that uses CLI args `--start-date`/`--end-date` and is never reached when the container runs in default (non-maintenance) mode. No conflict between the two mechanisms.

### Postprocessing changes

Same pattern — add `SAPPHIRE_FORECAST_DATE` override before line 189:
```python
_env_date = os.getenv('SAPPHIRE_FORECAST_DATE', '').strip()
if _env_date:
    try:
        today = dt.datetime.strptime(_env_date, '%Y-%m-%d').date()
        logger.info("Using explicit forecast date from SAPPHIRE_FORECAST_DATE: %s", today)
    except ValueError:
        logger.error(
            "Invalid SAPPHIRE_FORECAST_DATE=%r — expected YYYY-MM-DD. Falling back to today.",
            _env_date,
        )
        today = dt.date.today()
else:
    today = dt.date.today()
```

Since the overridden date IS a boundary day, the relevant guard passes: `is_pentad_boundary(today)` returns True when mode is PENTAD, `is_decad_boundary(today)` returns True when mode is DECAD. The dashboard always passes single-mode (never BOTH/ALL), so only one guard is evaluated per run.

### Dashboard changes

**`save_to_database`** (line 3742): Compute boundary date from `horizon_value` using the formula above. The computation should go **inside the Docker `try` block, immediately after the `environment` list is built** (after line 3833, before the volume resolution at line 3835). Append to the list:
```python
environment.append(f'SAPPHIRE_FORECAST_DATE={forecast_date.strftime("%Y-%m-%d")}')
```
This is safe because `save_to_database` only launches linreg and postprocessing (no preprunoff).

**Error handling context**: The boundary date computation uses only `horizon_value` (a widget-supplied integer, always 1–72 or 1–36) and `calendar.monthrange`. It cannot fail under normal operation. The Docker `try` block's `except docker.errors.DockerException` would not catch a hypothetical `ValueError`, but the `finally` block (line 3873) always resets the UI. A computation failure after the visibility save (Stage D) is acceptable — visibility changes are already committed, and the user can retry the container run.

**`run_docker_pipeline`** (line 4006): Compute boundary date using `get_previous_boundary_date(dt.date.today(), horizon)`. **Important: append `SAPPHIRE_FORECAST_DATE` to the environment list AFTER preprunoff has been launched** (after line 4061), before the linreg call (line 4067). This ensures preprunoff runs with today's date (it fetches the latest data) while linreg, ML models, and postprocessing receive the boundary date override.

```python
# After preprunoff completes (line 4061):
forecast_date = get_previous_boundary_date(dt.date.today(), horizon)
environment.append(f'SAPPHIRE_FORECAST_DATE={forecast_date.strftime("%Y-%m-%d")}')
# Then launch linreg (line 4067), ML models, and postprocessing
```

Note: `run_docker_pipeline` calls the **outer** `run_docker_container` (line 4156, 5-parameter module-level function), which does NOT mutate the environment list (unlike the inner 6-parameter helper at line 3618 used by `save_to_database`). The append works because `environment` is a Python list passed by reference — `.append()` modifies the object in place, visible to all subsequent callers. ML containers (line 4086) use `environment + [model-specific vars]` (list concatenation, non-mutating), so they also receive `SAPPHIRE_FORECAST_DATE` from the base list. ML models do not read this env var, so the extra variable is harmless.

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/linear_regression/linear_regression.py` | Add `SAPPHIRE_FORECAST_DATE` env var override in `main()` |
| `apps/postprocessing_forecasts/postprocessing_operational.py` | Add `SAPPHIRE_FORECAST_DATE` env var override |
| `apps/forecast_dashboard/src/vizualization.py` | Compute boundary date and add to container env in both dataflows |

### Implementation Steps

#### Phase 1: Module date override (linreg + postprocessing)

- [x] **Step 1.1**: In `linear_regression.py:main()`, add `SAPPHIRE_FORECAST_DATE` check before `forecast_date = dt.date.today()` (line 728). When set, parse it and use it as `forecast_date`. Log the override.
- [x] **Step 1.2**: In `postprocessing_operational.py:postprocessing_operational()`, add `SAPPHIRE_FORECAST_DATE` check before `today = dt.date.today()` (line 189). When set, parse it and use it. Log the override.

#### Phase 2: Dashboard date computation and passthrough

- [x] **Step 2.0**: Add `get_previous_boundary_date(today, horizon)` helper function to `vizualization.py` (module-level). Handles both pentad and decad modes.
- [x] **Step 2.1**: In `save_to_database`, compute boundary date from `horizon_value` and `horizon` (mode-aware: pentad uses `% 6` with boundaries 5/10/15/20/25/last, decad uses `% 3` with boundaries 10/20/last). Include year guard (roll back if computed date > today). Append `SAPPHIRE_FORECAST_DATE={date}` to `environment` before container launches.
- [x] **Step 2.2**: In `run_docker_pipeline`, call `get_previous_boundary_date(dt.date.today(), horizon)`. Append `SAPPHIRE_FORECAST_DATE={date}` to `environment` **after** preprunoff launch (line 4061), **before** linreg launch (line 4067). Preprunoff must not receive this env var.

#### Phase 3: Verify

- [ ] **Step 3.1**: Run on a non-boundary day (pentad mode) — Save Changes should produce updated forecasts for the selected pentad.
- [ ] **Step 3.2**: Run on a non-boundary day (decad mode) — Save Changes should produce updated forecasts for the selected decad.
- [ ] **Step 3.3**: Run on a boundary day — behavior unchanged (env var date matches today).
- [ ] **Step 3.4**: Trigger Forecasts on a non-boundary day — should recompute for the most recent boundary date.
- [ ] **Step 3.5**: Verify preprunoff does NOT receive `SAPPHIRE_FORECAST_DATE` in Trigger Forecasts flow (check container env in logs).

---

## Testing

### Automated

- [ ] Unit test for boundary date computation from pentad_in_year (all 72 values)
- [ ] Unit test for boundary date computation from decad_in_year (all 36 values)
- [ ] Unit test for year guard: pentad 72 viewed in January rolls back to previous December
- [ ] Unit test for `get_previous_boundary_date` — pentad mode (test on boundary day, day after, mid-period, first of month)
- [ ] Unit test for `get_previous_boundary_date` — decad mode (same cases)
- [ ] Unit test for `SAPPHIRE_FORECAST_DATE` parsing in linreg `main()` (mock `os.getenv`)
- [ ] Unit test for `SAPPHIRE_FORECAST_DATE` parsing in postprocessing

### Manual (server)

1. On a non-boundary day, edit visibility, Save Changes
2. Verify linreg runs for the correct pentad boundary date (check container logs)
3. Verify postprocessing runs for the same date
4. Verify updated forecast appears in the summary table (requires FD-008 or the data reload fix)

---

## Out of Scope

- Data reload mechanism (the summary table not refreshing from API — separate issue)
- Error handling in inner `run_docker_container` (FD-008)
- ML model date handling (ML models use `SAPPHIRE_PREDICTION_MODE` and `RUN_MODE`, not a specific date — they process whatever data is available)
- Hindcast mode — `SAPPHIRE_FORECAST_DATE` is for single-date retrigger, not batch catch-up

## Dependencies

- FD-007 (implemented) — env file path fix and rerun removal

## Acceptance Criteria

- [ ] `SAPPHIRE_FORECAST_DATE` env var accepted by both linreg and postprocessing
- [ ] When set to a boundary date, both modules run for that date instead of today
- [ ] When not set, existing behavior unchanged (`date.today()`)
- [ ] Dashboard Save Changes computes and passes the correct boundary date
- [ ] Dashboard Trigger Forecasts computes and passes the most recent boundary date
- [ ] Forecasts are produced on non-boundary days when triggered from dashboard
- [ ] No changes to Luigi pipeline behavior (Luigi does not set this env var)

---

## References

- `apps/linear_regression/linear_regression.py:728` — operational forecast_date assignment
- `apps/linear_regression/linear_regression.py:802` — ForecastFlags call; `:812` — pentad boundary guard
- `apps/postprocessing_forecasts/postprocessing_operational.py:189` — today assignment
- `apps/postprocessing_forecasts/postprocessing_operational.py:192` — pentad boundary guard
- `apps/postprocessing_forecasts/postprocessing_operational.py:202` — decad boundary guard
- `apps/postprocessing_forecasts/postprocessing_operational.py:54-63` — `is_pentad_boundary` / `is_decad_boundary` definitions
- `apps/forecast_dashboard/src/vizualization.py:3742` — `save_to_database`
- `apps/forecast_dashboard/src/vizualization.py:4006` — `run_docker_pipeline`
- `apps/iEasyHydroForecast/tag_library.py:672` — `get_date_for_pentad` (returns first day, not boundary)
- FD-007: `doc/plans/issues/mid_prio_gi_draft_fd_docker_dataflows_update.md`
- FD-008: `doc/plans/issues/low_prio_gi_draft_fd_inner_run_docker_error_handling.md`
