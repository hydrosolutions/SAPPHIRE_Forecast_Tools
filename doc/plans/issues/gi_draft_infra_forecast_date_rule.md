# INFRA-004: Enforce Forecast Date Rule — eliminate scattered `date.today()` calls

**Status**: Draft
**Module**: infra (cross-module)
**Priority**: High
**Labels**: `bug`, `testing`, `cross-module`, `date-handling`

---

## Summary

Replace scattered `date.today()` / `datetime.now()` calls in business logic with a
single forecast date captured at the pipeline entry point and passed as a parameter
to all downstream functions. Fix four latent year-boundary bugs in `tag_library.py`.

## Context

SAPPHIRE runs operational runoff forecasts daily. The pipeline is date-sensitive —
pentad/decad boundaries, year transitions, and leap years all affect which forecast
period is computed. The codebase currently has ~80 scattered `date.today()` calls in
business logic across multiple modules.

The `long_term_forecasting` module already implements the correct pattern:
`initialize_today()` captures the date once and `get_today()` returns it everywhere.
The rest of the codebase does not follow this pattern.

The CLAUDE.md "Forecast Date Rule" (added 2026-02-16) establishes this as a project
convention going forward. This issue retrofits existing code.

## Problem

Three classes of bugs arise from scattered `date.today()` calls:

1. **Clock-tick bug**: If the pipeline runs across midnight (common for long-running
   nightly jobs), different modules see different dates. `setup_library.define_run_dates()`
   might capture Jan 5, but `preprocessing_runoff` functions that independently call
   `dt.date.today()` two minutes later get Jan 6.

2. **Import-time default argument bug** (latent, confirmed): Four functions in
   `tag_library.py` use `year=dt.datetime.now().year` as a default argument. Python
   evaluates default arguments once at import time. If the module is imported before
   midnight on Dec 31 and the function is called after midnight on Jan 1, `year` is
   silently wrong. The same applies to long-running processes across year boundaries.

3. **Hindcast/backtest incompatibility**: When re-running forecasts for historical
   dates (e.g., via `reset_forecast_run_date`), scattered `date.today()` calls return
   the actual current date instead of the target forecast date, producing incorrect
   results.

## Desired Outcome

- All four `tag_library.py` default argument bugs are fixed
- `setup_library.define_run_dates()` is the single authoritative source of the
  forecast date for the main pipeline
- Functions in `preprocessing_runoff`, `postprocessing_forecasts`,
  `forecast_dashboard`, and `forecast_library` that need "the current year" or "today"
  receive it as a parameter
- Tests for date-sensitive logic exercise explicit boundary dates (Dec 31, Jan 1,
  Feb 29) without relying on `date.today()`
- Logging/timestamp uses of `datetime.now()` are unchanged (these correctly reflect
  wall-clock time)

---

## Technical Analysis

### Category 1: Import-time default arguments (BUG — fix immediately)

These four functions evaluate `dt.datetime.now().year` at import time, not at call
time:

| Function | File | Line |
|----------|------|------|
| `get_date_for_pentad()` | `iEasyHydroForecast/tag_library.py` | 660 |
| `get_date_for_decad()` | `iEasyHydroForecast/tag_library.py` | 716 |
| `get_date_for_last_day_in_pentad()` | `iEasyHydroForecast/tag_library.py` | 775 |
| `get_date_for_last_day_in_decad()` | `iEasyHydroForecast/tag_library.py` | 833 |

**Callers that rely on the default (pass no `year` argument):**

| Caller | File | Line | Status |
|--------|------|------|--------|
| `forecast_library.py` — `get_date_for_last_day_in_pentad` | `forecast_library.py` | 1370-1371 | **Already fixed** (passes `year=_year`) |
| `forecast_library.py` — `get_date_for_last_day_in_decad` | `forecast_library.py` | 1377-1378 | **Already fixed** (passes `year=_year`) |
| `vizualization.py` — `get_date_for_pentad` (lambda) | `forecast_dashboard/src/vizualization.py` | 3808 | **Needs fix** |
| `vizualization.py` — `get_date_for_decad` (lambda) | `forecast_dashboard/src/vizualization.py` | 3828 | **Needs fix** |
| `vizualization.py` — `horizon_fn_year = get_date_for_pentad` | `forecast_dashboard/src/vizualization.py` | 5071 | **Needs fix** |
| `vizualization.py` — `horizon_fn_year = get_date_for_decad` | `forecast_dashboard/src/vizualization.py` | 5076 | **Needs fix** |

**Callers that pass `year` explicitly (safe):**
- `iEasyHydroForecast/tests/test_tag_library.py:212` — `tl.get_date_for_pentad(pentad, year)`
- `iEasyHydroForecast/tests/test_tag_library.py:220` — `tl.get_date_for_last_day_in_pentad(pentad, year)`

### Category 2: Business logic calling `date.today()` directly

**`setup_library.py:167`** — The pipeline's "capture once" point:
```python
date_end = dt.date.today()
```
This is the correct place to capture the date, but it should accept an override
parameter for hindcast mode instead of always calling `today()`.

**`preprocessing_runoff/src/src.py`** — 5 functions use
`current_year = dt.date.today().year`:
- Line 3626: `generate_hydrograph_pentad()`
- Line 3751: `generate_hydrograph_decad()`
- Line 4098: `write_hydrograph_to_csv()`
- Line 4895: `merge_forecast_into_hydrograph()`
- Line 4972: `write_forecast_hydrograph_to_csv()`

**`preprocessing_station_forcing/src/src.py`** — 3 calls:
- Line 242: `current_year_data = data_df[data_df['year'] == dt.date.today().year]`
- Line 243: `last_year_data = data_df[data_df['year'] == (dt.date.today().year - 1)]`
- Line 255: `norm_data['year'] = dt.date.today().year`

**`forecast_dashboard`** — 3 calls:
- `vizualization.py:1472,1553`: `year = dt.datetime.now().year`
- `forecast_dashboard.py:122`: `today = dt.datetime.now()`

### Category 3: Logging / timestamps (NO CHANGE needed)

~40% of all calls are for log messages, file naming, or performance timers. These
correctly reflect wall-clock time and should not be changed:
- `logger.debug(f"Script started at {dt.datetime.now()}.")`
- `docker_logs_file_path = f"...log_{datetime.now().strftime(...)}.txt"`
- `timer_start = datetime.datetime.now()`

### Category 4: Test files using `date.today()`

Several test files use `today = date.today()` to build relative test data. These
work because tests run quickly (no midnight crossing), but they never exercise
specific boundary dates:
- `test_integration_maintenance_gaps.py`: 19 uses of `date.today()`
- `test_hydrograph_generation.py`: 20 uses of `dt.date.today().year`
- `test_api_write.py`: 3 uses
- `test_spot_check.py`: 2 uses

### Reference implementation

`long_term_forecasting/__init__.py` already follows the correct pattern:
```python
today = None

def initialize_today(today_override=None):
    global today
    today = pd.to_datetime(today_override) if today_override else pd.Timestamp.now()
    today = today.normalize()
    return today

today = initialize_today()

def get_today():
    return today
```

---

## Implementation Plan

### Approach

Phase the work into four steps of increasing scope. Phase 1 fixes confirmed bugs
and can be done immediately. Phase 2 establishes the pattern at entry points.
Phase 3 propagates to downstream functions module by module. Phase 4 hardens
tests.

Each phase is a separate branch/PR. Phase 1 is a standalone bug fix.

### Phase 1: Fix `tag_library.py` default arguments (bug fix)

**Branch**: `fix_iEHF_tag_library_import_time_defaults`

**Files to modify:**

| File | Changes |
|------|---------|
| `iEasyHydroForecast/tag_library.py:660,716,775,833` | Replace `year=dt.datetime.now().year` with `year: int` (required parameter) |
| `forecast_dashboard/src/vizualization.py:3808,3828,5071,5076` | Pass `year` explicitly |
| `iEasyHydroForecast/tests/test_tag_library.py` | Already passes `year` explicitly — add boundary tests |

**Note**: `forecast_library.py` callers are already fixed (pass `year=_year`). No
changes needed there.

**Steps:**

- [ ] **1a.** Change the four `tag_library.py` function signatures from
  `year=dt.datetime.now().year` to `year: int` (required, no default)
- [ ] **1b.** Update `forecast_dashboard/src/vizualization.py` — 4 call sites at
  lines 3808, 3828, 5071, 5076. The lambda at 3808 becomes
  `lambda horizon_value: tl.get_date_for_pentad(horizon_value, year=current_year)`
  where `current_year` must be derived from the calling context.
- [ ] **1c.** Verify `forecast_library.py` callers still work (they already pass
  `year` — just run tests to confirm).
- [ ] **1d.** Add boundary-date tests to `test_tag_library.py`:
  - Year transition: pentad 73 / decad 36 with year=2025 -> Dec 26/Dec 21 of 2025
  - Leap year: pentad 12 with year=2024 -> should account for Feb 29
  - Cross-year call: call with year=2025, then year=2026 — both correct
    (regression test for the import-time bug)

**Code example — Phase 1a:**

```python
# BEFORE (tag_library.py:660)
def get_date_for_pentad(pentad_in_year, year=dt.datetime.now().year):

# AFTER
def get_date_for_pentad(pentad_in_year: int, year: int) -> str | None:
```

**Code example — Phase 1b (vizualization.py:3808):**

```python
# BEFORE
add_date_column = lambda horizon_value: tl.get_date_for_pentad(horizon_value)

# AFTER — derive year from the data context
add_date_column = lambda horizon_value: tl.get_date_for_pentad(
    horizon_value, year=current_year
)
```

Note: Investigate how `current_year` is available in the calling context of each of
the 4 call sites. The dashboard likely has it available from the data being
displayed.

### Phase 2: Add `forecast_date` parameter to `setup_library.define_run_dates()`

**Branch**: `develop_iEHF_setup_library_today_override`

**Files to modify:**

| File | Changes |
|------|---------|
| `iEasyHydroForecast/setup_library.py:141-185` | Add optional `today_override: date \| None = None` parameter |
| `linear_regression/linear_regression.py` | Pass through from CLI args if in hindcast mode |

**Steps:**

- [ ] **2a.** Add `today_override` parameter to `define_run_dates()`:
  ```python
  def define_run_dates(prediction_mode='BOTH', today_override: date | None = None):
      ...
      date_end = today_override or dt.date.today()
  ```
- [ ] **2b.** Update `linear_regression.py` callers to pass through the override
  when in hindcast mode
- [ ] **2c.** Add tests for `define_run_dates()` with explicit dates including
  Dec 31 and Jan 1

### Phase 3: Propagate to downstream modules (incremental, per module)

Each module's functions that use `current_year = dt.date.today().year` should receive
the year or forecast_date as a parameter. This can be done one module at a time, each
as a separate PR:

**Priority order:**

1. `preprocessing_runoff/src/src.py` (5 calls) — highest risk, hydrograph generation
   is date-sensitive
2. `preprocessing_station_forcing/src/src.py` (3 calls)
3. `forecast_dashboard` (3 calls) — lower risk, display only

For each module:
- [ ] Add `forecast_date` or `year` parameter to affected functions
- [ ] Update all callers to pass the value from the entry point
- [ ] Add boundary-date tests (Dec 31 -> Jan 1, Feb 29)
- [ ] Verify existing tests still pass

### Phase 4: Harden test suite against date sensitivity

- [ ] **4a.** In `test_integration_maintenance_gaps.py` and
  `test_hydrograph_generation.py`, replace `today = date.today()` with explicit
  fixed dates that include boundary cases
- [ ] **4b.** Add a parametrized boundary-date test class that exercises key functions
  with:
  - `date(2025, 12, 31)` — last day of year
  - `date(2026, 1, 1)` — first day of year
  - `date(2024, 2, 29)` — leap year
  - `date(2025, 2, 28)` — non-leap year end of Feb
  - `date(2025, 6, 30)` — end of month (pentad boundary)

---

## Testing

### Test Cases

**Phase 1 — tag_library bug fix:**
- [ ] `get_date_for_pentad(1, 2025)` returns `'2025-01-01'`
- [ ] `get_date_for_pentad(1, 2026)` returns `'2026-01-01'` (verifies no stale year)
- [ ] `get_date_for_last_day_in_pentad(72, 2024)` returns `'2024-12-31'` (leap year)
- [ ] `get_date_for_last_day_in_pentad(72, 2025)` returns `'2025-12-31'` (non-leap)
- [ ] `get_date_for_decad(1, 2026)` returns `'2026-01-01'`
- [ ] `get_date_for_last_day_in_decad(36, 2024)` returns `'2024-12-31'`
- [ ] All four functions raise `TypeError` when called without `year` (no silent
  default)

**Phase 2 — setup_library:**
- [ ] `define_run_dates(today_override=date(2025, 12, 31))` returns correct dates
  for year-end
- [ ] `define_run_dates(today_override=date(2024, 2, 29))` works for leap year
- [ ] `define_run_dates()` with no override behaves as before (backward compatible)

**Phase 3 — per-module:**
- [ ] Each refactored function produces identical output when passed `date.today()`
  explicitly vs. the old implicit call (regression)
- [ ] Each refactored function produces correct output for Dec 31, Jan 1, and Feb 29

### Testing Commands

```bash
# Phase 1 — tag_library + forecast_dashboard
cd apps
SAPPHIRE_TEST_ENV=True pytest iEasyHydroForecast/tests/test_tag_library.py -v
SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard

# Phase 2 — setup_library
cd apps
SAPPHIRE_TEST_ENV=True pytest iEasyHydroForecast/tests/ -v -k "run_date"

# Phase 3 — per module
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_station_forcing
SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard

# Full suite after all phases
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh
```

### Manual Verification

After Phase 1, verify that `tag_library.py` functions no longer have
`datetime.now()` in their signatures:
```bash
grep -n "datetime.now().year" apps/iEasyHydroForecast/tag_library.py
# Expected: no output
```

---

## Effort Estimates

| Phase | Scope | Effort | Risk |
|-------|-------|--------|------|
| Phase 1 | 4 function signatures + 4 dashboard call sites + tests | ~3 hours | Low (isolated bug fix) |
| Phase 2 | 1 function signature + 1 caller + tests | ~2 hours | Low |
| Phase 3 | 11 call sites across 3 modules + tests per module | ~6 hours (2h/module) | Medium (threading params through call chains) |
| Phase 4 | Test refactoring (no production code) | ~4 hours | Low |

**Total**: ~15 hours across 4 PRs

---

## Out of Scope

- **Logging/timestamp calls**: `datetime.now()` used in log messages, file naming,
  and performance timers is correct and should not be changed
- **`long_term_forecasting` module**: Already follows the correct pattern — no
  changes needed
- **`machine_learning`**: Colleague's responsibility — coordinate separately
- **`machine_learning_monthly`**: Deprecated module, not worth refactoring
- **`backend/`**: Legacy module being phased out
- **Global singleton approach**: We considered a module-level `today` singleton (like
  `long_term_forecasting`) for the main pipeline but rejected it in favor of explicit
  parameter passing, which is safer for concurrent/parallel execution and clearer for
  testing

## Dependencies

- None — this is self-contained. Phase 1 is a standalone bug fix.
- Future issues that add new date-sensitive functions should follow the Forecast Date
  Rule in CLAUDE.md.

## Acceptance Criteria

- [ ] Zero `datetime.now().year` default arguments remain in function signatures
  anywhere in the codebase
- [ ] `setup_library.define_run_dates()` accepts a `today_override` parameter
- [ ] All functions in `preprocessing_runoff/src/src.py` that used
  `dt.date.today().year` receive `year` as a parameter
- [ ] Boundary-date tests exist for Dec 31, Jan 1, Feb 29 in at least `tag_library`,
  `setup_library`, and `preprocessing_runoff`
- [ ] All existing tests pass with zero new skips
- [ ] `grep -rn "date.today()" apps/ --include="*.py" | grep -v test | grep -v ".venv" | grep -v log | grep -v "machine_learning" | grep -v backend | grep -v long_term_forecasting`
  returns only logging/timestamp uses

---

## References

- CLAUDE.md "The Forecast Date Rule" section (added 2026-02-16)
- `long_term_forecasting/__init__.py` — reference implementation of
  `initialize_today()` / `get_today()`
- `LR-001` (completed) — previous fix for leap year date handling in
  linear_regression
- Planning docs: `doc/plans/module_issues.md`

---

*Last updated: 2026-02-27 — Verified line numbers against current codebase.
Noted forecast_library.py callers already fixed. Updated dashboard call sites
(3808, 3828, 5071, 5076).*
