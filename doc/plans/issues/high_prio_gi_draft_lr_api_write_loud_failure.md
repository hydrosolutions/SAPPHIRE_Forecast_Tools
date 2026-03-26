# LR-007: API write failures are silent when API is enabled

**Status**: Draft
**Module**: `linear_regression`, `iEasyHydroForecast`
**Priority**: High
**Labels**: `reliability`, `api-integration`, `data-integrity`

---

## Summary

When `SAPPHIRE_API_ENABLED=true` (the default in production), API write
failures in the linear regression pipeline are silently swallowed — the
pipeline logs a warning and falls through to CSV-only output, reporting
success. This creates a dangerous asymmetry: reads fail-fast (correct),
but writes degrade silently, leaving the database under-populated while
the pipeline appears healthy.

The goal is to make API write failures **loud**: when the API is the
primary interface and a write fails, the pipeline must clearly signal
failure rather than silently falling back to CSV.

---

## Root Cause

The non-blocking write pattern in `forecast_library.py` was designed for
the CSV-primary era, where CSV was the authoritative store and the API was
an optional secondary target. Now that the API is becoming the primary
store, the error handling has not been updated to match.

The current write pattern used by all six LR write functions follows this
structure:

```python
try:
    _write_lr_forecast_to_api(data, "pentad")
except Exception as e:
    logger.error(f"API write failed: {e}")
    # Falls through to CSV write — no failure signal
```

The `_write_*_to_api` helpers have three gates:

1. `SAPPHIRE_API_AVAILABLE` — library import check
2. `SAPPHIRE_API_ENABLED` — env var
3. Health check — `client.readiness_check()`

If any gate fails, the helper returns early without writing. Return types
differ by helper:

| Helper | Returns on success | Returns on skip/failure |
|--------|--------------------|------------------------|
| `_write_lr_forecast_to_api()` | `True` | `False` |
| `_write_hydrograph_to_api()` | `True` | `False` |
| `_write_runoff_to_api()` | `pd.DataFrame` (data written) | `None` |

The public wrapper functions ignore these return values entirely.

---

## Impact

A failed API write is undetectable from pipeline logs unless the operator
explicitly searches for the buried `logger.error` line. The DB remains
under-populated for the affected forecast cycle. Downstream consumers
(postprocessing, skill metrics, dashboards) silently miss data that
appears to be present based on pipeline exit codes.

**Blast radius**: All six write functions are called exclusively from
`linear_regression.py`. No other module calls them. Changes are safe to
scope to LR only.

The six write wrapper functions in `forecast_library.py`:

- `write_pentad_hydrograph_data()` (line 4452)
- `write_decad_hydrograph_data()` (line 4740)
- `write_pentad_time_series_data()` (line 5437)
- `write_decad_time_series_data()` (line 5534)
- `write_linreg_pentad_forecast_data()` (line 3646)
- `write_linreg_decad_forecast_data()` (line 3997)

---

## Design Decision

Three options were considered:

**Option A** — Change the `_write_*_to_api` helpers to raise on failure
when API is enabled. Pro: single point of change. Con: affects any future
caller, not only LR.

**Option B** — Make the public wrapper functions return a `bool` and have
`linear_regression.py` check it. Pro: non-breaking change to the helpers;
callers opt into loud failure. Con: requires changes in both files.

**Option C** — Add a `strict_api=True` parameter to the public wrapper
functions. Pro: backward compatible. Con: more complex function signatures.

**Option D** — Route the six wrapper functions' `except` blocks through
the existing `_handle_api_write_error()` helper (`forecast_library.py:94`),
which already supports `SAPPHIRE_API_FAILURE_MODE` (warn/fail/ignore) and is
used by postprocessing's `file_writer.py` (7 call sites). Currently the six
LR wrapper functions bypass this mechanism entirely with hand-rolled
`logger.error()` calls. Pro: single mechanism, consistent with postprocessing,
`SAPPHIRE_API_FAILURE_MODE=fail` would work for LR immediately. Con: does not
give `linear_regression.py` per-call visibility into which writes failed.

**Recommended: Option B + D combined.** First, route the `except` blocks in
the six wrapper functions through `_handle_api_write_error()` so the existing
configurable failure mode works for LR writes (Option D). Then, additionally
make the wrapper functions return `bool` (Option B) so `linear_regression.py`
can track which writes failed and exit non-zero after completing all
processing. Option D is a prerequisite cleanup; Option B adds the exit-code
behaviour on top.

---

## Implementation Plan

### Phase 1: Route through `_handle_api_write_error` and return API success status

**Goal**: Modify the six public write wrapper functions in
`forecast_library.py` to (a) route their API write `except` blocks through
the existing `_handle_api_write_error()` helper (line 94), and (b) return a
`bool` indicating whether the API write succeeded.

**Files allowed to modify**:
- `apps/iEasyHydroForecast/forecast_library.py`

**Changes for each of the six functions**:

1. **Route through `_handle_api_write_error`**: Replace each hand-rolled
   `except Exception as e: logger.error(...)` block with a call to
   `_handle_api_write_error(e, "<function_name>")`. This makes the six LR
   write functions consistent with postprocessing's `file_writer.py` (which
   already uses `_handle_api_write_error` at 6 call sites) and enables the
   existing `SAPPHIRE_API_FAILURE_MODE` env var (warn/fail/ignore) for LR
   writes. **Note**: when `SAPPHIRE_API_FAILURE_MODE=fail`, the helper
   re-raises the exception, so the function will not reach the CSV write or
   the `return False` line. This is intentional — `fail` mode means the
   operator wants a hard stop, not silent CSV fallback. Consequences of
   `fail` mode:
   - CSV write is **skipped** for the failing function
   - The exception propagates out of the `fl.write_*()` call in
     `linear_regression.py` (which has no try/except around these calls)
   - The process exits with an unhandled exception (exit code 1)
   - Remaining hindcast dates and write calls are **not processed**
   - This is an operator opt-in via env var; the default `warn` mode
     preserves the "complete everything, then exit non-zero" behavior

2. **Return `bool` using exception-only failure detection**: Change return
   type from `None` to `bool`. Use a simple `api_ok = True` default —
   only an actual exception caught in the `except` block sets it to `False`.
   This avoids false positives from ambiguous helper return values (`False`
   can mean "API disabled", "health check failed", or "no data to write"):
   - Initialize `api_ok = True` at the top of the API write section
   - In the `except` block, after `_handle_api_write_error` returns (i.e.,
     warn/ignore mode — `fail` mode re-raises and never reaches this line),
     set `api_ok = False`
   - Do NOT check the return value of `_write_*_to_api` helpers
   - Do NOT check `SAPPHIRE_API_ENABLED` in the wrapper — the helper
     handles this internally; no exception means no failure to report
   - For `write_linreg_*_forecast_data`: the existing `data_for_api`
     guard that skips the API call when there's no data is NOT a failure —
     `api_ok` stays `True` because no exception fires
   - CSV write continues regardless when `_handle_api_write_error` does not
     re-raise (i.e., warn or ignore mode)
   - Each function's docstring must document the new return value
   - For the two `write_linreg_*_forecast_data` functions that currently
     `return ret` (where `ret` is the `to_csv()` result): change to
     `return api_ok`. No caller checks the old return value.
   - For the four hydrograph/time-series functions that currently
     `return None`: change to `return api_ok`

**CRITICAL CONSTRAINT**: Do NOT change any existing function signatures
beyond the addition of a return value. Do NOT change the data flow, CSV
write logic, or control flow. Do NOT change the `_write_*_to_api` helper
functions or `_handle_api_write_error` itself. The only permitted changes
are: (1) replace bare `logger.error` in the `except` block with a call to
`_handle_api_write_error`, (2) add `api_ok = True` before the try/except
and `api_ok = False` in the except block, (3) return `api_ok` at the end
of each function.

**Acceptance criteria**:
- All six functions return `bool`
- `True` by default — including when API is disabled, when write succeeds,
  and when the `data_for_api` guard skips the write (no data = not a failure)
- `False` only when an actual exception was caught (in warn/ignore mode)
- Exception propagates when `SAPPHIRE_API_FAILURE_MODE=fail`
- CSV write still happens in ALL non-fail cases — no change to CSV behaviour
- The six `except` blocks use `_handle_api_write_error`, not bare `logger.error`
- Existing tests still pass

---

### Phase 2: Check API write results in `linear_regression.py`

**Goal**: In `linear_regression.py`, check the return value of each write
call. Log a loud error and set a failure flag if API was expected but
failed. Exit non-zero after all processing completes.

**Files allowed to modify**:
- `apps/linear_regression/linear_regression.py`

**Changes**:
- For each of the six `fl.write_*()` calls, capture the return value
- If the return is `False`, log. For the four pre-loop writes (lines
  760–766):
  ```
  logger.error(
      "CRITICAL: API write failed for %s. Data written to CSV only. "
      "API database is now behind CSV.",
      "<function name>",
  )
  ```
  For the two in-loop writes (lines 890, 976), include the forecast date:
  ```
  logger.error(
      "CRITICAL: API write failed for %s on %s. Data written to CSV only. "
      "API database is now behind CSV.",
      "<function name>",
      current_day,
  )
  ```
- Maintain a local `bool` variable `api_write_failures = False` inside
  `main()`, initialized before the pre-loop writes (line ~760). Set it
  to `True` on any `False` return. The variable persists across all
  hindcast loop iterations as a standard local variable.
- After all processing and CSV writes are complete (at the existing
  `sys.exit(0)` on line 987), check the flag: if `api_write_failures`
  is `True`, exit with `sys.exit(1)` instead of `sys.exit(0)`
- The pipeline must NOT abort mid-run on the first API failure — it must
  complete all processing and write all CSV files before reporting failure

**CRITICAL CONSTRAINT**: Do NOT change any existing data flow logic. The
pipeline must still complete all processing and write all CSV files. The
only additions are: capture return values, log errors, and exit non-zero
at the end if any API write failed.

**Acceptance criteria**:
- When API is enabled and healthy: pipeline exits 0 (unchanged behaviour)
- When API is disabled: pipeline exits 0 (CSV-only is intentional)
- When API is enabled but a write fails: pipeline completes all processing,
  writes all CSVs, then exits non-zero with clear error messages in the log
- No change to data flow or CSV write behaviour

---

### Phase 3: Add tests

**Goal**: Cover the new failure-reporting behaviour with targeted unit and
integration tests.

**Files allowed to modify**:
- `apps/linear_regression/test/test_api_write_failure_reporting.py` (new)

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | `write_linreg_pentad_forecast_data` — API disabled | Returns `True`; CSV written |
| 2 | `write_linreg_pentad_forecast_data` — API enabled, health check fails | Returns `False`; CSV still written |
| 3 | `write_linreg_pentad_forecast_data` — API enabled, write succeeds | Returns `True`; CSV written |
| 4 | `write_linreg_decad_forecast_data` — API enabled, health check fails | Returns `False`; CSV still written |
| 5 | Integration: mock API to fail; verify `api_write_failures` flag is set | Pipeline completes; `sys.exit` called with non-zero code |
| 6 | Integration: mock API healthy; verify `api_write_failures` flag is not set | Pipeline completes; `sys.exit` not called with non-zero code |

**Note**: Phase 3 tests should cover all six modified wrapper functions, not just
the two forecast-write functions. Add tests for hydrograph and time-series write
wrappers as well.

All tests use mocked API clients — no live API calls. CSV write assertions
must not depend on filesystem state; use a `tmp_path` fixture.

**Prerequisite fix**: `test_integration_main.py` lines 123–128 set
`return_value = None` for all six write functions. After Phase 2 adds
`if result is False:` checks, `None is False` evaluates to `False` in
Python — the failure path would never be exercised. Update
`_setup_common_mocks()` to set `return_value = True` for all six write
functions so existing tests correctly simulate successful API writes.

**Acceptance criteria**:
- All tests pass (covering all six wrapper functions + 2 integration tests) with:
  ```bash
  cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
  ```
- Zero skips (except valid `SAPPHIRE_API_AVAILABLE` dependency-gate skips)

---

### Phase 4: Enhance maintenance script error messaging

**Goal**: Ensure `bin/daily_linreg_maintenance.sh` produces an actionable
error message when the container exits non-zero due to API write failures.

**Files allowed to modify**:
- `bin/daily_linreg_maintenance.sh`

**Changes**:
- Lines 152–153 currently log a generic warning for any non-zero exit code.
  Enhance the warning to explicitly mention API write failures as a possible
  cause and direct the operator to check DB consistency:

  ```bash
  log_message "WARNING: $MODE hindcast completed with exit code: $CONTAINER_EXIT_CODE"
  log_message "Possible cause: API write failures — check DB consistency against CSV files"
  log_message "Check log file for details: $SERVICE_LOG"
  ```

- No change to script control flow; the loop continues to the next mode
  regardless of exit code (existing behaviour preserved)

**Acceptance criteria**:
- A non-zero container exit code produces a message that includes
  "API write failures" and "DB consistency"
- No change to script control flow

---

## Acceptance Criteria (overall)

- [ ] All six `write_*` functions in `forecast_library.py` return `bool`
- [ ] `True` when API is disabled or write succeeds; `False` when API was
      enabled but write failed
- [ ] CSV write behaviour is unchanged in `warn`/`ignore` modes; in `fail`
      mode, CSV is skipped for the failing function (intentional hard stop)
- [ ] `linear_regression.py` logs a `logger.error` for each failed API
      write and exits non-zero after completing all processing
- [ ] All new tests pass with zero skips
- [ ] `test_integration_main.py` mocks updated from `return_value = None`
      to `return_value = True`
- [ ] Full linear_regression test suite passes with zero skips
- [ ] `bin/daily_linreg_maintenance.sh` produces an actionable warning
      mentioning API write failures when the container exits non-zero
- [ ] No changes to `sapphire/services/` (ownership boundary respected)

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Functions returning `None` changed to return `bool` — callers that check `if result is None` would break | None | No current caller checks the return value; confirmed by blast-radius analysis |
| Pipeline that always exits 0 can now exit non-zero — maintenance scripts and monitoring alert | Expected | This is the desired behaviour; monitor during initial rollout; kill-switch: set `SAPPHIRE_API_ENABLED=false` |
| CSV write behaviour changes | None | CRITICAL CONSTRAINT in Phase 1 and 2 explicitly forbids changes to CSV write logic |
| Mid-run abort could lose CSV data | None | Phase 2 design (default `warn` mode): collect failures, abort only after all processing and CSV writes complete |
| `SAPPHIRE_API_FAILURE_MODE=fail` causes hard crash: CSV skipped, remaining dates unprocessed | Expected — operator opt-in | This is intentional. `fail` mode is for operators who want immediate stop on API failure. Default `warn` mode preserves "complete everything" behavior. Documented in Phase 1 notes. Kill-switch: unset `SAPPHIRE_API_FAILURE_MODE` or set to `warn` |
| Existing test mocks return `None` instead of `bool` — Phase 2 check `if result is False` silently passes | Test gap | Phase 3 must update `_setup_common_mocks()` in `test_integration_main.py` to `return_value = True` |

---

## Rollback

If the loud failure causes operational issues:

1. Set `SAPPHIRE_API_ENABLED=false` to suppress all API writes (existing
   kill-switch, no code change needed)
2. Or revert only the exit-code logic in `linear_regression.py` — the
   `bool` return values in `forecast_library.py` are harmless because no
   other caller checks them

---

## Related Issues

- **LR-001** through **LR-006**: Prior LR API integration work on this
  branch
- **LR-008**: LR pentad horizon offset — LR-007 is a **prerequisite for LR-008
  Phase 3** (historical data migration). The migration writes corrected records
  to the API; without loud failure reporting, migration failures would be silent.
- **LR-006**: Maintenance script sync mode + auto-detect filenames — both LR-006
  and LR-007 modify `bin/daily_linreg_maintenance.sh`. Implement LR-006 P1 (add
  sync mode line ~line 130) before LR-007 P4 (improve warning ~line 148) to
  avoid line-number conflicts.
- **PP-022**: Stale-record refresh in maintenance pipeline — analogous
  silent failure pattern in postprocessing
- **ML-012**: NaN flag crash in `recalculate_nan_forecasts.py` — same
  silent degradation theme in the ML module

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Make write wrappers return bool in forecast_library.py",
      "file": "apps/iEasyHydroForecast/forecast_library.py",
      "changes": [
        "Capture API write result in all six wrapper functions",
        "Return True (API disabled or success) / False (API enabled, write failed)",
        "Update docstrings to document return value"
      ],
      "depends_on": [],
      "parallel_with": ["phase_4"]
    },
    "phase_2": {
      "title": "Check return values and exit non-zero in linear_regression.py",
      "file": "apps/linear_regression/linear_regression.py",
      "changes": [
        "Capture return value of all six fl.write_*() calls",
        "Log logger.error on False return",
        "Track api_write_failures flag",
        "sys.exit(1) after all processing if any write failed"
      ],
      "depends_on": ["phase_1"],
      "parallel_with": []
    },
    "phase_3": {
      "title": "Add tests for failure-reporting behaviour",
      "file": "apps/linear_regression/test/test_api_write_failure_reporting.py",
      "changes": [
        "Unit tests covering all six wrapper functions (API disabled, API fail, API success) + 2 integration tests for exit-code behaviour"
      ],
      "depends_on": ["phase_1", "phase_2"],
      "parallel_with": []
    },
    "phase_4": {
      "title": "Enhance maintenance script error messaging (PREREQUISITE: LR-006 P1 must be merged first — both edit this file)",
      "file": "bin/daily_linreg_maintenance.sh",
      "changes": [
        "Improve non-zero exit code warning to mention API write failures and DB consistency"
      ],
      "depends_on": [],
      "parallel_with": ["phase_1"]
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": true,
      "agents": [
        {
          "id": "agent_forecast_library",
          "phases": ["phase_1"],
          "reason": "Return bool from six write wrappers in forecast_library.py"
        },
        {
          "id": "agent_maintenance_script",
          "phases": ["phase_4"],
          "reason": "Maintenance script warning message — fully independent of Python changes"
        }
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {
          "id": "agent_linreg",
          "phases": ["phase_2"],
          "reason": "Check return values and exit non-zero in linear_regression.py — depends on phase_1"
        }
      ]
    },
    {
      "group": 3,
      "parallel": false,
      "agents": [
        {
          "id": "agent_tests",
          "phases": ["phase_3"],
          "reason": "New test file — depends on phase_1 and phase_2 being complete"
        }
      ]
    }
  ]
}
```
