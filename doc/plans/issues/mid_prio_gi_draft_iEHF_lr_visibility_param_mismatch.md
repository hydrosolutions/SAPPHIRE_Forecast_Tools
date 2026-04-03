# FD-010: Fix lr-visibility parameter mismatch between dashboard and linreg

**Status**: Review
**Module**: iEasyHydroForecast (forecast_library), forecast_dashboard
**Priority**: Medium
**Labels**: `linear_regression`, `forecast_dashboard`, `visibility`, `iEasyHydroForecast`

---

## Summary

The dashboard saves lr-visibility records under **issue-pentad** parameters, but `perform_linear_regression` queries them under **target-pentad** parameters. The API lookup always returns empty, linreg falls back to no filtering, and visibility edits made by the user have no effect on recomputed forecasts.

## Context

The linear regression module uses a visibility mechanism to let hydrologists exclude anomalous years from regression training. For each (station, pentad-in-month, month) combination, records mark individual years as `visible=True/False`. The dashboard lets users toggle these checkboxes on the regression tab and saves changes via POST to `/api/postprocessing/lr-visibility/`. When "Save Changes" is clicked, linreg reruns and should read the updated visibility before fitting the regression.

The LR system uses **issue-date indexing**: `forecast_horizon_int = 17` refers to the boundary day March 25 (last day of pentad 17), which issues a forecast for the *target* pentad March 26–31 (pentad 18). This convention is correct throughout the pipeline and must not change.

Discovered during FD-009 server testing. Related to FD-007 (dashboard dataflows) and FD-009 (explicit forecast date passthrough).

## Problem

`perform_linear_regression` in `apps/iEasyHydroForecast/forecast_library.py` computes the visibility API query parameters by adding +1 day to the issue date and deriving month/period from the resulting **target** date (lines 1693–1702):

```python
first_day_of_forecast_horizon = pd.to_datetime(forecast_date_str).date() + pd.DateOffset(days=1)
if horizon_flag == "pentad":
    pentad_in_month = tl.get_pentad(first_day_of_forecast_horizon)   # target pentad
month_int = first_day_of_forecast_horizon.month                        # target month
```

The dashboard saves visibility using parameters derived **directly** from `horizon_value` (the issue pentad_in_year), without the +1 day offset (`vizualization.py:3768–3769`):

```python
month_for_horizon = math.ceil(horizon_value / periods_per_month)
horizon_value_in_month = horizon_value % periods_per_month or periods_per_month
```

Concrete example — pentad 17 (issue date Mar 25, `forecast_horizon_int = 17`):

| | month | period_in_month |
|---|---|---|
| Dashboard saves | 3 (March) | 5 |
| Linreg queries | 3 (March) | **6** |

For the last pentad of each month (period 6), the +1 day crosses a month boundary, making the mismatch worse:

| | month | period_in_month |
|---|---|---|
| Dashboard saves pentad 6 of March (horizon 18, boundary Mar 31) | 3 (March) | 6 |
| Linreg queries | **4 (April)** | **1** |

Result: `_read_lr_visibility` returns an empty DataFrame, linreg logs "Using CSV point selection" (or skips filtering entirely), and the regression uses all historical years regardless of what the user toggled.

## Desired Outcome

When the user edits visibility checkboxes in the dashboard and clicks "Save Changes", the reruns of linreg read the updated visibility and produce forecasts that reflect those edits. Specifically:

- `perform_linear_regression` queries `/api/postprocessing/lr-visibility/` with parameters that match what the dashboard POSTed
- The API returns non-empty visibility data and linreg filters training years accordingly
- The regression result changes when visibility changes
- Existing behaviour on boundary days (Luigi pipeline, no visibility data) is unchanged

---

## Technical Analysis

### Root Cause

`forecast_library.py:1693–1702` was written to query visibility using the *target* pentad (the one being forecasted). The dashboard, however, naturally uses the *issue* pentad (the one the user sees in the regression tab). Neither convention is inherently wrong, but they must match.

Option (B) — fixing linreg to use issue-pentad parameters — was chosen because:
1. The dashboard convention is correct for the user mental model (you see the issue pentad in the UI).
2. The arithmetic is simpler and more robust (no date addition, no cross-month edge cases).
3. `get_date_for_last_day_in_pentad` already uses the same formula internally (lines 833–834) — we can reuse it directly.
4. No dashboard changes required.

### Current code

`apps/iEasyHydroForecast/forecast_library.py`, inside `perform_linear_regression`, lines 1692–1710:

```python
# Compute date components needed by both API and CSV paths
first_day_of_forecast_horizon = pd.to_datetime(forecast_date_str).date() + pd.DateOffset(
    days=1
)
if horizon_flag == "pentad":
    pentad_in_month = tl.get_pentad(first_day_of_forecast_horizon)
elif horizon_flag == "decad":
    pentad_in_month = tl.get_decad_in_month(first_day_of_forecast_horizon)
else:
    raise ValueError(f"horizon_flag {horizon_flag} is not valid.")
month_int = first_day_of_forecast_horizon.month

# Map internal horizon_flag to API enum value
api_horizon = "decade" if horizon_flag == "decad" else horizon_flag

point_selection = None

# Try API first
api_result = _read_lr_visibility(api_horizon, station, month_int, int(pentad_in_month))
```

The variable `first_day_of_forecast_horizon` is used only twice after line 1702:
- Line 1710: passed indirectly (via `month_int` and `pentad_in_month`) to `_read_lr_visibility`
- Line 1724: `title_month = tl.get_month_str_en(first_day_of_forecast_horizon)` — for the CSV fallback filename

`forecast_date_str` is not used after line 1693.

### Correct formula

`get_date_for_last_day_in_pentad` itself computes month and period from `forecast_horizon_int` at lines 833–834:

```python
month = (pentad_in_year - 1) // 6 + 1
pentad_in_month = (pentad_in_year - 1) % 6 + 1
```

This is identical to the dashboard formula and produces the issue-pentad parameters.

For decad (3 periods per month):
```python
month = (decad_in_year - 1) // 3 + 1
period_in_month = (decad_in_year - 1) % 3 + 1
```

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/iEasyHydroForecast/forecast_library.py` | Replace date-offset computation at lines 1692–1702 with direct arithmetic from `forecast_horizon_int`. Update CSV path to use month name from `month_int` (must pass a string, not `dt.date`, to `get_month_str_en`). |
| `apps/iEasyHydroForecast/tests/test_forecast_library.py` | Update `test_api_failure_falls_back_to_csv` fixture filename from `_2_` to `_1_` (corrected issue-pentad convention). |

### Implementation Steps

- [x] **Step 1**: In `perform_linear_regression`, replace lines 1692–1702 with direct arithmetic:

```python
# Compute visibility lookup parameters from forecast_horizon_int
# (issue-pentad convention — matches dashboard saves)
if horizon_flag == "pentad":
    periods_per_month = 6
else:  # decad
    periods_per_month = 3
month_int = (forecast_horizon_int - 1) // periods_per_month + 1
pentad_in_month = (forecast_horizon_int - 1) % periods_per_month + 1
```

- [x] **Step 2**: Update the CSV fallback path at line 1724. Currently it calls `tl.get_month_str_en(first_day_of_forecast_horizon)`. Replace with a date string constructed from `month_int`:

```python
title_month = tl.get_month_str_en(f"{_year}-{month_int:02d}-01")
```

**Important**: `get_month_str_en` only accepts `str` or `pd.Timestamp` — passing a bare `datetime.date` object raises an unhandled `TypeError` (the `except ValueError` clause does not catch it). Do NOT use `dt.date(_year, month_int, 1)`.

Note: this also changes the CSV fallback **filename** convention from target-pentad to issue-pentad (e.g., for `forecast_horizon_int=17`: `{station}_6_pentad_of_March.csv` → `{station}_5_pentad_of_March.csv`). This is intentional — the CSV naming had the same mismatch as the API query. Any existing CSV files on deployment servers were accidentally aligned with the buggy code and were never being matched by the dashboard (which saves via API, not CSV). The CSV path is a legacy fallback being phased out; if a file is not found, `point_selection` stays `None` and regression runs unfiltered — the same behavior as the current broken API path.

- [x] **Step 3**: Remove the now-unused `first_day_of_forecast_horizon` variable (lines 1693–1695). Also remove `forecast_date_str` if it is no longer referenced anywhere after removing the offset computation (check lines 1590–1693 — `forecast_date_str` is assigned at line 1590/1596 and consumed only at line 1693, so it can be removed too if desired, but this is optional cleanup).

- [x] **Step 4**: Update existing test `test_api_failure_falls_back_to_csv` (line 3428 of `test_forecast_library.py`). This test creates a CSV fixture named `TEST1_2_pentad_of_January.csv` based on the old target-pentad derivation (`forecast_horizon_int=1` → Jan 5 + 1 day → Jan 6 → `get_pentad` = 2). After the fix, the code computes `pentad_in_month = (1-1)%6 + 1 = 1`, so it looks for `TEST1_1_pentad_of_January.csv`. Update the fixture filename and any associated comments to match the corrected issue-pentad convention.

- [x] **Step 5**: Write new unit tests (see Testing section).

- [x] **Step 6**: Run full linreg test suite. Zero failures, zero skips.

### Expected diff (lines 1692–1710 region)

```python
# BEFORE
first_day_of_forecast_horizon = pd.to_datetime(forecast_date_str).date() + pd.DateOffset(
    days=1
)
if horizon_flag == "pentad":
    pentad_in_month = tl.get_pentad(first_day_of_forecast_horizon)
elif horizon_flag == "decad":
    pentad_in_month = tl.get_decad_in_month(first_day_of_forecast_horizon)
else:
    raise ValueError(f"horizon_flag {horizon_flag} is not valid.")
month_int = first_day_of_forecast_horizon.month

# Map internal horizon_flag to API enum value
api_horizon = "decade" if horizon_flag == "decad" else horizon_flag

# AFTER
# Compute visibility lookup parameters from forecast_horizon_int
# (issue-pentad convention — matches dashboard saves to /api/postprocessing/lr-visibility/)
if horizon_flag == "pentad":
    periods_per_month = 6
else:  # decad
    periods_per_month = 3
month_int = (forecast_horizon_int - 1) // periods_per_month + 1
pentad_in_month = (forecast_horizon_int - 1) % periods_per_month + 1

# Map internal horizon_flag to API enum value
api_horizon = "decade" if horizon_flag == "decad" else horizon_flag
```

And at line 1724 (CSV fallback):

```python
# BEFORE
title_month = tl.get_month_str_en(first_day_of_forecast_horizon)

# AFTER
title_month = tl.get_month_str_en(f"{_year}-{month_int:02d}-01")
```

---

## Testing

### Unit tests (new file: `apps/iEasyHydroForecast/test/test_lr_visibility_params.py` or equivalent)

- [ ] For all 72 pentad values (1–72), verify that the new formula produces `(month, period_in_month)` identical to the dashboard formula `(ceil(h/6), h%6 or 6)`.
- [ ] For all 36 decad values (1–36), same verification against `(ceil(h/3), h%3 or 3)`.
- [ ] Verify pentad 6 of each month (periods 6, 12, 18, …, 72) does NOT cross a month boundary (old bug: +1 day on last day of month → next month).
- [ ] Verify pentad 72 (Dec 31) → `month=12, period=6` (not Jan of next year).
- [ ] Verify the CSV title_month string matches the month of `month_int`, not the next month.

### Existing test updates

- [ ] `test_api_failure_falls_back_to_csv`: update fixture filename from `TEST1_2_pentad_of_January.csv` to `TEST1_1_pentad_of_January.csv` and update associated comments explaining the derivation.

### Integration test (existing `test_integration_main.py` or new)

- [ ] Mock `_read_lr_visibility` to verify it is called with issue-pentad parameters matching the input `forecast_horizon_int`. For `forecast_horizon_int=17` (Mar 25): expect `month=3, horizon_value=5`. For `forecast_horizon_int=18` (Mar 31): expect `month=3, horizon_value=6` (not `month=4, horizon_value=1`).

### Manual verification

1. On the server: edit visibility for one station/pentad in the dashboard
2. Click "Save Changes"
3. Check linreg container logs: look for `"Read N lr-visibility records for code=... month=... horizon=.../..."` — N should be non-zero
4. Verify forecast value changes compared to before the visibility edit

### Testing commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast  # if separate suite exists
```

---

## Documentation Impact

- [ ] No documentation update required — this is an internal bug fix with no user-facing API or configuration changes. `doc/development.md` section 2.2.8 already describes the corrected behavior (visibility edits take effect on Save Changes).

---

## Out of Scope

- Migrating existing lr-visibility records in the database (records already saved under target-pentad keys will not be found; they can be re-entered via the dashboard after the fix, or ignored — they were never being used anyway)
- Renaming existing CSV visibility files on deployment servers (same mismatch as the API — old filenames used target-pentad convention; the CSV path is a legacy fallback being phased out and degrades gracefully to unfiltered regression if files are not found)
- Changing the dashboard visibility save parameters (option A — not chosen)
- Changing the `forecast_horizon_int` convention (issue-date indexing is correct and must not change; see memory `lr_issue_date_convention.md`)
- Decad CSV file naming alignment (same fix applies, covered by Step 2)

## Dependencies

- FD-007 (implemented) — dashboard dataflows prerequisite
- FD-009 (implemented) — explicit forecast date passthrough; ensures linreg receives the correct `SAPPHIRE_FORECAST_DATE` so `forecast_horizon_int` is computed for the right pentad

## Acceptance Criteria

- [x] `_read_lr_visibility` is called with `month` and `horizon_value` matching the dashboard's POST parameters for all 72 pentad and 36 decad values
- [x] For `forecast_horizon_int=18` (last pentad of March, boundary Mar 31), query uses `month=3, horizon_value=6` — not `month=4, horizon_value=1`
- [ ] Visibility edits in the dashboard produce different forecast values when Save Changes is clicked *(requires manual server testing)*
- [x] All existing linreg tests pass
- [x] New unit tests cover all 72 pentad and 36 decad parameter values

---

## References

- `apps/iEasyHydroForecast/forecast_library.py:1692–1710` — parameter computation to replace
- `apps/iEasyHydroForecast/forecast_library.py:1724–1725` — CSV fallback path (uses same month/period)
- `apps/iEasyHydroForecast/forecast_library.py:1414–1469` — `_read_lr_visibility` function
- `apps/iEasyHydroForecast/forecast_library.py:1740–1746` — visibility filter applied to training data
- `apps/iEasyHydroForecast/tag_library.py:833–834` — identical formula inside `get_date_for_last_day_in_pentad`
- `apps/forecast_dashboard/src/vizualization.py:3768–3769` — dashboard visibility save parameters
- FD-009: `doc/plans/issues/mid_prio_gi_draft_fd_retrigger_forecast_date.md`
- Memory: `lr_issue_date_convention.md` — issue-date indexing must not change
