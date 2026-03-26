# LR-009: Dec 31 cross-year boundary bug in linear regression pipeline

> **Closed 2026-03-26**: This issue was predicated on the original LR-008 plan
> which proposed changing `forecast_horizon_int` from the issue-date pentad to
> the target pentad. Investigation proved that the issue-date convention is
> correct — `forecast_horizon_int` must remain the issue pentad. The rescoped
> LR-008 applies a metadata-only override before the API write, which does not
> trigger the `_year` corruption described here. The Dec 31 metadata wrap
> (pentad 72 → target 1) is handled by simple arithmetic in the override.

**Status**: Closed (Invalid)
**Module**: `linear_regression`, `iEasyHydroForecast`
**Priority**: High
**Labels**: `data-integrity`, `forecast-correctness`
**Blocked by**: LR-008 (Dec 31 guard must be in place first)

---

## Summary

On Dec 31, the LR pipeline produces a forecast targeting pentad 1 / decad 1 of
the following year. After LR-008's `+1 day` fix, `get_pentad_in_year(Jan 1
next year)` correctly returns 1, but `perform_linear_regression` in
`forecast_library.py` derives `_year = forecast_date.year` from the Dec 31
production date (year Y). This causes
`get_date_for_last_day_in_pentad(1, year=Y)` to return Jan 5 of the **current**
year — 11 months in the past — corrupting the entire regression context
(training data window and forecast date string). LR-008 includes a temporary
guard that preserves old behavior on Dec 31 until this issue is fixed.

---

## Problem Description

On Dec 31 the forecast production date is Y-12-31 and the target period is
pentad 1 (Jan 1–5) or decad 1 (Jan 1–10) of year Y+1.

After LR-008 Phase 1 corrects the horizon offset, `forecast_horizon_int = 1`
and `forecast_date` passed into `perform_linear_regression` is still
`Y-12-31`. Inside that function:

```python
# forecast_library.py lines ~1562–1569
if forecast_date is not None:
    if hasattr(forecast_date, "year"):
        _year = forecast_date.year          # Dec 31: _year = Y  ← WRONG
    else:
        _year = pd.Timestamp(forecast_date).year
else:
    _year = dt.datetime.now().year
```

Then:

```python
# forecast_library.py line ~1576
forecast_date_str = tl.get_date_for_last_day_in_pentad(forecast_horizon_int, year=_year)
# With horizon_int=1, year=Y → returns "Y-01-05"  ← 11 months in the past
```

For the decad path (line ~1582):

```python
forecast_date_str = tl.get_date_for_last_day_in_decad(forecast_horizon_int, year=_year)
# With horizon_int=1, year=Y → returns "Y-01-10"  ← 11 months in the past
```

The `_year` derivation assumes the forecast horizon and the production date
fall in the same calendar year, which is false on Dec 31.

### Sub-bug 2: visibility lookup queries wrong month

At lines ~1679–1688, `first_day_of_forecast_horizon` is computed by adding
one day to `forecast_date_str`:

```python
first_day_of_forecast_horizon = pd.to_datetime(forecast_date_str).date() + pd.DateOffset(days=1)
```

When `forecast_date_str = "Y-01-05"` (wrong year, from sub-bug 1),
`first_day_of_forecast_horizon = Y-01-06`. The month and pentad-in-month
extracted from this date are used to look up visibility records:

```python
pentad_in_month = tl.get_pentad(first_day_of_forecast_horizon)   # pentad 2 of January
month_int = first_day_of_forecast_horizon.month                   # 1 (January)
api_result = _read_lr_visibility(api_horizon, station, month_int, int(pentad_in_month))
```

The correct lookup for a horizon targeting Jan 1–5 (pentad 1 of year Y+1)
should query January pentad 1 visibility records. However the visibility lookup
also depended on `forecast_date_str` being correct — if sub-bug 1 is fixed
(so `forecast_date_str = "Y+1-01-05"`), then `first_day_of_forecast_horizon
= Y+1-01-06` and the visibility lookup resolves to January pentad 2, which is
still wrong (the target is pentad 1 of January). This secondary offset in the
visibility path requires independent investigation.

**Note**: Sub-bug 2 exists independently of LR-008 and is present in the
current codebase on Dec 31. Sub-bug 1 (wrong `_year`) is the primary
corruption, since an 11-month-stale `forecast_date_str` breaks every
downstream calculation.

---

## Root Cause

### Sub-bug 1: `_year` derived from production date, not from horizon

`forecast_library.py` lines 1562–1569:

```python
if forecast_date is not None:
    if hasattr(forecast_date, "year"):
        _year = forecast_date.year      # Dec 31 year Y, but horizon belongs to Y+1
    ...
```

The function assumes `forecast_date.year` equals the year of the target
period. On Dec 31 this assumption breaks: the production date is in year Y but
the forecast horizon (pentad/decad 1) belongs to year Y+1.

### Sub-bug 2: `first_day_of_forecast_horizon` month/period extraction

`forecast_library.py` lines 1679–1688:

```python
first_day_of_forecast_horizon = pd.to_datetime(forecast_date_str).date() + pd.DateOffset(days=1)
pentad_in_month = tl.get_pentad(first_day_of_forecast_horizon)
month_int = first_day_of_forecast_horizon.month
```

For horizon 1 the last day of the period is Jan 5. Adding one day gives Jan 6,
which is pentad 2 of January — but the visibility records for this horizon
should be keyed to pentad 1 of January. This off-by-one in the visibility
lookup is independent of the year bug and requires separate investigation to
confirm the correct fix (the `+1 day` logic may be intentional for other
horizons).

---

## Impact

| Dimension | Detail |
|-----------|--------|
| Frequency | 1 day per year (Dec 31) |
| Horizon types | Pentad (horizon 1) and decad (horizon 1) |
| Stations affected | All stations |
| Data corrupted | `forecast_date_str` points 11 months into the past; training data is drawn from the wrong year context; visibility lookup queries wrong month records |
| Severity | High — the forecasted discharge values for all stations on Dec 31 are computed against the entirely wrong year's data |

LR-008 mitigates this by preserving pre-LR-008 behavior on Dec 31 via a
temporary guard. That guard must remain until LR-009 is fixed and verified.

---

## Proposed Fix Direction

Two candidate approaches for sub-bug 1:

**Option A — Pass `target_day` instead of `current_day`** (preferred):

At the call site in `apps/linear_regression/linear_regression.py`, pass
`current_day + timedelta(days=1)` as `forecast_date` to
`perform_linear_regression`. On Dec 31, `target_day = Jan 1` and
`target_day.year = Y+1`, so `_year = Y+1` everywhere inside
`perform_linear_regression`. This is the most surgical fix: zero changes to
`forecast_library.py`, and the semantics of `forecast_date` align with the
existing documentation ("the date the forecast is being produced for"
→ reinterpreted as "the first day of the target period").

**Option B — Derive `_year` from the horizon inside `perform_linear_regression`**:

Keep `forecast_date` as the production date, but add logic inside
`perform_linear_regression` to infer the correct year from `forecast_horizon_int`
and the production month. This is more complex and more invasive.

Option A is preferred because it is minimal, reversible, and consistent with
how LR-008 already shifted `forecast_pentad_of_year` to target-period
semantics.

For sub-bug 2, once sub-bug 1 is fixed:
- `forecast_date_str = "Y+1-01-05"` (correct last day of pentad 1)
- `first_day_of_forecast_horizon = Y+1-01-06` → January pentad 2

The visibility lookup would then query pentad 2 of January when the correct
lookup key is pentad 1 of January. This sub-bug requires investigation of
whether the `+1 day` step in `first_day_of_forecast_horizon` is correct for
all other horizons and wrong only for this edge case, or whether the
`_read_lr_visibility` call should use `forecast_date_str` directly (i.e.,
keyed to the last day of the period, not the first day of the next period).

---

## Files Likely Affected

- `apps/linear_regression/linear_regression.py` — call site: what date is
  passed as `forecast_date` to `fl.perform_linear_regression`
- `apps/iEasyHydroForecast/forecast_library.py` — `_year` derivation (lines
  1562–1569) inside `perform_linear_regression`; `first_day_of_forecast_horizon`
  month/period extraction (lines 1679–1688)

---

## Implementation Plan

### Phase 1: Fix `_year` derivation (sub-bug 1)

**Goal**: Ensure that on Dec 31, `perform_linear_regression` uses year Y+1 for
all date calculations, so `forecast_date_str` points to the correct period in
the following year.

**Depends on**: LR-008 Phase 1 (the Dec 31 guard in `linear_regression.py`
must already be present; this fix replaces that guard)

**Approach**: Apply Option A — change the call site in
`apps/linear_regression/linear_regression.py` to pass
`current_day + dt.timedelta(days=1)` as `forecast_date` to
`fl.perform_linear_regression`. This is already the `target_day` that LR-008
uses for `forecast_pentad_of_year` / `forecast_decad_of_year`; passing it as
`forecast_date` keeps the year context consistent.

**Files allowed to modify**:
- `apps/linear_regression/linear_regression.py`

**CRITICAL CONSTRAINT**: Do NOT change any other function signatures, data
flow logic, or control flow. Do NOT modify `forecast_library.py`. Remove the
Dec 31 temporary guard added by LR-008 only after this fix is verified correct
by the Phase 2 tests.

**Acceptance criteria**:
- On Dec 31 (pentad path): `forecast_date_str = "(Y+1)-01-05"`, not `"Y-01-05"`
- On Dec 31 (decad path): `forecast_date_str = "(Y+1)-01-10"`, not `"Y-01-10"`
- On all non-Dec-31 days: behavior is unchanged
- The Dec 31 guard from LR-008 is removed (superseded by this fix)

---

### Phase 2: Investigate and fix visibility lookup (sub-bug 2)

**Goal**: Determine whether `first_day_of_forecast_horizon` correctly identifies
the visibility record key for horizon 1 after sub-bug 1 is fixed, and apply a
targeted fix if it does not.

**Depends on**: Phase 1

**Investigation steps**:

1. With sub-bug 1 fixed, trace `first_day_of_forecast_horizon` for Dec 31
   (pentad 1, decad 1) through `_read_lr_visibility`. Confirm whether the
   resulting `(month_int, pentad_in_month)` tuple matches the visibility
   records stored for pentad 1 / decad 1 of January.

2. Check whether the same `+1 day` offset is correct for all other horizons
   (e.g., horizon 2 → `first_day_of_forecast_horizon = Jan 11`; visibility
   lookup for pentad 2 of January). Document whether the `+1 day` is
   intentional or whether it introduces a systematic off-by-one for all
   horizons.

3. If the `+1 day` step is wrong only for horizon 1 of pentad/decad (i.e., the
   off-by-one wraps from pentad 1 to pentad 2 of the same month), assess
   whether keying visibility lookup on `forecast_date_str` directly (the last
   day of the horizon) rather than `forecast_date_str + 1 day` is the correct
   fix.

**Files allowed to modify** (only if investigation confirms a bug):
- `apps/iEasyHydroForecast/forecast_library.py` — `first_day_of_forecast_horizon`
  computation and/or the `_read_lr_visibility` call arguments (lines 1679–1696)

**CRITICAL CONSTRAINT**: Do NOT change any function signatures. Do NOT change
behavior for any horizon other than the one confirmed as buggy. If the
investigation finds that the `+1 day` logic is intentional and correct for all
horizons, document the finding and close this sub-phase without a code change.

**Acceptance criteria**:
- Visibility records for Dec 31 forecasts are looked up with the correct
  `(month=1, pentad_in_month=1)` key (pentad path) or equivalent decad key
- Behavior for all other forecast days is unchanged
- Written investigation notes are included in the PR description

---

### Phase 3: Add regression tests for Dec 31

**Goal**: Cover the Dec 31 cross-year boundary with targeted tests so this bug
cannot silently regress after LR-008's guard is removed.

**Depends on**: Phase 1 (and Phase 2 if code changes were made)

**Files allowed to modify**:
- `apps/linear_regression/test/test_dec31_cross_year_boundary.py` (new file)

**Test matrix**:

| # | Input date | Horizon type | Expected `forecast_date_str` | Notes |
|---|------------|--------------|------------------------------|-------|
| 1 | 2026-12-31 | pentad | `"2027-01-05"` | Pentad 1 of year Y+1 |
| 2 | 2026-12-31 | decad  | `"2027-01-10"` | Decad 1 of year Y+1 |
| 3 | 2026-12-30 | pentad | `"2026-12-31"` | Non-cross-year: last day of pentad 72 |
| 4 | 2026-01-01 | pentad | `"2026-01-05"` | Normal Jan 1: pentad 1 of current year |
| 5 | 2026-12-31 | pentad | visibility lookup key `(month=1, pentad_in_month=1)` | Regression for sub-bug 2 (after Phase 2) |

**CRITICAL CONSTRAINT**: Tests must use mocked API clients and environments
(`SAPPHIRE_TEST_ENV=True`). No live API calls, no live filesystem writes
outside `tmp_path`.

**Acceptance criteria**:
- All tests pass:
  ```bash
  cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
  ```
- Zero skips (except valid `SAPPHIRE_API_AVAILABLE` dependency-gate skips)
- Full linear_regression and iEasyHydroForecast test suites pass with zero
  unexpected skips

---

## Acceptance Criteria (overall)

- [ ] On Dec 31, `forecast_date_str` for pentad 1 returns `"(Y+1)-01-05"`
- [ ] On Dec 31, `forecast_date_str` for decad 1 returns `"(Y+1)-01-10"`
- [ ] Visibility lookup on Dec 31 queries January pentad/decad 1 records,
      not January pentad/decad 2
- [ ] LR-008 Dec 31 temporary guard is removed (replaced by this fix)
- [ ] All Dec 31 boundary tests pass with zero skips
- [ ] Full test suite passes with zero skips
- [ ] No changes to `sapphire/services/` without coordination with the service
      owner

---

## Testing Plan

```bash
# Phase 1 + 3 verification
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression

# Regression check: iEasyHydroForecast tests unaffected
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast
```

Manual spot-check: run the LR pipeline in hindcast mode with
`forecast_date=2026-12-31` and verify:
- `forecast_date_str = "2027-01-05"` (pentad path)
- `forecast_date_str = "2027-01-10"` (decad path)
- Visibility lookup queries `(month=1, pentad_in_month=1)`
- Training data is drawn from the correct pentad-1 observations across years

---

## Risks and Considerations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Option A changes the semantics of `forecast_date` in `perform_linear_regression` | Low | `forecast_date` is documented as "the date the forecast is being produced for"; passing `target_day` is a minor semantic shift that is consistent with how LR-008 already uses target-period semantics for `forecast_pentad_of_year`. Document the change clearly. |
| Removing the LR-008 Dec 31 guard too early | Medium | Only remove the guard after Phase 2 tests (test row 1 and 2) are green. Gate the guard removal on test passage. |
| Sub-bug 2 affects all horizons, not just Dec 31 | Low | Phase 2 investigation will trace the `+1 day` logic for several representative horizons before making any change. If a systematic off-by-one is found, scope expands — escalate before modifying `forecast_library.py`. |
| Downstream change to visibility query breaks other months | Low | Phase 3 tests include a non-Dec-31 case (row 3) and the iEasyHydroForecast test suite serves as a regression guard. |

---

## Related Issues

- **LR-008**: LR forecasts tagged to wrong pentad/decad on boundary days —
  **prerequisite**; provides the Dec 31 guard that keeps production stable
  until LR-009 is fixed
- **LR-007**: Silent API write failures — prerequisite for any migration work;
  ensures write errors surface rather than failing silently

---

## Dependency Graph

```json
{
  "phases": {
    "LR008_P1": {
      "title": "LR-008 Phase 1: fix horizon offset in linear_regression.py",
      "depends_on": [],
      "note": "External prerequisite — must be complete before LR-009 begins"
    },
    "LR009_P1": {
      "title": "Fix _year derivation (sub-bug 1): pass target_day as forecast_date",
      "file": "apps/linear_regression/linear_regression.py",
      "depends_on": ["LR008_P1"],
      "parallel_agents": 1
    },
    "LR009_P2": {
      "title": "Investigate and fix visibility lookup (sub-bug 2)",
      "file": "apps/iEasyHydroForecast/forecast_library.py",
      "depends_on": ["LR009_P1"],
      "parallel_agents": 1
    },
    "LR009_P3": {
      "title": "Add Dec 31 regression tests",
      "file": "apps/linear_regression/test/test_dec31_cross_year_boundary.py",
      "depends_on": ["LR009_P1", "LR009_P2"],
      "parallel_agents": 1
    }
  }
}
```
