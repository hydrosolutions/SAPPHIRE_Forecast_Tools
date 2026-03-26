# LR-008: LR forecasts tagged to wrong pentad/decad on boundary days

**Status**: Draft
**Module**: `linear_regression`
**Priority**: High
**Labels**: `data-integrity`, `forecast-correctness`, `api-integration`

---

## Summary

On every pentad and decad boundary day, the linear regression pipeline uses
the wrong pentad/decad period. LR passes `current_day` (the production date)
to `get_pentad_in_year` / `get_decad_in_year`, but should pass
`current_day + 1` (the first day of the target period). On boundary days
these differ, causing LR to (1) train on the wrong pentad's historical data,
(2) use the wrong pentad's norm discharge, and (3) store the wrong
`horizon_in_year` metadata in the database. The forecasted discharge values
themselves are subtly incorrect on all boundary days.

---

## Problem Description

A forecast produced on March 25 targets the period March 26–31, which is
pentad 18 of the year. The ML pipeline writes:

```
date=2026-03-25, horizon_in_year=18
```

The LR pipeline writes:

```
date=2026-03-25, horizon_in_year=17
```

Pentad 17 is March 21–25 — the *current* period, not the target period.

**Note on postprocessing**: `postprocessing_forecasts/src/data_reader.py`
normalizers (`_normalize_lr_forecasts` at line 1748 and
`_normalize_ml_forecasts` at line 1883) both **discard** the stored
`horizon_in_year` and recompute `pentad_in_year` from `date + 1 day`, so
the EM/NE join is not directly broken by this metadata mismatch. However,
the bug has a deeper effect: `forecast_pentad_of_year` is used upstream as
the `group_id` for `fl.save_discharge_avg` (norm discharge lookup) and as
the `forecast_horizon_int` for `fl.perform_linear_regression` (training data
filter). On boundary days, LR trains on the wrong pentad's historical data
and uses the wrong pentad's norm discharge, producing subtly incorrect
forecasted values.

The same off-by-one is present for decad boundary days (days 10, 20, and the
last day of each month).

---

## Root Cause

**`apps/linear_regression/linear_regression.py`**

- Line 745 (pentad path):
  ```python
  forecast_pentad_of_year = tl.get_pentad_in_year(current_day)
  ```
- Line 846 (decad path):
  ```python
  forecast_decad_of_year = tl.get_decad_in_year(current_day)
  ```

Both calls pass `current_day` — the date the forecast is *produced*. They
should pass `current_day + timedelta(days=1)` — the first day of the *target*
period — to be consistent with the ML convention and with the filtering logic
in `data_reader.py`.

**`apps/iEasyHydroForecast/tag_library.py`** (lines 317–356 and 359–400)

`get_pentad_in_year` and `get_decad_in_year` are pure functions. They return
the period that contains the supplied date. On a boundary day (e.g., day 25,
which is the last day of pentad 17), passing `current_day` returns 17;
passing `current_day + timedelta(days=1)` correctly returns 18.

No change is required in `tag_library.py`. The fix is entirely in the two
assignment lines in `linear_regression.py`.

---

## Impact Assessment

### Operational Impact

On every boundary day, the bug causes **three correctness issues**:

1. **Wrong norm discharge**: `fl.save_discharge_avg` receives the current
   period's `group_id` instead of the target period's. On March 25, it pulls
   historical pentad-17 norms (March 21–25 data) instead of pentad-18 norms
   (March 26–31 data). The forecasted discharge value itself is wrong.

2. **Wrong training data**: `fl.perform_linear_regression` filters historical
   observations to `pentad_in_year == forecast_pentad_of_year`. With the bug,
   the regression is fitted on the wrong pentad's observations.

3. **Corrupted DB metadata**: The `horizon_in_year` value stored in the
   `lr_forecasts` table is wrong. Any downstream query that reads
   `horizon_in_year` directly from the DB (e.g., skill metric calculations
   that don't recompute it) gets incorrect values.

**Note on EM/NE combined forecasts**: The postprocessing normalizers
(`_normalize_lr_forecasts` at `data_reader.py:1748` and
`_normalize_ml_forecasts` at `data_reader.py:1883`) both **discard** the
stored `horizon_in_year` and **recompute** `pentad_in_year` from
`date + pd.Timedelta(days=1)`. Therefore the EM/NE join is not affected by
this bug — both LR and ML get the same recomputed `pentad_in_year`. The
primary harm is items 1 and 2: the forecast value itself is computed against
the wrong pentad's data.

**Boundary day frequency**:
- Pentad: 6 boundary days per month × 12 months = 72 affected days per year
- Decad: 3 boundary days per month × 12 months = 36 affected days per year
  (subset of pentad boundary days, so total unique affected days ≤ 72/year)

### Historical Data Impact

All historical LR forecast rows already committed to the database carry the
wrong `horizon_in_year` on boundary dates. The magnitude depends on how long
the system has been running with the API write path enabled.

A DB correction (Phase 3) is required to fix historical records. Without it,
historical skill metrics that group by `(code, horizon_in_year, year)` will
have inflated rows in the wrong bucket and missing rows in the correct bucket.

Additionally, all historical boundary-day forecasts were computed against the
wrong pentad/decad's norm discharge and training data. The forecasted values
themselves are subtly wrong and would need to be re-run to be corrected.

---

## Implementation Plan

### Phase 1: Fix the horizon offset in `linear_regression.py`

**Goal**: Apply `current_day + timedelta(days=1)` when computing
`forecast_pentad_of_year` and `forecast_decad_of_year`, so LR tags its
forecasts with the target period rather than the production date's period.

**Files allowed to modify**:
- `apps/linear_regression/linear_regression.py`

**Changes**:

1. The file imports `import datetime as dt` (line 102). Use `dt.timedelta(days=1)` —
   no import changes needed.

2. Replace line 745:
   ```python
   # Before
   forecast_pentad_of_year = tl.get_pentad_in_year(current_day)
   # After
   forecast_pentad_of_year = tl.get_pentad_in_year(current_day + dt.timedelta(days=1))
   ```

3. Replace line 846:
   ```python
   # Before
   forecast_decad_of_year = tl.get_decad_in_year(current_day)
   # After
   forecast_decad_of_year = tl.get_decad_in_year(current_day + dt.timedelta(days=1))
   ```

**CRITICAL CONSTRAINT**: Do NOT change any other logic. `forecast_pentad_of_year`
and `forecast_decad_of_year` are used downstream in `fl.save_discharge_avg`,
`fl.perform_linear_regression` (as `forecast_horizon_int`), and
`fl.perform_forecast` — all of these correctly receive the updated value
without any further modification. Do NOT change any function signatures, data
flow logic, or control flow. These are the only two lines that may be
modified.

**Verification required before implementation**: Trace `forecast_pentad_of_year`
through all four downstream calls (lines 749, 767, 777 for pentad; 850, 865,
874 for decad). Confirm that each call semantically wants the TARGET period
(next pentad/decad), not the production date's period. In particular,
`fl.save_discharge_avg` uses this value as a group key for norm discharge —
verify that norm-discharge lookups elsewhere in the pipeline use the same
target-period convention.

**Acceptance criteria**:
- On day 25 (March 25): `forecast_pentad_of_year == "18"` (not `"17"`)
- On day 5 (March 5): `forecast_pentad_of_year == "14"` (not `"13"`)
- On day 10 (March 10): `forecast_decad_of_year == "8"` (not `"7"`)
- On non-boundary days (e.g., March 12): result is unchanged
- Existing tests still pass

---

### Phase 2: Add tests to verify correct pentad/decad mapping on boundary days

**Goal**: Cover the corrected mapping with targeted unit and regression tests
so this offset cannot silently regress.

**Files allowed to modify**:
- `apps/linear_regression/test/test_horizon_offset.py` (new file)

**Test matrix**:

| # | Input date | Horizon type | Expected `horizon_in_year` | Notes |
|---|------------|-------------|---------------------------|-------|
| 1 | 2026-03-25 | pentad | `"18"` | Boundary day: last day of pentad 17, target is pentad 18 |
| 2 | 2026-03-05 | pentad | `"14"` | Boundary day: last day of pentad 13, target is pentad 14 |
| 3 | 2026-02-28 | pentad | `"13"` | End-of-month boundary, non-leap: Feb 28+1 = Mar 1 = pentad 13 |
| 4 | 2024-02-29 | pentad | `"13"` | End-of-month boundary, leap year: Feb 29+1 = Mar 1 = pentad 13 |
| 5 | 2026-03-12 | pentad | `"15"` | Non-boundary day: Mar 12+1 = Mar 13, both in pentad 15; no change vs old logic |
| 6 | 2026-03-10 | decad  | `"8"`  | Boundary day: last day of decad 7, target is decad 8 |
| 7 | 2026-03-20 | decad  | `"9"`  | Boundary day: last day of decad 8, target is decad 9 |
| 8 | 2026-03-31 | decad  | `"10"` | End-of-month decad boundary: Mar 31+1 = Apr 1 = decad 10 |
| 9 | 2026-03-15 | decad  | `"8"`  | Safety check (not a real decad forecast day): Mar 15+1 = Mar 16, both in decad 8. All actual decad forecast days (10, 20, EOM) are boundary days — this row verifies the formula is harmless on non-forecast dates |
| 10 | 2026-12-31 | decad | `"1"` (next year) | Year-end decad boundary: Dec 31+1 = Jan 1 next year = decad 1 |

Each test should directly call `tl.get_pentad_in_year(date + timedelta(days=1))`
or `tl.get_decad_in_year(date + timedelta(days=1))` to assert the target
period value (unit-level contract tests on the offset logic), plus at least
one integration-level test that calls into the LR pipeline with a mocked
environment to verify the `forecast_pentad_of_year` variable is set
correctly before it reaches `fl.save_discharge_avg`.

**CRITICAL CONSTRAINT**: Do NOT change any other file. Tests must use mocked
API clients and environments (`SAPPHIRE_TEST_ENV=True`). No live API calls,
no live filesystem writes outside `tmp_path`.

**Acceptance criteria**:
- All 10 tests (plus the integration smoke test) pass with:
  ```bash
  cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
  ```
- Zero skips (except valid `SAPPHIRE_API_AVAILABLE` dependency-gate skips)

---

### Phase 3: Historical data remediation

**Goal**: Assess the scope of incorrect `horizon_in_year` values already
committed to the database and execute a targeted correction.

**This phase is analysis + a migration script. No application code changes.**

**Step 3a — Scope assessment** (manual, before writing the migration):

1. Query the `lr_forecasts` table (or equivalent in the postprocessing API)
   for rows where `date` is a pentad boundary day and `horizon_in_year`
   equals the pentad of `date` (wrong) rather than the pentad of `date + 1`
   (correct):
   ```sql
   -- Pentad boundary days are where day IN (5, 10, 15, 20, 25)
   -- or day = last_day_of_month.
   -- Wrong rows: horizon_in_year = get_pentad_in_year(date)
   -- Correct:    horizon_in_year = get_pentad_in_year(date + 1)
   SELECT COUNT(*) FROM lr_forecasts
   WHERE EXTRACT(DAY FROM date) IN (5, 10, 15, 20, 25)
     OR date = DATE_TRUNC('month', date) + INTERVAL '1 month' - INTERVAL '1 day';
   ```
2. Cross-reference with `combined_forecasts` / `skill_metrics` tables to
   understand downstream contamination.
3. Document the earliest affected date and total row count before proceeding.

**Step 3b — Migration script**:

Write a one-time migration script
`apps/linear_regression/migrations/fix_horizon_in_year_boundary_days.py`
that:

- Connects to the postprocessing API (or DB directly) using the existing
  `sapphire_api_client` pattern
- Identifies all LR forecast rows with wrong `horizon_in_year` on boundary
  dates (using the logic above)
- Updates `horizon_in_year` (and `horizon_value` if it stores pentad-in-month)
  to the correct target-period value
- Is **idempotent**: running it twice must produce the same result
- Prints a summary of rows inspected, rows corrected, and rows skipped
- Accepts `--dry-run` flag to report without modifying

**Files allowed to create**:
- `apps/linear_regression/migrations/fix_horizon_in_year_boundary_days.py`

**CRITICAL CONSTRAINT**: This migration touches the `sapphire/services/`
database indirectly via the API client. Do NOT modify any service code. If
the API does not expose a bulk-update endpoint, escalate to the service owner
before proceeding. Do not issue raw SQL against the production database
without coordination.

**Acceptance criteria**:
- `--dry-run` reports the correct count of affected rows on a test dataset
- After running without `--dry-run`, no boundary-day LR rows have
  `horizon_in_year` equal to the production date's period
- Script is idempotent (second run reports 0 rows corrected)
- A decision has been made about whether to invalidate and recalculate
  affected skill metrics — document the decision in the PR

---

## Acceptance Criteria (overall)

- [ ] `forecast_pentad_of_year` on day 25 returns `"18"`, not `"17"`
- [ ] `forecast_decad_of_year` on day 10 returns `"8"`, not `"7"`
- [ ] All boundary-day test cases pass with zero skips
- [ ] Full linear_regression test suite passes with zero skips
- [ ] Historical data migration script exists, is idempotent, and has been
      reviewed
- [ ] Scope assessment (3a) is documented in the PR: row count, date range,
      downstream impact
- [ ] No changes to `sapphire/services/` without coordination with the
      service owner

---

## Testing Plan

```bash
# Phase 1 + 2 verification
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression

# After Phase 3 migration (staging environment)
python apps/linear_regression/migrations/fix_horizon_in_year_boundary_days.py --dry-run
python apps/linear_regression/migrations/fix_horizon_in_year_boundary_days.py
python apps/linear_regression/migrations/fix_horizon_in_year_boundary_days.py --dry-run  # should report 0
```

Manual spot-check: run the LR pipeline for a known boundary day (e.g.,
`forecast_date=2026-03-25` in hindcast mode) and verify:
(a) the written `horizon_in_year` is `18` (not `17`),
(b) the norm discharge used was for pentad 18 (March 26–31 historical data),
(c) the regression training data was filtered to pentad 18 observations.

---

## Risks and Considerations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Non-boundary days regressed by the fix | Low | `tl.get_pentad_in_year(date + 1)` on a non-boundary day returns the same value as `tl.get_pentad_in_year(date)` because the next day is still in the same period. Tests 5 and 9 explicitly cover this. |
| Historical skill metrics computed from wrong pentad buckets | High | Phase 3 migration corrects the LR forecast rows; a follow-on decision is needed on whether to re-run skill metric recalculation for affected stations and periods. Note: historical forecasted values themselves are also wrong (computed against the wrong pentad's norm/training data) and would need re-running to correct. |
| `fl.save_discharge_avg` uses `forecast_pentad_of_year` as `group_id` for norm discharge | Not a risk — it's a **benefit** | Currently on March 25, LR fetches pentad-17 norms for a pentad-18 forecast (wrong). The fix correctly selects pentad-18 norms. The historical `pentad_in_year` in `discharge_pentad` is computed per-row from each row's own date (`forecast_library.py:1013`), independent of `current_day`. Note: `perform_linear_regression` also uses `forecast_pentad_of_year` to filter training data (`data_df[horizon_col] == forecast_horizon_int`), so the fix similarly corrects the training data selection — both norm and training data are improved. |
| DB unique-key conflict if a correct row already exists at the same `(code, date, horizon_in_year=18)` | Low | Only arises if a previous run wrote a correct row; the API uses upsert — confirm the postprocessing API upsert key includes `horizon_in_year`. If not, coordinate with service owner before running migration. |
| Decad boundary: day 31 varies by month | Low | `tl.get_decad_in_year` uses `min((day - 1) // 10 + 1, 3)` so day 31 is already decad 3. `day 31 + 1 = day 1 of next month`, which is decad 1 of the next month. Verify the year-end boundary (Dec 31 → Jan 1) does not produce an out-of-range value. |

### Backward Compatibility

The fix changes the `horizon_in_year` tag written by LR for all future
boundary-day runs. Downstream consumers (postprocessing, dashboards, skill
metrics) all use the target-period convention — so the fix makes LR
consistent with the rest of the system. There is no backward compatibility
concern for new data. Historical data requires Phase 3.

### Impact on Forecast Values

This fix changes **both** the metadata tag (`horizon_in_year`) **and** the
forecasted discharge value itself. On boundary days, the fix causes LR to
train on the correct pentad/decad's historical data and use the correct
norm discharge, producing more accurate forecasts going forward. Historical
boundary-day forecasts were computed against the wrong pentad's data and
would need to be re-run (Phase 3) to be corrected.

---

## Related Issues

- **PP-022**: Stale-record refresh in maintenance pipeline — also affects
  combined forecast availability
- **LR-007**: Silent API write failures — **prerequisite for Phase 3**: the
  migration script writes corrected records to the API; if writes fail silently
  (the bug LR-007 fixes), the migration would report success but leave the DB
  unchanged. Land LR-007 before executing Phase 3.
- **ML-012**: NaN flag crash — if ML forecasts are also missing, the combined
  forecast gap from this bug is wider
- **INFRA-015**: Pentad/decade boundary audit — confirmed this bug as its only
  finding; downgraded to mid priority

---

## Dependency Graph

```json
{
  "phases": {
    "phase_1": {
      "title": "Fix horizon offset in linear_regression.py",
      "file": "apps/linear_regression/linear_regression.py",
      "changes": [
        "Line 745: pass current_day + dt.timedelta(days=1) to get_pentad_in_year",
        "Line 846: pass current_day + dt.timedelta(days=1) to get_decad_in_year"
      ],
      "depends_on": [],
      "parallel_with": []
    },
    "phase_2": {
      "title": "Add boundary-day tests",
      "file": "apps/linear_regression/test/test_horizon_offset.py",
      "changes": [
        "10 unit tests covering pentad and decad boundary and non-boundary days",
        "1 integration smoke test verifying forecast_pentad_of_year before write"
      ],
      "depends_on": ["phase_1"],
      "parallel_with": []
    },
    "phase_3": {
      "title": "Historical data remediation",
      "file": "apps/linear_regression/migrations/fix_horizon_in_year_boundary_days.py",
      "changes": [
        "Scope assessment: count affected rows and document date range",
        "Idempotent migration script with --dry-run support"
      ],
      "depends_on": ["phase_2"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": false,
      "agents": [
        {
          "id": "agent_fix",
          "phases": ["phase_1"],
          "reason": "Two-line fix in linear_regression.py — fully isolated"
        }
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {
          "id": "agent_tests",
          "phases": ["phase_2"],
          "reason": "New test file — depends on phase_1 being complete and tested"
        }
      ]
    },
    {
      "group": 3,
      "parallel": false,
      "agents": [
        {
          "id": "agent_migration",
          "phases": ["phase_3"],
          "reason": "Migration script — depends on the fix being deployed and tested"
        }
      ]
    }
  ]
}
```
