# PP-031: Pentad/decad aggregation assigns wrong issue date (off-by-one from pentad boundary)

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `bug`, `data-quality`, `postprocessing`

---

## Summary

`postprocessing_forecasts` does not aggregate ML daily forecasts on pentad issue days. Instead of selecting ML forecasts issued on pentad boundary days (5/10/15/20/25/EOM) and averaging targets in the next pentad, it aggregates on arbitrary ML run dates (e.g., March 24 instead of March 25). This causes missing pentad forecasts on actual pentad issue days.

## Context

The ML module (`machine_learning`) writes daily forecasts every day it runs, with `horizon=day` and the actual run date as `date`. Pentad issue days are 5, 10, 15, 20, 25, and the last day of the month.

`postprocessing_forecasts` is responsible for:
1. Reading ML daily forecasts
2. Aggregating daily targets to pentad/decad level (averaging targets within the period)
3. Computing EM (ensemble mean) and NE (norm error) combined forecasts
4. Writing pentad/decad records to the `forecast` endpoint

The aggregated pentad records should use the pentad issue day as their `date`, not the ML run date.

## Problem

**Observed during**: Local pipeline review checklist (`review_checklist_local_2026-03-28.md`).

Querying pentad forecasts for March 25, 2026 (a pentad issue day) returns 0 records, despite:
- ML daily forecasts existing for `issue_date=2026-03-25` with 11 target days (Mar 26 – Apr 5)
- LR pentad forecast existing at `lr-forecast` endpoint with `date=2026-03-25`

Instead, the pentad combined forecasts are dated March 24:

```
Pentad forecast dates Mar 15-31: ['2026-03-15', '2026-03-19', '2026-03-20', '2026-03-24']
  2026-03-20: ['EM', 'NE', 'TFT', 'TSMixer', 'TiDE']
  2026-03-24: ['EM', 'NE', 'TFT', 'TSMixer', 'TiDE']
```

The gap-fill maintenance run (`postprocessing_maintenance.py`) also does not produce a March 25 record.

**Impact**:
- Pentad forecasts are systematically misdated by 1 day when ML runs the day before a pentad boundary
- Consumers querying by pentad issue day get empty results
- LR forecasts (correctly dated to issue day) and combined forecasts (misdated) are misaligned
- Skill metric computation may pair the wrong forecast with observations

## Desired Outcome

- Pentad aggregated forecasts use the pentad issue day (5/10/15/20/25/EOM) as their `date`
- A pentad forecast query for any issue day returns the aggregated ML + EM + NE records
- The `date` field aligns between LR forecasts and combined forecasts for the same pentad

---

## Technical Analysis

### Current Implementation

The aggregation logic lives in `postprocessing_forecasts`. Key areas to investigate:

**Key files:**
- `apps/postprocessing_forecasts/src/data_reader.py` — `_normalize_ml_forecasts()` aggregates daily→pentad
- `apps/postprocessing_forecasts/postprocessing_operational.py` — operational entry point
- `apps/postprocessing_forecasts/postprocessing_maintenance.py` — maintenance gap-fill

PP-023 (completed) fixed which daily targets are included in the aggregation, but did not address the date assignment of the resulting pentad record.

### Correct Aggregation Logic

The pentad issue day is the **last day of the previous pentad** (5, 10, 15, 20, 25, or EOM). On each pentad issue day, postprocessing should:

1. **Select** ML daily forecasts (TFT, TiDE, TSMixer) **issued on the pentad issue day** (e.g., `date=2026-03-25`)
2. **Filter targets** to only those falling within the **next pentad** (e.g., Mar 26 – Mar 31 for the issue day Mar 25)
3. **Average** the daily `forecasted_discharge` values across those targets → this is the pentad forecast
4. **Write** the result to `forecast` table with `date=2026-03-25, horizon=pentad`

The ML run date is correctly preserved as the issue date — the bug is that postprocessing is not recognizing pentad issue days and is instead aggregating on arbitrary ML run dates (e.g., Mar 24).

### Evidence

ML daily forecasts exist for the pentad issue day Mar 25:
```
issue=2026-03-25  targets: Mar 26, 27, 28, 29, 30, 31, Apr 1, 2, 3, 4, 5  (flag=0, 11 records)
```

Targets Mar 26–31 fall within the next pentad. But postprocessing produced pentad records for `date=2026-03-24` (not a pentad issue day) and none for `date=2026-03-25`.

### Root Cause

The aggregation code does not identify pentad issue days. It likely processes all available ML daily dates and picks whichever date happens to be available, rather than specifically selecting forecasts issued on pentad boundary days (5/10/15/20/25/EOM).

---

## Implementation Plan

### Investigation Steps

- [ ] Step 1: Read `_normalize_ml_forecasts()` in `data_reader.py` to understand how the pentad `date` is determined
- [ ] Step 2: Identify where the code selects which ML issue dates to aggregate — does it iterate all dates or specifically look for pentad issue days?
- [ ] Step 3: Fix: ensure the code only aggregates for pentad issue days, using ML daily forecasts issued on that exact day
- [ ] Step 4: For each pentad issue day, average only targets within the next pentad period (PP-023 already handles target filtering)
- [ ] Step 5: Verify that maintenance gap-fill also applies pentad-issue-day selection
- [ ] Step 6: Same logic applies for decad (issue days 10, 20, EOM)

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/data_reader.py` | Fix date selection in `_normalize_ml_forecasts()` — only aggregate on pentad/decad issue days |

---

## Testing

### Manual Verification

```bash
# After fix, verify Mar 25 pentad records exist
curl -s "$BASE_URL/api/postprocessing/forecast/?code=15189&horizon=pentad&start_date=2026-03-25&end_date=2026-03-25&limit=50" | table
# Expect: EM, NE, TFT, TiDE, TSMixer records with date=2026-03-25

# Verify alignment with LR
curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=15189&horizon=pentad&start_date=2026-03-25&end_date=2026-03-25&limit=5" | table
# Expect: LR record also at date=2026-03-25
```

---

## Documentation Impact

- [ ] No documentation impact — this is a date assignment bug fix

## Out of Scope

- Daily ensemble creation (PP-012) — separate concern
- Decad dating (likely same issue but verify separately)
- Long-term forecast dating (different code path)

## Dependencies

- PP-023 (complete) — period-aware target filtering, prerequisite context

## Acceptance Criteria

- [ ] Pentad combined forecasts on pentad issue days (5/10/15/20/25/EOM) have `date` matching the issue day
- [ ] Query by pentad issue day returns EM + NE + ML model records
- [ ] LR forecast `date` and combined forecast `date` are aligned for the same pentad
- [ ] Decad combined forecasts similarly aligned (10/20/EOM)
- [ ] Existing tests pass
- [ ] Maintenance gap-fill produces correctly-dated records

---

## References

- Related completed issue: PP-023 (period-aware aggregation)
- Discovered: `review_checklist_local_2026-03-28.md`
- Key code: `apps/postprocessing_forecasts/src/data_reader.py` — `_normalize_ml_forecasts()`
