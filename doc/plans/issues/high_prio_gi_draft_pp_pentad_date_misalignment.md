# PP-031: Pentad/decad aggregation does not select boundary issue days (shared code path)

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `bug`, `data-quality`, `postprocessing`

---

## Summary

`postprocessing_forecasts` aggregates ML daily forecasts into pentad/decad records for **every ML run date** instead of only for pentad/decad boundary days. This produces spurious records on non-boundary dates (e.g., Mar 19, 24) while missing records on actual issue days (e.g., Mar 25). The same code path handles both pentad and decad.

## Context

The ML module (`machine_learning`) writes daily forecasts every day it runs, with `horizon=day` and the actual run date as `date`. Each daily forecast includes individual `target` dates (not a target period).

Pentad issue days: 5, 10, 15, 20, 25, last day of month.
Decad issue days: 10, 20, last day of month.

`postprocessing_forecasts` is responsible for:
1. Reading ML daily forecasts from the forecasts table
2. Aggregating daily targets to pentad/decad level (averaging targets within the period)
3. Computing EM (ensemble mean with LR) and NE (neural ensemble = average over ML models) combined forecasts
4. Writing pentad/decad records to the `forecast` endpoint

## Problem

**Observed during**: Local pipeline review checklist (`review_checklist_local_2026-03-28.md`).

Querying pentad forecasts for March 25, 2026 (a pentad issue day) returns 0 records, despite:
- ML daily forecasts existing for `issue_date=2026-03-25` with 11 target days (Mar 26 – Apr 5)
- LR pentad forecast existing at `lr-forecast` endpoint with `date=2026-03-25`

The error is **not** a systematic off-by-one. Full pentad forecast dates (Feb–Mar 2026):

```
date        boundary?  models
2026-02-04  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-02-05  YES        [EM, NE]                         ← missing ML models
2026-02-09  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-02-14  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-02-19  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-02-20  YES        [EM, NE]                         ← missing ML models
2026-02-24  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-02-25  YES        [EM, NE]                         ← missing ML models
2026-02-28  YES        [EM, NE, TFT, TSMixer, TiDE]   ← correct
2026-03-05  YES        [EM, NE, TFT, TSMixer, TiDE]   ← correct
2026-03-10  YES        [EM, NE, TFT, TSMixer, TiDE]   ← correct
2026-03-15  YES        [EM, NE]                         ← missing ML models
2026-03-19  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-03-20  YES        [EM, NE, TFT, TSMixer, TiDE]   ← correct
2026-03-24  NO         [EM, NE, TFT, TSMixer, TiDE]   ← spurious
2026-03-25  YES        — MISSING —                      ← missing entirely
```

**Pattern**: ML pentad records appear on whatever dates ML happened to run (often the day before a boundary). On boundary days where ML didn't run, only EM/NE appear (from LR only). Some boundary days are correct by coincidence (ML ran on the boundary day itself).

**Impact**:
- Non-boundary dates have spurious pentad records
- Boundary dates often missing ML model pentad records
- LR forecasts (correctly dated to issue day) and combined forecasts are misaligned
- Skill metric computation pairs wrong dates with observations

## Desired Outcome

- Pentad/decad ML aggregated forecasts exist ONLY on boundary dates
- For each boundary date, average only ML daily targets in the next pentad/decad
- On dates that are both pentad AND decad boundaries (10, 20, EOM), fetch daily forecasts once, aggregate to both pentad and decad averages using the appropriate number of target days
- No spurious records on non-boundary dates

---

## Technical Analysis

### Key Files

- `apps/postprocessing_forecasts/src/data_reader.py:1825-1952` — `_normalize_ml_forecasts()` aggregates daily→pentad/decad
- `apps/postprocessing_forecasts/postprocessing_operational.py` — operational entry point (has `is_pentad_boundary`/`is_decad_boundary` guards)
- `apps/postprocessing_forecasts/postprocessing_maintenance.py` — maintenance gap-fill (lookback: `POSTPROCESSING_GAPFILL_MAX_MONTHS=13`)

### Daily ML Forecast Structure

Each ML daily record in the forecasts table has:
- `date` — the ML run/issue date (e.g., 2026-03-25)
- `target` — individual target date (e.g., 2026-03-26), NOT a period number
- `forecasted_discharge`, `q05`, `q25`, `q75`, `q95` — values
- `flag` — quality flag

There is **no `target_period` field** — the target period must be computed from individual target dates. This needs investigation: the current code uses `tl.get_pentad_in_year(target)` to determine which pentad a target falls in, but this should be verified.

### Current Dataflow (OPERATIONAL)

```
postprocessing_operational.py
  │
  ├─ today = dt.date.today()
  ├─ if is_pentad_boundary(today): run pentad postprocessing    ← correct guard
  ├─ if is_decad_boundary(today): run decad postprocessing      ← correct guard
  │
  └─ _run_short_term_postprocessing(PENTAD, today, ...)
       │
       └─ data_reader.read_observed_and_modelled_data("pentad",
            │     start_year=today.year, end_year=today.year)
            │
            └─ read_individual_model_forecasts("pentad", start_year, end_year)
                 │
                 ├─ for each ML model (TFT, TiDE, TSMixer):
                 │    ├─ _read_ml_forecasts_pp_api(model, "pentad")
                 │    │    └─ queries API: horizon=day,
                 │    │       start_date={year}-01-01, end_date={year}-12-31
                 │    │       ← PROBLEM: fetches ALL daily forecasts for the year
                 │    │          should only fetch for today (the boundary day)
                 │    │
                 │    └─ _normalize_ml_forecasts(ml_raw, model, "pentad")
                 │         ├─ Filter: keep targets where pentad_in_year(target)
                 │         │    == pentad_in_year(date + 1 day)       (PP-023)
                 │         ├─ Aggregate: groupby(["code", "date"]).mean()
                 │         │    ← PROBLEM: groups by EVERY ML run date,
                 │         │       not just boundary days
                 │         └─ Compute pentad_in_year from date + 1 day
                 │
                 └─ concat all models → return
```

**The operational guard (`is_pentad_boundary`) is correct** — it only runs on boundary days. But the data reader fetches the entire year and aggregates every date, producing records for all run dates. These non-boundary records get written to the DB alongside the correct boundary-day records.

### Current Dataflow (MAINTENANCE)

```
postprocessing_maintenance.py
  │
  ├─ NO boundary day check
  ├─ gap_detector.detect_missing_ensembles()
  │    → finds dates where LR or ML pentad exists but EM/NE missing
  ├─ Lookback: POSTPROCESSING_GAPFILL_MAX_MONTHS (default: 13)
  │
  └─ data_reader.read_individual_model_forecasts_for_dates("pentad", affected_dates)
       │
       └─ read_individual_model_forecasts("pentad", min_year, max_year)
            │
            └─ (same normalize path — fetches all dates in year range)
                 │
                 └─ post-filter: forecasts[forecasts["date"].isin(date_set)]
                      ← only keeps dates the gap detector found
                      ← gap detector may find non-boundary dates
                         (spurious ML pentad records trigger EM/NE creation)
```

**Maintenance problems:**
1. Gap detector may flag non-boundary dates where ML pentad records exist but EM/NE don't
2. Fetches entire year(s) of daily data when only specific boundary dates are needed

### Intended Dataflow (OPERATIONAL)

```
postprocessing_operational.py
  │
  ├─ today = dt.date.today()
  ├─ if is_pentad_boundary(today): run pentad
  ├─ if is_decad_boundary(today): run decad
  │    (dates like 10th, 20th, EOM are BOTH — fetch daily forecasts once,
  │     aggregate to pentad AND decad using different target windows)
  │
  └─ For the boundary day (today):
       ├─ Fetch ML daily forecasts ONLY for date=today
       ├─ For pentad: filter targets to next pentad dates, average → write
       └─ For decad: filter targets to next decad dates, average → write
```

### Intended Dataflow (MAINTENANCE)

```
postprocessing_maintenance.py
  │
  ├─ Detect missing EM/NE on BOUNDARY DAYS ONLY within lookback window
  │    e.g., Feb pentad boundaries: 5, 10, 15, 20, 25, 28
  │         Mar pentad boundaries: 5, 10, 15, 20, 25, 31
  │    Missing: Feb 20, Mar 15, Mar 25 (where ML daily exists but EM/NE absent)
  │
  └─ For each missing boundary date:
       ├─ Fetch ML daily forecasts ONLY for that date
       ├─ Filter targets to next pentad/decad dates
       ├─ Average → compute EM, NE → write
       └─ (same aggregation as operational)
```

### Target Filtering Detail (needs investigation)

The current code computes target period using `tl.get_pentad_in_year(target)`. Since daily ML forecasts store individual `target` dates (e.g., `"target": "2026-03-26"`), not period numbers, the filtering works by:

1. Computing `expected_period = get_pentad_in_year(issue_date + 1 day)`
2. Computing `actual_period = get_pentad_in_year(target_date)`
3. Keeping rows where they match

**Needs verification**: Does `get_pentad_in_year` correctly identify pentad boundaries for all edge cases (EOM with varying month lengths, leap years)? The `target` column IS present on daily ML records — confirmed from API response.

---

## Implementation Plan

### Steps

- [ ] Step 1: Confirm the fix location — verify `_normalize_ml_forecasts` is the right place to add boundary filtering, or if it should be in the caller
- [ ] Step 2: Add boundary-day filter: before target filtering, drop rows where `date` is not a pentad/decad issue day
- [ ] Step 3: Optimize API fetch — in operational mode, query only `date=today` instead of the entire year. In maintenance mode, query only the specific missing boundary dates
- [ ] Step 4: Move `is_pentad_boundary()` / `is_decad_boundary()` to shared utility (currently in `postprocessing_operational.py:54-63`)
- [ ] Step 5: Update maintenance gap detector to only look for missing EM/NE on boundary dates
- [ ] Step 6: Handle dual-boundary dates (10, 20, EOM are both pentad AND decad) — fetch once, aggregate twice
- [ ] Step 7: Clean up spurious non-boundary pentad/decad records already in the DB
- [ ] Step 8: Run operational + maintenance and confirm correct records appear

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/data_reader.py:1825-1952` | `_normalize_ml_forecasts()` — add boundary-day filter before target filtering |
| `apps/postprocessing_forecasts/src/data_reader.py` | `_read_ml_forecasts_pp_api()` — optimize to fetch only needed dates, not entire year |
| `apps/postprocessing_forecasts/postprocessing_operational.py:54-63` | Move `is_pentad_boundary()` / `is_decad_boundary()` to shared `src/date_utils.py` |
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | Update gap detector to only flag boundary dates |

---

## Testing

### Manual Verification

```bash
# After fix, verify Mar 25 pentad records exist
curl -s "$BASE_URL/api/postprocessing/forecast/?code=15189&horizon=pentad&start_date=2026-03-25&end_date=2026-03-25&limit=50" | table
# Expect: EM, NE, TFT, TiDE, TSMixer records with date=2026-03-25

# Verify NO records on non-boundary dates
curl -s "$BASE_URL/api/postprocessing/forecast/?code=15189&horizon=pentad&start_date=2026-03-24&end_date=2026-03-24&limit=50" | table
# Expect: (no records)

# Verify alignment with LR
curl -s "$BASE_URL/api/postprocessing/lr-forecast/?code=15189&horizon=pentad&start_date=2026-03-25&end_date=2026-03-25&limit=5" | table
# Expect: LR record also at date=2026-03-25
```

---

## Documentation Impact

- [ ] No documentation impact — this is a date selection bug fix

## Out of Scope

- Daily ensemble creation (PP-012) — separate concern
- Long-term forecast dating (different code path)
- Cleaning up all historical spurious records (may need a separate migration)

## Dependencies

- PP-023 (complete) — period-aware target filtering, prerequisite context

## Acceptance Criteria

- [ ] Pentad combined forecasts exist ONLY on pentad issue days (5/10/15/20/25/EOM)
- [ ] Decad combined forecasts exist ONLY on decad issue days (10/20/EOM)
- [ ] Query by pentad issue day returns EM + NE + ML model records
- [ ] LR forecast `date` and combined forecast `date` are aligned for the same pentad/decad
- [ ] No spurious records on non-boundary dates
- [ ] Maintenance gap-fill only targets boundary dates
- [ ] Dual-boundary dates (10, 20, EOM) produce both pentad and decad records
- [ ] Existing tests pass

---

## References

- Related completed issue: PP-023 (period-aware aggregation)
- Discovered: `review_checklist_local_2026-03-28.md`
- Key code: `apps/postprocessing_forecasts/src/data_reader.py` — `_normalize_ml_forecasts()`
- Operational boundary guards: `apps/postprocessing_forecasts/postprocessing_operational.py:54-63`
- Maintenance lookback: `POSTPROCESSING_GAPFILL_MAX_MONTHS=13` (default)
