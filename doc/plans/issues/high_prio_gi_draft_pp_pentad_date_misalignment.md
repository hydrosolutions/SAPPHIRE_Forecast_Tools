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

There is **no `target_period` field** — the target period is computed from individual target dates using `tl.get_pentad_in_year(target)`. This logic (PP-023) has been verified correct for all edge cases including EOM, month boundaries, and dual pentad/decad dates.

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
- [ ] Step 3: Move `is_pentad_boundary()` / `is_decad_boundary()` to shared utility (currently in `postprocessing_operational.py:54-63`)
- [ ] Step 5: Update maintenance gap detector to only look for missing EM/NE on boundary dates
- [ ] Step 6: Run operational + maintenance and confirm correct records appear
- [ ] Step 7 (follow-up): Clean up spurious non-boundary pentad/decad records already in the DB
- [ ] Step 8 (follow-up): Verify dashboard no longer shows phantom forecasts on non-boundary dates

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/data_reader.py:1825-1952` | `_normalize_ml_forecasts()` — add boundary-day filter before target filtering. This is the minimal fix that covers both operational and maintenance paths. |
| `apps/postprocessing_forecasts/postprocessing_operational.py:54-63` | Move `is_pentad_boundary()` / `is_decad_boundary()` to shared `src/date_utils.py` so `_normalize_ml_forecasts` can import them. |
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | Verify gap detector only flags boundary dates. |

---

## Implementation Safety Guardrails

### Constraints for the implementing agent

1. **Do NOT change the PP-023 target filtering logic** (lines 1858-1879). It is verified correct for all edge cases. The boundary-day filter is a NEW step inserted BEFORE the existing target filter.

2. **Do NOT change the API query scope** in `_read_ml_forecasts_pp_api`. The fix is purely in `_normalize_ml_forecasts` — a read-time filter, not a query change.

3. **Do NOT change the write path** in `api_writer.py` or `file_writer.py`.

4. **Do NOT change `postprocessing_operational.py`** beyond extracting the boundary functions to a shared module. The existing `is_pentad_boundary`/`is_decad_boundary` guards at the entry point are correct and must remain.

5. **The boundary-day filter must be inserted AFTER the `pd.to_datetime` conversion** (line 1853) and BEFORE the target filter (line 1858). Exact insertion point: between lines 1853 and 1855.

6. **Type safety**: The `date` column is `pd.Timestamp` at the insertion point. The `is_pentad_boundary`/`is_decad_boundary` functions work with `pd.Timestamp` (verified — it has `.year`, `.month`, `.day` attributes). No type conversion needed.

### Required tests

All tests use **fake DataFrames** (no mocks on the function under test). TAG_LIBRARY
is real (resolved via sys.path). Tests follow the existing pattern in
`test_data_reader_ml_aggregation.py`: inline `pd.DataFrame({...})` with string dates,
`pytest.approx` for floats, `assert result.empty` for empty results.

#### A. Unit tests for boundary-day filter (in `test_data_reader_ml_aggregation.py`)

```python
class TestBoundaryDayFiltering:
    """PP-031: Only boundary-day ML records should survive normalization."""

    def test_non_boundary_pentad_date_dropped(self):
        """ML record with date=Jan 4 (not a pentad boundary) is dropped."""
        raw = pd.DataFrame({
            "code": ["10001"] * 6,
            "date": ["2024-01-04"] * 6,
            "target": ["2024-01-05", "2024-01-06", "2024-01-07",
                        "2024-01-08", "2024-01-09", "2024-01-10"],
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty

    def test_boundary_pentad_date_kept(self):
        """ML record with date=Jan 5 (pentad boundary) is kept and aggregated."""
        raw = pd.DataFrame({
            "code": ["10001"] * 6,
            "date": ["2024-01-05"] * 6,
            "target": ["2024-01-06", "2024-01-07", "2024-01-08",
                        "2024-01-09", "2024-01-10", "2024-01-11"],
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Targets Jan 6-10 are pentad 2 (matching date+1=Jan 6); Jan 11 is pentad 3 → dropped
        # Mean of 10, 20, 30, 40, 50 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_eom_boundary_pentad_28day_month(self):
        """ML record with date=Feb 28 2025 (EOM non-leap, pentad boundary) is kept."""
        raw = pd.DataFrame({
            "code": ["10001"] * 6,
            "date": ["2025-02-28"] * 6,
            "target": ["2025-03-01", "2025-03-02", "2025-03-03",
                        "2025-03-04", "2025-03-05", "2025-03-06"],
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Targets Mar 1-5 are pentad 13 (matching date+1=Mar 1); Mar 6 is pentad 14 → dropped
        # Mean of 10, 20, 30, 40, 50 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_eom_boundary_pentad_31day_month(self):
        """ML record with date=Mar 31 (EOM 31-day month, pentad boundary) is kept."""
        raw = pd.DataFrame({
            "code": ["10001"] * 6,
            "date": ["2024-03-31"] * 6,
            "target": ["2024-04-01", "2024-04-02", "2024-04-03",
                        "2024-04-04", "2024-04-05", "2024-04-06"],
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Targets Apr 1-5 are pentad 19; Apr 6 is pentad 20 → dropped
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_leap_year_eom_feb29(self):
        """ML record with date=Feb 29 2024 (leap year EOM, pentad boundary) is kept."""
        raw = pd.DataFrame({
            "code": ["10001"] * 5,
            "date": ["2024-02-29"] * 5,
            "target": ["2024-03-01", "2024-03-02", "2024-03-03",
                        "2024-03-04", "2024-03-05"],
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_non_boundary_decad_date_dropped(self):
        """ML record with date=Jan 15 (pentad boundary but NOT decad boundary) is dropped for decad."""
        raw = pd.DataFrame({
            "code": ["10001"] * 10,
            "date": ["2024-01-15"] * 10,
            "target": [f"2024-01-{d}" for d in range(16, 26)],
            "forecasted_discharge": [float(i) for i in range(10)],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert result.empty

    def test_boundary_decad_date_kept(self):
        """ML record with date=Jan 10 (decad boundary) is kept for decad."""
        raw = pd.DataFrame({
            "code": ["10001"] * 11,
            "date": ["2024-01-10"] * 11,
            "target": [f"2024-01-{d}" for d in range(11, 22)],
            "forecasted_discharge": [float(i * 10) for i in range(11)],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "decad")
        assert len(result) == 1
        # Targets Jan 11-20 are decad 2 (10 days); Jan 21 is decad 3 → dropped
        # Mean of 0,10,20,30,40,50,60,70,80,90 = 45.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(45.0)

    def test_mixed_boundary_and_non_boundary(self):
        """Only boundary dates survive when input has both."""
        raw = pd.DataFrame({
            "code": ["10001"] * 12,
            "date": (["2024-01-04"] * 6) + (["2024-01-05"] * 6),
            "target": (["2024-01-05", "2024-01-06", "2024-01-07",
                         "2024-01-08", "2024-01-09", "2024-01-10"]
                       + ["2024-01-06", "2024-01-07", "2024-01-08",
                          "2024-01-09", "2024-01-10", "2024-01-11"]),
            "forecasted_discharge": ([100.0] * 6) + ([10.0, 20.0, 30.0, 40.0, 50.0, 60.0]),
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Only Jan 5 boundary survives; Jan 4 non-boundary dropped
        assert pd.Timestamp(result["date"].iloc[0]) == pd.Timestamp("2024-01-05")
        # Mean of targets in pentad 2 from Jan 5 issue: 10,20,30,40,50 = 30.0
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)

    def test_dual_boundary_pentad_and_decad(self):
        """Date=Jan 10 is both pentad AND decad boundary — kept for both."""
        raw = pd.DataFrame({
            "code": ["10001"] * 11,
            "date": ["2024-01-10"] * 11,
            "target": [f"2024-01-{d}" for d in range(11, 22)],
            "forecasted_discharge": [float(i * 10) for i in range(11)],
        })
        # Pentad: targets Jan 11-15 kept (pentad 3), Jan 16+ dropped
        result_p = _normalize_ml_forecasts(raw.copy(), "TFT", "pentad")
        assert len(result_p) == 1
        # Mean of 0,10,20,30,40 = 20.0
        assert result_p["forecasted_discharge"].iloc[0] == pytest.approx(20.0)

        # Decad: targets Jan 11-20 kept (decad 2), Jan 21 dropped
        result_d = _normalize_ml_forecasts(raw.copy(), "TFT", "decad")
        assert len(result_d) == 1
        # Mean of 0,10,20,30,40,50,60,70,80,90 = 45.0
        assert result_d["forecasted_discharge"].iloc[0] == pytest.approx(45.0)

    def test_multiple_codes_boundary_filter_independent(self):
        """Boundary filter applies per-row, not per-code. Mixed dates across codes."""
        raw = pd.DataFrame({
            "code": (["10001"] * 5) + (["10002"] * 5),
            "date": (["2024-01-04"] * 5) + (["2024-01-05"] * 5),  # 10001=non-boundary, 10002=boundary
            "target": ([f"2024-01-{d}" for d in range(5, 10)]
                       + [f"2024-01-{d}" for d in range(6, 11)]),
            "forecasted_discharge": ([100.0] * 5) + ([10.0, 20.0, 30.0, 40.0, 50.0]),
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        # Only code 10002 (Jan 5, boundary) survives
        assert len(result) == 1
        assert result["code"].iloc[0] == "10002"

    def test_all_non_boundary_returns_empty(self):
        """If ALL input dates are non-boundary, result is empty DataFrame."""
        raw = pd.DataFrame({
            "code": ["10001"] * 5 + ["10001"] * 5,
            "date": ["2024-01-04"] * 5 + ["2024-01-06"] * 5,
            "target": ([f"2024-01-{d}" for d in range(5, 10)]
                       + [f"2024-01-{d}" for d in range(7, 12)]),
            "forecasted_discharge": [10.0] * 10,
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty
```

#### B. Unit tests for shared boundary functions (new file: `test_date_utils.py`)

```python
"""Tests for is_pentad_boundary and is_decad_boundary (moved to src/date_utils.py)."""
import datetime as dt
from src.date_utils import is_pentad_boundary, is_decad_boundary

class TestPentadBoundary:
    @pytest.mark.parametrize("day,expected", [
        (1, False), (4, False), (5, True), (6, False),
        (9, False), (10, True), (11, False),
        (14, False), (15, True), (16, False),
        (19, False), (20, True), (21, False),
        (24, False), (25, True), (26, False),
    ])
    def test_regular_days(self, day, expected):
        assert is_pentad_boundary(dt.date(2024, 1, day)) == expected

    @pytest.mark.parametrize("month,last_day", [
        (1, 31), (2, 28), (3, 31), (4, 30), (6, 30), (12, 31),
    ])
    def test_eom_is_boundary(self, month, last_day):
        assert is_pentad_boundary(dt.date(2025, month, last_day)) is True

    def test_leap_year_feb29(self):
        assert is_pentad_boundary(dt.date(2024, 2, 29)) is True  # EOM leap
        assert is_pentad_boundary(dt.date(2024, 2, 28)) is False  # not EOM in leap year

    def test_works_with_pd_timestamp(self):
        import pandas as pd
        assert is_pentad_boundary(pd.Timestamp("2024-01-05")) is True
        assert is_pentad_boundary(pd.Timestamp("2024-01-04")) is False

class TestDecadBoundary:
    @pytest.mark.parametrize("day,expected", [
        (1, False), (5, False), (9, False), (10, True),
        (15, False), (19, False), (20, True),
        (21, False), (25, False),
    ])
    def test_regular_days(self, day, expected):
        assert is_decad_boundary(dt.date(2024, 1, day)) == expected

    @pytest.mark.parametrize("month,last_day", [
        (1, 31), (2, 28), (4, 30), (2, 29),  # 2024 leap year Feb
    ])
    def test_eom_is_boundary(self, month, last_day):
        year = 2024 if last_day == 29 else 2025
        assert is_decad_boundary(dt.date(year, month, last_day)) is True

    def test_day25_not_decad_boundary(self):
        """Day 25 is pentad boundary but NOT decad boundary."""
        assert is_decad_boundary(dt.date(2024, 1, 25)) is False
```

#### C. Integration test for boundary-correct pipeline output (in `test_integration_postprocessing.py`)

Add a new class that feeds a mix of boundary and non-boundary ML daily data
through the full pipeline and verifies only boundary-date combined forecasts
are produced.

```python
class TestBoundaryDatePipelineIntegrity:
    """PP-031: Full pipeline produces combined forecasts only on boundary dates."""

    def test_pentad_pipeline_only_boundary_dates_in_output(self, env_setup):
        """Feed ML daily data for Jan 4 (non-boundary) and Jan 5 (boundary).
        Only Jan 5 should produce EM/NE/ML pentad records."""
        # Build fake observed data (only Jan 5 has observation — boundary date)
        observed = pd.DataFrame({
            "code": ["10001"], "date": pd.to_datetime(["2024-01-05"]),
            "discharge_avg": [50.0], "pentad_in_year": [1],
            "pentad_in_month": ["1"], "delta": [5.0],
        })
        # Build fake ML daily data: both Jan 4 and Jan 5 have targets
        ml_daily = pd.DataFrame({
            "code": ["10001"] * 12,
            "date": (["2024-01-04"] * 6) + (["2024-01-05"] * 6),
            "target": (["2024-01-05","2024-01-06","2024-01-07",
                        "2024-01-08","2024-01-09","2024-01-10"]
                      +["2024-01-06","2024-01-07","2024-01-08",
                        "2024-01-09","2024-01-10","2024-01-11"]),
            "forecasted_discharge": [100.0]*6 + [10.0,20.0,30.0,40.0,50.0,60.0],
            "model_short": ["TFT"] * 12,
        })
        # After normalize, only Jan 5 rows should survive
        normalized = _normalize_ml_forecasts(ml_daily, "TFT", "pentad")
        assert len(normalized) == 1
        assert pd.Timestamp(normalized["date"].iloc[0]) == pd.Timestamp("2024-01-05")

        # Feed through ensemble creation — EM should only have Jan 5
        # (requires skill stats with pentad_in_year=2 for the Jan 5 issue)
        skill_stats = pd.DataFrame({
            "pentad_in_year": [2], "code": ["10001"], "model_short": ["TFT"],
            "sdivsigma": [0.5], "nse": [0.8], "delta": [0.1],
            "accuracy": [0.9], "mae": [1.0], "n_pairs": [20.0],
        })
        # Verify no non-boundary dates leak through
        output_dates = normalized["date"].unique()
        for d in output_dates:
            assert is_pentad_boundary(pd.Timestamp(d)), \
                f"Non-boundary date {d} found in normalized output"

    def test_decad_pipeline_only_boundary_dates(self, env_setup):
        """Feed ML daily data for Jan 9 (non-boundary) and Jan 10 (boundary).
        Only Jan 10 should produce decad records."""
        ml_daily = pd.DataFrame({
            "code": ["10001"] * 22,
            "date": (["2024-01-09"] * 11) + (["2024-01-10"] * 11),
            "target": ([f"2024-01-{d}" for d in range(10, 21)]
                      +[f"2024-01-{d}" for d in range(11, 22)]),
            "forecasted_discharge": [999.0]*11 + [float(i*10) for i in range(11)],
        })
        result = _normalize_ml_forecasts(ml_daily, "TFT", "decad")
        assert len(result) == 1
        assert pd.Timestamp(result["date"].iloc[0]) == pd.Timestamp("2024-01-10")
```

#### D. Gap detector test (in `test_gap_detector.py`)

```python
class TestBoundaryDateGapDetection:
    """PP-031: Gap detector should not flag non-boundary dates."""

    def test_non_boundary_date_not_flagged_as_gap(self):
        """Combined table has ML on non-boundary date Jan 4 but no EM.
        This should NOT be flagged as a gap (Jan 4 is not a boundary day)."""
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-04"] * 2 + ["2024-01-05"] * 3),
            "code": ["10001"] * 5,
            "model_short": ["TFT", "TiDE", "LR", "TFT", "EM"],
            "forecasted_discharge": [1.0] * 5,
        })
        result = detect_missing_ensembles(df)
        # Jan 5 has EM → no gap. Jan 4 has no EM but is not a boundary → should not be flagged.
        # NOTE: This test documents the DESIRED behavior after the gap detector fix.
        # Before fix, Jan 4 WOULD be flagged as a gap.
        gap_dates = result["date"].tolist()
        assert pd.Timestamp("2024-01-04") not in gap_dates
```

### Existing test verification

Run the full test suite BEFORE and AFTER the change:
```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```
Zero failures, zero unexpected skips before AND after. If any existing test
fails, it means the boundary filter is too aggressive — investigate before
proceeding.

### Test execution order

1. Run existing tests → all pass (baseline)
2. Add `TestBoundaryDayFiltering` tests → they FAIL (expected: the filter doesn't exist yet)
3. Implement the boundary-day filter in `_normalize_ml_forecasts`
4. Run all tests → new tests pass AND existing tests still pass
5. Add gap detector test → may fail if gap detector not yet updated
6. Update gap detector → all tests pass

---

## Manual Verification

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

## Risk Analysis

### Target filtering (PP-023) — VERIFIED SAFE

The existing target filter correctly handles all edge cases:
- EOM months (28/29/30/31 days) — `min()` cap absorbs extra days
- Month boundaries (Mar 31 → Apr 1) — formula crosses months naturally
- Dual pentad/decad dates (10/20/EOM) — called independently per horizon_type
- Missing ML on boundary day — returns empty gracefully, no crash
**No changes needed to PP-023 filter logic.**

### Downstream consumers — RISKS IDENTIFIED

| Consumer | Risk from non-boundary records already in DB |
|----------|----------------------------------------------|
| **Skill metrics** | Safe — non-boundary records drop at inner join (no matching observation) |
| **Dashboard** | **AFFECTED** — reads API directly, shows phantom forecasts on non-boundary dates |
| **Gap detector** | **AFFECTED** — sees non-boundary ML records, may trigger phantom EM gap alarms |
| **iEasyHydroForecast** | Not affected — does not read combined forecasts |

### DB cleanup requirement

Adding the boundary filter to `_normalize_ml_forecasts` prevents **new** spurious records. But existing non-boundary records in the DB will continue to affect the dashboard and gap detector. A one-time cleanup is needed (delete `horizon=pentad`/`decade` records where `date` is not a boundary day). This can be a follow-up task.

### Write path — ALL models affected

`_normalize_ml_forecasts` output (TFT, TiDE, TSMixer rows) flows through `save_forecast_data` → `_write_combined_forecast_to_api` which also writes NE and EM records built from the same data. LR is stripped before write (goes to separate `lr-forecast` endpoint). So the boundary filter in `_normalize_ml_forecasts` controls what all downstream model records (ML + NE + EM) get written.

### Minimal fix location

The fix should be in `_normalize_ml_forecasts` (add boundary-day filter before target filtering), NOT in the API query scope. Reason: the function is called by both operational and maintenance paths. Changing the API query scope would require changes in multiple callers and could break the maintenance lookback logic.

---

## Documentation Impact

- [ ] No documentation impact — this is a date selection bug fix

## Out of Scope

- Daily ensemble creation (PP-012) — separate concern
- Long-term forecast dating (different code path)

## Dependencies

- PP-023 (complete) — period-aware target filtering, prerequisite context

## Follow-up Tasks

- DB cleanup: delete spurious non-boundary pentad/decad records from `forecast` table
- Dashboard: verify non-boundary records no longer appear after cleanup

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
