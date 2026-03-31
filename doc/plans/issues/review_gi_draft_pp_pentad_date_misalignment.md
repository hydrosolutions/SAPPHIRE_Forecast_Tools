# PP-031: Pentad/decad aggregation does not select boundary issue days (shared code path)

**Status**: Review
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
                 │    │       ← NOTE: fetches ALL daily forecasts for the year
                 │    │          (not the bug — API scope stays unchanged)
                 │    │
                 │    └─ _normalize_ml_forecasts(ml_raw, model, "pentad")
                 │         ├─ ← BUG: no boundary-day filter on `date` column
                 │         ├─ Filter: keep targets where pentad_in_year(target)
                 │         │    == pentad_in_year(date + 1 day)       (PP-023)
                 │         ├─ Aggregate: groupby(["code", "date"]).mean()
                 │         │    ← produces rows for EVERY ML run date,
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

### Intended Dataflow (AFTER FIX)

The fix is purely inside `_normalize_ml_forecasts()` — a new boundary-day filter
drops rows where `date` is not a pentad/decad issue day. No changes to API queries,
entry points, or write paths.

```
_normalize_ml_forecasts(df, model, horizon_type)
  │
  ├─ Parse dates (existing line 1853)
  ├─ NEW: Drop rows where date is not a boundary day        ← THE FIX
  │    pentad: date.day not in (5, 10, 15, 20, 25, last_day_of_month)
  │    decad:  date.day not in (10, 20, last_day_of_month)
  ├─ Filter targets to period (existing PP-023, line 1858)  ← unchanged
  ├─ Aggregate groupby(["code", "date"]).mean()             ← unchanged
  └─ Compute period columns from date + 1 day              ← unchanged
```

**Effect on operational path**: `postprocessing_operational.py` already guards on
`is_pentad_boundary(today)` / `is_decad_boundary(today)`, so on boundary days the
filter is a no-op for today's records. Non-boundary records from earlier ML runs
(fetched as part of the year-range query) are now correctly dropped.

**Effect on maintenance path**: The gap detector may still flag non-boundary dates
(from spurious records already in DB). But when maintenance reads through
`_normalize_ml_forecasts`, the boundary filter drops those rows → empty result →
no new spurious records written. The gap detector itself is not changed in this PR
(deferred to follow-up).

### Target Filtering Detail (verified — PP-023)

The current code computes target period using `tl.get_pentad_in_year(target)`. Since daily ML forecasts store individual `target` dates (e.g., `"target": "2026-03-26"`), not period numbers, the filtering works by:

1. Computing `expected_period = get_pentad_in_year(issue_date + 1 day)`
2. Computing `actual_period = get_pentad_in_year(target_date)`
3. Keeping rows where they match

**Verified correct** (PP-023, complete). `get_pentad_in_year` handles all edge cases: EOM with varying month lengths, leap years, month boundaries. Covered by `test_data_reader_ml_aggregation.py`. **This logic must NOT be changed.**

---

## Implementation Plan

### Steps

- [ ] Step 1: Add `import calendar` to `data_reader.py` imports (stdlib, not currently imported). Add `_is_pentad_boundary()` and `_is_decad_boundary()` as private helper functions in `data_reader.py`, near the top of the file (after imports, before `_clean_code_column`). These are 2-line functions using `calendar.monthrange`. Do NOT move or modify the copies in `postprocessing_operational.py`.
- [ ] Step 2: Insert boundary-day filter in `_normalize_ml_forecasts()` between date parsing (line 1853) and target filter (line 1858). Drop rows where `date` is not a boundary day for the given `horizon_type`.
- [ ] Step 3: Add tests in `test_data_reader_ml_aggregation.py` (see Testing section below).
- [ ] Step 4: Run tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
- [ ] Step 5 (follow-up): Clean up spurious non-boundary pentad/decad records already in the DB
- [ ] Step 6 (follow-up): Update gap detector to only flag boundary dates
- [ ] Step 7 (follow-up): Verify dashboard no longer shows phantom forecasts after DB cleanup

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/data_reader.py` | Add `_is_pentad_boundary()` / `_is_decad_boundary()` helpers; insert boundary-day filter in `_normalize_ml_forecasts()` between date parsing and target filter |
| `apps/postprocessing_forecasts/tests/test_data_reader_ml_aggregation.py` | Add `TestBoundaryDayFiltering` and `TestBoundaryFunctions` test classes |

**No other production files are modified.** `postprocessing_operational.py`, `postprocessing_maintenance.py`, `gap_detector.py`, and all write paths are untouched.

---

## Implementation Safety Guardrails

### Constraints for the implementing agent

1. **Do NOT change the PP-023 target filtering logic** (lines 1858-1879). It is verified correct for all edge cases. The boundary-day filter is a NEW step inserted BEFORE the existing target filter.

2. **Do NOT change the API query scope** in `_read_ml_forecasts_pp_api`. The fix is purely in `_normalize_ml_forecasts` — a read-time filter, not a query change.

3. **Do NOT change the write path** in `api_writer.py` or `file_writer.py`.

4. **Do NOT change `postprocessing_operational.py`** in any way. The existing `is_pentad_boundary`/`is_decad_boundary` guards at the entry point are correct and must remain. The boundary functions are duplicated as private helpers in `data_reader.py` — this is intentional to avoid cross-module coupling.

5. **Do NOT change `postprocessing_maintenance.py`** or `gap_detector.py`. The gap detector fix is deferred to a follow-up.

6. **Do NOT create new files** (`src/date_utils.py` or otherwise). All changes go into `data_reader.py`.

7. **The boundary-day filter must be inserted AFTER the `pd.to_datetime` conversion** (line 1853) and BEFORE the target filter (line 1858). Exact insertion point: between lines 1853 and 1855.

8. **Type safety**: The `date` column is `pd.Timestamp` at the insertion point. The boundary functions work with `pd.Timestamp` (verified — it has `.year`, `.month`, `.day` attributes via `calendar.monthrange`). No type conversion needed.

9. **The boundary filter must run unconditionally** — it does not depend on `TAG_LIBRARY_AVAILABLE` or the presence of a `target` column. Records on non-boundary dates should be dropped regardless.

### Code Example

```python
# Private helpers in data_reader.py (near top, after imports):

def _is_pentad_boundary(d) -> bool:
    """Return True if *d* is a pentad issue day (5/10/15/20/25/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)


def _is_decad_boundary(d) -> bool:
    """Return True if *d* is a decad issue day (10/20/last)."""
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)
```

```python
# Inside _normalize_ml_forecasts(), after date parsing (line 1853),
# BEFORE the target filter (line 1858):

    # PP-031: Drop rows where date is not a boundary day for this horizon.
    if "date" in df.columns:
        if horizon_type == "pentad":
            boundary_mask = df["date"].apply(_is_pentad_boundary)
        else:
            boundary_mask = df["date"].apply(_is_decad_boundary)

        n_non_boundary = (~boundary_mask).sum()
        if n_non_boundary > 0:
            logger.info(
                "Dropped %d/%d rows on non-%s-boundary dates for %s",
                n_non_boundary,
                len(df),
                horizon_type,
                model,
            )
        df = df[boundary_mask].copy()

        if df.empty:
            return pd.DataFrame()
```

### Callers of `_normalize_ml_forecasts` (verified)

| Caller | File:Line | Effect of boundary filter |
|--------|-----------|--------------------------|
| `read_individual_model_forecasts` | `data_reader.py:2112` | Non-boundary rows dropped before aggregation — the fix |
| Test files | `test_data_reader_ml_aggregation.py`, `test_data_reader.py` | All existing tests use boundary dates — unaffected |

There is exactly **one** production caller. The function is private (`_` prefix).

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

    def test_short_pentad_feb25_nonleap(self):
        """Feb 25 pentad: only 3 targets in period (Feb 26-28), not the usual 5."""
        raw = pd.DataFrame({
            "code": ["10001"] * 6,
            "date": ["2025-02-25"] * 6,
            "target": ["2025-02-26", "2025-02-27", "2025-02-28",
                        "2025-03-01", "2025-03-02", "2025-03-03"],
            "forecasted_discharge": [10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert len(result) == 1
        # Only Feb 26-28 in pentad 12; Mar 1+ is pentad 13 → dropped
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(20.0)

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

    def test_non_boundary_no_target_column_returns_empty(self):
        """Non-boundary date with no target column → empty.

        Before PP-031, records without a target column were aggregated
        regardless of date. After the fix, the boundary filter runs
        unconditionally (before the target-presence check), so non-boundary
        dates are dropped even without a target column.
        """
        raw = pd.DataFrame({
            "code": ["10001"] * 6,
            "date": ["2024-01-04"] * 6,  # NOT a boundary day
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        })
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")
        assert result.empty
```

#### B. Unit tests for boundary helper functions (in `test_data_reader_ml_aggregation.py`)

Same file, new classes. Import `_is_pentad_boundary` and `_is_decad_boundary` from `src.data_reader`.

```python
import datetime as dt
from src.data_reader import _is_pentad_boundary, _is_decad_boundary

class TestPentadBoundary:
    """PP-031: _is_pentad_boundary covers all edge cases."""

    @pytest.mark.parametrize("day,expected", [
        (1, False), (4, False), (5, True), (6, False),
        (9, False), (10, True), (11, False),
        (14, False), (15, True), (16, False),
        (19, False), (20, True), (21, False),
        (24, False), (25, True), (26, False),
    ])
    def test_regular_days(self, day, expected):
        assert _is_pentad_boundary(dt.date(2024, 1, day)) == expected

    @pytest.mark.parametrize("month,last_day", [
        (1, 31), (2, 28), (3, 31), (4, 30), (6, 30), (12, 31),
    ])
    def test_eom_is_boundary(self, month, last_day):
        assert _is_pentad_boundary(dt.date(2025, month, last_day)) is True

    def test_30day_month_non_eom_not_boundary(self):
        """Day 26-29 in a 30-day month are NOT boundaries (only 25 and 30 are)."""
        assert _is_pentad_boundary(dt.date(2025, 4, 26)) is False
        assert _is_pentad_boundary(dt.date(2025, 4, 29)) is False
        assert _is_pentad_boundary(dt.date(2025, 4, 30)) is True  # EOM

    def test_leap_year_feb29(self):
        assert _is_pentad_boundary(dt.date(2024, 2, 29)) is True  # EOM leap
        assert _is_pentad_boundary(dt.date(2024, 2, 28)) is False  # not EOM in leap year

    def test_works_with_pd_timestamp(self):
        import pandas as pd
        assert _is_pentad_boundary(pd.Timestamp("2024-01-05")) is True
        assert _is_pentad_boundary(pd.Timestamp("2024-01-04")) is False

class TestDecadBoundary:
    """PP-031: _is_decad_boundary covers all edge cases."""

    @pytest.mark.parametrize("day,expected", [
        (1, False), (5, False), (9, False), (10, True),
        (15, False), (19, False), (20, True),
        (21, False), (25, False),
    ])
    def test_regular_days(self, day, expected):
        assert _is_decad_boundary(dt.date(2024, 1, day)) == expected

    @pytest.mark.parametrize("month,last_day", [
        (1, 31), (2, 28), (4, 30), (2, 29),  # 2024 leap year Feb
    ])
    def test_eom_is_boundary(self, month, last_day):
        year = 2024 if last_day == 29 else 2025
        assert _is_decad_boundary(dt.date(year, month, last_day)) is True

    def test_day25_not_decad_boundary(self):
        """Day 25 is pentad boundary but NOT decad boundary."""
        assert _is_decad_boundary(dt.date(2024, 1, 25)) is False
```

### Existing test verification

All existing tests in `test_data_reader_ml_aggregation.py` use boundary dates
(Jan 5, Feb 25, Jan 10, Feb 20, etc.) — verified by inspection. The boundary
filter is a no-op for these tests, so they pass unchanged.

Run the full test suite BEFORE and AFTER the change:
```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```
Zero failures, zero unexpected skips before AND after. If any existing test
fails, it means the boundary filter is too aggressive — investigate before
proceeding.

### Test execution order

1. Run existing tests → all pass (baseline)
2. Add `TestBoundaryDayFiltering`, `TestPentadBoundary`, `TestDecadBoundary` tests → boundary filter tests FAIL (expected: the filter doesn't exist yet)
3. Implement the boundary helpers and filter in `data_reader.py`
4. Run all tests → new tests pass AND existing tests still pass

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

| Consumer | Risk from non-boundary records already in DB | Risk from this fix |
|----------|----------------------------------------------|--------------------|
| **Skill metrics** | Safe — non-boundary records drop at inner join (no matching observation) | None |
| **Dashboard** | **AFFECTED** — reads API directly, shows phantom forecasts on non-boundary dates | None (fix prevents new records; DB cleanup is follow-up) |
| **Gap detector** | **AFFECTED** — sees non-boundary ML records, may trigger phantom EM gap alarms | **Harmless** — maintenance reads through `_normalize_ml_forecasts`, boundary filter drops rows → empty result → no new spurious records written |
| **iEasyHydroForecast** | Not affected — does not read combined forecasts | None |
| **Records without `target` column** | N/A | Non-boundary dates now return empty even without `target` column. Correct behavior — tested explicitly. |

### DB cleanup requirement

Adding the boundary filter to `_normalize_ml_forecasts` prevents **new** spurious records. But existing non-boundary records in the DB will continue to affect the dashboard and gap detector. A one-time cleanup is needed (delete `horizon=pentad`/`decade` records where `date` is not a boundary day). **This should happen within ~1 week of deploying PP-031** — until then, the gap detector will re-flag the same non-boundary dates every nightly maintenance run (harmless but noisy: unnecessary API reads and log warnings).

### Write path — ALL models affected (correctly)

`_normalize_ml_forecasts` output (TFT, TiDE, TSMixer rows) flows through `save_forecast_data` → `_write_combined_forecast_to_api` which also writes NE and EM records built from the same data. LR is stripped before write (goes to separate `lr-forecast` endpoint). So the boundary filter in `_normalize_ml_forecasts` controls what all downstream model records (ML + NE + EM) get written. This is the desired behavior — all records should only exist on boundary dates.

### Minimal fix location — verified

The fix is in `_normalize_ml_forecasts` (add boundary-day filter before target filtering). This is the single production choke point — exactly one caller (`read_individual_model_forecasts:2112`). Both operational and maintenance paths flow through it. No changes to API query scope, write paths, or entry points.

---

## Documentation Impact

- [ ] No documentation impact — this is a date selection bug fix

## Out of Scope

- Daily ensemble creation (PP-012) — separate concern
- Long-term forecast dating (different code path)
- Gap detector boundary-aware filtering — deferred to follow-up (PP-033)
- Moving `is_pentad_boundary`/`is_decad_boundary` to a shared module — premature consolidation; only 2 callers exist
- DB cleanup of existing spurious records — separate follow-up task

## Dependencies

- PP-023 (complete) — period-aware target filtering, prerequisite context

## Follow-up Tasks

- PP-033: Gap detector boundary-aware filtering (only flag boundary dates as gaps)
- DB cleanup: delete spurious non-boundary pentad/decad records from `forecast` table
- Dashboard: verify non-boundary records no longer appear after cleanup
- Consolidate boundary functions into shared module when a third caller appears

## Acceptance Criteria

- [ ] Pentad combined forecasts exist ONLY on pentad issue days (5/10/15/20/25/EOM)
- [ ] Decad combined forecasts exist ONLY on decad issue days (10/20/EOM)
- [ ] Query by pentad issue day returns EM + NE + ML model records
- [ ] LR forecast `date` and combined forecast `date` are aligned for the same pentad/decad
- [ ] No spurious records on non-boundary dates
- [ ] Dual-boundary dates (10, 20, EOM) produce both pentad and decad records
- [ ] Non-boundary dates without `target` column also return empty (tested)
- [ ] All existing tests in `test_data_reader_ml_aggregation.py` pass unchanged
- [ ] All existing tests in the full suite pass
- [ ] No changes to `postprocessing_operational.py`, `postprocessing_maintenance.py`, or `gap_detector.py`

---

## Review Notes (2026-03-31)

**Reviewer**: Opus orchestrator, critical review before implementation.

**Verdict**: Plan is sound. Fix is correct, minimal, and well-tested. No showstoppers.

**Findings incorporated into plan above**:

1. **Must-fix** (done): Added `import calendar` requirement to Step 1 — `calendar` is not currently imported in `data_reader.py`.
2. **Test added** (done): `test_short_pentad_feb25_nonleap` — covers the short 3-day pentad (Feb 26-28) where only 3 of 6 daily targets fall in-period. Exercises both boundary filter and target filter together.
3. **Test added** (done): `test_30day_month_non_eom_not_boundary` — covers days 26-29 in April (30-day month) to confirm they are NOT boundaries, while day 30 (EOM) IS.
4. **Urgency note** (done): DB cleanup follow-up should happen within ~1 week of deploy to stop nightly gap detector noise from re-flagging stale non-boundary records.

**Verified correct**:
- Boundary functions work with both `datetime.date` and `pd.Timestamp` (both have `.year`, `.month`, `.day`; `calendar.monthrange` takes ints).
- All existing tests in `test_data_reader_ml_aggregation.py` use boundary dates (Jan 5, Jan 10, Feb 20, Feb 25, Feb 28) — boundary filter is a no-op for them.
- Single production caller of `_normalize_ml_forecasts` (`data_reader.py:2112`).
- PP-023 target filtering logic is untouched — the boundary filter composes cleanly before it.

---

## References

- Related completed issue: PP-023 (period-aware aggregation)
- Discovered: `review_checklist_local_2026-03-28.md`
- Key code: `apps/postprocessing_forecasts/src/data_reader.py` — `_normalize_ml_forecasts()`
- Operational boundary guards: `apps/postprocessing_forecasts/postprocessing_operational.py:54-63`
- Maintenance lookback: `POSTPROCESSING_GAPFILL_MAX_MONTHS=13` (default)
