# PP-014: Invert Skill Metrics Read Priority (API-first, CSV-fallback)

**Status**: Complete
**Module**: `postprocessing_forecasts`
**Priority**: High
**Labels**: `api-migration`, `data-integrity`

---

## Summary

`data_reader.read_skill_metrics()` and `read_monthly_skill_metrics()` currently
try CSV first and fall back to API only if CSV is empty or missing. This makes
CSV the de facto primary source for ensemble filtering decisions, even though
the API database is the intended single source of truth.

The fix is to invert the read order: try API first, fall back to CSV only if
the API is unavailable. This applies to all three horizon types (pentad,
decad, month).

## Problem

### Current read order (wrong)

```
read_skill_metrics(horizon_type)
  1. _read_skill_metrics_csv(horizon_type)  ← primary
  2. _read_skill_metrics_api(horizon_type)  ← fallback only if CSV empty/missing
```

### Why this matters

1. **Stale data**: CSV files are written once per recalculation. If the API
   database is updated (e.g., a targeted re-import, a fix to a single
   station's metrics), the CSV still contains the old values. Operational
   mode will use stale skill metrics for ensemble filtering.

2. **Inconsistency**: The write path already treats API as primary (upsert
   first, CSV as deprecated backup). Having the read path prefer CSV over
   API creates an asymmetry: data written to the API may never be read back.

3. **Migration blocker**: CSV removal (API-003 / D-002) requires confidence
   that nothing depends on CSV as a primary source. As long as the read path
   prefers CSV, we can't remove CSV files without risking data loss.

## Target read order (correct)

```
read_skill_metrics(horizon_type)
  1. _read_skill_metrics_api(horizon_type)  ← primary
  2. _read_skill_metrics_csv(horizon_type)  ← deprecated fallback
```

Same pattern for `read_monthly_skill_metrics()`.

## Scope

**File to modify**: `apps/postprocessing_forecasts/src/data_reader.py`

**Functions to change**:
- `read_skill_metrics()` (lines 29–70)
- `read_monthly_skill_metrics()` (lines 202–227)

**Functions unchanged**: `_read_skill_metrics_csv()`,
`_read_skill_metrics_api()`, `_read_monthly_skill_metrics_csv()`,
`_read_monthly_skill_metrics_api()`, `_normalize_api_skill_metrics()`,
`_normalize_api_monthly_skill_metrics()` — all internal helpers remain
as-is. Only the orchestration logic in the two public functions changes.

---

## Implementation Plan

### Step 1: Invert `read_skill_metrics()`

Change the orchestration in `read_skill_metrics()` from CSV-first to
API-first:

```python
def read_skill_metrics(horizon_type: str) -> pd.DataFrame:
    """Read pre-calculated skill metrics from API (primary) or CSV (fallback).

    Args:
        horizon_type: 'pentad', 'decad', or 'month'

    Returns:
        DataFrame with columns: [pentad_in_year|decad_in_year, code,
        model_short, sdivsigma, nse, delta, accuracy, mae, n_pairs]

    Raises:
        ValueError: If horizon_type is invalid.
    """
    if horizon_type not in ("pentad", "decad", "month"):
        raise ValueError(
            f"horizon_type must be 'pentad', 'decad', or 'month', "
            f"got: {horizon_type}"
        )

    if horizon_type == "month":
        return read_monthly_skill_metrics()

    # API-first: try the authoritative source
    df = _read_skill_metrics_api(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from API (%s)",
            len(df), horizon_type,
        )
        return df

    # CSV fallback (deprecated): only used when API is unavailable
    logger.info(
        "API skill metrics unavailable for %s, falling back to CSV",
        horizon_type,
    )
    df = _read_skill_metrics_csv(horizon_type)
    if df is not None and not df.empty:
        logger.info(
            "Read %d skill metric rows from CSV (%s)",
            len(df), horizon_type,
        )
        return df

    logger.warning("No skill metrics available for %s", horizon_type)
    return pd.DataFrame()
```

### Step 2: Invert `read_monthly_skill_metrics()`

Same change for the monthly path:

```python
def read_monthly_skill_metrics() -> pd.DataFrame:
    """Read pre-calculated monthly skill metrics from API or CSV.

    Returns:
        DataFrame with columns: [month_in_year, code, model_short,
        sdivsigma, nse, delta, accuracy, mae, n_pairs]
    """
    # API-first: try the authoritative source
    df = _read_monthly_skill_metrics_api()
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly skill metric rows from API", len(df)
        )
        return df

    # CSV fallback (deprecated)
    logger.info(
        "API monthly skill metrics unavailable, falling back to CSV"
    )
    df = _read_monthly_skill_metrics_csv()
    if df is not None and not df.empty:
        logger.info(
            "Read %d monthly skill metric rows from CSV", len(df)
        )
        return df

    logger.warning("No monthly skill metrics available")
    return pd.DataFrame()
```

### Step 3: Update docstring

The module-level docstring (line 1–6) says "Read pre-calculated skill metrics
and monthly data from CSV or API." Update to "from API or CSV (deprecated
fallback)" to match the new priority.

---

## Testing

### Existing tests to update

The test file `tests/test_data_reader.py` has tests that assert the
old CSV-first behavior. These must be updated to match the new API-first
priority:

1. **`TestReadSkillMetricsIntegration.test_csv_preferred_over_api`** (line 252)
   — Currently asserts that when CSV has data, API is not called. This test
   must be **inverted**: when API has data, CSV should not be called.
   Rename to `test_api_preferred_over_csv`.

2. **`TestReadSkillMetricsIntegration.test_falls_back_to_api_when_csv_empty`**
   (line 273) — Currently tests CSV→API fallback. Must be **inverted** to
   test API→CSV fallback. Rename to `test_falls_back_to_csv_when_api_empty`.

3. **`TestReadSkillMetricsIntegration.test_corrupted_csv_falls_back_to_api`**
   (line 308) — This scenario becomes: API unavailable, CSV corrupted →
   returns empty. Or: API unavailable, CSV valid → returns CSV data.
   Adjust accordingly.

4. **`TestReadSkillMetricsIntegration.test_truncated_csv_with_partial_rows_falls_back`**
   (line 343) — Same as above: reframe for API-first logic.

### New tests to add

5. **`test_api_preferred_over_csv`**: When API returns data, CSV is not read.
   Mock `_read_skill_metrics_api` to return valid data, mock
   `_read_skill_metrics_csv` and assert it is NOT called.

6. **`test_falls_back_to_csv_when_api_unavailable`**: When API returns None
   (unavailable), CSV is used. Mock `_read_skill_metrics_api` to return
   None, mock `_read_skill_metrics_csv` to return valid data.

7. **`test_falls_back_to_csv_when_api_returns_empty`**: When API returns
   empty DataFrame, CSV is used. Same pattern with empty DataFrame.

8. **`test_returns_empty_when_both_fail`**: Already exists (line 296), no
   change needed — both sources returning nothing should still return empty.

9. **`test_monthly_api_preferred_over_csv`**: Same as #5 but for
   `read_monthly_skill_metrics()`.

10. **`test_monthly_falls_back_to_csv_when_api_unavailable`**: Same as #6
    but for monthly.

### Test commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests/test_data_reader.py -v
```

Full suite after:
```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

---

## Out of Scope

- Changing `_read_skill_metrics_api()` or `_read_skill_metrics_csv()`
  internals — these already work correctly; only the orchestration changes.
- Removing CSV fallback entirely — that's API-003 / D-002, a separate
  decision with its own acceptance criteria.
- Changing the write path — it already writes API-first.
- `gap_detector.py` reads (CSV-only for combined forecasts) — that's
  PP-007 / PP-013, separate issues.

## Dependencies

- None. This change is self-contained in `data_reader.py` and its tests.
- No API schema changes needed.
- No coordination with colleague required.

## Acceptance Criteria

- [x] `read_skill_metrics('pentad')` tries API first, CSV only on API failure
- [x] `read_skill_metrics('decad')` tries API first, CSV only on API failure
- [x] `read_monthly_skill_metrics()` tries API first, CSV only on API failure
- [x] Log messages clearly indicate which source was used ("from API" vs
      "falling back to CSV")
- [x] All existing tests pass (updated to match new priority)
- [x] New tests cover API-first happy path and CSV fallback path
- [x] Full test suite passes: `SAPPHIRE_TEST_ENV=True bash run_tests.sh`

## Risk Assessment

**Low risk.** The change is a reordering of two existing code paths. Both
paths are already tested independently. The API read path has been in
production as a fallback since the initial API integration. The only
behavioral change is which path is tried first.

**Rollback**: If API reads prove unreliable in production, setting
`SAPPHIRE_API_ENABLED=false` disables the API path entirely, making
CSV the only source (same as pre-migration behavior).
