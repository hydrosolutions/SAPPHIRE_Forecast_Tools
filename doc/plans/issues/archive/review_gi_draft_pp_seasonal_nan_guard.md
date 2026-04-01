# PP-029: Guard against NaN in seasonal/quarterly API write

**Status**: Review
**Module**: postprocessing_forecasts
**Priority**: Medium
**Labels**: `bug`, `postprocessing`, `api`

---

## Summary

Add NaN validation in `_write_aggregated_forecasts_to_api()` before converting `season_year` / `year` / `quarter_in_year` to `int`, mirroring the existing guard in the short-term writer.

## Context

`postprocessing_forecasts` writes seasonal and quarterly long-term forecasts to the SAPPHIRE API via `_write_aggregated_forecasts_to_api()` in `apps/postprocessing_forecasts/src/api_writer.py`. During the 2026-03-27 pipeline run the following error was logged:

```
ERROR - Failed to write seasonal forecasts to API: cannot convert float NaN to integer
WARNING - Seasonal forecasts API write returned False (disabled, unavailable, or failed).
```

The entire seasonal batch was silently dropped. Despite this, 9626 seasonal forecast records were successfully written later during the long-term skill metric recalculation step (which calls a different write path). So the impact is limited to the operational write window, but it is a correctness gap.

## Problem

`_write_aggregated_forecasts_to_api()` (lines 968–1033, `api_writer.py`) iterates over DataFrame rows and calls `int(row["season_year"])` (line 987) or `int(row["year"])` (line 978) without checking for NaN first. If any row has a missing `season_year` or `year` value, Python raises `ValueError: cannot convert float NaN to integer`, which is caught by the outer `except Exception` at line 1050 and causes the **entire batch** to be dropped.

The short-term writer at lines 282–297 already handles this correctly:

```python
# Drop rows with missing horizon values (short-term writer — the correct pattern)
n_before = len(df_rec)
df_rec = df_rec.dropna(subset=[horizon_value_col, horizon_in_year_col])
skipped_count = n_before - len(df_rec)
if skipped_count > 0:
    dropped_detail = dropped[["code", "date"]].drop_duplicates().head(10).to_dict("records")
    logger.warning(
        "Dropped %d forecast records with missing horizon values "
        "after repair attempt (%s). Sample codes/dates: %s",
        skipped_count, horizon_type, dropped_detail,
    )
```

## Desired Outcome

- Rows with NaN `season_year` / `year` / `quarter_in_year` are skipped individually with a WARNING log, instead of the current behavior where a single NaN row causes the entire batch to be silently dropped.
- The remaining valid rows are written successfully.
- A test confirms that a DataFrame with mixed valid and NaN rows writes the valid rows and logs a warning.

---

## Technical Analysis

### Current Implementation

**File:** `apps/postprocessing_forecasts/src/api_writer.py:925–1056`

```python
def _write_aggregated_forecasts_to_api(
    data: pd.DataFrame,
    horizon_type: str,
    period_col: str,
    label: str,
) -> bool:
    ...
    records = []
    for _, row in data.iterrows():          # line 969
        code = str(row["code"]).replace(".0", "")
        ...
        if horizon_type == "quarter":
            year = int(row["year"])             # line 978 — CRASHES if NaN
            quarter = int(row["quarter_in_year"])  # line 979 — CRASHES if NaN
        else:  # season
            season_year = int(row.get("season_year", row.get("year")))  # line 987 — CRASHES if NaN
```

The exception propagates to the outer `except Exception` at line 1050, which logs the error and returns `False` — silently dropping all records.

**Correct pattern (short-term writer):** `apps/postprocessing_forecasts/src/api_writer.py:282–297`

### Root Cause

Missing pre-iteration NaN guard for the year/period columns used in `int()` conversion.

**Note on `season_year` fallback (line 987):** `row.get("season_year", row.get("year"))` is a **key-based** fallback — if the `season_year` column *exists* but contains NaN, `.get()` returns NaN (the key is present), it does NOT fall back to `year`. So the NaN guard on `season_year` alone is correct: those rows *would* crash at `int(NaN)`. Repairing NaN `season_year` by falling back to `year` is out of scope (upstream data issue).

---

## Implementation Plan

### Approach

Add a `dropna()` call on the key columns before the `for _, row in data.iterrows()` loop (lines 968–969), following a similar pattern to the short-term writer's NaN guard (lines 282–297). Log skipped rows at WARNING level. The aggregated version intentionally omits the `"date"` column from diagnostics (not present in these DataFrames) and has no repair step (no source data to derive missing year/period values from).

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/api_writer.py` | Add NaN guard before line 969 |

### Implementation Steps

- [ ] Insert the NaN guard (see Code Example below) inside the `try` block, after the readiness check (~line 966) and before `records = []` (line 968). **Not** after the `data.empty` guard at line 941 — that is outside the `try` block.
  - For `horizon_type == "quarter"`: check `["year", "quarter_in_year"]`
  - For `horizon_type == "season"`: check `["season_year"]` (fall back to `["year"]` if `season_year` not in columns)
- [ ] Run tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`

### Code Example

```python
# Insert inside the `try` block, after the readiness check (~line 966),
# before `records = []` (line 968)

# Determine which columns to check for NaN before int() conversion
if horizon_type == "quarter":
    nan_check_cols = [c for c in ["year", "quarter_in_year"] if c in data.columns]
else:  # season
    nan_check_cols = ["season_year"] if "season_year" in data.columns else ["year"]

if nan_check_cols:
    data_before_nan_drop = data  # keep reference for diagnostics
    data = data.dropna(subset=nan_check_cols)
    skipped_nan = len(data_before_nan_drop) - len(data)
    if skipped_nan > 0:
        nan_mask = data_before_nan_drop[nan_check_cols].isna().any(axis=1)
        dropped_detail = (
            data_before_nan_drop[nan_mask][["code"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        logger.warning(
            "Dropped %d %s forecast records with missing year/period values. "
            "Sample codes: %s",
            skipped_nan,
            label,
            dropped_detail,
        )

if data.empty:
    logger.info("No %s forecast records to write to API after NaN removal", label)
    return False
```

---

## Testing

### Test Cases

- [ ] NaN in `season_year`: DataFrame with 3 rows, one has `season_year=NaN` → assert `len(records) == 2` on `mock_client.write_long_forecasts.call_args[0][0]`, WARNING logged via `caplog`
- [ ] NaN in `year` only (quarter, valid `quarter_in_year`): row dropped, assert record count excludes NaN row, WARNING in `caplog`
- [ ] NaN in `quarter_in_year` only (quarter, valid `year`): row dropped, assert record count excludes NaN row, WARNING in `caplog`
- [ ] All rows valid (quarter) → all written, no WARNING in `caplog`
- [ ] All rows valid (season) → all written, no WARNING in `caplog`
- [ ] All rows NaN (either horizon type) → returns `False`, logs INFO "No records to write"
- [ ] NaN in `forecasted_discharge` only (valid year/period): row is **not** dropped — assert record is written with `q=None` (regression guard: the NaN guard must not interfere with the existing `pd.notna` handling at line 1019)
- [ ] Missing `season_year` column (seasonal): DataFrame has only `year` column, no `season_year` → guard falls back to checking `year`, valid rows written successfully

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

After fix, re-run the seasonal write and confirm no `cannot convert float NaN to integer` error appears in the log. The seasonal batch count should match the number of valid rows.

---

## Documentation Impact

- [ ] No documentation impact — purely an internal validation guard with no behavior change for valid data.

---

## Out of Scope

- Investigating *why* `season_year` is NaN in some rows (upstream data issue, separate from this guard)
- Changes to the seasonal forecast data preparation logic
- Removing the unused `period_col` parameter from `_write_aggregated_forecasts_to_api()` (dead code — never referenced in the function body; cleanup for a separate PR)

## Dependencies

None.

## Acceptance Criteria

- [ ] `_write_aggregated_forecasts_to_api()` does not crash when rows have NaN in year/period columns
- [ ] A WARNING is logged for each batch that has rows skipped (verified via `caplog`)
- [ ] Valid rows in the same batch are written successfully (verified by asserting on actual records passed to `client.write_long_forecasts()`)
- [ ] Rows with NaN in `forecasted_discharge` (but valid year/period) are NOT dropped — they write with `q=None`
- [ ] All existing tests pass (`run_tests.sh postprocessing_forecasts` — zero failures, zero unexpected skips)
- [ ] New test covers the NaN-guard behaviour for both quarter and season horizon types

---

## References

- Related issue: PP-027 (EM ensemble observability)
- Pipeline log: `apps/logs/run_locally_20260327_170642.log` — `ERROR - Failed to write seasonal forecasts to API: cannot convert float NaN to integer`
- Short-term NaN guard reference: `apps/postprocessing_forecasts/src/api_writer.py:282–297`
