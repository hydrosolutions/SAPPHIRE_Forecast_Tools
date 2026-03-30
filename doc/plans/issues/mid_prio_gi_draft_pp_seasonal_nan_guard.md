# PP-029: Guard against NaN in seasonal/quarterly API write

**Status**: Draft
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

- Rows with NaN `season_year` / `year` / `quarter_in_year` are skipped with a WARNING log (not silently dropping the whole batch).
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

---

## Implementation Plan

### Approach

Add a `dropna()` call on the key columns before the `for _, row in data.iterrows()` loop (lines 968–969), following the exact same pattern as the short-term writer. Log skipped rows at WARNING level.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/api_writer.py` | Add NaN guard before line 969 |

### Implementation Steps

- [ ] After line 941 (`if data is None or data.empty`) but before line 968 (`records = []`), determine the key columns to check:
  - For `horizon_type == "quarter"`: check `["year", "quarter_in_year"]`
  - For `horizon_type == "season"`: check `["season_year"]` (fall back to `["year"]` if `season_year` not in columns)
- [ ] Add `dropna()` on those columns with a WARNING log matching the short-term pattern:
  ```python
  # Determine NaN-check columns based on horizon type
  if horizon_type == "quarter":
      nan_check_cols = [c for c in ["year", "quarter_in_year"] if c in data.columns]
  else:  # season
      nan_check_cols = ["season_year"] if "season_year" in data.columns else ["year"]

  n_before = len(data)
  data = data.dropna(subset=nan_check_cols)
  skipped_nan = n_before - len(data)
  if skipped_nan > 0:
      dropped_detail = (
          data_orig[data_orig[nan_check_cols[0]].isna()][["code"]]
          .drop_duplicates().head(10).to_dict("records")
      )
      logger.warning(
          "Dropped %d %s forecast records with missing year/period values. "
          "Sample codes: %s",
          skipped_nan, label, dropped_detail,
      )
  ```
  Note: keep a reference to the original `data` for dropped-row inspection before the `dropna`.
- [ ] Run tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`

### Code Example

```python
# Insert after `if data is None or data.empty:` guard, before `records = []`

# Determine which columns to check for NaN before int() conversion
if horizon_type == "quarter":
    nan_check_cols = [c for c in ["year", "quarter_in_year"] if c in data.columns]
else:  # season
    nan_check_cols = ["season_year"] if "season_year" in data.columns else ["year"]

nan_check_cols = [c for c in nan_check_cols if c in data.columns]
if nan_check_cols:
    data_before_nan_drop = data  # keep reference for diagnostics
    data = data.dropna(subset=nan_check_cols)
    skipped_nan = len(data_before_nan_drop) - len(data)
    if skipped_nan > 0:
        dropped_codes = (
            data_before_nan_drop[data_before_nan_drop[nan_check_cols[0]].isna()][["code"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        logger.warning(
            "Dropped %d %s forecast records with missing year/period values. "
            "Sample codes: %s",
            skipped_nan,
            label,
            dropped_codes,
        )

if data.empty:
    logger.info("No %s forecast records to write to API after NaN removal", label)
    return False
```

---

## Testing

### Test Cases

- [ ] NaN in `season_year`: DataFrame with 3 rows, one has `season_year=NaN` → 2 records written, 1 WARNING logged
- [ ] NaN in `year` (quarter): DataFrame with 3 rows, one has `year=NaN` → 2 records written, 1 WARNING logged
- [ ] NaN in `quarter_in_year`: DataFrame with 3 rows, one has `quarter_in_year=NaN` → 2 records written, 1 WARNING logged
- [ ] All rows valid → all written, no WARNING
- [ ] All rows NaN → returns `False`, logs INFO "No records to write"

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

## Dependencies

None.

## Acceptance Criteria

- [ ] `_write_aggregated_forecasts_to_api()` does not crash when rows have NaN in year/period columns
- [ ] A WARNING is logged for each batch that has rows skipped
- [ ] Valid rows in the same batch are written successfully
- [ ] All existing tests pass (`run_tests.sh postprocessing_forecasts` — zero failures, zero unexpected skips)
- [ ] New test covers the NaN-guard behaviour for both quarter and season horizon types

---

## References

- Related issue: PP-027 (EM ensemble observability)
- Pipeline log: `apps/logs/run_locally_20260327_170642.log` — `ERROR - Failed to write seasonal forecasts to API: cannot convert float NaN to integer`
- Short-term NaN guard reference: `apps/postprocessing_forecasts/src/api_writer.py:282–297`
