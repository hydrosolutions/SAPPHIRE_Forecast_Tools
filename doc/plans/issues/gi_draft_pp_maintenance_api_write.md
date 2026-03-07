# PP-024: Write maintenance gap-fill records directly to API

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `bug`, `maintenance`, `data-quality`

---

## Summary

The maintenance pipeline detects and refreshes stale/missing forecast records,
but only writes "latest" forecasts to the API. Gap-filled records for older
dates go to CSV but never reach the database, leaving the DB gap intact.

## Context

The maintenance pipeline (`postprocessing_maintenance.py`) builds a `merged`
DataFrame containing all combined forecasts (existing + refreshed rows), then
calls `file_writer.save_forecast_data(config, merged)`.

`save_forecast_data` does three things:

1. **Full CSV**: writes all rows (`merged`) to the combined CSV file.
2. **Latest CSV**: extracts "latest" via `get_latest_forecasts()` and writes
   to the `_latest.csv` file.
3. **API**: writes `simulated_latest` (the filtered output) to the
   postprocessing API.

`get_latest_forecasts()` (`file_writer.py:86-144`) deduplicates on
`(code, period_in_year, model_short)` keeping only the most recent date,
then filters to `year >= (latest_year - 1)`. Any gap-filled record that is
shadowed by a newer-year record for the same period is discarded before the
API write.

## Problem

When maintenance fills gaps for dates where **a newer year already has a
record for the same (code, period_in_year, model_short)**, those refreshed
records:

- Go into the full CSV (correct)
- Are filtered out by `get_latest_forecasts` (by design for operational use)
- Never reach the API (bug for maintenance use case)

The database retains the original gap or stale record.

### When does this happen?

A refreshed record is dropped when the current year already has a forecast
for the same (code, period_in_year, model_short). This means:

- Pentads/decades that have already occurred in the current year shadow
  stale records from the previous year.
- As the year progresses, more periods are affected: ~25% of periods in
  March, ~50% by July, ~100% by December.
- Pentads/decades not yet reached in the current year are fine -- the
  old-year record IS the "latest" and reaches the API.

### Example

```
DB state:  code=X, date=2025-01-05, model=TFT, pentad=1, q05=NULL (stale)
           code=X, date=2026-01-05, model=TFT, pentad=1, q05=3.1 (current year)

Maintenance detects stale 2025-01-05, reads fresh data, gets q05=2.5.
Merged DataFrame has both rows with correct quantiles.

get_latest_forecasts deduplicates on (code, pentad=1, TFT):
  keeps 2026-01-05 (newer), drops 2025-01-05 (refreshed but older).
API write: only 2026-01-05 row sent.
DB state: 2025-01-05 still has q05=NULL.
```

Note: if the stale record's `pentad_in_year` has NO newer-year counterpart
(e.g., a stale pentad 42 from 2025 when it's March 2026), the stale record
IS the latest and WOULD reach the API. The bug is specific to shadowed
periods.

### Relationship to PP-022

PP-022 (`gi_draft_pp_maintenance_stale_refresh_fix.md`) fixed the detection
and merge logic so that stale individual/NE/EM rows are correctly refreshed
in the DataFrame. All PP-022 steps are implemented except the acceptance
criteria that the refreshed rows reach the API. This issue closes that gap.

---

## Technical Analysis

### `_write_combined_forecast_to_api` accepts any date

The API writer (`api_writer.py:102-349`) has no date validation. It will
accept and write records from any date. The function:

- Computes missing horizon values from dates (self-healing)
- Deduplicates on the DB unique constraint `(horizon_type, code, model_type, date, target)`
- Drops null-discharge records (important: logged count may differ from
  the count actually written)
- Uses upsert semantics (ON CONFLICT DO UPDATE)

There is no technical barrier to writing historical records directly.

### Options considered

**Option A: Call API writer directly from maintenance with gap-fill rows**

After building `joint` (the refreshed rows), call
`_write_combined_forecast_to_api(joint, config.name)` directly, before or
after `save_forecast_data`.

- Pros: Minimal change. Targeted write of exactly the rows that need updating.
- Cons: Bypasses CSV consistency check. Slight duplication if some rows overlap
  with "latest".

**Option B: Add `write_all_to_api` parameter to `save_forecast_data`**

```python
def save_forecast_data(config, simulated, write_all_to_api=False):
```

When True, write the full `simulated` DataFrame to API instead of just
`simulated_latest`.

- Pros: Keeps all write logic in one place.
- Cons: Sending the full combined CSV (potentially millions of rows) to
  the API is expensive and unnecessary. The API uses upsert, so it would
  work, but it's wasteful.

**Option C: Write only the `joint` (refreshed) rows to API from maintenance**

Call `_write_combined_forecast_to_api` with only the `joint` DataFrame
(the rows that were actually refreshed), separate from `save_forecast_data`.
Continue calling `save_forecast_data` for CSV output and latest-API write.

- Pros: Precise. Only refreshed rows are sent. No wasted API calls.
  `save_forecast_data` is unchanged.
- Cons: Two API writes per maintenance run (joint + latest). The "latest"
  write may partially overlap with joint, but upsert makes this idempotent.

### Chosen approach: Option C

Option C is the safest and most precise. The maintenance entry point already
has the `joint` DataFrame (line 335). Writing it directly to the API before
calling `save_forecast_data` ensures all refreshed rows reach the DB. The
subsequent `save_forecast_data` call handles CSV output and the "latest" API
write as before. Upsert semantics prevent conflicts.

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | Import `api_writer`; add direct API write of `joint` before `save_forecast_data` |
| `apps/postprocessing_forecasts/tests/test_maintenance_workflow.py` | Test that API writer is called with refreshed rows |

### Implementation Steps

- [ ] **Step 1: Add direct API write of refreshed rows in maintenance**

  In `postprocessing_maintenance.py`:

  **1a.** Add `api_writer` to the import line (not currently imported):

  ```python
  from src import api_writer, data_reader, ensemble_calculator, file_writer, gap_detector
  ```

  **1b.** In `_fill_gaps_for_horizon()`, after building `joint` (line 335,
  after `joint = joint.dropna(axis=1, how="all")` at line 342) and before
  the merge into `combined` (line 351), add:

  ```python
  # Write refreshed rows directly to API (bypasses get_latest_forecasts
  # filter so that historical gap-fills reach the database).
  if not joint.empty:
      try:
          ok = api_writer._write_combined_forecast_to_api(joint, config.name)
          if ok:
              logger.info(
                  "%s: submitted %d refreshed rows to API",
                  label, len(joint),
              )
          else:
              logger.warning(
                  "%s: direct API write returned False "
                  "(API may be unavailable or data filtered)",
                  label,
              )
      except Exception as e:
          logger.error(
              "%s: direct API write of refreshed rows failed: %s",
              label, e,
          )
          errors.append(f"{label} direct API write failed: {e}")
  ```

  **Why before `save_forecast_data`**: If the direct write succeeds but
  `save_forecast_data` fails, the DB still has the refreshed data. If the
  direct write fails, `save_forecast_data` still saves to CSV + writes
  "latest" to API. Either way, we don't lose data.

  **No redundant guard**: `_write_combined_forecast_to_api` already checks
  `SAPPHIRE_API_AVAILABLE` internally (line 146) and returns `False` when
  unavailable. The caller does not need to duplicate this check.

  **Return value**: `_write_combined_forecast_to_api` returns `bool`. Check
  it to distinguish success from silent skip (API disabled, data filtered).

  **Private function note**: `_write_combined_forecast_to_api` is `_`-prefixed
  but is now used by two callers. Accept this for now; renaming is tech debt.

  **Batch size note**: `_write_combined_forecast_to_api` sends all records
  in a single `client.write_forecasts()` call. If `joint` is very large
  (hundreds of stale records across 13 months), the API must handle this.
  The postprocessing API uses bulk upsert, so this should be fine, but
  monitor for timeouts on first deployment.

- [ ] **Step 2: Add tests for direct API write**

  In `test_maintenance_workflow.py`, add:

  ```python
  def test_gap_fill_writes_refreshed_rows_to_api(self):
      """Refreshed rows are written directly to API, not just via
      save_forecast_data's latest filter."""
      # Set up: combined has stale rows, modelled has fresh data
      # Mock api_writer._write_combined_forecast_to_api
      # Run _fill_gaps_for_horizon
      # Assert _write_combined_forecast_to_api was called
      # Assert the DataFrame passed contains the refreshed rows

  def test_api_write_failure_does_not_block_csv_save(self):
      """If direct API write fails, CSV save still proceeds."""
      # Mock _write_combined_forecast_to_api to raise
      # Assert save_forecast_data still called
      # Assert error is appended to errors list

  def test_api_write_returns_false_logged_as_warning(self):
      """When _write_combined_forecast_to_api returns False, a warning is logged."""
      # Mock _write_combined_forecast_to_api to return False
      # Assert warning logged, no error appended
  ```

- [ ] **Step 3: Run tests**

  ```bash
  cd apps
  SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
  ```

- [ ] **Step 4: Verify with SQL after deployment**

  ```sql
  -- Check stale record count before and after maintenance:
  SELECT model_type, COUNT(*) FROM forecasts
  WHERE horizon_type IN ('pentad', 'decade')
    AND q05 IS NULL
    AND date >= NOW() - INTERVAL '13 months'
  GROUP BY model_type;
  ```

---

## Testing

### Test Cases

- [ ] Refreshed rows (EM + NE + individual) are written to API via direct
  `_write_combined_forecast_to_api` call
- [ ] Historical dates (shadowed by newer-year records) reach the API
- [ ] Direct API write failure does not block CSV save
- [ ] Direct API write failure is logged and added to errors list
- [ ] When `_write_combined_forecast_to_api` returns False, a warning is logged
- [ ] When `joint` is empty, no API call is made
- [ ] Upsert semantics: rows that overlap between direct write and
  save_forecast_data's latest write don't cause conflicts

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

---

## Acceptance Criteria

- [ ] After maintenance, stale records (within lookback window) have q05
  populated in the database, not just in the CSV
- [ ] Historical gap-fill records shadowed by newer-year records are
  written to the database
- [ ] Direct API write failure is non-fatal (CSV save still proceeds)
- [ ] No regression in operational pipeline (save_forecast_data unchanged)
- [ ] All existing tests pass; 3 new tests cover the direct write path

---

## Out of Scope

- Changing `save_forecast_data` or `get_latest_forecasts` behavior (those
  are correct for the operational pipeline)
- Bulk historical backfill (handled by `reaggregate_day_to_periods.py`)
- Optimizing the combined forecast API read (unbounded pagination -- separate
  performance issue)

## Dependencies

- PP-022 (stale refresh detection and merge): implemented
- PP-023 (period-aware aggregation): independent, but both affect data
  quality of the same records

## References

- PP-022 plan: `doc/plans/issues/gi_draft_pp_maintenance_stale_refresh_fix.md`
- Maintenance entry point: `apps/postprocessing_forecasts/postprocessing_maintenance.py`
- API writer: `apps/postprocessing_forecasts/src/api_writer.py`
- File writer: `apps/postprocessing_forecasts/src/file_writer.py`
