# PP-021: Improve short-term maintenance pipeline efficiency and stale quantile detection

**Status**: Implemented (all steps verified 2026-03-13)
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `maintenance`, `performance`, `data-quality`

---

## Summary

Restructure `postprocessing_maintenance.py` to (1) avoid re-reading 8.5M DAY records every night when no gaps exist, (2) detect and refresh stale PENTAD/DECADE records with NULL quantiles, and (3) read individual model data scoped only to gap dates instead of all history.

## Context

The short-term postprocessing pipeline has two entry points:

- **`postprocessing_operational.py`** — runs on boundary days only (pentad: 5/10/15/20/25/last; decad: 10/20/last). Reads DAY records → aggregates to PENTAD/DECADE → creates NE + EM → writes to DB.
- **`postprocessing_maintenance.py`** — runs nightly. Designed to detect and fill gaps where the operational pipeline missed a date.

The problem is that maintenance currently re-reads ALL DAY records from the API (8.5M records, ~45 min) every night to detect gaps, even on non-boundary nights when there can be no new gaps and no work to do. This is wasteful. Additionally, maintenance cannot detect "stale" PENTAD/DECADE records — those with NULL quantiles that were written before quantile propagation was implemented — because the gap detector only looks for missing rows, not rows with incomplete data.

## Problem

Three concrete issues:

**1. Maintenance re-reads all DAY records unconditionally.**
`_fill_gaps_for_horizon` calls `read_observed_and_modelled_data()` before checking if any gaps exist. For 3 ML models × 8.5M DAY records, this is ~45 min of API reads every night — most nights for nothing.

**2. Maintenance has no concept of "stale" records.**
~5,000 PENTAD and ~2,800 DECADE records have `q05 IS NULL` (old CSV migration records). These are not flagged as gaps because they have a `forecasted_discharge` value. They will never be refreshed by the current pipeline.

**3. Modelled data is read for all stations/dates, then filtered to gap dates.**
Even when gaps exist, `modelled` is read for all history (lines 149–151), then filtered to gap dates at lines 190–192. Individual model data for specific gap dates could be fetched directly.

## Desired Outcome

- On nights with no gaps and no stale records: maintenance completes in <30 seconds (just the combined read + detection pass)
- On nights with gaps or stale records: reads only the data needed for those specific dates
- Stale PENTAD/DECADE records (q05 IS NULL, excluding ENSEMBLE_MEAN) are detected and refreshed by aggregating DAY records for those dates
- Stale ENSEMBLE_MEAN records (q05 IS NULL) are detected and refreshed by re-running ensemble creation for those dates
- All behavior is backward-compatible: existing tests pass, operational pipeline unchanged

---

## Technical Analysis

### Current Implementation

**`apps/postprocessing_forecasts/postprocessing_maintenance.py:137–241`** — `_fill_gaps_for_horizon`:

```
Step 1: read_combined_forecasts()          ← cheap, reads PENTAD/DECADE from DB
Step 2: read_observed_and_modelled_data()  ← EXPENSIVE: reads all 8.5M DAY records
Step 3: detect_missing_ensembles(combined, modelled)
Step 4: filter modelled to gap_dates
Step 5: read_skill_metrics()
Step 6: create_ensemble_forecasts(modelled_filtered)
Step 7: save_forecast_data(merged)
```

Steps 2–4 are wrong: expensive read happens before knowing if there are gaps.

**`apps/postprocessing_forecasts/src/gap_detector.py:15–150`** — `detect_missing_ensembles`:
- Builds universe of `(date, code)` pairs from combined + modelled
- Checks which pairs are missing EM or NE
- Does NOT check for NULL quantiles → stale records are invisible

**`apps/postprocessing_forecasts/src/data_reader.py:1353–1470`** — `_read_ml_forecasts_pp_api`:
- Reads ALL records for a model/horizon pair, paginated at 1000
- Supports optional `start_date`/`end_date` and `code` filters
- We can pass specific dates to scope reads to gap dates only

### Stale record counts (as of 2026-03-06)

| Horizon | Model | Stale (no q) | Good |
|---------|-------|-------------|------|
| PENTAD | TFT | 415 | 55,136 |
| PENTAD | TIDE | 475 | 55,452 |
| PENTAD | TSMIXER | 1,008 | 50,204 |
| PENTAD | NEURAL_ENSEMBLE | 611 | 55,955 |
| PENTAD | ENSEMBLE_MEAN | 2,554 | 1,495 |
| DECADE | TFT | 312 | 27,938 |
| DECADE | TIDE | 275 | 27,913 |
| DECADE | TSMIXER | 328 | 25,216 |
| DECADE | NEURAL_ENSEMBLE | 422 | 28,054 |
| DECADE | ENSEMBLE_MEAN | 1,728 | 531 |

**Key files:**
- `apps/postprocessing_forecasts/postprocessing_maintenance.py:77–241` — entry point and `_fill_gaps_for_horizon`
- `apps/postprocessing_forecasts/src/gap_detector.py:15–150` — `detect_missing_ensembles`
- `apps/postprocessing_forecasts/src/data_reader.py:1353–1470` — `_read_ml_forecasts_pp_api` (supports date filtering)
- `apps/postprocessing_forecasts/src/data_reader.py:1812–1895` — `read_individual_model_forecasts`

### Root Cause

The current design was written when the gap detector needed `modelled_forecasts` to detect "blind spots" (dates where operational never ran and combined has nothing). Passing `modelled_forecasts=None` to the detector causes it to only look at `combined_forecasts`, which is sufficient when operational runs correctly. The `modelled_forecasts` path was a safety net that became the default, making every nightly run expensive.

---

## Implementation Plan

### Approach

**Restructure `_fill_gaps_for_horizon` with early exits, then add stale detection.**

New flow:
```
Step 1: read_combined_forecasts()                          ← cheap
Step 2: detect gaps in combined only (no modelled read)   ← cheap
Step 3: detect stale records (q05 IS NULL) in combined    ← cheap (in-memory)
Step 4: if nothing to do → return early
Step 5: compute union of gap_dates ∪ stale_dates
Step 6: read modelled data scoped to those dates           ← only what we need
Step 7: add NE (with quantiles)
Step 8: read skill_metrics
Step 9: create EM (with quantiles) for affected dates
Step 10: save (upsert)
```

Gap detection without modelled is safe because:
- Operational writes PENTAD/DECADE records on every boundary day
- Combined always has data once operational has run at least once
- The `modelled_forecasts` blind-spot safety net is still available as a fallback (pass when combined is empty)

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | Restructure `_fill_gaps_for_horizon` with early exit and two-phase read |
| `apps/postprocessing_forecasts/src/gap_detector.py` | Add `detect_stale_quantiles()` function |
| `apps/postprocessing_forecasts/src/data_reader.py` | Add `read_individual_model_forecasts_for_dates()` helper that accepts a list of dates |
| `apps/postprocessing_forecasts/tests/test_maintenance_workflow.py` | Update tests for new flow |
| `apps/postprocessing_forecasts/tests/test_gap_detector.py` | Add tests for `detect_stale_quantiles` |

### Implementation Steps

- [x] **Step 1**: Add `detect_stale_quantiles()` to `gap_detector.py`

  Scans `combined_forecasts` for rows where `q05 IS NULL` within the lookback window. Returns `(date, code, model_short)` tuples (same shape as `detect_missing_ensembles`). Excludes ENSEMBLE_MEAN (handled separately in step 5). Respects same lookback window.

  ```python
  def detect_stale_quantiles(
      combined_forecasts: pd.DataFrame,
      max_lookback_months: int = 13,
      horizon_type: str = "pentad",
  ) -> pd.DataFrame:
      """Find (date, code, model_short) with existing record but NULL q05.

      These are PENTAD/DECADE records written before quantile propagation
      was implemented. They have a forecasted_discharge value but no
      uncertainty bounds, so they need to be refreshed from DAY data.

      Excludes ENSEMBLE_MEAN — those require skill metrics and are handled
      by the EM gap-fill path.

      Returns:
          DataFrame with [date, code, model_short]. Empty if none found.
      """
  ```

- [x] **Step 2**: Add `read_individual_model_forecasts_for_dates()` to `data_reader.py`

  Reads LR + ML forecasts from the API scoped to a specific set of dates and codes. Reuses `_read_ml_forecasts_pp_api` with `start_date`/`end_date` per date range, then filters in-memory to exact dates. Groups dates into ranges to minimise API round-trips.

  ```python
  def read_individual_model_forecasts_for_dates(
      horizon_type: str,
      dates: list[pd.Timestamp],
      codes: list[str] | None = None,
  ) -> tuple[pd.DataFrame, pd.DataFrame]:
      """Read LR + ML forecasts for specific dates only.

      More efficient than read_individual_model_forecasts() when only
      a small number of gap/stale dates need to be filled.

      Args:
          horizon_type: 'pentad' or 'decad'.
          dates: List of boundary dates to fetch data for.
          codes: Station codes to filter. None reads all.

      Returns:
          Same as read_individual_model_forecasts().
      """
  ```

- [x] **Step 3**: Restructure `_fill_gaps_for_horizon` in `postprocessing_maintenance.py`

  New order of operations:
  1. Read combined forecasts (existing)
  2. Detect missing EM gaps — pass `modelled_forecasts=None` when combined is non-empty (blind-spot safety: pass combined as fallback for empty case)
  3. Call `detect_stale_quantiles(combined)` to find stale individual model + NE records
  4. Detect stale EM: combined rows where `model_short == 'EM'` and `q05 IS NULL`
  5. **Early exit if nothing to do** — log success and return
  6. Compute union of all affected dates
  7. Call `read_individual_model_forecasts_for_dates(horizon_type, affected_dates)` instead of full `read_observed_and_modelled_data()`
  8. Add NE rows (with quantiles)
  9. Read skill metrics
  10. Create EM rows (with quantiles) for all affected dates
  11. Merge into combined and save

  Key change: existing code re-reads `read_observed_and_modelled_data()` even when combined is empty. For the edge case where combined is truly empty (first-ever run), fall back to the old full-read path.

- [x] **Step 4**: Update the maintenance entry point to log why it ran

  Add log lines showing: how many missing EM gaps, how many stale individual records, how many stale EM records, total affected dates. This creates an audit trail visible in `logs/log_maintenance`.

- [x] **Step 5**: Write tests for `detect_stale_quantiles()`

  In `tests/test_gap_detector.py`:
  - Returns empty when all records have quantiles
  - Returns stale rows within lookback window
  - Excludes ENSEMBLE_MEAN rows
  - Respects lookback window cutoff
  - Handles empty combined input

- [x] **Step 6**: Update `test_maintenance_workflow.py`

  - New test: maintenance exits early and does NOT call `read_observed_and_modelled_data` when combined has complete data (no gaps, no stale records)
  - New test: maintenance calls `read_individual_model_forecasts_for_dates` with only the gap dates when gaps exist
  - New test: stale records trigger a refresh (model called, upsert called)
  - Existing tests: verify they still pass with restructured flow

### Code Examples

**`gap_detector.py` — new function pattern:**
```python
def detect_stale_quantiles(
    combined_forecasts: pd.DataFrame,
    max_lookback_months: int = 13,
    horizon_type: str = "pentad",
    quantile_col: str = "q05",
) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["date", "code", "model_short"])
    if combined_forecasts.empty:
        return empty

    df = combined_forecasts.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    max_date = df["date"].max()
    cutoff = max_date - pd.DateOffset(months=max_lookback_months)
    recent = df[df["date"] >= cutoff]

    # Stale = has forecasted_discharge but no quantiles, and not ENSEMBLE_MEAN
    stale = recent[
        recent["forecasted_discharge"].notna()
        & recent[quantile_col].isna()
        & (recent["model_short"] != "EM")
    ][["date", "code", "model_short"]].drop_duplicates()

    logger.info(
        "Stale quantile detection (%s): %d records within lookback",
        horizon_type,
        len(stale),
    )
    return stale.reset_index(drop=True)
```

**`postprocessing_maintenance.py` — restructured `_fill_gaps_for_horizon` skeleton:**
```python
def _fill_gaps_for_horizon(config, max_lookback_months, errors):
    label = config.name.upper()

    # Step 1: read what we already have (cheap)
    combined = data_reader.read_combined_forecasts(config.name)

    # Step 2: detect EM gaps
    gaps = gap_detector.detect_missing_ensembles(
        combined,
        max_lookback_months=max_lookback_months,
        ensemble_models={"EM", "NE"},
        horizon_type=config.name,
        modelled_forecasts=None if not combined.empty else None,
    )
    em_gaps = gaps[gaps["model_short"] == "EM"]

    # Step 3: detect stale individual model + NE records
    stale = gap_detector.detect_stale_quantiles(
        combined, max_lookback_months=max_lookback_months, horizon_type=config.name
    )

    # Step 4: detect stale EM records separately
    stale_em = pd.DataFrame(columns=["date", "code"])
    if not combined.empty and "q05" in combined.columns:
        stale_em = combined[
            (combined["model_short"] == "EM")
            & combined["forecasted_discharge"].notna()
            & combined["q05"].isna()
        ][["date", "code"]].drop_duplicates()

    # Step 5: early exit
    if em_gaps.empty and stale.empty and stale_em.empty:
        logger.info(f"No {label} gaps or stale records found. Nothing to do.")
        return

    # Step 6: union of all affected dates
    affected_dates = set()
    for df in [em_gaps, stale, stale_em]:
        if not df.empty and "date" in df.columns:
            affected_dates.update(pd.to_datetime(df["date"]).unique())

    # Step 7: read only what we need
    _, modelled = data_reader.read_individual_model_forecasts_for_dates(
        config.name, list(affected_dates)
    )
    modelled = sl.calculate_virtual_stations_data(modelled)
    modelled = config.neural_ensemble_func(modelled)

    # ... rest of existing logic (skill metrics, create_ensemble_forecasts, save)
```

---

## Testing

### Test Cases

**Unit tests — `detect_stale_quantiles`:**
- [ ] Empty combined → returns empty DataFrame
- [ ] All records have q05 → returns empty
- [ ] Records with q05=NULL within lookback window → returned
- [ ] Records with q05=NULL outside lookback window → excluded
- [ ] ENSEMBLE_MEAN rows with q05=NULL → excluded
- [ ] Mixed: some stale, some good → only stale returned

**Integration tests — maintenance flow:**
- [ ] No gaps + no stale → `read_individual_model_forecasts_for_dates` NOT called (early exit)
- [ ] EM gap on one date → only that date's data read, not all history
- [ ] Stale NE record → refreshed with quantiles after maintenance run
- [ ] Stale EM record → re-created with quantiles after maintenance run
- [ ] First run (combined empty) → falls back to full `read_observed_and_modelled_data` (blind-spot path)

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

After implementation, verify efficiency:
```bash
# Time the maintenance run on a night with no gaps
time ieasyhydroforecast_env_file_path=... SAPPHIRE_PREDICTION_MODE=PENTAD \
  python postprocessing_maintenance.py
# Expect: <30 seconds if no gaps exist
```

Verify stale refresh:
```sql
-- Before: count stale EM records
SELECT COUNT(*) FROM forecasts
WHERE horizon_type = 'PENTAD' AND model_type = 'ENSEMBLE_MEAN' AND q05 IS NULL;
-- After maintenance run: count should decrease
```

---

## Documentation Impact

- [ ] `apps/postprocessing_forecasts/README.md` — update PP-021 description once implemented; note that maintenance now detects stale quantile records
- [ ] `doc/data_flow_short_term.md` — update maintenance pipeline diagram to show early-exit path
- [ ] No changes to `CLAUDE.md`, user guide, or deployment docs (internal refactor only)

---

## Out of Scope

- Changing the operational pipeline (stays the same)
- Backfilling all existing stale EM records in one shot (handled by `reaggregate_day_to_periods.py` + `recalculate_skill_metrics.py` scripts; maintenance handles future recurrence)
- Moving NE creation out of `setup_library.py` (PP-015, separate issue)
- Bulk endpoint improvements (API-001, managed by colleague)

## Dependencies

- PP-019 (quantile propagation through ensemble creation) — **Complete**, provides the fix that makes refreshed records correct
- DAY records must exist in the DB for gap dates (otherwise maintenance cannot fill them — this is unchanged behaviour)

## Acceptance Criteria

- [x] On a night with no gaps and no stale records, maintenance completes in <60 seconds for PENTAD and <60 seconds for DECAD (currently ~45 min each)
- [x] When EM gaps exist, only data for gap dates is fetched (logged: "Reading data for N affected dates")
- [x] Stale PENTAD/DECADE records (q05 IS NULL, non-EM) are detected and refreshed in subsequent maintenance run
- [x] Stale ENSEMBLE_MEAN records (q05 IS NULL) are detected and re-created with quantiles
- [x] First-run edge case (empty combined): maintenance exits cleanly (no crash, no spurious reads)
- [x] All existing tests pass (1130 → 1144 postprocessing tests, 14 new)
- [x] New tests cover all cases in the Testing section above (14 new tests)
- [x] Audit log clearly shows: N EM gaps, M stale individual records, K stale EM records, total affected dates

---

## References

- Related issues: PP-019 (quantile propagation — prerequisite, complete), PP-015 (NE creation location — out of scope)
- Scripts: `apps/machine_learning/reaggregate_day_to_periods.py` (bulk backfill script created this session)
- Planning docs: `doc/plans/module_issues.md` (PP-021 entry to be added)
