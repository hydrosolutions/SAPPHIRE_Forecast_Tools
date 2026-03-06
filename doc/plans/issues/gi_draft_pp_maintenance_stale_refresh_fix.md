# PP-022: Fix stale-record refresh and minor inconsistencies in maintenance pipeline

**Status**: Ready for Review
**Module**: postprocessing_forecasts
**Priority**: Critical
**Labels**: `bug`, `maintenance`, `data-quality`

---

## Summary

Fix four bugs introduced in PP-021 (`postprocessing_maintenance.py`) where stale
records are detected but never actually refreshed, plus three smaller correctness
and efficiency issues.

## Context

PP-021 restructured `postprocessing_maintenance.py` to avoid reading 8.5 M DAY
records on nights with no gaps, and added detection of "stale" PENTAD/DECADE rows
that have `forecasted_discharge` but `q05 IS NULL`. The detection logic was
implemented correctly but the **save path was not updated** to write the refreshed
rows back — only new EM rows are saved. Stale individual-model, NE, and EM records
are found, trigger a full data read, and are then silently discarded.

## Problem

Seven issues were found on code review after PP-021. They are grouped below by
severity.

### Bug A — Stale individual-model / NE records detected but never saved (HIGH)

`_fill_gaps_for_horizon` (`postprocessing_maintenance.py:259-273`) calls
`ensemble_calculator.create_ensemble_forecasts(modelled_filtered, ...)`, which
returns `joint` containing:

- Refreshed individual-model rows (LR, TFT, TiDE, TSMixer) with quantiles
- Refreshed NE rows with quantiles (added by `config.neural_ensemble_func`)
- New EM rows (added by `create_ensemble_forecasts`)

Then line 269 does:

```python
new_em_rows = joint[joint["model_short"] == "EM"].copy()
```

Only EM rows are kept; all refreshed individual-model and NE rows are discarded.
The stale `q05=NULL` records in `combined` are never replaced.

**Fix**: merge all rows from `joint` (not just EM) back into `combined`.

### Bug B — Stale EM records in combined are not replaced (MEDIUM)

Even if Bug A is fixed, the current merge at line 276:

```python
merged = pd.concat([combined, new_em_rows], ignore_index=True)
merged = merged.drop_duplicates(subset=["date", "code", "model_short"], keep="last")
```

puts `combined` first and `new_em_rows` last, so `keep="last"` does replace
stale EM entries — but only if the dedup key matches. Bug C below shows the key
is too narrow.

### Bug C — `drop_duplicates` key is missing `period_col` (LOW)

The dedup subset is `["date", "code", "model_short"]`. The operational pipeline
uses `(date, code, period_col, model_short)` as the natural key (consistent with
`file_writer.py:233` consistency check). For short-term horizons there is at most
one period per date, so in practice this doesn't cause data loss today, but it
diverges from the canonical key definition and will silently misbehave if the
assumption changes.

**Fix**: use `["date", "code", config.period_col, "model_short"]` as the dedup
subset.

### Bug D — `gap_codes` computed after data read; `codes` filter unused (LOW)

`gap_codes` is assembled at lines 239–242 (after the data read at line 228).
`read_individual_model_forecasts_for_dates` accepts a `codes` keyword argument
that scopes the API queries — passing `gap_codes` would reduce the data volume
when only a few stations have gaps.

**Fix**: compute `gap_codes` before the data read and pass it as `codes`.

### Issue E — Date-scoped reader is actually year-scoped (MEDIUM, out of scope)

`read_individual_model_forecasts_for_dates` (`data_reader.py:1898`) converts the
input date list to a `min_year`/`max_year` range and calls the full reader with
year boundaries. If gap dates span two calendar years, both full years are fetched.
This is still much better than reading all history, but does not achieve the
"reads only gap dates" goal stated in PP-021.

Fixing this properly requires passing specific dates or a tight date range to each
underlying API call (`_read_lr_forecasts_pp_api`, `_read_ml_forecasts_pp_api`).
Those functions already accept `start_date` / `end_date` as ISO strings. The
improvement is to derive tight bounds (min/max date in `dates`) rather than
min/max year, and if dates cluster in a single month, restrict to that month.

This is tracked as a separate follow-on because it requires changes to
`_read_ml_forecasts_pp_api` parameter passing and deserves its own tests. It does
not affect correctness, only efficiency.

### Issue F — NE gaps logged as "not fillable" but NE rows exist in `joint` (INFO)

`_fill_gaps_for_horizon` detects NE gaps and logs a warning
("created by operational pipeline, not fillable by maintenance") but then calls
`config.neural_ensemble_func(modelled)` which does create NE rows with quantiles.
After Bug A is fixed (saving all of `joint`), freshly created NE rows will be
saved correctly, making the warning partially incorrect. The comment should be
updated to reflect the real constraint: NE rows for gap dates CAN be created from
modelled data, but if modelled data is missing for a gap date then NE cannot be
created.

### Issue G — Operational reads all history; maintenance reads scoped (INFO)

The operational pipeline reads all individual-model history for ensemble creation;
maintenance reads only affected dates. This is intentional (operational needs full
history for the latest-forecast extraction in `get_latest_forecasts`; maintenance
only needs enough to build EM for gap dates). No fix needed — document the
asymmetry in the docstring.

---

## Desired Outcome

- Stale individual-model, NE, and EM rows (`q05 IS NULL`) are detected AND
  replaced in the output after a maintenance run.
- The `drop_duplicates` key is consistent with the operational pipeline's natural
  key.
- `gap_codes` is computed before the data read and passed to the API query.
- Warning comment about NE gaps is accurate.
- All existing tests pass; new tests cover the fixed save path.

---

## Technical Analysis

### Current Implementation

**`postprocessing_maintenance.py:259-277`** — the broken save path:

```python
# Step 7: create EM rows with quantiles
joint, _ = ensemble_calculator.create_ensemble_forecasts(
    forecasts=modelled_filtered,
    skill_stats=skill_stats,
    ...
)

new_em_rows = joint[joint["model_short"] == "EM"].copy()  # BUG: discards NE + individual rows

if new_em_rows.empty:
    logger.info(...)
    return

merged = pd.concat([combined, new_em_rows], ignore_index=True)
merged = merged.drop_duplicates(subset=["date", "code", "model_short"], keep="last")  # BUG: missing period_col
```

**`postprocessing_maintenance.py:228-246`** — gap_codes computed after read:

```python
modelled, _ = data_reader.read_individual_model_forecasts_for_dates(
    config.name, affected_dates          # BUG: codes not passed
)
...
gap_codes: set[str] = set()             # computed after read
for df_check in [em_gaps, stale, stale_em]:
    if not df_check.empty and "code" in df_check.columns:
        gap_codes.update(df_check["code"].unique())
```

**Key files:**

- `apps/postprocessing_forecasts/postprocessing_maintenance.py:137–308` —
  `_fill_gaps_for_horizon`
- `apps/postprocessing_forecasts/tests/test_maintenance_workflow.py` — tests for
  the maintenance entry point

### Root Cause

PP-021 implementation correctly structured the detection phase but the save phase
(`Step 8`) was written as if it were EM-only gap-fill (the old pre-PP-021 logic).
The stale refresh path (the main new feature of PP-021) was wired up through
detection and data-read but not through save.

---

## Implementation Plan

### Approach

Targeted fixes to `_fill_gaps_for_horizon` only. No changes to `data_reader.py`,
`gap_detector.py`, or `ensemble_calculator.py` — those are correct. Four edits to
`postprocessing_maintenance.py` plus updated/new tests.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | Bugs A, B, C, D fixes + comment update (Issue F) |
| `apps/postprocessing_forecasts/tests/test_maintenance_workflow.py` | New tests for refreshed non-EM rows; update mock to include NE in `joint` |

### Implementation Steps

- [ ] **Step 1: Compute `gap_codes` before the data read (Bug D)**

  Move the `gap_codes` assembly block (currently at lines 239–242) to immediately
  after the `all_affected` computation (before line 220 `affected_dates = sorted(...)`).
  Then pass `codes=list(gap_codes)` to `read_individual_model_forecasts_for_dates`.

  ```python
  # Compute gap codes BEFORE data read so we can scope the API query
  gap_codes: set[str] = set()
  for df_check in [em_gaps, stale, stale_em]:
      if not df_check.empty and "code" in df_check.columns:
          gap_codes.update(df_check["code"].unique())

  affected_dates = sorted(all_affected)

  # Step 6: read modelled data scoped to affected dates AND codes
  modelled, _ = data_reader.read_individual_model_forecasts_for_dates(
      config.name, affected_dates, codes=list(gap_codes) if gap_codes else None
  )
  modelled = sl.calculate_virtual_stations_data(modelled)
  modelled = config.neural_ensemble_func(modelled)
  ```

  Remove the now-redundant `gap_codes` block after the data read and simplify the
  `modelled_filtered` line (codes filter already applied by the reader):

  ```python
  modelled_filtered = modelled[modelled["date"].isin(all_affected)].copy()
  ```

- [ ] **Step 2: Save all refreshed rows, not just EM (Bugs A + B)**

  Replace lines 269–284 with:

  ```python
  if joint.empty:
      logger.info(f"No new {label} rows created from gap-fill data. Nothing to save.")
      return

  # Step 8: merge ALL refreshed rows (individual models + NE + EM) into combined
  # concat puts combined first, joint last → keep="last" replaces stale entries
  merged = pd.concat([combined, joint], ignore_index=True)
  merged = merged.drop_duplicates(
      subset=["date", "code", config.period_col, "model_short"], keep="last"
  )

  n_em = (joint["model_short"] == "EM").sum()
  n_ne = (joint["model_short"] == "NE").sum()
  n_individual = len(joint) - n_em - n_ne

  logger.info(
      "Merged %d refreshed rows (%d EM, %d NE, %d individual) into %d existing → %d total",
      len(joint),
      n_em,
      n_ne,
      n_individual,
      len(combined),
      len(merged),
  )
  ```

- [ ] **Step 3: Update NE gap warning comment (Issue F)**

  Replace the NE gap warning block (lines 172–180) comment text:

  ```python
  ne_gaps = all_gaps[all_gaps["model_short"] == "NE"]
  if not ne_gaps.empty:
      logger.warning(
          "Found %d NE gaps within lookback window. "
          "NE rows will be re-created if individual-model data exists for those dates; "
          "if modelled data is absent, NE gaps cannot be filled.",
          len(ne_gaps),
      )
  em_gaps = all_gaps[all_gaps["model_short"] == "EM"].reset_index(drop=True)
  ```

  Note: `ne_gaps` dates are NOT added to `all_affected` — they are only reported.
  NE rows for `stale` dates ARE created (via `config.neural_ensemble_func`) and
  saved because `stale` includes stale NE records. This is intentional and correct.

- [ ] **Step 4: Update audit log to include NE and individual-model counts**

  Replace the AUDIT log at lines 297–307 with:

  ```python
  n_new_em = (joint["model_short"] == "EM").sum() if not joint.empty else 0
  n_new_ne = (joint["model_short"] == "NE").sum() if not joint.empty else 0
  n_new_individual = len(joint) - n_new_em - n_new_ne if not joint.empty else 0

  logger.info(
      "AUDIT: %s — filled %d EM gaps, refreshed %d NE, %d individual rows; "
      "%d stale detected (%d NE/individual, %d EM); lookback=%d months",
      label,
      len(em_gaps),
      n_new_ne,
      n_new_individual,
      len(stale) + len(stale_em),
      len(stale),
      len(stale_em),
      max_lookback_months,
  )
  for _, gap_row in em_gaps.iterrows():
      logger.info("  Filled EM: date=%s, code=%s", gap_row["date"], gap_row["code"])
  ```

- [ ] **Step 5: Update `_setup_mocks` in `test_maintenance_workflow.py`**

  The mock `ensemble_result` already contains NE + individual rows (LR) alongside
  EM. Verify this is true and extend it explicitly:

  ```python
  ensemble_result = pd.concat(
      [
          # individual model row (already present)
          mock_data,  # model_short="LR"
          # NE row
          pd.DataFrame({
              "code": ["10001"],
              "date": pd.to_datetime(["2024-01-05"]),
              "forecasted_discharge": [105.0],
              "model_short": ["NE"],
              "pentad_in_year": [1],
              "q05": [90.0], "q50": [105.0], "q95": [120.0],
          }),
          # EM row
          pd.DataFrame({
              "code": ["10001"],
              "date": pd.to_datetime(["2024-01-05"]),
              "forecasted_discharge": [100.0],
              "model_short": ["EM"],
              "pentad_in_year": [1],
              "q05": [85.0], "q50": [100.0], "q95": [115.0],
          }),
      ],
      ignore_index=True,
  )
  ```

- [ ] **Step 6: Add new tests for the fixed save path**

  Add to `TestMaintenanceWorkflow`:

  ```python
  def test_stale_individual_rows_are_saved(self, mock_data, mock_skill):
      """Stale individual-model rows (q05=NULL) are replaced in the save."""
      combined = pd.DataFrame({
          "date": pd.to_datetime(["2024-01-05"] * 3),
          "code": ["10001"] * 3,
          "model_short": ["LR", "TFT", "EM"],
          "forecasted_discharge": [100.0, 110.0, 105.0],
          "q05": [None, None, None],  # all stale
          "pentad_in_year": [1, 1, 1],
      })
      stale_rows = pd.DataFrame({
          "date": pd.to_datetime(["2024-01-05", "2024-01-05"]),
          "code": ["10001", "10001"],
          "model_short": ["LR", "TFT"],
      })
      # ...set up mocks with stale_rows returned by detect_stale_quantiles
      # assert file_writer.save_forecast_data was called
      # assert the saved data contains LR+TFT rows with q05 not null (from joint)

  def test_stale_ne_rows_are_saved(self, mock_data, mock_skill):
      """Stale NE rows (q05=NULL) are replaced when modelled data exists."""
      # NE in stale → triggers data read → NE rows in joint → saved

  def test_save_uses_all_joint_rows_not_just_em(self, mock_data, mock_skill):
      """file_writer receives joint (EM+NE+individual), not just EM rows."""
      # assert save_forecast_data called with df containing NE and individual model rows

  def test_gap_codes_passed_to_data_reader(self, mock_data, mock_skill):
      """read_individual_model_forecasts_for_dates is called with gap_codes."""
      # assert call_args for read_individual_model_forecasts_for_dates
      # contains codes kwarg matching the codes from em_gaps
  ```

- [ ] **Step 7: Run tests**

  ```bash
  cd apps
  SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
  ```

  All tests must pass with zero skips.

---

## Testing

### Test Cases

- [ ] Stale LR/TFT/TiDE/TSMixer rows (`q05=NULL`) → replaced with fresh rows from
  `joint` after maintenance run
- [ ] Stale NE rows (`q05=NULL`) → replaced with fresh NE rows from `joint`
- [ ] Stale EM rows → replaced with new EM from `create_ensemble_forecasts`
- [ ] No gaps + no stale → early exit, `save_forecast_data` not called (existing)
- [ ] `read_individual_model_forecasts_for_dates` receives `codes` matching
  gap/stale stations
- [ ] `drop_duplicates` dedup key includes `config.period_col`
- [ ] Existing `test_no_gaps_skips_processing` and `test_gaps_trigger_ensemble_creation`
  still pass

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

After deployment, verify stale counts decrease:

```sql
-- Before:
SELECT model_type, COUNT(*) FROM forecasts
WHERE horizon_type = 'pentad' AND q05 IS NULL
GROUP BY model_type;

-- Run maintenance, then re-check the above query.
-- Counts should decrease for all model_types within lookback window.
```

---

## Documentation Impact

- [ ] `apps/postprocessing_forecasts/README.md` — update PP-021 status note; add
  PP-022 description; update maintenance pipeline description to say all refreshed
  rows (individual + NE + EM) are saved
- [ ] `doc/data_flow_short_term.md` — update maintenance pipeline diagram: the save
  step now writes individual models + NE + EM, not just EM
- [ ] `doc/plans/issues/gi_draft_pp_maintenance_pipeline_efficiency.md` — note that
  PP-022 fixes the stale refresh save path that PP-021 left incomplete
- [ ] No changes to `CLAUDE.md`, user guide, or deployment docs (internal refactor)

---

## Out of Scope

- Issue E (year-scoped vs date-scoped reader in `read_individual_model_forecasts_for_dates`) —
  tracked separately; does not affect correctness, only efficiency
- Issue G (operational vs maintenance data scope asymmetry) — intentional, no fix
  needed, docstring update only
- PP-015 (moving NE creation out of `setup_library`) — separate issue
- Bulk backfill of all existing stale records — handled by
  `apps/machine_learning/reaggregate_day_to_periods.py`

## Dependencies

- PP-019 (quantile propagation) — complete; required for refreshed rows to have
  correct quantiles
- PP-021 (maintenance restructure) — complete; this issue fixes the incomplete
  save path left by PP-021

## Acceptance Criteria

- [ ] After a maintenance run, stale individual-model rows (`q05=NULL`, within
  lookback window) have `q05` populated in the combined CSV and API
- [ ] After a maintenance run, stale NE rows have `q05` populated
- [ ] After a maintenance run, stale EM rows have `q05` populated
- [ ] `read_individual_model_forecasts_for_dates` receives `codes=list(gap_codes)`
  (verified by test)
- [ ] `drop_duplicates` uses `["date", "code", config.period_col, "model_short"]`
- [ ] No regression: no-gap early exit still skips all data reads
- [ ] All existing tests pass; 4 new tests added
- [ ] NE gap warning comment accurately describes behaviour

---

## References

- PP-021 issue file: `doc/plans/issues/gi_draft_pp_maintenance_pipeline_efficiency.md`
- PP-019 (quantile propagation): `doc/plans/issues/gi_draft_pp_short_term_ensemble_quantiles.md`
- Related scripts: `apps/machine_learning/reaggregate_day_to_periods.py` (bulk
  backfill for existing stale records)
