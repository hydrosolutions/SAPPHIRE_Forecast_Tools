# PP-022: Fix stale-record refresh and minor inconsistencies in maintenance pipeline

**Status**: Complete (verified 2026-03-24)
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

### Bug F — NE gap dates excluded from `all_affected`; NE gaps not filled (MEDIUM)

`_fill_gaps_for_horizon` detects NE gaps but does NOT add their dates to
`all_affected` (line 204 only unions `em_gap_dates | stale_dates | stale_em_dates`).
This means: if NE is missing for a date (gap, not stale), maintenance won't read
data for that date and won't create the NE row — even though NE creation only
requires individual-model data (no skill metrics needed).

The warning at lines 174–179 says NE gaps are "not fillable by maintenance", which
is incorrect: `config.neural_ensemble_func(modelled)` creates NE rows from
individual-model data without any skill metrics.

**Fix**: Add `ne_gap_dates` to `all_affected`. Update the warning to an info log.
NE rows will be created by `neural_ensemble_func` and saved via the Bug A fix.

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
- Missing NE rows (gaps) are also filled — NE creation only needs individual-model
  data, not skill metrics.
- Stale individual-model and NE rows are refreshed even when skill metrics are
  unavailable (skill metrics are only needed for EM creation).
- The `drop_duplicates` key is consistent with the operational pipeline's natural
  key.
- `gap_codes` is computed before the data read and passed to the API query.
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

- [x] **Step 1: Add NE gap dates to `all_affected` and compute `gap_codes` before
  the data read (Bugs D + F)**

  NE creation only needs individual-model data (no skill metrics), so NE gaps
  should be treated the same as EM gaps for the purpose of scoping the data read.

  After the `all_affected` computation (line 204), add `ne_gap_dates`:

  ```python
  ne_gap_dates = (
      set(pd.to_datetime(ne_gaps["date"]).unique()) if not ne_gaps.empty else set()
  )
  all_affected = em_gap_dates | stale_dates | stale_em_dates | ne_gap_dates
  ```

  Then move the `gap_codes` assembly block (currently at lines 239–242) to
  immediately after `all_affected`, and include `ne_gaps` in the loop. Pass
  `codes=list(gap_codes)` to `read_individual_model_forecasts_for_dates`.

  ```python
  # Compute gap codes BEFORE data read so we can scope the API query
  gap_codes: set[str] = set()
  for df_check in [em_gaps, ne_gaps, stale, stale_em]:
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

- [x] **Step 2: Decouple stale refresh from EM creation (new behaviour)**

  Currently, missing skill metrics causes an early return at line 255–257, which
  prevents refreshing stale individual/NE rows even though those don't need skill
  metrics. Restructure to: always build `joint` from `modelled_filtered` (which
  already has NE via `neural_ensemble_func`), then optionally add EM if skill
  metrics are available.

  Replace lines 252–273 with:

  ```python
  with timer(timing_stats, f"reading {label} skill metrics"):
      skill_stats = data_reader.read_skill_metrics(config.name)

  # Start with modelled_filtered as the base (individual + NE rows with quantiles)
  joint = modelled_filtered.copy()

  # Add EM rows only if skill metrics are available
  if skill_stats.empty:
      logger.warning(
          f"No {label} skill metrics available. "
          "Refreshing individual/NE rows but skipping EM creation."
      )
  else:
      with timer(timing_stats, f"creating {label} gap-fill ensembles"):
          joint, _ = ensemble_calculator.create_ensemble_forecasts(
              forecasts=modelled_filtered,
              skill_stats=skill_stats,
              period_col=config.period_col,
              period_in_month_col=config.period_in_month_col,
              get_period_in_month_func=config.get_period_func,
          )
  ```

- [ ] **Step 2b: Save all refreshed rows, not just EM (Bugs A + B + C)**

  Replace the EM-only filter and merge (old lines 269–284) with:

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

- [x] **Step 3: Update NE gap handling — include in affected dates, fix log (Bug F)**

  Replace the NE gap warning block (lines 172–180). NE gaps are now included in
  `all_affected` (Step 1), so downgrade the warning to info:

  ```python
  ne_gaps = all_gaps[all_gaps["model_short"] == "NE"]
  if not ne_gaps.empty:
      logger.info(
          "Found %d NE gaps within lookback window. "
          "NE rows will be re-created from individual-model data.",
          len(ne_gaps),
      )
  em_gaps = all_gaps[all_gaps["model_short"] == "EM"].reset_index(drop=True)
  ```

  Update the summary log (lines 206–214) to include NE gap count:

  ```python
  logger.info(
      "%s: %d EM gaps, %d NE gaps, %d stale individual/NE records, "
      "%d stale EM records → %d total affected dates",
      label,
      len(em_gaps),
      len(ne_gaps),
      len(stale),
      len(stale_em),
      len(all_affected),
  )
  ```

- [x] **Step 4: Update audit log to include NE and individual-model counts**

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

- [x] **Step 5: Update `_setup_mocks` in `test_maintenance_workflow.py`**

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

- [x] **Step 6: Add new tests for the fixed save path**

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

  def test_ne_gaps_are_filled(self, mock_data, mock_skill):
      """Missing NE rows (gap, not stale) trigger data read and NE creation."""
      # NE in all_gaps → ne_gap_dates added to all_affected → data read
      # → neural_ensemble_func creates NE → saved in joint

  def test_save_uses_all_joint_rows_not_just_em(self, mock_data, mock_skill):
      """file_writer receives joint (EM+NE+individual), not just EM rows."""
      # assert save_forecast_data called with df containing NE and individual model rows

  def test_stale_refresh_without_skill_metrics(self, mock_data, mock_skill):
      """Individual/NE rows are refreshed even when skill metrics are empty."""
      # skill_stats empty → EM not created, but individual + NE still saved
      # assert save_forecast_data called
      # assert saved df has no EM rows but does have LR/NE rows

  def test_gap_codes_passed_to_data_reader(self, mock_data, mock_skill):
      """read_individual_model_forecasts_for_dates is called with gap_codes."""
      # assert call_args for read_individual_model_forecasts_for_dates
      # contains codes kwarg matching the codes from em_gaps
  ```

- [x] **Step 7: Run tests**

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
- [ ] Missing NE rows (gaps) → created by `neural_ensemble_func` and saved
- [ ] No skill metrics → individual/NE rows still refreshed, EM skipped
- [ ] No gaps + no stale → early exit, `save_forecast_data` not called (existing)
- [ ] `read_individual_model_forecasts_for_dates` receives `codes` matching
  gap/stale stations (including NE gap codes)
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
- [ ] Missing NE rows (gaps) are created and saved
- [ ] Individual/NE rows are refreshed even when skill metrics are unavailable
  (EM is skipped, but stale refresh still happens)
- [ ] `read_individual_model_forecasts_for_dates` receives `codes=list(gap_codes)`
  (verified by test)
- [ ] `drop_duplicates` uses `["date", "code", config.period_col, "model_short"]`
- [ ] No regression: no-gap early exit still skips all data reads
- [ ] All existing tests pass; 6 new tests added

---

## References

- PP-021 issue file: `doc/plans/issues/gi_draft_pp_maintenance_pipeline_efficiency.md`
- PP-019 (quantile propagation): `doc/plans/issues/gi_draft_pp_short_term_ensemble_quantiles.md`
- Related scripts: `apps/machine_learning/reaggregate_day_to_periods.py` (bulk
  backfill for existing stale records)
