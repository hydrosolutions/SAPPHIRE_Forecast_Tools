# PP-019: Propagate Quantiles Through Short-Term Ensemble Creation

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `forecast-quality`, `uncertainty`, `postprocessing`

---

## Summary

Short-term ML model forecasts (TFT, TiDE, TSMixer) already produce
quantiles (q05, q25, q75, q95) from the API, but these are dropped
during ensemble creation. Propagate quantiles through EM ensemble
averaging and include them in CSV and API outputs so the dashboard
displays calibrated uncertainty bands instead of falling back to ±delta.

## Context

The forecast pipeline has two uncertainty mechanisms for short-term
forecasts:

1. **LR models**: Use ±delta (0.674 * sigma of observed flow). This is
   the Soviet/Central Asian hydromet convention and remains the correct
   approach for linear regression.
2. **ML models** (TFT, TiDE, TSMixer): Produce quantile forecasts
   (q05, q25, q75, q95) natively. These are stored in the postprocessing
   API and read into the pipeline via `data_reader._normalize_ml_forecasts()`
   (line 1617-1689).

The **forecast dashboard** (`processing.py:1186-1211`) already handles
both: it displays Q25/Q75 when available, falling back to ±delta when
they're absent. So propagating quantiles requires **no dashboard changes**.

The **problem**: `create_ensemble_forecasts()` (`ensemble_calculator.py:140-149`)
only averages `forecasted_discharge`. The quantile columns present in ML
forecast rows are not included in the aggregation or the outer join. EM
ensemble rows are created without quantile information, and the individual
model quantiles are also lost from the joint output.

### Interaction with active work

PP-007/PP-009/PP-013 are in Review status. This change modifies
`create_ensemble_forecasts()` which is also involved in PP-009, but the
change is purely **additive** — we add columns to the aggregation dict
and join columns. No existing behavior changes.

## Problem

1. `create_ensemble_forecasts()` drops q05/q25/q75/q95 during groupby
   aggregation — only `forecasted_discharge` and `model_short` are
   aggregated
2. The outer join `join_cols` (line 175-183) doesn't include quantile
   columns, so they are lost from the ensemble rows
3. `_write_combined_forecast_to_api()` (`api_writer.py:102`) does not
   include quantile columns in the records because they are not in the
   DataFrame by the time it reaches the writer (dropped upstream)
4. Result: the dashboard falls back to ±delta for all short-term
   forecasts, including ML models that have proper quantiles

## Desired Outcome

- Individual ML model forecast rows retain their q05-q95 columns in the
  combined output
- EM ensemble rows have averaged q05-q95 (vincentization — same pattern
  as monthly ensembles in `ensemble_calculator.py:276-278`)
- LR forecast rows continue to have NaN quantiles (dashboard uses ±delta)
- CSV output includes quantile columns
- API writer sends quantile columns for short-term forecasts
- Dashboard automatically displays Q25/Q75 bands for ML models and EM

---

## Technical Analysis

### Current Implementation

**ML quantile flow** (working correctly until ensemble creation):
```
API → data_reader._normalize_ml_forecasts() → forecasts DataFrame
  columns: code, date, model_short, forecasted_discharge, q05, q25, q75, q95, ...
```

**Ensemble creation** (drops quantiles):
```python
# ensemble_calculator.py:140-149
ensemble_avg = qualifying.groupby([period_col, "date", "code"]).agg({
    "forecasted_discharge": "mean",  # ← only these two
    "model_short": composition_agg,
})
```

**Monthly ensemble** (already handles quantiles — reference pattern):
```python
# ensemble_calculator.py:276-278 (create_monthly_ensemble_forecasts)
em_agg = {
    "month_in_year": "first",
    "forecasted_discharge": "mean",
    "model_short": composition_agg,
}
for qcol in _QUANTILE_COLS:           # ← this is what short-term needs
    if qcol in qualifying.columns:
        em_agg[qcol] = "mean"
```

**Key files:**
- `src/ensemble_calculator.py:71-191` — `create_ensemble_forecasts()` — needs quantile propagation
- `src/ensemble_calculator.py:194-309` — `create_monthly_ensemble_forecasts()` — reference pattern
- `src/skill_metrics.py:1008` — `_QUANTILE_COLS` definition
- `src/file_writer.py:152-242` — `save_forecast_data()` — CSV already passes through all columns
- `src/api_writer.py:102` — `_write_combined_forecast_to_api()` — needs quantile fields

### Approach

Follow the existing monthly ensemble pattern: add quantile columns to the
aggregation dict and join columns in `create_ensemble_forecasts()`. This
is a minimal, targeted change.

### Files to Modify

| File | Changes |
|------|---------|
| `src/ensemble_calculator.py` | Add quantile columns to `create_ensemble_forecasts()` agg dict and join_cols |
| `src/api_writer.py` | Add q05-q95 fields to `_write_combined_forecast_to_api()` records |

### Implementation Steps

- [ ] **Step 1: Add quantile averaging to `create_ensemble_forecasts()`**
  In the aggregation at line 141-150, add q05/q25/q75/q95 with "mean"
  aggregation when present (same pattern as monthly at line 276-278).
  Use `_QUANTILE_COLS` from skill_metrics or define a short-term subset.

  **Design note — quantile column mismatch**: Short-term ML models produce
  4 quantiles (q05, q25, q75, q95) per `data_reader.py:1652-1658`.
  `_QUANTILE_COLS` defines 7 (q05, q10, q25, q50, q75, q90, q95). The
  `if qcol in qualifying.columns` guard handles this correctly — missing
  columns are simply skipped. This is intentional: using the full
  `_QUANTILE_COLS` list ensures forward compatibility if ML models later
  produce additional quantile levels.

  **Design note — NaN averaging**: When LR has NaN quantiles and ML models
  have values, `groupby.agg("mean")` computes the mean of non-NaN values
  only (pandas default `skipna=True`). This is intentional: EM quantiles
  reflect only models that actually produce quantiles.

  ```python
  from src.skill_metrics import _QUANTILE_COLS

  agg_dict = {
      "forecasted_discharge": "mean",
      "model_short": composition_agg,
  }
  for qcol in _QUANTILE_COLS:
      if qcol in qualifying.columns:
          agg_dict[qcol] = "mean"

  ensemble_avg = qualifying.groupby([period_col, "date", "code"]).agg(
      agg_dict
  ).reset_index()
  ```

- [ ] **Step 2: Include quantile columns in join_cols**
  At line 175-184, add available quantile columns to `join_cols` so
  they are preserved in the outer merge:

  ```python
  join_cols = [
      "code", "date", period_in_month_col, period_col,
      "forecasted_discharge", "model_short", "composition",
  ]
  # Add quantile columns that exist in ensemble_avg
  for qcol in _QUANTILE_COLS:
      if qcol in ensemble_avg.columns:
          join_cols.append(qcol)
  ```

- [ ] **Step 3: Update API writer for short-term quantiles**
  In `_write_combined_forecast_to_api()` (`api_writer.py:102`), add
  quantile fields to the records when present in the DataFrame. The API combined_forecasts
  table already has q05/q25/q50/q75/q95 columns (used by ML writes from
  the ML module). Add to the records_df construction:

  ```python
  for qcol in ("q05", "q10", "q25", "q50", "q75", "q90", "q95"):
      if qcol in df_rec.columns:
          records_df[qcol] = df_rec[qcol].where(df_rec[qcol].notna())
  ```

- [ ] **Step 4: Write tests**
  - Existing `test_ensemble_calculator.py` tests should still pass
    (backward compatible — quantile columns are optional)
  - New: test that ensemble rows have averaged quantiles when input
    has quantile columns
  - New: test that LR-only input (no quantiles) still works
  - New: test that mixed input (LR without quantiles + TFT with
    quantiles) produces correct ensemble quantiles from qualifying models

### Code Examples

```python
# Test: ensemble propagates quantiles
def test_create_ensemble_forecasts_propagates_quantiles():
    """EM ensemble should average quantile columns from ML models."""
    forecasts = pd.DataFrame({
        "pentad_in_year": [1, 1, 1, 1],
        "pentad_in_month": [1, 1, 1, 1],
        "date": pd.Timestamp("2025-01-05"),
        "code": ["A", "A", "A", "A"],
        "model_short": ["LR", "TFT", "TiDE", "LR"],
        "forecasted_discharge": [100.0, 110.0, 90.0, 100.0],
        "q25": [np.nan, 95.0, 75.0, np.nan],  # LR has no quantiles
        "q75": [np.nan, 125.0, 105.0, np.nan],
    })
    skill_stats = pd.DataFrame({
        "pentad_in_year": [1, 1, 1],
        "code": ["A", "A", "A"],
        "model_short": ["LR", "TFT", "TiDE"],
        "sdivsigma": [0.3, 0.4, 0.5],
        "nse": [0.9, 0.85, 0.82],
        "accuracy": [0.9, 0.85, 0.85],
        "mae": [5.0, 8.0, 10.0],
        "n_pairs": [20, 20, 20],
        "delta": [15.0, 15.0, 15.0],
    })

    joint, _ = create_ensemble_forecasts(
        forecasts, skill_stats,
        period_col="pentad_in_year",
        period_in_month_col="pentad_in_month",
        get_period_in_month_func=lambda d: 1,
    )

    em_rows = joint[joint["model_short"] == "EM"]
    assert not em_rows.empty
    # EM should have averaged quantiles from qualifying ML models
    # (LR quantiles are NaN so only TFT and TiDE contribute)
    assert "q25" in em_rows.columns
    assert "q75" in em_rows.columns
```

---

## Testing

### Test Cases

- [ ] `test_ensemble_propagates_quantiles` — EM rows have mean of input
  quantiles
- [ ] `test_ensemble_no_quantiles` — input without quantile columns still
  works (backward compatible)
- [ ] `test_ensemble_mixed_quantiles` — LR (no quantiles) + ML (with
  quantiles) — EM uses only qualifying models' quantiles
- [ ] `test_api_writer_includes_quantiles` — API records include q05-q95
  when present in DataFrame
- [ ] `test_joint_output_preserves_individual_quantiles` — ML model rows
  in joint output retain their original quantiles

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

1. Run postprocessing operational on a pentad boundary day
2. Check combined forecast CSV: ML model rows should have q05-q95;
   LR rows should have NaN; EM rows should have averaged quantiles
3. Open dashboard: ML models should show Q25/Q75 bands; LR should
   show ±delta bands

---

## Documentation Impact

- [ ] Module README (`apps/postprocessing_forecasts/README.md`) — note
  that short-term ensembles now include quantile propagation
- [ ] No other documentation changes needed — this is fixing an omission,
  not adding new behavior

---

## Out of Scope

- Adding quantiles to LR forecasts (LR uses ±delta by design)
- Residual-based prediction intervals for LR (possible future work)
- Dashboard changes (dashboard already handles quantiles)
- Changes to ML model quantile production (upstream concern)

## Dependencies

- **API combined_forecasts table**: Must already have q05-q95 columns.
  Verify this is the case — the ML module already writes quantiles via
  its own API path, so the schema likely supports it.
- No dependency on PP-007/PP-009/PP-012/PP-013.

## Acceptance Criteria

- [ ] EM ensemble rows in short-term output include averaged q05-q95
- [ ] Individual ML model rows retain their quantiles in joint output
- [ ] LR model rows have NaN quantiles (unchanged behavior)
- [ ] API writer sends quantile columns for short-term forecasts
- [ ] Dashboard displays Q25/Q75 bands for ML and EM forecasts
- [ ] All existing tests pass
- [ ] New tests cover quantile propagation and edge cases

---

## References

- Monthly vincentization pattern:
  `src/ensemble_calculator.py:276-278`
- Monthly vincentization in recalculation path:
  `src/skill_metrics.py:1171-1173` (also has quantile averaging at
  `_add_naive_mean()` line 1294, `_add_skilled_mean()` line 1465)
- ML forecast normalization (quantile source):
  `src/data_reader.py:1617-1689`
- Dashboard fallback logic:
  `apps/forecast_dashboard/src/processing.py:1186-1211`
