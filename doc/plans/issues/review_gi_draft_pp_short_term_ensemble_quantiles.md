# PP-019: Propagate Quantiles Through Short-Term Ensemble Creation

**Status**: Implemented (all steps verified 2026-03-13)
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
   API `Forecast` table and read into the pipeline via
   `data_reader._normalize_ml_forecasts()` (lines 1622-1714).

**API schema note**: The short-term `Forecast` table has **4 quantile
columns** (q05, q25, q75, q95). The long-term `LongForecast` table has
all 7 (q05-q95). `_QUANTILE_COLS` in `skill_metrics.py` defines all 7,
but the `if qcol in qualifying.columns` guard means only the 4 present
in short-term data are processed. The API writer must also send only
the 4 columns the schema accepts.

The **forecast dashboard** currently only uses `delta` and manual
percentage ranges for uncertainty bands (`processing.py:1186-1211`).
It does **not** yet read or display quantile columns. Dashboard changes
are **out of scope** for this issue — a separate dashboard issue will
add Q25/Q75 band display once quantiles flow through the pipeline.

The **problem**: `create_ensemble_forecasts()` (`ensemble_calculator.py:141-149`)
only averages `forecasted_discharge`. The quantile columns present in ML
forecast rows are not included in the aggregation or the outer join. EM
ensemble rows are created without quantile information, and the individual
model quantiles are also lost from the joint output.

### Interaction with active work

PP-007/PP-009/PP-013 are in Review status (as of 2026-03-05). This
change modifies `create_ensemble_forecasts()` which is also involved in
PP-009, but the change is purely **additive** — we add columns to the
aggregation dict and replace the merge with `pd.concat`. No existing
behavior changes.

**Line number caution**: Line numbers in this plan (e.g., "lines 141-149",
"lines 175-183") were verified against the codebase as of 2026-03-05. If
PP-009 or other PRs merge first and touch `create_ensemble_forecasts()`,
line numbers will shift. Verify against current code at implementation time.

## Problem

1. `create_ensemble_forecasts()` drops q05/q25/q75/q95 during groupby
   aggregation — only `forecasted_discharge` and `model_short` are
   aggregated
2. The outer merge (lines 175-189) doesn't carry quantile columns through,
   so they are lost from the ensemble rows
3. `_write_combined_forecast_to_api()` (`api_writer.py:102-312`)
   explicitly constructs `records_df` without quantile columns — only
   `forecasted_discharge` is sent to the API
4. Result: quantiles are lost in the pipeline; the dashboard has no
   quantile data to display even if it were updated to use them

## Desired Outcome

- Individual ML model forecast rows retain their q05/q25/q75/q95
  columns in the combined output
- EM ensemble rows have averaged q05/q25/q75/q95 (vincentization — same
  pattern as monthly ensembles in `ensemble_calculator.py:276-278`)
- LR forecast rows continue to have NaN quantiles (unchanged)
- CSV output includes quantile columns
- API writer sends quantile columns (q05, q25, q75, q95) for short-term
  forecasts — matching the `Forecast` table schema

---

## Technical Analysis

### Current Implementation

**ML quantile flow** (working correctly until ensemble creation):
```
API → data_reader._normalize_ml_forecasts() (lines 1622-1714)
  → read_individual_model_forecasts() (lines 1812-1895)
  → forecasts DataFrame
  columns: code, date, model_short, forecasted_discharge, q05, q25, q75, q95, ...
```

ML quantiles are aggregated from daily to pentad/decad via mean
(lines 1653-1663). LR rows have NaN for quantile columns after
`pd.concat` with ML rows.

**Ensemble creation** (drops quantiles):
```python
# ensemble_calculator.py:141-149
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
- `src/skill_metrics.py:1008` — `_QUANTILE_COLS` (7 cols; short-term data has 4)
- `src/file_writer.py:152-242` — `save_forecast_data()` — CSV already passes through all columns
- `src/api_writer.py:102-312` — `_write_combined_forecast_to_api()` — needs quantile fields

### Approach

Follow the existing monthly ensemble pattern: add quantile columns to the
aggregation dict in `create_ensemble_forecasts()`, then use `pd.concat`
(not the existing outer merge) to combine ensemble rows with individual
forecasts. This is a minimal, targeted change.

**Why `pd.concat` instead of the outer merge**: The current code merges
ensemble rows into forecasts using `pd.merge(..., on=join_cols, how="outer")`.
Adding quantile columns to `join_cols` would be semantically wrong — they
are payload, not join keys. If an individual model row happened to have a
quantile value identical to an EM averaged quantile (possible with rounding),
the merge would incorrectly collapse those rows. The monthly path avoids
this by using `_append_to_joint()` (a `pd.concat`). We adopt the same
approach here: replace the outer merge with `pd.concat`, which is simpler,
safer, and consistent with the monthly pattern.

### Files to Modify

| File | Changes |
|------|---------|
| `src/ensemble_calculator.py` | Add quantile columns to `create_ensemble_forecasts()` agg dict; replace outer merge with `pd.concat` |
| `src/api_writer.py` | Add q05-q95 fields to `_write_combined_forecast_to_api()` records |

### Implementation Steps

- [ ] **Step 1: Add quantile averaging to `create_ensemble_forecasts()`**
  In the aggregation at line 141-150, add q05/q25/q75/q95 with "mean"
  aggregation when present (same pattern as monthly at line 276-278).
  Use `_QUANTILE_COLS` from skill_metrics or define a short-term subset.

  **Import note**: `_QUANTILE_COLS` is not currently imported in
  `create_ensemble_forecasts()`. The monthly function imports it as a
  **local import** inside the function body (line 223:
  `from src.skill_metrics import _QUANTILE_COLS`). Follow the same
  pattern — add the import inside `create_ensemble_forecasts()`, not at
  module level. This avoids circular imports and matches existing style.

  **Design note — `_QUANTILE_COLS` is a superset**: Short-term ML models
  produce 4 quantiles (q05, q25, q75, q95) per `data_reader.py:1653-1663`.
  `_QUANTILE_COLS` defines 7 (q05, q10, q25, q50, q75, q90, q95) and its
  docstring says "for long-term forecasts", but reusing it here is
  intentional: the `if qcol in qualifying.columns` guard means only the
  4 columns present in short-term data are processed. Using the full list
  ensures forward compatibility if ML models later produce additional
  quantile levels. The same pattern is used by `create_quarterly_ensemble_forecasts()`
  and `create_seasonal_ensemble_forecasts()` via `_create_aggregated_ensemble_forecasts()`.

  **Design note — NaN averaging**: When LR has NaN quantiles and ML models
  have values, `groupby.agg("mean")` computes the mean of non-NaN values
  only (pandas default `skipna=True`). This is intentional: EM quantiles
  reflect only models that actually produce quantiles.

  **Design note — model mix asymmetry**: This means the EM point forecast
  (mean of all qualifying models including LR) and the EM quantiles (mean
  of only ML models that produce quantiles) may come from different model
  subsets. For example, if LR + TFT + TiDE qualify, the point forecast is
  the mean of 3 models but the quantiles are the mean of 2. This is
  scientifically acceptable — the alternative (dropping LR from the point
  forecast to match) would degrade the ensemble. A test should verify and
  document this asymmetry explicitly.

  ```python
  # Local import (same pattern as create_monthly_ensemble_forecasts, line 223)
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

- [ ] **Step 2: Replace outer merge with `pd.concat`**
  Replace the outer merge (lines 170-189) with `pd.concat`, matching
  the monthly pattern (`_append_to_joint()`). This is simpler and avoids
  the risk of accidental row collapse from putting payload columns in
  merge keys.

  The existing outer merge uses `join_cols` as both merge keys and
  payload carriers — this works by accident (values rarely collide) but
  is semantically fragile. `pd.concat` is a clean union: each side
  keeps its own rows, pandas aligns columns by name, and missing columns
  become NaN automatically.

  ```python
  # Ensure forecasts has composition column for concat alignment
  if "composition" not in forecasts.columns:
      forecasts = forecasts.copy()
      forecasts["composition"] = ""

  joint_forecasts = pd.concat(
      [forecasts, ensemble_avg],
      ignore_index=True,
  )
  ```

  **Why this is safe**: `ensemble_avg` already has all the columns it
  needs (period_col, period_in_month_col, date, code, model_short,
  composition, forecasted_discharge, plus any quantile columns from
  Step 1). `pd.concat` aligns by column name. Columns present in only
  one side (e.g., quantiles missing from LR-only forecasts) become NaN
  in the other — which is the correct behavior.

  **What this replaces**: The `join_cols` list (lines 175-183) and the
  `pd.merge(..., on=join_cols, how="outer")` call (lines 184-189) are
  both removed entirely.

- [ ] **Step 3: Update API writer for short-term quantiles**
  In `_write_combined_forecast_to_api()` (`api_writer.py:102-312`), add
  quantile fields to `records_df` when present in the DataFrame. The
  short-term `Forecast` table schema has **only 4 quantile columns**
  (q05, q25, q75, q95) — not q10, q50, or q90. Add after the existing
  `records_df` construction:

  ```python
  # Short-term Forecast schema supports q05, q25, q75, q95 only
  _SHORT_TERM_QUANTILE_COLS = ("q05", "q25", "q75", "q95")
  for qcol in _SHORT_TERM_QUANTILE_COLS:
      if qcol in df_rec.columns:
          records_df[qcol] = df_rec[qcol]
  ```

  **Note on `.where()`**: The existing `forecasted_discharge` line uses
  `.where(df_rec[col].notna())` which is a no-op (it replaces NaN with
  NaN). We don't replicate that pattern here — just assign directly.
  NaN values are already converted to `None` by the dict comprehension
  at line 288 (`None if pd.isna(v) else v`), which is where the actual
  null handling happens.

  **Do NOT use `_QUANTILE_COLS` here** — that includes q10/q50/q90
  which do not exist in the `Forecast` table schema. Sending unknown
  fields would either be silently ignored or cause API errors depending
  on the Pydantic model's `extra` setting.

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
    # Numerical verification: mean of TFT + TiDE quantiles
    assert em_rows["q25"].iloc[0] == pytest.approx((95.0 + 75.0) / 2)  # 85.0
    assert em_rows["q75"].iloc[0] == pytest.approx((125.0 + 105.0) / 2)  # 115.0
```

---

## Testing

### Test Cases

- [ ] `test_ensemble_propagates_quantiles` — EM rows have mean of input
  quantiles (q05, q25, q75, q95)
- [ ] `test_ensemble_no_quantiles` — input without quantile columns still
  works (backward compatible)
- [ ] `test_ensemble_mixed_quantiles` — LR (no quantiles) + ML (with
  quantiles) — EM uses only qualifying models' quantiles
- [ ] `test_api_writer_includes_short_term_quantiles` — API records
  include q05, q25, q75, q95 when present in DataFrame
- [ ] `test_api_writer_excludes_extra_quantiles` — API records do NOT
  include q10, q50, q90 (not in Forecast schema)
- [ ] `test_joint_output_preserves_individual_quantiles` — ML model rows
  in joint output retain their original quantiles
- [ ] `test_ensemble_quantiles_numerical_verification` — hand-calculated
  mean of quantiles from qualifying models matches EM output
- [ ] `test_ensemble_quantile_model_mix_asymmetry` — when LR + ML models
  qualify, EM point forecast uses all models but EM quantiles use only
  ML models (documents the expected asymmetry)

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

1. Run postprocessing operational on a pentad boundary day
2. Check combined forecast CSV: ML model rows should have q05/q25/q75/q95;
   LR rows should have NaN; EM rows should have averaged quantiles
3. Query API: `GET /forecast/?model_type=EM&horizon_type=pentad` should
   return records with non-null q05/q25/q75/q95 values
4. (Dashboard verification deferred to separate dashboard issue)

---

## Documentation Impact

- [ ] Module README (`apps/postprocessing_forecasts/README.md`) — note
  that short-term ensembles now include quantile propagation
- [ ] Update FD-002 or create a follow-up dashboard issue to add Q25/Q75
  band display (prerequisite: this issue must be complete so quantile
  data is available in the API)

---

## Out of Scope

- Adding quantiles to LR forecasts (LR uses ±delta by design)
- Residual-based prediction intervals for LR (possible future work)
- Dashboard display of quantile bands (separate issue — the dashboard
  currently only uses delta/percentage ranges; adding Q25/Q75 display
  requires dashboard changes tracked separately, likely under FD-002)
- Changes to ML model quantile production (upstream concern)

## Dependencies

- **API `Forecast` table schema**: Must have q05, q25, q75, q95 columns.
  **Verified** (2026-03-05): `models.py` lines 82-86 confirm these 4
  columns exist. q10, q50, q90 are NOT in the short-term schema.
- No dependency on PP-007/PP-009/PP-012/PP-013.

## Acceptance Criteria

- [ ] EM ensemble rows in short-term output include averaged q05/q25/q75/q95
- [ ] Individual ML model rows retain their quantiles in joint output
- [ ] LR model rows have NaN quantiles (unchanged behavior)
- [ ] API writer sends q05, q25, q75, q95 for short-term forecasts
  (only the 4 columns in the `Forecast` schema)
- [ ] CSV output includes quantile columns
- [ ] All existing tests pass (backward compatible)
- [ ] New tests cover quantile propagation, edge cases, and numerical
  verification

---

## References

- Monthly vincentization pattern:
  `src/ensemble_calculator.py:276-278`
- Quarterly/seasonal vincentization (shared helper):
  `src/ensemble_calculator.py:559-561` (`_create_aggregated_ensemble_forecasts`)
- Monthly vincentization in recalculation path:
  `src/skill_metrics.py:1171-1173` (also has quantile averaging at
  `_add_naive_mean()` line 1294, `_add_skilled_mean()` line 1465)
- ML forecast normalization (quantile source):
  `src/data_reader.py:1622-1714`
- Short-term Forecast schema (4 quantile columns):
  `sapphire/services/postprocessing/app/models.py:82-86`
- Dashboard uncertainty bands (delta only, no quantiles yet):
  `apps/forecast_dashboard/src/processing.py:1186-1211`
