# PP-020: Probabilistic Forecast Quality Metrics & Documentation

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: Medium
**Labels**: `forecast-quality`, `uncertainty`, `documentation`, `postprocessing`

---

## Summary

Add calibration (PIT), sharpness, and reliability metrics for quantile
forecasts; add a quantile crossing guard after all vincentization steps;
and document the assumptions and known limitations of quantile averaging
across models and across time periods — both for developers and for
operational users.

## Context

The postprocessing module already computes CRPS for long-term forecasts,
which jointly measures calibration and sharpness. Global best practice
(Gneiting et al. 2007, Laio & Tamea 2007, WMO-No. 1154) recommends
decomposing these two aspects and reporting them separately:

- **Calibration/reliability**: Are the quantile bands honest? Does the
  observation fall below q10 approximately 10% of the time?
- **Sharpness**: How informative are the forecasts? Narrow intervals are
  more useful than wide ones (given good calibration).

Additionally, the codebase uses **vincentization** (averaging quantiles)
in two distinct contexts with different statistical properties:

1. **Across models** (ensemble creation): Averaging q05 from TFT and
   q05 from GBT for the same target period. This is valid vincentization
   and produces a well-defined quantile of the mixture distribution.

2. **Across time periods** (temporal aggregation): Averaging monthly q05
   values to get a quarterly q05. This is **not** valid vincentization —
   `mean(q05_jan, q05_feb, q05_mar) != q05(mean_discharge_Q1)`. The
   true quantile of the aggregate depends on the correlation structure
   between months. Averaging quantiles across time underestimates tail
   spread (intervals too narrow).

Both assumptions need to be documented for users and flagged for future
improvement.

### Where vincentization/averaging occurs (15 sites)

**`ensemble_calculator.py`** (6 sites — operational/maintenance ensembles):

| Location | Function | Type | Valid? |
|----------|----------|------|--------|
| Line 278 | `create_monthly_ensemble_forecasts()` EM agg | Across models, same period | Yes |
| Line 375 | `_add_skilled_mean_monthly()` weighted mean | Across models, same period | Yes |
| Line 420 | `_add_naive_mean_monthly()` naive agg | Across models, same period | Yes |
| Line 561 | `_create_aggregated_ensemble_forecasts()` EM agg | Across models, same period | Yes |
| Line 656 | `_add_skilled_mean_aggregated_ens()` weighted mean | Across models, same period | Yes |
| Line 696 | `_add_naive_mean_aggregated_ens()` naive agg | Across models, same period | Yes |

**`skill_metrics.py`** (6 sites — recalculation path):

| Location | Function | Type | Valid? |
|----------|----------|------|--------|
| Line 1173 | `calculate_monthly_skill_metrics()` EM agg | Across models, same period | Yes |
| Line 1294 | `_add_naive_mean()` naive agg | Across models, same period | Yes |
| Line 1465 | `_add_skilled_mean()` weighted mean | Across models, same period | Yes |
| Line 1991 | `_calculate_aggregated_skill_metrics()` EM agg | Across models, same period | Yes |
| Line 2115 | Aggregated naive mean | Across models, same period | Yes |
| Line 2253 | Aggregated skilled mean | Across models, same period | Yes |

**`aggregation.py`** (2 sites — temporal averaging, approximation):

| Location | Function | Type | Valid? |
|----------|----------|------|--------|
| Line 244 | `aggregate_monthly_to_quarterly()` | Across time periods | **Approximation** |
| Line 315 | `aggregate_monthly_to_seasonal()` | Across time periods | **Approximation** |

**`data_reader.py`** (1 site — temporal averaging, approximation):

| Location | Function | Type | Valid? |
|----------|----------|------|--------|
| Line 1662 | `_normalize_ml_forecasts()` daily→pentad/decad | Across time periods | **Approximation** |

**PP-019** (planned, not yet implemented):

| Location | Function | Type | Valid? |
|----------|----------|------|--------|
| TBD | `create_ensemble_forecasts()` short-term EM | Across models, same period | Yes |

### Interaction with active work

This issue is purely additive: new metric functions, a small guard in
existing code, and documentation. No existing data flows or function
signatures change. Safe to implement alongside PP-007/PP-009/PP-013.

## Problem

1. **No calibration diagnostic**: Users and developers cannot tell if
   quantile forecasts are well-calibrated. A model reporting q10 that
   actually captures 30% of observations gives a false sense of precision.
2. **No sharpness metric**: CRPS alone doesn't reveal whether forecasts
   are informative. Two models with equal CRPS can differ dramatically
   in sharpness vs. calibration tradeoff.
3. **No quantile crossing guard**: After vincentization (averaging
   quantiles across models), it's possible for q05 > q10 or similar
   crossings. No post-hoc check exists.
4. **Undocumented assumptions**: The vincentization approach and its
   limitations (especially for temporal aggregation) are not documented
   anywhere — not for developers and not for users.

## Desired Outcome

- PIT histogram data and reliability metrics computed during recalculation
- Sharpness metric (prediction interval width) computed alongside CRPS
- Quantile crossing guard applied after every vincentization step
- User-facing documentation explains what quantile bands mean, how they
  are produced, and what their known limitations are
- Developer documentation explains the vincentization assumption and the
  temporal aggregation approximation

---

## Technical Analysis

### Current Implementation

**CRPS** (`skill_metrics.py:952-1000`):
- Computed for long-term (monthly/quarterly/seasonal) forecasts
- Uses trapezoidal integration of pinball losses over q05-q95
- Will also be computed for short-term after PP-019

**Quantile columns** (`skill_metrics.py:1007-1008`):
```python
_QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]
_QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])
```

**Temporal aggregation** (`aggregation.py:244,315`, `data_reader.py:1662`):
- Both use `"mean"` aggregation on quantile columns
- No documentation of the approximation involved

**Key files:**
- `src/skill_metrics.py:952` — `calculate_crps()`
- `src/skill_metrics.py:1048` — `calculate_monthly_skill_metrics()` (extends well past line 1149; includes EM agg at 1171, naive mean at 1294, skilled mean at 1465)
- `src/skill_metrics.py:1991` — `_calculate_aggregated_skill_metrics()` (EM agg at 1991, naive at 2115, skilled at 2253)
- `src/ensemble_calculator.py` — all ensemble creation functions (6 vincentization sites)
- `src/aggregation.py:244,315` — monthly→quarterly/seasonal forecast aggregation (temporal quantile averaging)
- `src/data_reader.py:1662` — daily→pentad/decad ML forecast aggregation (temporal quantile averaging)
- `src/file_writer.py:340` — `save_monthly_skill_metrics()`
- `apps/postprocessing_forecasts/README.md` — developer docs

### Part A: PIT and Reliability Metrics

**PIT (Probability Integral Transform)**: For each observation, compute
which quantile band it falls in. If forecasts are well-calibrated, the
PIT values should be uniformly distributed.

Given discrete quantiles (q05..q95), we compute the empirical coverage:
for each quantile level tau, count the fraction of observations that
fall below q_tau. Compare to the nominal level tau. This produces a
reliability table.

```
Nominal level | Observed coverage | Deviation
0.05          | 0.03              | -0.02  (slightly under-dispersed)
0.10          | 0.09              | -0.01
0.25          | 0.24              | -0.01
...
```

A scalar summary: **reliability score** = mean absolute deviation of
observed coverage from nominal levels.

**Where to compute**: In `calculate_monthly_skill_metrics()` and the
corresponding quarterly/seasonal functions, and (after PP-019) in
short-term recalculation. Stored alongside existing skill metrics.

### Part B: Sharpness Metric

**Sharpness** = mean prediction interval width. Multiple widths are
informative:

- `sharpness_90` = mean(q95 - q05) — 90% interval width
- `sharpness_50` = mean(q75 - q25) — 50% interval width (IQR)

Lower is better (conditional on good calibration). Computed per
(period, code, model) group alongside other metrics.

### Part C: Quantile Crossing Guard

After any vincentization (averaging quantiles across models), enforce
monotonicity:

```python
for i in range(1, len(quantile_cols)):
    df[quantile_cols[i]] = df[[quantile_cols[i-1], quantile_cols[i]]].max(axis=1)
```

This is a 2-line addition at each vincentization site. PP-019 already
includes this for short-term. This issue adds it to all monthly,
quarterly, and seasonal ensemble functions.

### Part D: Documentation

Two audiences:

1. **User documentation** (new section in user guide or standalone doc):
   Explain what the quantile bands mean, how to interpret them, and
   their limitations. Written for operational hydrologists, not
   developers.

2. **Developer documentation** (README and inline):
   Document the vincentization assumption and temporal aggregation
   approximation. Reference the relevant literature.

### Files to Create

| File | Purpose |
|------|---------|
| `doc/forecast_uncertainty.md` | User-facing documentation on forecast ranges and uncertainty |

### Files to Modify

| File | Changes |
|------|---------|
| `src/skill_metrics.py` | New functions: `calculate_pit_reliability()`, `calculate_sharpness()`; integrate into monthly/quarterly/seasonal skill metric pipelines |
| `src/ensemble_calculator.py` | Add crossing guard after vincentization in `create_monthly_ensemble_forecasts()`, `_add_skilled_mean_monthly()`, `_add_naive_mean_monthly()`, `_create_aggregated_ensemble_forecasts()`, and their helper functions |
| `src/file_writer.py` | Include new metric columns (reliability_score, sharpness_90, sharpness_50) in skill metric CSV/API output |
| `src/api_writer.py` | Include new metric columns in API writes (when API schema supports them) |
| `apps/postprocessing_forecasts/README.md` | Developer documentation on vincentization and temporal aggregation |
| `doc/forecast_uncertainty.md` | New: user-facing uncertainty documentation |

### Implementation Steps

- [ ] **Step 1: Implement `calculate_pit_reliability()`**
  New function in `skill_metrics.py`. Input: observed values array and
  quantile forecast array (N x K). Output: dict with per-level observed
  coverage and a scalar reliability score.

  ```python
  def calculate_pit_reliability(
      observed: np.ndarray,
      quantile_forecasts: np.ndarray,
      quantile_levels: np.ndarray,
  ) -> dict:
      """PIT-based reliability metrics for quantile forecasts.

      Args:
          observed: shape (N,) — observed values.
          quantile_forecasts: shape (N, K) — forecasted quantiles.
          quantile_levels: shape (K,) — e.g. [0.05, ..., 0.95].

      Returns:
          Dict with keys:
            'observed_coverage': array of observed coverage per level,
            'reliability_score': mean |observed - nominal| (lower is better),
            'n_obs': number of valid observations used.
      """
      valid = ~np.isnan(observed)
      if not np.any(valid):
          return {"observed_coverage": np.full(len(quantile_levels), np.nan),
                  "reliability_score": np.nan, "n_obs": 0}

      obs = observed[valid]
      qf = quantile_forecasts[valid]
      n = len(obs)

      # For each quantile level, fraction of obs below forecast quantile
      observed_coverage = np.array([
          np.mean(obs <= qf[:, k]) for k in range(len(quantile_levels))
      ])

      reliability_score = float(np.mean(np.abs(
          observed_coverage - quantile_levels
      )))

      return {
          "observed_coverage": observed_coverage,
          "reliability_score": reliability_score,
          "n_obs": n,
      }
  ```

- [ ] **Step 2: Implement `calculate_sharpness()`**
  New function in `skill_metrics.py`. Input: quantile forecast array.
  Output: sharpness_90 and sharpness_50.

  ```python
  def calculate_sharpness(
      quantile_forecasts: np.ndarray,
      quantile_levels: np.ndarray,
  ) -> dict:
      """Sharpness metrics: mean prediction interval width.

      Args:
          quantile_forecasts: shape (N, K) — forecasted quantiles.
          quantile_levels: shape (K,) — must include 0.05, 0.25, 0.75, 0.95.

      Returns:
          Dict with 'sharpness_90' (mean q95-q05 width) and
          'sharpness_50' (mean q75-q25 width). NaN if levels missing.
      """
      level_idx = {float(l): i for i, l in enumerate(quantile_levels)}
      result = {}

      for name, lo, hi in [("sharpness_90", 0.05, 0.95),
                            ("sharpness_50", 0.25, 0.75)]:
          if lo in level_idx and hi in level_idx:
              widths = quantile_forecasts[:, level_idx[hi]] - \
                       quantile_forecasts[:, level_idx[lo]]
              valid = ~np.isnan(widths)
              result[name] = float(np.mean(widths[valid])) if np.any(valid) \
                             else np.nan
          else:
              result[name] = np.nan

      return result
  ```

- [ ] **Step 3: Integrate into monthly skill metric pipeline**
  In `calculate_monthly_skill_metrics()` (`skill_metrics.py:1118-1142`),
  add PIT reliability and sharpness computation alongside the existing
  CRPS loop. Store as additional columns in skill_stats:
  `reliability_score`, `sharpness_90`, `sharpness_50`.

  Apply the same pattern to `_calculate_aggregated_skill_metrics()`
  for quarterly/seasonal.

- [ ] **Step 4: Add quantile crossing guard to all ensemble functions**
  After every vincentization step in `ensemble_calculator.py`, add:

  ```python
  # Enforce monotonicity after quantile averaging
  for i in range(1, len(_QUANTILE_COLS)):
      if _QUANTILE_COLS[i] in df.columns and _QUANTILE_COLS[i-1] in df.columns:
          df[_QUANTILE_COLS[i]] = df[
              [_QUANTILE_COLS[i-1], _QUANTILE_COLS[i]]
          ].max(axis=1)
  ```

  Locations — **`ensemble_calculator.py`** (6 sites):
  - `create_monthly_ensemble_forecasts()` — after EM agg (line 278)
  - `_add_skilled_mean_monthly()` — after weighted mean (line 375)
  - `_add_naive_mean_monthly()` — after naive agg (line 420)
  - `_create_aggregated_ensemble_forecasts()` — after EM agg (line 561)
  - `_add_skilled_mean_aggregated_ens()` — after weighted mean (line 656)
  - `_add_naive_mean_aggregated_ens()` — after naive agg (line 696)

  Locations — **`skill_metrics.py`** (6 sites, recalculation path):
  - `calculate_monthly_skill_metrics()` — after EM agg (line 1173)
  - `_add_naive_mean()` — after naive agg (line 1294)
  - `_add_skilled_mean()` — after weighted mean (line 1465)
  - `_calculate_aggregated_skill_metrics()` — after EM agg (line 1991)
  - Aggregated naive mean (line 2115)
  - Aggregated skilled mean (line 2253)

  Total: 12 sites in `ensemble_calculator.py` + `skill_metrics.py`,
  plus 3 temporal-averaging sites in `aggregation.py` (244, 315) and
  `data_reader.py` (1662) = **15 sites**.

  Extract a helper to avoid repeating the loop:

  ```python
  def enforce_quantile_monotonicity(
      df: pd.DataFrame,
      quantile_cols: list[str] | None = None,
  ) -> pd.DataFrame:
      """Enforce q05 <= q10 <= ... <= q95 after vincentization.

      Fixes quantile crossings by propagating the running maximum
      from left to right across quantile columns.
      """
      if quantile_cols is None:
          from src.skill_metrics import _QUANTILE_COLS
          quantile_cols = _QUANTILE_COLS
      cols_present = [c for c in quantile_cols if c in df.columns]
      if len(cols_present) < 2:
          return df
      df = df.copy()
      for i in range(1, len(cols_present)):
          df[cols_present[i]] = df[
              [cols_present[i-1], cols_present[i]]
          ].max(axis=1)
      return df
  ```

- [ ] **Step 5: Update writers for new metric columns**
  Add `reliability_score`, `sharpness_90`, `sharpness_50` to the
  skill metrics CSV and API writes. Follow the existing pattern in
  `save_monthly_skill_metrics()` and `_write_skill_metrics_to_api()`.
  New columns are nullable — NaN when quantile data is insufficient.

- [ ] **Step 6: Write user-facing documentation**
  Create `doc/forecast_uncertainty.md` covering:

  - What forecast ranges mean for different model types:
    - **LR**: ±delta bands (0.674 * std of observed flow for the
      period). Represents climatological variability, not model-specific
      uncertainty. Roughly 50% of observations fall within ±delta.
    - **ML models** (TFT, TiDE, TSMixer): Quantile bands (q05-q95)
      produced by the model. Represent the model's estimate of the
      full probability distribution.
    - **Ensemble** (EM, Skilled Mean, Naive Mean): Quantile bands
      averaged across contributing models (vincentization).

  - How to interpret the bands:
    - q05-q95 = "we expect 90% of outcomes within this range"
    - q25-q75 = "the most likely 50% range"
    - Wider bands = more uncertain; narrower = more confident

  - Known limitations:
    - **Temporal aggregation** (daily→pentad, monthly→quarterly/seasonal):
      Quantile bands are averaged across time steps. This is an
      approximation that **underestimates tail spread** — the true
      q05 of the aggregate flow may be lower than the average of the
      daily/monthly q05 values. Forecast ranges at longer horizons
      (quarterly, seasonal) should be interpreted as indicative, not
      as precise probability statements.
    - **Vincentization across models**: Averaging quantiles across
      models is a valid approximation for the mixture distribution,
      but it produces bands that are slightly narrower than the true
      combined uncertainty (does not account for model disagreement
      about the shape of the distribution).
    - **LR delta bands**: These are not model-specific — all LR
      forecasts for the same station and period have the same delta.
      They represent average historical variability, not the quality
      of today's specific forecast.

  - How to check forecast quality:
    - **Reliability score**: Close to 0 means well-calibrated bands.
      Values > 0.1 suggest the bands are systematically too wide or
      too narrow.
    - **Sharpness**: Lower is better (given good calibration). Compare
      between models for the same station.
    - **CRPS**: Overall probabilistic skill. Lower is better. Combines
      calibration and sharpness.

- [ ] **Step 7: Write developer documentation**
  Add a "Quantile Averaging & Known Limitations" section to
  `apps/postprocessing_forecasts/README.md`:
  - Explain vincentization (cross-model) vs temporal averaging
  - Reference: Vincentization is valid for mixture distributions
    (Vincent 1912, Ratcliff 1979)
  - Reference: Temporal averaging of quantiles is an approximation;
    correct approach requires joint distribution or copula
    (Schefzik et al. 2013)
  - Flag the temporal aggregation paths in code
  - Note that fixing this properly requires either:
    (a) ML models producing quantile forecasts at the target resolution
        directly, or
    (b) A copula/resampling approach to reconstruct the aggregate
        distribution from marginals

- [ ] **Step 8: Write tests**
  See Testing section below.

---

## Testing

### Test Cases

**PIT / Reliability:**
- [ ] `test_pit_perfectly_calibrated` — synthetic data where obs falls
  below q_tau exactly tau fraction of the time → reliability_score ≈ 0
- [ ] `test_pit_biased_high` — all observations above q95 →
  observed_coverage near 0 for all levels, high reliability_score
- [ ] `test_pit_empty_input` — empty arrays → NaN result
- [ ] `test_pit_nan_handling` — NaN observations filtered correctly

**Sharpness:**
- [ ] `test_sharpness_constant_spread` — known constant interval width
  → correct sharpness values
- [ ] `test_sharpness_missing_levels` — missing q05 or q95 → NaN for
  sharpness_90
- [ ] `test_sharpness_zero_width` — degenerate forecast (all quantiles
  equal) → sharpness = 0

**Crossing guard:**
- [ ] `test_enforce_monotonicity_no_crossings` — already monotonic →
  unchanged
- [ ] `test_enforce_monotonicity_with_crossings` — q10 > q25 → fixed
- [ ] `test_enforce_monotonicity_partial_columns` — only q25/q75 present
  → still works
- [ ] `test_enforce_monotonicity_all_nan` — NaN columns → unchanged

**Integration:**
- [ ] `test_monthly_skill_metrics_include_new_columns` — verify
  reliability_score, sharpness_90, sharpness_50 in output
- [ ] `test_ensemble_output_monotonic` — after ensemble creation,
  quantiles are always monotonic

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

1. Run `recalculate_skill_metrics.py` for monthly
2. Check skill metrics CSV for new columns
3. Review reliability_score: values near 0 for well-predicted stations,
   higher for poor ones
4. Verify no quantile crossings in combined forecast output

---

## Documentation Impact

- [ ] New file: `doc/forecast_uncertainty.md` — user-facing uncertainty
  documentation (Step 6)
- [ ] Module README (`apps/postprocessing_forecasts/README.md`) — developer
  documentation on vincentization and temporal aggregation (Step 7)
- [ ] User guide (`doc/user_guide.md`) — link to new uncertainty doc
- [ ] Data flow docs (`doc/data_flow_long_term.md`) — note where
  temporal quantile averaging occurs
- [ ] CLAUDE.md — no changes needed

---

## Out of Scope

- **Fixing temporal aggregation**: Replacing the averaging approximation
  with a proper copula/resampling method is a substantial undertaking
  (requires joint distribution modeling). This issue documents the
  limitation and flags it for future work.
- **PIT visualization in the dashboard**: A PIT histogram plot would be
  valuable but belongs in a separate FD issue.
- **Calibration-based interval adjustment**: Post-hoc recalibration
  (e.g., isotonic regression on PIT values) is a potential future
  enhancement.
- **Short-term PIT/sharpness**: PIT and sharpness for short-term
  forecasts depends on PP-019 (quantiles must be in the DataFrame).
  The crossing guard (Part C) has no PP-019 dependency and is in scope.

## Dependencies

- **PP-019** (partial): PIT/sharpness for **short-term** forecasts
  requires PP-019 first (quantiles must be in the DataFrame). Long-term
  PIT/sharpness (Parts A, B) and the crossing guard (Part C) have **no
  dependency on PP-019** and can be implemented immediately.
- **API schema** (colleague-managed): New metric columns
  (reliability_score, sharpness_90, sharpness_50) need to be added to
  the skill metrics table. Until then, CSV output works independently.

## Acceptance Criteria

- [ ] `calculate_pit_reliability()` produces correct coverage fractions
  and reliability score
- [ ] `calculate_sharpness()` produces correct interval widths
- [ ] Monthly, quarterly, and seasonal skill metrics include new columns
- [ ] Quantile crossing guard applied at all 15 vincentization sites
- [ ] No quantile crossings in any forecast output
- [ ] `doc/forecast_uncertainty.md` exists and explains bands, limitations,
  and quality metrics for operational hydrologists
- [ ] Developer docs explain vincentization assumption and temporal
  aggregation approximation with literature references
- [ ] All existing tests pass; new tests cover all new functions
- [ ] Temporal aggregation limitation clearly flagged as known issue
  with future improvement path documented

---

## References

- Gneiting, T. & Raftery, A.E. (2007). Strictly proper scoring rules,
  prediction, and estimation. JASA 102(477), 359-378.
- Laio, F. & Tamea, S. (2007). Verification tools for probabilistic
  forecasts of continuous hydrological variables. HESS 11(4), 1267-1277.
- Schefzik, R., Thorarinsdottir, T.L. & Gneiting, T. (2013). Uncertainty
  quantification in complex simulation models using ensemble copula
  coupling. Statistical Science 28(4), 616-640.
- WMO-No. 1154: Guidelines on Communicating Forecast Uncertainty.
- Vincent, S.B. (1912). The function of the vibrissae in the behavior
  of the white rat. Behavioral Monographs 1(5).
- Yilmaz, K.K., Gupta, H.V. & Wagener, T. (2008). A process-based
  diagnostic approach to model evaluation. Water Resources Research 44.
- Existing CRPS implementation:
  `apps/postprocessing_forecasts/src/skill_metrics.py:952`
- Temporal aggregation code:
  `apps/postprocessing_forecasts/src/aggregation.py:244` (quarterly),
  `apps/postprocessing_forecasts/src/aggregation.py:315` (seasonal),
  `apps/postprocessing_forecasts/src/data_reader.py:1662` (daily→pentad/decad)
