# PP-020: Probabilistic Forecast Quality Metrics & Documentation

**Status**: Review — implementation complete (commit `8fd2a4e`), awaiting user review
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

### Where vincentization/averaging occurs (17 sites)

> **Review note (2026-03-27):** All line numbers verified against current
> `develop_long_term_fix_api_postprocessing_forecasts` branch. Line
> numbers shift as the code evolves — implementation agents must match
> by **function name + pattern** (e.g., `groupby(...).agg(em_agg)`),
> not by line number alone.

Sites are classified into two types that require **different treatment**:

- **Type A — Multi-model ensemble (true vincentization):** Averages
  quantiles from different models for the same target period. Crossings
  are a statistical artifact of mixing CDFs. **Crossing guard: APPLY.**
- **Type B — Single-model temporal aggregation:** Averages one model's
  quantiles across time steps (daily→pentad, monthly→quarterly). Crossings
  here indicate model quality issues. **Crossing guard: LOG ONLY, do NOT
  correct** — correcting would mask a real problem.

**Type A — `ensemble_calculator.py`** (7 sites — operational/maintenance):

| ~Line | Function | Context |
|-------|----------|---------|
| 153 | `create_ensemble_forecasts()` short-term EM | PP-019 (implemented); 4-quantile set |
| 277 | `create_monthly_ensemble_forecasts()` EM agg | 7-quantile set |
| 375 | `_add_skilled_mean_monthly()` weighted mean | 7-quantile set |
| 419 | `_add_naive_mean_monthly()` naive agg | 7-quantile set |
| 560 | `_create_aggregated_ensemble_forecasts()` EM agg | 7-quantile set |
| 655 | `_add_skilled_mean_aggregated_ens()` weighted | 7-quantile set |
| 695 | `_add_naive_mean_aggregated_ens()` naive agg | 7-quantile set |

**Type A — `skill_metrics.py`** (7 sites — recalculation path):

| ~Line | Function | Context |
|-------|----------|---------|
| 1739 | `calculate_skill_metrics()` short-term EM | **MISSED in original plan**; uses `_SHORT_TERM_Q_COLS` (4 cols) |
| 1181 | `calculate_monthly_skill_metrics()` EM agg | 7-quantile set |
| 1302 | `_add_naive_mean()` naive agg | 7-quantile set |
| 1476 | `_add_skilled_mean()` weighted mean | 7-quantile set |
| 2008 | `_calculate_aggregated_skill_metrics()` EM agg | 7-quantile set |
| 2132 | `_add_naive_mean_aggregated()` naive agg | 7-quantile set |
| 2271 | `_add_skilled_mean_aggregated()` weighted mean | 7-quantile set |

**Type B — temporal aggregation** (2 sites — LOG ONLY, no correction):

| ~Line | Function | Context |
|-------|----------|---------|
| 244 | `aggregation.aggregate_monthly_fc_to_quarterly()` | Single-model monthly→quarterly; 7-quantile set |
| 315 | `aggregation.aggregate_monthly_fc_to_seasonal()` | Single-model monthly→seasonal; 7-quantile set |

**Type B — `data_reader.py`** (1 site — LOG ONLY, no correction):

| ~Line | Function | Context |
|-------|----------|---------|
| 1860 | `_normalize_ml_forecasts()` daily→pentad/decad | Single-model daily→period; 4-quantile set. **Original plan said line 1662 — wrong by ~200 lines.** |

**Total: 14 Type A sites (apply guard) + 3 Type B sites (log only) = 17
check-points, of which 14 get corrections.**

### Interaction with active work

PP-019 is **already implemented** (commit `530cb7b`, verified 2026-03-13).
This fully unblocks Parts A, B, and C for all horizons including
short-term. PP-007, PP-009, PP-013 are also resolved.

This issue is purely additive: new metric functions, a small guard in
existing code, and documentation. No existing data flows or function
signatures change.

**Important**: `crps` is already in `SkillMetricBase` (schemas.py:175)
but missing from the short-term consistency check `value_columns`.
`reliability_score`, `sharpness_90`, `sharpness_50` are NOT yet in
`SkillMetricBase` — they will be calculated and written to CSV but
require a future schema update for API persistence.

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
- Quantile crossing guard applied after every Type A (multi-model)
  vincentization step; crossing detector (log-only) at Type B
  (temporal aggregation) sites
- User-facing documentation explains what quantile bands mean, how they
  are produced, and what their known limitations are
- Developer documentation explains the vincentization assumption and the
  temporal aggregation approximation

---

## Technical Analysis

### Current Implementation

**CRPS** (`skill_metrics.py:952-1000`):
- Computed for long-term (monthly/quarterly/seasonal) forecasts only
- Uses trapezoidal integration of pinball losses over q05-q95
- **Short-term: NOT computed.** `calculate_skill_metrics()` has no CRPS
  loop. Quantile columns (q05/q25/q75/q95) exist in `skill_metrics_df`
  but are excluded from the `calculate_all_skill_metrics` groupby.
  Adding CRPS requires a second groupby pass with
  `_SHORT_TERM_Q_COLS` (4 columns) and matching levels
  `[0.05, 0.25, 0.75, 0.95]`. `crps` is already in `SkillMetricBase`
  (schemas.py:175), so this is a gap that should be filled.

**Quantile columns** (`skill_metrics.py:1007-1008`):
```python
_QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]
_QUANTILE_LEVELS = np.array([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])
```

**Temporal aggregation** (`aggregation.py:244,315`, `data_reader.py:~1860`):
- Both use `"mean"` aggregation on quantile columns
- No documentation of the approximation involved

**Key files** (line numbers approximate — verify by function name):
- `src/skill_metrics.py:~952` — `calculate_crps()`
- `src/skill_metrics.py:~1048` — `calculate_monthly_skill_metrics()` (EM agg ~1181, naive ~1302, skilled ~1476)
- `src/skill_metrics.py:~1739` — `calculate_skill_metrics()` short-term EM (uses `_SHORT_TERM_Q_COLS`)
- `src/skill_metrics.py:~1889` — `_calculate_aggregated_skill_metrics()` (EM ~2008, naive ~2132, skilled ~2271)
- `src/ensemble_calculator.py` — all ensemble creation functions (7 vincentization sites incl. short-term)
- `src/aggregation.py:~244,~315` — monthly→quarterly/seasonal forecast aggregation (temporal quantile averaging)
- `src/data_reader.py:~1860` — daily→pentad/decad ML forecast aggregation (temporal quantile averaging)
- `src/file_writer.py:~340` — `save_monthly_skill_metrics()`
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

- [ ] **Step 3a: Add CRPS to short-term skill metric pipeline**
  `calculate_skill_metrics()` currently computes only point metrics.
  Add a CRPS loop (same pattern as long-term sites) using:
  - `_SHORT_TERM_Q_COLS = ["q05", "q25", "q75", "q95"]` (local var,
    ~line 1734)
  - `_SHORT_TERM_Q_LEVELS = np.array([0.05, 0.25, 0.75, 0.95])`
    (new constant, define alongside `_SHORT_TERM_Q_COLS`)
  - Guard: `if len(qf_cols) == len(_SHORT_TERM_Q_COLS)`
  - Two CRPS sub-loops: one for individual models (on `skill_metrics_df`,
    grouped by `[period_col, "code", "model_short"]`), one for EM
    (on `ensemble_skill_metrics_df` after vincentization)
  - Merge result into `skill_stats` via left join, same pattern as
    long-term sites
  - Update `save_skill_metrics()` consistency check `value_columns`
    (~line 319) to include `"crps"`

  **Note**: `crps` is already in `SkillMetricBase` (schemas.py:175)
  and in the API writer whitelist (api_writer.py:~585), so short-term
  CRPS will flow to both CSV and API immediately.

  PIT/sharpness are also computed inside this same loop (added in
  Phase 2b), so the loop must be designed to accommodate all three
  new metrics from the start. The CRPS loop iterates over
  `(period, code, model)` groups — PIT/sharpness are computed on the
  same `obs_arr` and `qf` arrays inside the same iteration.

- [ ] **Step 3b: Integrate PIT/sharpness into long-term skill metric pipeline**
  In `calculate_monthly_skill_metrics()` (~line 1123), add PIT
  reliability and sharpness computation alongside the existing CRPS
  loop. Store as additional columns in skill_stats:
  `reliability_score`, `sharpness_90`, `sharpness_50`.

  Apply the same pattern to `_calculate_aggregated_skill_metrics()`
  for quarterly/seasonal. There are **8 CRPS integration sites** total
  (not 6 — the base-path individual-model loops count separately):

  **Monthly** (4 sites):
  1. Base path individual models (~line 1121)
  2. EM ensemble (~line 1214)
  3. Naive Mean — `_add_naive_mean()` (~line 1340)
  4. Skilled Mean — `_add_skilled_mean()` (~line 1515)

  **Aggregated (quarterly/seasonal)** (4 sites):
  5. Base path individual models (~line 1953)
  6. EM ensemble (~line 2038)
  7. Naive Mean — `_add_naive_mean_aggregated()` (~line 2167)
  8. Skilled Mean — `_add_skilled_mean_aggregated()` (~line 2302)

  **Short-term PIT/sharpness: INCLUDED.** Step 3a creates the CRPS
  loop infrastructure for short-term; PIT/sharpness are added inside
  the same loop. The 4-column quantile set (q05/q25/q75/q95) gives
  fewer reliability table rows than the 7-column long-term set, but
  still provides meaningful calibration and sharpness assessment.
  Use `_SHORT_TERM_Q_LEVELS` for PIT computation at these sites.

  **Critical constraints for implementation agents:**

  1. **NaN guard required**: Surround PIT/sharpness calls with the same
     `if len(qf_cols) == len(_QUANTILE_COLS)` guard used for CRPS. Set
     `reliability_score = np.nan`, `sharpness_90 = np.nan`,
     `sharpness_50 = np.nan` in the else branch. Without this, LR
     groups (no quantile columns) will cause `KeyError`.

  2. **`empty_stats` declarations must be updated**: Both `empty_stats`
     DataFrames at ~line 1075 and ~line 1909 must include
     `"reliability_score"`, `"sharpness_90"`, `"sharpness_50"` in their
     `columns=` list, or callers receiving empty returns will get
     `KeyError` on the new columns.

  3. **Order of operations**: The crossing guard (Step 4) must be applied
     **before** PIT/sharpness computation at each site in
     `skill_metrics.py`, or `calculate_sharpness()` may return negative
     values for crossed quantiles.

  4. **Minimum n**: Return `reliability_score = np.nan` when `n_obs < 2`
     (a single observation produces a meaningless reliability score).

- [ ] **Step 4: Add quantile crossing guard (Type A) and detector (Type B)**

  **Type A sites (14) — apply correction:**

  After every multi-model vincentization step, call
  `enforce_quantile_monotonicity(result_var)`. Insert after the
  `is_multi_model_composition` filter `.copy()` and before the
  `if not result_var.empty` block.

  **`ensemble_calculator.py`** (7 sites):
  - `create_ensemble_forecasts()` — short-term EM (~line 153)
  - `create_monthly_ensemble_forecasts()` — EM agg (~line 277)
  - `_add_skilled_mean_monthly()` — weighted mean (~line 375)
  - `_add_naive_mean_monthly()` — naive agg (~line 419)
  - `_create_aggregated_ensemble_forecasts()` — EM agg (~line 560)
  - `_add_skilled_mean_aggregated_ens()` — weighted (~line 655)
  - `_add_naive_mean_aggregated_ens()` — naive agg (~line 695)

  **`skill_metrics.py`** (7 sites):
  - `calculate_skill_metrics()` — short-term EM (~line 1739; uses
    `_SHORT_TERM_Q_COLS`, 4 columns)
  - `calculate_monthly_skill_metrics()` — EM agg (~line 1181)
  - `_add_naive_mean()` — naive agg (~line 1302)
  - `_add_skilled_mean()` — weighted mean (~line 1476)
  - `_calculate_aggregated_skill_metrics()` — EM agg (~line 2008)
  - `_add_naive_mean_aggregated()` — naive agg (~line 2132)
  - `_add_skilled_mean_aggregated()` — weighted mean (~line 2271)

  **Type B sites (3) — log-only crossing detector, NO correction:**

  At temporal aggregation sites, crossings indicate model quality
  issues (the model's quantile estimates are inconsistent across time
  steps). Silently correcting them would mask diagnostic information.
  Instead, count and log crossings without modifying values.

  - `aggregation.aggregate_monthly_fc_to_quarterly()` (~line 244)
  - `aggregation.aggregate_monthly_fc_to_seasonal()` (~line 315)
  - `data_reader._normalize_ml_forecasts()` (~line 1860; **NOT** 1662
    as originally stated)

  **Helper functions** — place in `postprocessing_tools.py` (NOT
  `skill_metrics.py`). Reason: `skill_metrics.py` ↔
  `ensemble_calculator.py` have a circular import relationship
  (resolved via deferred inline imports). Placing helpers in
  `skill_metrics.py` would force `ensemble_calculator.py` to add 7
  new inline imports. `postprocessing_tools.py` has no circular
  dependencies with any of the 4 consumer files
  (`ensemble_calculator.py`, `skill_metrics.py`, `aggregation.py`,
  `data_reader.py`).

  **No constant relocation needed.** Make `quantile_cols` a **required
  parameter** (no default) in both helper functions. Every call site
  already has its own column list in scope:
  - `ensemble_calculator.py`: 3 sites import `_QUANTILE_COLS` inline
    from `skill_metrics`; 4 receive it as a function parameter
  - `skill_metrics.py`: module-level `_QUANTILE_COLS` (7 cols) and
    local `_SHORT_TERM_Q_COLS` (4 cols)
  - `aggregation.py`: module-level `_FC_QUANTILE_COLS` (7 cols)
  - `data_reader.py`: local hardcoded `["q05", "q25", "q75", "q95"]`

  This avoids any refactoring of existing imports or constants:

  ```python
  def enforce_quantile_monotonicity(
      df: pd.DataFrame,
      quantile_cols: list[str],
  ) -> pd.DataFrame:
      """Enforce q05 <= q10 <= ... <= q95 after vincentization.

      Fixes quantile crossings by propagating the running maximum
      from left to right across quantile columns. Rows where any
      quantile column is NaN are left untouched.

      Args:
          df: DataFrame with quantile columns.
          quantile_cols: Ordered list of column names (ascending).

      Returns:
          Copy of df with monotonicity enforced on non-NaN rows.
      """
      cols_present = [c for c in quantile_cols if c in df.columns]
      if len(cols_present) < 2:
          return df
      df = df.copy()
      # Only correct rows where ALL quantile cols are non-NaN.
      # This prevents max(NaN, 5.0) → 5.0 from creating fake values
      # (e.g., LR rows have all-NaN quantiles and must stay NaN).
      mask = df[cols_present].notna().all(axis=1)
      n_corrected = 0
      for i in range(1, len(cols_present)):
          prev, curr = cols_present[i - 1], cols_present[i]
          crossing = mask & (df[prev] > df[curr])
          n_corrected += crossing.sum()
          df.loc[crossing, curr] = df.loc[crossing, prev]
      if n_corrected > 0:
          logger.debug(
              "Quantile crossing guard: corrected %d values", n_corrected
          )
      return df


  def count_quantile_crossings(
      df: pd.DataFrame,
      quantile_cols: list[str],
      label: str = "",
  ) -> int:
      """Count quantile crossings without correcting them.

      For use at Type B (temporal aggregation) sites where crossings
      indicate model quality issues and should be logged, not fixed.

      Returns:
          Total number of crossing cells detected.
      """
      cols_present = [c for c in quantile_cols if c in df.columns]
      if len(cols_present) < 2:
          return 0
      mask = df[cols_present].notna().all(axis=1)
      n_crossings = 0
      for i in range(1, len(cols_present)):
          prev, curr = cols_present[i - 1], cols_present[i]
          n_crossings += (mask & (df[prev] > df[curr])).sum()
      if n_crossings > 0:
          logger.warning(
              "Quantile crossings detected%s: %d values (not corrected "
              "— temporal aggregation site, may indicate model quality "
              "issue)",
              f" [{label}]" if label else "",
              n_crossings,
          )
      return n_crossings
  ```

  **Key design decisions for the crossing guard:**

  1. **NaN safety**: Uses `notna().all(axis=1)` mask so rows with any
     NaN quantile are skipped entirely. `max(NaN, 5.0)` returns `5.0`
     in pandas (skipna=True default), which would silently create fake
     quantile values — this is avoided by the mask.
  2. **Audit trail**: Logs correction count at DEBUG level (Type A) or
     WARNING level (Type B). Operators can see how many values were
     corrected per run.
  3. **`.copy()` is required**: The column assignment modifies the
     DataFrame in-place; without `.copy()`, the caller's DataFrame
     would be mutated.
  4. **Column ordering precondition**: `quantile_cols` must be in
     ascending order. All four independently-defined quantile column
     lists are correctly ordered:
     - `_QUANTILE_COLS` in `skill_metrics.py` (7 cols, module-level)
     - `_SHORT_TERM_Q_COLS` in `skill_metrics.py` (4 cols, local var
       inside `calculate_skill_metrics()`)
     - `_FC_QUANTILE_COLS` in `aggregation.py` (7 cols, module-level)
     - `_SHORT_TERM_QUANTILE_COLS` in `api_writer.py` (4 cols, local
       var in forecast writer — not used by crossing guard)

- [ ] **Step 5: Update writers for new metric columns**

  **Step 5a — CSV (no blocker):** New columns automatically flow through
  to CSV via `atomic_write_csv` (no column whitelist).

  **Consistency check updates:**
  - `save_skill_metrics()` (~line 319): Add `"crps"` to `value_columns`
    (already in API schema, was missing from short-term consistency
    check). Do **NOT** add `reliability_score`, `sharpness_90`,
    `sharpness_50` to `value_columns` — these are NaN for LR models,
    and the consistency check compares `NaN != NaN` as a mismatch,
    producing false `CONSISTENCY CHECK FAILED` logs.
  - `save_monthly_skill_metrics()` (~line 399): Same — do NOT add the
    three new probabilistic columns to `value_columns`. `crps` is
    already present here.
  - The new columns still flow to CSV (no whitelist blocks them); they
    just aren't verified by the consistency check until the check is
    made NaN-tolerant (separate issue).

  **Step 5b — API (BLOCKED on colleague schema update):**
  `_write_skill_metrics_to_api()` uses an explicit whitelist (lines
  579–591). New columns are silently dropped until added. The service
  layer needs:
  - `models.py`: Three `Column(Float)` on `SkillMetric`
  - `schemas.py`: Three `float | None = None` fields on `SkillMetricBase`
  - `crud.py`: No changes (uses `item.model_dump()` → `setattr` loop)
  - DB: `ALTER TABLE skill_metrics ADD COLUMN` × 3 (no Alembic —
    requires manual SQL or container rebuild)

  Pydantic v2 defaults to `extra="ignore"`, so if Step 5b lands in
  `api_writer.py` before the schema update, the fields are silently
  dropped — no 422 errors, but no persistence either (silent data loss).

  **Do NOT implement Step 5b until the schema update is confirmed.**
  CSV output (Step 5a) works independently and is sufficient for now.

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
- [ ] `test_pit_single_observation` — n_obs=1 returns NaN reliability_score
  (single observation is statistically meaningless)

**Sharpness:**
- [ ] `test_sharpness_constant_spread` — known constant interval width
  → correct sharpness values
- [ ] `test_sharpness_missing_levels` — missing q05 or q95 → NaN for
  sharpness_90
- [ ] `test_sharpness_zero_width` — degenerate forecast (all quantiles
  equal) → sharpness = 0
- [ ] `test_sharpness_nan_rows` — NaN quantile rows → excluded from
  mean width calculation (symmetric with `test_pit_nan_handling`)

**Crossing guard:**
- [ ] `test_enforce_monotonicity_no_crossings` — already monotonic →
  unchanged
- [ ] `test_enforce_monotonicity_with_crossings` — q10 > q25 → fixed
- [ ] `test_enforce_monotonicity_partial_columns` — only q25/q75 present
  → still works
- [ ] `test_enforce_monotonicity_all_nan` — all-NaN rows → unchanged
- [ ] `test_enforce_monotonicity_mixed_nan` — rows with partial NaN
  quantiles are NOT modified (prevents fake value creation)
- [ ] `test_enforce_monotonicity_mixed_rows` — DataFrame with both
  all-NaN (LR) and non-NaN (ML) rows; only ML rows get corrected
- [ ] `test_enforce_monotonicity_short_term_4cols` — works correctly
  with `_SHORT_TERM_Q_COLS` (4 columns, not 7)
- [ ] `test_count_crossings_log_only` — `count_quantile_crossings()`
  detects crossings and logs warning but does NOT modify values

**Integration:**
- [ ] `test_monthly_skill_metrics_include_new_columns` — verify
  reliability_score, sharpness_90, sharpness_50 in output
- [ ] `test_ensemble_output_monotonic` — after ensemble creation,
  quantiles are always monotonic
- [ ] `test_empty_stats_has_new_columns` — verify both `empty_stats`
  DataFrames include the three new metric columns

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
- **Step 5b — API write of new metric columns**: Blocked on colleague
  schema update in `sapphire/services/postprocessing/`. CSV output
  (Step 5a) works independently.
- **Deduplicating quantile-column constants**: `_FC_QUANTILE_COLS` in
  `aggregation.py` duplicates `_QUANTILE_COLS` in `skill_metrics.py`;
  `_SHORT_TERM_QUANTILE_COLS` in `api_writer.py` duplicates
  `_SHORT_TERM_Q_COLS` (local in `skill_metrics.py`). Not a bug, but
  a maintenance liability. PP-020 intentionally does not consolidate
  these — each call site passes its own list to the helper functions.
- **NaN-tolerant consistency checks**: The `SAPPHIRE_CONSISTENCY_CHECK`
  in `file_writer.py` treats NaN != NaN as a mismatch. This prevents
  adding probabilistic metric columns to `value_columns`. Making the
  check NaN-tolerant is a separate infrastructure issue.

## Dependencies

- **PP-019**: ~~Partial blocker~~ **Resolved** — PP-019 is implemented
  (commit `530cb7b`, 2026-03-13). All parts of PP-020 are now unblocked
  for all horizons including short-term.
- **API schema** (colleague-managed): New metric columns
  (reliability_score, sharpness_90, sharpness_50) need to be added to
  the skill metrics table in `sapphire/services/postprocessing/`. Until
  then, CSV output works independently. The service uses `create_all`
  (no Alembic) — changes require manual `ALTER TABLE` or container
  rebuild. Pydantic v2 defaults to `extra="ignore"`, so premature
  API writes are silently dropped (no 422 errors, but no persistence).

## Implementation Phasing

> **Review note (2026-03-27):** Based on risk analysis and code review
> with 5 Sonnet 4.6 investigation agents across 2 rounds, PP-020
> should be implemented in four phases to minimize blast radius.

**Phase 1 — Crossing Guard (Part C, Steps 4 + tests)**
- Zero risk to existing data flows; purely additive
- Immediate data quality benefit (prevents quantile crossings in
  production ensemble output)
- No API schema dependency
- **No constant relocation**: `quantile_cols` is a required parameter;
  each call site passes its own locally-available column list
- Agent A: helper functions in `postprocessing_tools.py` + 7 guard
  insertions in `ensemble_calculator.py` + all unit tests for helpers
- Agent B (sequential, after A): 7 guard insertions in
  `skill_metrics.py` + 3 detector insertions in `aggregation.py` /
  `data_reader.py` + integration tests
- **Circular import note**: `skill_metrics.py` ↔
  `ensemble_calculator.py` have deferred inline imports. Placing
  helpers in `postprocessing_tools.py` avoids this entirely. All 4
  consumer files can import from `postprocessing_tools.py` at module
  level without circular risk.

**Phase 2a — Short-term CRPS (Step 3a + tests)**
- Fills existing gap: `crps` is in `SkillMetricBase` (schemas.py:175)
  and the API writer whitelist but never computed for short-term
- Adds CRPS loop to `calculate_skill_metrics()` using
  `_SHORT_TERM_Q_COLS` (4 columns) with matching quantile levels
- Two sub-loops: individual models + EM (after vincentization)
- Adds `"crps"` to `save_skill_metrics()` consistency check
  `value_columns`
- **No dependency on Phase 1** — can run in parallel

**Phase 2b — PIT + Sharpness (Parts A+B, Steps 1–2 + Step 3b + Step 5a + tests)**
- Depends on Phase 1 (guard must run before sharpness computation)
- New functions: `calculate_pit_reliability()`, `calculate_sharpness()`
- 8 long-term integration sites + 2 short-term sites (individual
  models + EM, piggyback on Phase 2a CRPS loop)
- `empty_stats` updates at ~line 1075 and ~line 1909
- New columns flow to CSV automatically; do NOT add to consistency
  check `value_columns` (NaN for LR → false failures)
- API write deferred (Step 5b)

**Phase 3 — Documentation (Parts D, Steps 6–7)**
- Independent of Phases 1–2; can run in parallel
- `doc/forecast_uncertainty.md` (user-facing)
- README developer docs (vincentization + temporal approximation)

**Deferred — API Write (Step 5b)**
- Gate condition: colleague confirms schema update deployed
- Then: add 3 fields to `_write_skill_metrics_to_api()` whitelist

```json
{
  "phases": {
    "P1_crossing_guard": { "depends_on": [], "parallel_agents": 1, "note": "sequential A then B to avoid skill_metrics.py conflicts" },
    "P2a_short_term_crps": { "depends_on": [], "parallel_agents": 1 },
    "P2b_pit_sharpness": { "depends_on": ["P1_crossing_guard", "P2a_short_term_crps"], "parallel_agents": 1 },
    "P3_documentation": { "depends_on": [], "parallel_agents": 1 },
    "deferred_api_write": { "depends_on": ["P2b_pit_sharpness"], "parallel_agents": 1 }
  }
}
```

## Acceptance Criteria

**Crossing Guard (Phase 1):**
- [ ] `enforce_quantile_monotonicity()` and `count_quantile_crossings()`
  live in `postprocessing_tools.py` (not `skill_metrics.py`)
- [ ] `quantile_cols` is a required parameter (no default) — no
  constants moved between files
- [ ] Crossing guard applied at all 14 Type A vincentization sites
  (including the short-term EM site at `skill_metrics.py:~1739`)
- [ ] Crossing detector (log-only) at all 3 Type B temporal sites
- [ ] Guard handles NaN correctly: all-NaN rows (LR) and partial-NaN
  rows are left untouched
- [ ] Guard logs correction count (DEBUG for Type A, WARNING for Type B)
- [ ] No quantile crossings in any multi-model ensemble output

**Short-term CRPS (Phase 2a):**
- [ ] `calculate_skill_metrics()` computes CRPS for all models with
  quantile columns (ML models + EM), using `_SHORT_TERM_Q_COLS` (4
  columns) and `_SHORT_TERM_Q_LEVELS = [0.05, 0.25, 0.75, 0.95]`
- [ ] `save_skill_metrics()` consistency check `value_columns` includes
  `"crps"` (already in API schema)
- [ ] LR models get `crps = NaN` (no quantile columns)

**PIT + Sharpness (Phase 2b):**
- [ ] `calculate_pit_reliability()` produces correct coverage fractions
  and reliability score; returns NaN when n_obs < 2
- [ ] `calculate_sharpness()` produces correct interval widths
- [ ] All 8 long-term CRPS sites + 2 short-term sites include
  `reliability_score`, `sharpness_90`, `sharpness_50`
- [ ] `empty_stats` DataFrames (~line 1075 and ~line 1909) include new
  metric columns
- [ ] New columns flow to CSV automatically; NOT added to consistency
  check `value_columns` (NaN for LR → false failures)
- [ ] Crossing guard is applied before PIT/sharpness computation at
  each integration point (guard-before-sharpness is required to avoid
  negative interval widths from crossed quantiles)

**Documentation (Phase 3):**
- [ ] `doc/forecast_uncertainty.md` exists and explains bands, limitations,
  and quality metrics for operational hydrologists
- [ ] Developer docs explain vincentization assumption and temporal
  aggregation approximation with literature references
- [ ] Temporal aggregation limitation clearly flagged as known issue
  with future improvement path documented
- [ ] `doc/user_guide.md` links to `doc/forecast_uncertainty.md`
- [ ] `doc/data_flow_long_term.md` notes temporal quantile averaging
  locations

**General:**
- [ ] All existing tests pass; new tests cover all new functions
- [ ] No circular imports introduced

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
  `apps/postprocessing_forecasts/src/data_reader.py:1860` (daily→pentad/decad)

---

## Review Notes (2026-03-27)

Critical findings from code review with Sonnet 4.6 investigation agents:

### Bugs found in original plan

1. **NaN-propagation bug in crossing guard**: The original
   `df[[col1, col2]].max(axis=1)` uses `skipna=True` (pandas default),
   so `max(NaN, 5.0) → 5.0`. This would create fake quantile values for
   any row where some quantile columns are NaN. Fixed by adding a
   `notna().all(axis=1)` mask before correction (see updated Step 4).

2. **Site 15 line number wrong by ~200 lines**: Plan said
   `data_reader.py:1662` but actual averaging is at line 1860.

3. **Missed site**: `calculate_skill_metrics()` short-term EM at
   `skill_metrics.py:1739` uses `_SHORT_TERM_Q_COLS` (4 columns).
   Added to site inventory.

### Design decisions changed

4. **Type A/B site distinction**: The original plan applied the crossing
   guard uniformly at all 15 sites. Review found that 3 temporal
   aggregation sites (aggregation.py × 2, data_reader.py × 1) should
   only LOG crossings, not correct them — corrections at these sites
   would mask model quality issues.

5. **Step 5 split**: API write (Step 5b) separated from CSV (Step 5a)
   because the postprocessing service schema lacks the new columns and
   is colleague-managed. No Alembic — schema changes require manual SQL
   or container rebuild.

### Service layer state (verified)

- `SkillMetricBase` has 12 metric fields (sdivsigma through flv).
  Missing: `reliability_score`, `sharpness_90`, `sharpness_50`.
- `SkillMetric` DB model matches schema. CRUD uses `model_dump()` →
  `setattr` loop, so no CRUD changes needed once schema is updated.
- Pydantic v2 `extra="ignore"` (default) — extra fields silently
  dropped, no 422 errors.
- `Forecast` table (short-term) has q05, q25, q75, q95 only (q50
  commented out; q10, q90 absent). `LongForecast` has all 7.
- No Alembic migrations directory exists.

### Data flow verified safe

- New metric columns auto-flow to CSV (no whitelist in file_writer).
- API writer whitelist silently drops unknown columns — safe.
- Crossing guard corrections flow to both CSV and API for short-term
  forecasts; CSV-only for monthly (operational path doesn't write
  monthly forecasts to API — they're written by `long_term_forecasting`).
- Quarterly/seasonal are API-only (no CSV). Log-only detector has zero
  side effects at these sites.

### Deep review findings (2026-03-27, round 2)

5 Sonnet 4.6 agents across 2 rounds verified the plan against the
actual codebase. All 17 vincentization sites confirmed at exact line
numbers. Additional findings:

6. **Circular import risk (HIGH)**: `skill_metrics.py` ↔
   `ensemble_calculator.py` have deferred inline imports to avoid
   circular dependencies. Placing `enforce_quantile_monotonicity()`
   in `skill_metrics.py` would force 7 new inline imports from
   `ensemble_calculator.py`. **Resolution**: place helper functions in
   `postprocessing_tools.py` with `quantile_cols` as a required
   parameter — no constants need to move between files.

7. **Short-term has no CRPS loop**: `calculate_skill_metrics()` never
   computes CRPS despite `crps` being in `SkillMetricBase`
   (schemas.py:175) and the API writer whitelist. **Resolution**: add
   Phase 2a to create CRPS loop using `_SHORT_TERM_Q_COLS` (4 cols)
   + matching levels. PIT/sharpness added in same loop.

8. **Consistency check NaN mismatch**: Adding probabilistic columns
   to `value_columns` causes false `CONSISTENCY CHECK FAILED` for LR
   rows (NaN != NaN). **Resolution**: do NOT add probabilistic cols
   to `value_columns`; only add `crps` to short-term check (already
   in API schema). NaN-tolerant check is a separate future issue.

9. **8 CRPS sites, not 6**: Plan said "6 integration sites" but the
   base-path individual-model loops (monthly ~1121, aggregated ~1953)
   are separate from the EM/Naive/Skilled loops. Updated count to 8.

10. **Stale docstring**: `_add_skilled_mean()` line 1397 says "CRPS is
    NaN (point forecast only)" but code at lines 1515–1540 actually
    computes CRPS from vincentized quantiles. Not blocking but could
    mislead implementation agents.

### Refactoring scope review (2026-03-27, round 3)

3 additional Sonnet 4.6 agents verified refactoring scope and
feasibility. Key finding: **no constant relocation needed.**

11. **`quantile_cols` as required parameter eliminates refactoring**:
    Every call site already has its own column list in scope —
    `ensemble_calculator.py` (3 inline imports + 4 function params),
    `skill_metrics.py` (module-level), `aggregation.py` (module-level
    `_FC_QUANTILE_COLS`), `data_reader.py` (local hardcoded list).
    Making `quantile_cols` required instead of defaulting to
    `_QUANTILE_COLS` means `postprocessing_tools.py` needs zero
    knowledge of quantile column names. No imports change.

12. **`postprocessing_tools.py` confirmed clean**: Exists, imports only
    stdlib + pandas, has zero intra-package imports. Current contents:
    `forecast_target_date()`, `TimingStats`, `timer()`,
    `log_most_recent_forecasts()`. Both `skill_metrics.py` and
    `ensemble_calculator.py` already import from it at module level.

13. **Short-term CRPS insertion points confirmed**: Individual models
    at lines 1702–1704 (after `test_for_tuples`, before EM block).
    EM at lines 1777–1780 (after `ensemble_skill_stats`, before
    `pd.concat`). Source DataFrames (`skill_metrics_df`,
    `ensemble_skill_metrics_df`) carry both `discharge_avg` and
    quantile columns. `round(4)` in `save_skill_metrics()` is safe
    for CRPS values.

14. **Short-term EM groupby key difference**: EM `ensemble_skill_stats`
    groups on `[period_col, "code", "model_short", "composition"]`
    (4 keys, not 3). The CRPS merge must use the same 4 keys to avoid
    fan-out — differs from long-term template which uses 3 keys.
