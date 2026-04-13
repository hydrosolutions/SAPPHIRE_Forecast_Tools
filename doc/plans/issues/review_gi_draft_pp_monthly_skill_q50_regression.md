# PP-028b: Skill metrics crash/silent failure — missing `q50` column across all horizons

**Status**: Review (implemented 2026-04-13, commits 90fd2a7 + 86b8640)
**Module**: postprocessing_forecasts
**Priority**: High
**Labels**: `bug`, `regression`, `skill-metrics`, `long-term-forecasting`
**Related**: `archive/mid_prio_gi_draft_pp_skill_metrics_broken.md` (Bug 3)

---

## Summary

Long-term skill metrics computation fails when the `q50` column is
absent from the forecast DataFrame. The `q50` column is stripped by
`dropna(axis=1, how="all")` in the data reader before it reaches the
skill metrics code. Three code paths are affected:

| Path | Function | Line | Symptom |
|------|----------|------|---------|
| Monthly | `calculate_monthly_skill_metrics` | 1156 | **Crash**: `KeyError: 'q50'` |
| Quarterly | `_calculate_aggregated_skill_metrics` | 2123 | **Silent failure**: `forecasted_discharge` never created, `KeyError` at groupby (line 2131) or zero metrics |
| Seasonal | `_calculate_aggregated_skill_metrics` | 2123 | Same as quarterly |

The quarterly/seasonal path guards `q50` but does **not** fall back to
`q`, so when `q50` is absent and `forecasted_discharge` doesn't already
exist, the column is never created.

## Root Cause (verified)

**Two-layer failure:**

### Layer 1 — Data reader strips all-null columns (`data_reader.py:1082`)

```python
all_records = [df.dropna(axis=1, how="all") for df in all_records if not df.empty]
```

When a batch of API records has `q50=None` for every row (true for GBT,
SM_GBT, SM_GBT_Norm, SM_GBT_LR — all non-LR models), this drops the
`q50` column from that batch. After `pd.concat`, the resulting
DataFrame may have `q50` for some rows (where LR_Base had it) but if
all batches drop it, the column is absent entirely.

This pattern appears 4 times in `data_reader.py`:
- Line 914 (short-term forecasts reader)
- Line 1082 (`_read_long_forecasts_api` — the one hitting this bug)
- Line 1269 (latest monthly reader)
- Line 1287 (alternative long reader)

### Layer 2 — Four locations resolve `forecasted_discharge` inconsistently

| # | Location | File:Line | Priority | Guarded? | Symptom |
|---|----------|-----------|----------|----------|---------|
| A | Monthly skill metrics | `skill_metrics.py:1156` | q50 first | **No** | **Crash**: `KeyError: 'q50'` |
| B | Quarterly/seasonal skill metrics | `skill_metrics.py:2123` | q50 only | Partial (no `q` fallback) | **Silent failure**: `forecasted_discharge` never created |
| C | Monthly→quarterly aggregation | `aggregation.py:274` | q50 only | Partial (no `q` fallback) | **Silent failure**: quarterly forecasts have no `forecasted_discharge` |
| D | Direct quarterly/seasonal normalize | `data_reader.py:2983` | q first | **Yes** | Correct |

Location D is the only one that handles `q`-first correctly. Locations
A, B, C all assume `q50` is present.

**Location C is upstream of Location B**: `read_quarterly_forecasts`
aggregates monthly data via `aggregate_monthly_fc_to_quarterly`
(Location C), which may already set `forecasted_discharge` from `q50`.
If `q50` is absent, Location C silently skips — then Location B also
skips — and the skill function crashes at the groupby (line 2131).

However, `read_quarterly_forecasts` also merges in direct quarterly
API records via `_normalize_combined_forecasts` (Location D), which
does handle `q` correctly. So the quarterly path **may** work if
direct quarterly records exist and have `q` set. It fails when only
the aggregated-from-monthly path is used.

### Why the existing tests don't catch this

- `test_q_fallback_produces_nonzero_npairs` (line 1303) constructs the
  forecast DataFrame with `"q50": [np.nan] * 4` — the column **exists**
  with NaN values. In production, `dropna(axis=1, how="all")` removes
  the column entirely, so it's absent from `merged.columns`.
- `_make_quarterly_fcst` and `_make_seasonal_fcst` always include `q50`.
- No test passes data through the data reader → skill metrics full path.

## API Data Context

Long-forecast records for GBT-family models have `q50=null` while
`q` is populated:

```json
{"model_type": "GBT", "q": 2.75, "q50": null, "q05": null, ...}
```

LR-family models have quantiles but `q50=null`:

```json
{"model_type": "LR_Base", "q": 1.73, "q50": null, "q05": 1.39, ...}
```

Note: **no model currently populates `q50`**. This is also tracked in
`high_prio_gi_draft_ltf_seasonal_quarterly_q_null.md` for the upstream
write side.

## Reproduction

```bash
ieasyhydroforecast_env_file_path=<path> \
  bash apps/run_locally.sh long-term-operational
```

Crashes in the "Recalculate: long-term skill metrics (MONTHLY)" phase.

## Error

```
File "src/skill_metrics.py", line 1157, in calculate_monthly_skill_metrics
    merged["forecasted_discharge"] = merged["q50"].fillna(merged["q"]).astype(float)
                                     ~~~~~~^^^^^^^
KeyError: 'q50'
```

## Semantic Note: `q` vs `q50` priority

The upstream writer (`lt_utils.py:354-362`) sets:
- **`q`** = authoritative point forecast (`Q_{model_name}`, fallback
  `Q50`, fallback `Q_loc`) — always the intended comparison target
- **`q50`** = 50th percentile quantile from the full distribution —
  **no current model populates this field**

The existing code at line 1157 has priority backwards: `q50.fillna(q)`.
This was safe only because MC_ALD (now deprecated) was the only model
that set `q50`, and it set `q50 == q`. For all other models `q50` is
null, so `fillna(q)` always resolves to `q` anyway.

**The correct priority is `q` first, `q50` as fallback.** This matches
the upstream write logic and ensures that if a future model writes both
with different values, the authoritative point forecast is used.

The existing test `test_q50_preferred_over_q_when_both_present` asserts
the opposite (q50 preferred). This test must be updated to match the
corrected priority.

## Proposed Fix

### Fix 1 — Extract shared helper (`skill_metrics.py`)

Extract the point-forecast resolution into a helper to avoid the
duplication that caused this regression. Place near the top of the
module with the other helpers:

```python
def _resolve_forecasted_discharge(df: pd.DataFrame) -> pd.Series | None:
    """Resolve the point forecast column from q or q50.

    Priority: q first (authoritative point forecast from the model),
    q50 as fallback (median quantile, rarely populated).

    Returns None if neither column is available.
    """
    has_q = "q" in df.columns
    has_q50 = "q50" in df.columns
    if has_q and has_q50:
        return df["q"].fillna(df["q50"]).astype(float)
    elif has_q:
        return df["q"].astype(float)
    elif has_q50:
        return df["q50"].astype(float)
    return None
```

### Fix 2 — Location A: Monthly path (`skill_metrics.py:1156-1159`)

Replace the unguarded access:

```python
fc = _resolve_forecasted_discharge(merged)
if fc is None:
    logger.warning("No q or q50 column — cannot compute monthly skill metrics")
    return empty_stats, empty_joint, timing_stats
merged["forecasted_discharge"] = fc
```

### Fix 3 — Location B: Quarterly/seasonal path (`skill_metrics.py:2123-2124`)

Replace the incomplete guard:

```python
if "forecasted_discharge" not in merged.columns:
    fc = _resolve_forecasted_discharge(merged)
    if fc is None:
        logger.warning("No q or q50 column — cannot compute skill metrics")
        return empty_stats, empty_joint, timing_stats
    merged["forecasted_discharge"] = fc
```

### Fix 4 — Location C: Monthly→quarterly aggregation (`aggregation.py:240-275`)

The `agg_dict` (line 240-247) only aggregates `_FC_QUANTILE_COLS`
(q05-q95) and optionally `forecasted_discharge`. The `q` column from
the monthly input is **dropped** by the groupby because it's not in
`agg_dict`. So checking `"q" in grouped.columns` after the groupby
would never match.

Fix: add `q` to the aggregation dict so it survives the groupby, then
use it as fallback:

```python
# After line 247, add:
if "q" in df.columns:
    agg_dict["q"] = ("q", "mean")
```

Then replace lines 273-275:

```python
# Ensure forecasted_discharge exists (q first, q50 fallback)
if "forecasted_discharge" not in grouped.columns:
    if "q" in grouped.columns:
        grouped["forecasted_discharge"] = pd.to_numeric(
            grouped["q"], errors="coerce"
        )
    elif "q50" in grouped.columns:
        grouped["forecasted_discharge"] = grouped["q50"].astype(float)
```

Note: `aggregation.py` does not import from `skill_metrics.py`, so do
NOT use the helper here. Keep it self-contained to avoid circular
imports.

### Fix 5 — Update existing tests, add new tests

**`test_monthly_skill_metrics.py`**:
- **Update** `test_q50_preferred_over_q_when_both_present` — change to
  assert `q` is preferred over `q50` (matching corrected priority).
  Rename to `test_q_preferred_over_q50_when_both_present`.
- **Update** comment on line 1521 in `test_partial_q50_fill_within_single_model`:
  change "q50 used where available, q elsewhere" to
  "q used where available, q50 as fallback". (No assertion change needed —
  test data has `q == q50` where both present, so results are identical.)
- **Add** test with **no `q50` column at all** and `q` populated
  (monthly path).

**`test_quarterly_skill_metrics.py`**:
- **Add** test with no `q50` column, `q` populated
  (quarterly/seasonal path via `_calculate_aggregated_skill_metrics`).

### Fix 6 (optional, not in scope) — Remove `dropna(axis=1, how="all")` from data reader

The `dropna(axis=1, how="all")` in `data_reader.py` is a footgun —
it silently strips semantically meaningful columns. Consider removing
it or replacing it with an explicit column selection. This is a broader
change that affects 4 call sites and should be evaluated separately.

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Changing `q50`-first to `q`-first priority changes MAE/NSE values for MC_ALD | MC_ALD is deprecated and set `q == q50`; no numeric change |
| Existing test `test_q50_preferred_over_q_when_both_present` will fail | Intentional — update test to match corrected semantics |
| EM ensemble averages `forecasted_discharge` (lines 1243/2191) | Unaffected — same column, just resolved from `q` instead of `q50` |
| EM ensemble also averages quantile cols including `q50` (lines 1246/2196) | Already guarded by `if qcol in columns`; safe when `q50` absent |
| Pentad/decad path (`calculate_skill_metrics`, line 1687) | Unaffected — expects `forecasted_discharge` pre-set by caller; never touches `q`/`q50` |
| CRPS computation uses `_QUANTILE_COLS` including `q50` (line 2150) | Already guarded by `if len(qf_cols) == len(_QUANTILE_COLS)` — gracefully returns NaN when incomplete |
| `dropna(axis=1, how="all")` may also strip `q` column | Only if `q` is all-null; no current model writes null `q`. The helper handles absent `q` gracefully regardless |
| Location D (`data_reader.py:2983`) already correct | No change needed — verified `q`-first priority already in place |
| Circular import from aggregation.py using skill_metrics helper | Avoided: Fix 4 is self-contained, does not use the helper |
| Adding `q` to aggregation `agg_dict` averages `q` across months | Semantically equivalent to existing `forecasted_discharge` averaging (line 247). The aggregated `q` column is dropped by `_QUARTERLY_FC_COLS` filtering anyway — it only serves as fallback source for `forecasted_discharge` |
| `q` column may have NaN for some months (no model currently writes null `q`, but defensive) | `pd.to_numeric(..., errors="coerce")` handles gracefully; `mean()` on mixed NaN/valid ignores NaN by default |
| Priority flip recovers pairs that were previously lost | Current `q50.fillna(q)` discards rows where q50=NaN even when q has a value (because NaN.fillna(value)=value but the original q50 was used). New `q.fillna(q50)` keeps these rows. This is a net improvement — more valid forecast-observation pairs |
| Line 2123 guard rarely triggers in practice | `forecasted_discharge` is usually pre-set by data reader. The guard is defensive. Fix makes it consistent with `_normalize_combined_forecasts` (data_reader.py:2983) which already uses q-first |

## Files to Modify

| File | Change |
|------|--------|
| `apps/postprocessing_forecasts/src/skill_metrics.py` | Add `_resolve_forecasted_discharge` helper; use at lines 1156 and 2123 |
| `apps/postprocessing_forecasts/src/aggregation.py` | Add `q` fallback at line 274 (self-contained, no helper import) |
| `apps/postprocessing_forecasts/tests/test_monthly_skill_metrics.py` | Update priority test; add absent-`q50` test for monthly path |
| `apps/postprocessing_forecasts/tests/test_quarterly_skill_metrics.py` | Add absent-`q50` test for quarterly/seasonal path |

## Acceptance Criteria

- [ ] `long-term-operational` completes without crash including skill
      metrics recalculation (monthly, quarterly, seasonal)
- [ ] Monthly skill metrics have non-zero `n_pairs` for models where
      `q` is populated (GBT, LR_Base, etc.)
- [ ] Quarterly/seasonal skill metrics don't silently return zero
      metrics when only `q` is available
- [ ] New test (monthly): forecast DataFrame without `q50` column,
      `q` populated -> produces valid skill metrics
- [ ] New test (quarterly): same scenario through aggregated path
- [ ] Updated test: `q` preferred over `q50` when both present
- [ ] Existing tests still pass (no regression in pentad/decad)
- [ ] EM ensemble computation unaffected (verify ensemble MAE unchanged)

## Observed During

Local pipeline review 2026-04-13, Section 6 (`long-term-operational`).
