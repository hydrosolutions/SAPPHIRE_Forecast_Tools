# LT seasonal/quarterly hindcasts: `q` field null for LR models, blocking skill computation

**Status**: Draft
**Module**: long_term_forecasting
**Priority**: High
**Labels**: `bug`, `long-term-forecasting`, `skill-metrics`

---

## Summary

LR_Base and LR_SM seasonal/quarterly hindcast records have `q=None` and `q50=None` despite having valid quantile values (q05-q95), which prevents skill metric computation for these models.

## Context

Long-term forecasting produces monthly, quarterly, and seasonal forecasts. The skill recalculation step computes NSE and other metrics by comparing `q` (point forecast) against observed values across historical hindcasts. If `q` is null, the pair is skipped and n_pairs=0.

MC_ALD is the only model currently showing seasonal/quarterly skill because it sets both `q` and `q50`. MC_ALD is being phased out, making LR_Base and LR_SM the primary models for these horizons.

## Problem

**Observed during**: Local pipeline review checklist (`review_checklist_local_2026-03-28.md`), section 6.2.

The seasonal/quarterly hindcast records for LR_Base and LR_SM (and LR_SM_DT, LR_SM_ROF) have:
- `q05`, `q10`, `q25`, `q75`, `q90`, `q95` — **filled** (valid quantile forecasts)
- `q50` — **None**
- `q` — **None** (except the 2026-03-25 operational forecast which has `q` filled)

Example (S1, SEASON, LR_Base, 2006-04-01):
```
q=None  q05=6.828  q10=7.255  q25=7.972  q50=None  q75=9.572  q90=10.29  q95=10.72
```

By contrast, MC_ALD has both `q` and `q50` filled for all years:
```
q=11.867  q05=8.673  q10=9.143  q25=10.309  q50=11.867  q75=13.425  q90=14.778  q95=15.495
```

**Impact**: Seasonal and quarterly skill metrics show `n_pairs=0, nse=None` for all LR models across both test stations (15189, 16059). Only MC_ALD (being deprecated) has computed skill.

**Scale**: Affects all stations, both SEASON and QUARTER horizon types.

## Desired Outcome

- All LR model seasonal/quarterly hindcast records have `q` populated
- Skill metrics compute valid n_pairs and NSE for LR_Base and LR_SM seasonal/quarterly
- `q50` should also be populated if the quantile distribution is symmetric (i.e., `q50 = median`)

---

## Technical Analysis

### Current Implementation

**Key file**: `apps/long_term_forecasting/lt_utils.py:354-362`

```python
# Main model output: Q_{model_name} -> q
if q_model_col in row.index and pd.notna(row.get(q_model_col)):
    record["q"] = float(row[q_model_col])
# Fallback to Q50 for uncertainty models
elif "Q50" in row.index and pd.notna(row.get("Q50")):
    record["q"] = float(row["Q50"])
# Fallback to Q_loc
elif "Q_loc" in row.index and pd.notna(row.get("Q_loc")):
    record["q"] = float(row["Q_loc"])
```

The logic looks for `Q_{model_name}` column first, then falls back to `Q50`, then `Q_loc`. For LR_Base seasonal hindcasts:
1. `Q_LR_Base` — either not present in the DataFrame, or is NaN
2. `Q50` — None (not computed by the LR model for seasonal)
3. `Q_loc` — not applicable

So `q` is never set.

### Root Cause

Two possibilities to investigate:

1. **The LR seasonal/quarterly forecast code doesn't produce a `Q_{model_name}` column** in the hindcast DataFrame — it only produces quantile columns (Q5-Q95) via uncertainty estimation but doesn't set the point forecast.

2. **The LR model does produce `Q_{model_name}` for the operational forecast** (2026-03-25 has q=10.19) but **not during hindcast mode** — different code paths for operational vs hindcast.

The fact that operational (2026-03-25) has `q` filled but hindcasts (2006-2025) don't suggests the hindcast code path is missing the point forecast assignment.

### DB state summary (S1, 15189)

| horizon | model | q filled | q null | q50 filled | q50 null | quantiles filled |
|---------|-------|----------|--------|------------|----------|-----------------|
| SEASON | LR_Base | 1 (operational) | 21 (hindcasts) | 0 | 22 | 22 |
| SEASON | LR_SM | 1 (operational) | 21 (hindcasts) | 0 | 22 | 22 |
| SEASON | MC_ALD | 21 | 0 | 21 | 0 | 21 |
| QUARTER | LR_Base | 2 (operational) | 24 (hindcasts) | 0 | 26 | 7 |
| QUARTER | LR_SM | 2 (operational) | 23 (hindcasts) | 0 | 25 | 6 |
| MONTH | LR_Base | 198 | 0 | 0 | 198 | — |

Note: Monthly LR_Base has `q` filled for all 198 records — the bug is specific to seasonal/quarterly.

---

## Implementation Plan

### Approach

Investigate in two steps:

1. **Trace the hindcast DataFrame** for LR_Base seasonal mode to determine whether `Q_LR_Base` column exists and what value it contains
2. **Fix**: Either ensure the hindcast code produces `Q_{model_name}`, or add a fallback in `prepare_long_forecast_records` that computes `q` from the median of quantiles when all else is null

### Files to Investigate

| File | Purpose |
|------|---------|
| `apps/long_term_forecasting/lt_utils.py:269-377` | `prepare_long_forecast_records()` — where q is set |
| `apps/long_term_forecasting/run_forecast.py` | Operational forecast entry point |
| `apps/long_term_forecasting/` (model code) | Where hindcast DataFrames are constructed |

### Implementation Steps

- [ ] Step 1: Add debug logging to `prepare_long_forecast_records()` to dump column names and first row for seasonal LR_Base
- [ ] Step 2: Run `seasonal_march` mode and inspect logs to find which columns exist
- [ ] Step 3: Identify why `Q_LR_Base` is missing/NaN in hindcast but present in operational
- [ ] Step 4: Fix the root cause (either in model code or in the record preparation)
- [ ] Step 5: Consider adding a fallback: `q = (q25 + q75) / 2` or `q = mean(q05..q95)` when q is still None but quantiles exist
- [ ] Step 6: Re-run hindcasts to backfill the null q values
- [ ] Step 7: Re-run skill metric recalculation and verify n_pairs > 0

---

## Testing

### Manual Verification

```bash
# After fix, check that q is populated for seasonal hindcasts
curl -s "http://localhost:8000/api/postprocessing/long-forecast/?code=15189&horizon_type=SEASON&limit=50" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
lr = [r for r in d if r['model_type'] == 'LR_Base']
q_null = sum(1 for r in lr if r.get('q') is None)
q_filled = sum(1 for r in lr if r.get('q') is not None)
print(f'LR_Base SEASON: {q_filled} filled, {q_null} null (expect 0 null)')
"

# After skill recalculation, check n_pairs > 0
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?code=15189&horizon=SEASON&limit=50" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
for r in sorted(d, key=lambda x: x['model_type']):
    if r['model_type'] in ('LR_Base', 'LR_SM'):
        print(f'{r[\"model_type\"]}: n_pairs={r[\"n_pairs\"]}, nse={r[\"nse\"]}')
"
```

---

## Documentation Impact

- [ ] No documentation impact — this is a data/code bug fix

## Out of Scope

- Monthly forecasts (LR_Base monthly already has q filled — not affected)
- GBT/SM_GBT models (separate issue — they have no quantiles at all for seasonal)
- MC_ALD deprecation (separate concern)

## Dependencies

- None — can be investigated independently

## Acceptance Criteria

- [ ] All LR_Base and LR_SM seasonal hindcast records have non-null `q`
- [ ] All LR_Base and LR_SM quarterly hindcast records have non-null `q`
- [ ] Seasonal skill metrics for LR_Base and LR_SM show n_pairs >= 15 and numeric NSE
- [ ] Quarterly skill metrics for LR_Base and LR_SM show n_pairs >= 15 and numeric NSE
- [ ] Existing monthly skill metrics are unchanged (regression check)

---

## References

- Discovered: `review_checklist_local_2026-03-28.md`, section 6.2
- Key code: `apps/long_term_forecasting/lt_utils.py:354-362`
- Related observation: `doc/plans/observations.md` (2026-03-27, skill metrics)
