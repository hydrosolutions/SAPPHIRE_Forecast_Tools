# LT seasonal/quarterly hindcasts: `q` field null for LR models, blocking skill computation

**Status**: **Resolved (2026-05-29)** — no code change required; archived as superseded by current data state.
**Module**: long_term_forecasting
**Priority**: High (historical)
**Labels**: `bug`, `long-term-forecasting`, `skill-metrics`
**Assigned**: @sandrohuni

## Resolution note (2026-05-29)

A live audit of the running kghm postprocessing stack confirmed this issue no longer reproduces as written:

- **`q` is populated** for LR_Base, LR_SM, LR_SM_DT, and LR_SM_ROF on monthly, quarterly, and seasonal hindcast rows. `q`-null fraction is under 3% across these models (likely incidental missing-input rows, not the systemic absence the original problem statement described).
- **`q50` is still null** for nearly all non-MC long forecasts, but `apps/postprocessing_forecasts/src/skill_metrics.py:1090` resolves point forecasts as `q` first with `q50` as fallback. With `q` populated, skill calculation proceeds normally.
- For most models, `q50` is semantically equal to `q` (same point estimate), so the null `q50` carries no information loss and breaks no downstream consumer that uses `q`.
- Skill metrics for LR_Base and LR_SM are present in the DB for all three long-term horizons. Tajik deployment scope only requires seasonal forecasts for these two models, so the current emit set is sufficient.

What remains unresolved but is no longer high priority:
- **q50 backfill** — if any downstream consumer ever requires `q50` directly (rather than via the `q` → `q50` fallback), q50 should be backfilled to equal q for models where they are definitionally the same. Low priority; only actionable if a concrete consumer is identified.
- **Seasonal model coverage gap** — GBT, SM_GBT, MC_ALD, LR_SM_DT, LR_SM_ROF have no seasonal long_forecasts upstream. Out of scope for Tajik (only LR_Base + LR_SM needed). Track separately if other deployments need broader seasonal model coverage.

The remaining real concern surfaced by the audit is **dashboard quarter/season skill visibility** (`apps/forecast_dashboard/src/db.py:737–765` returns empty `forecast_stats` for quarter and season). That is tracked under a separate plan.

---

## Original plan (historical, retained for context)

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

### Root Cause (confirmed 2026-04-01)

Investigation traced the divergence to the upstream `lt_forecasting` library
(`LINEAR_REGRESSION.py`), specifically the difference between operational and
hindcast prediction paths:

**Operational path** (`predict_operational`, line ~498): A row is **always**
appended to `pred_df`, even when the prediction is NaN. So `Q_LR_Base` is
always present in the DataFrame (possibly as NaN, but the column exists).

**Hindcast path** (`calibrate_model_and_hindcast`, line ~671-688): Failed
predictions are **silently skipped** — `if not np.isnan(predictions[i])` gates
the row append. When a station/period combination fails the training-data
threshold check (`num_features * 2`), no record is emitted. In seasonal/
quarterly mode, early years with insufficient training data fail this check,
so:
- `Q_LR_Base` is absent or NaN for those years
- The fallback chain in `prepare_long_forecast_records()` finds nothing
  (`Q_{model}` → `Q50` → `Q_loc` all fail)
- `q` is never set → written as `None` to the API

**Call chain**:
```
calibrate_and_hindcast.py
  → model.calibrate_model_and_hindcast()   # upstream library
    → skips rows where prediction=NaN      # ← root cause
  → post_process_lt_forecast()
  → save_forecast() → save_forecast_to_db()
    → prepare_long_forecast_records()       # q fallback chain fails
```

**Operational forecasts are NOT affected** — the daily scheduled run always
produces a real `Q_LR_Base` value. The bug only manifests during yearly
recalibration (`calibrate_and_hindcast.py`) for seasonal/quarterly modes.

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

Two complementary fixes, in separate repos:

**Fix A (upstream library — @sandrohuni):** In `LINEAR_REGRESSION.py`,
investigate why seasonal/quarterly hindcasts produce NaN for `Q_LR_Base`
in early years. The rows are emitted (flag=3 set at
`calibrate_and_hindcast.py:241`) but the Q value is NaN. The root cause
is likely a training data threshold check (`num_features * 2`) that
rejects early years with insufficient data. Consider:
- Lowering the threshold for seasonal/quarterly modes
- Or using a simpler fallback model for years with insufficient data

This is a model quality improvement, not strictly a bug fix.

**Fix B (this repo — fallback guard):** In `lt_utils.py:354-362`, add a
final fallback that derives `q` from quantiles when all three existing
branches fail. The quantiles (Q5-Q95) ARE populated even when
`Q_{model_name}` is NaN (verified: lines 364-367 write quantiles
independently). Using `(Q25 + Q75) / 2` as a pragmatic point estimate:
```python
# Fallback: derive q from quantiles when point forecast is missing
elif "Q25" in row.index and "Q75" in row.index:
    q25 = row.get("Q25")
    q75 = row.get("Q75")
    if pd.notna(q25) and pd.notna(q75):
        record["q"] = float((q25 + q75) / 2)
```

Note: `(Q25 + Q75) / 2` is the IQR midpoint, not the true median (Q50
is not populated). For symmetric distributions this approximates the
median. For skill metrics (NSE, MAE) this is acceptable — the quantile
forecast already exists, we just need a representative point value.

**Fix B is independently actionable** in this repo. Fix A is a model
improvement for the upstream library. Fix B alone unblocks seasonal/
quarterly skill metrics for all LR models.

### Files to Modify

| File | Repo | Owner | Change |
|------|------|-------|--------|
| `lt_forecasting/.../LINEAR_REGRESSION.py:671-688` | `hydrosolutions/long-term-forecasting` | @sandrohuni | Fix A: always emit hindcast row |
| `apps/long_term_forecasting/lt_utils.py:354-362` | this repo | @mabesa | Fix B: quantile fallback guard |

### Implementation Steps

- [ ] Step 1 (@mabesa): Add quantile fallback in `prepare_long_forecast_records()` after the existing Q_loc branch — this is Fix B, independently actionable
- [ ] Step 2: Add unit test for the fallback in `apps/long_term_forecasting/tests/`
- [ ] Step 3: Re-run `calibrate_long_term` to regenerate seasonal/quarterly hindcasts with backfilled q values
- [ ] Step 4: Re-run `recalculate_skill_metrics` with `SAPPHIRE_PREDICTION_MODE=ALL` and verify n_pairs > 0
- [ ] Step 5 (separate, @sandrohuni): Investigate upstream model threshold for seasonal/quarterly — Fix A

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
