
# PP-030: Fix EM skill metric degradation in recalculate_skill_metrics.py (boundary-pentad n_pairs=1-2)

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: Medium
**Labels**: `bug`, `postprocessing`, `skill-metrics`

---

## Summary

`recalculate_skill_metrics.py` re-derives the EM ensemble from individual model forecasts using raw "day"-horizon API records. At period-boundary pentads (year-end, month-end transitions), TFT/TiDE/TSMixer normalization produces different issue dates from LR, so the `groupby` that builds EM pairs finds no multi-model overlap → n_pairs=1-2 → wildly negative NSE (observed: S1 decade EM NSE=-134.53, S1 pentad EM NSE=-5.71).

The operational daily run avoids this because it reads pre-aggregated pentad CSV data where all issue dates are already aligned. The fix is to make the recalculation read combined forecast records (EM, NE) directly from the postprocessing API instead of re-deriving them from individual models.

## Context

Two skill metric "generations" coexist in the DB:

| Generation | Date convention | n_pairs (EM) | Origin |
|---|---|---|---|
| OLD (operational / migration) | LR issue date = last day of previous pentad | 14–17 | `postprocessing_forecasts.py` daily run, reads pentad CSV |
| NEW (recalculation) | `get_date_for_pentad(p, year)` = first day of target pentad | 1–2 (boundaries), 14–17 (mid-period) | `recalculate_skill_metrics.py`, reads raw day-horizon API |

The upsert key is `(horizon_type, code, model_type, date, horizon_in_year)`. The two date conventions produce different keys → both records coexist. The OLD records are good. The NEW records are bad for boundary pentads only.

This is related to but distinct from the INFRA-015 boundary-date convention (already documented). The actionable bug is that the recalculation path re-derives EM incorrectly at boundaries.

## Problem

In `recalculate_skill_metrics.py` → `_run_short_term_recalc()` → `data_reader.read_observed_and_modelled_data()`:

1. TFT/TiDE/TSMixer forecasts are read as raw "day"-horizon records from the API.
2. `_normalize_ml_forecasts()` aggregates them to pentad level, producing one issue date per pentad.
3. For period-boundary pentads (especially pentad 1, 72, and month-end pentads 6, 13, 19, 25, 31, etc.), the normalization maps to a **different** issue date than LR uses.
4. `calculate_skill_metrics()` builds EM by grouping on `[period_col, date, code]`. At boundaries, no LR row has the same `date` as the normalized ML rows → no multi-model group → only 1-2 pairs when accidentally overlapping.
5. This produces EM NSE values like −134.53 (1 real pair, essentially noise).

**S2 EM absence (separate, expected):** LR `sdivsigma` > 0.4 threshold for S2 → LR excluded → single model → EM skipped by Gate C. This is correct behavior; the gate thresholds are per the `.env` config.

## Desired Outcome

- `recalculate_skill_metrics.py` produces EM skill metrics with n_pairs consistent with the operational run (i.e., 14-17 for well-covered pentads, not 1-2 at boundaries).
- The DB no longer accumulates "bad" NEW-generation EM skill records after each yearly recalculation.
- S2 EM absence remains (expected: LR below skill threshold for S2).

---

## Technical Analysis

### Current Implementation

**`recalculate_skill_metrics.py:117–157`** — `_run_short_term_recalc()` reads raw individual model forecasts from the API and passes them to `calculate_skill_metrics()`, which re-derives EM internally.

**`apps/postprocessing_forecasts/src/skill_metrics.py:1653–1979`** — `calculate_skill_metrics()` builds EM by grouping qualifying model forecasts. The `groupby([period_col, date, code])` step requires all models to have the same `date` for the same pentad.

**`apps/postprocessing_forecasts/src/data_reader.py:1729–1952`** — `_read_ml_forecasts_pp_api()` reads TFT/TiDE/TSMixer as "day" horizon and normalizes; `_normalize_ml_forecasts()` maps daily records to pentad issue dates, which can produce different dates than LR at boundaries.

### Root Cause

The EM derivation in the recalculation path requires that LR and ML records share the same `date` column value for the same pentad. The normalization of raw daily ML records does not guarantee this for all boundary pentads.

### Approaches Considered

**Option A — Align normalization date convention with LR issue date (the INFRA-015 convention)**
- Modify `_normalize_ml_forecasts()` to compute the same issue date as LR (last day of previous pentad) for all pentads.
- Pro: fixes the root cause consistently for all callers.
- Con: complex to get right for all boundary cases; risks breaking other callers.

**Option B — Read combined EM/NE records directly from the API for skill recalculation**
- Instead of re-deriving EM, read the operational EM records (written by the daily run) directly from the postprocessing API.
- Merge with observed discharge and compute skill metrics.
- Pro: uses the same records that were actually used in operation; avoids re-derivation entirely.
- Con: requires EM records to exist in the DB (they do since at least 2024); S2 EM absence is preserved correctly.

**Option C — Filter out recalculation skill records for EM; rely solely on the operational generation**
- In the recalculation, skip EM skill metric computation entirely; only compute LR/TFT/TiDE/TSMixer/NE.
- Pro: zero risk of creating bad EM records; simple to implement.
- Con: EM skill metrics would not be refreshed during yearly recalculation; stale if historical EM records change.

### Recommended Approach

**Option C** (skip EM in recalculation, rely on operational records) is the safest short-term fix.
**Option B** (read EM from API for recalculation) is the correct long-term fix.

Implement Option C first (1 line change: skip EM in recalculation), then track Option B as a follow-up.

---

## Implementation Plan

### Phase 1: Option C — Skip EM derivation in recalculate_skill_metrics.py (short-term fix)

### Files to Modify

| File | Changes |
|------|---------|
| `apps/postprocessing_forecasts/src/skill_metrics.py` | Add an `exclude_models` parameter to `calculate_skill_metrics()` that skips specified model derivations |
| `apps/postprocessing_forecasts/recalculate_skill_metrics.py` | Pass `exclude_models=["EM"]` when calling `_run_short_term_recalc` (or equivalently, skip the EM write step) |

### Implementation Steps

- [ ] In `calculate_skill_metrics()` (`skill_metrics.py`), find where EM rows are derived (the `groupby` + `filter_for_highly_skilled_forecasts` block). Add a guard: if `"EM"` is in an `exclude_models` parameter (default `[]`), skip building and writing EM skill metrics.
- [ ] In `recalculate_skill_metrics.py`, pass `exclude_models=["EM"]` for pentad and decad recalculation modes.
- [ ] Add a log message: `"Skipping EM skill metric recalculation (excluded by config); operational EM metrics retained."`
- [ ] Verify: after fix, a recalculation run no longer writes new EM skill metric records with the 2026-date convention. Existing 2025-date EM records remain.
- [ ] Run tests: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`

### Code Example

```python
# In calculate_skill_metrics() signature:
def calculate_skill_metrics(
    config: ShortTermHorizonConfig,
    observed: pd.DataFrame,
    modelled: pd.DataFrame,
    timing_stats: TimingStats,
    exclude_models: list[str] | None = None,  # NEW
) -> tuple[pd.DataFrame, pd.DataFrame, TimingStats]:
    ...
    exclude_models = exclude_models or []
    ...
    # Before EM derivation block:
    if "EM" in exclude_models:
        logger.info(
            "Skipping EM ensemble derivation (excluded). "
            "Operational EM skill metrics are retained in DB."
        )
    else:
        # existing EM derivation code
        ...
```

```python
# In recalculate_skill_metrics.py, _run_short_term_recalc():
skill_metrics_result, modelled, returned_timing_stats = (
    skill_metrics.calculate_skill_metrics(
        config, observed, modelled, timing_stats_,
        exclude_models=["EM"],   # PP-030: skip EM re-derivation; see issue
    )
)
```

---

## Testing

### Test Cases

- [ ] Unit: `calculate_skill_metrics()` with `exclude_models=["EM"]` does not produce EM rows in output
- [ ] Unit: `calculate_skill_metrics()` with `exclude_models=[]` (default) still produces EM rows as before
- [ ] Integration: `_run_short_term_recalc` with `exclude_models=["EM"]` does not write new EM skill metrics to the API
- [ ] Regression: all existing skill metric tests pass

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

### Manual Verification

After fix, run a local recalculation:
```bash
cd apps/postprocessing_forecasts
SAPPHIRE_PREDICTION_MODE=BOTH SAPPHIRE_SKILL_METRICS_YEAR=2026 \
  ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
  uv run python recalculate_skill_metrics.py
```

Then query:
```bash
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?code=15189&horizon=pentad&model=EM&limit=200" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); \
    bad=[r for r in d if r.get('n_pairs',99) <= 2]; \
    print(f'{len(d)} total EM records, {len(bad)} with n_pairs<=2')"
```

Expected: count of n_pairs<=2 records does NOT increase from the pre-fix baseline.

---

## Documentation Impact

- [ ] No documentation impact — internal recalculation logic change; no user-visible behavior change.

---

## Out of Scope

- Option B (reading EM records from API for recalculation) — tracked as a follow-up.
- S2 EM absence — expected behavior; LR below skill threshold.
- Cleaning up the existing bad NEW-generation EM skill records in the DB — a separate DB migration task.
- Changes to the operational daily run skill metric logic.

## Dependencies

None. INFRA-015 (boundary date convention) is already closed.

## Acceptance Criteria

- [ ] `recalculate_skill_metrics.py` no longer produces EM skill metric records with n_pairs ≤ 2
- [ ] All existing EM skill metric records (OLD generation, n_pairs=14-17) are untouched
- [ ] `calculate_skill_metrics()` signature is backward-compatible (`exclude_models` defaults to `[]`)
- [ ] All existing tests pass — zero failures, zero unexpected skips
- [ ] New test confirms EM is skipped when `exclude_models=["EM"]`
- [ ] Log message confirms skip when EM is excluded

---

## References

- INFRA-015 boundary date convention: commit `91c56f0` (closed)
- PP-027: EM ensemble silent skip observability
- Pipeline log: `apps/logs/run_locally_20260327_170642.log`
- Recalculation script: `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
- Skill metrics calculation: `apps/postprocessing_forecasts/src/skill_metrics.py:1653–1979`
- Data reader: `apps/postprocessing_forecasts/src/data_reader.py:1729–1952`
