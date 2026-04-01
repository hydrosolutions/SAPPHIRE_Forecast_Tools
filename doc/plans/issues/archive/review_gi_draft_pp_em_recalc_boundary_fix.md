
# PP-030: Fix EM skill metric degradation in recalculate_skill_metrics.py (boundary-pentad n_pairs=1-2)

**Status**: Review
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
| `apps/postprocessing_forecasts/src/skill_metrics.py` | Add `exclude_models` parameter to `calculate_skill_metrics()`; guard the EM-specific block at line 1868 |
| `apps/postprocessing_forecasts/recalculate_skill_metrics.py` | Pass `exclude_models=["EM"]` at line 133 |

### Critical: Guard placement

The `with timer(...)` block at lines 1800-1981 contains three distinct sections:

| Lines | Section | Skip when EM excluded? |
|-------|---------|------------------------|
| 1804-1835 | Filter highly-skilled models, build `em_agg_dict`, prep quantile cols | No — harmless setup, no side effects |
| 1836-1866 | **Individual model CRPS/PIT/sharpness** (runs on ALL models, not just EM) | **No — must NOT skip** |
| 1868-1979 | EM groupby, EM skill metrics, EM CRPS, EM merge into `joint_forecasts` | **Yes — all EM-specific** |

The guard goes at **line 1868** — after the individual model CRPS loop ends and before the EM `groupby` starts. The guard wraps lines 1868-1979 (including the inner `else: joint_forecasts = simulated.copy()` at line 1978). The new outer `else` branch logs the skip and sets `joint_forecasts = simulated.copy()`.

### Implementation Steps

- [x] Add `exclude_models: list[str] | None = None` to `calculate_skill_metrics()` signature (line 1692). Normalize to `exclude_models = exclude_models or []` at line 1711.
- [x] At line 1875, after the individual model CRPS merge ends, insert a guard: `if "EM" not in exclude_models:` wrapping lines 1875-1996 (EM groupby through the inner `else: joint_forecasts = simulated.copy()`). The new outer `else` branch logs the skip and sets `joint_forecasts = simulated.copy()`.
- [x] In `recalculate_skill_metrics.py` line 133, pass `exclude_models=["EM"]`.
- [x] Run tests: 1243 passed, 2 failed (pre-existing `test_file_writer.py` import error, unrelated to PP-030).

### What is NOT touched

- Lines 1836-1866 (individual model CRPS/PIT/sharpness) — completely outside the guard
- Lines 1804-1835 (highly-skilled filter, agg_dict setup) — runs unconditionally (harmless when EM is later skipped)
- NE exclusion at line 1818-1820 — upstream of the guard, unaffected
- The operational daily run (`postprocessing_operational.py`) — does NOT call `calculate_skill_metrics()` at all; it reads pre-calculated skill metrics via `data_reader.read_skill_metrics()` and uses `ensemble_calculator.create_ensemble_forecasts()`. Unaffected.
- The legacy/deprecated callers (`postprocessing_forecasts.py` lines 135, 175-178) — pass no `exclude_models`, so default `[]` preserves existing behavior

### Callers of `calculate_skill_metrics`

| Caller | File | Passes `exclude_models`? | Effect |
|--------|------|--------------------------|--------|
| Legacy pentad (deprecated) | `postprocessing_forecasts.py:135` | No (default `[]`) | EM derived as before |
| Legacy decad (deprecated) | `postprocessing_forecasts.py:175-178` | No (default `[]`) | EM derived as before |
| Recalculation | `recalculate_skill_metrics.py:133` | `["EM"]` | EM skipped — the fix |

**Note:** The current operational daily run (`postprocessing_operational.py`) does not call `calculate_skill_metrics()`. It uses `ensemble_calculator.create_ensemble_forecasts()` with pre-calculated skill metrics. Only the deprecated `postprocessing_forecasts.py` and the recalculation script call this function.

### Code Example

```python
# In calculate_skill_metrics() signature (skill_metrics.py line 1687):
def calculate_skill_metrics(
    config,
    observed: pd.DataFrame,
    simulated: pd.DataFrame,
    timing_stats=None,
    exclude_models: list[str] | None = None,  # NEW — PP-030
):
    ...
    exclude_models = exclude_models or []
    ...
```

```python
        # After individual model CRPS merge (line 1866), before EM groupby:

        if "EM" not in exclude_models:
            skill_metrics_df_ensemble_avg = (
                skill_metrics_df_ensemble.groupby([period_col, "date", "code"])
                .agg(em_agg_dict)
                .reset_index()
            )
            # ... existing EM derivation lines 1873-1979 indented one level ...

            number_of_models = simulated["model_short"].nunique()
            if number_of_models > 1 and not skill_metrics_df_ensemble_avg.empty:
                # ... existing EM skill metrics + CRPS + merge ...
                skill_stats = pd.concat([skill_stats, ensemble_skill_stats], ...)
                joint_forecasts = pd.merge(simulated, ensemble_skill_metrics_df[join_cols], ...)
            else:
                joint_forecasts = simulated.copy()
        else:
            logger.info(
                "Skipping EM ensemble derivation (excluded). "
                "Operational EM skill metrics are retained in DB."
            )
            joint_forecasts = simulated.copy()
```

```python
# In recalculate_skill_metrics.py line 133:
skill_metrics_result, modelled, returned_timing_stats = (
    skill_metrics.calculate_skill_metrics(
        config, observed, modelled, timing_stats_,
        exclude_models=["EM"],  # PP-030: skip EM re-derivation at boundaries
    )
)
```

---

## Testing

### Test approach

Use **fake data with hand-crafted values** (no mocks of `calculate_skill_metrics` internals). Follow the existing pattern in `test_skill_metrics.py::TestCalculateSkillMetricsPentad::test_ensemble_creation`:
- Build `observed` and `simulated` DataFrames with 2 models, 2 stations, 2 years
- Call `calculate_skill_metrics()` with the real `PENTAD` config from `conftest.py`
- Assert on the returned DataFrames

### Test fixtures

Reuse the existing `observed` and `simulated` fixture pattern from `test_skill_metrics.py`:
- **observed**: columns `code`, `date`, `discharge_avg`, `model_short` ("Obs"), `delta`
- **simulated**: columns `code`, `date`, `pentad_in_year` (str), `pentad_in_month` (str), `forecasted_discharge`, `model_short`
- 2 models (e.g., `"MA"`, `"MB"`), 2 stations (`"123"`, `"456"`), 2 years (2022, 2023)
- Use relaxed thresholds (`efficiency=2.0`, `accuracy=0.0`, `nse=-1.0`) to force all models into the ensemble, same as `test_ensemble_creation`

### Test Cases

- [x] `test_exclude_em_no_em_in_output` — EM absent from both `skill_stats` and `joint_forecasts`; MA/MB present
- [x] `test_exclude_em_individual_crps_still_computed` — `crps` column exists for individual models
- [x] `test_exclude_em_logs_skip_message` — `"Skipping EM ensemble derivation"` in `caplog.text`
- [x] `test_default_exclude_models_em_present` — backward compat: default `None` → EM derived as before
- [x] `test_exclude_em_with_decad_config` — same exclusion verified on DECAD path
- [x] Regression: all existing tests pass unchanged
- [x] Updated `test_workflow_integration.py::test_em_excluded_in_recalc_output` — asserts no EM in recalc output
- [x] Updated `test_wiring_integration.py::test_pentad_em_excluded_in_recalc` — asserts no EM, LR/TFT still present

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

- [x] No documentation impact — internal recalculation logic change; no user-visible behavior change.

---

## Out of Scope

- Option B (reading EM records from API for recalculation) — tracked as a follow-up.
- S2 EM absence — expected behavior; LR below skill threshold.
- Cleaning up the existing bad NEW-generation EM skill records in the DB — a separate DB migration task.
- Changes to the operational daily run skill metric logic.

## Dependencies

None. INFRA-015 (boundary date convention) is already closed.

## Acceptance Criteria

- [x] `recalculate_skill_metrics.py` no longer produces EM skill metric records with n_pairs ≤ 2
- [x] All existing EM skill metric records (OLD generation, n_pairs=14-17) are untouched
- [x] `calculate_skill_metrics()` signature is backward-compatible (`exclude_models` defaults to `None` → `[]`)
- [x] Individual model CRPS/PIT/sharpness (lines 1843-1873) is computed regardless of `exclude_models`
- [x] All existing tests pass — 1243 passed (2 pre-existing failures in `test_file_writer.py`, unrelated)
- [x] New tests (fake data, no mocks) confirm: EM absent when excluded, EM present when not excluded, individual metrics unaffected, both pentad and decad configs
- [x] Log message confirms skip when EM is excluded (verified via `caplog`)

---

## References

- INFRA-015 boundary date convention: commit `91c56f0` (closed)
- PP-027: EM ensemble silent skip observability
- Pipeline log: `apps/logs/run_locally_20260327_170642.log`
- Recalculation script: `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
- Skill metrics calculation: `apps/postprocessing_forecasts/src/skill_metrics.py:1653–1979`
- Data reader: `apps/postprocessing_forecasts/src/data_reader.py:1729–1952`
