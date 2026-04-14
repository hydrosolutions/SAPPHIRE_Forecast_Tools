# PP: Deduplicate skill metrics before API write to fix monthly/quarterly/seasonal bulk upsert

| Field       | Value                                                   |
|-------------|---------------------------------------------------------|
| **Module**  | `postprocessing_forecasts`                              |
| **Priority**| Mid                                                     |
| **Status**  | Review                                                  |
| **Branch**  | `fix_fd_monthly_skill_metrics` (bundle with FD-013)     |

## Problem

Monthly, quarterly, and seasonal skill metric recalculation fails to write to the API with:

```
UniqueViolation: duplicate key value violates unique constraint
"uq_skill_metrics_horizon_code_model_date_horizon"
Key (horizon_type, code, model_type, date, horizon_in_year)=(MONTH, 15013, NAIVE_MEAN, 2026-01-01, 1) already exists.
```

The batch sent to the API contains two rows with the same upsert key `(horizon_type, code, model_type, date, horizon_in_year)` but different `composition` values. PostgreSQL rejects the second row within the same INSERT.

This means monthly, quarterly, and seasonal skill metrics have **never** been successfully written to the API.

## Root Cause Analysis

### Why two rows exist for the same key

Three ensemble-model functions produce duplicate rows due to a **CRPS merge key mismatch**:

1. **Point metrics** are grouped by `["month_in_year", "code", "model_short", "composition"]` — producing one row per composition variant.
2. **CRPS** is grouped by `["month_in_year", "code", "model_short"]` — **without composition** — producing one pooled CRPS value.
3. The **left merge** of CRPS back into point metrics uses only `["month_in_year", "code", "model_short"]`. When multiple composition variants exist for the same key, the single CRPS row fans out to match all of them.

Example from `_add_naive_mean()` (skill_metrics.py):

- **Line 1431**: Point metrics groupby includes `composition`
- **Line 1447**: CRPS groupby excludes `composition`
- **Line 1467**: Merge on smaller key → one-to-many fan-out → duplicate upsert keys

The same fan-out pattern exists in 5 other code locations:

- **EM in `calculate_monthly_skill_metrics()`**: point metrics L1297, CRPS L1313, merge L1333
- **`_add_skilled_mean()`**: point metrics L1614, CRPS L1630, merge L1650
- **EM in `_calculate_aggregated_skill_metrics()`**: point metrics L2248, CRPS L2263, merge L2282
- **`_add_naive_mean_aggregated()`**: point metrics L2385, CRPS L2400, merge L2419
- **`_add_skilled_mean_aggregated()`**: point metrics L2528, CRPS L2543, merge L2562

### Why composition is correctly excluded from CRPS

Excluding composition from the CRPS groupby is **statistically correct**, not an oversight:

- **CRPS evaluates the aggregated quantile distribution**, which is singular per `(month_in_year, code, model_short)`. The quantiles are already vincentized (averaged) from all contributing models — composition is metadata about which models participated, not a distinct forecast variant.
- **Splitting by composition would fragment the dataset**. Monthly data typically has 1–12 observations per (code, model, month). Splitting into composition variants reduces N further, potentially below 2 (the minimum for meaningful reliability scoring).
- **Pentad/decad avoids this differently**: Short-term code creates separate EM rows per composition variant *before* CRPS calculation (skill_metrics.py:1897), then includes composition in both groupby keys (line 1927 for point metrics, line 1941 for CRPS). Monthly code aggregates all compositions into one row first, creating the mismatch.

### The actual bug

The bug is **not** in the CRPS groupby logic (which is correct) but in the **absence of deduplication before the API write**. Two rows with the same upsert key but different `composition` values are both sent, and PostgreSQL rejects the batch.

The combined forecast write path already has deduplication (api_writer.py, lines ~376-388), but skill metrics lack the same guard.

### DB constraint

The upsert unique key in `crud.py` is `(horizon_type, code, model_type, date, horizon_in_year)`. `composition` is **not** part of the constraint — it is metadata.

## Affected Horizons

| Horizon | Functions affected | Status |
|---------|-----------|--------|
| **Monthly** | `EM inline in calculate_monthly_skill_metrics` (L1296-1341), `_add_naive_mean`, `_add_skilled_mean` | **Fails** (confirmed) |
| **Quarterly** | `EM inline in _calculate_aggregated_skill_metrics` (L2248-2285), `_add_naive_mean_aggregated`, `_add_skilled_mean_aggregated` | **Fails** (confirmed in logs) |
| **Seasonal** | Same `_aggregated` functions + EM inline | **Fails** (confirmed in logs) |
| Pentad/Decad | `calculate_skill_metrics` — separate code path, includes `composition` in both groupby keys (lines 1927, 1941) | **Works** — no duplicates |
| Daily | `calculate_daily_skill_metrics` | **Not affected** — no composition dimension, operates at (code, model_short) level only |

## Implementation Plan

### Phase 1: Add deduplication guard in `_write_skill_metrics_to_api()`

**File to modify**: `apps/postprocessing_forecasts/src/api_writer.py`

**Do NOT change any existing function signatures, data flow logic, or control flow. Changes must be purely additive — a single dedup block inserted at the right location.**

**Location**: After the `_date` column computation (lines ~544-583) and after the `dropna(subset=[horizon_in_year_col])` filter (~line 498), but before building records (line ~610).

```python
# --- Deduplicate on DB upsert key ---
# The DB unique constraint is (horizon_type, code, model_type, date,
# horizon_in_year).  composition is NOT part of the constraint.
# Monthly/quarterly/seasonal ensemble baselines (EM, Naive Mean,
# Skilled Mean) produce multiple rows per key with different
# composition values due to the CRPS merge fan-out.  Retain the row
# with a non-None composition (the true ensemble record).
upsert_key = ["code", "model_type", "_date", horizon_in_year_col]
n_before = len(df_rec)
df_rec = df_rec.sort_values("_composition", na_position="first")
df_rec = df_rec.drop_duplicates(subset=upsert_key, keep="last")
n_dupes = n_before - len(df_rec)
if n_dupes > 0:
    logger.warning(
        "Dropped %d duplicate skill metric records before API write (%s)",
        n_dupes, horizon_type,
    )
```

Changes from the initially drafted code:
- `horizon_type_col` and `model_type_col` → replaced with string literals `"model_type"` and removed `horizon_type` from key (it is constant per function call — all rows share the same horizon_type)
- Sort on `_composition` (the cleaned column) not `composition` (the raw input column)
- `horizon_in_year_col` is correct — it's a variable holding the column name

**Why `sort(na_position="first") + keep="last"`**: Rows with `_composition=None` (NaN) sort first, so `keep="last"` retains the row with a non-None composition string. When multiple non-None compositions exist (rare — would require the same station+model+period to appear in different ensemble variants), the alphabetically last composition string is kept. This is a pragmatic choice — the DB can only store one row per key, and any consistent selection beats the current total failure.

**Why this goes after `_date` computation**: `_date` is computed from `horizon_in_year + year` and is part of the upsert key. The dedup must happen after `_date` exists.

### Phase 2: Write tests

**File to modify**: Add to existing test file or create `apps/postprocessing_forecasts/tests/test_api_writer_dedup.py`

#### Test 1: Duplicate skill metrics are deduplicated before write

Create a DataFrame with two skill metric records sharing the same `(horizon_type, code, model_type, horizon_in_year)` but different `composition` values (one None, one with a string). Mock the API client. Call `_write_skill_metrics_to_api()`. Assert:
- The API receives only **one** record per unique key
- The retained record has the non-None composition

#### Test 2: Non-duplicate records pass through unchanged

Create skill metric records with distinct keys. Call the function. Assert all records are sent to the API without loss.

#### Test 3: Pentad/decad records are unaffected

Create pentad skill metric records (no composition duplicates). Call the function. Assert all records pass through — zero dropped by dedup.

#### Test 4: Multiple non-None compositions for same key

Create a DataFrame with two skill metric records sharing the same `(code, model_type, horizon_in_year)` but different non-None `composition` values (e.g., "LR, TFT" and "LR, TFT, TiDE"). Mock the API client. Call `_write_skill_metrics_to_api()`. Assert:
- The API receives only **one** record per unique key
- The retained record has the alphabetically last composition (i.e., "LR, TFT, TiDE")

### Phase 3: Verify recalculation succeeds for all horizons

1. **Monthly**: `SAPPHIRE_PREDICTION_MODE=MONTHLY bash apps/run_locally.sh recalculate_skill_metrics`
   - No 500 errors
   - `SELECT COUNT(*) FROM skill_metrics WHERE horizon_type='MONTH'` > 0

2. **Quarterly + Seasonal**: `SAPPHIRE_PREDICTION_MODE=ALL bash apps/run_locally.sh recalculate_skill_metrics`
   - No 500 errors for quarterly or seasonal
   - `SELECT horizon_type, COUNT(*) FROM skill_metrics WHERE horizon_type IN ('MONTH','QUARTER','SEASON') GROUP BY horizon_type` — all non-zero

3. **Pentad/Decad regression check**: `SAPPHIRE_PREDICTION_MODE=BOTH bash apps/run_locally.sh recalculate_skill_metrics`
   - Zero duplicate warnings in log (pentad/decad should have no composition duplicates)
   - Skill metric record counts unchanged vs. before the fix

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "note": "Add dedup guard in api_writer.py" },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1, "note": "Write tests" },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 0, "note": "Run recalculation for ALL horizons and verify" }
  }
}
```

## Verification

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` — all tests pass, zero skips
2. Monthly recalculation succeeds (no 500 errors)
3. Quarterly + seasonal recalculation succeeds
4. Pentad/decad recalculation shows zero duplicate warnings (regression check)
5. DB has records for MONTH, QUARTER, SEASON horizons
6. Dashboard: select monthly horizon, station 15013 — skill metrics visible

## Out of Scope

- **Refactor CRPS merge in `skill_metrics.py`**: The CRPS groupby correctly pools across compositions (statistically sound). The duplicate rows are a write-layer concern, not a calculation-layer error. Refactoring the merge to avoid producing duplicates is possible but touches 6+ functions and risks regressions in the calculation logic.
- **Filter `n_pairs=0` records before API write**: Related but separate issue (see investigation of deprecated `postprocessing_forecasts.py` corruption).
- **Daily skill metrics**: Verified — daily operates at `(code, model_short)` level without composition or monthly aggregation. No dedup needed.
