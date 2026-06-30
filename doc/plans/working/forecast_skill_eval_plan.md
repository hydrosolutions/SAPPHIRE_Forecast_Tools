# Forecast Skill Eval — Plan of Record (post-P0 rescope)

Supersedes the phase list in `forecast_skill_eval_planner_prompt.md` after the
P0 inventory (`forecast_skill_eval_inventory.md`) and review. Decisions locked:
**target DB = local dev (now)**; **long-horizon truth = derived from daily
aggregation**.

## Verified facts (from P0, live local gateway, client 0.5.0)

- Norm join key is `horizon_in_year` for day/pentad/decade (same meaning across
  hydrographs/runoffs/forecasts). `horizon_value` is the within-month index and
  is **never** a join key. `long_forecasts` has no `horizon_in_year`.
- Short-term forecasts contain `horizon_in_year=0` sentinel rows → filter out.
- Short-term point value = `forecasted_discharge`. Long-term = `q → q50 → q_loc`.
- No provenance/source column on norms → provenance is **assigned** from the
  configured horizon mapping, never read.

## Per-horizon scope (local DB)

| Horizon | Norm source | Truth source | Notes |
|---|---|---|---|
| day | stored (usable 85.6%) | runoffs `day` | exploratory; point-value 65.6% non-null; partial-year |
| pentad | **calculated LOO** (stored null) | runoffs `pentad` (27 yr) | provenance=calculated; operational for one region |
| decade | **calculated LOO** (stored null) | runoffs `decade` (27 yr) | provenance=calculated |
| month | stored (usable 100%) | **derived from daily** | calendar windows only; exclude rolling |
| quarter | stored (usable 100%) | **derived from daily** | calendar windows only; exclude rolling |
| season | stored (usable 100%) | **derived from daily** | 2 models only (LR_Base, LR_SM) |

## Phases (revised)

- **P1 — Scaffold + API readers + period mapping.** New app `apps/forecast_skill_eval`.
  Readers via non-deprecated client methods, pagination looped to completion,
  `decade` enum normalized, `horizon_in_year=0` sentinels filtered, point-value
  column selection (`forecasted_discharge` short; `q→q50→q_loc` long). `periods.py`
  maps short-term on `horizon_in_year` and long-term `valid_from/valid_to` →
  calendar period key (+ flags rolling windows). Fake in-memory client for tests.
  Pin `sapphire-api-client` to the same commit as `apps/machine_learning`;
  `iEasyHydroForecast` as a path source.
- **P2 — Norm resolution + provenance.** Stored norm when finite & >0, tagged by
  configurable horizon mapping; else calculated LOO from runoffs (≥10 distinct
  yr, exclude scored year). Duplicate norm keys → ledger. Provenance assigned,
  never read. Single fallback path (no monthly re-aggregation in the eval app).
- **P3 — Observed-truth layer.** day/pentad/decade read runoffs directly;
  month/quarter/season **derive truth by aggregating daily runoffs** to calendar
  periods with completeness rules; align to calendar valid windows only.
- **P4 — Classifier + pair builder + exclusion ledger.** Strict `< threshold*norm`
  = limit-plan; equality = normal. All missing/non-finite/zero-norm/sentinel/
  unmatched-join/rolling-window → exclude-and-count. Each valid pair carries
  `norm_provenance`.
- **P5 — Contingency + metrics + baselines.** TP/FP/FN/TN, base rate, POD,
  FAR=FP/(TP+FP), POFD=FP/(FP+TN), CSI, bias, HSS, PSS, Wilson CIs; denominator-
  zero → NaN+flag. Baselines (climatology, LR/LR_Base proxy) on matched samples.
  Rows for provenance ∈ {official, aggregated_from_monthly, calculated, all}.
  Lead-time stratification (long-term) treats issue date as part of the instance.
- **P6 — Orchestration + CLI + artifacts.** Params captured once at entry;
  per-station + pooled CSV/parquet + `summary.md`; pooled rows reported with the
  per-station distribution.
- **P7 — E2E validation.** Fake-client integration; live local smoke on **day**
  (cleanest 1:1) then **pentad** (exercises calculated-norm path); full
  `run_tests.sh` pass, zero skips except the api-client gate.

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P2", "P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P4"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P5"], "parallel_agents": 1 },
    "P7": { "depends_on": ["P6"], "parallel_agents": 1 }
  }
}
```
