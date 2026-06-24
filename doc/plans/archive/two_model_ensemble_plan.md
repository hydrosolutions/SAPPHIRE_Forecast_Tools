# P-PIPE Follow-on: Two-model Quarterly/Seasonal Ensemble Plan

Branch: `develop_ppipe_ensemble_hv` (P-PIPE / PR #382 follow-on)
Status: plan only; no implementation, no DB writes.

Scope: `apps/` and documentation. Do not edit `sapphire/services/**`. This
follow-on is a behavior change, not just the P-PIPE horizon-value convention, so
it should be considered as a separate PR from PR #382 unless the reviewer asks
to fold it in.

Use sentinel station codes only in operational evidence. DB work is
aggregate-only until an approved cleanup execution step.

## Review outcome (2026-06-23): GO-WITH-CHANGES -- integrated

A critical review verified the core (both EM paths, durability mechanism,
quarter/season-only scoping). Decisions + fixes folded in:

- **M1 (decided, owner): leave Naive Mean / Skilled Mean as-is; de-skill-gate
  ONLY EM.** EM must reliably = `mean(LR_Base, LR_SM)`. Do NOT stop writing Naive
  Mean / Skilled Mean in this PR (additive, no dashboard-visible product removal;
  EM and Naive Mean will be numerically identical for quarter/season -- accepted).
  This is now a hard prerequisite of P3 (resolved).
- **M2 (Must): change only EM's FORMATION gate, never its downstream skill
  computation.** The recalc EM path computes EM-row skill (CRPS/PIT/sharpness,
  `skill_metrics.py:2238-2291`) and concats into skill_stats -- keep that intact,
  or we re-starve the very skill displays this work populates. The P3 agent prompt
  must state this explicitly.
- **S1: the P2 reader filter is THE durable fix.** Quarter raw is synthesized at
  read time from MONTH rows (`source=aggregated_from_monthly`), so the
  `long_forecasts` QUARTER-raw deprecated DELETE is likely a **0-row no-op = success,
  not failure**. Real physical cleanup targets: (a) `skill_metrics` QUARTER
  deprecated rows (stored), (b) stale ensemble rows in both tables.
- **S2: filter ALL FOUR aggregated readers** -- add `read_seasonal_forecasts`
  (`data_reader.py:2711`) + `read_latest_seasonal_forecasts` (`:2878`) to P2, not
  just the two quarter readers.
- **S3: put the filter in the READER, not `aggregation.py`** (the reader combines
  monthly-aggregated + direct-quarter sources; `aggregate_monthly_fc_to_quarterly`
  misses the direct-quarter source). Drop the `aggregation.py` option from P2.
- **S4: P4 runbook must reference + supersede by name** the P-PIPE plan's MF-A
  `DELETE ... horizon_type='QUARTER' AND model_type NOT IN ('LR_BASE','LR_SM')`
  statement, so an operator doesn't nuke the regenerated EM.
- **N1:** "both LR present or no EM" matches current behavior (`is_multi_model_composition`
  drops single-model groups today); keep it as the conservative default, but note
  the continuity gap (a station with only one LR gets no quarter/season ensemble).
  **N3:** P1 must test the **recalc** EM path (not just operational), since it must
  match operational for full-history regen.

## Findings

### 1. Current quarter/season ensemble formation

Quarterly and seasonal ensemble forecasts use the shared
`_create_aggregated_ensemble_forecasts()` path:

- Quarter calls it with `period_col="quarter_in_year"` and
  `time_group_cols=["year", "quarter_in_year", "code"]`:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:506-531`.
- Season calls it with `period_col="season_in_year"` and
  `time_group_cols=["season_year", "season_in_year", "code"]`:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:534-558`.
- The function first normalizes `forecasted_discharge`, copies the input to
  `joint`, and excludes baseline rows `{"EM", "Naive Mean", "Skilled Mean"}`:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:577-590`.

Current EM is not a simple LR mean. It is a threshold-filtered mean:

- EM calls `filter_for_highly_skilled_forecasts(skill_stats)`:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:592-594`.
- It inner-joins forecasts to the filtered skill keys
  `[period_col, "code", "model_short"]`, drops baseline rows, and drops NaN
  forecasts: `apps/postprocessing_forecasts/src/ensemble_calculator.py:603-610`.
- It averages `forecasted_discharge` and every available quantile column:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:613-630`.
- It writes `composition`, sets `model_short="EM"`, then discards any group
  whose composition is not multi-model:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:631-638`.

The skill gate is strict:

- Threshold defaults are `sdivsigma < 0.6`, `nse > 0.8`, and
  `accuracy > 0.8`: `apps/postprocessing_forecasts/src/skill_metrics.py:23-59`.
- Threshold filtering reads env overrides, allows `"False"` to disable a metric,
  and applies `>` for higher-is-better metrics and `<` otherwise:
  `apps/postprocessing_forecasts/src/skill_metrics.py:1673-1699`.

This explains why a two-model LR ensemble does not reliably form: both
`LR_Base` and `LR_SM` must pass every skill threshold for the same
period/code/lead. If either fails, the inner join leaves one or zero qualifying
models, and the multi-model composition filter discards the EM row.

Minimal behavior change for operational EM:

- For quarter/season only, define the candidate pool as available non-null
  `LR_Base` + `LR_SM` rows and compute EM as the equal-weight mean of those two
  rows, independent of the high-skill threshold gate.
- Keep the existing two-model requirement: if either LR row is missing or NaN
  for a target group, do not fabricate EM.
- Apply the same rule in both operational ensemble creation and
  full-history recalculation. Recalculation has its own aggregated EM path that
  also filters skill and discards single-model groups:
  `apps/postprocessing_forecasts/src/skill_metrics.py:2199-2236`.

### 2. EM, Naive Mean, and Skilled Mean semantics with two models

Current semantics:

- EM: threshold-filtered unweighted mean of qualifying models:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:592-638`.
- Naive Mean: unweighted average of all non-baseline models, no skill filter:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:738-777`.
- Skilled Mean: inverse-MAE weighted mean of threshold-filtered models:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:662-735`.

With the owner-confirmed quarter/season input set reduced to `LR_Base` and
`LR_SM`:

- Recommended EM: equal-weight mean of `LR_Base` and `LR_SM`.
- Naive Mean becomes identical to EM if its input pool is also only
  `LR_Base` and `LR_SM`.
- Skilled Mean remains distinct only when both LR models pass the skill gate
  and have usable MAE; otherwise it will still disappear under current logic.

Owner/modeller decision point:

- Keep all three rows for continuity, accepting that EM and Naive Mean are
  redundant for quarter/season under a two-model-only pool.
- Or keep EM as the official simple LR mean and drop/stop writing Naive Mean and
  Skilled Mean for quarter/season to avoid duplicate or inconsistently missing
  products.

Planner recommendation: make EM the required acceptance target and treat Naive
Mean / Skilled Mean as an explicit modeller decision before implementation. Do
not silently change their public meaning.

### 3. Horizon scope

This change must be scoped to quarter/season only.

Monthly ensembles use a separate implementation:

- `create_monthly_ensemble_forecasts()` builds monthly groups and includes
  `horizon_value` when present for PP-032:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:222-281`.
- Monthly EM still uses the threshold-filtered skill pool:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:283-333`.
- Monthly Skilled Mean and Naive Mean remain the existing 1/MAE and all-model
  implementations: `apps/postprocessing_forecasts/src/ensemble_calculator.py:334-349`.

Do not change monthly behavior or its full model set. The aggregate evaluation
confirms monthly currently includes the full long-term model population:
`doc/plans/working/forecast_skill_eval_results.md:25-32`.

### 4. Deprecated-model cleanup scope

Quarterly forecasts are built from monthly forecasts:

- `read_quarterly_forecasts()` aggregates monthly forecasts with
  `aggregate_monthly_fc_to_quarterly()` and also reads direct quarterly API
  forecasts: `apps/postprocessing_forecasts/src/data_reader.py:2621-2672`.
- `aggregate_monthly_fc_to_quarterly()` groups by
  `["code", "year", "quarter_in_year", "model_short"]`, so all monthly model
  types become quarterly model rows unless filtered:
  `apps/postprocessing_forecasts/src/aggregation.py:217-257`.

Therefore the code change must prevent re-creation of deprecated raw quarter
rows during recalc. A DB delete alone is not durable.

Deprecated quarter model inputs:

- Drop for `horizon_type=QUARTER`: `GBT`, `LR_SM_DT`, `LR_SM_ROF`, `MC_ALD`,
  `SM_GBT`, `SM_GBT_LR`, `SM_GBT_NORM` / app spelling `SM_GBT_Norm`.
- Keep: `LR_Base`, `LR_SM`.

Season check:

- Current aggregate evaluation shows season already has only `LR_Base` and
  `LR_SM`: `doc/plans/working/forecast_skill_eval_results.md:25-32` and
  `doc/plans/working/forecast_skill_eval_results.md:143-148`.
- Still include a dry-run count for the seven deprecated models under
  `horizon_type=SEASON`; if nonzero, delete them under the same policy after
  reviewer approval.

Cleanup tables and predicates:

- `long_forecasts`: delete deprecated raw model rows for
  `horizon_type IN ('QUARTER', 'SEASON')`, scoped to the seven deprecated model
  labels after confirming exact DB enum/storage labels.
- `skill_metrics`: delete matching skill rows for the same horizon/model set so
  stale skill cannot feed downstream displays or future calculations.
- Old ensemble rows (`EM` / `ENSEMBLE_MEAN`, `Naive Mean` / `NAIVE_MEAN`,
  `Skilled Mean` / `SKILLED_MEAN`) should be regenerated under the new
  semantics. Do not preserve old 9-model or skill-gated quarter/season ensemble
  rows as authoritative.

Model label caution:

- App write mapping sends long-term app labels such as `LR_Base`, `LR_SM`, and
  `SM_GBT_Norm`: `apps/postprocessing_forecasts/src/api_writer.py:20-44`.
- Existing P-PIPE runbook examples refer to DB enum names such as
  `ENSEMBLE_MEAN`, `NAIVE_MEAN`, and `SKILLED_MEAN`:
  `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:82-98`.
- Dry-run counts must report the actual labels present before any cleanup
  statement is approved.

Checked-in aggregate evidence:

- Quarter p8 pairs include all seven deprecated inputs plus LR models and
  ensembles: `doc/plans/working/forecast_skill_eval_results.md:117-142`.
- Season p8 pairs include only `LR_Base` and `LR_SM`:
  `doc/plans/working/forecast_skill_eval_results.md:143-148`.
- Aggregate pair totals by horizon are quarter `40305` and season `10880`:
  `doc/plans/working/forecast_skill_eval_results.md:206-224`.

### 5. MIG-008 / P-PIPE reconciliation

This follow-on supersedes and clarifies the older Dataset B understanding:

- P-PIPE PR #382 fixes quarter/season ensemble `horizon_value` placement,
  seasonal issue threading, and per-lead skill.
- This follow-on changes the quarter/season ensemble model policy: the seven
  non-LR models are genuinely deprecated for quarter/season, EM is the simple
  `LR_Base` + `LR_SM` mean, and old quarter/season ensemble rows must be
  regenerated under that policy.
- The P-PIPE plan already blocks old-hv cleanup until code is deployed,
  full-history recalc runs, and aggregate verification passes:
  `doc/plans/archive/ppipe_postprocessing_ensemble_hv_plan.md:126-159`.
- The P-PIPE runbook currently says not to move raw `LR_BASE` / `LR_SM` rows as
  part of P-PIPE: `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:9-19`. This
  follow-on is a separate cleanup policy: delete deprecated non-LR
  quarter/season raw rows and regenerate LR-only ensembles.

Cross-plan impact: any cleanup plan that deletes all non-LR quarter rows must
not delete regenerated EM rows. The raw deprecated-model cleanup and stale
ensemble cleanup need separate predicates.

### 6. Sequence and re-validation

Required order per deployment:

1. Implement and deploy code that scopes quarter/season raw inputs and EM
   calculation to `LR_Base` + `LR_SM`.
2. Run local/sentinel validation that EM forms when both LR rows are present,
   even when one or both LR skill rows fail current high-skill thresholds.
3. Dry-run aggregate DB counts for deprecated raw rows and old ensemble rows in
   `long_forecasts` and `skill_metrics`.
4. Delete approved deprecated quarter/season raw rows and stale quarter/season
   ensemble rows.
5. Recalculate quarter and season full history per deployment.
6. Verify EM equals the arithmetic mean of `LR_Base` and `LR_SM` at the
   deployment's config-lead `horizon_value` per issue/quarter.
7. Verify deprecated models are gone for quarter/season, monthly rows are
   unchanged, and test suites are green.

## Recommended Semantics

Required:

- Quarter/season EM = equal-weight mean of the two available non-null raw
  inputs: `LR_Base` and `LR_SM`.
- EM is not skill-gated for quarter/season.
- EM forms only when both LR inputs exist in the same target group and both have
  non-null forecast values.
- Quantiles are averaged column-wise and monotonicity enforcement remains in
  place.

Decision needed:

- Naive Mean: keep as the all-input mean, knowing it equals EM with only two LR
  inputs, or stop writing it for quarter/season.
- Skilled Mean: keep as inverse-MAE weighted LR mean, knowing it remains
  skill-gated and may be absent, or stop writing it for quarter/season.

## Phased Plan

### P1 -- Pin the two-model policy in tests

**Goal**: Add focused failing tests that describe the desired quarter/season
behavior before implementation.

**Files**:

- `apps/postprocessing_forecasts/tests/test_quarterly_ensemble_creation.py`
- `apps/postprocessing_forecasts/tests/test_quarterly_skill_metrics.py`
- `apps/postprocessing_forecasts/tests/test_quarterly_data_reader.py`
- Optional: `apps/postprocessing_forecasts/tests/test_quarterly_workflow_integration.py`

**Depends on**: none.

**Agents**: 1 agent.

**Acceptance criteria**:

- Tests assert quarter/season EM is `mean(LR_Base, LR_SM)` when LR skill does
  not pass current high-skill thresholds.
- Tests assert deprecated model inputs are ignored for quarter/season
  aggregation/recalc and do not enter EM composition.
- Tests assert monthly ensemble tests remain unchanged.

### P2 -- Scope quarter/season raw forecast inputs

**Goal**: Prevent quarter/season recalc and operational paths from recreating or
using deprecated raw model rows.

**Files**:

- `apps/postprocessing_forecasts/src/data_reader.py`
- `apps/postprocessing_forecasts/src/aggregation.py` only if the filter belongs
  at aggregation boundary
- Relevant tests from P1

**Depends on**: P1.

**Agents**: 1 agent.

**Acceptance criteria**:

- `read_quarterly_forecasts()` and `read_latest_quarterly_forecasts()` return
  only `LR_Base` and `LR_SM` raw model rows for quarter, whether the source is
  aggregated monthly or direct quarter API rows.
- Seasonal reads continue to return only supported LR rows; any deprecated
  season rows are ignored if present.
- Monthly reads still expose the full model set.

### P3 -- Change aggregated EM behavior

**Goal**: Make quarter/season EM a deterministic two-LR arithmetic mean in both
operational ensemble creation and full-history recalculation.

**Files**:

- `apps/postprocessing_forecasts/src/ensemble_calculator.py`
- `apps/postprocessing_forecasts/src/skill_metrics.py`
- Relevant tests from P1

**Depends on**: P2.

**Agents**: 1 agent.

**Acceptance criteria**:

- Quarter/season EM uses exactly `LR_Base` and `LR_SM` when both are present.
- EM no longer depends on `filter_for_highly_skilled_forecasts()` for
  quarter/season.
- Single-LR or missing-value groups do not create EM.
- Monthly EM remains skill-gated and full-model.
- Naive Mean and Skilled Mean behavior follows the modeller decision recorded
  before implementation.

### P4 -- Cleanup runbook and aggregate dry-runs

**Goal**: Prepare per-deployment cleanup instructions without executing DB
writes.

**Files**:

- `doc/prod/ppipe_ensemble_hv_deploy_runbook.md`
- Optional follow-on cleanup note under `doc/plans/working/`

**Depends on**: P3.

**Agents**: 1 agent.

**Acceptance criteria**:

- Runbook separates raw deprecated-model cleanup from stale ensemble cleanup.
- Dry-run queries report aggregate counts by `horizon_type`, `horizon_value`,
  `model_type`, and date range for `long_forecasts` and `skill_metrics`.
- Predicates are explicitly scoped to quarter/season and reviewed before any
  delete.

### P5 -- Deploy, delete, recalc, verify

**Goal**: Execute the operational data transition per deployment after reviewed
code is deployed.

**Files**: no repository files unless private deployment notes are maintained
outside git.

**Depends on**: P4.

**Agents**: 0 code agents; operator/reviewer execution.

**Acceptance criteria**:

- Deprecated raw quarter models are absent from `long_forecasts` and
  `skill_metrics`; deprecated season models are absent if any existed.
- Stale old-semantics quarter/season ensemble rows are removed or superseded by
  reviewed predicates.
- Full-history quarter and season recalc completes per deployment.
- Aggregate verification shows EM rows at the correct config-lead
  `horizon_value`.
- Sentinel spot checks confirm `EM == (LR_Base + LR_SM) / 2` for q and each
  quantile, within rounding tolerance.
- Monthly aggregate counts and model population are unchanged.

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P4"], "parallel_agents": 0 }
  }
}
```

## Final Acceptance

- Quarter and season EM reliably form from `LR_Base` + `LR_SM` at the correct
  config-lead `horizon_value` per issue/quarter.
- EM equals the arithmetic mean of `LR_Base` and `LR_SM` for q and quantiles,
  after normal rounding/monotonicity handling.
- Deprecated quarter models are gone from `long_forecasts` and `skill_metrics`;
  season is confirmed clean or cleaned if needed.
- Old 9-model/skill-gated quarter/season ensemble rows no longer appear as the
  active product.
- Monthly ensemble behavior and monthly full model population are unchanged.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` is green, plus targeted
  postprocessing tests for quarter/season.
