# P-PIPE: Postprocessing Quarterly/Seasonal Ensemble `horizon_value` Plan

Branch: `fix_migration_long_forecast_multihorizon`  
Scope: `apps/postprocessing_forecasts` and read-side app consumers only. Do not edit `sapphire/services/**`.  
Status: plan only; no implementation, no DB writes.

P-PIPE is a hard prerequisite for the data cleanup phases in
`doc/plans/working/longforecast_hv_convention_plan.md`. P3/P4 cleanup/backfill
must not run until P-PIPE is coded, reviewed, deployed, and the quarterly/seasonal
ensembles have been regenerated at the new `horizon_value`.

## Review outcome (2026-06-22): GO-WITH-CHANGES -- PP0 gates everything

A critical review verified every file:line and surfaced three must-fixes that PP0's answers must
resolve before implementation. **Implementation is blocked on PP0 owner/modeller decisions** (see
`doc/prod/ppipe_seasonal_ensemble_decisions_request.md`).

- **MF-1 (make-or-break): seasonal issue identity is destroyed at the reader**, upstream of every file
  PP3 edits. `_SEASONAL_FC_COLS` (`data_reader.py:37-52`) carries no issue-date; `season_in_year` is
  hardcoded `1` (`:3064`); `season_year` comes from `valid_from` (the April target), not the issue.
  Ensemble groups by `["season_year","code"]` (`ensemble_calculator.py:553-558`); both write paths
  dedup `["season_year","code","model_short"]` keep=last (`operational:206-211`, `maintenance:334-342`);
  the writer overwrites `date` with `season_year-04-01` (`api_writer.py:1059,1063,1079`). Since `date`,
  `valid_from`, `valid_to` are identical across the 4 Kyrgyz issues, **only `horizon_value` can
  distinguish them** -- but the 4 issues have already collapsed to one row before the writer. So a
  writer-only hv change CANNOT produce season `hv3/2/1/0` from the ensemble pipeline. Outcome depends
  on the PP0 product-semantics decision; if "preserve 4 issues", PP3 must expand to thread an
  issue-date through reader projection + ensemble groupby + dedup + a per-row issue->lead map (much
  larger than the current 1-agent ensemble-only scope).
- **MF-2: regenerate-vs-delete data-loss.** The cleanup's P3 deletes ~41,225 QUARTER rows (the entire
  historical EM/Naive/Skilled ensemble, 2000-2026), trusting "regenerate". But operational makes only
  the latest quarter; maintenance gap-fill is a 2-quarter window; **only `recalculate_skill_metrics.py`
  rebuilds full history, with non-default `SAPPHIRE_RECALC_START_YEAR=2000`, and the cron
  (`bin/bimonthly_long_term_postprocessing.sh:153,160`) never calls it.** PP6 must bind regeneration to
  an explicit full-history recalc gate before any delete, OR the historical ensembles must be
  **re-stamped, not deleted** (mirroring the LR decision). Owner choice required.
- **MF-3: PP6 acceptance presumes the unresolved PP0 outcome** and conflates raw-LR vs ensemble rows.
  Rewrite PP6 acceptance per model class (raw LR vs EM/Naive/Skilled) and per the PP0 decision.

Should-fix (fold in during implementation): **SF-1** consumer list incomplete -- `db.py:867`
(implicit `hv=1`), `bulletin_manager.py:393,495`, and `validate_pipeline.py:493-522` has no
quarter/season presence check (add one so an empty bucket fails CI); **SF-2** seasonal readers
(`data_reader.py:2696,2848`) don't dedup per-lead -> duplicate ensemble inputs until PP4's hv filter;
either flip PP3<->PP4 or have PP3 dedup its seasonal input; **SF-3** put the lead resolver in
`iEasyHydroForecast` (or an env-var contract each module reads), NOT a `forecast_dashboard ->
postprocessing_forecasts` import; **SF-4** per-issue seasonal skill is a modeller question (skill
degrades with lead) -> PP0. Nits: PP2/PP3 must never deploy independently (state explicitly);
cross-link the two plans' dependency graphs; PP6 regen checks run per deployment DB.

**Status: implementation NOT started; blocked on PP0. PP3 scope + PP6 acceptance to be rewritten once
PP0 answers land.**

## Target Convention

`long_forecasts.horizon_value = operational_month_lead_time` from the
deployment's long-term config for that product.

- Quarter: one product per deployment. Kyrgyz -> `hv1`; Tajik -> `hv0`.
- Season: one product per issue month. Kyrgyz Jan/Feb/Mar/Apr -> `hv3/hv2/hv1/hv0`;
  Tajik April-only -> `hv0`.
- Calendar quarter and issue identity stay in `date`, `valid_from`, and `valid_to`;
  they are not encoded in `horizon_value`.

Use sentinel station codes only in tests and docs. Any DB verification must be
aggregate-only.

## Findings

### 1. `horizon_value` writers and grouping semantics

Evidence:

- `apps/postprocessing_forecasts/src/api_writer.py:917-935` routes quarterly
  ensemble writes into `_write_aggregated_forecasts_to_api(...)` with
  `period_col="quarter_in_year"`.
- `apps/postprocessing_forecasts/src/api_writer.py:938-957` routes seasonal
  ensemble writes into the same helper with `period_col="season_in_year"`.
- `apps/postprocessing_forecasts/src/api_writer.py:1043-1051` computes quarter
  `valid_from` / `valid_to` from `quarter_in_year`, then sets
  `horizon_value = quarter`.
- `apps/postprocessing_forecasts/src/api_writer.py:1052-1067` computes seasonal
  `valid_from` / `valid_to`, then sets `horizon_value = 1`.
- `apps/postprocessing_forecasts/src/api_writer.py:1075-1083` sends that value
  in each `write_long_forecasts` record; `apps/postprocessing_forecasts/src/api_writer.py:1105-1107`
  calls `client.write_long_forecasts(records)`.
- `apps/postprocessing_forecasts/src/ensemble_calculator.py:277-281` includes
  `horizon_value` in monthly ensemble groups when the column exists. Its comments
  at `:313-318`, `:423-428`, and `:477-482` assume all models inside a
  `(year, month, code, horizon_value)` group share the same issue date.
- Quarterly ensemble creation uses `time_group_cols=["year", "quarter_in_year", "code"]`
  at `apps/postprocessing_forecasts/src/ensemble_calculator.py:526-531`.
- Seasonal ensemble creation uses `time_group_cols=["season_year", "code"]` at
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:553-558`.
- Operational quarterly dedup keeps one row per
  `["year", "quarter_in_year", "code", "model_short"]` at
  `apps/postprocessing_forecasts/postprocessing_operational_long_term.py:164-179`;
  maintenance uses the same key at
  `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py:271-285`.
- Operational seasonal dedup keeps one row per `["season_year", "code", "model_short"]`
  at `apps/postprocessing_forecasts/postprocessing_operational_long_term.py:200-212`;
  maintenance does the same at
  `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py:329-343`.

Decision:

- Quarter can likely move to constant deployment `horizon_value` without merging
  distinct quarters, because ensemble and dedup keys still include
  `year` + `quarter_in_year`.
- Seasonal is the central design risk. If a deployment can hold several issue
  products for the same `season_year` at once, the current group/dedup keys
  (`season_year`, `code`, `model_short`) can merge or overwrite Jan/Feb/Mar/Apr
  issue products. The implementation must not silently rely on `horizon_value`
  after changing it unless owner/modeller confirms the intended product semantics.
  Owner decision required: either seasonal postprocessing is only ever one active
  issue per run/deployment, or seasonal ensemble/gap/dedup keys must include an
  issue identifier such as config lead `horizon_value` and/or issue `date`.

### 2. Lead source and deployment/config context

Evidence:

- The long-term forecast config object exposes the authoritative lead via
  `get_operational_month_lead_time()` at
  `apps/long_term_forecasting/config_forecast.py:230-231` and the horizon type
  via `get_horizon_type()` at `:269-276`.
- The long-term forecast writer already uses the config lead for API writes at
  `apps/long_term_forecasting/run_forecast.py:269` and `:409` (also recorded in
  `doc/plans/working/longforecast_hv_convention_plan.md`).
- Postprocessing long-term entry points call `sl.load_environment()` and read
  station-selection config only:
  `apps/postprocessing_forecasts/postprocessing_operational_long_term.py:77-83`
  and `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py:81-86`.
- `apps/postprocessing_forecasts/src/api_writer.py:77-109` has only a station
  selection config loader for the write guard; it does not know quarter/season
  long-term config leads.
- `recalculate_skill_metrics.py` also uses station-selection config for
  quarter/season recalc at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:285-299`
  and `:332-346`, then calls `save_quarterly_forecast_data` / `save_seasonal_forecast_data`
  at `:314-324` and `:361-371`.

Decision:

- Avoid hardcoding Kyrgyz/Tajik values in writer code. Add an app-owned
  deployment config resolver that reads the same long-term config JSONs used by
  `apps/long_term_forecasting`, resolves `operational_month_lead_time` for the
  current quarter/season product, and threads the value into postprocessing
  readers/writers.
- Owner/modeller decision required before implementation: confirm the exact config
  root/env contract for postprocessing (`ieasyforecast_configuration_path` plus
  the long-term config directory env) and the seasonal issue selector mapping
  from issue month to `seasonal_january` / `seasonal_february` /
  `seasonal_march` / `seasonal_april`.

### 3. Read side must move in lockstep

Evidence:

- `_read_long_forecasts_api(...)` accepts `horizon_type` but does not accept or
  pass `horizon_value` to `client.read_long_term_forecasts` at
  `apps/postprocessing_forecasts/src/data_reader.py:1026-1074`.
- Quarterly direct reads call it without an hv filter at
  `apps/postprocessing_forecasts/src/data_reader.py:2657` and `:2798`.
- Seasonal direct/latest reads call it without an hv filter at
  `apps/postprocessing_forecasts/src/data_reader.py:2726` and `:2869`.
- Combined quarter/season reads use `_read_long_combined_forecasts_api`, which
  also calls `client.read_long_term_forecasts` without `horizon_value` at
  `apps/postprocessing_forecasts/src/data_reader.py:2956-3022`.
- Normalization derives `quarter_in_year` from `valid_from` and then drops
  `horizon_value` at `apps/postprocessing_forecasts/src/data_reader.py:3055-3080`.
- Dashboard quarter getter defaults to `horizon_value=1` and passes it to the API
  at `apps/forecast_dashboard/src/db.py:600-609`.
- Dashboard seasonal getter passes no `horizon_value` filter at
  `apps/forecast_dashboard/src/db.py:648-660`.
- Dashboard monthly data always fetches quarter `hv1` at
  `apps/forecast_dashboard/src/db.py:806`; bulletin manager also hardcodes
  quarter `hv1` at `apps/forecast_dashboard/dashboard/bulletin_manager.py:168`
  and calls unfiltered seasonal at `:186`, `:411`.

Decision:

- P-PIPE must update read paths in the same release as writes. The postprocessing
  data reader should support deployment/config-aware `horizon_value` filters for
  quarter and season, and should preserve enough issue identity for seasonal
  selection before dropping API-only columns.
- The dashboard must stop assuming `quarter hv1` globally. It needs the same
  deployment-aware resolver so Kyrgyz quarter reads `hv1`, Tajik quarter reads
  `hv0`, and seasonal reads the issue-appropriate lead. This resolves the SF-2
  Tajik `hv0` dashboard gap after data regeneration.

### 4. Skill metrics alignment

Evidence:

- `_write_skill_metrics_to_api` maps quarter/season to `horizon_in_year_col` at
  `apps/postprocessing_forecasts/src/api_writer.py:475-487`, computes skill
  metric dates from `quarter_in_year` or season start at `:543-583`, and writes
  `horizon_in_year` at `:629-636`.
- Its upsert-key comment states the skill metric key is
  `(horizon_type, code, model_type, date, horizon_in_year)` at
  `apps/postprocessing_forecasts/src/api_writer.py:585-592`; there is no
  skill-metrics `horizon_value` in this writer.
- Quarterly skill metrics group by `quarter_in_year` at
  `apps/postprocessing_forecasts/src/skill_metrics.py:2026-2052`.
- Seasonal skill metrics group by `season_year` and use single
  `season_in_year=1` at `apps/postprocessing_forecasts/src/skill_metrics.py:2060-2087`.
- The reader maps skill metric `horizon_in_year` back to `quarter_in_year` /
  `season_in_year` at `apps/postprocessing_forecasts/src/data_reader.py:2515-2524`.

Decision:

- No `skill_metrics.horizon_value` restamp is required because the skill metrics
  API path carries `horizon_in_year`, not `long_forecasts.horizon_value`.
- Do not conflate skill metric `quarter_in_year` / `season_in_year` with
  `long_forecasts.horizon_value`. However, if owner decides seasonal products
  must be issue-specific, seasonal skill/ensemble joins may need an issue/lead
  dimension or a documented rule that the same seasonal skill applies to every
  issue lead for a target season.

### 5. Tests affected

Evidence:

- Writer expectations currently assert old hv behavior in
  `apps/postprocessing_forecasts/tests/test_quarterly_api_writer.py:189-202`
  and `:222-242`.
- NaN/write-guard writer tests exercise quarter/season records in
  `apps/postprocessing_forecasts/tests/test_aggregated_nan_guard.py:50-150`
  and `:185-250`.
- Data-reader tests exercise quarter/season API reads and latest filtering in
  `apps/postprocessing_forecasts/tests/test_quarterly_data_reader.py:191-239`,
  `:294-303`, and `:334-382`.
- Ensemble and workflow tests cover quarter/season grouping in
  `apps/postprocessing_forecasts/tests/test_quarterly_ensemble_creation.py`,
  `apps/postprocessing_forecasts/tests/test_seasonal_integration.py`, and
  `apps/postprocessing_forecasts/tests/test_quarterly_workflow_integration.py`.
- Aggregation tests preserve calendar quarter and season identity in
  `apps/postprocessing_forecasts/tests/test_aggregation.py`.
- Dashboard tests contain quarter/season hv assumptions in
  `apps/forecast_dashboard/tests/test_db.py:850-878` and computed
  `quarter_in_year` / `season_in_year` checks through `:913-984`.

Decision:

- Update tests to lock the new convention using synthetic configs and sentinel
  codes only. Required coverage:
  quarter writer stamps config lead, not `quarter_in_year`; seasonal writer
  stamps lead by issue/config; API readers pass hv filters; dashboard reads Kyrgyz
  `hv1` vs Tajik `hv0`; seasonal grouping/dedup behavior is protected according
  to the owner decision from Finding 1.
- Verification command for implementation acceptance:
  `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`.
  Dashboard tests touched by read-side changes should also run in their normal
  app test command if available in the implementation branch.

### 6. Migration, regeneration, cleanup sequencing

Evidence:

- `doc/plans/working/longforecast_hv_convention_plan.md` records that
  `QUARTER hv1-4` and `SEASON hv1` are live output from this postprocessing
  ensemble pipeline, not just deprecated history.
- The same plan states P-PIPE is a hard prerequisite for cleanup because the next
  operational run would otherwise regenerate the old hv rows.
- MIG-008
  (`doc/plans/issues/mid_prio_gi_draft_migration_long_forecast_quarter_season_horizon_value.md`)
  resolves the convention and explicitly scopes `apps/postprocessing_forecasts`
  into the fix.

Decision:

- Required order: P-PIPE code -> reviewer pass -> deploy app changes -> regenerate
  quarter/season ensembles at the config-lead hv -> aggregate verification -> only
  then run the previously planned P3/P4 quarantine/cleanup/backfill work.
- Old `QUARTER hv2/3/4`, old calendar-quarter `hv1`, and old seasonal `hv1`
  rows are not safe to clean merely because the convention is known. Cleanup is
  safe only after regenerated replacement rows exist and owner-approved dry-run
  counts have been reviewed.

## Phased Plan

### PP0 - Owner Decisions and Config Contract

**Goal**: Close the two design decisions before implementation: seasonal grouping
semantics and the authoritative config source for quarter/season leads.

**Files**:

- `doc/plans/working/ppipe_postprocessing_ensemble_hv_plan.md`
- No code files.
- No `sapphire/services/**`.

**Depends-on**: none.

**Parallel agents**: 1 planner/reviewer agent. No implementation.

**Acceptance criteria**:

- Owner/modeller confirms whether seasonal postprocessing must preserve multiple
  issue products per `season_year`, or only one active issue product exists per
  run/deployment.
- Owner/modeller confirms how postprocessing should locate long-term config JSONs
  and map issue month to seasonal config name.
- The decision is recorded before PP1 starts.

### PP1 - Lead Resolver Design and Threading Points

**Goal**: Add an app-owned resolver for quarter/season config leads, and define
where those leads are passed through postprocessing.

**Files**:

- `apps/postprocessing_forecasts/src/` new small helper module or existing
  config-adjacent helper.
- `apps/postprocessing_forecasts/postprocessing_operational_long_term.py`
- `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py`
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
- Tests under `apps/postprocessing_forecasts/tests/`.
- No `sapphire/services/**`.

**Depends-on**: PP0.

**Parallel agents**: 1 implementation agent.

**Acceptance criteria**:

- Quarter lead is resolved from config, not hardcoded.
- Seasonal lead is resolved from issue/config according to PP0.
- Missing/malformed config fails loudly enough to prevent silent old-convention
  writes.
- Unit tests use temporary synthetic config files and sentinel codes.

### PP2 - Postprocessing Write Path

**Goal**: Change quarterly/seasonal long-forecast writes so
`long_forecasts.horizon_value` is the resolved config lead, while `date`,
`valid_from`, and `valid_to` continue to carry calendar/issue identity.

**Files**:

- `apps/postprocessing_forecasts/src/api_writer.py`
- `apps/postprocessing_forecasts/src/file_writer.py`
- Callers scoped in PP1 if signatures or context objects must be threaded.
- `apps/postprocessing_forecasts/tests/test_quarterly_api_writer.py`
- `apps/postprocessing_forecasts/tests/test_aggregated_nan_guard.py`
- No `sapphire/services/**`.

**Depends-on**: PP1.

**Parallel agents**: 1 implementation agent.

**Acceptance criteria**:

- Quarter writer no longer uses `horizon_value = quarter_in_year`.
- Seasonal writer no longer uses hardcoded `horizon_value = 1`.
- Writer tests assert Kyrgyz-style quarter `hv1`, Tajik-style quarter `hv0`, and
  seasonal issue leads `3/2/1/0` using synthetic config.
- Existing `valid_from` / `valid_to` override behavior remains intact.

### PP3 - Ensemble, Gap, and Dedup Keys

**Goal**: Make ensemble creation, gap detection, and merge/dedup keys safe under
constant deployment hv for quarter and per-issue hv for season.

**Files**:

- `apps/postprocessing_forecasts/src/ensemble_calculator.py`
- `apps/postprocessing_forecasts/src/gap_detector.py`
- `apps/postprocessing_forecasts/postprocessing_operational_long_term.py`
- `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py`
- `apps/postprocessing_forecasts/tests/test_quarterly_ensemble_creation.py`
- `apps/postprocessing_forecasts/tests/test_quarterly_gap_detector.py`
- `apps/postprocessing_forecasts/tests/test_seasonal_integration.py`
- `apps/postprocessing_forecasts/tests/test_quarterly_workflow_integration.py`
- No `sapphire/services/**`.

**Depends-on**: PP0, PP1.

**Parallel agents**: 1 implementation agent.

**Acceptance criteria**:

- Quarterly grouping continues to distinguish `year` + `quarter_in_year` when
  hv is constant.
- Seasonal grouping and dedup behavior implements the PP0 owner decision and has
  regression coverage for multiple issue leads in the same `season_year`, if that
  case is supported.
- No monthly PP-032 behavior regresses; monthly `horizon_value` grouping remains
  separate.

### PP4 - Postprocessing Read Path

**Goal**: Filter postprocessing quarter/season reads by the same resolved config
lead used by writes, and preserve issue identity long enough for selection.

**Files**:

- `apps/postprocessing_forecasts/src/data_reader.py`
- Config resolver from PP1.
- `apps/postprocessing_forecasts/tests/test_quarterly_data_reader.py`
- `apps/postprocessing_forecasts/tests/test_seasonal_integration.py`
- No `sapphire/services/**`.

**Depends-on**: PP1, PP3.

**Parallel agents**: 1 implementation agent.

**Acceptance criteria**:

- `_read_long_forecasts_api` and `_read_long_combined_forecasts_api` can pass
  `horizon_value` to `client.read_long_term_forecasts` for quarter/season.
- Quarter direct/latest reads filter to deployment lead (`hv1` Kyrgyz, `hv0`
  Tajik) without losing `quarter_in_year` derived from `valid_from`.
- Seasonal reads select the issue lead required by PP0 and do not mix issue
  products unexpectedly.
- Tests confirm API client calls include the expected hv filter.

### PP5 - Dashboard Read Lockstep

**Goal**: Make dashboard quarter/season getters deployment-aware so consumers
see the new buckets immediately after regeneration.

**Files**:

- `apps/forecast_dashboard/src/db.py`
- `apps/forecast_dashboard/dashboard/bulletin_manager.py`
- `apps/forecast_dashboard/tests/test_db.py`
- Any small dashboard config helper needed to reuse the PP1 convention without
  importing postprocessing internals in a brittle way.
- No `sapphire/services/**`.

**Depends-on**: PP1, PP4.

**Parallel agents**: 1 implementation agent.

**Acceptance criteria**:

- `get_long_forecasts_quarter` no longer defaults every deployment to `hv1`.
- Dashboard monthly payload and bulletin paths no longer hardcode quarter `hv1`.
- Seasonal dashboard reads use the selected issue lead rather than unfiltered
  `horizon_type="season"` when PP0 requires issue-specific selection.
- Tests cover Tajik quarter `hv0` and Kyrgyz quarter `hv1`.

### PP6 - Verification, Deploy, Regenerate, Then Cleanup

**Goal**: Verify the implementation, deploy, regenerate live ensemble rows at
the new hv convention, and unblock the existing cleanup plan.

**Files**:

- Code files changed in PP1-PP5.
- Deployment/runbook notes in `doc/plans/working/longforecast_hv_convention_plan.md`
  or a production runbook if the reviewer requests it.
- No `sapphire/services/**`.

**Depends-on**: PP2, PP3, PP4, PP5.

**Parallel agents**: 1 verification/release agent.

**Acceptance criteria**:

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
  is green.
- Dashboard tests touched by PP5 are green.
- Reviewer confirms no service-owned files changed.
- After deploy, quarter/season ensembles are regenerated for each deployment.
- Aggregate-only verification shows new rows in the expected buckets:
  Kyrgyz quarter `hv1`, Tajik quarter `hv0`, Kyrgyz season `hv3/hv2/hv1/hv0`
  by issue, Tajik season `hv0`.
- Only after that verification may the data-cleanup P3/P4 work from the
  longforecast convention plan proceed.

## Regeneration and Cleanup Ordering

1. Implement and test P-PIPE in `apps/`.
2. Reviewer pass with special attention to seasonal grouping semantics and
   read/write hv lockstep.
3. Deploy P-PIPE.
4. Regenerate quarterly and seasonal ensembles so `long_forecasts` has the
   config-lead buckets.
5. Run aggregate-only acceptance queries; do not expose real station codes or
   discharge values in the plan artifacts.
6. Proceed to P3/P4 cleanup/backfill from
   `doc/plans/working/longforecast_hv_convention_plan.md` only after owner
   sign-off and reviewed dry-run counts.

## Dependency Graph

```json
{
  "phases": {
    "PP0": { "depends_on": [], "parallel_agents": 1 },
    "PP1": { "depends_on": ["PP0"], "parallel_agents": 1 },
    "PP2": { "depends_on": ["PP1"], "parallel_agents": 1 },
    "PP3": { "depends_on": ["PP0", "PP1"], "parallel_agents": 1 },
    "PP4": { "depends_on": ["PP1", "PP3"], "parallel_agents": 1 },
    "PP5": { "depends_on": ["PP1", "PP4"], "parallel_agents": 1 },
    "PP6": { "depends_on": ["PP2", "PP3", "PP4", "PP5"], "parallel_agents": 1 },
    "P3/P4-data-cleanup": {
      "depends_on": ["PP6", "deploy", "regenerate-ensembles", "owner-signoff"],
      "parallel_agents": 1
    }
  }
}
```
