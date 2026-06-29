# P-PIPE: Postprocessing Quarterly/Seasonal Ensemble `horizon_value` Plan

Branch: `fix_migration_long_forecast_multihorizon`
Scope: B branch from PP0: seasonal issue-threading, per-lead seasonal skill, quarter config-lead
alignment, read-side lockstep, and regenerate-then-clean sequencing.
Status: plan only; no implementation, no DB writes.

`apps/` modules are in scope. Do not edit `sapphire/services/**`; service files may be read for
contract evidence only. If an implementation phase discovers that a service schema/API contract must
change, stop and raise a coordination blocker.

P-PIPE is a hard prerequisite for the data cleanup phases in
`doc/plans/archive/longforecast_hv_convention_plan.md`. Cleanup P3/P4 must not run until P-PIPE is
coded, reviewed, deployed, the one-time full-history quarterly/seasonal recalc has written
new-convention rows for each deployment, aggregate verification passes, and obsolete old-hv rows are
then cleaned.

## Per-lead Skill Feasibility Verdict

**Verdict: no `skill_metrics` schema change is needed for B.** Seasonal per-lead skill can be carried
by reinterpreting `season_in_year` / API `horizon_in_year` as the seasonal lead (`3/2/1/0`) for
`horizon_type="season"`. This stays in app-owned code and does not require `sapphire/services/**`
edits.

Evidence:

- The service model already stores `horizon_in_year` on `skill_metrics` and includes it in the unique
  key: `sapphire/services/postprocessing/app/models.py:196-235`.
- The service create/upsert path keys on `(horizon_type, code, model_type, date, horizon_in_year)`:
  `sapphire/services/postprocessing/app/crud.py:277-313`.
- The service schema accepts `horizon_in_year` as an integer field without a season-specific enum:
  `sapphire/services/postprocessing/app/schemas.py:161-188`.
- The app writer already maps seasonal `season_in_year` to API `horizon_in_year`:
  `apps/postprocessing_forecasts/src/api_writer.py:475-487` and writes it at `:629-636`.
- Current seasonal skill calculation uses `period_col="season_in_year"` but hardcodes the old
  single-season semantics via `time_group_cols=["season_year","code"]` and
  `merge_cols=["code","season_year"]`: `apps/postprocessing_forecasts/src/skill_metrics.py:2060-2087`.
- Skill readers already map API `horizon_in_year` back to `season_in_year`:
  `apps/postprocessing_forecasts/src/data_reader.py:2511-2528`.

Decision:

- For `horizon_type="season"`, `season_in_year` becomes the lead dimension, not a literal "season
  number". Seasonal skill rows for the same target season use the same target-season `date`, while
  `horizon_in_year=3/2/1/0` separates Jan/Feb/Mar/Apr lead skill in the existing DB key.
- The implementation must update seasonal skill calculation, skill writing, skill reading, and
  skill-to-ensemble joins together so every layer keys on lead. If a reviewer rejects this semantic
  reuse of `horizon_in_year`, that becomes a service-owner coordination blocker; the current evidence
  does not require it.

## Review Outcome (2026-06-22): GO-WITH-CHANGES -- PP0 now answered

A critical review verified every file:line and surfaced must-fixes that PP0 has now resolved. The
implementation branch is B:

- **MF-1: seasonal issue identity must be threaded through every layer.** The raw seasonal rows carry
  distinct issue dates upstream, but the postprocessing reader currently destroys that identity:
  `_SEASONAL_FC_COLS` excludes `date` and `horizon_value`
  (`apps/postprocessing_forecasts/src/data_reader.py:37-52`); seasonal normalization derives
  `season_year` from `valid_from`, hardcodes `season_in_year = 1`, and drops `horizon_value`
  (`data_reader.py:3059-3080`). The ensemble groups by `["season_year","code"]`
  (`ensemble_calculator.py:553-558`), both operational and maintenance dedup on
  `["season_year","code","model_short"]` (`postprocessing_operational_long_term.py:200-212`,
  `postprocessing_maintenance_long_term.py:330-343`), and the writer overwrites the issue date with
  target `valid_from` while hardcoding `horizon_value = 1` (`api_writer.py:1052-1079`). B requires
  preserving an issue dimension through reader projection, ensemble grouping, dedup, writer date, and
  per-row issue->lead resolution.
- **MF-2: regenerate before cleanup.** PP0 chose regenerate, not re-stamp as the primary ensemble
  correction. Sequence is P-PIPE code -> deploy -> full-history recalc with
  `SAPPHIRE_RECALC_START_YEAR=2000` per deployment -> aggregate verify -> cleanup obsolete old-hv
  rows. Cleanup P3/P4 in `longforecast_hv_convention_plan.md` is blocked until that gate passes.
- **MF-3: acceptance is model-class specific.** Raw LR rows are produced by `apps/long_term_forecasting`
  / from-file importer and already use config lead. P-PIPE changes the EM / Naive Mean / Skilled Mean
  postprocessing ensemble rows so they land in the same buckets as the raw rows.
- **SF-1: complete consumers and add CI guard.** Consumer changes include
  `apps/forecast_dashboard/src/db.py:600-609`, `:806`, `:867`,
  `apps/forecast_dashboard/dashboard/bulletin_manager.py:168`, `:186`, `:393`, `:411`, `:495`, and a
  quarter/season presence assertion in `apps/validate_pipeline/validate_pipeline.py:493-522` so empty
  buckets fail CI.
- **SF-2: seasonal readers must not duplicate issues before PP-ensemble.** `read_seasonal_forecasts`
  and `read_latest_seasonal_forecasts` (`data_reader.py:2696-2750`, `:2848-2878`) currently read
  unfiltered seasonal rows and do not dedup per lead. The B plan puts issue/lead preservation and
  filtering ahead of ensemble calculation.
- **SF-3: shared resolver belongs outside dashboard internals.** The resolver should live in
  `apps/iEasyHydroForecast` or an equivalent env-var contract read by each module, mirroring
  `get_season_months()` / `SAPPHIRE_SEASON_*` in
  `apps/postprocessing_forecasts/src/aggregation.py:48-66`. Do not import
  `forecast_dashboard -> postprocessing_forecasts`.

## Review 2 (2026-06-22): GO-WITH-CHANGES -- required text fixes integrated

A second critical review confirmed the load-bearing claims (regen writes ensemble rows via
`save_quarterly/seasonal_forecast_data` -> the same `api_writer` PP4 edits; the per-lead-skill schema
reuse is sound -- `skill_metrics` key `(horizon_type, code, model_type, date, horizon_in_year)`
`models.py:228-235`/`crud.py:281-313`, `horizon_in_year` a bare int, no service `==1` assumption). One
Must-fix + targeted Should-fixes, folded in here:

- **MF-A (cross-plan): regenerate and cleanup are not reconciled** -- see the dedicated section below.
  This is the gate; resolve in text before any PP4/PP7 code.
- **SF-1: per-lead seasonal skill never reaches the dashboard without two unlisted changes.**
  `_get_data_season` (`db.py:913-927`) left-merges seasonal forecasts (pinned to `season_in_year=1` at
  `db.py:681`) against per-lead skill (`0-3`) on `["code","season_in_year","model_short"]`, so only the
  lead-1 (March) skill survives. **PP6 scope expands** to `db.py:681` + `db.py:913-927`, plus a test
  asserting all four leads' skill survive the merge; also remove the forced-`1` assumption at
  `aggregation.py:198` and the `skill_metrics.py:2068` docstring ("season_in_year is always 1").
- **SF-2: fix the recalc helper, don't rely on operator `-e`.** `bin/utils/run_skill_metrics_recalc.sh`
  forwards `SAPPHIRE_PREDICTION_MODE` (`:79`) but NOT `SAPPHIRE_RECALC_START_YEAR`, and uses
  `docker run` without `--env-file`, so the one-time deep recalc silently falls to the
  `current_year-20` (~2006) default. **PP7 scope adds** a contained `bin/`-only fix: conditionally pass
  `-e SAPPHIRE_RECALC_START_YEAR=...` (guarded so an unset value is not passed as an empty string that
  breaks `int()`).
- **SF-3 (reframe):** the four seasonal issues fold today because there is **no lead/issue carrier
  column** in the seasonal frame (`_SEASONAL_FC_COLS` lacks `date`/`horizon_value`
  `data_reader.py:37-52`; `_normalize_combined_forecasts` hardcodes `season_in_year=1` `:3064`;
  groupby `["season_year","code"]`), NOT because of latest-filtering (`read_latest_seasonal_forecasts`
  `:2848-2902` does not collapse). PP2 must target the carrier column.
- **SF-4:** "quarter is a small *writer* change" is true, but collapsing the 4 calendar quarters to a
  single `hv1` is exactly what creates the old `hv2/3/4` orphan population that MF-A cleanup must
  remove -- a small writer change, not a small overall change.
- **N1:** start-year precedence is three-level: `SAPPHIRE_SKILL_METRICS_START_YEAR` ->
  `SAPPHIRE_RECALC_START_YEAR` -> `current_year-20` (`recalculate_skill_metrics.py:287-292,334-339`).
- **N2:** the PP4 gap-detector test must assert a per-lead gap (one missing issue among four) is
  detected. **N4 (impl hint):** the cleanest seasonal lead source is the raw row's own
  `horizon_value` (already `3/2/1/0` from the importer), preferred over re-deriving from issue month.

## Cross-plan cleanup reconciliation (MF-A) -- supersedes longforecast P3 ensemble handling

The cleanup in `doc/plans/archive/longforecast_hv_convention_plan.md` P3 was designed pre-regen and now
**conflicts** with the PP0 regenerate decision:

- P3 step 2 `DELETE ... horizon_type='QUARTER' AND model_type NOT IN ('LR_BASE','LR_SM')` would delete
  the EM/Naive/Skilled **ensembles that PP0 chose to regenerate** and PP7 writes into `QUARTER hv1`.
- Old vs new rows mostly **coexist, not overwrite** (old: `hv1`/`date=target-start`; new: per-issue
  `hv`/`date=issue`), so a cleanup is still required -- but on a **different row population** than P3
  measured, and its pre-regen counts (2,890 / ~41,225 / 10,665) are now **stale**.
- Strategy conflict: longforecast SIGNOFF set Dataset B = "delete deprecated + re-stamp LR"; PP0
  Decision 3 = "regenerate the EM/Naive/Skilled ensembles". **PP0 regenerate wins for the ensemble
  model class.**

Resolution (authoritative; longforecast P3 ensemble handling is superseded by this):

1. **Cleanup is re-derived POST-regen**, against a fresh aggregate snapshot, scoped to the
   **old-convention signature** so predicates can never match regenerated rows:
   - season: old ensemble rows at `hv1` with `date == valid_from` (target-season start) -- new rows
     have `date == issue` and per-issue `hv`;
   - quarter: old ensemble orphans at `hv2/3/4`, and old calendar-`hv1` rows whose `date == quarter
     start` distinct from the regenerated product.
2. **Ordering decided**: regen-first is allowed ONLY with old-signature-specific deletes (option b);
   the current blanket `model_type NOT IN (...)` delete is NOT safe post-regen. (Alternative: delete
   old ensembles BEFORE regen, then let recalc write only the new convention.)
3. **Raw-LR carve-out (stated once, here):** P-PIPE never moves raw `LR_BASE`/`LR_SM` rows; the
   re-stamp of historical raw LR `hv2/3/4 -> hv1` is a **cleanup op** (longforecast P3 step 3), not a
   P-PIPE op -- compatible, but owned by the cleanup plan.
4. **Counts re-measured**: the longforecast P3 collision/row counts must be re-verified against the
   post-regen state before any delete; the pre-regen numbers are recorded as stale.

PP7 cleanup gate is therefore: regen -> aggregate-verify new-convention rows exist for the full date
range -> **re-derive + reviewer-approve old-signature-scoped delete predicates** -> execute. The
longforecast cleanup plan now carries a banner pointing here.

## Target Convention

`long_forecasts.horizon_value = operational_month_lead_time` from the deployment's long-term config
for that product.

- Quarter: one product per deployment. Kyrgyz -> `hv1`; Tajik -> `hv0`.
- Season: one product per issue month. Kyrgyz Jan/Feb/Mar/Apr -> `hv3/hv2/hv1/hv0`; Tajik April-only
  -> `hv0`.
- Seasonal B branch: four distinct ensemble products per target season, computed independently per
  issue. The seasonal issue date is the `long_forecasts.date`; target season coverage remains in
  `valid_from` / `valid_to`.
- Seasonal skill: per-lead. `season_in_year` / `horizon_in_year` is the lead (`3/2/1/0`) for
  `horizon_type="season"` in skill metrics and joins.

Use sentinel station codes only in tests and docs. Any DB verification must be aggregate-only.

## Findings

### 1. Issue identity, grouping, and write semantics

Evidence:

- Seasonal reader projection omits issue `date` and `horizon_value`:
  `apps/postprocessing_forecasts/src/data_reader.py:37-52`.
- Seasonal API reads are unfiltered by lead:
  `apps/postprocessing_forecasts/src/data_reader.py:1026-1074`, `:2726`, `:2869`, `:2988-3009`.
- `_normalize_combined_forecasts(..., "season")` derives only the target-season year, sets
  `season_in_year = 1`, and drops `horizon_value`: `data_reader.py:3059-3080`.
- Seasonal ensemble grouping omits issue/lead:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:553-558`.
- Operational and maintenance seasonal dedup omit issue/lead:
  `apps/postprocessing_forecasts/postprocessing_operational_long_term.py:200-212`,
  `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py:330-343`.
- Seasonal writer computes target `valid_from` / `valid_to`, hardcodes `horizon_value = 1`, and writes
  `date = valid_from`: `apps/postprocessing_forecasts/src/api_writer.py:1052-1079`.

Decision:

- Preserve `date` and `horizon_value` from raw seasonal API rows in postprocessing normalized frames.
  Store the issue identity under a clear app-side column such as `issue_date` plus `season_lead`, or
  preserve `date` directly until the writer builds API records.
- For seasonal B products, ensemble time groups and dedup keys must include the lead/issue dimension:
  at minimum `["season_year", "season_in_year", "code"]` for calculation and
  `["season_year", "season_in_year", "code", "model_short"]` for dedup, with `date` preserved per
  group.
- The writer must use each row's issue date for API `date` and use the per-row resolved lead for
  API `horizon_value`. It must not replace the issue date with target `valid_from`.

### 2. Per-lead seasonal skill representation

Evidence:

- Skill writer selects `season_in_year` as the seasonal API `horizon_in_year` source:
  `apps/postprocessing_forecasts/src/api_writer.py:475-487`, `:629-636`.
- Its upsert-key comment and implementation dedup on `code`, `model_type`, `_date`, and
  `season_in_year`: `api_writer.py:585-595`.
- Current seasonal skill groups by `season_in_year`, `code`, `model_short` but computes only one
  lead because readers/calculation set `season_in_year = 1`:
  `apps/postprocessing_forecasts/src/skill_metrics.py:2060-2087`, `:2115-2163`.
- The ensemble skill join already keys on `[period_col, "code", "model_short"]`:
  `apps/postprocessing_forecasts/src/ensemble_calculator.py:593-608`, `:687-703`.

Decision:

- No schema change. Update seasonal forecast frames so `season_in_year` is the resolved lead before
  seasonal skill calculation. Then the existing skill key and ensemble join shape can separate Jan,
  Feb, Mar, and Apr lead skill.
- Update seasonal observations/forecast merge for skill calculation to merge target-season
  observations to each lead-specific forecast row by `["code","season_year"]`, while metrics group by
  `["season_in_year","code","model_short"]`. This gives per-lead skill while reusing the same target
  observed season.
- Writer dates for seasonal skill may remain target-season-start dates; `horizon_in_year` separates
  the lead in the DB key. Do not introduce a service-owned `horizon_value` column for skill metrics.

### 3. Lead/issue resolver home and config contract

Evidence:

- Long-term forecast configs load from `ieasyforecast_configuration_path` plus
  `ieasyhydroforecast_ml_long_term_configuration`:
  `apps/long_term_forecasting/config_forecast.py:37-65`.
- The authoritative lead is `get_operational_month_lead_time()`:
  `apps/long_term_forecasting/config_forecast.py:230-231`; horizon type is
  `get_horizon_type()` at `:269-276`.
- Operational long-term writes already use this lead:
  `apps/long_term_forecasting/run_forecast.py:409-410`.
- Postprocessing entry points currently call `sl.load_environment()` and load only station-selection
  config: `apps/postprocessing_forecasts/postprocessing_operational_long_term.py:56-83`,
  `apps/postprocessing_forecasts/recalculate_skill_metrics.py:111-119`, `:197-203`.
- The existing season-month contract is an env-var helper in app code:
  `apps/postprocessing_forecasts/src/aggregation.py:48-66`.

Decision:

- Add the resolver in `apps/iEasyHydroForecast` as shared app-owned functionality, or add a small
  env-var/config contract there that each module reads. Do not define it in `forecast_dashboard` or
  import dashboard code from postprocessing.
- Contract:
  - Config root: `ieasyforecast_configuration_path`.
  - Long-term config directory: `ieasyhydroforecast_ml_long_term_configuration`.
  - Quarter mode: `quarter`.
  - Seasonal issue-month mapping: January -> `seasonal_january`, February -> `seasonal_february`,
    March -> `seasonal_march`, April -> `seasonal_april`.
  - Lead source: the JSON field `operational_month_lead_time`.
  - Deployment awareness: supported modes come from
    `ieasyhydroforecast_ml_long_term_supported_modes`; if a deployment lacks Jan/Feb/Mar seasonal
    modes, the resolver must not synthesize them.
- Missing/malformed config should fail loudly for writes and tests. Readers may return empty with a
  logged warning only where existing dashboard behavior requires graceful degradation.

### 4. Quarter half

Evidence:

- Quarterly reader/ensemble/dedup keys retain calendar quarter:
  `data_reader.py:2675-2680`, `:2816-2819`;
  `ensemble_calculator.py:526-531`;
  `postprocessing_operational_long_term.py:164-179`;
  `postprocessing_maintenance_long_term.py:276-285`.
- Quarterly writer currently sets `horizon_value = quarter_in_year`:
  `apps/postprocessing_forecasts/src/api_writer.py:1043-1051`.
- The resolved convention says quarter is one config-lead product per deployment, not calendar
  quarter encoded in `horizon_value`.

Decision:

- Quarter is a small writer/reader change compared with season. Set API `horizon_value` to the
  resolved deployment quarter lead while preserving `year`, `quarter_in_year`, `valid_from`, and
  `valid_to`.
- Keep quarter grouping/dedup keys as `year + quarter_in_year + code (+ model_short)`; do not add
  lead to those keys unless needed for API read filtering metadata.

### 5. Read-side lockstep and consumers

Evidence:

- `_read_long_forecasts_api` and `_read_long_combined_forecasts_api` do not pass `horizon_value` to
  the API: `apps/postprocessing_forecasts/src/data_reader.py:1026-1074`, `:2956-3022`.
- Dashboard quarter getter defaults to `horizon_value=1`:
  `apps/forecast_dashboard/src/db.py:600-609`.
- Dashboard monthly data hardcodes quarter `hv1`: `apps/forecast_dashboard/src/db.py:806`.
- Dashboard quarter horizon implicitly gets `hv1` through the default:
  `apps/forecast_dashboard/src/db.py:867`.
- Seasonal dashboard getter is unfiltered: `apps/forecast_dashboard/src/db.py:648-660`.
- Bulletin code hardcodes quarter `hv1` and uses unfiltered season reads:
  `apps/forecast_dashboard/dashboard/bulletin_manager.py:168`, `:186`, `:393`, `:411`, `:495`.
- Long-term validation currently checks only monthly long forecasts and monthly skill metrics:
  `apps/validate_pipeline/validate_pipeline.py:493-522`.

Decision:

- Postprocessing readers, dashboard getters, monthly dashboard enrichment, quarter dashboard horizon,
  seasonal dashboard horizon, and bulletin paths must resolve and pass the same deployment/issue
  lead used by writers.
- Add a validation presence assertion for quarter and season buckets so an empty regenerated bucket
  fails CI. The assertion must be aggregate/presence only and must not log real station codes.

### 6. Two-writer alignment

Evidence:

- Long-term forecasting already reads/writes dependencies and forecast rows using config lead:
  `apps/long_term_forecasting/run_forecast.py:269`, `:409-410`.
- MIG-008 records that the from-file importer also stamps the config lead and that service code is
  hv-agnostic:
  `doc/plans/issues/mid_prio_gi_draft_migration_long_forecast_quarter_season_horizon_value.md`.
- Postprocessing writer currently diverges for quarter/season:
  `apps/postprocessing_forecasts/src/api_writer.py:1043-1067`.

Decision:

- Raw LR rows and EM / Naive Mean / Skilled Mean rows must land in the same natural-key bucket for
  the same product:
  `(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`.
- Seasonal B alignment means the ensemble writer must retain the raw issue `date` and use that issue
  lead. For Kyrgyz Jan/Feb/Mar/Apr target-season products, raw LR and ensemble rows coexist at
  `hv3/hv2/hv1/hv0` respectively.

### 7. Regenerate-then-clean sequence

Evidence:

- `recalculate_skill_metrics.py` supports `QUARTERLY` and `SEASONAL`:
  `apps/postprocessing_forecasts/recalculate_skill_metrics.py:91-92`, with quarter work at `:285-330`
  and season work at `:332-371`.
- Its default history window is `current_year - 20` unless `SAPPHIRE_RECALC_START_YEAR` is set:
  `apps/postprocessing_forecasts/recalculate_skill_metrics.py:239-246`, `:286-293`, `:333-340`.
- The recurring bimonthly long-term skill recalc wrapper runs `MONTHLY`, `QUARTERLY`, and `SEASONAL`:
  `bin/bimonthly_long_term_skill_metrics_recalculation.sh:100-107`; yearly recalc delegates to the
  same helper: `bin/yearly_skill_metrics_recalculation.sh:98-107`.
- The shared Docker helper forwards `SAPPHIRE_PREDICTION_MODE` but not
  `SAPPHIRE_RECALC_START_YEAR`: `bin/utils/run_skill_metrics_recalc.sh:72-87`.

Decision:

- No new cron. The recurring bimonthly/yearly drivers remain the forward maintenance path.
- The one-time deep history recalc must explicitly pass `SAPPHIRE_RECALC_START_YEAR=2000` into the
  deployed postprocessing runtime for each deployment and each mode:
  - `SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=QUARTERLY uv run recalculate_skill_metrics.py`
  - `SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=SEASONAL uv run recalculate_skill_metrics.py`
- If operators use the existing Docker wrapper for the one-time run, they must first ensure the
  helper forwards `SAPPHIRE_RECALC_START_YEAR`; otherwise use an explicit container command with
  `-e SAPPHIRE_RECALC_START_YEAR=2000`. Do not treat the current wrapper invocation alone as a
  full-history gate.
- Cleanup P3/P4 is blocked until aggregate counts prove the new-convention EM / Naive Mean / Skilled
  Mean rows exist for the full expected date range.

### 8. Tests affected

Required coverage:

- Shared resolver tests with synthetic configs and supported-mode lists.
- Writer tests: quarter config-lead; seasonal per-issue leads; seasonal writer keeps issue date.
- Reader tests: quarter and season API calls pass hv filters; seasonal normalization preserves
  issue date and lead.
- Ensemble/dedup tests: four seasonal issues in the same `season_year` produce four independent
  ensemble products; no duplicate fan-out from unfiltered seasonal readers.
- Skill tests: seasonal skill rows and skill-to-ensemble joins key on lead.
- Dashboard tests: Kyrgyz quarter `hv1`, Tajik quarter `hv0`, seasonal issue lead selection, monthly
  quarter-card paths, bulletin paths.
- Validation tests: empty quarter/season bucket fails presence check.

Verification commands for implementation acceptance:

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
- Dashboard tests touched by PP5/PP6 in their normal app test command.
- Validate-pipeline focused tests for the new long-term quarter/season presence guard.

## Phased Plan

### PP0 - Decisions Recorded and Feasibility Gate

**Goal**: Freeze PP0 B-branch decisions and the no-schema-change per-lead skill verdict before any
implementation agent starts.

**Files**:

- `doc/prod/ppipe_seasonal_ensemble_decisions_request.md`
- `doc/plans/archive/ppipe_postprocessing_ensemble_hv_plan.md`
- No code files.
- No `sapphire/services/**`.

**Depends-on**: none.

**Agents**: 1 planner/reviewer agent; no implementation.

**Acceptance criteria**:

- PP0 answers are recorded as B: four distinct seasonal ensemble products per target season, computed
  independently per issue.
- Per-lead skill feasibility is recorded as no service schema change, using seasonal
  `horizon_in_year` / `season_in_year` as lead.
- Any reviewer objection to reusing `horizon_in_year` is captured as a service-owner coordination
  blocker before PP1 starts.

### PP1 - Shared Lead and Issue Resolver

**Goal**: Add shared app-owned resolution of quarter/season lead and seasonal issue mode, using the
long-term config JSONs and supported-mode list.

**Files**:

- `apps/iEasyHydroForecast/` shared helper module or `setup_library.py`-adjacent helper.
- Tests under `apps/iEasyHydroForecast/tests/`.
- Import call sites only as needed in later phases.
- No `sapphire/services/**`.

**Depends-on**: PP0.

**Agents**: 1 implementation agent.

**Acceptance criteria**:

- Resolver reads `ieasyforecast_configuration_path` and
  `ieasyhydroforecast_ml_long_term_configuration`.
- Resolver maps quarter -> `quarter` and issue months Jan/Feb/Mar/Apr ->
  `seasonal_january/february/march/april`.
- Resolver reads `operational_month_lead_time` from the selected JSON.
- Resolver respects `ieasyhydroforecast_ml_long_term_supported_modes`; unsupported seasonal issues
  are reported and not synthesized.
- Missing or malformed config fails loudly for write/recalc paths.
- Unit tests use temporary synthetic config files and sentinel values only.

### PP2 - Seasonal Identity Preservation in Postprocessing Readers

**Goal**: Preserve raw seasonal issue identity and lead before any ensemble or skill work consumes
seasonal frames.

**Files**:

- `apps/postprocessing_forecasts/src/data_reader.py`
- Tests under `apps/postprocessing_forecasts/tests/` for seasonal data reader and integration paths.
- Shared resolver from PP1.
- No `sapphire/services/**`.

**Depends-on**: PP1.

**Agents**: 1 implementation agent.

**Acceptance criteria**:

- `_SEASONAL_FC_COLS` or equivalent normalized output includes issue `date` and a lead field
  (`season_in_year` as lead and/or explicit `season_lead`) through the ensemble boundary.
- `_read_long_forecasts_api` and `_read_long_combined_forecasts_api` can pass `horizon_value` for
  quarter/season reads.
- Seasonal normalization no longer hardcodes all rows to `season_in_year = 1`; it maps raw
  `horizon_value` / issue date to the lead.
- Seasonal readers dedup by lead/issue where needed and do not feed duplicate issue rows into
  ensemble creation.
- Tests prove Jan/Feb/Mar/Apr issue rows for one `season_year` survive as four products.

### PP3 - Per-lead Seasonal Skill Calculation and Persistence

**Goal**: Recalculate and persist seasonal skill per lead, with skill-to-ensemble joins keying on
that lead.

**Files**:

- `apps/postprocessing_forecasts/src/skill_metrics.py`
- `apps/postprocessing_forecasts/src/api_writer.py` for skill writer behavior if needed.
- `apps/postprocessing_forecasts/src/data_reader.py` for skill reader normalization if needed.
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py` only if recalc orchestration needs
  explicit lead context.
- Tests under `apps/postprocessing_forecasts/tests/`.
- No `sapphire/services/**`.

**Depends-on**: PP2.

**Agents**: 1 implementation agent.

**Acceptance criteria**:

- Seasonal skill stats include separate rows for lead `3/2/1/0` when those issue products exist.
- Skill metric API records use `horizon_in_year=lead` for season; no service schema change.
- Seasonal skill dedup keeps separate leads for the same code/model/date.
- Seasonal ensemble skill joins include lead via `season_in_year`.
- Tests cover different MAE/skill values by lead and prove the Jan lead does not use April lead
  weights.

### PP4 - Ensemble, Gap, Dedup, and Writers

**Goal**: Produce and write independent quarter/season ensemble products under the config-lead
convention.

**Files**:

- `apps/postprocessing_forecasts/src/ensemble_calculator.py`
- `apps/postprocessing_forecasts/src/gap_detector.py`
- `apps/postprocessing_forecasts/src/api_writer.py`
- `apps/postprocessing_forecasts/src/file_writer.py` if sorting/diagnostics require lead columns.
- `apps/postprocessing_forecasts/postprocessing_operational_long_term.py`
- `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py`
- Tests under `apps/postprocessing_forecasts/tests/`.
- No `sapphire/services/**`.

**Depends-on**: PP2, PP3.

**Agents**: 1 implementation agent.

**Acceptance criteria**:

- Quarter writer uses resolved config lead, not `quarter_in_year`.
- Seasonal writer uses per-row issue lead, not hardcoded `1`.
- Seasonal writer preserves issue `date` as API `date`; `valid_from` / `valid_to` continue to describe
  the target season.
- Seasonal ensemble groupby and dedup include lead/issue, producing four independent Kyrgyz products
  for Jan/Feb/Mar/Apr when all exist.
- Gap detection checks missing seasonal ensembles per lead, not only per target season.
- Raw LR and EM / Naive Mean / Skilled Mean rows land in the same hv/date/valid range buckets for
  each product.
- Monthly PP-032 behavior remains unchanged.

### PP5 - Postprocessing Read Lockstep

**Goal**: Make postprocessing read paths deployment/lead-aware so maintenance, operational, and recalc
jobs read the same buckets written by PP4.

**Files**:

- `apps/postprocessing_forecasts/src/data_reader.py`
- `apps/postprocessing_forecasts/postprocessing_operational_long_term.py` if explicit issue selection
  is needed.
- `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py` if explicit issue selection
  is needed.
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py` if full-history recalc needs explicit
  quarter/season mode loops.
- Tests under `apps/postprocessing_forecasts/tests/`.
- No `sapphire/services/**`.

**Depends-on**: PP4.

**Agents**: 1 implementation agent.

**Acceptance criteria**:

- Quarter direct/latest/combined reads filter to deployment lead (`hv1` Kyrgyz, `hv0` Tajik) without
  losing `quarter_in_year`.
- Seasonal direct/latest/combined reads filter by the selected supported issue lead or deliberately
  iterate supported issue leads for recalc/full-history work.
- Seasonal maintenance gap-fill cannot collapse Jan/Feb/Mar/Apr products into one row.
- Tests assert API client calls include expected `horizon_value` filters and expected issue selection.

### PP6 - Dashboard and Validation Consumers

**Goal**: Update read-side app consumers so dashboards, bulletins, and CI presence checks use the new
quarter/season buckets immediately after regeneration.

**Files**:

- `apps/forecast_dashboard/src/db.py`
- `apps/forecast_dashboard/dashboard/bulletin_manager.py`
- `apps/forecast_dashboard/tests/test_db.py`
- Bulletin/dashboard focused tests as needed.
- `apps/validate_pipeline/validate_pipeline.py`
- `apps/validate_pipeline/tests/` if present, or the existing validate-pipeline test location.
- Shared resolver from PP1.
- No `sapphire/services/**`.

**Depends-on**: PP1, PP5.

**Agents**: 1 implementation agent.

**Acceptance criteria**:

- `get_long_forecasts_quarter` no longer globally defaults every deployment to `hv1`; callers either
  pass the resolved lead or the function resolves it.
- Monthly dashboard enrichment, quarter dashboard data, and bulletin quarter paths stop hardcoding
  `hv1`.
- Seasonal dashboard and bulletin paths pass the selected issue lead instead of unfiltered
  `horizon_type="season"`.
- Tests cover Tajik quarter `hv0`, Kyrgyz quarter `hv1`, and Kyrgyz seasonal issue leads.
- `validate_pipeline` Tier 1 long-term presence checks fail when expected quarter or season buckets
  are empty after deployment/recalc.

### PP7 - Verification, Deploy, Full-history Recalc, Then Cleanup Gate

**Goal**: Verify implementation, deploy it, regenerate full-history quarter/season ensembles under
the new convention, and only then unblock cleanup P3/P4.

**Files**:

- Code files changed in PP1-PP6.
- Deployment/run notes if reviewer requests them.
- `doc/plans/archive/longforecast_hv_convention_plan.md` may be updated only to record the P-PIPE
  completion gate; no DB SQL changes in this implementation phase.
- No `sapphire/services/**`.

**Depends-on**: PP4, PP5, PP6.

**Agents**: 1 verification/release agent.

**Acceptance criteria**:

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` is green.
- Dashboard tests touched by PP6 are green.
- Validate-pipeline focused tests for quarter/season presence are green.
- Reviewer confirms no `sapphire/services/**` files changed.
- Deployment sequence is explicit per deployment:
  1. Deploy P-PIPE code.
  2. Run one-time deep recalc for quarter and season with `SAPPHIRE_RECALC_START_YEAR=2000` passed
     into the runtime.
  3. Run aggregate-only verification.
  4. Only then allow cleanup P3/P4 from `longforecast_hv_convention_plan.md`.
- Aggregate verification distinguishes model classes:
  - Raw LR rows: already in config-lead buckets from long-term forecasting/from-file importer; P-PIPE
    must not move or rewrite them.
  - EM / Naive Mean / Skilled Mean rows: regenerated by postprocessing recalc into Kyrgyz quarter
    `hv1`, Tajik quarter `hv0`, Kyrgyz season `hv3/hv2/hv1/hv0` by issue, and Tajik season `hv0`.
- Cleanup P3/P4 is not authorized until regenerated-row counts cover the expected full-history date
  range and obsolete old-hv rows are identified by reviewed aggregate/dry-run counts.

## Regeneration and Cleanup Ordering

1. Implement and test PP1-PP6 in `apps/`.
2. Reviewer pass with special attention to seasonal issue identity, per-lead skill, and read/write
   lockstep.
3. Deploy P-PIPE per deployment.
4. Run one-time full-history recalc per deployment:
   - `SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=QUARTERLY uv run recalculate_skill_metrics.py`
   - `SAPPHIRE_RECALC_START_YEAR=2000 SAPPHIRE_PREDICTION_MODE=SEASONAL uv run recalculate_skill_metrics.py`
5. If using Docker, verify `SAPPHIRE_RECALC_START_YEAR=2000` is actually passed into the container.
   The current `bin/utils/run_skill_metrics_recalc.sh:72-87` helper does not pass it.
6. Run aggregate-only acceptance queries for raw LR versus EM / Naive Mean / Skilled Mean buckets; do
   not expose real station codes or discharge values in plan artifacts.
7. Proceed to cleanup P3/P4 from `doc/plans/archive/longforecast_hv_convention_plan.md` only after
   owner sign-off and reviewed dry-run counts.

## Dependency Graph

```json
{
  "phases": {
    "PP0": { "depends_on": [], "parallel_agents": 1, "type": "planner_gate" },
    "PP1": { "depends_on": ["PP0"], "parallel_agents": 1 },
    "PP2": { "depends_on": ["PP1"], "parallel_agents": 1 },
    "PP3": { "depends_on": ["PP2"], "parallel_agents": 1 },
    "PP4": { "depends_on": ["PP2", "PP3"], "parallel_agents": 1 },
    "PP5": { "depends_on": ["PP4"], "parallel_agents": 1 },
    "PP6": { "depends_on": ["PP1", "PP5"], "parallel_agents": 1 },
    "PP7": { "depends_on": ["PP4", "PP5", "PP6"], "parallel_agents": 1 },
    "longforecast_cleanup_P3": {
      "depends_on": [
        "PP7",
        "deploy",
        "quarterly_seasonal_recalc_start_year_2000",
        "aggregate_verify_new_convention_rows",
        "owner_signoff_reviewed_dry_run_counts"
      ],
      "parallel_agents": 1,
      "external_plan": "doc/plans/archive/longforecast_hv_convention_plan.md#P3"
    },
    "longforecast_cleanup_P4": {
      "depends_on": [
        "longforecast_cleanup_P3",
        "PP7",
        "aggregate_verify_new_convention_rows",
        "owner_signoff_reviewed_dry_run_counts"
      ],
      "parallel_agents": 1,
      "external_plan": "doc/plans/archive/longforecast_hv_convention_plan.md#P4"
    }
  }
}
```
