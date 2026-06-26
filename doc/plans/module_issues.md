# SAPPHIRE Unified Backlog

This file is the **single source of truth** for all tracked work: bugs, features, infrastructure, API, documentation, and architectural decisions. For detailed implementation plans, see the corresponding file in `issues/`.

For the full workflow, see [README.md](README.md).

---

## Decisions Needed

These are blocking decisions — work downstream cannot advance until they are resolved.

| ID | Decision | Impact | Context | Status |
|----|----------|--------|---------|--------|
| **D-001** | Prefect vs Airflow for orchestration replacement | Blocks all v2 orchestration work | `architecture_review_claude.md` recommends Prefect; `architecture_review_copilot.md` recommends Airflow | **Open** |
| **D-002** | CSV fallback removal criteria (Phase 6) | Blocks API integration completion | `sapphire_api_integration_plan.md` Phase 6 has no acceptance criteria defined | **Open** |
| **D-003** | Docker non-root user solution (Option A/B/C) | Blocks Docker security improvement | `docker_health_score_improvement.md` Phase 2 | **Open** |
| **D-004** | Monitoring alert strategy (Option A/B/C/D) | Blocks monitoring improvement | `monitoring_improvement_plan.md` — 4 options, no decision | **Open** |

---

## Issue Statuses

| Status | Meaning |
|--------|---------|
| Open | Known issue, not yet planned |
| Draft | Detailed plan in `issues/` directory |
| Ready | Plan reviewed, ready for implementation |
| In Progress | Being implemented |
| Review | Implementation done, tests pass, docs updated — awaiting user review |
| Complete | User approved, resolved and verified |
| Blocked | Cannot proceed (see notes) |

---

## Tier 1: Production Bugs & Orphaned Tasks

| ID | Title | Module | Priority | Status | File |
|----|-------|--------|----------|--------|------|
| ~~**INFRA-006**~~ | ~~Fix postprocessing boundary day guard, LR sentinel, and validation queries~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_infra_date_mismatch_and_sentinel.md`](issues/archive/review_gi_draft_infra_date_mismatch_and_sentinel.md) |
| ~~**ML-001**~~ | ~~Maintenance mode hindcast failure not handled, causes FileNotFoundError~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_maintenance_hindcast_file_not_found.md`](issues/archive/review_gi_draft_ml_maintenance_hindcast_file_not_found.md) | Step 5 (investigate with Sandro) is separate task |
| **ML-002** | Investigate hindcast subprocess root cause (why hindcast_ML_models.py fails) | ml | **High** | Draft | [`high_prio_gi_draft_ml_hindcast_subprocess_root_cause.md`](issues/high_prio_gi_draft_ml_hindcast_subprocess_root_cause.md) |
| ~~**ML-003**~~ | ~~Migrate maintenance scripts to API-primary reads (`fill_ml_gaps`, `recalculate_nan_forecasts`, `add_new_station`)~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_api_primary_reads.md`](issues/archive/review_gi_draft_ml_api_primary_reads.md) |
| ~~**ML-009**~~ | ~~Fix ML forecast CSV schema corruption (API column leak + corruption loop)~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_csv_schema_corruption_fix.md`](issues/archive/review_gi_draft_ml_csv_schema_corruption_fix.md) | Phase 3 (server-side CSV restoration) is ops task |
| ~~**ML-010**~~ | ~~Read old_forecast from API with CSV fallback in make_forecast.py~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_api_read_old_forecasts.md`](issues/archive/review_gi_draft_ml_api_read_old_forecasts.md) |
| ~~**ML-004**~~ | ~~Hindcast gap-fill never persists to API — silent write failure (3 bugs)~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_hindcast_api_write_broken.md`](issues/archive/review_gi_draft_ml_hindcast_api_write_broken.md) | Bugs E/F tracked as ML-007, INFRA-013 |
| ~~**ML-005**~~ | ~~ML consistency check reads forecasts without station code filter~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_pp_org_scoped_data_readers.md`](issues/archive/review_gi_draft_pp_org_scoped_data_readers.md) (Phase 4) |
| ~~**ML-006**~~ | ~~NumPy shape mismatch in recalculate_nan_forecasts `.loc` assignment~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_nan_recalc_shape_mismatch.md`](issues/archive/review_gi_draft_ml_nan_recalc_shape_mismatch.md) |
| **ML-007** | Non-deterministic API pagination causes inconsistent gap detection | ml | **Medium** | Review | [`mid_prio_gi_draft_ml_api_pagination_nondeterministic.md`](issues/mid_prio_gi_draft_ml_api_pagination_nondeterministic.md) | Option 1 (ORDER BY): colleague; Option 2 (per-code reads): done |
| ~~**ML-008b**~~ | ~~fill_ml_gaps infinite hindcast loop on null-discharge (flag=3) rows~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_fill_gaps_null_loop.md`](issues/archive/review_gi_draft_ml_fill_gaps_null_loop.md) | — |
| ~~**ML-011**~~ | ~~flag=2 semantic collision in make_forecast.py~~ | ~~ml~~ | | Deleted | — | Deleted 2026-03-20: no behavioral impact |
| ~~**ML-012**~~ | ~~recalculate_nan_forecasts crashes on NaN flag values (astype crash)~~ | ~~ml~~ | | Complete | [`archive/high_prio_gi_draft_ml_recalc_flag_astype_crash.md`](issues/archive/high_prio_gi_draft_ml_recalc_flag_astype_crash.md) | — |
| ~~**ML-013**~~ | ~~recalculate_nan_forecasts API write overwrites valid operational rows~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_ml_recalc_api_overwrite.md`](issues/archive/review_gi_draft_ml_recalc_api_overwrite.md) | — |
| ~~**ML-014**~~ | ~~Hindcast subprocess has no timeout — maintenance containers hang indefinitely~~ | ~~ml~~ | | Complete | [`archive/high_prio_gi_draft_ml_hindcast_subprocess_timeout.md`](issues/archive/high_prio_gi_draft_ml_hindcast_subprocess_timeout.md) | — |
| **ML-015** | Operational ML NaN forecasts never remediated → blank ML dashboard (MLMaintenance 900s timeout kills the 4h recalc-hindcast + not configurable; flag-convention drift) | ml | **High** | Draft | [`high_prio_gi_draft_ml_operational_nan_not_remediated.md`](issues/high_prio_gi_draft_ml_operational_nan_not_remediated.md) | Found 2026-06-15 on kghm. The 900s timeout ML-014 added is now too short + unwired from `config/timeout_config`. Related: ML-002; memo `working/ml_maintenance_timeout_investigation.md`. |
| **ML-016** | Standalone `run_locally.sh machine_learning` target crashes — empty `SAPPHIRE_PREDICTION_MODE` (ignores `ML_MODE`; unformatted `%s` in error) | ml + run_locally | **High** | Draft | [`high_prio_gi_draft_ml_standalone_target_prediction_mode.md`](issues/high_prio_gi_draft_ml_standalone_target_prediction_mode.md) | Found 2026-06-19 local review; recurring (noted in 05-05, 06-12 checklists). Workaround: `SAPPHIRE_PREDICTION_MODE=DECAD`. Production daily/all unaffected. |
| **ML-017** | A single missing ERA5 day silently zeros out ALL short-term ML forecasts (interior forcing gap → NaN cascade) | prepg + ml | **High** | Draft | [`high_prio_gi_draft_prepg_ml_era5_interior_gap_cascade.md`](issues/high_prio_gi_draft_prepg_ml_era5_interior_gap_cascade.md) | Found 2026-06-19 local review (tjhm+kghm). Upstream DG gap (2026-05-08/05-28) + no gateway interior backfill + no ML covariate guard. WS-1 ML guard first. Assigned: sandrohuni. Related: ML-002 V9, ML-015; memo `working/prepg_era5_interior_gap_memo.md`. |
| **SEC-005** | Verify bokeh>=3.8.2 compatibility post-merge | fd | **High** | Open | See `sapphire_v2_planning.md` post-merge checklist |
| **SEC-006** | Remove hardcoded DB password and add fail-fast validation for connection env vars | ltf | **High** | Review | [`review_gi_draft_ltf_remove_hardcoded_db_credentials.md`](issues/review_gi_draft_ltf_remove_hardcoded_db_credentials.md) |
| ~~**PP-002**~~ | ~~Add missing `ieasyforecast_decadal_skill_metrics_file` to .env~~ | ~~pp~~ | | Complete | Moved to Completed Issues |
| ~~**PREPQ-001**~~ | ~~Add Uzbek Hydromet (uzhm) wide-matrix Excel adapter~~ | ~~prepq~~ | | Complete | [`archive/high_prio_gi_draft_prepq_uzhm_wide_matrix_adapter.md`](issues/archive/high_prio_gi_draft_prepq_uzhm_wide_matrix_adapter.md) | — |
| **FD-001** | Fix forecast dashboard for LR-only deployments | fd | **High** | Review | [`archive/high_prio_gi_draft_fd_lr_only_deployment_fixes.md`](issues/archive/high_prio_gi_draft_fd_lr_only_deployment_fixes.md) | — |
| **FD-002** | Fix range_type label mismatch and inverted math in summary table | fd | **Mid** | Review | [`archive/mid_prio_gi_draft_fd_range_type_label_mismatch.md`](issues/archive/mid_prio_gi_draft_fd_range_type_label_mismatch.md) | ~~FD-001~~ |
| **INFRA-014** | Add deployment initialization workflow | infra | **High** | Review | [`review_gi_draft_infra_new_deployment_initialization.md`](issues/review_gi_draft_infra_new_deployment_initialization.md) | FD-001 (partial) |
| **P-004** | Fix silent timeout failure in `execute_with_retries` — timed-out tasks marked DONE | pipeline | **High** | Review | [`high_prio_gi_draft_pipeline_disable_unfulfilled_deps_check.md`](issues/high_prio_gi_draft_pipeline_disable_unfulfilled_deps_check.md) |
| **MIG-002** | Runbook §10 rollback uses wrong dump filename glob — silent data destruction on operator rollback | migration-toolkit | **High** | Draft | [`high_prio_gi_draft_runbook_rollback_dump_glob_bug.md`](issues/high_prio_gi_draft_runbook_rollback_dump_glob_bug.md) | Discovered during Tajik runbook walkthrough 2026-06-08; data-loss class. Reviewer round-1 APPROVE WITH REVISIONS applied. |
| **MIG-003** | Wrapper SQL uses lowercase horizon_type literals against UPPERCASE PG enum — deployment-blocking | migration-toolkit | **High** | Draft | [`high_prio_gi_draft_migration_horizon_type_case_coercion.md`](issues/high_prio_gi_draft_migration_horizon_type_case_coercion.md) | Discovered during P3 verification 2026-06-10; same class as Finding 11. 4 SQL fix sites in ML export + long-term initialize wrappers. |
| **MIG-004** | Full-history ML hindcast wrapper missing — toolkit P4b ships export/import but no generator | machine_learning + migration-toolkit | **High** | Draft | [`high_prio_gi_draft_migration_ml_hindcast_wrapper.md`](issues/high_prio_gi_draft_migration_ml_hindcast_wrapper.md) | Surfaced during dev_local_backfill.sh v3.x review (3 reviewer rounds 2026-06-10). Sibling-in-spirit to bin/initialize_site_backfill.sh (LR) and apps/long_term_forecasting/calibrate_and_hindcast.py (LT). Critical gap: existing daily_ml_maintenance.sh is gap-fill only, not initial backfill. |
| **MIG-005** | LT config raw .split(',') doesn't strip whitespace — operator env-var edits with spaces cause membership check failures | long_term_forecasting | Low | Draft | [`low_prio_gi_draft_lt_config_strip_on_split.md`](issues/low_prio_gi_draft_lt_config_strip_on_split.md) | Surfaced 2026-06-10 during dev_local_backfill.sh v3.x reviewer round-5. Companion to bin/ workaround. |
| **MIG-007** | Long-term from-file importer needs quarter/season support and code-scoped gap-fill | migration-toolkit | **High** | Review | [`high_prio_gi_draft_migration_long_forecast_importer_multihorizon_gapfill.md`](issues/high_prio_gi_draft_migration_long_forecast_importer_multihorizon_gapfill.md) | Implemented on `fix_migration_long_forecast_multihorizon` (`71c1141`, `767138d`); under owner review. MIG-006 remains reserved for possible scoped ML reaggregation follow-up. |
| **MIG-008** | Quarter/season `horizon_value` convention (RESOLVED 2026-06-22) — `hv = operational_month_lead_time`; config audit + DB reconciliation | long_term_configs + long_term_forecasting | Mid | Draft (planning) | [`mid_prio_gi_draft_migration_long_forecast_quarter_season_horizon_value.md`](issues/mid_prio_gi_draft_migration_long_forecast_quarter_season_horizon_value.md) | Resolved: hv = config lead (no date-derivation, no calendar-quarter mapping). Quarter is a single product: kyg lead 1, taj lead 0. Season per issue month: kyg Jan/Feb/Mar/Apr = 3/2/1/0, taj Apr = 0. Importer + service need no hv change. Remaining work = per-deployment config audit (esp. kyg seasonal Jan/Feb/Mar configs+CSVs), reconcile existing DB hv1-4 rows, reconsider held taj quarter (hv0 is correct). Planner pass next. |

---

## Tier 2: API Completion & Data Pipeline

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **API-001** | Add bulk-read endpoints to preprocessing/postprocessing services | infra | Low | Draft | [`low_prio_gi_draft_api_bulk_read_endpoints.md`](issues/low_prio_gi_draft_api_bulk_read_endpoints.md) | Colleague (sapphire/services/) |
| ~~**API-002**~~ | ~~Add missing params to sapphire-api-client (model, target, dates)~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_api_client_missing_params.md`](issues/archive/review_gi_draft_api_client_missing_params.md) | — |
| **API-003** | Define CSV removal acceptance criteria per module | infra | **Medium** | Open | — (needs D-002 decision) | D-002 |
| **API-004** | Migrate forecast_dashboard to use sapphire-api-client | fd | **Medium** | Open | — | — |
| **API-005** | Migrate long_term_forecasting from direct SQL to API client | infra | **Medium** | Open | — | API-001 |
| **API-006** | Support flag field on long-term forecasts (operational vs hindcast) | infra | **Medium** | Draft | [`mid_prio_gi_draft_infra_long_forecast_flag_support.md`](issues/mid_prio_gi_draft_infra_long_forecast_flag_support.md) | Colleague (sapphire/services/) |
| **ML-008** | Replace hindcast subprocess+CSV IPC with direct function call | ml | **Low** | Draft | [`low_prio_gi_draft_ml_hindcast_api_io.md`](issues/low_prio_gi_draft_ml_hindcast_api_io.md) | ML-009, hindcast API consistency (complete, archived) |

---

## Tier 3: Developer Infrastructure

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **INFRA-001** | Create Makefile and local dev infrastructure | infra | **High** | Draft | [`high_prio_gi_draft_infra_makefile_local_dev.md`](issues/high_prio_gi_draft_infra_makefile_local_dev.md) | — |
| **INFRA-002** | Update uv.lock files for all py312 modules (security) | infra | **Medium** | Open | See `security_updates.md` + `docker_health_score_improvement.md` | — |
| **INFRA-003** | Add pytest-cov with threshold enforcement to CI | infra | **Medium** | Open | — (from `architecture_review_claude.md` gap #10) | — |
| **INFRA-004** | Enforce Forecast Date Rule — eliminate scattered `date.today()` calls | infra | **High** | In Progress | [`high_prio_gi_draft_infra_forecast_date_rule.md`](issues/high_prio_gi_draft_infra_forecast_date_rule.md) | — |
| **INFRA-005** | Remove `model_long` from app pipeline (incremental) | infra | **Medium** | In Progress | [`mid_prio_gi_draft_infra_model_registry.md`](issues/mid_prio_gi_draft_infra_model_registry.md) | — |
| ~~**INFRA-007**~~ | ~~Fix ML forecast API reader & align write/read architecture~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_fix_ml_forecast_api_reader.md`](issues/archive/review_gi_draft_fix_ml_forecast_api_reader.md) | Phase 3 (DB cleanup + fallback removal) is ops task |
| **INFRA-008** | CPU-only PyTorch + Dockerize long_term_forecasting | infra | **High** | Review | [`dockerization_ltf_and_optimization_dockerization.md`](dockerization_ltf_and_optimization_dockerization.md) | — |
| ~~**INFRA-009**~~ | ~~Organization-based station filtering (app-side)~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_infra_org_station_filtering.md`](issues/archive/review_gi_draft_infra_org_station_filtering.md) | — |
| ~~**INFRA-010**~~ | ~~Bootstrap config_all_stations_library.json from HF SDK~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_infra_config_all_stations_bootstrap.md`](issues/archive/review_gi_draft_infra_config_all_stations_bootstrap.md) | — |
| **INFRA-011** | Upstream module org-scoped API reads (audit + deferred fixes) | infra | **Low** | In Progress | [`low_prio_gi_draft_infra_upstream_org_scoped_reads.md`](issues/low_prio_gi_draft_infra_upstream_org_scoped_reads.md) | PP-025, INFRA-009 |
| ~~**INFRA-012**~~ | ~~Multi-org safety guards (write guard, collision check, isolation test)~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_infra_multi_org_safety_guards.md`](issues/archive/review_gi_draft_infra_multi_org_safety_guards.md) | — |
| ~~**INFRA-013**~~ | ~~Postprocessing API container crashes on bulk forecast writes (118 restarts)~~ | ~~infra~~ | | Complete | [`archive/high_prio_gi_draft_infra_postprocessing_api_bulk_write_crash.md`](issues/archive/high_prio_gi_draft_infra_postprocessing_api_bulk_write_crash.md) | — |
| **INFRA-014** | Extend validate_pipeline.py: JSON output, baseline/delta, new checks | infra | **Medium** | Review | [`mid_prio_gi_draft_infra_validate_pipeline_extensions.md`](issues/mid_prio_gi_draft_infra_validate_pipeline_extensions.md) | — |
| ~~**INFRA-015**~~ | ~~Audit pentad/decade boundary date convention across modules~~ | ~~infra~~ | | Complete | [`archive/review_gi_draft_infra_pentad_decade_boundary_audit.md`](issues/archive/review_gi_draft_infra_pentad_decade_boundary_audit.md) | LR-008 (only finding) |
| ~~**INFRA-016**~~ | ~~Switch default branch from `main` to `maxat_sapphire_2` (v2 cut)~~ | ~~infra~~ | | Complete | [`archive/high_prio_gi_draft_infra_default_branch_switch.md`](issues/archive/high_prio_gi_draft_infra_default_branch_switch.md) | — |
| **FD-002b** | Synthetic integration tests with fake data | fd | **Medium** | Open | — (plan file never created) | — |
| **INFRA-017** | Document DB initialization for fresh deployments (`SAPPHIRE_SYNC_MODE=initial`) | infra | **Medium** | Draft | [`mid_prio_gi_draft_infra_initial_sync_docs.md`](issues/mid_prio_gi_draft_infra_initial_sync_docs.md) | — |
| **INFRA-018** | Playwright integration tests fail at browser launch under macOS sandbox (verification noise + potential CI coverage gap) | infra | **Low** | Draft | [`low_prio_gi_draft_infra_playwright_sandbox_browser_launch.md`](issues/low_prio_gi_draft_infra_playwright_sandbox_browser_launch.md) | — |
| ~~**INFRA-019**~~ | ~~sapphire-api-client `horizon_type` Literals diverge — `quarter` missing on most paths (hygiene, deferred from PREPQ-008 D3)~~ | infra | | Complete | [`mid_prio_gi_draft_infra_api_client_quarter_literal_consistency.md`](issues/mid_prio_gi_draft_infra_api_client_quarter_literal_consistency.md) | Fixed in sapphire-api-client v0.5.0 (`4fd543e`); pin bumped via PR #373 (2026-06-12). |
| ~~**P-006**~~ | ~~Move long-term schedule query into Luigi pipeline~~ | ~~pipeline~~ | | Complete | [`review_gi_draft_pipeline_lt_schedule_into_luigi.md`](issues/review_gi_draft_pipeline_lt_schedule_into_luigi.md) | — |

---

## Tier 4: Module Work

### Preprocessing Gateway (`prepg`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PREPG-001** | Yearly snow norm recalculation | **Medium** | In Progress | [`mid_prio_gi_draft_prepg_yearly_norm_recalculation.md`](issues/mid_prio_gi_draft_prepg_yearly_norm_recalculation.md) | Historical snow CSVs must exist |
| **PREPG-002** | Add coverage endpoints to preprocessing service | **Low** | Draft | [`low_prio_gi_draft_preprocessing_coverage_endpoints.md`](issues/low_prio_gi_draft_preprocessing_coverage_endpoints.md) | — |
| ~~**PREPG-003**~~ | ~~Snow operational API write discards all data — wall-clock-anchored window vs DG lag~~ | | Closed (Not a Bug) | [`archive/high_prio_gi_draft_prepg_snow_api_operational_window.md`](issues/archive/high_prio_gi_draft_prepg_snow_api_operational_window.md) | — |
| ~~**PREPG-005**~~ | ~~Meteo API write discards all forecast rows — `date <= today` upper bound~~ | **High** | Complete | [`archive/high_prio_gi_draft_prepg_meteo_forecast_not_in_api.md`](issues/archive/high_prio_gi_draft_prepg_meteo_forecast_not_in_api.md) | — |
| **PREPG-006** | Migrate snow norm computation from CSV to API | **Medium** | Draft | [`mid_prio_gi_draft_prepg_snow_norms_csv_to_api.md`](issues/mid_prio_gi_draft_prepg_snow_norms_csv_to_api.md) | PREPG-001, API must have historical snow data |
| **PREPG-007** | Snow visualization population gaps: self-healing curve and Jan-1 snow norms | **High** | Review | [`review_gi_draft_prepg_snow_population_self_heal.md`](issues/review_gi_draft_prepg_snow_population_self_heal.md) | — |
| **PREPG-008** | Snow climatology leap-year day-of-year alignment bug | **Medium** | Draft | [`mid_prio_gi_draft_prepg_snow_climatology_leapyear_dayofyear.md`](issues/mid_prio_gi_draft_prepg_snow_climatology_leapyear_dayofyear.md) | — |

### Preprocessing Runoff (`prepq`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PREPQ-004** | Swiss data source integration & module refactoring | **Low** | Blocked | [`low_prio_gi_PR-003_swiss_data_source_refactor.md`](issues/low_prio_gi_PR-003_swiss_data_source_refactor.md) | Swiss API docs unavailable |
| **PREPQ-007** | External site data ingestion (manual sites via Google Sheets) | **High** | Code complete; deployment wiring + hardening landed; pending operator rollout | [`external_site_data_ingestion_plan.md`](external_site_data_ingestion_plan.md) | — |
| **PREPQ-008** | Long-horizon quarterly hydrograph write fails — deployed API rejects `horizon_type="quarter"` (schema/deployment drift) | **High** | Draft | [`high_prio_gi_draft_runoff_quarter_horizon_type_rejected.md`](issues/high_prio_gi_draft_runoff_quarter_horizon_type_rejected.md) | Discovered 2026-06-12 in maintenance run; 422 abort. Source already has QUARTER (commit 2be58f7 + migration d4e5f6a7b8c9); deployed image/DB behind. Related: MIG-003, FD-015. |

### Linear Regression (`lr`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **LR-003** | Clean up dead `last_successful_run` code and state | **Low** | Draft | [`low_prio_gi_draft_LR_cleanup_dead_run_date_code.md`](issues/low_prio_gi_draft_LR_cleanup_dead_run_date_code.md) | — |
| **LR-004** | Remove iEH HF SDK dependency (use config-file path) | **High** | Done | [`external_site_data_ingestion_plan.md`](external_site_data_ingestion_plan.md) Phase 2 | — |
| **LR-005** | LR hindcast NaN API write skip has misleading log message | **Low** | Archived | [`archive/low_prio_gi_draft_lr_hindcast_station_restriction.md`](issues/archive/low_prio_gi_draft_lr_hindcast_station_restriction.md) | — |
| ~~**LR-006**~~ | ~~Fix maintenance script sync mode and hindcast auto-detect filenames~~ | | Complete | [`archive/review_gi_draft_lr_maintenance_and_autodetect_fixes.md`](issues/archive/review_gi_draft_lr_maintenance_and_autodetect_fixes.md) | — |
| ~~**LR-007**~~ | ~~API write failures are silent when API is enabled~~ | ~~ml~~ | | Complete | [`archive/review_gi_draft_lr_api_write_loud_failure.md`](issues/archive/review_gi_draft_lr_api_write_loud_failure.md) | — |
| ~~**LR-008**~~ | ~~Align LR `horizon_in_year` metadata with target-period convention~~ | | Complete | [`archive/review_gi_draft_lr_pentad_horizon_offset.md`](issues/archive/review_gi_draft_lr_pentad_horizon_offset.md) | — |
| ~~**LR-009**~~ | ~~Dec 31 cross-year boundary: wrong year context in `perform_linear_regression`~~ | ~~**High**~~ | Closed (Invalid) | [`archive/high_prio_gi_draft_lr_dec31_cross_year_boundary.md`](issues/archive/high_prio_gi_draft_lr_dec31_cross_year_boundary.md) | LR-008 |

### Long-Term Forecasting (`ltf`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| ~~**LTF-001**~~ | ~~`--today` flag in `run_forecast.py` runs zero models~~ | | Complete | [`archive/review_gi_draft_lt_today_flag_runs_no_models.md`](issues/archive/review_gi_draft_lt_today_flag_runs_no_models.md) | — |
| ~~**LTF-002**~~ | ~~SQL org-scoping for long-term forecasting queries~~ | | Complete | [`archive/review_gi_draft_ltf_sql_org_scoping.md`](issues/archive/review_gi_draft_ltf_sql_org_scoping.md) | — |
| ~~**LTF-003**~~ | ~~run_forecast.py sets flag=0 on null forecasts — marks failures as valid~~ (Fixed by @sandrohuni: NaN-aware flag=2 + dependency propagation) | | Complete | [`archive/high_prio_gi_draft_ltf_flag_zero_on_null.md`](issues/archive/high_prio_gi_draft_ltf_flag_zero_on_null.md) | — |
| ~~**LTF-004**~~ | ~~Seasonal/quarterly hindcasts have `q=None` for LR models — blocks skill computation~~ | | Complete | [`archive/high_prio_gi_draft_ltf_seasonal_quarterly_q_null.md`](issues/archive/high_prio_gi_draft_ltf_seasonal_quarterly_q_null.md) | Resolved 2026-05-29: `q` is now populated; `q50` null is harmless via `q`-first fallback at `skill_metrics.py:1090` |
| **LTF-005** | Add climatological quantile bounds (Q25/Q75) for GBT forecasts | **Medium** | Review | [`review_gi_draft_lt_gbt_quantile_bounds.md`](issues/review_gi_draft_lt_gbt_quantile_bounds.md) | — |
| **LTF-006** | `GBT_Base` has no `ModelType` enum value → long-term forecast writes 422 (operational + from-file backfill) | **High** | Draft — **next-week high-prio** | [`high_prio_gi_draft_ltf_gbt_base_modeltype_gap.md`](issues/high_prio_gi_draft_ltf_gbt_base_modeltype_gap.md) | Coordinate with LT/model owner: map→GBT, add enum value (service change), or remove from config. Found 2026-06-13 during ML from-file backfill WS-B planning. |

### Postprocessing Forecasts (`pp`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| ~~**PP-003**~~ | ~~Implement batch upsert in postprocessing CRUD~~ | | Complete | See `postprocessing_unified_plan.md` Phase 3 | — |
| ~~**PP-004**~~ | ~~Replace iterrows() with vectorized operations~~ | | Complete | See `postprocessing_unified_plan.md` Phase 3 | — |
| ~~**PP-005**~~ | ~~Create operational/maintenance entry point split~~ | | Complete | See `postprocessing_unified_plan.md` Phase 2 | — |
| ~~**PP-006**~~ | ~~Remove deprecated `GAPFILL_WINDOW_DAYS` references from shell scripts~~ | | Complete | Archived: [`gi_draft_pp_config_yaml_DONE_2026-03-05.md`](../archive/gi_draft_pp_config_yaml_DONE_2026-03-05.md) | — |
| **PP-007** | Maintenance should read from API, not CSV | **High** | Review | See `postprocessing_forecasts/README.md` PP-007 | — |
| **PP-008** | No audit trail for gap-filled rows | **Low** | Open | See `postprocessing_forecasts/README.md` PP-008 | API schema (colleague) |
| **PP-009** | Stop calculating EM skill metrics on the fly in operational mode | **Medium** | Review | See `postprocessing_forecasts/README.md` PP-009 | — |
| **PP-010** | Pentad/decad reads should use API (operational + recalculation) | | Review | Migrated to `data_reader.read_observed_and_modelled_data()` | — |
| **PP-011** | Skill metrics API unique key should include date | **Medium** | Review | Unique constraint now includes `date` in API schema | — |
| **PP-012** | Daily ensemble creation | **Medium** | Open | See `postprocessing_forecasts/README.md` PP-012 | — |
| **PP-013** | Monthly maintenance uses CSV-first gap detection | **High** | Review | See `postprocessing_forecasts/README.md` PP-013 | — |
| ~~**PP-014**~~ | ~~Skill metrics read priority inverted (CSV-first, should be API-first)~~ | | Complete | Moved to Completed Issues |
| **PP-015** | Move NE creation from setup_library to postprocessing | **Low** | Open | See `postprocessing_forecasts/README.md` PP-015 | — |
| **PP-016** | Recalculation bootstrap for new sites (auto-detect) | **Low** | Open | See `postprocessing_forecasts/README.md` PP-016 | — |
| ~~**PP-017**~~ | ~~Quarterly forecast postprocessing (aggregation + ensembles + skill metrics)~~ | | Complete | See `postprocessing_unified_plan.md` Phase 4b | — |
| ~~**PP-018**~~ | ~~Seasonal forecast postprocessing (aggregation + ensembles + skill metrics)~~ | | Complete | See `postprocessing_unified_plan.md` Phase 4b | — |
| ~~**PP-019**~~ | ~~Propagate quantiles through short-term ensemble creation~~ | | Complete | [`archive/review_gi_draft_pp_short_term_ensemble_quantiles.md`](issues/archive/review_gi_draft_pp_short_term_ensemble_quantiles.md) | — |
| **PP-020** | Probabilistic forecast quality metrics & documentation | **Medium** | Draft | [`review_gi_draft_pp_probabilistic_forecast_quality.md`](issues/review_gi_draft_pp_probabilistic_forecast_quality.md) | PP-019 (complete) |
| ~~**PP-021**~~ | ~~Improve short-term maintenance pipeline efficiency and stale quantile detection~~ | | Complete | [`archive/review_gi_draft_pp_maintenance_pipeline_efficiency.md`](issues/archive/review_gi_draft_pp_maintenance_pipeline_efficiency.md) | — |
| ~~**PP-022**~~ | ~~Fix stale-record refresh and minor inconsistencies in maintenance pipeline~~ | | Complete | [`archive/review_gi_draft_pp_maintenance_stale_refresh_fix.md`](issues/archive/review_gi_draft_pp_maintenance_stale_refresh_fix.md) | PP-021 (complete) |
| ~~**PP-023**~~ | ~~Period-aware aggregation of ML daily targets (fix contamination from adjacent periods)~~ | | Complete | [`archive/review_gi_draft_pp_period_aware_aggregation.md`](issues/archive/review_gi_draft_pp_period_aware_aggregation.md) | — |
| ~~**PP-024**~~ | ~~Write maintenance gap-fill records directly to API (DB retains gaps after maintenance)~~ | | Closed | [`archive/review_gi_draft_pp_maintenance_api_write.md`](issues/archive/review_gi_draft_pp_maintenance_api_write.md) | Absorbed into PP-022 |
| ~~**PP-025**~~ | ~~Org-scoped data readers (add codes filtering to all read functions)~~ | | Complete | [`archive/review_gi_draft_pp_org_scoped_data_readers.md`](issues/archive/review_gi_draft_pp_org_scoped_data_readers.md) | — |
| ~~**PP-026**~~ | ~~Make consumers flag-aware for null-discharge ML forecasts; clean stale flag=1/2 records~~ | | Complete | [`archive/high_prio_gi_draft_pp_clean_null_forecasts.md`](issues/archive/high_prio_gi_draft_pp_clean_null_forecasts.md) | — |
| ~~**PP-027**~~ | ~~Add per-station observability when EM ensemble is skipped~~ | | Complete | [`archive/review_gi_draft_pp_em_silent_skip_observability.md`](issues/archive/review_gi_draft_pp_em_silent_skip_observability.md) | Phase 1 (logging) done; Phase 2 (write_diagnostics) optional |
| ~~**PP-028**~~ | ~~Skill metrics writer: model=None, missing RMSE, empty decad/monthly metrics~~ (Bug 3 monthly reopened+fixed 2026-03-26: q50 fallback to q) | | Complete | [`archive/mid_prio_gi_draft_pp_skill_metrics_broken.md`](issues/archive/mid_prio_gi_draft_pp_skill_metrics_broken.md) | — |
| ~~**PP-029**~~ | ~~NaN guard in seasonal/quarterly API write (`_write_aggregated_forecasts_to_api`)~~ | | Complete | [`archive/review_gi_draft_pp_seasonal_nan_guard.md`](issues/archive/review_gi_draft_pp_seasonal_nan_guard.md) | — |
| ~~**PP-030**~~ | ~~Fix EM skill metric degradation in recalculate_skill_metrics.py (boundary-pentad n_pairs=1-2)~~ | | Complete | [`archive/review_gi_draft_pp_em_recalc_boundary_fix.md`](issues/archive/review_gi_draft_pp_em_recalc_boundary_fix.md) | — |
| ~~**PP-031**~~ | ~~Pentad/decad aggregation does not select boundary issue days (shared code path in `_normalize_ml_forecasts`)~~ | | Complete | [`archive/review_gi_draft_pp_pentad_date_misalignment.md`](issues/archive/review_gi_draft_pp_pentad_date_misalignment.md) | — |
| ~~**PP-032**~~ | ~~Monthly ensemble forecasts not written to API (4 bugs: early-return, ensemble groupby missing horizon_value, horizon_value mismatch, date mismatch)~~ | | Complete | [`archive/mid_prio_gi_draft_pp_monthly_ensemble_api_write.md`](issues/archive/mid_prio_gi_draft_pp_monthly_ensemble_api_write.md) | — |
| **PP-033** | Gap detector should only flag boundary dates as missing ensembles | **Low** | Won't Fix | [`archive/low_prio_gi_draft_pp_gap_detector_boundary_filter.md`](issues/archive/low_prio_gi_draft_pp_gap_detector_boundary_filter.md) | PP-031 |
| ~~**PP-034**~~ | ~~`read_daily_forecasts()` passes wrong keyword `horizon_type` to API client — daily skill metrics never computed~~ | | Complete | [`archive/high_prio_gi_draft_pp_daily_forecast_read_horizon_type.md`](issues/archive/high_prio_gi_draft_pp_daily_forecast_read_horizon_type.md) | — |
| **PP-035** | Deduplicate skill metrics before API write to fix monthly/quarterly/seasonal bulk upsert | **Medium** | Review | [`review_gi_draft_pp_skill_metric_dedup.md`](issues/review_gi_draft_pp_skill_metric_dedup.md) | — |
| **PP-028b** | Skill metrics crash/silent failure — missing `q50` column across all horizons | **High** | Review | [`review_gi_draft_pp_monthly_skill_q50_regression.md`](issues/review_gi_draft_pp_monthly_skill_q50_regression.md) | — |
| ~~**PP-036**~~ | ~~ML pentad/decadal skill metrics starved by `horizon='day'` short-circuit in API reader~~ | | Complete | [`archive/high_prio_gi_draft_pp_ml_skill_horizon_archive_split.md`](issues/archive/high_prio_gi_draft_pp_ml_skill_horizon_archive_split.md) | — |
| **PP-037** | Maintenance `model_short` KeyError on empty DECAD individual-model read (neural ensemble called before empty guard) | Crash fix **High** / P2–P3 **Low** | Phase 1 Complete; P2/P3 deferred | [`review_gi_draft_pp_maintenance_model_short_keyerror.md`](issues/review_gi_draft_pp_maintenance_model_short_keyerror.md) | Phase 1 (ensemble + maintenance + operational empty guards) shipped — crash resolved. Phase 2 (reader contract) and Phase 3 (stale-EM lookback scoping) deferred **low-prio**: defensive hardening / efficiency + an operational coverage decision, not crash fixes. |

### Forecast Dashboard (`fd`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **FD-012** | Dashboard crashes when current year data missing | **Low** | Draft | [`low_prio_gi_draft_dashboard_missing_current_year_data.md`](issues/low_prio_gi_draft_dashboard_missing_current_year_data.md) | — |
| **FD-002** | Add new skill metrics visualization with plain-language interpretation | **Medium** | Draft | [`mid_prio_gi_draft_dashboard_skill_metrics_visualization.md`](issues/mid_prio_gi_draft_dashboard_skill_metrics_visualization.md) | Phase 4c/4d |
| **FD-003** | Duplicate forecast rows caused by skill metric merge fan-out | **High** | Draft | [`high_prio_gi_draft_dashboard_duplicate_forecasts_skill_merge.md`](issues/high_prio_gi_draft_dashboard_duplicate_forecasts_skill_merge.md) | — |
| **FD-004** | LR forecast bounds and stats missing due to dropped delta column | **High** | Draft | [`high_prio_gi_draft_dashboard_lr_forecast_bounds_missing.md`](issues/high_prio_gi_draft_dashboard_lr_forecast_bounds_missing.md) | — |
| **FD-005** | Daily ML forecasts not displayed due to API limit truncation | **High** | Draft | [`high_prio_gi_draft_dashboard_daily_ml_forecast_limit_truncation.md`](issues/high_prio_gi_draft_dashboard_daily_ml_forecast_limit_truncation.md) | — |
| **FD-006** | Skill metrics missing for LR and NE due to model_long merge key mismatch | **High** | Draft | [`high_prio_gi_draft_dashboard_skill_metrics_model_long_mismatch.md`](issues/high_prio_gi_draft_dashboard_skill_metrics_model_long_mismatch.md) | — |
| ~~**FD-007**~~ | ~~Update dashboard Docker dataflows to operational mode with logging~~ | ~~**Medium**~~ | Complete | [`archive/mid_prio_gi_draft_fd_docker_dataflows_update.md`](issues/archive/mid_prio_gi_draft_fd_docker_dataflows_update.md) | — |
| **FD-008** | Fix error handling in inner `run_docker_container` (Save Changes) | **Low** | Draft | [`low_prio_gi_draft_fd_inner_run_docker_error_handling.md`](issues/low_prio_gi_draft_fd_inner_run_docker_error_handling.md) | — |
| ~~**FD-009**~~ | ~~Pass explicit forecast date to containers in dashboard retrigger flows~~ | ~~**Medium**~~ | Complete | [`archive/mid_prio_gi_draft_fd_retrigger_forecast_date.md`](issues/archive/mid_prio_gi_draft_fd_retrigger_forecast_date.md) | ~~FD-007~~ |
| **FD-010** | Fix lr-visibility parameter mismatch — linreg queries target-pentad, dashboard saves issue-pentad | **Medium** | Review | [`mid_prio_gi_draft_iEHF_lr_visibility_param_mismatch.md`](issues/mid_prio_gi_draft_iEHF_lr_visibility_param_mismatch.md) | FD-009 |
| **FD-011** | Horizon selector widget passes translated strings as API enum values | **High** | Draft | [`high_prio_gi_draft_fd_horizon_selector_i18n.md`](issues/high_prio_gi_draft_fd_horizon_selector_i18n.md) | — |
| ~~**FD-013**~~ | ~~Monthly skill metrics not loaded from API — all columns show "-"~~ | ~~**Mid**~~ | Complete | [`archive/mid_prio_gi_draft_fd_monthly_skill_metrics.md`](issues/archive/mid_prio_gi_draft_fd_monthly_skill_metrics.md) | — |
| **FD-014** | Snow visualization — configurable year start, units & labels | **Medium** | In Progress | [`mid_prio_gi_draft_dashboard_snow_visualization_enhancements.md`](issues/mid_prio_gi_draft_dashboard_snow_visualization_enhancements.md) | Phase 2 blocked on colleague (services) |
| ~~**FD-015**~~ | ~~Quarter/season skill metrics not rendered in summary table (data layer returns empty `forecast_stats`); reservoir quarterly card on month tab also affected~~ | | Complete | [`archive/high_prio_gi_draft_dashboard_long_horizon_skill_summary.md`](issues/archive/high_prio_gi_draft_dashboard_long_horizon_skill_summary.md) | Red-phase verified 2026-05-31 across P1-P4; tajik-deployment visible-impact target |

### iEasyHydro HF Migration

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **iEHF-001** | Audit iEH vs iEH HF code paths across all modules | **Medium** | Open | See `ieasyhydro_hf_migration_plan.md` Phase 1 | — |

---

## Tier 5: Configuration & Architecture

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **ARCH-001** | Implement ConfigManager with Pydantic schema | infra | **Medium** | Open | See `configuration_update_plan.md` Phase 1 | — |
| **ARCH-002** | Separate demo_data from operational config | infra | **Medium** | Open | See `configuration_update_plan.md` Phase 0 | — |
| **ARCH-003** | Design PostgreSQL + TimescaleDB schema for v2 | infra | **Medium** | Open | See `architecture_review_claude.md` gap #1 | D-001 |

---

## Tier 6: Documentation

| ID | Title | Module | Priority | Status | File |
|----|-------|--------|----------|--------|------|
| **DOC-001** | Add Quick Start section to README | infra | **Medium** | Open | See `documentation_improvement_plan.md` Priority 1 |
| **DOC-002** | Fix incomplete TODOs in doc/configuration.md | infra | **Low** | Open | See `documentation_improvement_plan.md` Priority 2 |
| **DOC-003** | Create doc/modules/ documentation structure | infra | **Low** | Open | See `documentation_improvement_plan.md` Priority 3 |
| **DOC-004** | Update `data_flow_long_term.md` — seasonal pipeline, model output specs, config conventions (Assigned: @sandrohurni, @mabesa) | doc, LTF, PP | **Medium** | Draft | [`mid_prio_gi_draft_doc_data_flow_long_term_gaps.md`](issues/mid_prio_gi_draft_doc_data_flow_long_term_gaps.md) |

---

## Completed Issues

### Pipeline Module (`p`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| P-001 | Marker files owned by root not cleaned up | 2026-02-03 | — |

### Preprocessing Runoff (`prepq`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| PREPQ-001 | Runoff data not updated in Docker container | 2025-01-09 | [`archive/gi_draft_preprunoff_operational_modes.md`](issues/archive/gi_draft_preprunoff_operational_modes.md) |
| PREPQ-002 | Slow data retrieval from iEasyHydro HF | Superseded by PREPQ-003 | — |
| PREPQ-003 | iEasyHydro HF Data Retrieval Validation (9 phases) | 2026-01-29 | [`archive/gi_PR-002_data_retrieval_validation.md`](issues/archive/gi_PR-002_data_retrieval_validation.md) |
| PREPQ-005 | Maintenance Mode Data Gaps (2 bugs, 164 tests) | 2026-01-29 | [`archive/gi_PREPQ-005_maintenance_mode_data_gaps.md`](issues/archive/gi_PREPQ-005_maintenance_mode_data_gaps.md) |
| PREPQ-006 | Pagination bug — station_type across pages | 2026-01-29 | [`archive/gi_draft_PR-004_pagination_station_type_bug.md`](issues/archive/gi_draft_PR-004_pagination_station_type_bug.md) |

### Conceptual Model (`cm`)

| ID | Title | Resolved | Notes |
|----|-------|----------|-------|
| CM-001 | CI/CD builds disabled — R dependencies broken | 2026-02-03 | Module being phased out |

### Linear Regression (`lr`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| LR-001 | Leap year date handling and hindcast mode | 2026-02-03 | [`archive/gi_LR-001_linreg_bugfix_hindcast_COMPLETED_2026-02-03.md`](issues/archive/gi_LR-001_linreg_bugfix_hindcast_COMPLETED_2026-02-03.md) |
| LR-002 | Replace `last_successful_run` file with "just run for today" | 2026-03-03 | [`archive/gi_LR-002_replace_last_run_file_COMPLETED_2026-03-03.md`](issues/archive/gi_LR-002_replace_last_run_file_COMPLETED_2026-03-03.md) |
| LR-005 | LR hindcast NaN API write skip — Issues A+C resolved; Issue B (log message) archived as low-prio | 2026-03-24 | [`archive/low_prio_gi_draft_lr_hindcast_station_restriction.md`](issues/archive/low_prio_gi_draft_lr_hindcast_station_restriction.md) |
| LR-009 | Dec 31 cross-year boundary: wrong year context in `perform_linear_regression` — Closed (Invalid) — predicated on wrong LR-008 diagnosis | 2026-03-26 | [`archive/high_prio_gi_draft_lr_dec31_cross_year_boundary.md`](issues/archive/high_prio_gi_draft_lr_dec31_cross_year_boundary.md) |

### Postprocessing (`pp`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| PP-001 | Duplicate Skill Metrics for Ensemble Mean | 2026-01-24 | [`archive/gi_duplicate_skill_metrics_RESOLVED_2026-01-24.md`](issues/archive/gi_duplicate_skill_metrics_RESOLVED_2026-01-24.md) |
| PP-002 | Add missing `ieasyforecast_decadal_skill_metrics_file` to .env | 2026-02-12 | See `postprocessing_unified_plan.md` Phase 1 |
| PP-003 | Implement batch upsert in postprocessing CRUD | 2026-02-15 | See `postprocessing_unified_plan.md` Phase 3 |
| PP-004 | Replace iterrows() with vectorized operations | 2026-02-15 | See `postprocessing_unified_plan.md` Phase 3 |
| PP-005 | Create operational/maintenance entry point split | 2026-02-15 | See `postprocessing_unified_plan.md` Phase 2 |
| PP-014 | Skill metrics read priority inverted (CSV-first, should be API-first) | 2026-02-27 | [`archive/gi_draft_pp_skill_metrics_read_priority.md`](issues/archive/gi_draft_pp_skill_metrics_read_priority.md) |
| PP-017 | Quarterly forecast postprocessing (aggregation + ensembles + skill metrics) | 2026-03-05 | See `postprocessing_unified_plan.md` Phase 4b |
| PP-018 | Seasonal forecast postprocessing (aggregation + ensembles + skill metrics) | 2026-03-05 | See `postprocessing_unified_plan.md` Phase 4b |
| PP-022 | Fix stale-record refresh and minor inconsistencies in maintenance pipeline | 2026-03-24 | [`archive/review_gi_draft_pp_maintenance_stale_refresh_fix.md`](issues/archive/review_gi_draft_pp_maintenance_stale_refresh_fix.md) |
| PP-023 | Period-aware aggregation of ML daily targets (fix contamination from adjacent periods) | 2026-03-24 | [`archive/review_gi_draft_pp_period_aware_aggregation.md`](issues/archive/review_gi_draft_pp_period_aware_aggregation.md) |
| PP-026 | Make consumers flag-aware for null-discharge ML forecasts — Phase 1 code fixes done; Phase 2 DB cleanup skipped (harmless, tombstones needed) | 2026-03-24 | [`archive/high_prio_gi_draft_pp_clean_null_forecasts.md`](issues/archive/high_prio_gi_draft_pp_clean_null_forecasts.md) |
| PP-028 | Skill metrics writer: model=None, missing RMSE, empty decad/monthly metrics — Bug 3 monthly reopened+fixed 2026-03-26 (q50 fallback to q in skill_metrics.py:1091) | 2026-03-26 | [`archive/mid_prio_gi_draft_pp_skill_metrics_broken.md`](issues/archive/mid_prio_gi_draft_pp_skill_metrics_broken.md) |

### Machine Learning (`ml`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| ML-BF3 | ML datetime format crash blocks API write (BF-3) | 2026-03-14 | See `review_checklist_local_2026-03-13.md` BF-3 |

### Long-Term Forecasting (`ltf`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| LTF-003 | run_forecast.py sets flag=0 on null forecasts — Fixed by @sandrohuni: NaN-aware flag=2 + dependency propagation; Fix 2 (skeleton record guard) not implemented but non-critical | 2026-03-27 | [`archive/high_prio_gi_draft_ltf_flag_zero_on_null.md`](issues/archive/high_prio_gi_draft_ltf_flag_zero_on_null.md) |

### API (`api`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| API-002 | Add missing params to sapphire-api-client (model, target, dates) | 2026-03-05 | [`archive/review_gi_draft_api_client_missing_params.md`](issues/archive/review_gi_draft_api_client_missing_params.md) |

### Pipeline (`p`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| P-002 | Gateway double-run | 2026-02-03 | [`archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md`](../archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md) |
| P-003 | Consolidate maintenance scripts into Luigi pipeline | 2026-02-27 | See plan: `ethereal-dazzling-zebra.md` |
| P-005 | Remove RunAllMLMaintenance WrapperTask to fix unfulfilled dependencies | 2026-04-15 | [`archive/high_prio_gi_draft_pipeline_remove_wrapper_task.md`](issues/archive/high_prio_gi_draft_pipeline_remove_wrapper_task.md) |
| P-006 | Move long-term schedule query into Luigi pipeline | 2026-04-15 | [`review_gi_draft_pipeline_lt_schedule_into_luigi.md`](issues/review_gi_draft_pipeline_lt_schedule_into_luigi.md) |

---

## Active Planning Documents

These documents contain context and specifications referenced by issues above.

| Document | Purpose | Status |
|----------|---------|--------|
| `sapphire_api_integration_plan.md` | API integration roadmap (Phases 1-6) | Active — Phase 6 pending |
| `postprocessing_unified_plan.md` | Postprocessing refactoring (Phases 1-5) | Active — Phases 1-3 done, Phase 4 planned |
| `configuration_update_plan.md` | Config refactor to Pydantic + feature flags | Planning — not started |
| `deployment_improvement_planning.md` | Makefile + local dev workflow | Planning — design complete |
| `documentation_improvement_plan.md` | Doc restructuring + MkDocs | Active — Phase 1.1 done |
| `docker_health_score_improvement.md` | Docker image security | Planning — Phase 1 identified |
| `monitoring_improvement_plan.md` | Alert strategy improvement | Planning — needs D-004 decision |
| `security_updates.md` | Vulnerability tracking | Active — SEC-005 pending |
| `ieasyhydro_hf_migration_plan.md` | Legacy iEH deprecation | Planning — Phase 1 audit needed |
| `external_site_data_ingestion_plan.md` | Manual sites + Google Sheets ingestion (4 phases) | Phases 1-3 done |
| `sapphire_v2_planning.md` | v2 architecture + demo milestones | Active |
| `sub_daily_forecasting_architecture.md` | Sub-daily forecasting design | Blocked — API spec undefined |
| `bulk_read_endpoints_instructions.md` | Bulk endpoint specification | Spec complete — implementation not started |
| `forecast_dashboard_api_client_comparison.md` | Client gap analysis | Analysis complete — fixes not started |
| `architecture_review_claude.md` | 10-dimension architecture assessment | Reference |
| `architecture_review_copilot.md` | 10-step modernization roadmap | Reference |
| `observations.md` | Running log of production issues | Active — triage weekly |
| ~~`Makefile.planned`~~ | ~~Makefile template~~ | Deleted — superseded by rewritten INFRA-001 plan |

---

## Module Abbreviations

| Module | Abbreviation |
|--------|--------------|
| conceptual_model | `cm` |
| preprocessing_runoff | `prepq` |
| preprocessing_gateway | `prepg` |
| preprocessing_station_forcing | `prepf` |
| linear_regression | `lr` |
| machine_learning | `ml` |
| long_term_forecasting | `ltf` |
| postprocessing_forecasts | `pp` |
| forecast_dashboard | `fd` |
| configuration_dashboard | `cd` |
| pipeline | `p` |
| iEasyHydroForecast | `iEHF` |
| reset_forecast_run_date | `r` |
| cross-module/infrastructure | `infra` |

---

*Last updated: 2026-06-19 — Added ML-016 (standalone run_locally.sh machine_learning crashes on empty SAPPHIRE_PREDICTION_MODE) + ML-017 (single missing ERA5 day → NaN cascade across all short-term ML; prepg interior-gap + ML covariate guard; found in local review tjhm+kghm). Earlier: 2026-06-16 — Added PP-037 (maintenance `model_short` KeyError on empty DECAD read, Ready — plan through two review cycles; Phase 1 is the minimum-safe production fix). 2026-04-16 — SEC-006 Draft→Review (PR #330); P-005/P-006 Complete; LTF-005 Draft→Review; added P-004, FD-014, PP-028b.*
