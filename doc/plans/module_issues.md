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
| **INFRA-006** | Fix postprocessing boundary day guard, LR sentinel, and validation queries | infra | **High** | Review | [`review_gi_draft_infra_date_mismatch_and_sentinel.md`](issues/review_gi_draft_infra_date_mismatch_and_sentinel.md) |
| **ML-001** | Maintenance mode hindcast failure not handled, causes FileNotFoundError | ml | **High** | Draft | [`high_prio_gi_draft_ml_maintenance_hindcast_file_not_found.md`](issues/high_prio_gi_draft_ml_maintenance_hindcast_file_not_found.md) |
| **ML-002** | Investigate hindcast subprocess root cause (why hindcast_ML_models.py fails) | ml | **High** | Open | — (requires investigation with Sandro; likely CSV→API migration gap) |
| **ML-003** | Migrate maintenance scripts to API-primary reads (`fill_ml_gaps`, `recalculate_nan_forecasts`, `add_new_station`) | ml | **High** | Review | [`high_prio_gi_draft_ml_api_primary_reads.md`](issues/high_prio_gi_draft_ml_api_primary_reads.md) |
| **ML-009** | Fix ML forecast CSV schema corruption (API column leak + corruption loop) | ml | **Critical** | Review | [`review_gi_draft_ml_csv_schema_corruption_fix.md`](issues/review_gi_draft_ml_csv_schema_corruption_fix.md) |
| **ML-010** | Read old_forecast from API with CSV fallback in make_forecast.py | ml | **High** | Review | [`review_gi_draft_ml_api_read_old_forecasts.md`](issues/review_gi_draft_ml_api_read_old_forecasts.md) |
| **ML-004** | Hindcast gap-fill never persists to API — silent write failure (3 bugs) | ml | **Critical** | Review | [`review_gi_draft_ml_hindcast_api_write_broken.md`](issues/review_gi_draft_ml_hindcast_api_write_broken.md) |
| **ML-005** | ML consistency check reads forecasts without station code filter | ml | **Medium** | Complete | [`review_gi_draft_pp_org_scoped_data_readers.md`](issues/review_gi_draft_pp_org_scoped_data_readers.md) (Phase 4) |
| **ML-006** | NumPy shape mismatch in recalculate_nan_forecasts `.loc` assignment | ml | **High** | Review | [`review_gi_draft_ml_nan_recalc_shape_mismatch.md`](issues/review_gi_draft_ml_nan_recalc_shape_mismatch.md) |
| **ML-007** | Non-deterministic API pagination causes inconsistent gap detection | ml | **Medium** | Review | [`mid_prio_gi_draft_ml_api_pagination_nondeterministic.md`](issues/mid_prio_gi_draft_ml_api_pagination_nondeterministic.md) | Option 1 (ORDER BY): colleague; Option 2 (per-code reads): done |
| **ML-008b** | fill_ml_gaps infinite hindcast loop on null-discharge (flag=3) rows | ml | **High** | Review | [`review_gi_draft_ml_fill_gaps_null_loop.md`](issues/review_gi_draft_ml_fill_gaps_null_loop.md) | — |
| ~~**ML-011**~~ | ~~flag=2 semantic collision in make_forecast.py~~ | ~~ml~~ | | Deleted | — | Deleted 2026-03-20: no behavioral impact |
| **ML-012** | recalculate_nan_forecasts crashes on NaN flag values (astype crash) | ml | **High** | Review | [`high_prio_gi_draft_ml_recalc_flag_astype_crash.md`](issues/high_prio_gi_draft_ml_recalc_flag_astype_crash.md) | — |
| **ML-013** | recalculate_nan_forecasts API write overwrites valid operational rows | ml | **High** | Review | [`review_gi_draft_ml_recalc_api_overwrite.md`](issues/review_gi_draft_ml_recalc_api_overwrite.md) | ML-012 |
| **SEC-005** | Verify bokeh>=3.8.2 compatibility post-merge | fd | **High** | Open | See `sapphire_v2_planning.md` post-merge checklist |
| ~~**PP-002**~~ | ~~Add missing `ieasyforecast_decadal_skill_metrics_file` to .env~~ | ~~pp~~ | | Complete | Moved to Completed Issues |

---

## Tier 2: API Completion & Data Pipeline

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **API-001** | Add bulk-read endpoints to preprocessing/postprocessing services | infra | Low | Draft | [`low_prio_gi_draft_api_bulk_read_endpoints.md`](issues/low_prio_gi_draft_api_bulk_read_endpoints.md) | Colleague (sapphire/services/) |
| ~~**API-002**~~ | ~~Add missing params to sapphire-api-client (model, target, dates)~~ | ~~infra~~ | | Complete | [`review_gi_draft_api_client_missing_params.md`](issues/review_gi_draft_api_client_missing_params.md) | — |
| **API-003** | Define CSV removal acceptance criteria per module | infra | **Medium** | Open | — (needs D-002 decision) | D-002 |
| **API-004** | Migrate forecast_dashboard to use sapphire-api-client | fd | **Medium** | Open | — | — |
| **API-005** | Migrate long_term_forecasting from direct SQL to API client | infra | **Medium** | Open | — | API-001 |
| **API-006** | Support flag field on long-term forecasts (operational vs hindcast) | infra | **Medium** | Draft | [`mid_prio_gi_draft_infra_long_forecast_flag_support.md`](issues/mid_prio_gi_draft_infra_long_forecast_flag_support.md) | Colleague (sapphire/services/) |
| **ML-008** | Replace hindcast subprocess+CSV IPC with direct function call | ml | **Low** | Draft | [`low_prio_gi_draft_ml_hindcast_api_io.md`](issues/low_prio_gi_draft_ml_hindcast_api_io.md) | ML-009, `mid_prio_gi_draft_ml_hindcast_api_consistency.md` |

---

## Tier 3: Developer Infrastructure

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **INFRA-001** | Create Makefile and local dev infrastructure | infra | **High** | Draft | [`high_prio_gi_draft_infra_makefile_local_dev.md`](issues/high_prio_gi_draft_infra_makefile_local_dev.md) | — |
| **INFRA-002** | Update uv.lock files for all py312 modules (security) | infra | **Medium** | Open | See `security_updates.md` + `docker_health_score_improvement.md` | — |
| **INFRA-003** | Add pytest-cov with threshold enforcement to CI | infra | **Medium** | Open | — (from `architecture_review_claude.md` gap #10) | — |
| **INFRA-004** | Enforce Forecast Date Rule — eliminate scattered `date.today()` calls | infra | **High** | In Progress | [`high_prio_gi_draft_infra_forecast_date_rule.md`](issues/high_prio_gi_draft_infra_forecast_date_rule.md) | — |
| **INFRA-005** | Remove `model_long` from app pipeline (incremental) | infra | **Medium** | Draft | [`mid_prio_gi_draft_infra_model_registry.md`](issues/mid_prio_gi_draft_infra_model_registry.md) | — |
| **INFRA-007** | Fix ML forecast API reader & align write/read architecture | infra | **High** | Review | [`review_gi_draft_fix_ml_forecast_api_reader.md`](issues/review_gi_draft_fix_ml_forecast_api_reader.md) | Phase 3 cleanup pending production deployment |
| **INFRA-008** | CPU-only PyTorch + Dockerize long_term_forecasting | infra | **High** | Review | [`dockerization_ltf_and_optimization_dockerization.md`](dockerization_ltf_and_optimization_dockerization.md) | — |
| **INFRA-009** | Organization-based station filtering (app-side) | infra | **High** | Review | [`review_gi_draft_infra_org_station_filtering.md`](issues/review_gi_draft_infra_org_station_filtering.md) | — |
| **INFRA-010** | Bootstrap config_all_stations_library.json from HF SDK | infra | **High** | Review | [`review_gi_draft_infra_config_all_stations_bootstrap.md`](issues/review_gi_draft_infra_config_all_stations_bootstrap.md) | — |
| **INFRA-011** | Upstream module org-scoped API reads (audit + deferred fixes) | infra | **Low** | In Progress | [`low_prio_gi_draft_infra_upstream_org_scoped_reads.md`](issues/low_prio_gi_draft_infra_upstream_org_scoped_reads.md) | PP-025, INFRA-009 |
| **INFRA-012** | Multi-org safety guards (write guard, collision check, isolation test) | infra | **High** | Review | [`review_gi_draft_infra_multi_org_safety_guards.md`](issues/review_gi_draft_infra_multi_org_safety_guards.md) | — |
| ~~**INFRA-013**~~ | ~~Postprocessing API container crashes on bulk forecast writes (118 restarts)~~ | ~~infra~~ | | Complete | [`archive/high_prio_gi_draft_infra_postprocessing_api_bulk_write_crash.md`](issues/archive/high_prio_gi_draft_infra_postprocessing_api_bulk_write_crash.md) | — |
| **INFRA-014** | Extend validate_pipeline.py: JSON output, baseline/delta, new checks | infra | **Medium** | Draft | [`mid_prio_gi_draft_infra_validate_pipeline_extensions.md`](issues/mid_prio_gi_draft_infra_validate_pipeline_extensions.md) | — |
| **FD-001** | Synthetic integration tests with fake data | fd | **Medium** | Draft | [`gi_draft_fd_synthetic_integration_tests.md`](issues/gi_draft_fd_synthetic_integration_tests.md) | — |

---

## Tier 4: Module Work

### Preprocessing Gateway (`prepg`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PREPG-001** | Yearly snow norm recalculation | **Medium** | Review | [`mid_prio_gi_draft_prepg_yearly_norm_recalculation.md`](issues/mid_prio_gi_draft_prepg_yearly_norm_recalculation.md) | Historical snow CSVs must exist |
| **PREPG-002** | Add coverage endpoints to preprocessing service | **Low** | Draft | [`low_prio_gi_draft_preprocessing_coverage_endpoints.md`](issues/low_prio_gi_draft_preprocessing_coverage_endpoints.md) | — |
| ~~**PREPG-003**~~ | ~~Snow operational API write discards all data — wall-clock-anchored window vs DG lag~~ | | Closed (Not a Bug) | [`archive/high_prio_gi_draft_prepg_snow_api_operational_window.md`](issues/archive/high_prio_gi_draft_prepg_snow_api_operational_window.md) | — |
| ~~**PREPG-005**~~ | ~~Meteo API write discards all forecast rows — `date <= today` upper bound~~ | **High** | Complete | [`archive/high_prio_gi_draft_prepg_meteo_forecast_not_in_api.md`](issues/archive/high_prio_gi_draft_prepg_meteo_forecast_not_in_api.md) | — |

### Preprocessing Runoff (`prepq`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PREPQ-004** | Swiss data source integration & module refactoring | **Low** | Blocked | [`low_prio_gi_PR-003_swiss_data_source_refactor.md`](issues/low_prio_gi_PR-003_swiss_data_source_refactor.md) | Swiss API docs unavailable |
| **PREPQ-007** | External site data ingestion (manual sites via Google Sheets) | **High** | Done | [`external_site_data_ingestion_plan.md`](external_site_data_ingestion_plan.md) | — |

### Linear Regression (`lr`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **LR-003** | Clean up dead `last_successful_run` code and state | **Low** | Draft | [`low_prio_gi_draft_LR_cleanup_dead_run_date_code.md`](issues/low_prio_gi_draft_LR_cleanup_dead_run_date_code.md) | — |
| **LR-004** | Remove iEH HF SDK dependency (use config-file path) | **High** | Done | [`external_site_data_ingestion_plan.md`](external_site_data_ingestion_plan.md) Phase 2 | — |
| **LR-005** | LR hindcast NaN API write skip has misleading log message | **Low** | Archived | [`archive/low_prio_gi_draft_lr_hindcast_station_restriction.md`](issues/archive/low_prio_gi_draft_lr_hindcast_station_restriction.md) | — |

### Long-Term Forecasting (`ltf`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **LTF-001** | `--today` flag in `run_forecast.py` runs zero models | **Medium** | Review | [`review_gi_draft_lt_today_flag_runs_no_models.md`](issues/review_gi_draft_lt_today_flag_runs_no_models.md) | — |
| **LTF-002** | SQL org-scoping for long-term forecasting queries | **High** | Review | [`review_gi_draft_ltf_sql_org_scoping.md`](issues/review_gi_draft_ltf_sql_org_scoping.md) | INFRA-009 (complete), INFRA-012 (complete) |
| **LTF-003** | run_forecast.py sets flag=0 on null forecasts — marks failures as valid (Assigned: @sandrohuni) | **High** | Draft | [`high_prio_gi_draft_ltf_flag_zero_on_null.md`](issues/high_prio_gi_draft_ltf_flag_zero_on_null.md) | — |

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
| **PP-019** | Propagate quantiles through short-term ensemble creation | **High** | Review | [`review_gi_draft_pp_short_term_ensemble_quantiles.md`](issues/review_gi_draft_pp_short_term_ensemble_quantiles.md) | — |
| **PP-020** | Probabilistic forecast quality metrics & documentation | **Medium** | Draft | [`mid_prio_gi_draft_pp_probabilistic_forecast_quality.md`](issues/mid_prio_gi_draft_pp_probabilistic_forecast_quality.md) | PP-019 (partial) |
| **PP-021** | Improve short-term maintenance pipeline efficiency and stale quantile detection | **High** | Review | [`review_gi_draft_pp_maintenance_pipeline_efficiency.md`](issues/review_gi_draft_pp_maintenance_pipeline_efficiency.md) | PP-019 (complete) |
| ~~**PP-022**~~ | ~~Fix stale-record refresh and minor inconsistencies in maintenance pipeline~~ | | Complete | [`archive/review_gi_draft_pp_maintenance_stale_refresh_fix.md`](issues/archive/review_gi_draft_pp_maintenance_stale_refresh_fix.md) | PP-021 (complete) |
| ~~**PP-023**~~ | ~~Period-aware aggregation of ML daily targets (fix contamination from adjacent periods)~~ | | Complete | [`archive/review_gi_draft_pp_period_aware_aggregation.md`](issues/archive/review_gi_draft_pp_period_aware_aggregation.md) | — |
| **PP-024** | Write maintenance gap-fill records directly to API (DB retains gaps after maintenance) | **High** | Review | [`review_gi_draft_pp_maintenance_api_write.md`](issues/review_gi_draft_pp_maintenance_api_write.md) | Absorbed into PP-022 |
| **PP-025** | Org-scoped data readers (add codes filtering to all read functions) | **High** | Complete | [`review_gi_draft_pp_org_scoped_data_readers.md`](issues/review_gi_draft_pp_org_scoped_data_readers.md) | INFRA-009 (complete) |
| ~~**PP-026**~~ | ~~Make consumers flag-aware for null-discharge ML forecasts; clean stale flag=1/2 records~~ | | Complete | [`archive/high_prio_gi_draft_pp_clean_null_forecasts.md`](issues/archive/high_prio_gi_draft_pp_clean_null_forecasts.md) | — |
| **PP-027** | Add per-station observability when EM ensemble is skipped | **Medium** | Draft | [`mid_prio_gi_draft_pp_em_silent_skip_observability.md`](issues/mid_prio_gi_draft_pp_em_silent_skip_observability.md) | — |
| ~~**PP-028**~~ | ~~Skill metrics writer: model=None, missing RMSE, empty decad/monthly metrics~~ | | Complete | [`archive/mid_prio_gi_draft_pp_skill_metrics_broken.md`](issues/archive/mid_prio_gi_draft_pp_skill_metrics_broken.md) | — |

### Forecast Dashboard (`fd`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **FD-001** | Dashboard crashes when current year data missing | **Low** | Draft | [`gi_draft_dashboard_missing_current_year_data.md`](issues/gi_draft_dashboard_missing_current_year_data.md) | — |
| **FD-002** | Add new skill metrics visualization with plain-language interpretation | **Medium** | Draft | [`gi_draft_dashboard_skill_metrics_visualization.md`](issues/gi_draft_dashboard_skill_metrics_visualization.md) | Phase 4c/4d |

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
| PP-028 | Skill metrics writer: model=None, missing RMSE, empty decad/monthly metrics — all bugs closed (not-a-bug or upstream data gap) | 2026-03-24 | [`archive/mid_prio_gi_draft_pp_skill_metrics_broken.md`](issues/archive/mid_prio_gi_draft_pp_skill_metrics_broken.md) |

### Machine Learning (`ml`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| ML-BF3 | ML datetime format crash blocks API write (BF-3) | 2026-03-14 | See `review_checklist_local_2026-03-13.md` BF-3 |

### API (`api`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| API-002 | Add missing params to sapphire-api-client (model, target, dates) | 2026-03-05 | [`review_gi_draft_api_client_missing_params.md`](issues/review_gi_draft_api_client_missing_params.md) |

### Pipeline (`p`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| P-002 | Gateway double-run | 2026-02-03 | [`archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md`](../archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md) |
| P-003 | Consolidate maintenance scripts into Luigi pipeline | 2026-02-27 | See plan: `ethereal-dazzling-zebra.md` |

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

*Last updated: 2026-03-24 (PP-022, PP-023, PP-026 completed; LR-005 archived; PP-028 closed)*
