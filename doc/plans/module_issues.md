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
| Complete | Resolved and verified |
| Blocked | Cannot proceed (see notes) |

---

## Tier 1: Production Bugs & Orphaned Tasks

| ID | Title | Module | Priority | Status | File |
|----|-------|--------|----------|--------|------|
| **ML-001** | Maintenance mode hindcast failure not handled, causes FileNotFoundError | ml | **High** | Draft | [`gi_draft_ml_maintenance_hindcast_file_not_found.md`](issues/gi_draft_ml_maintenance_hindcast_file_not_found.md) |
| **ML-002** | Investigate hindcast subprocess root cause (why hindcast_ML_models.py fails) | ml | **High** | Open | — (requires investigation with Sandro; likely CSV→API migration gap) |
| **SEC-005** | Verify bokeh>=3.8.2 compatibility post-merge | fd | **High** | Open | See `sapphire_v2_planning.md` post-merge checklist |
| **PP-002** | Add missing `ieasyforecast_decadal_skill_metrics_file` to .env | pp | **High** | Open | — (discovered in `postprocessing_unified_plan.md` Section 1.5; quick fix) |

---

## Tier 2: API Completion & Data Pipeline

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **API-001** | Add bulk-read endpoints to preprocessing/postprocessing services | infra | **High** | Draft | [`gi_draft_api_bulk_read_endpoints.md`](issues/gi_draft_api_bulk_read_endpoints.md) | — |
| **API-002** | Add missing params to sapphire-api-client (model, target, dates) | infra | **High** | Draft | [`gi_draft_api_client_missing_params.md`](issues/gi_draft_api_client_missing_params.md) | — |
| **API-003** | Define CSV removal acceptance criteria per module | infra | **Medium** | Open | — (needs D-002 decision) | D-002 |
| **API-004** | Migrate forecast_dashboard to use sapphire-api-client | fd | **Medium** | Open | — | API-002 |
| **API-005** | Migrate long_term_forecasting from direct SQL to API client | infra | **Medium** | Open | — | API-001 |

---

## Tier 3: Developer Infrastructure

| ID | Title | Module | Priority | Status | File | Blocked By |
|----|-------|--------|----------|--------|------|------------|
| **INFRA-001** | Create Makefile and local dev infrastructure | infra | **High** | Draft | [`gi_draft_infra_makefile_local_dev.md`](issues/gi_draft_infra_makefile_local_dev.md) | — |
| **INFRA-002** | Update uv.lock files for all py312 modules (security) | infra | **Medium** | Open | See `security_updates.md` + `docker_health_score_improvement.md` | — |
| **INFRA-003** | Add pytest-cov with threshold enforcement to CI | infra | **Medium** | Open | — (from `architecture_review_claude.md` gap #10) | — |

---

## Tier 4: Module Work

### Preprocessing Gateway (`prepg`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PREPG-001** | Yearly norm recalculation for snow and meteo data | **Medium** | Draft | [`gi_draft_prepg_yearly_norm_recalculation.md`](issues/gi_draft_prepg_yearly_norm_recalculation.md) | Verify API snow endpoints exist |

### Preprocessing Runoff (`prepq`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PREPQ-004** | Swiss data source integration & module refactoring | **Low** | Blocked | [`gi_PR-003_swiss_data_source_refactor.md`](issues/gi_PR-003_swiss_data_source_refactor.md) | Swiss API docs unavailable |

### Postprocessing Forecasts (`pp`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **PP-003** | Implement batch upsert in postprocessing CRUD (Phase 2) | **Medium** | Open | See `postprocessing_unified_plan.md` Phase 2 | — |
| **PP-004** | Replace iterrows() with vectorized operations (Phase 2) | **Medium** | Open | See `postprocessing_unified_plan.md` Phase 2 | — |
| **PP-005** | Create operational/maintenance entry point split (Phase 3) | **Medium** | Open | See `postprocessing_unified_plan.md` Phase 3 | PP-003, PP-004 |

### Forecast Dashboard (`fd`)

| ID | Title | Priority | Status | File | Blocked By |
|----|-------|----------|--------|------|------------|
| **FD-001** | Dashboard crashes when current year data missing | **Low** | Draft | [`gi_draft_dashboard_missing_current_year_data.md`](issues/gi_draft_dashboard_missing_current_year_data.md) | — |

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

### Postprocessing (`pp`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| PP-001 | Duplicate Skill Metrics for Ensemble Mean | 2026-01-24 | [`archive/gi_duplicate_skill_metrics_RESOLVED_2026-01-24.md`](issues/archive/gi_duplicate_skill_metrics_RESOLVED_2026-01-24.md) |

### Pipeline (`p`)

| ID | Title | Resolved | File |
|----|-------|----------|------|
| P-002 | Gateway double-run | 2026-02-03 | [`archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md`](../archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md) |

---

## Active Planning Documents

These documents contain context and specifications referenced by issues above.

| Document | Purpose | Status |
|----------|---------|--------|
| `sapphire_api_integration_plan.md` | API integration roadmap (Phases 1-6) | Active — Phase 6 pending |
| `postprocessing_unified_plan.md` | Postprocessing refactoring (Phases 1-4) | Active — Phase 1 done, 2-4 pending |
| `configuration_update_plan.md` | Config refactor to Pydantic + feature flags | Planning — not started |
| `deployment_improvement_planning.md` | Makefile + local dev workflow | Planning — design complete |
| `documentation_improvement_plan.md` | Doc restructuring + MkDocs | Active — Phase 1.1 done |
| `docker_health_score_improvement.md` | Docker image security | Planning — Phase 1 identified |
| `monitoring_improvement_plan.md` | Alert strategy improvement | Planning — needs D-004 decision |
| `security_updates.md` | Vulnerability tracking | Active — SEC-005 pending |
| `ieasyhydro_hf_migration_plan.md` | Legacy iEH deprecation | Planning — Phase 1 audit needed |
| `sapphire_v2_planning.md` | v2 architecture + demo milestones | Active |
| `sub_daily_forecasting_architecture.md` | Sub-daily forecasting design | Blocked — API spec undefined |
| `bulk_read_endpoints_instructions.md` | Bulk endpoint specification | Spec complete — implementation not started |
| `forecast_dashboard_api_client_comparison.md` | Client gap analysis | Analysis complete — fixes not started |
| `architecture_review_claude.md` | 10-dimension architecture assessment | Reference |
| `architecture_review_copilot.md` | 10-step modernization roadmap | Reference |
| `observations.md` | Running log of production issues | Active — triage weekly |
| `Makefile.planned` | Makefile template | Template — ready for INFRA-001 |

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
| postprocessing_forecasts | `pp` |
| forecast_dashboard | `fd` |
| configuration_dashboard | `cd` |
| pipeline | `p` |
| iEasyHydroForecast | `iEHF` |
| reset_forecast_run_date | `r` |
| cross-module/infrastructure | `infra` |

---

*Last updated: 2026-02-11*
