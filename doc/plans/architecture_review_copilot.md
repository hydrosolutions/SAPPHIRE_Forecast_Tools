# SAPPHIRE 2 Architecture Modernization Plan

**Date:** 2026-02-03  
**Status:** Draft for Review  
**Context:** Copilot-assisted architecture comparison against state-of-the-art operational forecasting systems

---

## Executive Summary

This plan synthesizes findings from analyzing 10 architectural dimensions of SAPPHIRE Forecast Tools, comparing each to modern best practices. The goal is to inform SAPPHIRE 2 development with database services and Airflow orchestration.

---

## Current Strengths (Retain)

1. **Docker containerization** — Layered base images, uv-based reproducible builds
2. **Luigi orchestration** — `DockerTaskBase` pattern with timeout/retry/notifications
3. **ML predictor hierarchy** — `BasePredictor` → `BaseDartsDLPredictor` → model-specific classes
4. **Modular preprocessing** — Clear service boundaries per data source

---

## Critical Gaps (10 Areas)

| Area | Current State | State-of-the-Art Target |
|------|---------------|-------------------------|
| **1. Orchestration** | Luigi + cron + marker files | Airflow with DB-backed state, dynamic task mapping |
| **2. Data Layer** | 100% file-based (CSV/JSON) | TimescaleDB (time-series), PostgreSQL (metadata), Redis (cache/state) |
| **3. ML Architecture** | External training, no versioning | MLflow for tracking, registry, artifact versioning |
| **4. OOP Design** | Mixed; `setup_library.py` = 6,231 lines | Domain modules, Repository pattern |
| **5. Configuration** | Scattered .env, secrets in repo | Pydantic settings, external secrets manager |
| **6. Testing** | CI runs imports only; no ML tests | Full pytest, model regression tests, coverage |
| **7. Monitoring** | Email alerts, file logs | Prometheus metrics, structured logging, OpenTelemetry |
| **8. API Layer** | No internal APIs | FastAPI gateway with JWT auth |
| **9. Data Quality** | Test-only validation | Runtime Pydantic, Great Expectations-style checks |
| **10. Security** | CSV sessions, plaintext creds | JWT tokens, bcrypt, Vault integration |

---

## Implementation Steps

### Step 1: Migrate Luigi → Airflow
- Convert `DockerTaskBase` tasks to `DockerOperator`
- Replace marker files with Airflow metadata DB
- Use `expand()` for dynamic ML model execution
- **Reference:** `apps/pipeline/pipeline_docker.py`

### Step 2: Implement Data Access Layer (DAL)
- Create abstract `Repository` classes in `sapphire/services/`
- Add database backends:
  - TimescaleDB for forecasts/runoff time-series
  - PostgreSQL for stations/configurations
  - Redis for pipeline state (replace marker files)
- Parallel write to CSV + DB during migration

### Step 3: Add MLflow Integration
- Track model versions in `apps/machine_learning/`
- Add `model_metadata.json` with checksums
- Implement prediction logging for drift detection
- Consider bringing training pipeline into repository

### Step 4: Refactor Monolithic Libraries
- Break `apps/iEasyHydroForecast/setup_library.py` into:
  - `domain/site.py`, `domain/forecast.py`
  - `services/forecast_service.py`, `services/discharge_service.py`
  - `io/csv_reader.py`, `io/csv_writer.py`
- Consolidate duplicate `Site` classes
- Fix ARIMA to inherit from `BasePredictor`

### Step 5: Modernize Configuration
- Create Pydantic `SapphireSettings` schema with validation
- Move secrets to Docker secrets or HashiCorp Vault
- Replace organization-based branching with feature flags:
```
#Before: 
ieasyhydroforecast_organization=kghm
#After:
ieasyhydroforecast_enable_ml_models=true
ieasyhydroforecast_enable_cm_models=true
ieasyhydroforecast_data_source=gateway 
``` 

### Step 6: Expand Test Coverage
- Run actual pytest in CI (not just imports)
- Add ML model inference tests
- Implement pytest-cov with 60% threshold
- Add property-based tests with Hypothesis

### Step 7: Add Observability Stack
- Prometheus metrics endpoint (`/metrics`)
- Structured JSON logging with correlation IDs
- Grafana dashboards for:
- Pipeline execution times
- Forecast accuracy trends
- Error rates by component

### Step 8: Build FastAPI Gateway
- Endpoint: `/api/v1/forecasts/{station}`
- JWT authentication (replace CSV sessions)
- Health check: `/api/v1/health`
- OpenAPI documentation

### Step 9: Implement Runtime Data Validation
- Pydantic schema validation on preprocessing outputs
- Externalize outlier thresholds to YAML config
- Data lineage metadata files
- Great Expectations-style quality checks

### Step 10: Security Hardening
- Replace JSON credentials with bcrypt-hashed passwords
- JWT tokens with expiry for dashboard auth
- HTTPS enforcement
- Docker image scanning with Docker Scout

---

## Open Questions

### Q1: Airflow Executor Choice
- **Celery:** Multi-worker scaling, simpler setup
- **Kubernetes:** Cloud-native isolation, better resource control
- **Recommendation:** Start with Celery, migrate to K8s when needed

### Q2: Database Migration Strategy
- **Parallel write:** Write to CSV + DB for 3 months with comparison checks
- **Direct cutover:** Faster but riskier
- **Recommendation:** Parallel write with automated validation

### Q3: Training Pipeline Scope
- **External:** Keep current approach (pre-trained models copied in)
- **Internal:** Add `apps/machine_learning/training/` with MLflow
- **Recommendation:** Bring training in-repo, triggered on schedule or drift

---

## Priority Matrix

| Priority | Items | Effort | Impact |
|----------|-------|--------|--------|
| **P0** | Security (Step 10), Data Layer (Step 2) | High | Critical |
| **P1** | Airflow migration (Step 1), API gateway (Step 8) | High | High |
| **P2** | MLflow (Step 3), Testing (Step 6), Observability (Step 7) | Medium | High |
| **P3** | OOP refactor (Step 4), Config (Step 5), Data validation (Step 9) | Medium | Medium |

---

## References

- Current pipeline: `apps/pipeline/pipeline_docker.py`
- ML predictors: `apps/machine_learning/scr/BasePredictor.py`
- Core library: `apps/iEasyHydroForecast/setup_library.py`
- Preprocessing: `apps/preprocessing_runoff/`, `apps/preprocessing_gateway/`
- Configuration: `apps/config/.env`
- Existing services stub: `sapphire/services/`

---

*Generated by GitHub Copilot architecture review, 2026-02-03*
