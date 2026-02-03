# SAPPHIRE Architecture Review for SAPPHIRE 2

**Date:** 2026-02-03
**Reviewed by:** Claude Opus 4.5 (10 parallel exploration agents)
**Purpose:** Comprehensive comparison of SAPPHIRE v1 architecture to state-of-the-art operational forecasting systems

---

## Executive Summary

SAPPHIRE is a **functional operational forecasting system** built pragmatically for hydromet services in Central Asia, Nepal, Caucasus, and Switzerland. However, compared to modern architectures, it has significant gaps in nearly every dimension. The planned SAPPHIRE 2 with database services and Airflow/Prefect addresses many core issues.

### Scorecard: Current State vs State-of-the-Art

| Dimension | Current | State-of-Art | Gap | Priority |
|-----------|---------|--------------|-----|----------|
| **Data Pipeline** | File-based CSV, Luigi | Event-driven, Parquet, Airflow | Large | High |
| **Orchestration** | Luigi + marker files | Airflow/Dagster/Prefect | Large | High |
| **Database/Storage** | CSV/JSON/pickle files | PostgreSQL + TimescaleDB + Redis | Large | **Critical** |
| **OOP/Architecture** | 6K-line procedural core | Clean Architecture, DI, SOLID | Large | High |
| **Model Management** | No training code, no registry | MLflow, Optuna, feature stores | Large | Medium |
| **Testing** | 2/9 modules tested in CI | Full coverage, property-based | Medium | Medium |
| **Configuration** | 1000+ os.getenv() scattered | Pydantic Settings, centralized | Large | High |
| **Monitoring** | File logs, email alerts | Prometheus, Grafana, Loki | Large | High |
| **API/Communication** | Shared filesystem | REST APIs, message queues | Large | High |
| **DevOps** | Docker Compose on VM | Kubernetes, GitOps, IaC | Medium | Medium |

---

## Top 10 Critical Gaps

### 1. No Database Layer (Critical)
- All data in CSV files with no indexing, transactions, or concurrency
- **Fix:** PostgreSQL + TimescaleDB for time-series

### 2. Procedural God Module (Critical)
- `forecast_library.py`: 6,230 lines, 55+ functions, 64 `os.getenv()` calls
- **Fix:** Extract domain model, repository pattern, dependency injection

### 3. No Experiment Tracking / Model Registry (Critical)
- Training code not in repo, models overwritten, no versioning
- **Fix:** MLflow for experiment tracking and model registry

### 4. Secrets Management (Security)
- Passwords in `.env` files on servers (not in git, but unencrypted on disk)
- **Fix:** Docker secrets, HashiCorp Vault, or AWS Secrets Manager

### 5. No Centralized Observability (High)
- Plain text logs, email-only alerts, no metrics
- **Fix:** Prometheus + Grafana + Loki stack

### 6. File-Based Orchestration (High)
- Luigi marker files cause permission issues (P-001), no event-driven patterns
- **Fix:** Airflow with XCom or Prefect 2.0

### 7. No Schema Validation (High)
- CSV files read with inferred types, runtime errors
- **Fix:** Pydantic models, Pandera DataFrames

### 8. Row-Oriented CSV Storage (High)
- 10-100x slower than Parquet for analytics workloads
- **Fix:** Apache Parquet or Delta Lake

### 9. No API Layer (High)
- Modules communicate via shared filesystem
- **Fix:** FastAPI services with proper contracts

### 10. Limited CI Testing (Medium)
- Only 2 of 9 modules run actual pytest in CI
- **Fix:** pytest-cov with thresholds, pytest-xdist

---

## What SAPPHIRE Does Well

1. **Atomic file operations** - temp file + rename pattern prevents corruption
2. **Probabilistic forecasting** - proper quantile regression with uncertainty
3. **ML module OOP** - good abstract base classes, template method pattern
4. **Quarterly security rebuilds** - automated base image updates
5. **SLSA provenance** - supply chain security attestations
6. **Modular Docker containers** - clear separation of concerns
7. **Dual operating modes** - operational (fast) vs maintenance (thorough)

---

## Detailed Analysis by Component

### 1. Data Pipeline Architecture

**Current State:**
- Luigi task-based DAG with marker files for orchestration
- CSV files for all intermediate data
- Atomic writes using temp file + rename pattern (good!)
- No schema enforcement - types inferred at read time

**State-of-the-Art:**
- Event-driven with Kafka/Pulsar message brokers
- Parquet/Delta Lake columnar storage (10-100x faster reads)
- Pydantic/Pandera schema validation at boundaries
- OpenLineage for data lineage tracking

**Recommendations:**
- Tier 1: Add Pydantic models for data contracts
- Tier 2: Implement Parquet storage for intermediate data
- Tier 3: Add OpenLineage instrumentation
- Tier 4: Evaluate event-driven architecture

### 2. Orchestration Architecture

**Current State:**
- Luigi with custom marker files, timeout/retry handling
- Email notifications for failures
- Cron-based scheduling (external)
- Known issues: P-001 root-owned marker files, P-002 double gateway runs (fixed)

**State-of-the-Art Comparison:**

| Feature | Luigi | Airflow | Prefect 2.0 | Dagster |
|---------|-------|---------|-------------|---------|
| Deployment | Low | High | Medium | Medium |
| Scheduling | External | Built-in | Built-in | Built-in |
| Retries | Custom | Native | Native | Native |
| State | Marker files | Database | Database | Database |
| Observability | Minimal | Rich UI | Rich UI | Best UI |
| Docker Support | Custom | Mature | Good | Good |

**Recommendation:** Migrate to **Prefect 2.0** (best balance of simplicity and power)
- Pythonic (low retraining for team)
- Lightweight infrastructure
- Solves current pain points (state, retries, observability)
- Self-hosted version is free (Apache 2.0 license)

### 3. Database/Storage Patterns

**Current State:**
- CSV files for time-series data (runoff, forecasts)
- JSON for configuration (station metadata)
- Pickle for ML models (security risk)
- External iEasyHydro SDK for read-only database access

**Proposed Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│                  Database Infrastructure                     │
│                                                              │
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │  PostgreSQL   │  │    Redis     │  │   MinIO/S3      │  │
│  │ + TimescaleDB │  │   (Cache)    │  │ (Object Store)  │  │
│  │               │  │              │  │                 │  │
│  │ - Time series │  │ - Sessions   │  │ - Models        │  │
│  │ - Forecasts   │  │ - SDK cache  │  │ - Backups       │  │
│  │ - Metadata    │  │ - UI state   │  │ - Large files   │  │
│  └───────────────┘  └──────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Key Patterns to Implement:**
- Repository pattern for data access abstraction
- Unit of Work for transactional consistency
- Connection pooling for performance
- Alembic for database migrations

### 4. OOP and Design Patterns

**Current State:**
- ML module has good OOP (abstract base classes, template method)
- `forecast_library.py` is 6,230 lines with 55+ procedural functions
- 64 occurrences of `os.getenv()` - hard-coded dependencies
- No dependency injection, no repository pattern
- Site class mixes data + behavior + factory methods

**SOLID Violations:**
- **SRP:** God modules (forecast_library.py)
- **OCP:** Modification needed to extend
- **DIP:** Concrete dependencies everywhere (os.getenv, file paths)

**Recommended Refactoring:**
1. Extract domain model from forecast_library.py
2. Implement Repository pattern for all data access
3. Add dependency injection (constructor injection)
4. Create service layer to orchestrate business logic
5. Strategy pattern for different forecast types (pentad, decad)
6. Replace os.getenv() with injected configuration objects

### 5. Forecast Model Architecture

**Current State:**
- Linear regression: trained fresh each time, no persistence
- ML models (TFT, TiDE, TSMixer): Darts framework, proper .pt serialization
- **NO TRAINING CODE IN REPOSITORY** - models trained externally
- No model versioning or registry
- No hyperparameter optimization
- Good probabilistic forecasting with quantiles

**State-of-the-Art:**
- scikit-learn Pipelines for preprocessing + model
- MLflow for experiment tracking and model registry
- Feature stores for consistent feature engineering
- Optuna for hyperparameter optimization
- Automated backtesting with walk-forward validation

**Recommendations:**
1. Add MLflow for experiment tracking and model registry
2. Create training pipeline in repository (`apps/model_training/`)
3. Integrate Optuna for hyperparameter search
4. Add lightweight feature store
5. Implement automated backtesting

### 6. Testing Infrastructure

**Current State:**
- Inconsistent test directory naming (tests/ vs test/)
- Only 2 of 9 modules run actual pytest in CI
- No coverage tracking or thresholds
- Good mocking patterns but no advanced patterns
- Strong integration test for maintenance mode gaps (1,800 lines)

**Missing:**
- Property-based testing (Hypothesis)
- Snapshot testing for bulletins
- Contract testing for SDK
- Test containers for database tests
- Mutation testing

**Recommendations:**
1. Add pytest-cov with minimum threshold (60%)
2. Enable pytest-xdist for parallel execution
3. Add test markers (@pytest.mark.integration, @pytest.mark.slow)
4. Expand CI to run actual pytest for all modules
5. Add Hypothesis for property-based testing (date calculations)

### 7. Configuration Management

**Current State:**
- 1000+ environment variable references across 53 files
- No validation - uses `os.getenv()` without type checking
- Three different config loading patterns across modules
- Inconsistent naming prefixes (ieasyforecast_*, ieasyhydroforecast_*, SAPPHIRE_*)

**Recommendations:**
1. Implement Pydantic Settings for centralized, validated config
2. Create shared config library used by all modules
3. Use Docker secrets for production credentials
4. Rename tracked .env to .env.example
5. Add JSON schema validation for station configs

### 8. Monitoring and Observability

**Current State:**
- Plain text logging (370+ statements in preprocessing_runoff)
- Email-only alerting with acknowledged notification fatigue
- No metrics collection (no Prometheus)
- No distributed tracing (no OpenTelemetry)
- No log aggregation (no Loki/ELK)
- Docker healthchecks exist but monitoring scripts not deployed everywhere

**Recommended Stack:**
```
┌─────────────────────────────────────────────────────────────┐
│                    Observability Stack                       │
│  Prometheus + Grafana + Loki + Alertmanager                  │
│  (Metrics, Dashboards, Logs, Alerts)                         │
└─────────────────────────────────────────────────────────────┘
```

**Implementation Phases:**
1. Add structlog for structured JSON logging
2. Implement daily email digest (reduce notification fatigue)
3. Deploy Prometheus + Grafana for metrics
4. Add Loki + Promtail for log aggregation
5. Consider OpenTelemetry for distributed tracing

### 9. API and Module Communication

**Current State:**
- File-based communication via shared filesystem (CSV/JSON)
- Marker files for workflow coordination
- iEasyHydroForecast shared library for common code
- Docker containers orchestrated by Luigi
- No REST APIs, message queues, or event-driven patterns

**Recommended Evolution:**
1. **Phase 1:** FastAPI gateway service (Q2 2026)
2. **Phase 2:** Event-driven with Kafka/RabbitMQ (Q3-Q4 2026)
3. **Phase 3:** TimescaleDB + GraphQL (2027)
4. **Phase 4:** Full service mesh (2027+)

### 10. DevOps and Deployment

**Current State:**
- Good CI/CD foundation (GitHub Actions, SLSA provenance, SBOM)
- Docker Compose on single VM (no Kubernetes)
- No Infrastructure as Code (Terraform, Ansible)
- Plain text secrets on servers
- Containers run as root (security concern)
- No staging environment

**Recommendations:**
1. Add Trivy security scanning to CI pipeline
2. Set up Prometheus + Grafana for monitoring
3. Migrate secrets to Docker secrets / Vault
4. Harden Docker images (non-root, distroless)
5. Create staging environment
6. Implement Infrastructure as Code with Terraform
7. Consider Kubernetes for production scaling

---

## Recommended SAPPHIRE 2 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          API Gateway (FastAPI)                       │
│    /api/v1/forecasts, /api/v1/stations, /api/v1/observations        │
└───────────────────────────────────────┬─────────────────────────────┘
                                        │
┌───────────────────────────────────────┼─────────────────────────────┐
│                    Application Services Layer                        │
│  ForecastService, PreprocessingService, DashboardService            │
│  (Dependency Injection, Repository Pattern)                          │
└───────────────────────────────────────┬─────────────────────────────┘
                                        │
┌───────────────────────────────────────┼─────────────────────────────┐
│                    Domain Model Layer                                │
│  Station, Discharge, Forecast, ForecastStrategy                      │
│  (Rich domain objects, SOLID principles)                             │
└───────────────────────────────────────┬─────────────────────────────┘
                                        │
┌───────────────────────────────────────┴─────────────────────────────┐
│                    Infrastructure Layer                              │
├─────────────────┬─────────────────┬─────────────────┬───────────────┤
│  PostgreSQL +   │    Redis        │    MinIO/S3     │   Prefect/    │
│  TimescaleDB    │   (Cache)       │   (Models)      │   Airflow     │
│  (Time-series)  │                 │                 │               │
└─────────────────┴─────────────────┴─────────────────┴───────────────┘
                                        │
┌───────────────────────────────────────┴─────────────────────────────┐
│                    Observability Stack                               │
│  Prometheus + Grafana + Loki + Alertmanager                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack Recommendations

| Layer | Current | SAPPHIRE 2 Recommendation |
|-------|---------|---------------------------|
| **Orchestration** | Luigi | **Prefect 2.0** (or Airflow if scaling significantly) |
| **Database** | CSV files | **PostgreSQL + TimescaleDB** |
| **Cache** | None | **Redis** |
| **Object Storage** | Pickle files | **MinIO/S3** |
| **Model Registry** | None | **MLflow** |
| **Configuration** | os.getenv() scattered | **Pydantic Settings** |
| **Monitoring** | File logs + email | **Prometheus + Grafana + Loki** |
| **Secrets** | Plain .env files | **Docker Secrets / Vault** |
| **API** | Shared filesystem | **FastAPI services** |
| **Data Format** | CSV | **Parquet** (intermediate), **PostgreSQL** (operational) |

---

## Migration Roadmap

### Phase 1: Foundation (Q2 2026)
- [ ] Pydantic Settings for configuration management
- [ ] PostgreSQL + TimescaleDB deployment
- [ ] Repository pattern for data access
- [ ] Structured logging (structlog)
- [ ] Move secrets to Docker secrets

### Phase 2: Core Services (Q3 2026)
- [ ] FastAPI gateway service
- [ ] MLflow for model registry
- [ ] Parquet storage for intermediate data
- [ ] Prometheus + Grafana monitoring
- [ ] Prefect/Airflow migration (replace Luigi)

### Phase 3: Advanced (Q4 2026)
- [ ] Event-driven architecture (Kafka/RabbitMQ)
- [ ] OpenTelemetry distributed tracing
- [ ] Feature store (lightweight)
- [ ] Automated backtesting pipeline

### Phase 4: Scale (2027)
- [ ] Kubernetes deployment
- [ ] GitOps with ArgoCD
- [ ] Multi-region/HA
- [ ] Full Clean Architecture refactor

---

## Quick Wins (Immediate Impact, Low Effort)

1. **Add pytest-cov** - Track test coverage, set thresholds
2. **Implement Pydantic Settings** - Centralized, validated config
3. **Daily email digest** - Reduce notification fatigue
4. **Health check endpoints** - `/health` for all services
5. **Parquet for large CSVs** - 10x storage and speed improvement
6. **Trivy in CI** - Automated vulnerability scanning

---

## Cost Considerations

### Prefect 2.0 (Recommended Orchestrator)
- **Self-hosted:** $0 (Apache 2.0 license)
- **Cloud Hobby tier:** $0 (1 user, 5 workflows)
- **Cloud Starter:** ~$100/month (small teams)

### Infrastructure Additions
| Component | RAM | Notes |
|-----------|-----|-------|
| Prefect Server | ~200MB | Self-hosted |
| PostgreSQL + TimescaleDB | ~500MB | Time-series optimized |
| Redis | ~100MB | Caching layer |
| Prometheus + Grafana + Loki | ~400MB | Observability |
| **Total additional** | **~1.2GB** | On existing VMs |

---

## References

This review was conducted using 10 parallel exploration agents analyzing:

1. Data Pipeline Architecture
2. Orchestration (Luigi vs alternatives)
3. Database/Storage Patterns
4. OOP and Design Patterns
5. Forecast Model Architecture
6. Testing Infrastructure
7. Configuration Management
8. Monitoring and Observability
9. API and Module Communication
10. DevOps and Deployment

Key files analyzed:
- `apps/iEasyHydroForecast/forecast_library.py` (6,230 lines)
- `apps/pipeline/pipeline_docker.py` (orchestration)
- `apps/machine_learning/` (ML models)
- `apps/preprocessing_runoff/` (data pipeline)
- `.github/workflows/` (CI/CD)
- `doc/plans/` (existing planning documents)

---

## Next Steps

1. Review this document with the team
2. Prioritize based on SAPPHIRE 2 timeline
3. Create detailed implementation plans for top priorities
4. Set up proof-of-concept for database layer
5. Evaluate Prefect vs Airflow with a small pilot workflow
