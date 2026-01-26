# Testing Workflow for SAPPHIRE Forecast Tools

This document describes the 4-stage testing workflow for validating changes before they reach production.

## Prerequisites

### Required Tools

- **uv** package manager: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Python 3.12**: `uv python install 3.12`
- **Docker** with Docker Compose v2
- **Git** for version control

### Initial Setup (One-Time)

1. Clone the repository
2. Set up virtual environments for modules you'll be testing:
   ```bash
   cd apps/<module_name>
   uv sync --all-extras
   ```

> **Note**: Each module has its own `.venv/` - they are NOT shared.

### Key Environment Variables

| Variable | Purpose | Required For |
|----------|---------|--------------|
| `SAPPHIRE_TEST_ENV` | Isolates tests from production paths | All tests |
| `ieasyhydroforecast_env_file_path` | Path to organization config | Docker testing |
| `SAPPHIRE_PREDICTION_MODE` | Forecast mode (`PENTAD` or `DECAD`) | Docker testing |

---

## Testing Workflow Overview

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  Stage 1: Local     │────>│  Stage 2: Docker    │────>│  Stage 3: CI/CD     │────>│  Stage 4: Server    │
│  Unit Tests         │     │  Module Testing     │     │  Automated Tests    │     │  Validation         │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

All stages must pass before changes are considered production-ready.

---

## Stage 1: Local Unit/Integration Tests

**Purpose**: Catch logic errors and regressions before building Docker images.

### Running Tests

All tests are run from the `apps/` directory:

```bash
cd apps

# Run ALL module tests
SAPPHIRE_TEST_ENV=True bash run_tests.sh

# Run single module tests
SAPPHIRE_TEST_ENV=True bash run_tests.sh <module_name>

# Example: test preprocessing_runoff only
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff
```

### Modules with Tests

| Module | Test Framework | Test Directory | Notes |
|--------|---------------|----------------|-------|
| iEasyHydroForecast | unittest | `tests/` | Core library |
| preprocessing_runoff | pytest | `test/` | Data processing |
| postprocessing_forecasts | pytest | `tests/` | Output formatting |
| pipeline | pytest | `tests/` | Container orchestration |
| forecast_dashboard | pytest + Playwright | `tests/` | Integration tests disabled by default |
| linear_regression | pytest | `test/` | Forecasting models |

> **Note**: Test directories are named inconsistently (`test/` vs `tests/`). The `run_tests.sh` script handles both.

### Dashboard Integration Tests (Optional)

Dashboard tests require additional setup:

```bash
# Install Playwright browser (one-time)
cd apps/forecast_dashboard
uv sync --all-extras
playwright install chromium

# Run with specific flags
TEST_LOCAL=true bash run_tests.sh forecast_dashboard   # Local server
TEST_PENTAD=true bash run_tests.sh forecast_dashboard  # Pentad production
TEST_DECAD=true bash run_tests.sh forecast_dashboard   # Decad production
```

### Success Criteria Checklist

- [ ] All tests pass (exit code 0)
- [ ] No unexpected skipped tests
- [ ] Summary shows "All tests completed successfully!"

### Common Issues

| Symptom | Cause | Resolution |
|---------|-------|------------|
| "No .venv found" | Missing virtual environment | Run `uv sync --all-extras` in module directory |
| Tests connect to production | Missing env var | Ensure `SAPPHIRE_TEST_ENV=True` is set |
| Import errors | Wrong directory | Run from `apps/` directory |

---

## Stage 2: Local Docker Module Testing

**Purpose**: Verify Docker containerization works correctly before CI/CD.

### Prerequisites

1. Docker running locally
2. Base image built:
   ```bash
   docker build -f apps/docker_base_image/Dockerfile.py312 \
     -t mabesa/sapphire-pythonbaseimage:py312 .
   ```
3. Valid `.env` file for your organization

### Building Module Images

```bash
# From repository root
docker build -f apps/<module>/Dockerfile.py312 \
  -t mabesa/sapphire-<module>:local .
```

### Testing Each Module

Test each module **separately** in **both modes** before integration:

#### Operational Mode (Daily Forecast Runs)

```bash
# Preprocessing
bash bin/run_preprocessing_gateway.sh <env_file_path>
bash bin/run_preprocessing_runoff.sh <env_file_path>

# Forecasting
bash bin/run_pentadal_forecasts.sh <env_file_path>
bash bin/run_decadal_forecasts.sh <env_file_path>
```

#### Maintenance Mode (Hindcast/Gap-Filling)

```bash
bash bin/daily_preprunoff_maintenance.sh <env_file_path>
bash bin/daily_ml_maintenance.sh <env_file_path>
bash bin/daily_linreg_maintenance.sh <env_file_path>
```

### What These Scripts Do

1. Start Luigi daemon if not running (port 8082)
2. Read configuration from `.env` file
3. Set up SSH tunnel if required
4. Run Docker container with correct volume mounts
5. Clean up containers on exit

### Verification Commands

```bash
# Check container exit code
docker inspect <container_name> --format='{{.State.ExitCode}}'

# Check logs for errors
docker logs <container_name> 2>&1 | grep -iE "error|exception|traceback"

# Verify output files exist
ls -la <output_directory>/

# Check Luigi web UI
open http://localhost:8082
```

### Success Criteria Checklist - Operational Mode

- [ ] Container starts without errors
- [ ] No Python import errors in logs
- [ ] Output files created in expected locations
- [ ] Exit code 0

### Success Criteria Checklist - Maintenance Mode

- [ ] Container completes with exit code 0
- [ ] Log file created at expected location
- [ ] No ERROR or CRITICAL messages in logs
- [ ] Gap-filling operations reported in logs

### Common Failure Patterns

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| Container fails immediately | Missing env vars | Check `.env` file completeness |
| Import errors | Missing dependency | Rebuild Docker image |
| Connection refused | SSH tunnel not established | Check tunnel configuration |
| Permission denied | Root ownership issue | Check volume mount permissions |
| Data not updating | Timestamp calculation bug | Use maintenance mode for backfill |

---

## Stage 3: CI/CD Automated Testing

**Purpose**: Automated gate ensuring tests pass on multiple Python versions before Docker images are built.

### Workflow Files

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `build_test.yml` | Push to non-main branches, PRs to main | Build-only (no push to DockerHub) |
| `deploy_main.yml` | Push to main | Build + push to DockerHub |

### CI Pipeline Stages

```
Tests (py311 + py312)
        │
        ▼
Build Base Images
        │
        ▼
Build Module Images (parallel)
        │
        ▼
Summarize Builds
```

### What CI Tests

- Python 3.11 tests (pip-based)
- Python 3.12 tests (uv-based)
- Docker image builds for all modules
- Import verification for modules without tests

### Success Criteria Checklist

- [ ] All jobs show green checkmark in GitHub Actions
- [ ] No test failures in either Python version
- [ ] Docker images build successfully
- [ ] Build summary shows all expected images

### Interpreting CI Failures

#### 1. Test Failures (`test_*` jobs)

**Reproduce locally:**
```bash
cd apps/<module>
SAPPHIRE_TEST_ENV=True .venv/bin/pytest test*/ -v
```

#### 2. Import Verification Failures

**Error:** `ModuleNotFoundError: No module named 'xxx'`

**Fix:** Check `pyproject.toml` dependencies and run `uv sync --all-extras`

#### 3. Docker Build Failures

**Reproduce locally:**
```bash
docker build -f ./apps/<module>/Dockerfile.py312 . 2>&1 | tee build.log
```

#### 4. Base Image Dependency Failures

**Error:** `Unable to find image 'mabesa/sapphire-pythonbaseimage:build-test'`

**Cause:** Base image job failed. Check `build_python_3xx_base_image` job first.

### CI vs Local Environment Variables

| Variable | CI Value | Local Value |
|----------|----------|-------------|
| `SAPPHIRE_TEST_ENV` | `True` (set in workflow) | `True` (set manually or by run_tests.sh) |
| `IMAGE_TAG` | `build-test` or `latest` | `local` or custom |

---

## Stage 4: Server Validation

**Purpose**: Final verification with real data on production server.

### When Required

- Before merging significant changes to main
- Before deploying new Docker image tags
- When testing Python 3.12 migration (`:py312` tag)

### Procedure

1. **SSH to production server**

2. **Pull latest images:**
   ```bash
   docker pull mabesa/sapphire-<module>:<tag>
   ```

3. **Update `.env` if testing new tag:**
   ```bash
   # Edit .env: IMAGE_TAG=py312
   ```

4. **Run end-to-end workflow** using cron job scripts or manually

5. **Verify outputs** (see checklist below)

6. **Revert after testing** if using test tags

### Server Validation Checklist

#### Module Verification

- [ ] preprocessing_runoff: `runoff_day.csv` updated with new data
- [ ] preprocessing_gateway: Quantile-mapped forecasts generated
- [ ] linear_regression: Forecasts generated for all stations
- [ ] machine_learning: ML model inference completed
- [ ] postprocessing: Output files formatted correctly
- [ ] forecast_dashboard: Dashboard accessible and displays data

#### System Verification

- [ ] Logs checked: No ERROR/CRITICAL messages
- [ ] Luigi scheduler: Tasks completing successfully (http://localhost:8082)
- [ ] File permissions: Output files not root-owned
- [ ] Email notifications: Working (if configured)

### Troubleshooting Server Issues

| Issue | Check |
|-------|-------|
| Permission errors | Container user vs host file ownership |
| Connection to iEasyHydro HF | SSH tunnel running on server |
| Module fails to start | `docker run --rm <image> python --version` |
| Missing dependencies | `docker run --rm <image> python -c "import <package>"` |

---

## Quick Reference

### Test Commands

| Action | Command |
|--------|---------|
| Run all tests | `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` |
| Run single module | `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>` |
| Run with verbose | `cd apps/<module> && SAPPHIRE_TEST_ENV=True .venv/bin/pytest test*/ -v` |

### Docker Commands

| Action | Command |
|--------|---------|
| Build base image | `docker build -f apps/docker_base_image/Dockerfile.py312 -t mabesa/sapphire-pythonbaseimage:py312 .` |
| Build module | `docker build -f apps/<module>/Dockerfile.py312 -t mabesa/sapphire-<module>:local .` |
| Verify Python version | `docker run --rm <image> python --version` |
| Check imports | `docker run --rm <image> python -c "import pandas; print('OK')"` |

### CI/CD Commands

| Action | Command |
|--------|---------|
| View workflow status | `gh run list --workflow=build_test.yml` |
| View specific run | `gh run view <run_id> --log` |

---

## Related Documentation

- [Deployment Guide](../deployment.md) - Server deployment procedures
- [Development Guide](../development.md) - Module-specific development
- [Module Issues](../plans/module_issues.md) - Known issues index
- [UV Migration Plan](../plans/uv_migration_plan.md) - Python 3.12 migration details
