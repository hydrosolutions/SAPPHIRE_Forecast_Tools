# Testing Workflow for SAPPHIRE Forecast Tools

This document describes the testing workflow for validating changes before they reach production.

## Prerequisites

### Required Tools

- **uv** package manager: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Python 3.12**: `uv python install 3.12`
- **Docker** with Docker Compose v2
- **Git** for version control

### Initial Setup (One-Time)

1. Clone the repository
2. Set up virtual environments for app modules you'll be testing:
   ```bash
   cd apps/<module_name>
   uv sync --all-extras
   ```
3. Set up virtual environments for sapphire services you'll be testing:
   ```bash
   cd sapphire/services/<service_name>
   uv sync --all-extras
   ```

> **Note**: Each module and service has its own `.venv/` - they are NOT shared.

### Key Environment Variables

| Variable | Purpose | Required For |
|----------|---------|--------------|
| `SAPPHIRE_TEST_ENV` | Isolates tests from production paths | All tests |
| `ieasyhydroforecast_env_file_path` | Path to organization config | Docker testing |
| `SAPPHIRE_PREDICTION_MODE` | Forecast mode (`PENTAD` or `DECAD`) | Docker testing |

---

## Test Writing Guide

This section defines the test categories, quality standards, and patterns required
for all new code in the SAPPHIRE project. See also the Testing Philosophy and
Golden Rules in [CLAUDE.md](../../CLAUDE.md).

### Test Categories

Every new feature or bug fix must include tests. The required categories depend on what changed:

#### 1. Unit Tests (always required)

Isolated tests for individual functions with all external dependencies mocked. Each new or modified public function needs at least:
- A happy-path test with typical input
- An error-path test (invalid input, exception handling)

For error-path tests, always assert both **exception type** and **message fragment**:

```python
with pytest.raises(ValueError, match="horizon must be positive"):
    calculate_forecast(data, horizon=-1)
```

#### 2. Edge Case Tests (required for DataFrame, date, or numeric code)

Any code that processes DataFrames, dates, or numeric values must have edge case tests covering these scenarios:

| Category | Scenarios to test |
|----------|-------------------|
| **Empty data** | Empty DataFrame, single-row DataFrame, all-NaN columns |
| **NaN handling** | All NaN values, mixed NaN/valid, NaN-to-None conversion for API |
| **Date boundaries** | Year transitions (Dec 31 → Jan 1), leap year Feb 29, month boundaries |
| **Value boundaries** | Zero values, very small positives (0.001), very large values (10000+) |
| **Duplicates** | Duplicate date-station combinations |
| **Multi-entity** | Single station many dates, many stations single date |
| **Data preservation** | Non-transformed columns, schema, and row order remain intact after processing |

See `preprocessing_runoff/test/test_edge_cases.py` as the reference implementation.

**File naming**: `test_edge_cases.py` or edge case classes within the relevant test file.

#### 3. Integration Tests (required for multi-step workflows)

Tests that exercise the real logic across multiple internal functions, only mocking external boundaries (API clients, file I/O). Required when:
- A function calls multiple internal modules in sequence
- Data flows through a pipeline (read → transform → write)
- Entry points orchestrate multiple steps

Integration tests should:
- Use real logic for everything inside the boundary
- Only mock the external API client and filesystem — prefer fakes (e.g., a temp directory with real CSVs) over `MagicMock` chains for file I/O
- Validate the full data flow, not just final output — check intermediate state at each pipeline stage
- Verify data preservation: columns not touched by the pipeline must survive unchanged
- Include at least one test that exercises the CSV-fallback path (API disabled) and one that exercises the API path (API enabled with mocked client)

See `postprocessing_forecasts/tests/test_integration_postprocessing.py` as the reference.

**File naming**: `test_integration_<topic>.py`

#### 4. API Failure Tests (required for any code using `sapphire_api_client`)

Any function that reads from or writes to the SAPPHIRE API must have tests for all failure modes:

```python
class TestWriteToApi:
    def test_returns_false_when_api_unavailable(self, data):
        """When sapphire_api_client is not installed."""
        with patch.object(module, "SAPPHIRE_API_AVAILABLE", False):
            assert module._write_to_api(data) is False

    def test_returns_false_when_api_disabled(self, data):
        """When SAPPHIRE_API_ENABLED=false."""
        with patch.object(module, "SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            assert module._write_to_api(data) is False

    def test_returns_false_when_api_not_ready(self, data):
        """When readiness_check fails."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = False
        # ... assert returns False, no exception

    def test_csv_still_written_on_api_failure(self, data, tmp_path):
        """CSV fallback works when API fails."""
        # ... verify CSV written even when API raises
```

The full pattern is documented in `preprocessing_runoff/test/test_api_write.py`.

#### 5. Performance Benchmarks (optional, for optimization work)

Mark with `@pytest.mark.benchmark`. Skipped by default, run explicitly:
```bash
pytest <module>/tests/test_performance.py -v -k bench
```

See `postprocessing_forecasts/tests/test_performance.py` for the pattern.

### Assertion Quality

**Tests must verify correctness, not just existence.** A test that checks "an EM row exists" is not sufficient — it must also check that the EM row has the correct discharge value, station code, date, and row count. Weak assertions let bugs pass silently.

Rules:
- Use **exact counts** (`assert len(em_rows) == 2`) not vague checks (`assert len(em_rows) > 0`)
- **Spot-check at least one record's values** (e.g., `assert record['forecasted_discharge'] == 105.0`)
- For DataFrames, prefer `pd.testing.assert_frame_equal` over row-count comparisons
- For API records, verify field values (not just key existence) for at least one representative record
- Avoid ambiguous `or` in assertions (`assert x.empty or 'EM' not in x` can mask bugs — be explicit about which condition you expect)

### Required conftest.py Pattern

Any module that imports `forecast_library` or `setup_library` (directly or transitively) **must** have a `conftest.py` with the API singleton reset fixture:

```python
"""Shared fixtures for <module> tests."""
import os, sys
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
import forecast_library as fl

@pytest.fixture(autouse=True)
def _reset_api_singletons():
    """Reset forecast_library API client singletons between tests.

    Without this, a mock injected by one test leaks into subsequent tests
    because the singleton caches the first client instance it creates.
    """
    fl._reset_api_clients()
    yield
    fl._reset_api_clients()
```

This fixture is already present in `iEasyHydroForecast`, `postprocessing_forecasts`, and `linear_regression`. Any new module using the API must add it.

### Test File Naming Conventions

| File name pattern | Contents |
|-------------------|----------|
| `test_<topic>.py` | Unit tests for a specific topic or module |
| `test_edge_cases.py` | Edge case and boundary condition tests |
| `test_api_write.py` / `test_api_read.py` | API integration tests (write/read paths) |
| `test_api_integration.py` | Combined API read/write tests |
| `test_integration_<topic>.py` | Multi-step workflow integration tests |
| `test_performance.py` | Performance benchmarks (`@pytest.mark.benchmark`) |

### Test Anti-Patterns (avoid these)

- **Asserting on private attributes** (`._steps`, `._internal_cache`) — test public behavior instead
- **Giant integration tests covering all cases** — push variation into unit tests; integration tests cover the happy-path pipeline and one or two failure modes
- **Hiding critical setup in deeply nested fixtures** — if a test is hard to understand without reading three conftest files, flatten the setup
- **Bare `except:` in test helpers** — let unexpected exceptions propagate so they surface as test failures
- **`MagicMock` chains for internal modules** — if you're mocking three internal functions to test a fourth, the test is too coupled to implementation; restructure or test at a higher level
- **Tests that pass regardless of correctness** — e.g., `assert len(result) > 0` when the function could return garbage rows. Always verify values, not just shapes (see Assertion Quality above)
- **Non-deterministic time dependence** — tests that break on Jan 1 or Feb 29 because they call `date.today()` instead of receiving the forecast date as a parameter
- **`datetime.now()` in default arguments** — `def f(year=datetime.now().year)` is evaluated once at import time and goes stale at year boundaries; always require explicit arguments

### Checks that look like verification and aren't

All four of these shapes were found in this repository in a single day. They are grouped
here because reading the check does not distinguish them from a real one — only mutating
the thing being checked does.

- **A check that cannot fail.** An assertion was narrowed from the whole captured output to a
  single line so it would pass. The narrowing *was* the finding: a
  `print(traceback.format_exc())` beside it was re-leaking an API key that the line above had
  just redacted. (PREPG-017.)
- **A check that cannot pass.** `bin/docker-compose-dashboards.yml` healthchecks probe
  `/pentad_dashboard` and `/decad_dashboard`, but both services serve
  `forecast_dashboard.py`, i.e. `/forecast_dashboard`. Both containers are therefore reported
  unhealthy forever, which trains everyone to ignore the one column that would reveal a real
  restart loop. (INFRA-035.)
- **A check that is never called (historical).** `validate_dashboard_origins` had a full
  passing suite of fixed, parametrised logic and edge-case tests — no fuzzing or
  property-based testing; ad-hoc fuzzing by hand was done separately while drafting the
  fix and is not part of the suite — and deleting all three of its call sites in the
  dashboard launchers used to leave the entire suite green: the logic was tested
  thoroughly, but the fact that production invokes it was not covered at all. This gap is
  now closed — `apps/iEasyHydroForecast/tests/test_launcher_validation_order.py` asserts
  each of the three launcher scripts actually calls `validate_dashboard_origins` before
  bringing the dashboard up, so deleting a call site now fails that suite. (INFRA-032.)
- **A check that reports success while destroying what it validated.** The same validator
  lowercases its input with `tr`. With `tr` absent from `PATH`, the command substitution
  yields empty, so the function silently emptied both origin values and returned 0 — the
  launcher then took the running stack down and started Bokeh with an empty origin, which
  crash-loops. It did not fail to detect a bad value; it manufactured one and passed.
  (INFRA-032.) A second, subtler round of the same shape: a `tr` that exits 0 without
  actually lowercasing (e.g. a locale where `[:upper:]`/`[:lower:]` is a no-op) produces a
  non-empty but *not-canonical* value ("HOST.EXAMPLE:5006" instead of
  "host.example:5006") — structurally well-formed, so a well-formedness re-check still
  returns 0, and Bokeh's allow-list comparison is case-sensitive against a
  Bokeh-lowercased Origin header, so the entry silently matches nothing. **"If a non-empty
  input becomes empty, fail loudly" is necessary but not sufficient** — it is exactly the
  guard that let this second round through. Generalise instead to the durable rule: any
  validator that *transforms* its input — normalise, lowercase, trim, canonicalise — must
  assert the **canonical postcondition** on its output (e.g. "no ASCII uppercase char
  remains"), not merely that the output is non-blank and well-formed.

Two rules fall out of this, and they are the practical payload:

- **Mutate the call site, not just the logic.** Delete the call and confirm tests go red.
  Shape 3 is invisible to anyone reading a green suite, and it is the hardest to see
  precisely because the check itself looks impeccable.
- **Fuzz the environment, not only the input.** Shape 4 cannot be found by fuzzing values,
  however thoroughly — the fault is in the environment. Vary `PATH`, missing coreutils,
  locale, and cron's stripped environment. Shell-side code in this repo runs under cron,
  which is exactly where `PATH` is stripped.

One more, worth its own line because it inverts the usual failure direction: `${var,,}`
(bash 4+ lowercasing) works on the Linux servers and fails on a macOS dev machine, where
`/bin/bash` is 3.2 — the inverse of the usual "works on my machine", and harder to
diagnose because the failure appears where you least expect an environment problem.
**Precisely: this is not a syntax error.** `bash -n` on 3.2 parses `${var,,}` without
complaint (verified) — the failure only happens when the expansion actually *executes*,
where it fails with `bad substitution` (also verified). The consequence matters more than
the label: a syntax-only lint or a `bash -n` gate in CI will not catch this — the only way
to catch it is to actually run the code path under the older bash. Prefer POSIX-portable
constructs in `bin/` shell code, and guard any external command you depend on.

**Telling a real check from a fake one:** make the thing it watches actually break, once, on
purpose. Reading a test cannot distinguish a check that cannot fail from one that works;
mutation can, in seconds. A useful discriminator when an assertion is changed *after* a red
run: ask whether the new assertion admits *more* outcomes than the old one — if it does, it
is a weakening and the failure was probably real; if fewer, it is a correction.

---

## Testing Workflow Overview

```
Stage 1 ──> Stage 1b ──> Stage 2a ──> Stage 2b ──> Stage 3 ──> Stage 4
 Unit        Local        Docker       Docker       CI/CD       Server
 Tests       Pipeline     Smoke        Runs         Tests       Validation
                          Tests        (server)
```

All stages must pass before changes are considered production-ready.

> **Quick start**: `bash apps/run_validation.sh` runs Stage 1 (unit tests).
> `bash apps/run_validation.sh full` runs Stages 1 + 1b + 2a in sequence.
> See [Quick Reference](#unified-validation-recommended) for all options.

> **Don't have server access?** Stages 1 + 1b + 2a + 3 (CI) provide sufficient
> pre-merge validation. Stage 2b and 4 require server infrastructure.

---

## Stage 1: Local Unit/Integration Tests

**Purpose**: Catch logic errors and regressions before building Docker images.

### Running Tests

All tests are run from the `apps/` directory. The `run_tests.sh` script runs both
app module tests and sapphire service tests.

**Recommended** - run all tests (app modules + services):

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh
```

To run a single app module:

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh <module_name>

# Example: test preprocessing_runoff only
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff
```

To run a single sapphire service (use the `service:` prefix):

```bash
cd apps
bash run_tests.sh service:<service_name>

# Example: test postprocessing service only
bash run_tests.sh service:postprocessing
```

> **Note**: Service tests do not require `SAPPHIRE_TEST_ENV` — they use SQLite
> in-memory databases and are fully self-contained.

### App Modules with Tests

| Module | Test Framework | Test Directory | Notes |
|--------|---------------|----------------|-------|
| iEasyHydroForecast | unittest | `tests/` | Core library |
| preprocessing_runoff | pytest | `test/` | Data processing |
| preprocessing_gateway | pytest | `test/` | DG transforms, API integration, pipeline integration |
| postprocessing_forecasts | pytest | `tests/` | Output formatting |
| pipeline | pytest | `tests/` | Container orchestration |
| forecast_dashboard | pytest + Playwright | `tests/` | Integration tests disabled by default |
| linear_regression | pytest | `test/` | Forecasting models |

> **Note**: Test directories are named inconsistently (`test/` vs `tests/`). The `run_tests.sh` script handles both.

### Sapphire Services with Tests

| Service | Location | Test Directory | Notes |
|---------|----------|----------------|-------|
| postprocessing | `sapphire/services/postprocessing/` | `tests/` | CRUD, endpoint, and data migrator tests (SQLite in-memory) |

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
| "No .venv found" | Missing virtual environment | Run `uv sync --all-extras` in the module or service directory |
| Tests connect to production | Missing env var | Ensure `SAPPHIRE_TEST_ENV=True` is set (app modules only) |
| Import errors | Wrong directory | Run from `apps/` directory |
| "Unknown module" | Typo or missing `service:` prefix | Use `service:<name>` for services, plain name for app modules |

---

## Stage 1b: Local Pipeline Run (Optional)

**Purpose**: Run the full forecast pipeline locally using uv-based venvs, without Docker.

This is useful for end-to-end validation against real data before building Docker images.

### Running the Pipeline

All commands are run from the repository root.

**Recommended** - run the full pipeline (both pentad and decad) plus maintenance:

```bash
# 1. Operational pipeline (short-term + long-term, both prediction modes)
SAPPHIRE_PREDICTION_MODE=BOTH \
  ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh all

# 2. Maintenance pipeline (gap-fill + hindcast, both prediction modes)
SAPPHIRE_PREDICTION_MODE=BOTH \
  ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh maintenance
```

> **Note**: `SAPPHIRE_PREDICTION_MODE=BOTH` is handled by `run_locally.sh`,
> which runs PENTAD then DECAD sequentially. Individual modules receive only
> `PENTAD` or `DECAD`. Do not pass `BOTH` directly to machine_learning scripts
> — they only accept `PENTAD` or `DECAD` and will raise an error otherwise.
> (`linear_regression` and `postprocessing_forecasts` do handle `BOTH` natively.)

Selective runs:

```bash
# Dry-run (validates env and venvs without executing)
bash apps/run_locally.sh --dry-run short-term

# Short-term pipeline only (single mode)
SAPPHIRE_PREDICTION_MODE=PENTAD \
  ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh short-term

# Long-term pipeline only (all months 0-9)
ieasyhydroforecast_env_file_path=/path/to/.env \
  bash apps/run_locally.sh long-term

# Single module
ieasyhydroforecast_env_file_path=/path/to/.env \
  SAPPHIRE_PREDICTION_MODE=PENTAD \
  bash apps/run_locally.sh linear_regression

# Continue past failures
bash apps/run_locally.sh --continue-on-error short-term
```

### What It Does

1. Validates the environment (env file, prediction mode, venvs)
2. Runs modules in production dependency order
3. Logs all output to `apps/logs/run_locally_*.log`
4. Prints a timing summary at the end

For full usage details: `bash apps/run_locally.sh --help`

### Success Criteria Checklist

- [ ] `--dry-run` reports all venvs found and env valid
- [ ] Pipeline completes with exit code 0
- [ ] Summary shows all modules PASS
- [ ] Log file created in `apps/logs/`

---

## Stage 2a: Local Docker Smoke Tests

**Purpose**: Verify Docker images build and critical imports work, without needing server infrastructure. This mirrors the build + import checks that CI performs in `build_test.yml`.

### Prerequisites

1. Docker daemon running
2. Run from the repository root (parent of `apps/`)

No `.env` file, server access, or SSH tunnels required.

### Running Smoke Tests

**Recommended** - build all images and run smoke tests (skip ML to save time):

```bash
bash apps/run_docker_tests.sh --skip-ml
```

Full run including ML (~10+ min for ML image build):

```bash
bash apps/run_docker_tests.sh
```

Selective runs:

```bash
# Single target
bash apps/run_docker_tests.sh preprunoff

# Multiple targets
bash apps/run_docker_tests.sh preprunoff linreg dashboard

# Build only (no smoke tests)
bash apps/run_docker_tests.sh --build-only

# Smoke test existing images (no builds)
bash apps/run_docker_tests.sh --skip-build
```

For full usage: `bash apps/run_docker_tests.sh --help`

### What It Does

1. **Build phase**: Builds the base image first (tags as `:local-test` and `:latest` so child Dockerfiles resolve), then builds each module image
2. **Smoke test phase**: Runs `python -c "import ..."` inside each container to verify critical dependencies installed correctly
3. Prints a colored summary with pass/fail/skip counts and timing

### Targets

| Target | Image | Smoke Test |
|--------|-------|------------|
| `base` | `mabesa/sapphire-pythonbaseimage` | `python --version` + `uv --version` |
| `pipeline` | `mabesa/sapphire-pipeline` | `import luigi; import docker; import yaml; import requests; import tenacity` |
| `preprunoff` | `mabesa/sapphire-preprunoff` | `import preprocessing_runoff` |
| `prepgateway` | `mabesa/sapphire-prepgateway` | `import pandas; import numpy; import scipy; import sklearn; import luigi; import sapphire_dg_client` |
| `linreg` | `mabesa/sapphire-linreg` | `import pandas; import numpy; import docker; from ieasyhydro_sdk.sdk import IEasyHydroSDK` |
| `ml` | `mabesa/sapphire-ml` | `import torch; import darts; import pandas; import numpy` |
| `dashboard` | `mabesa/sapphire-dashboard` | `import panel; import holoviews; import bokeh; import pandas; import numpy` |
| `postprocessing` | `mabesa/sapphire-postprocessing` | `import pandas; import numpy; import openpyxl` |

### Success Criteria Checklist

- [ ] All builds pass (no FAIL in build summary)
- [ ] All smoke tests pass (no FAIL in smoke summary)
- [ ] Script exits with code 0

### Common Issues

| Symptom | Cause | Resolution |
|---------|-------|------------|
| "Docker daemon is not running" | Docker not started | Start Docker Desktop or `dockerd` |
| "Must run from the repository root" | Wrong working directory | `cd` to repo root before running |
| Base build fails | System dependency or network issue | Check `docker build` output manually |
| Child build fails | Base image not available | Ensure base builds first (script handles this) |
| Smoke test import fails | Missing dependency in `pyproject.toml` | Check `uv.lock` and rebuild |

---

## Stage 2b: Server Docker Pipeline Runs

**Purpose**: Verify Docker containers run correctly with real data and server infrastructure.

> **Note**: If you don't have server access, Stage 2a + CI (Stage 3) provides
> sufficient pre-merge validation.

### Prerequisites

1. Docker running locally
2. Valid `.env` file for your organization
3. Server access (SSH tunnels, data volumes)
4. Updated Docker images on DockerHub (see below)

### Pushing Images to DockerHub from a Feature Branch

The server pulls images from DockerHub, so your branch's images must be pushed
there before you can test on the server. By default, only pushes to `main`
trigger `deploy_main.yml` (which builds and pushes to DockerHub).

To test a feature branch on the server **before merging to main**:

1. **Temporarily edit** `.github/workflows/deploy_main.yml` to add your branch:
   ```yaml
   on:
     push:
       branches:
         - main
         - your-branch-name   # TEMPORARY — remove before merging
   ```
2. Push the commit. CI will build and push images to DockerHub with the `:latest` tag.
3. On the server, `docker pull` the updated images.
4. **Remove the branch trigger** from `deploy_main.yml` before merging to main.

> **Important**: This is a temporary edit. Do not merge the branch trigger into
> main — it would cause every push to that branch to overwrite production images
> after the merge.

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

**Purpose**: Automated gate ensuring tests pass before Docker images are built.

### Workflow Files

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `build_test.yml` | Push to non-main branches, PRs to main | Build-only (no push to DockerHub) |
| `deploy_main.yml` | Push to main | Build + push to DockerHub |

### CI Pipeline Stages

```
Tests (Python 3.12)
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

- Python 3.12 tests (uv-based)
- Docker image builds for all modules
- Import verification for modules without tests

> **Tip**: CI performs the same build + import tests as `run_docker_tests.sh`.
> Running Stage 2a locally catches failures before pushing, saving CI time.

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
docker build -f ./apps/<module>/Dockerfile . 2>&1 | tee build.log
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
- When testing significant infrastructure changes

### Procedure

1. **SSH to production server**

2. **Pull latest images:**
   ```bash
   docker pull mabesa/sapphire-<module>:<tag>
   ```

3. **Update `.env` if testing new tag:**
   ```bash
   # Edit .env: IMAGE_TAG=latest
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

### Unified Validation (recommended)

| Action | Command |
|--------|---------|
| **Quick validation (tests only)** | `bash apps/run_validation.sh` |
| **Full validation (all stages)** | `ieasyhydroforecast_env_file_path=<path> bash apps/run_validation.sh full` |
| Full, skip pipeline | `bash apps/run_validation.sh full --skip-pipeline --skip-ml` |
| Full, skip Docker | `ieasyhydroforecast_env_file_path=<path> bash apps/run_validation.sh full --skip-docker` |
| Dry-run check | `bash apps/run_validation.sh full --dry-run` |

`run_validation.sh` orchestrates `run_tests.sh` (Stage 1), `run_locally.sh` (Stage 1b), and `run_docker_tests.sh` (Stage 2a) in sequence. Use `--continue-on-error` to run all stages even if an earlier one fails. Logs are saved to `apps/logs/validation_*.log`.

### Test Commands

| Action | Command |
|--------|---------|
| **Run all tests (recommended)** | `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` |
| Run single app module | `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>` |
| Run single service | `cd apps && bash run_tests.sh service:<service>` |
| Run with verbose | `cd apps/<module> && SAPPHIRE_TEST_ENV=True .venv/bin/pytest test*/ -v` |
| **Full local pipeline (recommended)** | `SAPPHIRE_PREDICTION_MODE=BOTH ieasyhydroforecast_env_file_path=<path> bash apps/run_locally.sh all` |
| **Full maintenance (recommended)** | `SAPPHIRE_PREDICTION_MODE=BOTH ieasyhydroforecast_env_file_path=<path> bash apps/run_locally.sh maintenance` |
| Dry-run validation | `bash apps/run_locally.sh --dry-run short-term` |

### Docker Commands

| Action | Command |
|--------|---------|
| **Docker smoke tests (recommended)** | `bash apps/run_docker_tests.sh --skip-ml` |
| Docker smoke tests (all) | `bash apps/run_docker_tests.sh` |
| Single target smoke test | `bash apps/run_docker_tests.sh preprunoff` |
| Build only (no smoke tests) | `bash apps/run_docker_tests.sh --build-only` |
| Smoke test existing images | `bash apps/run_docker_tests.sh --skip-build` |
| Build base image (manual) | `docker build -f apps/docker_base_image/Dockerfile -t mabesa/sapphire-pythonbaseimage:latest .` |
| Build module (manual) | `docker build -f apps/<module>/Dockerfile -t mabesa/sapphire-<module>:local .` |
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
- [UV Migration Plan](../plans/archive/uv_migration_plan_COMPLETED_2026-01-29.md) - Python 3.12 migration details (completed)
