# UV Migration Plan

## Overview

This plan outlines the migration from `pip` + `requirements.txt` to `uv` for dependency management across the SAPPHIRE Forecast Tools project.

**Combined with Python 3.12 upgrade** — The migration will also upgrade from Python 3.11 to Python 3.12 for improved performance, better error messages, and extended security support (EOL: Oct 2028).

**Combined with Docker image health improvement** — The migration will address DockerHub Scout security grades by using fresh base images, updated dependencies, and minimal image layers.

---

## Docker Image Health Status (Pre-Migration)

| Image | Current Grade | Primary Issues |
|-------|---------------|----------------|
| sapphire-pythonbaseimage | C | OS packages, Python deps |
| sapphire-ml | E/F | torch, darts dependencies |
| sapphire-dashboard | C | Panel/Bokeh dependencies |
| sapphire-preprunoff | C | Base image inheritance |
| sapphire-prepgateway | C | Base image inheritance |
| sapphire-linreg | C | Base image inheritance |
| sapphire-postprocessing | D | Base image inheritance |
| sapphire-conceptmod | C | R packages |
| sapphire-pipeline | C | Base image inheritance |
| sapphire-rerun | C | Base image inheritance |

### Health Grade Targets

| Grade | Meaning | Target |
|-------|---------|--------|
| A | No vulnerabilities | Ideal, may not be achievable |
| B | Low severity only | **Target for most images** |
| C | Medium severity | Acceptable minimum |
| D/E/F | High/Critical | **Must improve** |

---

## Current State Analysis

### Dependency Files Found

| Module | requirements.txt | pyproject.toml | Dependencies |
|--------|-----------------|----------------|--------------|
| preprocessing_runoff | Yes | No | 14 packages |
| preprocessing_gateway | Yes | No | 6 packages |
| preprocessing_station_forcing | Yes | No | 13 packages |
| postprocessing_forecasts | Yes | No | 13 packages |
| linear_regression | Yes | No | 13 packages |
| machine_learning | Yes | No | 4 packages (darts, torch) |
| conceptual_model | Yes | No | R packages only |
| pipeline | Yes | No | 11 packages |
| forecast_dashboard | Yes | No | 13 packages |
| reset_forecast_run_date | Yes | No | 3 packages |
| iEasyHydroForecast | Yes | **Yes** | 12 packages |
| docker_base_image | Yes | No | 18 packages |

### Key Findings

1. **Base Image Dependency**: Most modules use `mabesa/sapphire-pythonbaseimage:latest` which pre-installs dependencies
2. **iEasyHydroForecast**: Already has `pyproject.toml` - good starting point
3. **Version Inconsistencies**: numpy (1.24.3 - 2.0.0), pandas (2.0.3 - 2.2.2) vary across modules
4. **External SDK**: `ieasyhydro-python-sdk` installed from GitHub, not PyPI

---

## Migration Strategy

### Approach: Parallel Images + Gradual Module Migration

To minimize risk to colleagues and production, we run Python 3.11 and 3.12 images in parallel during migration:

```
                              :latest (Python 3.11 + pip)
                             ╱
Current: sapphire-baseimage ─
                             ╲
                              :py312 (Python 3.12 + uv) ← NEW

Phase 0: Create :py312 base image (parallel to :latest)
    ↓
Phase 1: iEasyHydroForecast (migrate to uv, test with :py312)
    ↓
Phase 2: preprocessing_runoff (pilot module with :py312)
    ↓
Phase 3: Other preprocessing modules (migrate to :py312)
    ↓
Phase 4: Forecasting modules (migrate to :py312)
    ↓
Phase 5: Dashboard & pipeline (migrate to :py312)
    ↓
Phase 6: Flip :latest to Python 3.12 + uv, archive :py311
```

### Docker Image Tag Strategy

| Tag | Python | Package Manager | Status |
|-----|--------|-----------------|--------|
| `:latest` | 3.11 | pip | **Current production** (unchanged during migration) |
| `:py312` | 3.12 | uv | **Testing/migration** (new) |
| `:py311` | 3.11 | pip | **Archive** (created at end of migration) |

### Benefits of Parallel Approach

1. **Zero disruption** — Colleagues continue using `:latest` unchanged
2. **Safe testing** — Validate Python 3.12 + uv on `:py312` tag first
3. **Easy rollback** — Just switch back to `:latest` if issues arise
4. **Gradual adoption** — Migrate modules one-by-one to `:py312`
5. **Clear transition** — Flip `:latest` to 3.12 only after all modules validated

---

## Pre-Migration: Create Release (Python 3.11 Baseline)

**Goal**: Tag the current `main` branch before starting migration. This provides a stable rollback point and reference for users who need to stay on Python 3.11.

**Status**: ✅ Completed — Release `v0.2.0` created.

### Branch Situation

```
main ────────────────────────────── (tested, will be tagged as v0.2.0)
  │
  ├── local ─────────────────────── (deployed at Kyrgyz/Tajik, behind main)
  │
  ├── colleague branches ─────────── (can rebase later)
  │
  └── feature/python312-uv ──────── (migration work, created after tagging)
```

### Step -1.1: Verify Main Branch is Ready

```bash
git checkout main
git pull origin main
git status  # Should be clean
```

Check that CI is passing on GitHub for `main` branch.

### Step -1.2: Create Release Tag

✅ **Completed**: Release `v0.2.0` created and pushed.

### Step -1.3: Create GitHub Release

✅ **Completed**: GitHub release `v0.2.0` published.

### Step -1.4: Create Migration Branch

✅ **Completed**: Migration branch created and active.

All migration work (Phases 0-6) will be done on this branch.

### Step -1.5: (Optional) Tag Docker Images with Version

Manually tag current Docker images with version tag:

```bash
# For each image (run once per image)
docker pull mabesa/sapphire-pythonbaseimage:latest
docker tag mabesa/sapphire-pythonbaseimage:latest mabesa/sapphire-pythonbaseimage:v0.2.0
docker push mabesa/sapphire-pythonbaseimage:v0.2.0

# Repeat for other images as needed
```

---

## Phase 0: Create Python 3.12 + uv Base Image (`:py312` tag)

**Goal**: Create a new `:py312` tagged base image with Python 3.12 and uv, while keeping `:latest` unchanged for production stability.

### Step 0.1: Create New Dockerfile for py312

Create `apps/docker_base_image/Dockerfile.py312` (or use build args):

```dockerfile
# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        git \
        libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SAPPHIRE_OPDEV_ENV=True
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Create non-root user (same as existing base image)
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN groupadd --gid $GROUP_ID appgroup && \
    useradd --uid $USER_ID --gid $GROUP_ID --create-home appuser

WORKDIR /app
RUN chown -R appuser:appgroup /app

# Copy and install iEasyHydroForecast
COPY --chown=appuser:appgroup apps/iEasyHydroForecast /app/apps/iEasyHydroForecast

# Install base dependencies using uv
WORKDIR /app/apps/iEasyHydroForecast
RUN uv sync --frozen

USER appuser
WORKDIR /app
```

### Step 0.2: Update GitHub Actions to Build Both Tags

Update `.github/workflows/deploy_main.yml` to build `:py312` in parallel:

```yaml
env:
  IMAGE_TAG: latest
  IMAGE_TAG_PY312: py312

jobs:
  # Existing job - unchanged
  build_and_push_python_311_base_image:
    # ... keeps building :latest with Python 3.11

  # NEW job - builds :py312 in parallel
  build_and_push_python_312_base_image:
    needs: [test_ieasyhydroforecast]
    runs-on: ubuntu-latest
    name: Build and push Python 3.12 + uv base image

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Build Docker image with Python 3.12 + uv
        run: |
          DOCKER_BUILDKIT=1 docker build --pull --no-cache \
          --build-arg PYTHON_VERSION=3.12 \
          -t "${{ env.BASE_IMAGE_NAME }}:${{ env.IMAGE_TAG_PY312 }}" \
          -f ./apps/docker_base_image/Dockerfile.py312 .

      - name: Log in to Docker registry
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}

      - name: Push :py312 image to Dockerhub
        run: |
          docker push "${{ env.BASE_IMAGE_NAME }}:${{ env.IMAGE_TAG_PY312 }}"
```

### Step 0.3: Validate Critical Dependencies

Test that heavy dependencies work with Python 3.12 + uv:

| Package | Version | Python 3.12 + uv Support |
|---------|---------|--------------------------|
| darts | 0.35.0 | ✅ Confirmed |
| torch | 2.8.0 | ✅ Confirmed |
| pandas | >=2.2.2 | ✅ Confirmed |
| numpy | >=1.26.4 | ✅ Confirmed |
| scikit-learn | >=1.5.0 | ✅ Confirmed |
| luigi | >=3.5.0 | ✅ Confirmed |

### Step 0.4: Build and Test Locally

```bash
# Build the :py312 image locally
cd apps/docker_base_image
docker build -f Dockerfile.py312 -t sapphire-baseimage:py312-test ..

# Verify Python version
docker run --rm sapphire-baseimage:py312-test python --version
# Expected: Python 3.12.x

# Verify uv is available
docker run --rm sapphire-baseimage:py312-test uv --version

# Verify key packages
docker run --rm sapphire-baseimage:py312-test \
  python -c "import pandas; import numpy; import sklearn; print('All imports OK')"
```

### Step 0.5: Team Communication

Notify team that `:py312` tag is now available for testing:

```
📢 Python 3.12 + uv Image Available for Testing

A new base image tag is now available:
  mabesa/sapphire-pythonbaseimage:py312

This image uses Python 3.12 and uv package manager.

**For testing only** — Production continues using :latest (Python 3.11)

To test a module with the new image, update your Dockerfile:
  FROM mabesa/sapphire-pythonbaseimage:py312

Please report any issues to @[your-handle]
```

### Step 0.6: Update README Badge (after Phase 6 completion)

**Note**: Keep badge as `Python 3.10+` until `:latest` is switched to Python 3.12 in Phase 6.

---

## Phase 1: Migrate iEasyHydroForecast (with `:py312`)

**Why first?** It's a dependency for other modules and already has `pyproject.toml`.

**Uses**: `mabesa/sapphire-pythonbaseimage:py312` (not `:latest`)

### Step 1.1: Update pyproject.toml

Current `apps/iEasyHydroForecast/pyproject.toml`:
```toml
[build-system]
requires = ["setuptools>=64.0.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "iEasyHydroForecast"
version = "0.1"
dependencies = [
    "pandas",
    "scikit-learn"
]
```

Updated version for uv:
```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "iEasyHydroForecast"
version = "0.1.0"
description = "A package for hydrological forecasting"
readme = "README.md"
requires-python = ">=3.12"
authors = [
    {name = "Beatrice Marti", url = "https://github.com/mabesa"},
    {name = "Sandro Hunziker", url = "https://github.com/sandrohuni"},
    {name = "Maxat Pernebaev", url = "https://github.com/maxatp"},
    {name = "Adrian Kreiner", url = "https://github.com/adriankreiner"},
    {name = "Vjekoslav Večković", url = "https://github.com/vjekoslavveckovic"},
    {name = "Aidar Zhumabaev", url = "https://github.com/LagmanEater"},
]
dependencies = [
    "pandas>=2.2.2",
    "numpy>=1.26.4",
    "scikit-learn>=1.5.0",
    "python-dotenv>=1.0.1",
    "DateTime>=5.5",
    "openpyxl>=3.1.2",
    "requests>=2.32.0",
    "pytz>=2024.1",
    "ieasyhydro-python-sdk @ git+https://github.com/hydrosolutions/ieasyhydro-python-sdk@master",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2.0",
    "pre-commit>=3.7.0",
]

[tool.hatch.build.targets.wheel]
packages = ["forecast_library.py", "tag_library.py", "setup_library.py"]
```

### Step 1.2: Generate lock file

```bash
cd apps/iEasyHydroForecast
uv lock
```

### Step 1.3: Test installation

```bash
uv sync
uv run pytest tests/
```

---

## Phase 2: Migrate preprocessing_runoff (Pilot with `:py312`)

### Step 2.1: Create pyproject.toml

Create `apps/preprocessing_runoff/pyproject.toml`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "preprocessing-runoff"
version = "0.1.0"
description = "Preprocessing module for runoff data in SAPPHIRE Forecast Tools"
requires-python = ">=3.12"
dependencies = [
    # Core data processing
    "pandas>=2.2.2",
    "numpy>=1.26.4",
    "openpyxl>=3.1.2",

    # Date/time handling
    "DateTime>=5.5",
    "python-dateutil>=2.9.0",
    "pytz>=2024.1",

    # Configuration
    "python-dotenv>=1.0.1",

    # HTTP/networking
    "requests>=2.32.0",
    "charset-normalizer>=3.3.2",
    "idna>=3.7",

    # Orchestration
    "luigi>=3.5.0",

    # Packaging
    "packaging>=23.0",
    "et-xmlfile>=1.1.0",

    # Internal dependency
    "iEasyHydroForecast @ file:///${PROJECT_ROOT}/../iEasyHydroForecast",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2.0",
]

[tool.uv]
# Allow local path dependencies
find-links = ["../iEasyHydroForecast"]

[tool.uv.sources]
iEasyHydroForecast = { path = "../iEasyHydroForecast", editable = true }
```

### Step 2.2: Generate lock file

```bash
cd apps/preprocessing_runoff
uv lock
```

### Step 2.3: Update Dockerfile

Current Dockerfile (simplified):
```dockerfile
FROM mabesa/sapphire-pythonbaseimage:latest AS base
WORKDIR /app
COPY apps/preprocessing_runoff /app/apps/preprocessing_runoff
# No pip install - relies on base image
CMD ["sh", "-c", "PYTHONPATH=/app/apps/iEasyHydroForecast python apps/preprocessing_runoff/preprocessing_runoff.py"]
```

New Dockerfile with uv:
```dockerfile
# syntax=docker/dockerfile:1

FROM python:3.12-slim-bookworm AS base

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        git \
        libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SAPPHIRE_OPDEV_ENV=True
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Create non-root user
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN groupadd --gid $GROUP_ID appgroup && \
    useradd --uid $USER_ID --gid $GROUP_ID --create-home appuser

WORKDIR /app

# Copy iEasyHydroForecast first (dependency)
COPY --chown=appuser:appgroup apps/iEasyHydroForecast /app/apps/iEasyHydroForecast

# Copy preprocessing_runoff module
COPY --chown=appuser:appgroup apps/preprocessing_runoff /app/apps/preprocessing_runoff

# Install dependencies using uv
WORKDIR /app/apps/preprocessing_runoff
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Switch to non-root user
USER appuser

WORKDIR /app

# Run the application
CMD ["uv", "run", "--project", "/app/apps/preprocessing_runoff", "python", "/app/apps/preprocessing_runoff/preprocessing_runoff.py"]
```

### Step 2.4: Test locally

```bash
cd apps/preprocessing_runoff

# Create virtual environment and install
uv sync

# Run tests
uv run pytest

# Test the main script
uv run python preprocessing_runoff.py
```

### Step 2.5: Test Docker build

```bash
# From project root
docker build -f apps/preprocessing_runoff/Dockerfile -t preprocessing-runoff-uv .
docker run --rm preprocessing-runoff-uv
```

---

## Phase 3: Migrate Other Preprocessing Modules

Repeat Phase 2 pattern for:
- [ ] `preprocessing_gateway`
- [ ] `preprocessing_station_forcing`

These share similar dependency profiles with preprocessing_runoff.

---

## Phase 4: Migrate Forecasting Modules

### linear_regression
- Similar to preprocessing modules
- Add `docker` package dependency

### machine_learning
- **Special case**: Heavy dependencies (darts, torch)
- Consider keeping separate base image for ML
- Lock file will be large (~500+ packages)

### conceptual_model
- **R-based**: Not applicable for uv migration
- Keep current approach

---

## Phase 5: Migrate Dashboard & Pipeline

### forecast_dashboard
- Panel/Bokeh visualization stack
- Consider pinning older numpy/pandas if needed for compatibility

### pipeline
- Luigi orchestration
- Docker client for container management

---

## Phase 6: Flip `:latest` to Python 3.12 + uv

**Goal**: After all modules are validated with `:py312`, switch `:latest` to Python 3.12 + uv and archive the old Python 3.11 image.

### Prerequisites

Before starting Phase 6, ensure:
- [ ] All modules tested and working with `:py312`
- [ ] No open issues related to Python 3.12 compatibility
- [ ] Team notified and agreed on transition date

### Step 6.1: Create Archive Tag for Python 3.11

```bash
# Tag current :latest as :py311 for archive
docker pull mabesa/sapphire-pythonbaseimage:latest
docker tag mabesa/sapphire-pythonbaseimage:latest mabesa/sapphire-pythonbaseimage:py311
docker push mabesa/sapphire-pythonbaseimage:py311
```

### Step 6.2: Update deploy_main.yml

Change the `:latest` build job to use Python 3.12 + uv:

```yaml
build_and_push_base_image:
  # Now builds Python 3.12 + uv as :latest
  steps:
    - name: Build Docker image
      run: |
        DOCKER_BUILDKIT=1 docker build --pull --no-cache \
        -t "${{ env.BASE_IMAGE_NAME }}:${{ env.IMAGE_TAG }}" \
        -f ./apps/docker_base_image/Dockerfile.py312 .
```

### Step 6.3: Update All Module Dockerfiles

Change modules back to `:latest` (now Python 3.12):

```dockerfile
# All modules can now use :latest again
FROM mabesa/sapphire-pythonbaseimage:latest
```

### Step 6.4: Update README Badge

```markdown
![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)
```

### Step 6.5: Team Communication

```
📢 Migration Complete: Python 3.12 + uv Now Default

The :latest tag now uses Python 3.12 and uv package manager.

**What changed**:
- mabesa/sapphire-pythonbaseimage:latest → Python 3.12 + uv
- mabesa/sapphire-pythonbaseimage:py311 → Archived Python 3.11 (if needed)

**Action required**:
- Run `docker pull` to get new images
- No Dockerfile changes needed if using :latest

**Rollback** (if needed):
  FROM mabesa/sapphire-pythonbaseimage:py311
```

### Step 6.6: Cleanup (Optional)

After 2-4 weeks of stable operation:
- [ ] Remove `:py312` tag (redundant with `:latest`)
- [ ] Consider removing `:py311` tag if no longer needed
- [ ] Remove `Dockerfile.py312` if consolidated into main Dockerfile

---

## Docker Compose Integration

No changes needed to `docker-compose.yml` files. The Dockerfile changes handle uv integration transparently.

Example service definition remains the same:
```yaml
services:
  preprocessing-runoff:
    build:
      context: ..
      dockerfile: apps/preprocessing_runoff/Dockerfile
    volumes:
      - ../data:/app/data
      - ../apps/config:/app/apps/config
```

---

## Handling iEasyHydroForecast as Shared Dependency

### Option 1: Path dependency (recommended for development)
```toml
[tool.uv.sources]
iEasyHydroForecast = { path = "../iEasyHydroForecast", editable = true }
```

### Option 2: Git dependency (for production)
```toml
dependencies = [
    "iEasyHydroForecast @ git+https://github.com/hydrosolutions/SAPPHIRE_forecast_tools@main#subdirectory=apps/iEasyHydroForecast",
]
```

### Option 3: UV Workspace (advanced)
Create root `pyproject.toml`:
```toml
[tool.uv.workspace]
members = [
    "apps/iEasyHydroForecast",
    "apps/preprocessing_runoff",
    "apps/preprocessing_gateway",
    # ... other modules
]
```

---

## Migration Checklist for Each Module

- [ ] Create `pyproject.toml` with dependencies from `requirements.txt`
- [ ] Add `[tool.uv.sources]` for iEasyHydroForecast
- [ ] Run `uv lock` to generate lock file
- [ ] Run `uv sync` and test locally
- [ ] Update Dockerfile to use uv
- [ ] Test Docker build
- [ ] Add module path to `test_modules.yml` workflow
- [ ] Ensure `tests/` directory exists with at least one test
- [ ] Verify CI passes before merging migration PR
- [ ] Remove old `requirements.txt` (or keep for reference)
- [ ] Update documentation

---

## CI/CD Testing Strategy

### Phased Approach to CI Testing

Adding automated tests to GitHub Actions provides a safety net during migration. We recommend a phased approach:

| Phase | CI Testing Scope | Purpose |
|-------|------------------|---------|
| Phase 0 | Verify base image builds | Validate Python 3.12 compatibility |
| Phase 1 | Add tests for iEasyHydroForecast | First real test automation |
| Phase 2+ | Expand tests per module | Incremental coverage |

### Phase 0: Base Image Validation Workflow

Create `.github/workflows/test_python312.yml` (temporary, can be removed after Phase 0):

```yaml
name: Validate Python 3.12 Base Image

on:
  pull_request:
    paths:
      - 'apps/docker_base_image/**'

jobs:
  build-base-image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build base image with Python 3.12
        run: |
          docker build -t sapphire-baseimage:py312-test \
            apps/docker_base_image/

      - name: Verify Python version
        run: |
          docker run --rm sapphire-baseimage:py312-test \
            python --version | grep "3.12"

      - name: Verify key packages import
        run: |
          docker run --rm sapphire-baseimage:py312-test \
            python -c "import pandas; import numpy; import sklearn; print('All imports OK')"
```

### Phase 1: iEasyHydroForecast Test Workflow

Add to existing workflow or create `.github/workflows/test_modules.yml`:

```yaml
name: Test Python Modules

on:
  push:
    branches: [main]
  pull_request:
    paths:
      - 'apps/iEasyHydroForecast/**'
      - 'apps/preprocessing_runoff/**'
      # Add paths as modules are migrated

jobs:
  test-iEasyHydroForecast:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install uv
        uses: astral-sh/setup-uv@v4
        with:
          version: "latest"

      - name: Install dependencies and run tests
        working-directory: apps/iEasyHydroForecast
        run: |
          uv sync --frozen
          uv run pytest tests/ -v --tb=short

  # Add more jobs as modules are migrated
  test-preprocessing-runoff:
    runs-on: ubuntu-latest
    needs: test-iEasyHydroForecast  # Depends on shared library
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Install and test
        working-directory: apps/preprocessing_runoff
        run: |
          uv sync --frozen
          uv run pytest tests/ -v --tb=short
```

### Benefits of This Approach

1. **Catch regressions early** — PRs are tested before merge
2. **Validate Python 3.12** — Automated verification of compatibility
3. **Incremental adoption** — Add tests per module as they're migrated
4. **Fast feedback** — Path filters ensure only relevant tests run
5. **Documentation as code** — Workflows document how to test each module

### Optional: Matrix Testing

For critical modules, test across Python versions:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.12', '3.13']  # Future-proofing
    steps:
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      # ... rest of steps
```

### Migration Checklist Addition

Add to each module's migration checklist:
- [ ] Add module path to `test_modules.yml` workflow
- [ ] Ensure `tests/` directory exists with at least one test
- [ ] Verify CI passes before merging migration PR

---

## Team Coordination

### Impact on Colleagues During Migration

The parallel image strategy minimizes disruption:

| What | Impact | Who's Affected |
|------|--------|----------------|
| `:latest` tag | **Unchanged** during Phases 0-5 | Nobody (production safe) |
| `:py312` tag | New, for testing only | Only those opting in |
| Module Dockerfiles | Optional switch to `:py312` | Per-module basis |

### What Colleagues Can Safely Do During Migration

- ✅ Continue using `:latest` for all work
- ✅ Merge PRs to `main` (won't affect `:latest`)
- ✅ Deploy to production (uses `:latest`)
- ✅ Create new branches and features

### Communication Checkpoints

| Phase | Communication |
|-------|---------------|
| Phase 0 | "`:py312` tag available for testing" |
| Phase 1-5 | "Module X now supports `:py312`" (optional updates) |
| Phase 6 (before) | "Migration to Python 3.12 scheduled for [DATE]" |
| Phase 6 (after) | "`:latest` now uses Python 3.12 + uv" |

### Rollback During Migration (Phases 0-5)

If issues with `:py312`:
1. Module can switch back to `:latest` immediately
2. No impact on other modules or production
3. Debug and fix before retrying

### Rollback After Phase 6

If issues after `:latest` is switched to Python 3.12:

```dockerfile
# Quick fix: Use archived Python 3.11 image
FROM mabesa/sapphire-pythonbaseimage:py311
```

Or revert the deploy_main.yml change to rebuild `:latest` with Python 3.11.

---

## Rollback Plan

If issues arise:
1. Keep `requirements.txt` files during migration
2. Dockerfile can fall back to pip:
   ```dockerfile
   RUN pip install -r requirements.txt
   ```
3. Lock files can be regenerated: `uv lock --upgrade`
4. **During Phases 0-5**: Simply switch module back to `:latest`
5. **After Phase 6**: Use `:py311` archive tag as fallback

---

## Version Alignment

During migration, align versions across modules:

| Package | Recommended Version |
|---------|---------------------|
| numpy | >=1.26.4,<2.0.0 |
| pandas | >=2.2.2 |
| python-dotenv | >=1.0.1 |
| DateTime | >=5.5 |
| requests | >=2.32.0 |
| pytest | >=8.2.0 |
| luigi | >=3.5.0 |

**Note**: numpy 2.0 may cause compatibility issues with some packages. Test thoroughly.

---

## Docker Image Health Improvement Strategy

The migration to Python 3.12 + uv provides an opportunity to significantly improve DockerHub Scout health grades.

### Key Strategies

#### 1. Use Fresh Base Images with `--pull`

Always pull the latest upstream base image:

```dockerfile
# In Dockerfile
FROM python:3.12-slim-bookworm

# In build command
docker build --pull --no-cache ...
```OK

This ensures OS-level security patches are included.

#### 2. Minimize OS Packages

Current base image installs many OS packages. Review and remove unnecessary ones:

```dockerfile
# Before (many packages)
RUN apt-get install -y gcc git libkrb5-dev libtasn1-6 libpcre2-dev \
    openssl libssl-dev libncurses-dev binutils openssh-server \
    libtiff5-dev libpcre3-dev gnutls-bin libc6 sudo

# After (minimal - only what's needed for pip/build)
RUN apt-get install -y --no-install-recommends \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*
```

#### 3. Use Multi-Stage Builds

Separate build dependencies from runtime:

```dockerfile
# Build stage - has compilers
FROM python:3.12-slim-bookworm AS builder
RUN apt-get update && apt-get install -y gcc
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/
COPY . /app
WORKDIR /app
RUN uv sync --frozen --no-dev

# Runtime stage - minimal
FROM python:3.12-slim-bookworm AS runtime
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
# No gcc, no build tools in final image
```

#### 4. Pin and Update Dependencies

Use uv lock files to ensure consistent, auditable dependencies:

```bash
# Update all dependencies to latest compatible versions
uv lock --upgrade

# Update specific package
uv lock --upgrade-package requests
```

#### 5. Address Known CVEs

For packages with known CVEs:

| Package | Issue | Solution |
|---------|-------|----------|
| setuptools | CVE in older versions | Use `>=70.0.0` |
| requests | Various CVEs | Use `>=2.32.0` |
| urllib3 | CVE-2023-45803 | Use `>=2.0.7` |
| certifi | Certificate issues | Use latest |

#### 6. Special Case: ML Image (E/F Grade)

The ML image has the worst grade due to torch/darts. This is a structural challenge inherent to deep learning dependencies.

##### Why torch/darts Creates Unavoidable Vulnerability Accumulation

**PyTorch's Massive Footprint:**
- ~2GB package with CUDA binaries, C++ runtime libraries, and 150+ transitive dependencies
- Bundles compiled components (libtorch, libcuda stubs, NCCL) that contain C/C++ code with potential CVEs
- Many dependencies are maintained by different organizations with varying security practices

**Darts' Dependency Chain:**
- Darts is a time series forecasting library that depends on PyTorch, TensorFlow (optional), statsmodels, and many scientific computing packages
- Each major dependency brings its own transitive dependencies
- A typical darts installation resolves to 300-500 packages

**CVE Accumulation Example:**
```
darts → torch → libtorch → libcuda → (CVEs in NVIDIA libraries)
      → numpy → (CVEs in C extensions)
      → scipy → (CVEs in Fortran/C code)
      → pandas → (CVEs in cython extensions)
      → pytorch-lightning → (transitive CVEs)
```

**Quantified Impact:**
- Simple modules (e.g., preprocessing): 50-80 packages → 7-15 CVEs typical
- ML module with darts+torch: 400-500 packages → 35-55 CVEs typical
- Most CVEs are in low-level libraries we don't directly use but cannot remove

##### Why We Accept C/D Grade for ML Image

1. **Industry reality** - PyTorch/TensorFlow images from major organizations (Google, Meta, NVIDIA) also show similar grades on DockerHub Scout
2. **Isolated execution** - ML containers run forecasts and exit; no persistent network exposure
3. **Unused code paths** - Many CVEs are in CUDA/GPU code we don't use (CPU-only inference)
4. **No practical alternative** - Cannot remove torch without losing ML capability

##### Mitigation Strategies

1. **Accept higher risk** - ML dependencies are large and complex (current approach)
2. **Use official PyTorch image** - Better maintained base:
   ```dockerfile
   FROM pytorch/pytorch:2.8.0-py3.12-cuda12.1-cudnn9-runtime
   ```
3. **Pin to specific versions** - Known-good combinations:
   ```toml
   torch = "==2.8.0"
   darts = "==0.35.0"
   ```
4. **CPU-only build** - Smaller attack surface:
   ```bash
   pip install torch --index-url https://download.pytorch.org/whl/cpu
   ```
5. **Quarterly rebuilds** - Pick up security patches in upstream dependencies

### Per-Phase Health Checks

Add health grade verification to each migration phase:

| Phase | Action | Health Target |
|-------|--------|---------------|
| 0 | Base image with minimal OS packages | B or better |
| 1-5 | Module images inherit improved base | C or better |
| 4b | ML image - document known issues | D acceptable |
| 6 | Final verification before flip | All C or better |

### Monitoring Health After Migration

1. Check DockerHub Scout after each phase
2. Document any CVEs that cannot be fixed (no upstream fix)
3. Quarterly rebuilds (via `scheduled_security_rebuild.yml`) keep images fresh

---

## DockerHub Scout Compliance

DockerHub Scout checks several security and best-practice criteria. Here's our strategy for each:

### Scout Check Summary

| Check | Current Status | Priority | Strategy |
|-------|---------------|----------|----------|
| High-profile vulnerabilities | Flagged | High | Fresh base images + updated deps |
| Fixable critical/high CVEs | Flagged | High | `uv lock --upgrade` during migration |
| Unapproved base images | N/A | Skip | DockerHub org policy (not relevant for open source) |
| Missing supply chain attestation | Failing | Medium | Add SLSA provenance to builds |
| Outdated base images | Flagged | High | `--pull --no-cache` in builds |
| AGPL v3 licenses found | ✅ Addressed | Low | Dashboard attribution implemented |
| No default non-root user | Failing | Medium | Careful implementation (see below) |

### 1. Supply Chain Attestation (SLSA Provenance)

Add attestation to prove where images were built. This helps customers verify image authenticity.

**Implementation**: Update `.github/workflows/deploy_main.yml` to use `docker/build-push-action` with attestation:

```yaml
- name: Build and push with attestation
  uses: docker/build-push-action@v6
  with:
    context: .
    file: ./apps/docker_base_image/Dockerfile
    push: true
    tags: ${{ env.BASE_IMAGE_NAME }}:${{ env.IMAGE_TAG }}
    # Enable SLSA provenance attestation
    provenance: true
    sbom: true
```

**Requirements**:
- Uses `docker/build-push-action@v6` (already standard)
- Docker Buildx (included in GitHub Actions runners)
- Attestations stored alongside image on DockerHub

**Add to Phase 0**: Include attestation when building the new `:py312` base image.

### 2. Non-Root User Strategy

**Current State Analysis**:
- Base image creates `appuser` (UID 1000) and switches to it at the end
- Dashboard explicitly runs as `root` (required for Docker socket)
- Pipeline uses `group_add` for Docker socket access
- Volume mounts from host can have permission mismatches

**The Permission Problem**:
```
Host filesystem        Container
--------------        ---------
/data (owned by       /app/data (appuser sees
 host user 1001)       permission denied)
```

When host directories are mounted into containers, the container user (`appuser` UID 1000) may not have read/write access if the host files are owned by a different UID.

**Cross-Platform UID Mismatch**:

| Platform | Default UID | Default GID |
|----------|-------------|-------------|
| Ubuntu | 1000 | 1000 |
| macOS | 501 | 20 (staff) |
| Some servers | 1001+ | varies |

This mismatch causes permission errors when developing on macOS and deploying on Ubuntu, or vice versa.

#### Default Strategy: Keep Root for Volume-Writing Containers

**Decision**: To avoid permission hassles across different platforms, we keep root as the runtime user for containers that write to mounted volumes. This is a pragmatic trade-off:

| Image Category | User | Rationale |
|----------------|------|-----------|
| `sapphire-pythonbaseimage` | `appuser` | No mounted volume writes |
| `preprocessing_*` | `root` | Writes to data volumes |
| `linear_regression` | `root` | Writes to data volumes |
| `machine_learning` | `root` | Writes to data volumes |
| `postprocessing_*` | `root` | Writes to data volumes |
| `conceptual_model` | `root` | Writes to data volumes |
| `forecast_dashboard` | `root` | Docker socket + volume writes |
| `pipeline` | `root` | Docker socket + orchestration |

**Security Context**:
- These containers run internal forecasting workloads, not public-facing services
- Containers are ephemeral (run forecast, exit) with no persistent network exposure
- Host volume isolation limits blast radius of any container compromise
- Root inside container ≠ root on host (Docker's user namespace provides isolation)

**DockerHub Scout**: This approach will fail the "non-root user" check. We accept this trade-off and document the rationale.

#### Alternative Strategy: Non-Root for Security-Conscious Organizations

If a partner organization requires non-root containers for compliance, they can enable non-root mode using runtime user override. **This requires the organization to manage host permissions.**

##### Option A: Runtime User Override (Recommended for Partners)

Override the user at runtime via docker-compose without rebuilding images:

**Step 1**: Create `.env` file with host UID/GID:
```bash
# .env file - run this once on deployment machine
echo "HOST_UID=$(id -u)" >> .env
echo "HOST_GID=$(id -g)" >> .env
```

**Step 2**: Update `docker-compose.yml` services:
```yaml
services:
  preprocessing-runoff:
    image: mabesa/sapphire-preprunoff:latest
    user: "${HOST_UID:-0}:${HOST_GID:-0}"  # 0 = root (default)
    volumes:
      - ./data:/app/data
    environment:
      - HOST_UID=${HOST_UID:-0}
      - HOST_GID=${HOST_GID:-0}
```

**Step 3**: Ensure host directories match the UID:
```bash
# On deployment machine
sudo chown -R $(id -u):$(id -g) /path/to/data
```

**Pros**:
- Works on any platform (macOS UID 501, Ubuntu UID 1000, etc.)
- No image rebuilds required
- Organization controls their own security posture

**Cons**:
- Requires host permission management
- More complex deployment documentation

##### Option B: Build-Time UID Matching

For organizations that build their own images:

```dockerfile
# In module Dockerfile
ARG USER_ID=1000
ARG GROUP_ID=1000

# Create user with matching UID/GID
RUN groupadd --gid $GROUP_ID appgroup || true && \
    useradd --uid $USER_ID --gid $GROUP_ID --create-home appuser || true

# ... copy files with --chown=appuser:appgroup ...

USER appuser
```

Build with host UID:
```bash
docker build \
  --build-arg USER_ID=$(id -u) \
  --build-arg GROUP_ID=$(id -g) \
  -t my-org/sapphire-preprunoff:latest .
```

##### Security Documentation for Partners

Partners requiring non-root containers should:

1. **Assess their threat model** - Internal forecasting has different risks than public APIs
2. **Choose their approach** - Runtime override (Option A) or custom builds (Option B)
3. **Manage host permissions** - Ensure data directories match container UID
4. **Test thoroughly** - Verify on their specific platform (Ubuntu, RHEL, etc.)
5. **Accept responsibility** - Permission issues from non-root mode are outside our support scope

**Template response for security inquiries**:
> Our default images run as root for cross-platform compatibility. For organizations requiring non-root containers, we provide documented runtime override options. The deploying organization assumes responsibility for host permission management when enabling non-root mode.

#### Implementation Plan

**Phase 0**:
1. Verify base image USER directive is at end of Dockerfile ✅ (already correct)
2. Keep module Dockerfiles running as root (no USER directive or explicit `USER root`)
3. Document non-root alternative in deployment guide for security-conscious partners
4. Add security rationale to customer-facing documentation

### 3. AGPL v3 License Review

**What it means**: AGPL requires that if you run AGPL-licensed software as a network service, you must provide source code to users of that service.

**Impact for SAPPHIRE**:
- Customers run forecasts internally (not public service) → likely no impact
- If dashboard is exposed publicly → source links are now provided (see below)

**Status**: ✅ Addressed — Open source attribution added to dashboard.

#### Implemented Attribution

The forecast dashboard now includes comprehensive open source attribution:

1. **Footer Attribution Block** ([layout.py:85-102](apps/forecast_dashboard/src/layout.py#L85-L102)):
   - SAPPHIRE MIT License link to GitHub
   - Key dependencies with their licenses:
     - Panel, Bokeh, HoloViews (BSD-3-Clause)
     - Luigi (Apache 2.0)
     - Darts (Apache 2.0)
     - PyTorch (BSD)
     - NumPy, Pandas, scikit-learn (BSD)
   - Links to source code and documentation

2. **Data Attribution on Plots** ([vizualization.py:1853-1860](apps/forecast_dashboard/src/vizualization.py#L1853-L1860)):
   - "Data: ECMWF IFS HRES via Open Data" displayed on precipitation and temperature plots
   - Uses `make_frame_attribution_hook()` for consistent placement

#### Remaining Action Items

**Optional (Phase 1)**: Run license audit to identify any AGPL packages:

```bash
# Run inside container to check licenses
pip-licenses --format=csv | grep -i agpl
```

**Common AGPL packages in scientific Python**:
- Some database connectors
- Certain geospatial libraries
- Some ML frameworks

If AGPL packages are found:
1. Document them in release notes
2. Source code repository is already public (MIT licensed) ✅
3. Dashboard attribution already links to source ✅

### Scout Compliance Checklist per Phase

| Phase | Attestation | Non-Root | License Audit |
|-------|-------------|----------|---------------|
| 0 | ✅ Add to base image build | Base image only (appuser) | — |
| 1 | ✅ Add to iEasyHydroForecast | N/A (library) | Optional: Run pip-licenses |
| 2-5 | Inherited from base | Keep root (document rationale) | Inherited |
| 6 | Verify all images | Document partner alternatives | ✅ Dashboard attribution complete |

---

## Timeline Tracking

| Phase | Module | Status | Image Tag | Health Target | Notes |
|-------|--------|--------|-----------|---------------|-------|
| Pre | Create v0.2.0 release | ✅ Completed | `:latest` | — | Python 3.11 baseline |
| 0 | Create `:py312` base image | ✅ Completed | `:py312` | B | Minimal OS packages, uv 0.9.13 |
| 1 | iEasyHydroForecast | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock, 173 tests pass |
| 2 | preprocessing_runoff | Not started | `:py312` | B | Pilot module |
| 3a | preprocessing_gateway | Not started | `:py312` | B | |
| 3b | preprocessing_station_forcing | Not started | `:py312` | B | |
| 4a | linear_regression | Not started | `:py312` | B | |
| 4b | machine_learning | Not started | `:py312` | C/D | Heavy deps, torch/darts |
| 4c | conceptual_model | N/A | N/A | — | R-based, skip |
| 5a | forecast_dashboard | Not started | `:py312` | B | |
| 5b | pipeline | Not started | `:py312` | B | |
| 6 | Flip `:latest` to py312 | Not started | `:latest` | B avg | Final transition |

---

## Resources

- [uv documentation](https://docs.astral.sh/uv/)
- [uv with Docker](https://docs.astral.sh/uv/guides/integration/docker/)
- [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/)
- [Docker Security Maintenance](../doc/maintenance/docker-security-maintenance.md) - Quarterly rebuild process and vulnerability management

---

*Document created: 2025-12-01*
*Last updated: 2025-12-01*
