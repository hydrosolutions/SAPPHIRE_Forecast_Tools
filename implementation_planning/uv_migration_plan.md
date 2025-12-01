# UV Migration Plan

## Overview

This plan outlines the migration from `pip` + `requirements.txt` to `uv` for dependency management across the SAPPHIRE Forecast Tools project.

**Combined with Python 3.12 upgrade** — The migration will also upgrade from Python 3.11 to Python 3.12 for improved performance, better error messages, and extended security support (EOL: Oct 2028).

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

## Timeline Tracking

| Phase | Module | Status | Image Tag | Notes |
|-------|--------|--------|-----------|-------|
| Pre | Create v0.2.0 release | ✅ Completed | `:latest` | Python 3.11 baseline |
| 0 | Create `:py312` base image | Not started | `:py312` | Parallel to `:latest` |
| 1 | iEasyHydroForecast | Not started | `:py312` | Shared library + uv |
| 2 | preprocessing_runoff | Not started | `:py312` | Pilot module |
| 3a | preprocessing_gateway | Not started | `:py312` | |
| 3b | preprocessing_station_forcing | Not started | `:py312` | |
| 4a | linear_regression | Not started | `:py312` | |
| 4b | machine_learning | Not started | `:py312` | Heavy deps |
| 4c | conceptual_model | N/A | N/A | R-based, skip |
| 5a | forecast_dashboard | Not started | `:py312` | |
| 5b | pipeline | Not started | `:py312` | |
| 6 | Flip `:latest` to py312 | Not started | `:latest` | Final transition |

---

## Resources

- [uv documentation](https://docs.astral.sh/uv/)
- [uv with Docker](https://docs.astral.sh/uv/guides/integration/docker/)
- [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/)
- [Docker Security Maintenance](../doc/maintenance/docker-security-maintenance.md) - Quarterly rebuild process and vulnerability management

---

*Document created: 2025-12-01*
*Last updated: 2025-12-01*
