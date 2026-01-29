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
| reset_forecast_run_date | Yes | No | 3 packages (DEPRECATED) |
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

### conceptual_model ⚠️ MAINTENANCE-ONLY

- **R-based**: Not applicable for uv migration
- **Status (2025-12-06)**: This module is now in **maintenance-only mode**
- **Deprecation planned**: Resources will focus on machine_learning module going forward
- **Reason**: Upstream GitHub dependencies (`hydrosolutions/airGR_GM`, `hydrosolutions/airgrdatassim`) are no longer actively maintained
- **Action**: Keep current approach (rocker/tidyverse base image) with periodic security updates only
- **Customer support**: Existing customers will continue to be supported, but no new features
- See `documentation_improvement_plan.md` Step 7.5b for full details

### reset_forecast_run_date ⚠️ DEPRECATED

- **Status (2026-01-29)**: This module is **deprecated** and will not be migrated to Python 3.12 + uv
- **Reason**: Functionality superseded by other approaches; module is rarely used
- **Action**: Skip uv migration; module will remain on Python 3.11 + pip until removal
- **CI/CD**: Existing `test_reset_forecast_run_date` and `push_reset_forecast_to_Dckerhub` jobs in `deploy_main.yml` continue to build the legacy `:latest` image using the old Dockerfile
- **Future**: Module will be removed in a future cleanup

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

**Status**: ✅ Completed (2026-01-29)

### Prerequisites

All prerequisites were met:
- [x] All modules tested and working with `:py312`
- [x] No open issues related to Python 3.12 compatibility
- [x] Server testing completed (2026-01-26)
- [x] OBS-001 ML issue fixed (marker file check removed)
- [x] Workflows consolidated (2026-01-29)

### Step 6.0: Commit Staged Plan Files

Before merging, commit any staged files to keep the branch clean:

```bash
git status  # Check for staged files
git commit -m "Add planning documents from maxat_sapphire_2 branch"
```

**Current staged files:**
- `doc/plans/postprocessing_forecasts_improvement_plan.md`
- `doc/plans/sapphire_showcase_positioning.md`

### Step 6.1: Create Archive Tags for Python 3.11

Tag current `:latest` images as `:py311` for rollback capability:

```bash
# Archive all production images
for img in pythonbaseimage pipeline preprunoff prepgateway linreg ml dashboard postprocessing; do
  docker pull mabesa/sapphire-$img:latest
  docker tag mabesa/sapphire-$img:latest mabesa/sapphire-$img:py311
  docker push mabesa/sapphire-$img:py311
done
```

### Step 6.2: Update deploy_main.yml

Three changes required in `.github/workflows/deploy_main.yml`:

#### 6.2a: Revert branch trigger to `main`

```yaml
on:
  push:
    # CHANGE FROM: branches: [ "implementation_planning" ]
    branches: [ "main" ]
```

#### 6.2b: Switch `:latest` jobs to use `Dockerfile.py312`

For each module's `:latest` build job, change the Dockerfile reference:

```yaml
# BEFORE
-f ./apps/<module>/Dockerfile .

# AFTER
-f ./apps/<module>/Dockerfile.py312 .
```

**Jobs updated (2026-01-29):**
- [x] `build_and_push_base_image` → uses `Dockerfile.py312`
- [x] `push_pipeline_to_Dockerhub` → uses `Dockerfile.py312`
- [x] `push_preprocessing_runoff_to_Dockerhub` → uses `Dockerfile.py312`
- [x] `push_preprocessing_gateway_to_Dockerhub` → uses `Dockerfile.py312`
- [x] `push_machine_learning_to_Dockerhub` → uses `Dockerfile.py312`
- [x] `push_dashboard_to_Dockerhub` → uses `Dockerfile.py312`
- [x] `push_postprocessing_to_Dockerhub` → uses `Dockerfile.py312`
- [x] `push_linreg_to_Dockerhub` → uses `Dockerfile.py312`

#### 6.2c: Keep `:py312` jobs (for now)

**Decision**: Keep the `:py312` jobs during transition period. They will build identical images to `:latest` but provide a safety net. Remove them in Step 6.10 after confirming stability.

### Step 6.3: Update README Badge

```markdown
![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)
```

### Step 6.4: Create Pull Request

Create PR from `implementation_planning` → `main`:

```bash
gh pr create --base main --head implementation_planning \
  --title "Python 3.12 + uv migration" \
  --body "## Summary
- Migrates all modules from Python 3.11 + pip to Python 3.12 + uv
- Adds pyproject.toml and uv.lock to all modules
- Creates Dockerfile.py312 for all modules
- Flips :latest tag to use Python 3.12

## Testing
- All modules tested locally with Python 3.12
- Server testing completed 2026-01-26
- CI/CD py312 jobs passing

## Rollback
If issues arise, use :py311 archived images."
```

### Step 6.5: Merge Pull Request

After PR review and approval:

```bash
gh pr merge --squash  # or --merge depending on preference
```

### Step 6.6: Verify CI/CD Passes on Main

After merge, monitor GitHub Actions:

1. Go to https://github.com/hydrosolutions/SAPPHIRE_forecast_tools/actions
2. Verify the `deploy_main.yml` workflow triggers on the merge commit
3. Check that all jobs complete successfully:
   - [ ] Test jobs pass
   - [ ] `:latest` images build and push successfully
   - [ ] `:py312` images build and push successfully

**If CI fails**: Do NOT proceed. Debug and fix issues first.

### Step 6.7: Verify DockerHub Images Updated

Confirm new images are on DockerHub:

```bash
# Check image manifests show Python 3.12
docker pull mabesa/sapphire-pythonbaseimage:latest
docker run --rm mabesa/sapphire-pythonbaseimage:latest python --version
# Expected: Python 3.12.x

# Verify uv is available
docker run --rm mabesa/sapphire-pythonbaseimage:latest uv --version
```

### Step 6.8: Team Communication

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

### Step 6.9: Completed - Dockerfile Migration to `uv sync`

All modules have been updated to use `uv sync` instead of `uv export`:

- [x] `preprocessing_runoff` - uses `uv sync`
- [x] `forecast_dashboard` - uses `uv sync`
- [x] `linear_regression` - uses `uv sync`
- [x] `machine_learning` - uses `uv sync`
- [x] `pipeline` - uses `uv sync`
- [x] `postprocessing_forecasts` - uses `uv sync`
- [x] `preprocessing_gateway` - uses `uv sync`
- [x] `preprocessing_station_forcing` - uses `uv sync`

### Step 6.10: Cleanup

**Completed (2026-01-29):**
- [x] Remove `:py312` jobs from `deploy_main.yml` - consolidated into `:latest` jobs
- [x] Remove `:py312` jobs from `deploy_local.yml` - consolidated into `:local` jobs
- [x] Remove legacy Python 3.11 test jobs from `deploy_main.yml`
- [x] Update `deploy_docs.yml` to Python 3.12 + uv

**Remaining cleanup (optional):**
- [ ] Remove Python 3.11 jobs from `build_test.yml` (if py311 support is fully dropped)
- [ ] Consider removing `:py311` archive tags from DockerHub if no longer needed
- [ ] Optionally rename `Dockerfile.py312` to `Dockerfile` (cosmetic)

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

### Step 1: Create uv Configuration
- [ ] Create `pyproject.toml` with dependencies from `requirements.txt`
- [ ] Add `[tool.uv.sources]` for iEasyHydroForecast
- [ ] Run `uv lock` to generate lock file
- [ ] Run `uv sync --python 3.12` to create venv with Python 3.12 and verify imports work
  ```bash
  cd apps/<module_name>
  uv sync --python 3.12
  .venv/bin/python --version  # Should show Python 3.12.x
  ```

### Step 2: Local Testing (without Docker)
- [ ] Run module script directly with uv venv:
  ```bash
  cd apps/<module_name>
  ieasyhydroforecast_env_file_path=/path/to/config/.env .venv/bin/python <script>.py
  ```
- [ ] Verify output files are created correctly
- [ ] Run pytest if tests exist: `.venv/bin/python -m pytest`

### Step 3: Docker Configuration
- [ ] Create `Dockerfile.py312` using uv
- [ ] Build base image locally (if not already built):
  ```bash
  # From repository root
  docker build -f apps/docker_base_image/Dockerfile.py312 -t mabesa/sapphire-pythonbaseimage:py312 .
  ```
- [ ] Build module Docker image locally:
  ```bash
  # From repository root
  docker build -f apps/<module_name>/Dockerfile.py312 -t mabesa/sapphire-<module>:py312-test .
  ```
- [ ] Verify image works (quick test):
  ```bash
  # Check Python version
  docker run --rm mabesa/sapphire-<module>:py312-test python --version

  # Verify all imports work
  docker run --rm mabesa/sapphire-<module>:py312-test python -c "import pandas; import numpy; print('OK')"
  ```
- [ ] Run Docker container locally with production-like setup:
  ```bash
  # Adjust paths to your local data directory
  docker run --rm \
      --network host \
      -e ieasyhydroforecast_data_root_dir=/kyg_data_forecast_tools \
      -e ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env_develop_kghm \
      -e SAPPHIRE_OPDEV_ENV=True \
      -e IN_DOCKER=True \
      -v /path/to/kyg_data_forecast_tools/config:/kyg_data_forecast_tools/config \
      -v /path/to/kyg_data_forecast_tools/daily_runoff:/kyg_data_forecast_tools/daily_runoff \
      -v /path/to/kyg_data_forecast_tools/intermediate_data:/kyg_data_forecast_tools/intermediate_data \
      -v /path/to/kyg_data_forecast_tools/bin:/kyg_data_forecast_tools/bin \
      mabesa/sapphire-<module>:py312-test
  ```

  **For modules connecting to iEasyHydro HF** (e.g., linear_regression, preprocessing_runoff):
  ```bash
  # Add these environment variables:
  -e IEASYHYDROHF_HOST=http://host.docker.internal:5556/api/v1/

  # Note: Requires SSH tunnel to iEasyHydro HF server running on host
  # Start tunnel with: ssh -L 5556:localhost:5556 <server>
  ```

### Step 4: CI/CD Integration
- [ ] Add py312 test job to `build_test.yml`
- [ ] Add py312 build/push job to `deploy_main.yml`
- [ ] Update summary job dependencies in both workflows
- [ ] Verify CI passes before merging

### Step 5: Server Testing
- [ ] Test module on production server
- [ ] Verify end-to-end workflow on server

### Step 6: Cleanup
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

#### Attestation Status for py312 Images

| py312 Image | Has Attestation | Status | Action Required |
|-------------|-----------------|--------|-----------------|
| `sapphire-pythonbaseimage:py312` | ✅ Yes | `build-push-action` with provenance | None |
| `sapphire-preprunoff:py312` | ✅ Yes | `build-push-action` with provenance | None |
| `sapphire-prepgateway:py312` | ✅ Yes | `build-push-action` with provenance | None |
| `sapphire-pipeline:py312` | ✅ Yes | `build-push-action` with provenance | Updated 2025-12-17 |
| `sapphire-ml:py312` | ✅ Yes | `build-push-action` with provenance | Updated 2025-12-17 |
| `sapphire-dashboard:py312` | ✅ Yes | `build-push-action` with provenance | Updated 2025-12-17 |
| `sapphire-postprocessing:py312` | ✅ Yes | `build-push-action` with provenance | Updated 2025-12-17 |
| `sapphire-linreg:py312` | ✅ Yes | `build-push-action` with provenance | Updated 2025-12-17 |

**✅ COMPLETED (2025-12-17)**: All py312 images now use `docker/build-push-action@v6` with `provenance: true` and `sbom: true`.

**Template for adding attestation** (replace manual docker build/push):

```yaml
- name: Set up Docker Buildx
  uses: docker/setup-buildx-action@v3

- name: Log in to the Docker registry
  uses: docker/login-action@v3
  with:
    username: ${{ secrets.DOCKER_USERNAME }}
    password: ${{ secrets.DOCKER_PASSWORD }}

- name: Build and push Docker image with attestation
  uses: docker/build-push-action@v6
  with:
    context: .
    file: ./apps/<module>/Dockerfile.py312
    push: true
    tags: ${{ env.<IMAGE_NAME> }}:${{ env.IMAGE_TAG_PY312 }}
    provenance: true
    sbom: true
    cache-from: type=gha
    cache-to: type=gha,mode=max
```

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
| 0 | Create `:py312` base image | ✅ Completed | `:py312` | B | Minimal OS packages, uv |
| 1 | iEasyHydroForecast | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock, 173 tests pass |
| 2 | preprocessing_runoff | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock, 13 tests pass; local Py3.12 venv test OK; Docker test OK |
| 3a | preprocessing_gateway | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock + Dockerfile.py312; local test OK; Docker test OK (all 3 scripts). CI/CD workflows fixed. |
| 3b | preprocessing_station_forcing | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock + Dockerfile.py312; local Py3.12 test OK; Docker test OK (fails at DB connection as expected - uses old ieasyhydro SDK). No unit tests. No CI/CD (future development). |
| 4a | linear_regression | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock + Dockerfile.py312; local Py3.12 test OK; Docker test OK. CI/CD workflows updated. |
| 4b | machine_learning | ✅ Completed | `:py312` | C/D | pyproject.toml + uv.lock (~500KB, 77 packages) + Dockerfile.py312; torch 2.8.0, darts 0.35.0; local Py3.12 test OK (TFT model predictions working); Docker test OK. CI/CD workflows updated. |
| 4c | conceptual_model | N/A | N/A | — | R-based, skip |
| 4d | reset_forecast_run_date | N/A (DEPRECATED) | N/A | — | Deprecated, skip migration |
| 5a | forecast_dashboard | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock (67 packages) + Dockerfile.py312; Panel 1.4.5, Bokeh 3.4.3 (pinned <3.5 for FuncTickFormatter compatibility), HoloViews 1.19.1; local Py3.12 test OK; Docker operational test OK. CI/CD workflows updated. |
| 5b | pipeline | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock (35 packages) + Dockerfile.py312; Luigi 3.6.0; local Py3.12 test OK; Docker operational test OK (Docker socket access verified). CI/CD workflows updated. |
| 5c | postprocessing_forecasts | ✅ Completed | `:py312` | B | pyproject.toml + uv.lock (29 packages) + Dockerfile.py312; local Py3.12 test OK (script runs successfully with pentad/decadal forecasts). |
| 6 | Flip `:latest` to py312 | ✅ Completed | `:latest` | B avg | **2026-01-29**: All workflows consolidated. See Phase 6 completion notes below. |
| 7 | Full package upgrade | Not started | `:latest` | B | Upgrade all packages to latest versions after py312 migration complete. |

### Shell Script Fixes for py312 (2026-01-16)

The following fixes were required for shell scripts to work with the py312 Docker images:

**Issue 1: PYTHONPATH override breaking Luigi module resolution**

Scripts were overriding PYTHONPATH with Python 3.11 paths, breaking the `apps.pipeline` module import:
```bash
# BROKEN (Python 3.11 paths)
-e PYTHONPATH="/home/appuser/.local/lib/python3.11/site-packages:${PYTHONPATH}"
```

**Fixed scripts** (removed PYTHONPATH override, docker-compose-luigi.yml sets `PYTHONPATH=/app`):
- `bin/run_preprocessing_gateway.sh`
- `bin/run_pentadal_forecasts.sh`
- `bin/run_decadal_forecasts.sh`
- `bin/run_preprocessing_runoff.sh`

**Issue 2: luigi.cfg ConfigParser interpolation error**

The `%(LUIGI_SCHEDULER_URL)s` syntax is ConfigParser interpolation (looks for config keys), not environment variable substitution.

**Fixed**: `apps/pipeline/luigi.cfg` - removed broken interpolation (scheduler host/port passed via CLI args)

**Issue 3: linear_regression maintenance script path duplication**

The maintenance script was overriding CMD with a relative path that got resolved relative to WORKDIR (`/app/apps/linear_regression/`), causing `/app/apps/linear_regression/apps/linear_regression/linear_regression.py`.

**Fixed**:
- `apps/linear_regression/Dockerfile.py312` - Added `RUN_MODE` env var support in CMD
- `bin/daily_linreg_maintenance.sh` - Use `-e RUN_MODE=maintenance` instead of command override

**Issue 4: pipeline Dockerfile WORKDIR/CMD mismatch**

**Fixed**: `apps/pipeline/Dockerfile.py312` - CMD now uses `cd /app/apps/pipeline && uv run luigi ...` to ensure venv is found

### Phase 6 Flip Procedure

**Status**: ✅ Completed (2026-01-29)

**All prerequisites were met and Phase 6 has been executed.**

**Completion Summary (2026-01-29):**

| Step | Task | Status |
|------|------|--------|
| 6.0 | Commit staged plan files | ✅ No staged files needed |
| 6.1 | Create `:py311` archive tags | ✅ Base image archived |
| 6.2a | Branch trigger on `main` | ✅ Already configured |
| 6.2b | Switch `:latest` to `Dockerfile.py312` | ✅ Consolidated |
| 6.2c | Remove redundant `:py312` jobs | ✅ Done (kept quality features) |
| 6.3 | Update README.md badge | ✅ Shows Python 3.12 |
| 6.4/6.5 | PR to main | ✅ Previously merged |
| 6.6 | Verify CI/CD | ✅ Passing |
| 6.7 | Verify DockerHub images | ✅ Python 3.12.12, uv 0.9.27 |
| 6.8 | Team communication | Pending |

**Workflow Consolidation (2026-01-29):**

All production workflows have been updated to use Python 3.12 + uv exclusively:

| Workflow | Branch | Tag | Changes |
|----------|--------|-----|---------|
| `deploy_main.yml` | `main` | `:latest` | Consolidated py312 jobs with attestation, removed legacy py311 tests |
| `deploy_local.yml` | `local` | `:local` | Full rewrite to match deploy_main.yml structure |
| `scheduled_security_rebuild.yml` | Quarterly | `:latest` + `:YYYY-QN` | Updated to Dockerfile.py312 with attestation |
| `deploy_docs.yml` | Manual | N/A | Updated to Python 3.12 + uv |

**Deprecated modules removed from all workflows:**
- `reset_forecast_run_date` - deprecated, removed
- `conceptual_model` - R-based, commented out

**All workflows now use:**
- `docker/build-push-action@v6` with `provenance: true` and `sbom: true`
- `docker/setup-buildx-action@v3` and `docker/login-action@v3`
- GitHub Actions caching (`cache-from: type=gha`, `cache-to: type=gha,mode=max`)
- `Dockerfile.py312` for all modules

---

## Phase 6.11: Dockerfile Cleanup (2026-01-29)

**Goal**: Clean up the Dockerfile naming by removing old Python 3.11 Dockerfiles and renaming `Dockerfile.py312` to `Dockerfile`.

**Status**: ✅ Completed

### Step 6.11.1: Update Base Image References (CRITICAL)

**Issue found**: All `Dockerfile.py312` files currently reference `:py312` tag instead of `:latest`.

**Action**: Before renaming, update all module Dockerfiles to use `:latest`:

```dockerfile
# BEFORE (WRONG):
FROM mabesa/sapphire-pythonbaseimage:py312 AS base

# AFTER (CORRECT):
FROM mabesa/sapphire-pythonbaseimage:latest AS base
```

**Modules to update (8 total):**
- [x] `preprocessing_runoff/Dockerfile.py312`
- [x] `preprocessing_gateway/Dockerfile.py312`
- [x] `linear_regression/Dockerfile.py312`
- [x] `machine_learning/Dockerfile.py312`
- [x] `forecast_dashboard/Dockerfile.py312`
- [x] `pipeline/Dockerfile.py312`
- [x] `postprocessing_forecasts/Dockerfile.py312`
- [x] `preprocessing_station_forcing/Dockerfile.py312`

**Exception**: `docker_base_image/Dockerfile.py312` uses `FROM python:3.12-slim-bookworm` (no base image dependency).

### Step 6.11.2: Remove Old Dockerfiles

**Modules with both `Dockerfile` (py311) and `Dockerfile.py312` (8 total):**
- [x] `docker_base_image`
- [x] `preprocessing_runoff`
- [x] `preprocessing_gateway`
- [x] `linear_regression`
- [x] `machine_learning`
- [x] `forecast_dashboard`
- [x] `pipeline`
- [x] `postprocessing_forecasts`

```bash
git rm apps/docker_base_image/Dockerfile
git rm apps/preprocessing_runoff/Dockerfile
git rm apps/preprocessing_gateway/Dockerfile
git rm apps/linear_regression/Dockerfile
git rm apps/machine_learning/Dockerfile
git rm apps/forecast_dashboard/Dockerfile
git rm apps/pipeline/Dockerfile
git rm apps/postprocessing_forecasts/Dockerfile
```

**Modules with only old `Dockerfile` (deprecated/R-based - leave as-is):**
- `reset_forecast_run_date` - deprecated
- `conceptual_model` - R-based, maintenance-only
- `configuration_dashboard` - R-based, deprecated

### Step 6.11.3: Rename Dockerfile.py312 to Dockerfile

**Modules to rename (9 total):**
```bash
git mv apps/docker_base_image/Dockerfile.py312 apps/docker_base_image/Dockerfile
git mv apps/preprocessing_runoff/Dockerfile.py312 apps/preprocessing_runoff/Dockerfile
git mv apps/preprocessing_gateway/Dockerfile.py312 apps/preprocessing_gateway/Dockerfile
git mv apps/linear_regression/Dockerfile.py312 apps/linear_regression/Dockerfile
git mv apps/machine_learning/Dockerfile.py312 apps/machine_learning/Dockerfile
git mv apps/forecast_dashboard/Dockerfile.py312 apps/forecast_dashboard/Dockerfile
git mv apps/pipeline/Dockerfile.py312 apps/pipeline/Dockerfile
git mv apps/postprocessing_forecasts/Dockerfile.py312 apps/postprocessing_forecasts/Dockerfile
git mv apps/preprocessing_station_forcing/Dockerfile.py312 apps/preprocessing_station_forcing/Dockerfile
```

### Step 6.11.4: Update GitHub Actions Workflows

Update all workflow files to reference `Dockerfile` instead of `Dockerfile.py312`:

**Files and reference counts:**
- [x] `.github/workflows/deploy_main.yml` (8 references)
- [x] `.github/workflows/deploy_local.yml` (8 references)
- [x] `.github/workflows/scheduled_security_rebuild.yml` (9 references)
- [x] `.github/workflows/build_test.yml` (14 references)

**Total: 39 references to update**

### Step 6.11.5: Update Docker Compose and Shell Scripts in bin/

**Docker Compose files:**
- [x] `bin/docker-compose-luigi.yml` - line 26: `Dockerfile.py312` → `Dockerfile`
- [x] `bin/docker-compose-dashboards.yml` - lines 35, 79: `Dockerfile.py312` → `Dockerfile`

**Total: 3 references to update**

**Special files (leave as-is):**
- `bin/luigi-daemon.Dockerfile` - separate daemon, Python 3.11 for stability

### Step 6.11.6: Verify No Stray References

After all updates, verify no references remain:
```bash
# Should find 0 results:
grep -r "Dockerfile\.py312" .github/ bin/ apps/ --include="*.yml" --include="*.yaml" --include="*.sh"
grep -r "Dockerfile\.py312" . --include="*.md"
```

### Step 6.11.7: Verify CI/CD Still Works

After all changes:
1. Push to feature branch
2. Verify `build_test.yml` passes
3. Create PR and verify all checks pass

### Summary of Changes

| Action | Count |
|--------|-------|
| Update base image refs (`:py312` → `:latest`) | 8 |
| Remove old Dockerfiles | 8 |
| Rename `.py312` → `Dockerfile` | 9 |
| Update GitHub Actions | 39 |
| Update docker-compose | 3 |
| **Total** | **67**

---

## Phase 6.12: Remove Legacy requirements.txt Files (2026-01-29)

**Goal**: Clean up legacy requirements.txt files from modules that have migrated to uv (pyproject.toml + uv.lock).

**Status**: ✅ Completed

### Files Removed (9 total)

All modules with uv.lock had their requirements.txt removed:

- [x] `apps/iEasyHydroForecast/requirements.txt`
- [x] `apps/preprocessing_runoff/requirements.txt`
- [x] `apps/preprocessing_gateway/requirements.txt`
- [x] `apps/preprocessing_station_forcing/requirements.txt`
- [x] `apps/forecast_dashboard/requirements.txt`
- [x] `apps/linear_regression/requirements.txt`
- [x] `apps/machine_learning/requirements.txt`
- [x] `apps/postprocessing_forecasts/requirements.txt`
- [x] `apps/pipeline/requirements.txt`

### Files Retained (3 total)

These modules still need requirements.txt:

- `apps/docker_base_image/requirements.txt` - Base image, no pyproject.toml yet
- `apps/conceptual_model/requirements.txt` - R-based module, lists R packages
- `apps/reset_forecast_run_date/requirements.txt` - Deprecated module, kept for reference

---

## Phase 6.13: Documentation Cleanup (2026-01-29)

**Goal**: Update all documentation to remove outdated references to `requirements.txt` and pip commands.

**Status**: ✅ Completed

### Files Updated

| File | Changes |
|------|---------|
| `README.md` | Updated preprocessing_gateway folder structure: `requirements.txt` → `pyproject.toml` + `uv.lock` |
| `doc/plans/deployment_improvement_planning.md` | Replaced `pip install -r requirements.txt` with `uv sync` commands |
| `doc/plans/Makefile.planned` | Updated `setup-venv` target to use `uv sync` per module |
| `doc/plans/postprocessing_forecasts_improvement_plan.md` | Updated folder tree: `requirements.txt` → `pyproject.toml` + `uv.lock` |
| `doc/development.md` | Updated machine_learning folder structure and installation instructions |
| `doc/dev/testing_workflow.md` | Removed Dockerfile.py312 references |
| `doc/maintenance/docker-security-maintenance.md` | Updated to Python 3.12 |
| `doc/plans/docker_health_score_improvement.md` | Updated Dockerfile examples |
| `apps/iEasyHydroForecast/README.md` | Consolidated to single uv workflow |
| `apps/postprocessing_forecasts/README.md` | Added uv sync instructions |
| `bin/docker-compose-*.yml` | Updated default tags to `:latest` |

### References Retained (Historical Context)

The following files intentionally retain `requirements.txt` references as historical/migration documentation:
- `doc/plans/uv_migration_plan.md` - Documents the migration FROM requirements.txt
- `doc/uv-migration-guide.md` - Comparison guide for old vs new approach
- `doc/development.md` (conceptual_model section) - R-based module still uses requirements.txt

---

## Phase 7: Full Package Upgrade (Post-Migration)

**Goal**: Update all packages to latest secure versions after Python 3.12 migration is stable.

**When**: Incrementally, as part of module refactoring work (not as a dedicated phase).

> **Note**: Package upgrades will be performed when working on refactoring each module, rather than as a separate bulk upgrade. This allows testing changes in context and reduces risk of introducing regressions across multiple modules simultaneously.

### Steps

1. **Update iEasyHydroForecast (base)**
   ```bash
   cd apps/iEasyHydroForecast
   uv lock --upgrade
   uv sync
   uv run pytest tests/ -v
   ```

2. **Update each dependent module**
   ```bash
   # For each module:
   cd apps/<module>
   uv lock --upgrade
   uv sync
   # Run tests if available
   ```

   Modules to update:
   - [ ] preprocessing_runoff
   - [ ] preprocessing_gateway
   - [ ] preprocessing_station_forcing
   - [ ] linear_regression
   - [ ] machine_learning
   - [ ] postprocessing_forecasts
   - [ ] forecast_dashboard
   - [ ] pipeline

3. **Rebuild and push all images**
   - Push changes to trigger CI/CD rebuild of all py312 images

4. **Full server validation**
   - Re-test complete forecast workflow with updated packages

### Known Package Updates Available (as of 2025-12-17)

| Package | Current | Latest | Priority |
|---------|---------|--------|----------|
| scikit-learn | 1.7.2 | 1.8.0 | Medium |
| urllib3 | 2.6.0 | 2.6.2 | High (security) |
| pytest | 9.0.1 | 9.0.2 | Low |
| filelock | 3.20.0 | 3.20.1 | Low |
| pre-commit | 4.5.0 | 4.5.1 | Low |
| tzdata | 2025.2 | 2025.3 | Low |

---

## CI/CD Integration

**Status (2026-01-29):** All production workflows consolidated to Python 3.12 + uv only.

### Production Workflows (Python 3.12 + uv only)

#### deploy_main.yml (push to main → `:latest` tag)

| Job | Purpose |
|-----|---------|
| `test_ieasyhydroforecast_py312` | Test iEasyHydroForecast library |
| `test_pipeline_py312` | Test pipeline module |
| `test_preprocessing_runoff_py312` | Test preprocessing_runoff |
| `test_preprocessing_gateway_py312` | Test preprocessing_gateway |
| `test_machine_learning_py312` | Test ML module |
| `test_dashboard_py312` | Test forecast_dashboard |
| `test_postprocessing_py312` | Test postprocessing_forecasts |
| `test_linear_regression_py312` | Test linear_regression |
| `build_and_push_base_image` | Build base image with attestation |
| `push_*_to_Dockerhub` | Build module images with attestation |

#### deploy_local.yml (push to local → `:local` tag)

Same structure as deploy_main.yml, pushes to `:local` tag for stable hydromet deployments.

#### scheduled_security_rebuild.yml (quarterly → `:latest` + `:YYYY-QN`)

Quarterly security rebuilds with fresh base images. Uses `pull: true` and `no-cache: true`.

### Testing Workflow (Mixed Python versions)

#### build_test.yml (PRs and feature branches)

Intentionally tests both Python 3.11 and 3.12 for broader compatibility coverage during development.

### Key Implementation Details

1. **uv Installation**: Uses `astral-sh/setup-uv@v5` GitHub Action
2. **Python Setup**: `uv python install 3.12` (managed by uv)
3. **Dependency Install**: `uv sync --all-extras`
4. **Docker Builds**: `docker/build-push-action@v6` with:
   - `provenance: true` (SLSA attestation)
   - `sbom: true` (Software Bill of Materials)
   - `cache-from: type=gha` and `cache-to: type=gha,mode=max`
5. **All modules use**: `Dockerfile.py312`

---

## Server Testing Procedure

After CI/CD builds and pushes `:py312` images to DockerHub, test them on the AWS production server before flipping `:latest`.

### Step 1: Temporarily Deploy from Feature Branch

To push `:py312` images from a feature branch (e.g., `implementation_planning`):

1. **Edit `.github/workflows/deploy_main.yml`** - Change the branch trigger:
   ```yaml
   on:
     push:
       # TEMPORARY: Deploy from implementation_planning branch to test py312 images on AWS server
       # TODO: Revert to "main" after server testing is complete
       branches: [ "implementation_planning" ]
   ```

2. **Push to trigger the workflow**:
   ```bash
   git add .github/workflows/deploy_main.yml
   git commit -m "chore: temporarily deploy from implementation_planning for server testing"
   git push origin implementation_planning
   ```

3. **Monitor the GitHub Actions workflow** to ensure images are built and pushed successfully.

### Step 2: Configure Server to Use `:py312` Tag

On the AWS server, update the `.env` file to use the `:py312` tag instead of `:latest`:

```bash
# SSH into the server
ssh user@aws-server

# Edit the .env file
cd /path/to/deployment
nano .env

# Change image tags from :latest to :py312
# Example:
#   IMAGE_TAG=latest  →  IMAGE_TAG=py312
# Or for specific images:
#   BASEIMAGE_TAG=py312
#   PREPRUNOFF_TAG=py312
#   LINREG_TAG=py312
#   etc.
```

### Step 3: Pull and Test Images

```bash
# Pull the new images
docker compose pull

# Run a test forecast cycle
docker compose up preprocessing-runoff
docker compose up linear-regression
# ... test other modules as needed

# Check logs for errors
docker compose logs --tail=100 preprocessing-runoff
```

### Step 4: Verify End-to-End Workflow

Run a complete forecast cycle and verify:
- [ ] All modules start successfully
- [ ] Data is read correctly from mounted volumes
- [ ] Forecasts are generated without errors
- [ ] Output files are written with correct permissions
- [ ] Dashboard displays results correctly (if applicable)

### Step 5: Revert After Testing

After successful server testing:

1. **Revert `.github/workflows/deploy_main.yml`** back to `main` branch:
   ```yaml
   on:
     push:
       branches: [ "main" ]
   ```

2. **On the server**, either:
   - Keep using `:py312` if proceeding to Phase 6, or
   - Revert to `:latest` if more testing is needed

### Troubleshooting

**Permission errors on mounted volumes**:
- Check that container user matches host file ownership
- See [Non-Root User Strategy](#2-non-root-user-strategy) for options

**Module fails to start**:
- Check Python version: `docker run --rm <image>:py312 python --version`
- Verify dependencies: `docker run --rm <image>:py312 python -c "import <package>"`

**iEasyHydro HF connection issues**:
- Ensure SSH tunnel is running on server
- Verify `IEASYHYDROHF_HOST` environment variable is set correctly

---

## Resources

- [uv documentation](https://docs.astral.sh/uv/)
- [uv with Docker](https://docs.astral.sh/uv/guides/integration/docker/)
- [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/)
- [Docker Security Maintenance](../doc/maintenance/docker-security-maintenance.md) - Quarterly rebuild process and vulnerability management

---

---

## Server Testing Observations (2026-01-26)

### Summary

Testing py312 images via operational cronjobs on the production server.

**Verified Working:**
- No errors in `~/logs` or `intermediate_data/docker_logs`
- `hydrograph_decad.csv` - expected output
- `hydrograph_day.csv` - expected output
- `forecast_*_linreg_latest.csv` - all good

**Issue Identified: Patchy ML Forecasts**

ML forecasts in `combined_forecasts_pentad_latest.csv` are intermittent. Example for station 16059:

| Date | Pentad | LR | TFT | TiDE | TSMixer | NE | Exit Code | Status |
|------|--------|----|----|------|---------|----|----|--------|
| 2026-01-25 | 6 | ✅ | ❌ | ❌ | ❌ | ❌ | 3 | **FAILED** |
| 2026-01-20 | 5 | ✅ | ✅ | ✅ | ✅ | ✅ | 0 | OK |
| 2026-01-15 | 4 | ✅ | ✅ | ✅ | ✅ | ✅ | 4 | OK |
| 2026-01-10 | 3 | ✅ | ✅ | ✅ | ✅ | ✅ | 4 | OK |
| 2026-01-05 | 2 | ✅ | ❌ | ❌ | ❌ | ❌ | 3 | **FAILED** |

**Pattern:** Exit code 3 correlates with missing ML forecasts. Linear regression (LR) works consistently.

### Issues to Investigate

| ID | Issue | Priority | Status |
|----|-------|----------|--------|
| OBS-001 | ML forecasts missing - Luigi skips preprocessing_runoff | High | **FIXED** - removed marker file check |

### OBS-001: ML Forecasts Missing Due to Insufficient Input Data

**Root Cause Identified:** ML models fail because discharge input data has too many missing values.

**Evidence from Server (2026-01-26):**

```bash
# CSV header confirms structure:
head -1 combined_forecasts_pentad_latest.csv
# date,code,predictor,pentad_in_month,pentad_in_year,slope,intercept,forecasted_discharge,
# rsquared,model_long,model_short,Q5,Q10,...,Q95,flag,sd_Qsim,discharge

# Raw TiDE output shows flag=1 and empty quantiles:
cat predictions/TIDE/pentad_TIDE_forecast_latest.csv | grep 16059
# ,,,,,,,,,,,,,,,,,,,2026-01-27,16059,2026-01-26 00:00:00,1
#                                                        ↑ flag=1

# Same pattern for TFT and TSMIXER - all show flag=1
```

**What flag=1 means** (from `make_forecast.py:prepare_forecast_data()`):
- Input discharge data has too many missing days (`missing_values['exceeds_threshold']`)
- OR too many missing days at the end of input (`nans_at_end >= threshold_missing_days_end`)

Thresholds are set via environment variables:
- `ieasyhydroforecast_THRESHOLD_MISSING_DAYS_TFT` (etc.)
- `ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END`

**Note on flag values 3.0/4.0 in combined_forecasts:**
The `read_daily_probabilistic_ml_forecasts_pentad()` function averages ALL numeric columns
including `flag` when grouping 5-6 daily forecasts into one pentad value. Mixed success/failure
days produce non-integer averages.

**Upstream Issue:** The ML module isn't receiving enough recent discharge data.

**Server Investigation (2026-01-26):**
- Station 16059 last data in `hydrograph_day.csv`: January 22, 2026 (4 days stale)
- No errors in preprunoff maintenance logs
- Running `daily_preprunoff_maintenance.sh` **manually** successfully fetched the missing data
- Data IS available in iEasyHydro HF database

**Root Cause:** Luigi pipeline skipping preprocessing_runoff task

The `run_pentadal_forecasts.sh` DOES include preprocessing_runoff as a Luigi dependency, but Luigi
appears to skip it (likely due to existing marker files from previous runs).

**Cronjob Schedule Analysis:**
```
03:00 UTC - Gateway Preprocessing (weather)
04:00 UTC - Pentadal Forecasts (includes preprocessing_runoff via Luigi)
05:00 UTC - Decadal Forecasts
19:04 UTC - preprunoff_maintenance (standalone, works correctly)
20:00 UTC - ML maintenance
20:34 UTC - Linreg maintenance
```

**The discrepancy:**
- `daily_preprunoff_maintenance.sh` (standalone) → fetches data correctly
- `preprocessing_runoff` in Luigi pipeline → skipped or not fetching

**Likely cause:** Luigi marker files indicate task is "complete" so Luigi skips re-running it,
even though new data is available in iEasyHydro HF. This is a known issue (see P-001 in module_issues.md).

**Fix Applied (2026-01-26):**

Removed marker file check for `preprocessing_runoff` from pipeline. Changes in `apps/pipeline/pipeline_docker.py`:

1. `LinearRegression.requires()` - now always returns `PreprocessingRunoff()`
2. `ConceptualModel.requires()` - now always returns `[PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]`
3. `RunMLModel.requires()` - now always returns `[PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]`
4. `RunAllMLModels.requires()` - now always yields `PreprocessingRunoff()` and `PreprocessingGatewayQuantileMapping()`
5. `PreprocessingRunoff.run()` - removed marker file writing (no longer needed)

**Rationale:** With py312 migration, preprocessing_runoff is fast enough to run every time, ensuring fresh data.

**Cleanup Note:** The `ExternalPreprocessingRunoff` class (line 342) is no longer used and can be removed in a future cleanup.

**Deployment:** Rebuild pipeline Docker image and redeploy to server.

---

*Document created: 2025-12-01*
*Last updated: 2026-01-29*
