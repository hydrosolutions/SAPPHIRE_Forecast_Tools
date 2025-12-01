# UV Migration Plan

## Overview

This plan outlines the migration from `pip` + `requirements.txt` to `uv` for dependency management across the SAPPHIRE Forecast Tools project.

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

### Approach: Module-by-Module with Shared Library

```
Phase 1: iEasyHydroForecast (shared library)
    ↓
Phase 2: preprocessing_runoff (pilot module)
    ↓
Phase 3: Other preprocessing modules
    ↓
Phase 4: Forecasting modules
    ↓
Phase 5: Dashboard & pipeline
    ↓
Phase 6: Base image consolidation
```

---

## Phase 1: Migrate iEasyHydroForecast

**Why first?** It's a dependency for other modules and already has `pyproject.toml`.

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
requires-python = ">=3.11"
authors = [
    {name = "Beatrice Marti", email = "marti@hydrosolutions.ch"}
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

## Phase 2: Migrate preprocessing_runoff (Pilot)

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
requires-python = ">=3.11"
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

FROM python:3.11-slim-bookworm AS base

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

## Phase 6: Base Image Consolidation

### Option A: Keep base image, add uv
Update `docker_base_image/Dockerfile` to use uv but maintain shared dependencies.

### Option B: Eliminate base image
Each module becomes fully self-contained with its own dependencies.

**Recommendation**: Option B for cleaner separation, but evaluate build times first.

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
- [ ] Update CI/CD workflows if needed
- [ ] Remove old `requirements.txt` (or keep for reference)
- [ ] Update documentation

---

## CI/CD Considerations

### GitHub Actions Update

Add uv to workflows:
```yaml
- name: Install uv
  uses: astral-sh/setup-uv@v4
  with:
    version: "latest"

- name: Install dependencies
  run: uv sync --frozen

- name: Run tests
  run: uv run pytest
```

---

## Rollback Plan

If issues arise:
1. Keep `requirements.txt` files during migration
2. Dockerfile can fall back to pip:
   ```dockerfile
   RUN pip install -r requirements.txt
   ```
3. Lock files can be regenerated: `uv lock --upgrade`

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

| Phase | Module | Status | Notes |
|-------|--------|--------|-------|
| 1 | iEasyHydroForecast | Not started | Shared library |
| 2 | preprocessing_runoff | Not started | Pilot |
| 3a | preprocessing_gateway | Not started | |
| 3b | preprocessing_station_forcing | Not started | |
| 4a | linear_regression | Not started | |
| 4b | machine_learning | Not started | Heavy deps |
| 4c | conceptual_model | N/A | R-based |
| 5a | forecast_dashboard | Not started | |
| 5b | pipeline | Not started | |
| 6 | docker_base_image | Not started | Consolidation |

---

## Resources

- [uv documentation](https://docs.astral.sh/uv/)
- [uv with Docker](https://docs.astral.sh/uv/guides/integration/docker/)
- [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/)

---

*Document created: 2025-12-01*
*Last updated: 2025-12-01*
