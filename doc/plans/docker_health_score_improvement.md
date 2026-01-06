# Docker Health Score Improvement Plan: sapphire-linreg-py312

## Current Status
- **Image**: `sapphire-linreg-py312`
- **Health Score**: D (needs improvement)
- **Base Image**: `mabesa/sapphire-pythonbaseimage:py312`

## Root Causes of Low Health Score

Docker Hub health scores are affected by:
1. **Vulnerable packages** (CVEs in dependencies)
2. **Running as root** (security best practice violation)
3. **Outdated base images**
4. **Unnecessary packages** (larger attack surface)

### Analysis of Current linreg Dockerfile.py312

```dockerfile
# Current issues:
USER root  # ❌ Running as root (line 14)
```

The image runs as root because "This module writes to shared volumes, requiring root access." This is a common pattern but penalizes health score.

---

## Improvement Plan

### Phase 1: Update Dependencies (Quick Win)

**Goal**: Update all packages to latest versions to fix known CVEs.

**Steps**:
1. Update `uv.lock` files for all py312 modules:
   ```bash
   cd apps/linear_regression && uv lock --upgrade
   cd apps/iEasyHydroForecast && uv lock --upgrade
   # Repeat for all modules with uv.lock
   ```

2. Rebuild base image with updated pip (already done: `pip>=25.3`)

3. Test that modules still work after updates

**Modules to update**:
- [ ] `apps/iEasyHydroForecast/uv.lock`
- [ ] `apps/linear_regression/uv.lock`
- [ ] `apps/machine_learning/uv.lock` (if exists)
- [ ] `apps/pipeline/uv.lock` (if exists)
- [ ] `apps/postprocessing_forecasts/uv.lock` (if exists)
- [ ] `apps/forecast_dashboard/uv.lock` (if exists)

### Phase 2: Address Root User Issue (Medium Effort)

**Problem**: Linear regression needs root to write to shared volumes mounted from host.

**Options**:

#### Option A: Use appuser with proper volume permissions (Recommended)
Instead of running as root, configure host volumes with proper permissions:

```dockerfile
# In Dockerfile.py312
USER appuser

# Ensure the entrypoint fixes permissions if needed
# Or document that host must set: chmod -R 777 /path/to/shared/volume
```

**Deployment change**: Update docker-compose or run scripts to ensure mounted volumes are writable by UID 1000.

#### Option B: Use gosu for privilege dropping
Run as root initially, then drop to appuser after volume access:

```dockerfile
# Install gosu
RUN apt-get update && apt-get install -y gosu && rm -rf /var/lib/apt/lists/*

# Entrypoint script that:
# 1. Fixes permissions on mounted volumes
# 2. Drops to appuser
# 3. Runs the actual command
```

#### Option C: Accept the trade-off
If operational constraints require root, document the security implications and accept the lower health score. This is valid for internal/operational images.

### Phase 3: Minimize Base Image (Lower Priority)

Review base image for unnecessary packages:

```dockerfile
# Current in Dockerfile.py312:
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \      # Needed for compiling Python extensions
        git \      # Needed for git+ dependencies
        libkrb5-dev \  # Kerberos - is this needed?
    && rm -rf /var/lib/apt/lists/*
```

**Questions to answer**:
- Is `libkrb5-dev` needed? (Kerberos development headers)
- Can we use multi-stage build to exclude gcc from final image?

### Phase 4: Multi-stage Build (Advanced)

Use multi-stage build to minimize final image:

```dockerfile
# Build stage
FROM python:3.12-slim-bookworm AS builder
RUN apt-get update && apt-get install -y gcc git
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
# Install all dependencies

# Runtime stage
FROM python:3.12-slim-bookworm AS runtime
# Only copy installed packages, no build tools
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
```

---

## Implementation Order

1. **Immediate** (this session):
   - [x] Fix pip vulnerability in base image (done: `pip>=25.3`)
   - [ ] Update uv.lock files with `uv lock --upgrade`

2. **Short-term** (next session):
   - [ ] Test updated dependencies in CI
   - [ ] Evaluate Option A for running as non-root

3. **Medium-term**:
   - [ ] Implement non-root user solution if feasible
   - [ ] Review and remove unnecessary system packages

4. **Long-term**:
   - [ ] Multi-stage builds for smaller images
   - [ ] Automated dependency updates (Dependabot/Renovate)

---

## Commands Reference

```bash
# Update a single module's lock file
cd apps/linear_regression && uv lock --upgrade

# Check for outdated packages
uv pip list --outdated

# Audit for known vulnerabilities (requires pip-audit)
pip-audit

# Build and test locally
docker build -f apps/linear_regression/Dockerfile.py312 -t test-linreg:py312 .
docker run --rm test-linreg:py312 python -c "import linear_regression; print('OK')"
```

---

## Notes

- Health score D → B typically requires fixing high/critical CVEs
- Health score B → A requires best practices (non-root, minimal image)
- Some trade-offs are acceptable for operational images
