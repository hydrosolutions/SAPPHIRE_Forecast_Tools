# SAPPHIRE Forecast Tools - Task Completion Checklist

When completing a task, verify the following before considering it done.

---

## Quick Reference: Which Workflow to Use?

| Module | Package Manager | Python Version | Status |
|--------|-----------------|----------------|--------|
| iEasyHydroForecast | uv | 3.12 | ✅ Migrated |
| preprocessing_runoff | uv | 3.12 | ✅ Migrated |
| preprocessing_gateway | uv | 3.12 | 🔄 In Progress |
| All other modules | pip | 3.11 | Legacy |

---

## Code Changes

### 1. Tests Pass

**For uv-migrated modules (Python 3.12):**
```bash
cd apps/<module_name>

# Sync dependencies (creates/updates .venv)
uv sync --python 3.12

# Verify Python version
.venv/bin/python --version  # Should show 3.12.x

# Run tests
SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest tests/ -v
```

**For legacy modules (Python 3.11 + pip):**
```bash
cd apps
SAPPHIRE_TEST_ENV=True pytest <module>/test[s]/ -v
```

### 2. Code Style
- Follow snake_case for functions and variables
- Use PascalCase for classes
- Include docstrings for new functions (Google style)
- Add logging for important operations (use `logger`, not `print()`)
- Handle errors gracefully with informative messages
- Use type hints for all new code (see code_style_conventions.md)

### 3. Dependencies
**For uv modules:**
- Update `pyproject.toml` with new dependency
- Run `uv lock` to update lock file
- Commit both `pyproject.toml` and `uv.lock`

**For pip modules:**
- Update `requirements.txt`
- Test installation in clean environment

---

## Local Testing (without Docker)

Before testing in Docker, verify the module works locally:

```bash
cd apps/<module_name>

# For uv modules
uv sync --python 3.12
ieasyhydroforecast_env_file_path=/path/to/config/.env .venv/bin/python <script>.py

# Verify output files are created correctly
# Run pytest if tests exist
.venv/bin/python -m pytest tests/ -v
```

---

## Docker Testing

### Step 1: Build Base Image (if not already built)

```bash
# From repository root

# Python 3.12 + uv base image
docker build -f apps/docker_base_image/Dockerfile.py312 \
  -t mabesa/sapphire-pythonbaseimage:py312 .

# Python 3.11 base image (legacy)
docker build -f apps/docker_base_image/Dockerfile \
  -t mabesa/sapphire-pythonbaseimage:latest .
```

### Step 2: Build Module Image

```bash
# For py312 modules
docker build -f apps/<module_name>/Dockerfile.py312 \
  -t mabesa/sapphire-<module>:py312-test .

# For legacy modules
docker build -f apps/<module_name>/Dockerfile \
  -t mabesa/sapphire-<module>:test .
```

### Step 3: Quick Verification

```bash
# Check Python version
docker run --rm mabesa/sapphire-<module>:py312-test python --version

# Verify imports work
docker run --rm mabesa/sapphire-<module>:py312-test \
  python -c "import pandas; import numpy; print('OK')"
```

### Step 4: Run with Production-Like Setup

```bash
docker run --rm \
    --network host \
    -e ieasyhydroforecast_data_root_dir=/kyg_data_forecast_tools \
    -e ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env_develop \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    -v /path/to/config:/kyg_data_forecast_tools/config \
    -v /path/to/daily_runoff:/kyg_data_forecast_tools/daily_runoff \
    -v /path/to/intermediate_data:/kyg_data_forecast_tools/intermediate_data \
    mabesa/sapphire-<module>:py312-test
```

**For modules connecting to iEasyHydro HF** (requires SSH tunnel):
```bash
# First, start SSH tunnel on host:
ssh -L 5556:localhost:5556 <server>

# Then add to docker run:
-e IEASYHYDROHF_HOST=http://host.docker.internal:5556/api/v1/
```

---

## CI/CD Integration

### Before Merging a PR

- [ ] All existing CI tests pass (check GitHub Actions)
- [ ] If adding py312 support, add test job to `build_test.yml`
- [ ] Ensure `tests/` directory exists with at least one test
- [ ] Verify local Docker build succeeds

### CI Workflow Overview

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `build_test.yml` | Push to non-main, PRs to main | Build and test (no push) |
| `deploy_main.yml` | Push to main | Build, test, and push to DockerHub |

### Adding py312 Test Job

When migrating a module, add to `.github/workflows/build_test.yml`:

```yaml
test_<module>_py312:
  runs-on: ubuntu-latest
  name: Test <module> (Python 3.12 + uv)
  steps:
    - uses: actions/checkout@v4
    - uses: astral-sh/setup-uv@v5
    - run: uv python install 3.12
    - working-directory: ./apps/<module>
      run: uv sync --all-extras
    - working-directory: ./apps
      run: |
        SAPPHIRE_TEST_ENV=True <module>/.venv/bin/python -m pytest <module>/tests/ -v
```

---

## Module Migration Checklist (uv + Python 3.12)

When migrating a module from pip to uv:

### Step 1: Create uv Configuration
- [ ] Create `pyproject.toml` with dependencies from `requirements.txt`
- [ ] Add `[tool.uv.sources]` for iEasyHydroForecast dependency
- [ ] Run `uv lock` to generate lock file
- [ ] Run `uv sync --python 3.12` and verify imports

### Step 2: Local Testing
- [ ] Run module script with uv venv
- [ ] Verify output files created correctly
- [ ] Run pytest if tests exist

### Step 3: Docker Configuration
- [ ] Create `Dockerfile.py312`
- [ ] Build base image locally
- [ ] Build module image locally
- [ ] Test with production-like volume mounts

### Step 4: CI/CD Integration
- [ ] Add py312 test job to `build_test.yml`
- [ ] Add py312 build job to `deploy_main.yml`
- [ ] Verify CI passes

### Step 5: Server Testing
- [ ] Test module on production server
- [ ] Verify end-to-end workflow

### Step 6: Cleanup
- [ ] Keep `requirements.txt` for rollback reference
- [ ] Update module README if needed

---

## Common Module Test Commands

### uv Modules (Python 3.12)

| Module | Test Command |
|--------|--------------|
| iEasyHydroForecast | `cd apps/iEasyHydroForecast && SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest tests/ -v` |
| preprocessing_runoff | `cd apps/preprocessing_runoff && SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest test/ -v` |
| preprocessing_gateway | `cd apps/preprocessing_gateway && SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest tests/ -v` |

### Legacy Modules (Python 3.11 + pip)

| Module | Test Command |
|--------|--------------|
| linear_regression | `cd apps && SAPPHIRE_TEST_ENV=True pytest linear_regression/test` |
| reset_forecast_run_date | `cd apps && SAPPHIRE_TEST_ENV=True pytest reset_forecast_run_date/tests` |

---

## Environment Variables

```bash
# Required for tests
export SAPPHIRE_TEST_ENV=True

# For local development
export SAPPHIRE_OPDEV_ENV=True

# Config file path (adjust to your setup)
export ieasyhydroforecast_env_file_path=/path/to/config/.env_develop
```

---

## Rollback Plan

If issues arise after migration:

1. **Switch back to legacy Dockerfile:**
   ```dockerfile
   FROM mabesa/sapphire-pythonbaseimage:latest  # Python 3.11
   ```

2. **Use pip installation:**
   ```dockerfile
   RUN pip install -r requirements.txt
   ```

3. **Regenerate lock file if needed:**
   ```bash
   uv lock --upgrade
   ```

---

## Future: Automation Ideas

These manual steps could be automated:

- **Pre-commit hook**: Run module tests before commit
- **Local Docker test script**: Build and test all modules
- **CI matrix testing**: Test across Python 3.11 and 3.12
- **Dependency audit**: Automated CVE scanning in CI
- **Health grade monitoring**: Alert on DockerHub Scout grade changes