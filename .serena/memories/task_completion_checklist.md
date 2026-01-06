# SAPPHIRE Forecast Tools - Task Completion Checklist

When completing a task, verify the following before considering it done.

---

## Quick Reference: Module Status

| Module | Package Manager | Python | Docker Status |
|--------|-----------------|--------|---------------|
| iEasyHydroForecast | uv | 3.12 | ✅ py312 |
| preprocessing_runoff | uv | 3.12 | ✅ py312 |
| preprocessing_gateway | uv | 3.12 | ✅ py312 |
| linear_regression | uv | 3.12 | ✅ py312 |
| postprocessing_forecasts | uv | 3.12 | ✅ py312 |
| machine_learning | uv | 3.12 | ✅ py312 |
| forecast_dashboard | uv | 3.12 | ✅ py312 |
| pipeline | uv | 3.12 | ✅ py312 |
| preprocessing_station_forcing | uv | 3.12 | ✅ py312 |

All modules have been migrated to uv + Python 3.12 with `uv sync` Dockerfiles.

---

## Code Changes

### 1. Tests Pass

```bash
cd apps/<module_name>

# Sync dependencies (creates/updates .venv)
uv sync

# Run tests
uv run pytest test/ -v   # or tests/ depending on module
```

### 2. Code Style
- Follow snake_case for functions and variables
- Use PascalCase for classes
- Include docstrings for new functions (Google style)
- Add logging for important operations (use `logger`, not `print()`)
- Handle errors gracefully with informative messages
- Use type hints for all new code

### 3. Dependencies
- Update `pyproject.toml` with new dependency
- Run `uv lock` to update lock file
- Commit both `pyproject.toml` and `uv.lock`

---

## Docker Testing

### Step 1: Build Module Image

```bash
# From repository root - use DOCKER_BUILDKIT=0 if platform issues occur
DOCKER_BUILDKIT=0 docker build -f apps/<module_name>/Dockerfile.py312 \
  -t mabesa/sapphire-<module>:py312 .
```

### Step 2: Run with Volume Mounts

```bash
DATA_DIR=/path/to/sapphire_data

docker run --rm \
    -e ieasyhydroforecast_data_root_dir=/sapphire_data \
    -e ieasyhydroforecast_env_file_path=/sapphire_data/config/.env \
    -e SAPPHIRE_OPDEV_ENV=True \
    -v $DATA_DIR/config:/sapphire_data/config \
    -v $DATA_DIR/daily_runoff:/sapphire_data/daily_runoff \
    -v $DATA_DIR/intermediate_data:/sapphire_data/intermediate_data \
    -v $DATA_DIR/bin:/sapphire_data/bin \
    mabesa/sapphire-<module>:py312
```

**For modules connecting to iEasyHydro HF** (requires SSH tunnel):
```bash
# First, start SSH tunnel on host:
ssh -L <port>:localhost:<port> <server>

# Then add to docker run (macOS):
-e IEASYHYDROHF_HOST=http://host.docker.internal:<port>/api/v1/
```

---

## CI/CD Integration

### Before Merging a PR

- [ ] All existing CI tests pass (check GitHub Actions)
- [ ] Local Docker build succeeds
- [ ] Docker container runs without errors

### CI Workflow Overview

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `build_test.yml` | Push to non-main, PRs to main | Build and test (no push) |
| `deploy_main.yml` | Push to main | Build, test, and push to DockerHub |

---

## Common Module Test Commands

| Module | Test Command |
|--------|--------------|
| iEasyHydroForecast | `cd apps && SAPPHIRE_TEST_ENV=True uv run --directory iEasyHydroForecast pytest iEasyHydroForecast/tests/ -v` |
| preprocessing_runoff | `cd apps/preprocessing_runoff && uv run pytest test/ -v` |
| forecast_dashboard | `cd apps/forecast_dashboard && uv run pytest tests/ -v` |
| pipeline | `cd apps/pipeline && uv run pytest tests/ -v` |

---

## Environment Variables

```bash
# Required for tests
export SAPPHIRE_TEST_ENV=True

# For local development
export SAPPHIRE_OPDEV_ENV=True

# Config file path (adjust to your setup)
export ieasyhydroforecast_env_file_path=/path/to/config/.env
```

---

## Sensitive Data Checklist

Before committing, verify:
- [ ] No passwords, API keys, or tokens in code
- [ ] No hardcoded hostnames or IPs
- [ ] No personal paths in committed files
- [ ] `.gitignore` includes `.env*`, `.claude/`, credentials files
- [ ] Review with `git diff` before committing
