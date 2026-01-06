# SAPPHIRE Forecast Tools - Suggested Commands (Bea's Local Setup)

## My Local Paths

```bash
# Environment file
ENV_FILE=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm

# Data directory
DATA_DIR=~/Documents/GitHub/kyg_data_forecast_tools

# Repo root
REPO=~/Documents/GitHub/SAPPHIRE_forecast_tools
```

---

## Quick Commands (Copy-Paste Ready)

### Preprocessing Runoff

```bash
# Operational mode (fast daily updates)
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/preprocessing_runoff
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm uv run preprocessing_runoff.py

# Maintenance mode (full lookback, gap filling)
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/preprocessing_runoff
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm uv run preprocessing_runoff.py --maintenance
```

### Linear Regression

```bash
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/linear_regression
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm uv run linear_regression.py
```

### SDK Data Retrieval Test

```bash
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/preprocessing_runoff

# Test single site (default: 17462)
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm uv run python test/testspecial_sdk_data_retrieval.py

# Check all sites for recent data
CHECK_ALL_SITES=1 ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm uv run python test/testspecial_sdk_data_retrieval.py

# Test specific site
TEST_SITE_CODE=15013 ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm uv run python test/testspecial_sdk_data_retrieval.py
```

### Add/Restore Fake Test Data

```bash
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/preprocessing_runoff

# Add fake recent data for testing
python test/add_fake_recent_data.py ~/Documents/GitHub/kyg_data_forecast_tools/intermediate_data

# Restore original files
python test/add_fake_recent_data.py ~/Documents/GitHub/kyg_data_forecast_tools/intermediate_data --restore
```

---

## Running Tests

```bash
# iEasyHydroForecast tests
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps
SAPPHIRE_TEST_ENV=True uv run --directory iEasyHydroForecast pytest iEasyHydroForecast/tests/ -v

# Preprocessing runoff tests
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/preprocessing_runoff
uv run pytest test/ -v

# Forecast dashboard tests
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/forecast_dashboard
uv run pytest tests/ -v

# Pipeline tests
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools/apps/pipeline
uv run pytest tests/ -v
```

---

## Docker Commands

### Build Images (py312)

```bash
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools

# Preprocessing runoff
DOCKER_BUILDKIT=0 docker build -f apps/preprocessing_runoff/Dockerfile.py312 -t mabesa/sapphire-preprocessing-runoff:py312 .

# Linear regression
DOCKER_BUILDKIT=0 docker build -f apps/linear_regression/Dockerfile.py312 -t mabesa/sapphire-linear_regression:py312 .

# Machine learning
DOCKER_BUILDKIT=0 docker build -f apps/machine_learning/Dockerfile.py312 -t mabesa/sapphire-machine_learning:py312 .

# Postprocessing
DOCKER_BUILDKIT=0 docker build -f apps/postprocessing_forecasts/Dockerfile.py312 -t mabesa/sapphire-postprocessing_forecasts:py312 .

# Pipeline
DOCKER_BUILDKIT=0 docker build -f apps/pipeline/Dockerfile.py312 -t mabesa/sapphire-pipeline:py312 .
```

### Run Docker Containers Locally

```bash
DATA_DIR=~/Documents/GitHub/kyg_data_forecast_tools

# Preprocessing runoff in Docker
docker run --rm \
    -e ieasyhydroforecast_data_root_dir=/sapphire_data \
    -e ieasyhydroforecast_env_file_path=/sapphire_data/config/.env_develop_kghm \
    -e IEASYHYDROHF_HOST=http://host.docker.internal:5555/api/v1/ \
    -v $DATA_DIR/config:/sapphire_data/config \
    -v $DATA_DIR/daily_runoff:/sapphire_data/daily_runoff \
    -v $DATA_DIR/intermediate_data:/sapphire_data/intermediate_data \
    mabesa/sapphire-preprocessing-runoff:py312

# Linear regression in Docker
docker run --rm \
    -e ieasyhydroforecast_data_root_dir=/sapphire_data \
    -e ieasyhydroforecast_env_file_path=/sapphire_data/config/.env_develop_kghm \
    -e IEASYHYDROHF_HOST=http://host.docker.internal:5555/api/v1/ \
    -v $DATA_DIR/config:/sapphire_data/config \
    -v $DATA_DIR/daily_runoff:/sapphire_data/daily_runoff \
    -v $DATA_DIR/intermediate_data:/sapphire_data/intermediate_data \
    mabesa/sapphire-linear_regression:py312
```

### SSH Tunnel (if needed for iEasyHydro HF)

```bash
# Start tunnel before running containers that need HF access
ssh -L 5555:localhost:5555 <server>
```

---

## Environment Variables Reference

| Variable | Value | Purpose |
|----------|-------|---------|
| `ieasyhydroforecast_env_file_path` | `~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm` | Main config |
| `IEASYHYDROHF_HOST` | `http://localhost:5555/api/v1/` | HF API endpoint |
| `SAPPHIRE_TEST_ENV` | `True` | Enable test mode |
| `SAPPHIRE_OPDEV_ENV` | `True` | Operational dev mode |

---

## Git Commands

```bash
git status
git log --oneline -10
git diff
git diff main...HEAD
git checkout <branch>
git pull origin main
```
