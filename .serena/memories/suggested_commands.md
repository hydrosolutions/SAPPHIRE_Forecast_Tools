# SAPPHIRE Forecast Tools - Suggested Commands

## Environment Variables (Required for Most Modules)

Most modules require the path to the `.env` configuration file. Set this before running any module:

```bash
# Required: Path to environment configuration file
export ieasyhydroforecast_env_file_path=/path/to/config/.env

# Example paths:
# Local development (from repo root)
export ieasyhydroforecast_env_file_path=$(pwd)/apps/config/.env_develop

# Production-like setup
export ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env
```

### Other Common Environment Variables

```bash
# Enable test mode (required for running tests)
export SAPPHIRE_TEST_ENV=True

# Enable operational development mode
export SAPPHIRE_OPDEV_ENV=True

# Data root directory (used by some modules)
export ieasyhydroforecast_data_root_dir=/path/to/data

# Set Python path for imports
export PYTHONPATH=./apps:./apps/iEasyHydroForecast

# For Docker containers
export IN_DOCKER=True
```

---

## Running the Full System

```bash
# Run the demo version from project root
source bin/run_sapphire_forecast_tools.sh /path/to/data/root/folder
```

---

## Running Individual Modules Locally

### With uv (Python 3.12 - migrated modules)

```bash
# General pattern
cd apps/<module_name>
uv sync --python 3.12
ieasyhydroforecast_env_file_path=/path/to/.env .venv/bin/python <script>.py

# Example: preprocessing_runoff
cd apps/preprocessing_runoff
uv sync --python 3.12
ieasyhydroforecast_env_file_path=../config/.env_develop .venv/bin/python preprocessing_runoff.py

# Example: iEasyHydroForecast (library - typically imported, not run directly)
cd apps/iEasyHydroForecast
uv sync --python 3.12
.venv/bin/python -c "from iEasyHydroForecast import forecast_library; print('OK')"
```

### With pip/conda (Python 3.11 - legacy modules)

```bash
# General pattern
cd apps/<module_name>
pip install -r requirements.txt
ieasyhydroforecast_env_file_path=/path/to/.env python <script>.py

# Example: linear_regression
cd apps/linear_regression
ieasyhydroforecast_env_file_path=../config/.env_develop python linear_regression.py
```

---

## Package Installation

### Using uv (modern approach - for modules with pyproject.toml)

```bash
cd apps/iEasyHydroForecast
uv sync --all-extras

cd apps/preprocessing_runoff
uv sync --all-extras

cd apps/preprocessing_gateway
uv sync --all-extras
```

### Using pip (traditional approach)

```bash
cd apps/<module_name>
pip install -r requirements.txt
```

### Installing iEasyHydroForecast as editable package

```bash
pip install -e ./apps/iEasyHydroForecast
```

---

## Testing

### Run all tests (from apps directory)

```bash
cd apps
bash run_tests.sh  # Note: requires conda environments to be configured
```

### Run specific module tests with uv

```bash
# iEasyHydroForecast
cd apps/iEasyHydroForecast
SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest tests/ -v

# Preprocessing runoff
cd apps/preprocessing_runoff
SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest test/ -v

# Preprocessing gateway
cd apps/preprocessing_gateway
SAPPHIRE_TEST_ENV=True .venv/bin/python -m pytest tests/ -v
```

### Run specific module tests with pip/conda

```bash
cd apps
SAPPHIRE_TEST_ENV=True pytest preprocessing_runoff/test
SAPPHIRE_TEST_ENV=True pytest linear_regression/test
SAPPHIRE_TEST_ENV=True pytest reset_forecast_run_date/tests
```

### Run unittest-style tests

```bash
cd apps
SAPPHIRE_TEST_ENV=True python -m unittest discover -s iEasyHydroForecast/tests -p 'test_*.py'
```

---

## Docker Commands

### Build individual images

```bash
# Base image (Python 3.11)
docker build -f ./apps/docker_base_image/Dockerfile -t mabesa/sapphire-pythonbaseimage:latest .

# Base image (Python 3.12 + uv)
docker build -f ./apps/docker_base_image/Dockerfile.py312 -t mabesa/sapphire-pythonbaseimage:py312 .

# Preprocessing runoff (py312)
docker build -f ./apps/preprocessing_runoff/Dockerfile.py312 -t mabesa/sapphire-preprunoff:py312 .

# Preprocessing runoff (legacy)
docker build -f ./apps/preprocessing_runoff/Dockerfile -t mabesa/sapphire-preprunoff:latest .

# Linear regression
docker build -f ./apps/linear_regression/Dockerfile -t mabesa/sapphire-linreg:latest .

# Forecast dashboard
docker build -f ./apps/forecast_dashboard/Dockerfile -t mabesa/sapphire-dashboard:latest .
```

### Run Docker containers locally

```bash
# Quick test - verify image works
docker run --rm mabesa/sapphire-preprunoff:py312 python --version

# Run with environment variables and volume mounts
docker run --rm \
    --network host \
    -e ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env_develop \
    -e ieasyhydroforecast_data_root_dir=/kyg_data_forecast_tools \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    -v /path/to/local/config:/kyg_data_forecast_tools/config \
    -v /path/to/local/data:/kyg_data_forecast_tools/daily_runoff \
    -v /path/to/local/intermediate:/kyg_data_forecast_tools/intermediate_data \
    mabesa/sapphire-preprunoff:py312
```

### For modules connecting to iEasyHydro HF (requires SSH tunnel)

```bash
# First, start SSH tunnel on host machine
ssh -L 5556:localhost:5556 <server>

# Then run container with HF host variable
docker run --rm \
    --network host \
    -e ieasyhydroforecast_env_file_path=/kyg_data_forecast_tools/config/.env_develop \
    -e IEASYHYDROHF_HOST=http://host.docker.internal:5556/api/v1/ \
    -e SAPPHIRE_OPDEV_ENV=True \
    -e IN_DOCKER=True \
    -v /path/to/config:/kyg_data_forecast_tools/config \
    mabesa/sapphire-linreg:latest
```

---

## Git Commands (Darwin/macOS)

```bash
git status
git log --oneline -10
git diff
git diff <branch>...HEAD
git checkout <branch>
git pull origin main
```

---

## Useful File Operations (Darwin/macOS)

```bash
ls -la                    # List files with details
pwd                       # Print working directory
find . -name "*.py"       # Find Python files
grep -r "pattern" .       # Search in files
```

---

## Pre-commit Hooks

```
