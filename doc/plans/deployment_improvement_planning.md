# SAPPHIRE Forecast Tools: Deployment Improvement Plan

## User Requirements Summary
- **Primary goal**: Streamline local testing to avoid the slow cycle of: debug prints → GitHub Actions → test on server
- **Testing needs**: Both individual Python modules AND full pipeline in containers
- **Configuration**: Long-term move to YAML config, but not urgent
- **Deployment**: Simplify scripts while respecting different schedules per hydromet
- **APIs**: Can use real APIs with real data for testing

---

## Current Workflow Summary

### Architecture Overview
The SAPPHIRE Forecast Tools uses a **containerized pipeline architecture** orchestrated by Luigi:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Shell Scripts (Entry Points)                  │
│  run_pentadal_forecasts.sh | run_decadal_forecasts.sh | etc.    │
└─────────────────────────────────────┬───────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────┐
│              Luigi Daemon (Port <port>) - Task Scheduler         │
│              docker-compose-luigi.yml                            │
└─────────────────────────────────────┬───────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────┐
│                    Pipeline Container                            │
│  apps/pipeline/pipeline_docker.py - Luigi Task Definitions       │
└─────────────┬───────────────────────┬───────────────────────────┘
              │                       │
    ┌─────────▼─────────┐   ┌────────▼────────┐
    │ Preprocessing     │   │ Forecast Models │
    │ - preprunoff      │   │ - linreg        │
    │ - prepgateway     │   │ - ml (TFT,TIDE) │
    │                   │   │ - conceptmod    │
    └───────────────────┘   └────────┬────────┘
                                     │
                           ┌─────────▼─────────┐
                           │ Post-Processing   │
                           │ - skill metrics   │
                           │ - combined output │
                           └───────────────────┘
```

### Key Scripts
| Script | Purpose | When Run |
|--------|---------|----------|
| `run_pentadal_forecasts.sh` | 5-day forecasts | Daily ~09:30 UTC |
| `run_decadal_forecasts.sh` | 10-day forecasts | After pentadal |
| `daily_ml_maintenance.sh` | Retrain ML models | ~20:00 UTC |
| `daily_update_sapphire_frontend.sh` | Update dashboards | ~23:00 UTC |

### Configuration Loading
Current state: **~100+ environment variables** in `.env` file including:
- Credentials (iEasyHydro, SMTP, API keys)
- File paths (relative paths like `../../../kyg_data_forecast_tools/...`)
- Model configuration
- Feature flags

**How configuration is consumed:**
- `apps/pipeline/src/environment.py` loads from `.env` using `python-dotenv`
- Shell scripts use `bin/utils/common_functions.sh` → `read_configuration()`
- Each Docker container receives env vars via `-e` flags or compose env_file

---

## Local Testing: Current Challenges

1. **Path complexity**: Relative paths (`../../../`) assume specific directory structure
2. **Docker-in-Docker**: Pipeline spawns containers from within containers
3. **Luigi daemon requirement**: Need orchestrator running
4. **External dependencies**: iEasyHydro API, Data Gateway API, SMTP
5. **Data requirements**: Need runoff data, model files, intermediate data

---

## Recommendations for Local Testing

### Option A: Run Python directly (No Docker)
**Best for**: Debugging individual modules, unit tests

```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate
pip install -r apps/pipeline/requirements.txt

# Set env file path
export ieasyhydroforecast_env_file_path=/path/to/.env

# Run individual modules
python -m apps.iEasyHydroForecast.linreg  # Linear regression
```

**Pros**: Fast iteration, easy debugging, IDE integration
**Cons**: May miss Docker-specific issues

### Option B: Docker Compose for local development
**Best for**: Testing full pipeline locally

Create `docker-compose-local.yml`:
```yaml
services:
  luigi-daemon:
    # ... same as production

  pipeline-dev:
    build: ./apps/pipeline
    volumes:
      - ./apps:/app/apps  # Mount source for live reload
      - ./data:/data
    env_file: ./config/.env_local
    command: ["python", "-m", "pytest", "-v"]  # or interactive shell
```

### Option C: Makefile/script wrapper (Recommended)

A **Makefile** provides the best developer experience because:
- Self-documenting with `make help`
- Tab completion in most shells
- Familiar to most developers
- Can combine shell scripts and Docker commands seamlessly
- Works consistently across Mac and Linux

**Create `Makefile` in repo root:**

```makefile
# SAPPHIRE Forecast Tools - Development & Deployment Makefile
# Usage: make help

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Configuration (can be overridden: make ENV_FILE=path/to/.env run-linreg)
ENV_FILE ?= config/.env_local
DATA_DIR ?= data
MODE ?= PENTAD

# Derived paths
REPO_ROOT := $(shell pwd)
PYTHONPATH := $(REPO_ROOT)/apps/iEasyHydroForecast

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m  # No Color

##@ Setup
.PHONY: setup setup-venv setup-data setup-env

setup: setup-venv setup-data setup-env  ## Complete local development setup
	@echo -e "$(GREEN)✓ Setup complete! Run 'make help' for available commands$(NC)"

setup-venv:  ## Create Python virtual environment and install dependencies
	@echo -e "$(BLUE)Creating virtual environment...$(NC)"
	python3.11 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -e apps/iEasyHydroForecast
	./venv/bin/pip install -r apps/linear_regression/requirements.txt
	./venv/bin/pip install -r apps/preprocessing_runoff/requirements.txt
	./venv/bin/pip install -r apps/postprocessing_forecasts/requirements.txt
	./venv/bin/pip install -r apps/machine_learning/requirements.txt || true
	@echo -e "$(GREEN)✓ Virtual environment ready. Activate with: source venv/bin/activate$(NC)"

setup-data:  ## Set up local data directory with symlinks to Dropbox
	@echo -e "$(BLUE)Setting up data directory...$(NC)"
	@./bin/setup_local_data.sh
	@echo -e "$(GREEN)✓ Data directory ready at $(DATA_DIR)/$(NC)"

setup-env:  ## Create local .env file from template
	@if [ ! -f config/.env_local ]; then \
		cp config/.env_local_template config/.env_local; \
		echo -e "$(YELLOW)Created config/.env_local - please edit with your credentials$(NC)"; \
	else \
		echo -e "$(GREEN)✓ config/.env_local already exists$(NC)"; \
	fi

##@ Local Python Testing (No Docker)
.PHONY: run-linreg run-preprunoff run-prepgateway run-postproc run-ml

run-linreg:  ## Run linear regression module locally
	@echo -e "$(BLUE)Running linear regression ($(MODE))...$(NC)"
	@./bin/local_run.sh $(ENV_FILE) apps/linear_regression/linear_regression.py $(MODE)

run-preprunoff:  ## Run preprocessing runoff module locally
	@echo -e "$(BLUE)Running preprocessing runoff...$(NC)"
	@./bin/local_run.sh $(ENV_FILE) apps/preprocessing_runoff/preprocessing_runoff.py

run-prepgateway:  ## Run preprocessing gateway module locally
	@echo -e "$(BLUE)Running preprocessing gateway...$(NC)"
	@./bin/local_run.sh $(ENV_FILE) apps/preprocessing_gateway/Quantile_Mapping_OP.py

run-postproc:  ## Run post-processing module locally
	@echo -e "$(BLUE)Running post-processing ($(MODE))...$(NC)"
	@./bin/local_run.sh $(ENV_FILE) apps/postprocessing_forecasts/postprocessing_forecasts.py $(MODE)

run-ml:  ## Run ML forecast module locally (requires MODEL=TFT|TIDE|TSMIXER)
	@echo -e "$(BLUE)Running ML model $(MODEL) ($(MODE))...$(NC)"
	SAPPHIRE_MODEL_TO_USE=$(MODEL) ./bin/local_run.sh $(ENV_FILE) apps/machine_learning/make_forecast.py $(MODE)

##@ Local Docker Testing
.PHONY: docker-build docker-test-pentad docker-test-decad docker-shell

docker-build:  ## Build local Docker images
	@echo -e "$(BLUE)Building Docker images...$(NC)"
	docker compose -f bin/docker-compose-local.yml build

docker-test-pentad:  ## Run pentadal pipeline in Docker with local scheduler
	@echo -e "$(BLUE)Running pentadal pipeline in Docker...$(NC)"
	@./bin/local_docker_test.sh $(ENV_FILE) PENTAD

docker-test-decad:  ## Run decadal pipeline in Docker with local scheduler
	@echo -e "$(BLUE)Running decadal pipeline in Docker...$(NC)"
	@./bin/local_docker_test.sh $(ENV_FILE) DECAD

docker-shell:  ## Start interactive shell in pipeline container
	@echo -e "$(BLUE)Starting interactive shell...$(NC)"
	docker compose -f bin/docker-compose-local.yml run --rm pipeline-local /bin/bash

##@ Production Deployment
.PHONY: forecast-pentad forecast-decad maintenance dashboard-update

forecast-pentad:  ## Run pentadal forecasts (production)
	@./bin/run_pentadal_forecasts.sh $(ENV_FILE)

forecast-decad:  ## Run decadal forecasts (production)
	@./bin/run_decadal_forecasts.sh $(ENV_FILE)

maintenance:  ## Run ML maintenance (production)
	@./bin/daily_ml_maintenance.sh $(ENV_FILE)

dashboard-update:  ## Update dashboards (production)
	@./bin/daily_update_sapphire_frontend.sh $(ENV_FILE)

##@ Logs & Debugging
.PHONY: logs logs-follow logs-clean

logs:  ## Show recent pipeline logs
	@echo -e "$(BLUE)Recent logs:$(NC)"
	@tail -100 $(DATA_DIR)/logs/$$(ls -t $(DATA_DIR)/logs/ | head -1)/*.log 2>/dev/null || \
		docker compose -f bin/docker-compose-luigi.yml logs --tail=100

logs-follow:  ## Follow pipeline logs in real-time
	docker compose -f bin/docker-compose-luigi.yml logs -f

logs-clean:  ## Clean old log files (>15 days)
	find $(DATA_DIR)/logs -type f -mtime +15 -delete 2>/dev/null || true
	@echo -e "$(GREEN)✓ Old logs cleaned$(NC)"

##@ Utilities
.PHONY: clean clean-docker status help

clean:  ## Remove build artifacts and temp files
	rm -rf __pycache__ .pytest_cache .mypy_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

clean-docker:  ## Stop and remove all SAPPHIRE containers
	docker compose -f bin/docker-compose-luigi.yml down
	docker compose -f bin/docker-compose-dashboards.yml down
	docker container prune -f

status:  ## Show status of SAPPHIRE services
	@echo -e "$(BLUE)Docker containers:$(NC)"
	@docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo -e "$(BLUE)Luigi daemon:$(NC)"
	@curl -s http://localhost:<port>/ > /dev/null && echo "✓ Running at http://localhost:<port>" || echo "✗ Not running"

help:  ## Show this help message
	@echo -e "$(BLUE)SAPPHIRE Forecast Tools$(NC)"
	@echo ""
	@echo "Usage: make [target] [VAR=value]"
	@echo ""
	@echo "Variables:"
	@echo "  ENV_FILE  Path to .env file (default: config/.env_local)"
	@echo "  MODE      Prediction mode: PENTAD or DECAD (default: PENTAD)"
	@echo "  MODEL     ML model: TFT, TIDE, TSMIXER (for run-ml target)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf ""} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-18s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
```

**Usage Examples:**

```bash
# Initial setup (one-time)
make setup

# Quick local testing (no Docker)
make run-linreg                              # Run linear regression
make run-linreg MODE=DECAD                   # Run with decadal mode
make run-ml MODEL=TFT MODE=PENTAD            # Run specific ML model
make run-preprunoff                          # Run preprocessing

# Full pipeline in Docker (slower but more realistic)
make docker-test-pentad                      # Test pentadal pipeline
make docker-shell                            # Interactive debugging

# Production (unchanged from current workflow)
make forecast-pentad ENV_FILE=/path/to/.env
make maintenance

# Debugging
make status                                  # Check running services
make logs                                    # View recent logs
make logs-follow                             # Stream logs live

# Cleanup
make clean-docker                            # Stop all containers
```

---

## Simplification Priorities (Re-ordered based on user feedback)

### Priority 0: WORKING DEMO VERSION

**Status:** Will be implemented in SAPPHIRE v2

The working demo (Swiss stations, linear regression, automated updates) will be implemented in the SAPPHIRE v2 codebase.

See **[sapphire_v2_planning.md](sapphire_v2_planning.md)** for detailed milestones and requirements.

---

### Priority 1: LOCAL TESTING INFRASTRUCTURE (Highest impact on workflow)
**Problem**: Current testing cycle is painful: debug prints → push → GitHub Actions → test on server
**Goal**: Test locally on Mac before pushing, with fast iteration

#### 1A: Direct Python Module Testing (No Docker)
For quick iteration and debugging individual modules.

**Current module invocation pattern** (from Dockerfiles):
```bash
PYTHONPATH=/app/apps/iEasyHydroForecast python apps/linear_regression/linear_regression.py
```

**New local test script**: `bin/local_run.sh`
```bash
#!/bin/bash
# Run any SAPPHIRE module locally without Docker
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

ENV_FILE=${1:-"$REPO_ROOT/config/.env_local"}
SCRIPT=${2:-"apps/linear_regression/linear_regression.py"}
MODE=${3:-"PENTAD"}  # PENTAD or DECAD

# Export environment
export ieasyhydroforecast_env_file_path="$ENV_FILE"
export SAPPHIRE_PREDICTION_MODE="$MODE"
export PYTHONPATH="$REPO_ROOT/apps/iEasyHydroForecast:$PYTHONPATH"

# Source .env file to export all variables
set -a
source "$ENV_FILE"
set +a

cd "$REPO_ROOT"
python "$SCRIPT"
```

**Usage examples**:
```bash
# Linear regression (pentadal)
./bin/local_run.sh config/.env_local apps/linear_regression/linear_regression.py PENTAD

# Preprocessing runoff
./bin/local_run.sh config/.env_local apps/preprocessing_runoff/preprocessing_runoff.py

# Post-processing
./bin/local_run.sh config/.env_local apps/postprocessing_forecasts/postprocessing_forecasts.py PENTAD

# ML forecast
./bin/local_run.sh config/.env_local apps/machine_learning/make_forecast.py PENTAD
```

**Requirements**:
- Python 3.11 virtual environment with all dependencies
- Local `.env` file with paths adjusted for Mac (no `../../../`)
- Access to data files (symlinked or copied locally)
- iEasyHydroForecast installed: `pip install -e apps/iEasyHydroForecast`

#### 1B: Local Docker Pipeline (Full integration test)
Run the containerized pipeline locally without Luigi daemon overhead:

```bash
# Create bin/local_docker_test.sh
#!/bin/bash
# Run pipeline container locally with local scheduler

ENV_FILE=${1:-"config/.env_local"}
MODE=${2:-"PENTAD"}  # PENTAD, DECAD, or ALL

docker compose -f bin/docker-compose-local.yml run --rm \
  -e SAPPHIRE_PREDICTION_MODE=$MODE \
  -e ieasyhydroforecast_env_file_path=/app/config/.env \
  pipeline-local \
  luigi --local-scheduler --module apps.pipeline.pipeline_docker Run${MODE}Workflow
```

**New file**: `bin/docker-compose-local.yml`
```yaml
services:
  pipeline-local:
    build:
      context: .
      dockerfile: apps/pipeline/Dockerfile
    volumes:
      - ./apps:/app/apps:ro           # Source code (read-only for safety)
      - ./config:/app/config:ro       # Config files
      - ./data:/app/data              # Local data directory
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - SAPPHIRE_LOCAL_TEST=true
    working_dir: /app
```

#### 1C: Local Data Setup

**Option A: Symlink to Dropbox data** (easiest)
```bash
# Create data directory that mirrors server structure
mkdir -p data
ln -s "/Users/bea/hydrosolutions Dropbox/.../kyg_data_forecast_tools/config" data/config
ln -s "/Users/bea/hydrosolutions Dropbox/.../kyg_data_forecast_tools/intermediate_data" data/intermediate_data
ln -s "/Users/bea/hydrosolutions Dropbox/.../kyg_data_forecast_tools/daily_runoff" data/daily_runoff
# etc.
```

**Option B: rsync subset of data** (for offline work)
```bash
# Sync only required data
rsync -av --include='*.csv' --include='*.json' --exclude='*' \
  "/Users/bea/hydrosolutions Dropbox/.../kyg_data_forecast_tools/" data/
```

#### 1D: Local `.env` template for Mac
Create `config/.env_local_template`:
```bash
# Paths adjusted for local Mac development
# Copy to .env_local and customize

ieasyhydroforecast_organization=kghm

# Use paths relative to repo root (no ../../../)
ieasyforecast_configuration_path=./data/config
ieasyforecast_intermediate_data_path=./data/intermediate_data
ieasyforecast_daily_discharge_path=./data/daily_runoff
ieasyreports_templates_directory_path=./data/templates
ieasyreports_report_output_path=./data/reports
ieasyforecast_gis_directory_path=./data/GIS
ieasyhydroforecast_models_and_scalers_path=./data/config/models_and_scalers

# API endpoints (same as production - can use real APIs)
IEASYHYDRO_HOST=http://host.docker.internal:8881
IEASYHYDROHF_HOST='http://localhost:5555/api/v1/'
SAPPHIRE_DG_HOST=https://data-gateway.ieasyhydro.org/

# Credentials (copy from production .env)
IEASYHYDRO_USERNAME=...
IEASYHYDRO_PASSWORD=...
# etc.
```

---

### Priority 2: UNIFIED ENTRY POINT SCRIPT
**Problem**: 4 scripts with duplicated logic, different schedules per hydromet
**Solution**: Single script with subcommands, schedule config separate

```bash
# Create bin/sapphire.sh (unified entry point)

./bin/sapphire.sh forecast pentad     # Run pentadal forecasts
./bin/sapphire.sh forecast decad      # Run decadal forecasts
./bin/sapphire.sh forecast all        # Run both
./bin/sapphire.sh maintenance         # ML model retraining
./bin/sapphire.sh dashboard update    # Update dashboards
./bin/sapphire.sh test local          # Local Python test
./bin/sapphire.sh test docker         # Local Docker test
```

**Schedule handling**: Keep schedules in crontab/systemd, just call different subcommands:
```cron
# KGHM schedule
30 9  * * * /path/to/sapphire.sh forecast pentad
0  11 * * * /path/to/sapphire.sh forecast decad
0  20 * * * /path/to/sapphire.sh maintenance
0  23 * * * /path/to/sapphire.sh dashboard update

# Other hydromet - different times
0  8  * * * /path/to/sapphire.sh forecast pentad
0  9  * * * /path/to/sapphire.sh forecast decad
```

**Backward compatibility**: Keep old scripts as thin wrappers:
```bash
# run_pentadal_forecasts.sh (deprecated, calls new script)
#!/bin/bash
exec "$(dirname "$0")/sapphire.sh" forecast pentad "$@"
```

---

### Priority 3: IMPROVED LOGGING AND DEBUGGING
**Problem**: Reading Docker logs is tedious, debug prints require rebuild
**Solution**: Better log access and optional verbose mode

```bash
# Add to sapphire.sh
./bin/sapphire.sh forecast pentad --verbose    # Extra logging
./bin/sapphire.sh forecast pentad --dry-run    # Validate config only
./bin/sapphire.sh logs pentad                  # Tail recent logs
./bin/sapphire.sh logs pentad --follow         # Live log stream
```

**Implementation**:
- Store logs in consistent location: `data/logs/{date}/{service}.log`
- Add `--verbose` flag that sets `LOG_LEVEL=DEBUG`
- Add `logs` subcommand to quickly access recent logs

---

### Priority 4: CONSOLIDATE DOCKER COMPOSE (Medium priority)
Merge `docker-compose-luigi.yml` and `docker-compose-dashboards.yml` using profiles:

```yaml
# bin/docker-compose.yml (unified)
services:
  luigi-daemon:
    profiles: [pipeline, full]
    # ...
  pentadal:
    profiles: [pipeline, full]
    # ...
  decadal:
    profiles: [pipeline, full]
    # ...
  pentaddashboard:
    profiles: [dashboard, full]
    # ...
  decaddashboard:
    profiles: [dashboard, full]
    # ...
```

---

### Priority 5: CONFIGURATION REFACTOR (Future - not urgent)
Long-term: Move to YAML config with layered structure. Deferred per user request.

---

## Implementation Plan (Makefile-Focused)

### Phase 1: Core Infrastructure - RECOMMENDED START

**Files to create:**

| File | Purpose |
|------|---------|
| `Makefile` | Main entry point with all targets |
| `bin/local_run.sh` | Direct Python execution wrapper |
| `bin/local_docker_test.sh` | Docker with local scheduler |
| `bin/setup_local_data.sh` | Symlink/copy data setup |
| `bin/docker-compose-local.yml` | Simplified compose for local dev |
| `config/.env_local_template` | Mac-friendly paths template |

**Files to modify:** None (additive only)

**Step-by-step:**

1. **Create `Makefile`** (see Option C above for full content)

2. **Create `bin/local_run.sh`:**
```bash
#!/bin/bash
# Run any SAPPHIRE module locally without Docker
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

ENV_FILE=${1:-"$REPO_ROOT/config/.env_local"}
SCRIPT=${2:-"apps/linear_regression/linear_regression.py"}
MODE=${3:-"PENTAD"}

# Validate inputs
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: ENV_FILE not found: $ENV_FILE"
    echo "Run 'make setup-env' to create one from template"
    exit 1
fi

# Export environment
export ieasyhydroforecast_env_file_path="$ENV_FILE"
export SAPPHIRE_PREDICTION_MODE="$MODE"
export PYTHONPATH="$REPO_ROOT/apps/iEasyHydroForecast:$PYTHONPATH"

# Source .env file to export all variables
set -a
source "$ENV_FILE"
set +a

cd "$REPO_ROOT"
echo "Running: python $SCRIPT"
echo "Mode: $MODE"
echo "Env: $ENV_FILE"
echo "---"
python "$SCRIPT"
```

3. **Create `bin/setup_local_data.sh`:**
```bash
#!/bin/bash
# Set up local data directory with symlinks to Dropbox data
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${1:-$REPO_ROOT/data}"

# Default Dropbox path (customize as needed)
DROPBOX_BASE="${DROPBOX_BASE:-$HOME/hydrosolutions Dropbox}"
KYG_DATA="$DROPBOX_BASE/Bea martibeatrice@gmail.com/SAPPHIRE_Central_Asia_Technical_Work/data/kyg_data_forecast_tools"

echo "Setting up data directory at: $DATA_DIR"
mkdir -p "$DATA_DIR"

# Create symlinks
create_link() {
    local source="$1"
    local target="$2"
    if [ -e "$source" ]; then
        if [ -L "$target" ]; then
            rm "$target"
        fi
        ln -s "$source" "$target"
        echo "✓ Linked: $target -> $source"
    else
        echo "⚠ Source not found: $source"
    fi
}

create_link "$KYG_DATA/config" "$DATA_DIR/config"
create_link "$KYG_DATA/intermediate_data" "$DATA_DIR/intermediate_data"
create_link "$KYG_DATA/daily_runoff" "$DATA_DIR/daily_runoff"
create_link "$KYG_DATA/templates" "$DATA_DIR/templates"
create_link "$KYG_DATA/reports" "$DATA_DIR/reports"
create_link "$KYG_DATA/GIS" "$DATA_DIR/GIS"
create_link "$KYG_DATA/conceptual_model" "$DATA_DIR/conceptual_model"

echo ""
echo "Data directory setup complete!"
echo "You can customize DROPBOX_BASE if your Dropbox is in a different location."
```

4. **Create `bin/local_docker_test.sh`:**
```bash
#!/bin/bash
# Run pipeline container locally with local scheduler
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

ENV_FILE=${1:-"$REPO_ROOT/config/.env_local"}
MODE=${2:-"PENTAD"}

echo "Running ${MODE} pipeline in Docker with local scheduler..."

docker compose -f "$REPO_ROOT/bin/docker-compose-local.yml" run --rm \
  -e SAPPHIRE_PREDICTION_MODE="$MODE" \
  -e ieasyhydroforecast_env_file_path=/app/config/.env_local \
  pipeline-local \
  luigi --local-scheduler --module apps.pipeline.pipeline_docker "Run${MODE}Workflow"
```

5. **Create `bin/docker-compose-local.yml`:**
```yaml
# Docker Compose for local development/testing
# Uses local scheduler instead of Luigi daemon

services:
  pipeline-local:
    build:
      context: ..
      dockerfile: apps/pipeline/Dockerfile
    volumes:
      # Mount source code for live editing (read-only for safety)
      - ../apps:/app/apps:ro
      # Mount config (read-only)
      - ../config:/app/config:ro
      # Mount data directory (read-write for outputs)
      - ../data:/app/data
      # Docker socket for container-in-container
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - SAPPHIRE_LOCAL_TEST=true
      - PYTHONPATH=/app/apps/iEasyHydroForecast
    working_dir: /app
    network_mode: host  # Use host network for API access
```

6. **Create `config/.env_local_template`:**
```bash
# Local development .env template for Mac
# Copy to .env_local and customize with your credentials
# Usage: cp config/.env_local_template config/.env_local

ieasyhydroforecast_organization=kghm

# =============================================================================
# FILE PATHS (relative to repo root - no ../../../)
# =============================================================================
ieasyforecast_configuration_path=./data/config
ieasyforecast_intermediate_data_path=./data/intermediate_data
ieasyforecast_daily_discharge_path=./data/daily_runoff
ieasyreports_templates_directory_path=./data/templates
ieasyreports_report_output_path=./data/reports
ieasyforecast_gis_directory_path=./data/GIS
ieasyhydroforecast_models_and_scalers_path=./data/config/models_and_scalers
ieasyhydroforecast_conceptual_model_path=./data/conceptual_model

# =============================================================================
# API ENDPOINTS
# =============================================================================
# For local testing, use real APIs
IEASYHYDRO_HOST=http://host.docker.internal:8881
IEASYHYDROHF_HOST='http://localhost:5555/api/v1/'
SAPPHIRE_DG_HOST=https://data-gateway.ieasyhydro.org/

# =============================================================================
# CREDENTIALS (copy from production .env)
# =============================================================================
IEASYHYDRO_USERNAME=your_username
IEASYHYDRO_PASSWORD=your_password
ORGANIZATION_ID=1

IEASYHYDROHF_USERNAME=your_hf_username
IEASYHYDROHF_PASSWORD=your_hf_password

ieasyhydroforecast_API_KEY_GATEAWAY=your_api_key

# =============================================================================
# EMAIL (optional for local testing)
# =============================================================================
SAPPHIRE_PIPELINE_SMTP_SERVER=smtp.gmail.com
SAPPHIRE_PIPELINE_SMTP_PORT=587
SAPPHIRE_PIPELINE_SMTP_USERNAME=your_email
SAPPHIRE_PIPELINE_SMTP_PASSWORD=your_app_password
SAPPHIRE_PIPELINE_SENDER_EMAIL=your_email
SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=your_email

# =============================================================================
# MODEL CONFIGURATION
# =============================================================================
ieasyhydroforecast_run_ML_models=True
ieasyhydroforecast_available_ML_models=TFT,TIDE,TSMIXER
ieasyhydroforecast_run_CM_models=False

# =============================================================================
# OTHER SETTINGS (copy remaining from production .env as needed)
# =============================================================================
ieasyhydroforecast_ssh_to_iEH=False
ieasyhydroforecast_backend_docker_image_tag=local
ieasyhydroforecast_frontend_docker_image_tag=local
```

### Phase 2: Documentation

**Create `docs/LOCAL_DEVELOPMENT.md`:**

```markdown
# Local Development Guide

## Quick Start

```bash
# 1. Clone the repo
git clone <repo-url>
cd SAPPHIRE_forecast_tools

# 2. Run setup (creates venv, symlinks data, creates .env)
make setup

# 3. Edit credentials
vi config/.env_local

# 4. Activate virtual environment
source venv/bin/activate

# 5. Test a module
make run-linreg
```

## Available Commands

Run `make help` to see all available commands:

- **Setup**: `make setup`, `make setup-venv`, `make setup-data`, `make setup-env`
- **Local Python**: `make run-linreg`, `make run-preprunoff`, `make run-postproc`
- **Docker Testing**: `make docker-test-pentad`, `make docker-shell`
- **Debugging**: `make logs`, `make logs-follow`, `make status`

## Testing Individual Modules

```bash
# Linear regression
make run-linreg MODE=PENTAD

# Preprocessing
make run-preprunoff

# ML models
make run-ml MODEL=TFT MODE=PENTAD

# Post-processing
make run-postproc MODE=PENTAD
```

## Troubleshooting

### "ENV_FILE not found"
Run `make setup-env` to create config/.env_local from template.

### API connection errors
Check that you've copied credentials from production .env to config/.env_local.

### Missing data files
Run `make setup-data` and verify Dropbox path in bin/setup_local_data.sh.
```

### Phase 3: Integration with Existing Scripts (Optional)

If you want `make` to also work for production deployment, the existing shell scripts can remain unchanged. The Makefile simply calls them:

```makefile
forecast-pentad:
    @./bin/run_pentadal_forecasts.sh $(ENV_FILE)
```

This provides a unified interface (`make forecast-pentad`) while keeping backward compatibility with direct script usage.

---

## Pending Integration Tasks

### Linear Regression Maintenance Script

**Status**: Created, needs Docker testing and integration with deployment

A nightly maintenance script for linear regression hindcast has been created:
- **Script**: `bin/daily_linreg_maintenance.sh`
- **Purpose**: Catch up on missed forecasts by running hindcast mode nightly
- **Planning doc**: `implementation_planning/linear_regression_bugfix_plan.md` (Part 4)

**Remaining tasks**:
1. Test maintenance script with Docker container
2. Verify end-to-end workflow
3. Add to cron schedule (after `daily_ml_maintenance.sh`)
4. Add Makefile target: `make linreg-maintenance`

**Suggested cron entry**:
```cron
# Run after ML maintenance completes
30 21 * * * /path/to/bin/daily_linreg_maintenance.sh /path/to/.env
```

---

## Critical Files Reference

| Purpose | File Path |
|---------|-----------|
| Pipeline orchestration | `apps/pipeline/pipeline_docker.py` |
| Environment loading | `apps/pipeline/src/environment.py` |
| Common shell functions | `bin/utils/common_functions.sh` |
| Luigi compose | `bin/docker-compose-luigi.yml` |
| Dashboard compose | `bin/docker-compose-dashboards.yml` |
| Pipeline Dockerfile | `apps/pipeline/Dockerfile` |
| Linear regression maintenance | `bin/daily_linreg_maintenance.sh` |
| Current .env example | `config/.env_develop_kghm` |
| Demo config | `apps/config/.env_develop` |
| Demo station library | `apps/config/config_all_stations_library.json` |
| Demo runoff data | `data/daily_runoff/*.xlsx` |

## External Dependencies

| Purpose | Repository | Notes |
|---------|------------|-------|
| Operational data scraping | [hydro_data_scraper](https://github.com/hydrosolutions/hydro_data_scraper) | Fetches runoff data from BAFU (Swiss) and other sources |
| iEasyHydro SDK | [ieasyhydro-python-sdk](https://github.com/hydrosolutions/ieasyhydro-python-sdk) | For iEasyHydro HF integration |
| Data Gateway client | sapphire-dg-client (private) | For ECMWF/ERA5 data access |

---

*Last updated: 2026-01-07 - Added Priority 0 (Working Demo) with hydro_data_scraper integration*
