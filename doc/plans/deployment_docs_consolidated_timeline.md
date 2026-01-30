# Deployment Documentation: Consolidated Priority & Timeline

## Executive Summary

This document consolidates 6 deployment improvement strategies into a single prioritized implementation timeline. Items are organized by impact-to-effort ratio, with dependencies identified.

**Guiding Principles:**
1. Fix existing documentation before creating new documentation
2. Scripts that automate common pain points have higher priority than documentation
3. Infrastructure (Makefile) enables faster iteration on all other items
4. Defer "nice-to-have" items that don't address immediate operational needs

---

## Phase 1: Week 1 - Immediate Value (Quick Wins)

**Goal:** Address highest-impact items with minimal effort. Focus on fixing broken/incomplete content and enabling faster workflows.

### Deliverables

| Item | Type | Effort | Rationale |
|------|------|--------|-----------|
| Fix TODOs in `doc/configuration.md` | FIX | 1-2h | Incomplete documentation causes user confusion |
| Fill `doc/deployment.md` Quick Start placeholder | FIX | 1h | Line 84 says "TODO: Detailed instructions" |
| Create `bin/utils/health_check.sh` | SCRIPT | 2h | Most requested operational need |
| Create `bin/utils/data_freshness.sh` | SCRIPT | 2h | Diagnoses common "stale data" issues |

### Dependencies
- None - these are independent tasks

### Success Criteria
- [ ] `doc/configuration.md` has no TODO markers
- [ ] `doc/deployment.md` has complete demo deployment instructions
- [ ] `health_check.sh` returns pass/fail status for: Luigi daemon, dashboards, Docker containers
- [ ] `data_freshness.sh` reports age of key data files (runoff_day.csv, last_successful_run.txt)

### Detailed Specifications

#### Fix TODOs in configuration.md

**Current TODOs (lines ~5, ~140, ~161, ~195):**

1. **Line 5-6**: "TODO: UPDATE FIGURE" - Either update the figure or remove the reference
2. **Line ~140**: Missing link to data gateway chapter
3. **Line ~161**: Document what each output folder stores:
   - `ieasyhydroforecast_OUTPUT_PATH_DG`: Raw downloads from data gateway
   - `ieasyhydroforecast_OUTPUT_PATH_CM`: Control member forcing (bias-corrected)
   - `ieasyhydroforecast_OUTPUT_PATH_ENS`: Ensemble forcing data
4. **Line ~195**: Complete machine learning configuration section

#### bin/utils/health_check.sh

```bash
#!/bin/bash
# Quick health check for SAPPHIRE deployment
# Usage: ./bin/utils/health_check.sh [--verbose]

VERBOSE=${1:-""}

echo "=== SAPPHIRE Health Check ==="
echo "Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

FAILURES=0

# Check Luigi daemon
echo -n "Luigi Daemon (port 8082)... "
if curl -sf http://localhost:8082/ > /dev/null 2>&1; then
    echo "OK"
else
    echo "FAIL"
    ((FAILURES++))
fi

# Check Pentad Dashboard
echo -n "Pentad Dashboard (port 5006)... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard 2>/dev/null)
if [ "$HTTP_CODE" = "200" ]; then
    echo "OK"
else
    echo "FAIL (HTTP $HTTP_CODE)"
    ((FAILURES++))
fi

# Check Decad Dashboard
echo -n "Decad Dashboard (port 5007)... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5007/forecast_dashboard 2>/dev/null)
if [ "$HTTP_CODE" = "200" ]; then
    echo "OK"
else
    echo "FAIL (HTTP $HTTP_CODE)"
    ((FAILURES++))
fi

# Check container health
echo -n "Container Health... "
UNHEALTHY=$(docker ps --filter "health=unhealthy" --filter "name=sapphire" --format "{{.Names}}" 2>/dev/null)
if [ -z "$UNHEALTHY" ]; then
    echo "OK"
else
    echo "FAIL: $UNHEALTHY"
    ((FAILURES++))
fi

echo ""
if [ $FAILURES -eq 0 ]; then
    echo "Status: ALL CHECKS PASSED"
    exit 0
else
    echo "Status: $FAILURES CHECK(S) FAILED"
    exit 1
fi
```

#### bin/utils/data_freshness.sh

```bash
#!/bin/bash
# Check freshness of key SAPPHIRE data files
# Usage: ./bin/utils/data_freshness.sh /path/to/.env

source "${1:-/data/kyg_data_forecast_tools/config/.env_develop_kghm}"

DATA_PATH="${ieasyforecast_intermediate_data_path:-/data/kyg_data_forecast_tools/intermediate_data}"

echo "=== Data Freshness Report ==="
echo "Checking: $DATA_PATH"
echo ""

check_file() {
    local file="$1"
    local max_age_hours="$2"
    local label="$3"

    if [ -f "$file" ]; then
        age_seconds=$(($(date +%s) - $(stat -c %Y "$file" 2>/dev/null || stat -f %m "$file")))
        age_hours=$((age_seconds / 3600))
        mod_date=$(date -r "$file" '+%Y-%m-%d %H:%M' 2>/dev/null || stat -c %y "$file" | cut -d' ' -f1,2)

        if [ $age_hours -gt $max_age_hours ]; then
            echo "WARNING: $label - ${age_hours}h old (last: $mod_date)"
        else
            echo "OK: $label - ${age_hours}h old (last: $mod_date)"
        fi
    else
        echo "MISSING: $label - file not found"
    fi
}

check_file "$DATA_PATH/runoff_day.csv" 30 "Runoff data"
check_file "$DATA_PATH/last_successful_run.txt" 30 "Last run timestamp"
check_file "$DATA_PATH/hydrograph_day.pkl" 30 "Daily hydrograph"
check_file "$DATA_PATH/offline_forecasts_pentad.csv" 30 "Pentad forecasts"
check_file "$DATA_PATH/offline_forecasts_decad.csv" 30 "Decad forecasts"

echo ""
echo "Note: Files older than 30 hours flagged as WARNING"
```

---

## Phase 2: Month 1 - Core Infrastructure

**Goal:** Establish the foundation that enables faster iteration on documentation and operations.

### Deliverables

| Item | Type | Effort | Rationale |
|------|------|--------|-----------|
| Root `Makefile` | TOOLING | 4h | Unified entry point for all operations |
| `bin/utils/pipeline_status.sh` | SCRIPT | 2h | Check Luigi task states |
| `bin/update/rollback.sh` | SCRIPT | 3h | Critical for safe deployments |
| `GETTING_STARTED.md` | DOC | 4h | Most impactful doc for new users |
| `doc/deployment/` restructure | DOC | 3h | Separate concerns: setup vs. update vs. troubleshooting |

### Dependencies
- Phase 1 completion (fixes provide stable base for new docs)
- Makefile depends on existing scripts (calls them, doesn't replace)

### Success Criteria
- [ ] `make help` shows all available commands with descriptions
- [ ] `make health-check` runs health_check.sh
- [ ] `make forecast-pentad ENV_FILE=...` runs pentadal forecasts
- [ ] `rollback.sh` can restore previous Docker image version
- [ ] New user can go from zero to running demo in 30 minutes using GETTING_STARTED.md

### Detailed Specifications

#### Root Makefile

```makefile
# SAPPHIRE Forecast Tools - Makefile
# Usage: make help
#
# This Makefile provides a unified interface for common operations.
# It wraps existing shell scripts without replacing them.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Configuration (can be overridden: make ENV_FILE=/path/to/.env forecast-pentad)
ENV_FILE ?= /data/kyg_data_forecast_tools/config/.env_develop_kghm
REPO_ROOT := $(shell pwd)

# Colors for terminal output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

##@ Health & Status

.PHONY: health-check status data-freshness pipeline-status

health-check: ## Run deployment health check
	@bash bin/utils/health_check.sh

status: ## Show status of all SAPPHIRE services
	@echo -e "$(BLUE)=== Docker Containers ===$(NC)"
	@docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@bash bin/utils/health_check.sh

data-freshness: ## Check age of key data files
	@bash bin/utils/data_freshness.sh $(ENV_FILE)

pipeline-status: ## Show Luigi task status
	@bash bin/utils/pipeline_status.sh

##@ Forecasting

.PHONY: forecast-pentad forecast-decad forecast-gateway

forecast-pentad: ## Run pentadal forecast pipeline
	@echo -e "$(BLUE)Running pentadal forecasts...$(NC)"
	@cd $(REPO_ROOT) && bash bin/run_pentadal_forecasts.sh $(ENV_FILE)

forecast-decad: ## Run decadal forecast pipeline
	@echo -e "$(BLUE)Running decadal forecasts...$(NC)"
	@cd $(REPO_ROOT) && bash bin/run_decadal_forecasts.sh $(ENV_FILE)

forecast-gateway: ## Run gateway preprocessing only
	@echo -e "$(BLUE)Running gateway preprocessing...$(NC)"
	@cd $(REPO_ROOT) && bash bin/run_preprocessing_gateway.sh $(ENV_FILE)

##@ Maintenance

.PHONY: maintenance-ml maintenance-linreg maintenance-preprunoff dashboard-update

maintenance-ml: ## Run ML model maintenance (hindcast catch-up)
	@cd $(REPO_ROOT) && bash bin/daily_ml_maintenance.sh $(ENV_FILE)

maintenance-linreg: ## Run linear regression maintenance
	@cd $(REPO_ROOT) && bash bin/daily_linreg_maintenance.sh $(ENV_FILE)

maintenance-preprunoff: ## Run preprocessing runoff maintenance
	@cd $(REPO_ROOT) && bash bin/daily_preprunoff_maintenance.sh $(ENV_FILE)

dashboard-update: ## Update dashboards
	@cd $(REPO_ROOT) && bash bin/daily_update_sapphire_frontend.sh $(ENV_FILE)

##@ Services

.PHONY: start-luigi start-dashboards stop-all restart-all

start-luigi: ## Start Luigi daemon
	@echo -e "$(BLUE)Starting Luigi daemon...$(NC)"
	@docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
	@until curl -fsS http://localhost:8082/ >/dev/null 2>&1; do echo "Waiting for Luigi..."; sleep 2; done
	@echo -e "$(GREEN)Luigi daemon ready$(NC)"

start-dashboards: ## Start forecast dashboards
	@echo -e "$(BLUE)Starting dashboards...$(NC)"
	@docker compose -f bin/docker-compose-dashboards.yml --env-file $(ENV_FILE) up -d
	@echo -e "$(GREEN)Dashboards starting on ports 5006, 5007$(NC)"

stop-all: ## Stop all SAPPHIRE services
	@echo -e "$(YELLOW)Stopping all SAPPHIRE services...$(NC)"
	@docker compose -f bin/docker-compose-dashboards.yml down 2>/dev/null || true
	@docker compose -f bin/docker-compose-luigi.yml down 2>/dev/null || true
	@echo -e "$(GREEN)All services stopped$(NC)"

restart-all: stop-all start-luigi start-dashboards ## Restart all services
	@echo -e "$(GREEN)All services restarted$(NC)"

##@ Deployment

.PHONY: pull-images deploy rollback

pull-images: ## Pull latest Docker images
	@bash bin/utils/pull_docker_images.sh $(ENV_FILE)

deploy: ## Full deployment (pull images, restart services)
	@echo -e "$(BLUE)Starting deployment...$(NC)"
	@$(MAKE) stop-all
	@$(MAKE) pull-images
	@$(MAKE) start-luigi
	@$(MAKE) start-dashboards
	@$(MAKE) health-check
	@echo -e "$(GREEN)Deployment complete$(NC)"

rollback: ## Rollback to previous Docker image version
	@bash bin/update/rollback.sh $(ENV_FILE)

##@ Logs & Debugging

.PHONY: logs logs-pentad logs-decad logs-clean

logs: ## Show recent logs from all sources
	@echo -e "$(BLUE)=== Recent Pipeline Logs ===$(NC)"
	@ls -lt /home/ubuntu/logs/sapphire_*.log 2>/dev/null | head -5 || echo "No log files found"
	@echo ""
	@tail -50 /home/ubuntu/logs/sapphire_pentadal_$$(date +%Y%m%d).log 2>/dev/null || echo "No pentadal log for today"

logs-pentad: ## Follow pentadal forecast logs
	@tail -f /home/ubuntu/logs/sapphire_pentadal_$$(date +%Y%m%d).log

logs-decad: ## Follow decadal forecast logs
	@tail -f /home/ubuntu/logs/sapphire_decadal_$$(date +%Y%m%d).log

logs-clean: ## Remove logs older than 7 days
	@find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete 2>/dev/null || true
	@echo -e "$(GREEN)Old logs cleaned$(NC)"

##@ Docker Maintenance

.PHONY: docker-prune docker-images

docker-prune: ## Remove unused Docker resources
	@docker system prune -f
	@echo -e "$(GREEN)Docker cleanup complete$(NC)"

docker-images: ## List SAPPHIRE Docker images
	@docker images | grep -E "sapphire|mabesa" | sort

##@ Help

.PHONY: help

help: ## Show this help message
	@echo -e "$(BLUE)SAPPHIRE Forecast Tools$(NC)"
	@echo ""
	@echo "Usage: make [target] [VAR=value]"
	@echo ""
	@echo "Variables:"
	@echo "  ENV_FILE   Path to .env file (default: production path)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf ""} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
```

#### doc/deployment/ Restructure

Current state: All deployment info in single `doc/deployment.md` (250+ lines)

Proposed structure:
```
doc/deployment/
  README.md              # Overview with links to other docs
  initial_setup.md       # First-time server setup
  update_checklist.md    # (move from doc/prod/)
  troubleshooting.md     # Common issues and solutions
  cron_schedule.md       # Crontab configuration
```

This is a MOVE/SPLIT operation, not net new content.

#### GETTING_STARTED.md

Location: Repository root (alongside README.md)

Content outline:
1. Prerequisites (5 min)
2. Clone repository (1 min)
3. Pull Docker images (5 min)
4. Copy example .env (2 min)
5. Start Luigi daemon (1 min)
6. Start dashboards (1 min)
7. Run first forecast (5 min)
8. Verify outputs (5 min)
9. Next steps (links to full docs)

Target: Complete demo running in 30 minutes.

---

## Phase 3: Quarter 1 - Complete Implementation

**Goal:** Fill remaining gaps and create a comprehensive documentation suite.

### Deliverables

| Item | Type | Effort | Rationale |
|------|------|--------|-----------|
| `quick-reference.md` | DOC | 2h | One-page cheat sheet for operators |
| `bin/utils/pre_update.sh` | SCRIPT | 2h | Pre-flight checks before updates |
| `bin/utils/env_diff.sh` | SCRIPT | 2h | Compare local vs server .env |
| `bin/update/update_all.sh` | SCRIPT | 3h | Automated full update procedure |
| `doc/concepts/` directory | DOC | 4h | Stable conceptual documentation |
| `doc/migrations/` directory | DOC | 2h | Version migration guides |
| `doc/architecture/decisions/` | DOC | 4h | Architecture Decision Records (ADRs) |
| `doc/troubleshooting/` guides | DOC | 6h | Issue-specific troubleshooting |
| `doc/runbooks/` | DOC | 4h | Step-by-step operational procedures |

### Dependencies
- Phase 2 Makefile (runbooks can reference make targets)
- Phase 2 GETTING_STARTED.md (quick-reference.md links to it)

### Success Criteria
- [ ] Operator can diagnose common issues using troubleshooting guides
- [ ] `pre_update.sh` catches 90% of pre-update issues (SSH access, disk space, cron timing)
- [ ] `update_all.sh` automates the full update checklist
- [ ] At least 3 ADRs documenting key architectural decisions

### Detailed Specifications

#### quick-reference.md

One-page reference card:
```markdown
# SAPPHIRE Quick Reference

## Essential Commands
| Action | Command |
|--------|---------|
| Health check | `make health-check` |
| Run pentadal | `make forecast-pentad` |
| Run decadal | `make forecast-decad` |
| View logs | `make logs` |
| Restart all | `make restart-all` |

## Key Ports
| Service | Port |
|---------|------|
| Luigi | 8082 |
| Pentad Dashboard | 5006 |
| Decad Dashboard | 5007 |

## Key Files
| File | Purpose |
|------|---------|
| `.env_develop_kghm` | Main configuration |
| `last_successful_run.txt` | Last forecast timestamp |
| `runoff_day.csv` | Daily discharge data |

## Cron Schedule (UTC)
| Time | Task |
|------|------|
| 03:00 | Gateway preprocessing |
| 04:00 | Pentadal forecasts |
| 05:00 | Decadal forecasts |
| 19:02 | Dashboard update |
| 20:00 | ML maintenance |
```

#### doc/concepts/ - Stable Content

Move conceptual documentation that rarely changes:
- Architecture overview (from deployment_improvement_planning.md)
- Data flow diagrams
- Component relationships
- Glossary of terms

#### doc/migrations/

Version-specific migration guides:
- `from-py311-to-py312.md` (extract from uv_migration_plan)
- Template for future migrations

#### doc/architecture/decisions/

ADR format:
```markdown
# ADR-001: Use Luigi for Pipeline Orchestration

## Status
Accepted

## Context
We needed a way to orchestrate multiple Docker containers...

## Decision
We chose Luigi because...

## Consequences
- Positive: ...
- Negative: ...
```

Initial ADRs to create:
- ADR-001: Luigi for orchestration
- ADR-002: Docker-in-Docker architecture
- ADR-003: Separate pentad/decad dashboards

---

## Backlog: Deferred Items

These items are "nice-to-have" but not essential for current operations:

| Item | Reason for Deferral |
|------|---------------------|
| `doc/onboarding/` package | Comprehensive onboarding can wait until core docs stable |
| `deprecations.md` | No deprecated features currently need documenting |
| Architecture diagrams (Mermaid) | Useful but not blocking operations |
| GitHub Pages deployment | Waiting for content to be polished first |
| Module-level README updates | Lower priority than operational docs |

### Criteria to Promote from Backlog
- Item blocks user adoption
- Item addresses repeated support questions
- Phase 1-3 items are complete

---

## Implementation Notes

### Sequencing

```
Week 1 (Phase 1)
├── Day 1-2: Fix configuration.md TODOs
├── Day 2-3: Fill deployment.md Quick Start
├── Day 3-4: Create health_check.sh
└── Day 4-5: Create data_freshness.sh

Month 1 (Phase 2)
├── Week 2: Root Makefile
├── Week 2: pipeline_status.sh
├── Week 3: rollback.sh
├── Week 3-4: GETTING_STARTED.md
└── Week 4: doc/deployment/ restructure

Quarter 1 (Phase 3)
├── Month 2 Week 1-2: Scripts (pre_update, env_diff, update_all)
├── Month 2 Week 3-4: quick-reference.md + doc/concepts/
├── Month 3 Week 1-2: doc/troubleshooting/
└── Month 3 Week 3-4: ADRs + doc/runbooks/
```

### Testing Strategy

Each script should be tested:
1. On local development machine (macOS)
2. On AWS server (Ubuntu)
3. With both valid and invalid inputs

### Review Checkpoints

- End of Week 1: Review Phase 1 deliverables with user
- End of Month 1: Review Phase 2, adjust Phase 3 priorities
- End of Quarter 1: Full documentation audit

---

## Appendix: Source Documents

This consolidated plan synthesizes the following strategy documents:

1. `doc/plans/deployment_improvement_planning.md` - Makefile and local testing
2. `doc/plans/documentation_improvement_plan.md` - Documentation structure
3. `doc/prod/update_deployment_checklist.md` - Update procedures
4. `doc/deployment.md` - Current deployment documentation
5. `doc/configuration.md` - Configuration reference
6. User request for deployment documentation improvements

---

*Created: 2026-01-30*
*Last updated: 2026-01-30*
