# INFRA-001: Create Makefile as unified developer entry point

**Status**: Draft
**Module**: infra
**Priority**: High
**Labels**: `infrastructure`, `developer-experience`, `local-testing`

---

## Summary

Create a Makefile at the repo root that wraps existing scripts (`apps/run_tests.sh`,
`apps/run_locally.sh`, `apps/run_docker_tests.sh`) with discoverable `make` targets.
Add a `.env` template and data setup script for first-time local development.

## Context

The project already has comprehensive scripts:

| Script | Lines | Purpose |
|--------|-------|---------|
| `apps/run_tests.sh` | 249 | Runs pytest for all modules and services |
| `apps/run_locally.sh` | ~1400 | Runs any pipeline mode locally with uv |
| `apps/run_docker_tests.sh` | ~200 | Docker smoke tests (build + run) |

These scripts work well but are hard to discover. New developers must read docs to
find them, and the invocation patterns differ between scripts. A Makefile provides:
- `make help` for self-documenting targets
- Tab completion in most shells
- Consistent interface: `make test`, `make run`, `make lint`
- No new functionality — just a convenience layer

## Problem

- No standard entry point — developers must know which script to run and from where
- No `.env` template for Mac/local development (first-time setup is manual)
- No data directory setup helper (symlinks to Dropbox must be created manually)
- `ruff` invocations require remembering the config path

## What Already Exists (DO NOT duplicate)

These are the authoritative scripts. The Makefile wraps them, it does not replace
them:

**`apps/run_tests.sh`** — Test runner
```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh              # all modules
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>     # one module
cd apps && bash run_tests.sh service:<service>                   # one service
```

**`apps/run_locally.sh`** — Local pipeline runner
```bash
cd apps && bash run_locally.sh daily                  # full daily pipeline
cd apps && bash run_locally.sh short-term             # preprocessing + forecasts
cd apps && bash run_locally.sh maintenance            # all maintenance tasks
cd apps && bash run_locally.sh maintenance:ml         # ML maintenance only
cd apps && bash run_locally.sh recalculate_skill_metrics
```
Requires `ieasyhydroforecast_env_file_path` to be set.

**`apps/run_docker_tests.sh`** — Docker smoke tests
```bash
cd apps && bash run_docker_tests.sh          # all modules
cd apps && bash run_docker_tests.sh --skip-ml  # skip ML (slow)
```

---

## Implementation Plan

### Phase 1: Makefile + test/run/lint targets

**Files to create:**

| File | Purpose |
|------|---------|
| `Makefile` | Developer entry point wrapping existing scripts |

**Makefile design:**

```makefile
# SAPPHIRE Forecast Tools — Development Makefile
# Usage: make help

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Configuration (override: make ENV_FILE=path/to/.env run-daily)
ENV_FILE ?= $(HOME)/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm

##@ Testing
.PHONY: test test-all

test:  ## Run all module tests (fast, no Docker)
	cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh

test-%:  ## Run tests for one module (e.g., make test-postprocessing_forecasts)
	cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh $*

test-service-%:  ## Run tests for one service (e.g., make test-service-postprocessing)
	cd apps && bash run_tests.sh service:$*

test-docker:  ## Run Docker smoke tests (slower, builds images)
	cd apps && bash run_docker_tests.sh --skip-ml

test-docker-all:  ## Run all Docker smoke tests including ML
	cd apps && bash run_docker_tests.sh

##@ Running Locally
.PHONY: run-daily run-short-term run-long-term run-maintenance

run-daily:  ## Run full daily pipeline locally
	cd apps && ieasyhydroforecast_env_file_path=$(ENV_FILE) bash run_locally.sh daily

run-short-term:  ## Run short-term pipeline (preprocessing + forecasts)
	cd apps && ieasyhydroforecast_env_file_path=$(ENV_FILE) bash run_locally.sh short-term

run-long-term:  ## Run long-term forecasting pipeline
	cd apps && ieasyhydroforecast_env_file_path=$(ENV_FILE) bash run_locally.sh long-term

run-maintenance:  ## Run all maintenance tasks
	cd apps && ieasyhydroforecast_env_file_path=$(ENV_FILE) bash run_locally.sh maintenance

run-maintenance-%:  ## Run specific maintenance (e.g., make run-maintenance-ml)
	cd apps && ieasyhydroforecast_env_file_path=$(ENV_FILE) bash run_locally.sh maintenance:$*

run-recalc:  ## Run skill metrics recalculation
	cd apps && ieasyhydroforecast_env_file_path=$(ENV_FILE) bash run_locally.sh recalculate_skill_metrics

##@ Code Quality
.PHONY: lint lint-fix format

lint:  ## Check linting (ruff check, no fixes)
	ruff check apps/

lint-fix:  ## Fix linting issues automatically
	ruff check --fix apps/ && ruff format apps/

format:  ## Format code with ruff
	ruff format apps/

lint-%:  ## Lint one module (e.g., make lint-postprocessing_forecasts)
	ruff check --fix apps/$*/ && ruff format apps/$*/

##@ Setup
.PHONY: setup setup-venvs

setup-venvs:  ## Install dependencies for all modules (uv sync)
	@for dir in apps/*/; do \
		if [ -f "$$dir/pyproject.toml" ]; then \
			echo "Installing: $$dir"; \
			(cd "$$dir" && uv sync) || true; \
		fi; \
	done

##@ Utilities
.PHONY: clean status help

clean:  ## Remove __pycache__ and .pytest_cache directories
	find . -type d -name "__pycache__" -not -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -not -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true

status:  ## Show running SAPPHIRE Docker containers
	@docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "No containers running"

help:  ## Show this help message
	@echo "SAPPHIRE Forecast Tools"
	@echo ""
	@echo "Usage: make [target] [VAR=value]"
	@echo ""
	@echo "Variables:"
	@echo "  ENV_FILE  Path to .env file (default: kyg_data .env_develop_kghm)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf ""} /^[a-zA-Z_%-]+:.*?##/ { printf "  \033[32m%-24s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[34m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
```

**Key design decisions:**

1. **Wraps existing scripts** — `make test` calls `run_tests.sh`, `make run-daily`
   calls `run_locally.sh`. No logic duplication.
2. **Pattern targets** — `make test-<module>` and `make lint-<module>` use Make's
   `%` wildcard pattern to support any module without listing them all.
3. **ENV_FILE default** — Points to the standard develop env file. Override with
   `make run-daily ENV_FILE=/other/path/.env`.
4. **No setup-data or setup-env targets initially** — These are Phase 2. Keep
   Phase 1 focused on wrapping what exists.

**Steps:**

- [ ] **1a.** Create `Makefile` at repo root with the targets above
- [ ] **1b.** Verify `make help` displays all targets correctly
- [ ] **1c.** Verify `make test` runs all module tests
- [ ] **1d.** Verify `make test-postprocessing_forecasts` runs one module
- [ ] **1e.** Verify `make lint` runs ruff check
- [ ] **1f.** Verify `make run-daily` runs the pipeline (requires valid ENV_FILE)
- [ ] **1g.** Add `Makefile` note to CLAUDE.md or README (one line, not a whole
  section)

### Phase 2: First-time setup helpers (optional, can be separate PR)

**Files to create:**

| File | Purpose |
|------|---------|
| `bin/setup_local_data.sh` | Create symlinks from `data/` to Dropbox |
| `config/.env_local_template` | Mac-friendly `.env` with relative paths and docs |

**`bin/setup_local_data.sh`** — Interactive script that:
- Asks user for their data directory path (or accepts as argument)
- Creates symlinks for `config`, `intermediate_data`, `daily_runoff`, `GIS`, etc.
- Validates that expected files exist
- Prints summary of what was linked

**`config/.env_local_template`** — Documented template with:
- Relative paths (`./data/config` instead of `../../../...`)
- All required variables with comments explaining each section
- Placeholder credentials marked clearly
- Added to `.gitignore`: `config/.env_local` (but NOT the template)

**Makefile additions:**

```makefile
setup: setup-venvs setup-data setup-env  ## Complete first-time setup

setup-data:  ## Set up data directory with symlinks
	@./bin/setup_local_data.sh

setup-env:  ## Create .env_local from template (if not exists)
	@if [ ! -f config/.env_local ]; then \
		cp config/.env_local_template config/.env_local; \
		echo "Created config/.env_local — edit with your credentials"; \
	else \
		echo "config/.env_local already exists"; \
	fi
```

### Phase 3: Docker local testing targets (optional, lower priority)

Only if Docker local testing proves useful beyond `run_docker_tests.sh`:

- `make docker-build` — build images
- `make docker-shell` — interactive shell in pipeline container
- `bin/docker-compose-local.yml` — simplified compose for local dev

**Deferred**: `apps/run_docker_tests.sh` already handles Docker smoke tests.
Only implement Phase 3 if a need arises for interactive Docker debugging that
the existing script doesn't cover.

---

## What NOT to Create

The original plan (`Makefile.planned`) included several things that are now
redundant:

| Planned | Why skip |
|---------|----------|
| `bin/local_run.sh` | `apps/run_locally.sh` already does this (1400 lines, comprehensive) |
| `bin/local_docker_test.sh` | `apps/run_docker_tests.sh` already does this |
| Production deploy targets in Makefile | Existing `bin/` scripts work, operators use them directly |
| `bin/sapphire.sh` unified entry point | Makefile replaces this need |
| Colored output helpers | Keep it simple — echo is fine |

Delete that deprecated plan to avoid confusion. The new Makefile is a thin wrapper around existing scripts, not a full redesign. The focus is on discoverability and convenience, not adding new functionality.

---

## Testing

### Verification Steps

```bash
# Phase 1 verification
make help                          # should display all targets
make test                          # should run all module tests
make test-iEasyHydroForecast       # should run one module
make lint                          # should run ruff check
make clean                         # should remove __pycache__

# Phase 2 verification (after setup scripts exist)
make setup-env                     # should create .env_local
make setup-data                    # should create data symlinks
```

### What Could Go Wrong

| Risk | Mitigation |
|------|------------|
| `make test-%` pattern doesn't match module names with underscores | Test with `make test-postprocessing_forecasts` before committing |
| ENV_FILE default path doesn't exist on CI | CI doesn't use Makefile — it calls scripts directly |
| `ruff` not installed globally | Modules have ruff in their .venv — adjust lint target to use `uv run ruff` if needed |

---

## Effort Estimates

| Phase | Scope | Effort |
|-------|-------|--------|
| Phase 1 | Makefile with test/run/lint/clean targets | ~2 hours |
| Phase 2 | setup_local_data.sh + .env_local_template | ~3 hours |
| Phase 3 | Docker local compose (if needed) | ~2 hours |

**Total**: ~5 hours for Phases 1-2 (Phase 3 deferred)

---

## Acceptance Criteria

- [ ] `make help` displays all available targets with descriptions
- [ ] `make test` runs all module tests via `run_tests.sh`
- [ ] `make test-<module>` runs tests for a specific module
- [ ] `make lint` runs ruff check on `apps/`
- [ ] `make run-daily` runs the daily pipeline locally (requires valid ENV_FILE)
- [ ] No existing scripts are modified (Makefile is additive only)
- [ ] All existing tests pass with zero new skips

---

## Supersedes

This plan supersedes the original `Makefile.planned` template and the INFRA-001
section of `deployment_improvement_planning.md`. Those documents designed the
Makefile before `apps/run_locally.sh` and `apps/run_tests.sh` existed. The new
approach is simpler: wrap existing scripts instead of duplicating their logic.

## Related

- `doc/plans/deployment_improvement_planning.md` (original design, now partially
  superseded)
- `doc/plans/Makefile.planned` (original template, now superseded by this plan)
- `apps/run_tests.sh` — authoritative test runner
- `apps/run_locally.sh` — authoritative local pipeline runner
- `apps/run_docker_tests.sh` — authoritative Docker smoke test runner

---

*Last updated: 2026-02-27 — Rewritten to wrap existing scripts instead of
duplicating them. Original Makefile.planned predated run_locally.sh and
run_tests.sh.*
