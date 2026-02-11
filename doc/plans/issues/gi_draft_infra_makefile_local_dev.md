> **NOTE**: This plan was auto-extracted from `deployment_improvement_planning.md`
> and `Makefile.planned`. It needs review and refining before it can be implemented.

# Create Makefile and local development infrastructure

**Status**: Draft
**Module**: infra
**Priority**: High
**Labels**: `infrastructure`, `developer-experience`, `local-testing`

---

## Summary

Create a Makefile at the repo root with test/build/deploy targets, along with supporting scripts for local development. This unblocks the local developer testing workflow which currently requires memorizing long ad-hoc commands.

## Context

`doc/plans/deployment_improvement_planning.md` designed a comprehensive Makefile and local dev setup. `doc/plans/Makefile.planned` contains the full Makefile template. No implementation has started.

## Problem

- No standard way to run modules locally (each has different commands)
- No local Docker testing infrastructure
- No `.env` template for Mac/local development
- Developers must read multiple docs to reconstruct the right incantation

## Implementation Steps

### Step 1: Create Makefile

Copy `doc/plans/Makefile.planned` to `Makefile` at repo root. Verify all referenced paths exist.

**File to create**: `Makefile`
**Source**: `doc/plans/Makefile.planned`

### Step 2: Create bin/local_run.sh

Wrapper script that sets up PYTHONPATH, sources .env, and runs a specified module with `uv run`.

**File to create**: `bin/local_run.sh`

Key behavior:
- Accept module name as argument (e.g., `preprocessing_runoff`, `linear_regression`)
- Source the env file from `$ENV_FILE` or `config/.env_local`
- Set `PYTHONPATH` to include `apps/iEasyHydroForecast`
- Run the module's main script with `uv run`

### Step 3: Create bin/setup_local_data.sh

Script to create symlinks from a local `data/` directory to the user's forecast data (e.g., Dropbox). Must be generalizable (not hardcoded to any user).

**File to create**: `bin/setup_local_data.sh`

Key behavior:
- Prompt user for data directory path (or accept as argument)
- Create symlinks for required data directories
- Validate that expected files exist

### Step 4: Create config/.env_local_template

Template `.env` file with Mac-friendly paths and comments explaining each variable.

**File to create**: `config/.env_local_template`

### Step 5: Create bin/docker-compose-local.yml

Docker Compose file for running the pipeline locally with a local Luigi scheduler (no systemd needed).

**File to create**: `bin/docker-compose-local.yml`

### Step 6: Verify and test

- Run `make help` to verify all targets display
- Run `make setup-venv` to verify dependency installation
- Run `make test-preprunoff` to verify a module test works
- Run `make lint` if linting is configured

## Acceptance Criteria

- [ ] `make help` displays all available targets
- [ ] `make setup` completes without errors on a clean checkout
- [ ] `make test-preprunoff` runs preprocessing_runoff tests
- [ ] `make test-linreg` runs linear_regression tests
- [ ] `config/.env_local_template` contains all required variables with documentation
- [ ] `bin/setup_local_data.sh` works on Mac without hardcoded paths

## Files to Create

| File | Purpose |
|------|---------|
| `Makefile` | Main development entry point |
| `bin/local_run.sh` | Module runner wrapper |
| `bin/setup_local_data.sh` | Data directory setup |
| `config/.env_local_template` | Environment template |
| `bin/docker-compose-local.yml` | Local Docker testing |

## Related

- `doc/plans/deployment_improvement_planning.md` (design source)
- `doc/plans/Makefile.planned` (Makefile template)
