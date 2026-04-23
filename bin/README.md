# bin/ — Shell Scripts and Docker Compose

Shell wrappers, Docker Compose files, and utilities for deploying and running
the SAPPHIRE forecast pipeline. All production scripts use Docker containers
orchestrated by a Luigi scheduler daemon.

## Prerequisites

- Docker Engine (with Compose v2)
- A configured `.env` file (see `doc/deployment.md`)
- The Luigi daemon container running (see [Luigi Daemon](#luigi-daemon) below)

## Quick Start

```bash
# From the SAPPHIRE_forecast_tools root directory:

# 1. Start the Luigi daemon (runs persistently on port 8082)
docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon

# 2. Run a forecast workflow
bash bin/run_pentadal_forecasts.sh /path/to/config/.env

# 3. Check progress in the Luigi web UI
open http://localhost:8082
```

## Directory Layout

```
bin/
├── docker-compose-luigi.yml        # Pipeline services + Luigi daemon
├── docker-compose-dashboards.yml   # Forecast dashboards (pentad + decad)
├── luigi-daemon.Dockerfile         # Luigi scheduler daemon image
│
├── run_preprocessing_gateway.sh    # Gateway preprocessing (early morning)
├── run_preprocessing_runoff.sh     # Runoff preprocessing (after data available)
├── run_pentadal_forecasts.sh       # Full pentadal forecast pipeline
├── run_decadal_forecasts.sh        # Full decadal forecast pipeline
├── run_daily_maintenance.sh        # Daily maintenance via Luigi (consolidated)
├── run_long_term_forecasts.sh       # Long-term forecast pipeline (self-gating via schedule query)
├── run_periodic_maintenance.sh     # Periodic maintenance via Luigi (bimonthly/yearly)
│
├── daily_preprunoff_maintenance.sh     # [Legacy] Runoff gap-filling
├── daily_gateway_maintenance.sh        # [Legacy] Gateway gap-filling
├── daily_linreg_maintenance.sh         # [Legacy] LinReg hindcast catch-up
├── daily_ml_maintenance.sh             # [Legacy] ML hindcast catch-up
├── daily_postprc_maintenance.sh        # [Legacy] Postprocessing gap-fill
├── daily_update_sapphire_frontend.sh   # [Legacy] Dashboard container restart
├── bimonthly_long_term_postprocessing.sh  # [Legacy] Monthly ensemble gap-fill
├── yearly_skill_metrics_recalculation.sh  # [Legacy] Full skill metric recalc
├── yearly_snow_norm_recalculation.sh      # [Legacy] Snow norm recalc
│
├── setup_docker.sh                     # Pull images + check Docker daemon
├── restart_sapphire_stack.sh           # Restart sapphire/docker-compose.yml (DBs + APIs + pentadal dashboard on 5006)
├── reset_sapphire_db.sh                # Destructive: wipe DB volumes, rebuild, re-migrate
├── backup_sapphire_db.sh               # pg_dump all four SAPPHIRE DBs (see doc/operations/backup_restore.md)
├── rerun_latest_forecasts.sh           # Re-run the most recent forecast
│
├── deploy_sapphire_forecast_tools.sh   # [Deprecated] Use the step-by-step flow in doc/deployment.md
├── run_sapphire_forecast_tools.sh      # [Deprecated] Superseded by the individual Luigi runners above
├── locally_run_forecast_tools.sh       # [Deprecated] Use apps/run_locally.sh
│
├── utils/
│   ├── common_functions.sh         # Shared functions (read_configuration, etc.)
│   ├── build_docker_images.sh      # Build all images with a given tag
│   ├── pull_docker_images.sh       # Pull images from Docker Hub
│   ├── push_docker_images.sh       # Push images to Docker Hub
│   └── clean_docker.sh             # Remove containers/images (dry-run default)
│
└── monitoring/
    ├── docker.sh                   # Docker container health monitor
    ├── docker_log_watcher.sh       # Log error scanner
    ├── preprunoff.sh               # Preprocessing-specific monitor
    └── README.md                   # Monitoring documentation
```

## Luigi Daemon

All Luigi-orchestrated scripts require a running Luigi scheduler daemon. The
daemon runs as a Docker container with `restart: unless-stopped`.

```bash
# Start (runs persistently, survives reboots)
docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon

# Verify
docker ps | grep luigi-daemon

# Web UI
http://localhost:8082
```

The daemon image is built from `luigi-daemon.Dockerfile` (Python 3.11 + luigid).
Pipeline services connect to it via Docker DNS name `luigi-daemon` on port 8082.

## Docker Compose Files

### `docker-compose-luigi.yml` — Pipeline

All backend pipeline services. Each extends a `pipeline-base` service that
provides the common image, working directory, environment, and volume mounts.

| Service | Purpose |
|---------|---------|
| `luigi-daemon` | Persistent Luigi scheduler (port 8082) |
| `pipeline-base` | Base config (not run directly) |
| `preprocessing-gateway` | Gateway preprocessing |
| `preprocessing-runoff` | Runoff preprocessing |
| `pentadal` | Pentadal forecast workflow |
| `decadal` | Decadal forecast workflow |
| `daily-maintenance` | Daily maintenance (6 workers, ml_memory=3) |
| `long-term` | Long-term forecast workflow (parameterized by `LT_ACTIVE_MODES`) |
| `periodic-maintenance` | Periodic tasks (parameterized by `MAINTENANCE_TASK_TYPE`) |

```bash
# Run a specific service
docker compose -f bin/docker-compose-luigi.yml run --rm pentadal

# Run daily maintenance
docker compose -f bin/docker-compose-luigi.yml run --rm daily-maintenance

# Run periodic maintenance (long_term, skill_recalc, snow_norms, or monthly_norms)
MAINTENANCE_TASK_TYPE=skill_recalc \
  docker compose -f bin/docker-compose-luigi.yml run --rm periodic-maintenance
```

### `docker-compose-dashboards.yml` — Legacy dashboards compose

Historically both forecast dashboards (pentad on 5006, decad on 5007) were served from this compose file. The **pentadal dashboard** has moved to `sapphire/docker-compose.yml` and is now part of the integrated SAPPHIRE stack. This compose is kept for the decadal dashboard, and as a fallback for deployments that have not yet migrated to the integrated stack.

| Service | Port | Status |
|---------|------|--------|
| `pentaddashboard` | 5006 | **Legacy** — migrated to `sapphire/docker-compose.yml` → service `dashboard` |
| `decaddashboard` | 5007 | Still served from here |

Both services (when used) have health checks and `restart: always`. To avoid a port-5006 conflict between the legacy pentadal service and the integrated one, `bin/restart_sapphire_stack.sh` stops this compose before starting the sapphire stack.

## Shell Script Patterns

### Luigi-orchestrated scripts (recommended for cron)

These scripts follow a common pattern:

1. Source `utils/common_functions.sh`
2. Read configuration from the `.env` file argument
3. Ensure the Luigi daemon is running (start if needed)
4. Wait for daemon readiness (up to 60s)
5. Submit the workflow via `docker compose run --rm <service>`

Scripts: `run_pentadal_forecasts.sh`, `run_decadal_forecasts.sh`,
`run_long_term_forecasts.sh`, `run_preprocessing_gateway.sh`,
`run_preprocessing_runoff.sh`, `run_daily_maintenance.sh`,
`run_periodic_maintenance.sh`

Usage:
```bash
bash bin/run_pentadal_forecasts.sh /path/to/config/.env
bash bin/run_long_term_forecasts.sh /path/to/config/.env
bash bin/run_long_term_forecasts.sh --dry-run /path/to/config/.env  # Validate without running
bash bin/run_daily_maintenance.sh /path/to/config/.env
bash bin/run_periodic_maintenance.sh long_term /path/to/config/.env
```

The `--dry-run` flag (supported by `run_long_term_forecasts.sh`) validates the
env file and compose YAML, prints what would happen, and exits without starting
any containers.

### Legacy maintenance scripts (still functional for manual use)

The individual `daily_*.sh`, `bimonthly_*.sh`, and `yearly_*.sh` scripts use
raw `docker run` commands. They remain functional for manual invocation and
debugging but are superseded by `run_daily_maintenance.sh` and
`run_periodic_maintenance.sh` for automated cron scheduling. The Luigi
alternatives enforce dependency ordering explicitly instead of relying on cron
timing offsets.

## Operational scripts

Non-cron helpers for day-to-day management of a running deployment. All three require the external env-file path as their single argument.

### `restart_sapphire_stack.sh <env_file>`

Restart the SAPPHIRE services stack (4 databases + 5 FastAPI services + integrated pentadal dashboard on 5006). Stops the legacy dashboards compose first to prevent a port-5006 conflict, then pulls the latest dashboard image, then brings the sapphire stack back up. Does **not** touch Luigi, pipeline scripts, or SSH tunnels.

Typical use: after changing the env file, after pulling a new dashboard image, or after migrating from the legacy two-compose dashboards setup.

### `reset_sapphire_db.sh [flags]`

Destructive reset of the SAPPHIRE service databases. Stops services, removes DB volumes (wiping all data), rebuilds images, restarts services, waits for health, and re-runs the data migrators. Supports `--preprocessing-only`, `--postprocessing-only`, `--skip-migration`, `--skip-rebuild`, and `-y` to skip the confirmation prompt. User and auth databases are **not** affected.

Typical use: after a breaking schema change when Base.metadata.create_all() cannot migrate in place. **Take a backup first** (see next script).

### `backup_sapphire_db.sh [-d DIR] [-r DAYS] [--dry-run]`

`pg_dump` all four active SAPPHIRE databases (preprocessing, postprocessing, user, auth) to the backup directory (default `/var/backups/sapphire`). Verifies each archive with `pg_restore --list`, renames failures to `.FAILED`, and prunes `.dump` files older than the retention window.

Typical use: nightly cron (see `doc/operations/backup_restore.md` for the full restore and drill procedure).

## Shared Utilities (`utils/common_functions.sh`)

All scripts source this file for shared functionality:

| Function | Purpose |
|----------|---------|
| `read_configuration` | Parse `.env` file, export paths and image tags |
| `print_banner` | Print the SAPPHIRE ASCII banner |
| `establish_ssh_tunnel` | SSH tunnel to iEasyHydro HF (if configured) |
| `cleanup` | Kill SSH tunnel on script exit (used with `trap`) |
| `pull_docker_images` | Pull images with the configured tag |
| `clean_out_docker_space` | Remove all containers and images |
| `clean_out_backend` | Remove backend containers only |
| `stop_and_remove_container` | Stop/remove containers matching a name pattern |
| `start_docker_compose_luigi` | Start a specific pipeline service |
| `start_docker_compose_dashboards` | Start dashboard services |

## Cron Schedule

See `doc/deployment.md` for the full recommended crontab. Summary:

| Time (UTC) | Script | Purpose |
|------------|--------|---------|
| 03:00 | `run_preprocessing_gateway.sh` | Gateway preprocessing |
| 04:00 | `run_pentadal_forecasts.sh` | Pentadal forecast |
| 05:00 | `run_decadal_forecasts.sh` | Decadal forecast |
| 06:00 10th,25th | `run_long_term_forecasts.sh` | Long-term forecast (self-gating via schedule query) |
| 19:00 | `run_daily_maintenance.sh` | Daily maintenance (all steps) |
| 22:00 1st odd months | `run_periodic_maintenance.sh long_term` | Long-term postprocessing |
| 01:00 Dec 31 | `run_periodic_maintenance.sh skill_recalc` | Yearly skill recalculation |
| 02:00 Aug 31 | `run_periodic_maintenance.sh snow_norms` | Yearly snow norm recalculation |
| 03:00 Jan 1  | `run_periodic_maintenance.sh monthly_norms` | Yearly monthly discharge norm recalculation from iEH HF SDK |

Log files use timestamped names (`sapphire_*_YYYYMMDD.log`) with automatic
cleanup of logs older than 7 days.

## Ports

| Port | Service |
|------|---------|
| 5006 | Pentadal forecast dashboard |
| 5007 | Decadal forecast dashboard |
| 8082 | Luigi web UI |

## Environment Variables

Scripts expect these variables (set in `.env` or exported before running):

| Variable | Purpose |
|----------|---------|
| `ieasyhydroforecast_env_file_path` | Path to `.env` file |
| `ieasyhydroforecast_data_root_dir` | Root data directory (derived from `.env` path) |
| `ieasyhydroforecast_data_ref_dir` | Reference data directory |
| `ieasyhydroforecast_backend_docker_image_tag` | Backend image tag (`latest`, `py312`, `local`) |
| `ieasyhydroforecast_frontend_docker_image_tag` | Frontend image tag |
| `ieasyhydroforecast_organization` | Organization (`demo`, `kghm`, `tjhm`) |
| `ieasyhydroforecast_ssh_to_iEH` | Enable SSH tunnel to iEasyHydro (`true`/`false`) |
| `COMPOSE_PROJECT_NAME` | Docker Compose project name (default: `sapphire`) |

## Testing Long-Term Pipeline Locally

The long-term forecasting pipeline can be validated locally in layers, from
fast unit tests to full Docker integration:

```bash
# Layer 1: Unit tests (no Docker required)
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline

# Layer 2: Dry-run validation (validates env + compose, no containers started)
bash bin/run_long_term_forecasts.sh --dry-run /path/to/.env

# Layers 2-4: Full Docker test (build + smoke + integration)
bash apps/run_docker_tests.sh ltforecast --with-integration /path/to/.env
```

The `--with-integration <env_path>` flag on `run_docker_tests.sh` adds a
PIPELINE INTEGRATION phase after the smoke tests. It runs:

1. **Compose YAML validation** — `docker compose config` on `docker-compose-luigi.yml`
2. **Bash syntax check** — `bash -n` on `run_long_term_forecasts.sh`
3. **Schedule query smoke** — runs `lt_schedule_query.py` inside the ltforecast
   container with a known date to verify active modes are returned
4. **Dry-run validation** — runs `run_long_term_forecasts.sh --dry-run`
