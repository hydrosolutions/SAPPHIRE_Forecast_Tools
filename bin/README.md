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
├── deploy_sapphire_forecast_tools.sh   # First-time deployment
├── run_sapphire_forecast_tools.sh      # Full daily run (all steps)
├── rerun_latest_forecasts.sh           # Re-run the most recent forecast
├── locally_run_forecast_tools.sh       # [Deprecated] Use apps/run_locally.sh
├── setup_docker.sh                     # Pull images + check Docker daemon
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
| `periodic-maintenance` | Periodic tasks (parameterized by `MAINTENANCE_TASK_TYPE`) |

```bash
# Run a specific service
docker compose -f bin/docker-compose-luigi.yml run --rm pentadal

# Run daily maintenance
docker compose -f bin/docker-compose-luigi.yml run --rm daily-maintenance

# Run periodic maintenance (long_term, skill_recalc, or snow_norms)
MAINTENANCE_TASK_TYPE=skill_recalc \
  docker compose -f bin/docker-compose-luigi.yml run --rm periodic-maintenance
```

### `docker-compose-dashboards.yml` — Dashboards

Forecast dashboards built with Panel (Python, served via Bokeh Server).

| Service | Port | Purpose |
|---------|------|---------|
| `pentaddashboard` | 5006 | Pentadal forecast dashboard |
| `decaddashboard` | 5007 | Decadal forecast dashboard |

Both have health checks and `restart: always`.

## Shell Script Patterns

### Luigi-orchestrated scripts (recommended for cron)

These scripts follow a common pattern:

1. Source `utils/common_functions.sh`
2. Read configuration from the `.env` file argument
3. Ensure the Luigi daemon is running (start if needed)
4. Wait for daemon readiness (up to 60s)
5. Submit the workflow via `docker compose run --rm <service>`

Scripts: `run_pentadal_forecasts.sh`, `run_decadal_forecasts.sh`,
`run_preprocessing_gateway.sh`, `run_preprocessing_runoff.sh`,
`run_daily_maintenance.sh`, `run_periodic_maintenance.sh`

Usage:
```bash
bash bin/run_pentadal_forecasts.sh /path/to/config/.env
bash bin/run_daily_maintenance.sh /path/to/config/.env
bash bin/run_periodic_maintenance.sh long_term /path/to/config/.env
```

### Legacy maintenance scripts (still functional for manual use)

The individual `daily_*.sh`, `bimonthly_*.sh`, and `yearly_*.sh` scripts use
raw `docker run` commands. They remain functional for manual invocation and
debugging but are superseded by `run_daily_maintenance.sh` and
`run_periodic_maintenance.sh` for automated cron scheduling. The Luigi
alternatives enforce dependency ordering explicitly instead of relying on cron
timing offsets.

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
| 19:00 | `run_daily_maintenance.sh` | Daily maintenance (all steps) |
| 22:00 1st odd months | `run_periodic_maintenance.sh long_term` | Long-term postprocessing |
| 01:00 Jan 1 | `run_periodic_maintenance.sh skill_recalc` | Yearly skill recalculation |
| 02:00 Aug 25 | `run_periodic_maintenance.sh snow_norms` | Yearly snow norm recalculation |

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
