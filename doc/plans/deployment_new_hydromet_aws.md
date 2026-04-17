# Deployment Plan: SAPPHIRE on a New AWS Server

**Target date:** Week of 2026-03-31
**Server:** AWS (Ubuntu 24.04 LTS)

This plan traces the full deployment end-to-end for a new hydromet service,
referencing existing docs where they cover a step, and flagging gaps inline as
`[DOC-GAP]` issues. Each gap has a concrete fix action so the docs can be
improved before or after deployment.

Placeholders used throughout:
- `<org>` — short organization identifier (e.g., `demo`)
- `<country>` — country code (e.g., `che`)
- `<data_folder>` — `<country>_data_forecast_tools`
- `<env_file>` — `.env_<org>`
- `<tz_offset>` — UTC offset for the target timezone

---

## Table of Contents

- [Deployment Plan: SAPPHIRE on a New AWS Server](#deployment-plan-sapphire-on-a-new-aws-server)
  - [Table of Contents](#table-of-contents)
  - [Phase 1: AWS Server Provisioning](#phase-1-aws-server-provisioning)
  - [Phase 2: Base Software Installation](#phase-2-base-software-installation)
  - [Phase 3: Clone Repo \& Create Data Folder](#phase-3-clone-repo--create-data-folder)
  - [Phase 4: SAPPHIRE Services (API Stack)](#phase-4-sapphire-services-api-stack)
    - [4.1 Create services .env](#41-create-services-env)
    - [4.2 Start the services](#42-start-the-services)
    - [4.3 Run data migrations (if migrating from CSV)](#43-run-data-migrations-if-migrating-from-csv)
  - [Phase 5: Pipeline .env Configuration](#phase-5-pipeline-env-configuration)
  - [Phase 6: Station Configuration \& Data](#phase-6-station-configuration--data)
  - [Phase 7: iEasyHydro HF Connectivity](#phase-7-ieasyhydro-hf-connectivity)
  - [Phase 8: Luigi Daemon \& Pipeline Images](#phase-8-luigi-daemon--pipeline-images)
    - [8.1 Pull Docker images](#81-pull-docker-images)
    - [8.2 Start Luigi daemon](#82-start-luigi-daemon)
    - [8.3 Test a pipeline run](#83-test-a-pipeline-run)
  - [Phase 9: Dashboards \& Reverse Proxy](#phase-9-dashboards--reverse-proxy)
    - [9.1 Start dashboards](#91-start-dashboards)
    - [9.2 Reverse proxy \& HTTPS](#92-reverse-proxy--https)
  - [Phase 10: Cron Jobs](#phase-10-cron-jobs)
  - [Phase 11: Monitoring](#phase-11-monitoring)
  - [Phase 12: Testing \& Validation](#phase-12-testing--validation)
    - [Validation checklist](#validation-checklist)
  - [Doc-Gap Summary](#doc-gap-summary)
    - [Priority for pre-deployment fixes](#priority-for-pre-deployment-fixes)
  - [Deployment Log](#deployment-log)

---

## Phase 1: AWS Server Provisioning

**Ref:** `doc/deployment.md` > Prerequisites > Server requirements > Provisioning on AWS

- [ ] Provision Ubuntu 24.04 LTS instance following `doc/deployment.md`
  - Use the AWS settings table for instance type, storage, and AMI
  - For ML models: use `t3.xlarge` (16 GB RAM) and 30 GB storage
- [ ] Configure AWS security group — open inbound ports per the port table
  in `doc/deployment.md` > "Configuring your server"
  - DB ports (5433–5436): **do not expose** — localhost only
  - API ports (8000–8005): localhost only unless using reverse proxy
  - Dashboard ports (5006, 5007): expose directly or via reverse proxy
- [ ] Set up SSH access (key-based)
- [ ] Confirm sudo privileges

---

## Phase 2: Base Software Installation

**Ref:** `doc/deployment.md` > Software requirements

- [ ] Install Docker Engine (follow Docker docs for Ubuntu)
  ```bash
  # https://docs.docker.com/engine/install/ubuntu/
  ```
- [ ] Verify Docker Compose v2 is available
  ```bash
  docker compose version
  ```
- [ ] Install Git
  ```bash
  sudo apt-get update && sudo apt-get install -y git
  ```
- [ ] Install autossh (needed later if SSH tunnel required)
  ```bash
  sudo apt-get install -y autossh
  ```
- [ ] Create directory structure
  ```bash
  sudo mkdir -p /data
  sudo chown ubuntu:ubuntu /data
  mkdir -p /home/ubuntu/logs
  ```

---

## Phase 3: Clone Repo & Create Data Folder

**Ref:** `doc/deployment.md` > Download this repository, `doc/configuration.md` > New deployment setup

- [ ] Clone the repository
  ```bash
  cd /data
  git clone https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools.git
  cd SAPPHIRE_Forecast_Tools
  git checkout maxat_sapphire_2   # or whichever branch is production-ready
  ```

- [ ] Create the data folder with the documented structure
  ```bash
  mkdir -p /data/<data_folder>/{config,daily_runoff,intermediate_data,GIS,templates,reports}
  ```

  Expected structure (from `doc/configuration.md`):
  ```
  <data_folder>/
  ├── config/
  │   ├── <env_file>
  │   ├── config_all_stations_library.json
  │   ├── config_station_selection.json
  │   ├── config_output.json
  │   ├── config_development_restrict_station_selection.json
  │   └── locale/          # copy from apps/config/locale/
  ├── daily_runoff/
  ├── intermediate_data/
  ├── GIS/
  ├── templates/
  └── reports/
  ```

- [ ] Copy locale files
  ```bash
  cp -r /data/SAPPHIRE_Forecast_Tools/apps/config/locale \
        /data/<data_folder>/config/
  ```

  > `[DOC-GAP-2]` ✅ **Resolved** — rudimentary `.po`/`.mo` workflow + "Adding a new language" steps in `doc/configuration.md#localization`.

---

## Phase 4: SAPPHIRE Services (API Stack)

> `[DOC-GAP-3]` ✅ **Resolved** — SAPPHIRE services section added to `doc/deployment.md#sapphire-services-api-stack`.

**Ref:** `sapphire/README.md`

### 4.1 Create services .env

- [ ] Copy the template
  ```bash
  cp /data/SAPPHIRE_Forecast_Tools/sapphire/.env.example \
     /data/SAPPHIRE_Forecast_Tools/sapphire/.env
  ```

- [ ] Edit `sapphire/.env` — fill in:
  ```
  POSTGRES_USER=postgres
  POSTGRES_PASSWORD=<generate-strong-password>
  PREPROCESSING_DB=preprocessing_db
  POSTPROCESSING_DB=postprocessing_db
  USER_DB=user_db
  AUTH_DB=auth_db
  JWT_SECRET_KEY=<generate-strong-random-secret>
  # Service URLs (internal Docker network names)
  PREPROCESSING_API_URL=http://preprocessing-api:8002
  POSTPROCESSING_API_URL=http://postprocessing-api:8003
  USER_API_URL=http://user-api:8004
  AUTH_API_URL=http://auth-api:8005
  ```

  > `[DOC-GAP-4]` ✅ **Resolved** — `sapphire/.env.example` annotated with placeholder values and inline comments; `sapphire/README.md` Environment setup expanded with a per-group walkthrough.

### 4.2 Start the services

- [ ] Start all services
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools/sapphire
  docker compose --env-file .env up -d
  ```

- [ ] Wait for databases to be healthy
  ```bash
  # Check all containers
  docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}"
  ```

- [ ] Health check
  ```bash
  curl http://localhost:8000/health
  curl http://localhost:8000/health/ready
  ```

### 4.3 Populate the database with historical data

The preprocessing database **must** contain historical daily runoff data
(horizon_type=DAY) going back multiple years before the pipeline can produce
forecasts. Without it, `linear_regression` will crash in DECAD mode with
`ValueError: Cannot write empty runoff_stats DataFrame to CSV` (the PENTAD
mode silently writes empty hydrographs — equally broken, just non-fatal).

There are two paths to populate the `runoffs` table:

**Path A — Data migrator (bulk CSV import, inside Docker container):**

Use this when you have historical CSV files already mounted in the container
(e.g., migrating from CSV-based SAPPHIRE or restoring from backup).

```bash
# Preprocessing
docker exec -it sapphire-preprocessing-api /bin/bash
python app/data_migrator.py --type runoff
python app/data_migrator.py --type hydrograph
python app/data_migrator.py --type meteo
python app/data_migrator.py --type snow

# Postprocessing
docker exec -it sapphire-postprocessing-api /bin/bash
python app/data_migrator.py --type skillmetric --batch-size 1
python app/data_migrator.py --type lrforecast
python app/data_migrator.py --type combinedforecast
python app/data_migrator.py --type forecast
python app/data_migrator.py --type longforecast
```

**Path B — `initialize` target (fresh deployment: Excel/iEH HF → API →
hindcast → skill metrics):**

Use this for a fresh deployment where historical data lives in Excel files
in `daily_runoff/` and/or in the iEasyHydro HF database. The `initialize`
target in `apps/run_locally.sh` wraps the full first-time sequence in a
single command (added per the reviewed issue
`doc/plans/issues/review_gi_draft_infra_new_deployment_initialization.md`):

1. **Preprocessing (maintenance fetch)** — re-reads Excel files and
   detects gaps. `operational` mode would only fetch today, so
   `maintenance` is required for a cold start.
2. **`initial_api_sync.py`** — pushes the full CSV history to the
   preprocessing API. This sets `SAPPHIRE_SYNC_MODE=initial` internally
   so every record is written, not just today's.
3. **LR hindcast (PENTAD + DECAD)** — runs `linear_regression.py
   --hindcast --start-date $ieasyhydroforecast_START_DATE` for each
   horizon. Populates the postprocessing database with historical
   forecasts used by the skill-metric calculation.
4. **Skill metrics (PENTAD + DECAD)** — runs
   `recalculate_skill_metrics.py` for each horizon.

```bash
# ieasyhydroforecast_START_DATE is a new REQUIRED env variable for the
# initialize path (per R7 in the reviewed issue). Set it in your external
# env file first, e.g. ieasyhydroforecast_START_DATE=2000-01-01 for a
# full hindcast. If it is missing, `run_locally.sh initialize` exits
# with an error.
ieasyhydroforecast_env_file_path=/data/<data_folder>/config/<env_file> \
  bash apps/run_locally.sh initialize
```

After initialization completes, daily runs use the normal targets
(`short-term`, `long-term`, `daily`, `maintenance`, etc.) which default
to `operational` mode.

For reference, `SAPPHIRE_SYNC_MODE` is the internal mechanism that the
`_write_*_to_api()` functions in `preprocessing_runoff`,
`preprocessing_gateway`, and `linear_regression` check to decide how much
data to push on each run:
- `operational` (default): today only
- `maintenance`: last 30 days
- `initial`: all data

`run_locally.sh initialize` sets `SAPPHIRE_SYNC_MODE=initial` internally
for the `initial_api_sync.py` step; you should not need to set it
manually at the command line. Note in particular that setting
`SAPPHIRE_SYNC_MODE=initial` on a plain `preprocessing_runoff` run does
**not** work as a substitute — `get_runoff_data_for_sites_HF` only
recognises `operational` and `maintenance` as fetch modes (see
`apps/preprocessing_runoff/src/src.py:3520-3524`), so the fetch is
demoted to `operational` (today only) even though the API write would
try to send "all data" that was never fetched.

> `[DOC-GAP-5]` ✅ **Resolved** — `initialize` target implemented in `apps/run_locally.sh` (see `review_gi_draft_infra_new_deployment_initialization.md`); Path B above is the fresh-deployment recipe.

---

## Phase 5: Pipeline .env Configuration

**Ref:** `doc/configuration.md`, `apps/config/.env` (as template)

- [ ] Create `<env_file>` by copying an existing example and adapting
  ```bash
  cp /data/SAPPHIRE_Forecast_Tools/apps/config/.env \
     /data/<data_folder>/config/<env_file>
  ```

- [ ] Edit the file — key variables to change:

  | Variable | What to set | Documented? |
  |----------|------------|-------------|
  | `ieasyhydroforecast_organization` | `<org>` | Yes |
  | All data paths (`../../../...`) | Point to `../../../<data_folder>/` | Implicit |
  | `IEASYHYDRO_HOST` | Endpoint or `False` | Yes |
  | `IEASYHYDROHF_HOST` | Endpoint or `False` | **No** |
  | `IEASYHYDROHF_USERNAME` | Credentials | **No** |
  | `IEASYHYDROHF_PASSWORD` | Credentials | **No** |
  | `ieasyhydroforecast_connect_to_iEH` | `True` or `False` | Yes |
  | `ieasyhydroforecast_ssh_to_iEH` | Depends on network setup | Yes |
  | `ieasyhydroforecast_API_KEY_GATEAWAY` | API key or empty | Yes |
  | `ieasyhydroforecast_run_ML_models` | `true` or `false` | Yes |
  | `ieasyhydroforecast_run_CM_models` | `true` or `false` | Yes |
  | `ieasyhydroforecast_locale` | `ru_KG` or `en_CH` | Yes |
  | `ieasyforecast_country_borders_file_name` | GADM shapefile for your country | Yes |
  | `ieasyhydroforecast_backend_docker_image_tag` | `local` or `latest` | Yes |
  | `ieasyhydroforecast_frontend_docker_image_tag` | `local` or `latest` | Yes |
  | `SAPPHIRE_PIPELINE_SMTP_*` | SMTP config for alerts | In monitoring doc |
  | `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS` | Alert recipients | In monitoring doc |

  > `[DOC-GAP-6]` ✅ **Resolved** — categorised `.env variable reference` with minimal deployment profile added to `doc/configuration.md`.

  > `[DOC-GAP-7]` ✅ **Resolved** — `IEASYHYDROHF_HOST`/`USERNAME`/`PASSWORD` covered in the `doc/configuration.md` reference table and Add-ons section.

- [ ] Update all relative paths to point to your `<data_folder>`
  ```bash
  sed -i 's/<old_data_folder>/<data_folder>/g' \
    /data/<data_folder>/config/<env_file>
  ```

- [ ] Verify path convention: from `apps/<module>/`, the data folder is at
  `../../../<data_folder>/` (three levels up). Confirm this
  matches the actual directory layout on the server.

  > `[DOC-GAP-8]` ✅ **Resolved** — directory-tree diagram and walk-up table added to `doc/configuration.md` for the `../../../<data_folder>/` convention.

---

## Phase 6: Station Configuration & Data

**Ref:** `doc/configuration.md` > The config all stations library file

- [ ] Create `config_all_stations_library.json` with your stations
  - Required fields per station: `code`, `name_ru`, `river_ru`, `punkt_ru`,
    `lat`, `long`, `region`, `basin`
  - All values as single-element lists (e.g., `"lat": [41.3]`)
  - Station codes must be numeric strings starting with `1`
  - For manual sites (no iEH HF): add `"data_source": ["manual"]`

- [ ] Create `config_station_selection.json` — subset of stations to forecast
  - Format: same as documented, copy structure from example

- [ ] Create `config_output.json`
  - Copy from `apps/config/config_output.json` and adapt

- [ ] Create `config_development_restrict_station_selection.json`
  - Start with a small subset (2-3 stations) for initial testing
  - Set to `null` in `.env` once everything works

- [ ] Download admin boundaries for your country
  - From https://gadm.org/data.html — download the appropriate shapefile
  - Place in `/data/<data_folder>/GIS/`
  - Update `ieasyforecast_country_borders_file_name` in `.env`

- [ ] Place historical discharge Excel files in `daily_runoff/`
  - One file per station: `<station_code>_*.xlsx`
  - Must have sheet named `2000` with columns `date` (YYYY-MM-DD) and
    `discharge` (m3/s)

- [ ] Create forecast bulletin templates in `templates/`
  - Copy from existing deployment and adapt headers/labels
  - See `doc/bulletin_template_tags.md` for available template tags

---

## Phase 7: iEasyHydro HF Connectivity

**Ref:** `doc/prod/update_deployment_checklist.md` > Section 1.2

**Decision point:** Does the target hydromet have iEasyHydro HF installed?

- **If YES with direct network access** → Option A (set `IEASYHYDROHF_HOST`
  directly)
- **If YES but API on localhost only** → Option B (SSH tunnel, same network)
- **If YES but different network (AWS to on-premise)** → Option C (SSH tunnel
  with port forwarding) — this is most likely for AWS
- **If NO** → Set `ieasyhydroforecast_connect_to_iEH=False` and use manual
  data entry / Google Sheets

For Option C (most likely scenario for AWS):

- [ ] Get remote server details from the hydromet IT team:
  - SSH username, IP, port
  - iEasyHydro HF API port (typically 5555)
  - Request firewall rule: allow inbound SSH from AWS server IP
- [ ] Generate SSH key pair on AWS server
  ```bash
  ssh-keygen -t ed25519 -f ~/.ssh/<org>_ieh_key -N ""
  ```
- [ ] Send public key to the hydromet IT team for installation
- [ ] Test SSH connection, then set up autossh + systemd service
  (follow `doc/prod/update_deployment_checklist.md` Option C steps 4-8)
- [ ] Update `.env`:
  ```
  IEASYHYDROHF_HOST=http://localhost:<local-tunnel-port>
  ieasyhydroforecast_ssh_to_iEH=false  # tunnel is managed by systemd, not by pipeline scripts
  ```

The SSH tunnel documentation in `update_deployment_checklist.md` is thorough
and generic — this section works well.

---

## Phase 8: Luigi Daemon & Pipeline Images

**Ref:** `doc/deployment.md` > Set up Luigi Daemon, `bin/README.md`

### 8.1 Pull Docker images

- [ ] Pull all required images
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/setup_docker.sh /data/<data_folder>/config/<env_file>
  ```
  Or pull manually:
  ```bash
  export TAG=latest  # or "local" depending on CI/CD branch
  docker pull mabesa/sapphire-pythonbaseimage:$TAG
  docker pull mabesa/sapphire-pipeline:$TAG
  docker pull mabesa/sapphire-preprunoff:$TAG
  docker pull mabesa/sapphire-linreg:$TAG
  docker pull mabesa/sapphire-postprocessing:$TAG
  docker pull mabesa/sapphire-dashboard:$TAG
  # If ML models enabled:
  docker pull mabesa/sapphire-prepgateway:$TAG
  docker pull mabesa/sapphire-ml:$TAG
  ```

### 8.2 Start Luigi daemon

- [ ] Start the daemon
  ```bash
  docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
  ```

- [ ] Verify
  ```bash
  docker ps | grep luigi-daemon
  curl -s http://localhost:8082/ > /dev/null && echo "OK" || echo "FAIL"
  ```

### 8.3 Test a pipeline run

- [ ] Run preprocessing as a smoke test
  ```bash
  bash bin/run_preprocessing_runoff.sh /data/<data_folder>/config/<env_file>
  ```

- [ ] Check Luigi UI at `http://<server-ip>:8082`

> `[DOC-GAP-9]` ✅ **Resolved** — `## Deployment order` section added to `doc/deployment.md` (services → Luigi → pipeline → dashboards).

---

## Phase 9: Dashboards & Reverse Proxy

**Ref:** `bin/README.md` > docker-compose-dashboards.yml

### 9.1 Start dashboards

- [ ] Start both dashboards
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml \
    --env-file /data/<data_folder>/config/<env_file> up -d
  ```

- [ ] Verify health
  ```bash
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5007/forecast_dashboard
  ```

### 9.2 Reverse proxy & HTTPS

> `[DOC-GAP-10]` ✅ **Resolved** — `### Reverse proxy and HTTPS` general recommendations added to `doc/deployment.md` (nginx vs Caddy, Let's Encrypt, WebSocket upgrade, gateway hardening).

Practical steps (not yet documented):

- [ ] Install nginx or Nginx Proxy Manager
- [ ] Configure DNS: `fc.pentad.<domain>` → server IP,
  `fc.decad.<domain>` → server IP
- [ ] Set up SSL certificates (Let's Encrypt)
- [ ] Configure reverse proxy rules:
  - `fc.pentad.<domain>` → `localhost:5006`
  - `fc.decad.<domain>` → `localhost:5007`
  - Include WebSocket upgrade headers for Panel/Bokeh

---

## Phase 10: Cron Jobs

**Ref:** `doc/deployment.md` > Set up cron job

- [ ] Create log directory
  ```bash
  mkdir -p /home/ubuntu/logs
  ```

- [ ] Edit crontab
  ```bash
  crontab -e
  ```

- [ ] Add schedule (adapt times for your timezone — replace `<tz_offset>`
  comments with local times):
  ```bash
  # m h  dom mon dow   command
  # ---------------------------------------------------------------------------
  # SAPPHIRE Forecast Tools Schedule (Times in UTC)
  # Adapt paths and timezone comments for your deployment.
  # ---------------------------------------------------------------------------

  # Log cleanup: delete logs older than 7 days
  0 2 * * * find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete

  # (1) Gateway Preprocessing at 03:00 UTC
  0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_gateway_$(date +\%Y\%m\%d).log 2>&1

  # (2) Pentadal Forecast at 04:00 UTC
  0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_pentadal_$(date +\%Y\%m\%d).log 2>&1

  # (3) Decadal Forecast at 05:00 UTC
  0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_decadal_$(date +\%Y\%m\%d).log 2>&1

  # (4) Long-term Forecast at 06:00 UTC on the 10th and 25th of each month.
  # The script self-gates via lt_schedule_query.py (±5 day tolerance on operational_issue_day).
  0 6 10,25 * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_longterm_$(date +\%Y\%m\%d).log 2>&1

  # (5) Daily maintenance via Luigi at 19:00 UTC (replaces individual maintenance cron jobs)
  # Luigi enforces dependency order: PrepRunoff + Gateway → LinReg → ML → PostProcessing → Frontend
  # ML concurrency is limited to 3 via Luigi resources.
  0 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_maintenance_$(date +\%Y\%m\%d).log 2>&1

  # (6) Periodic maintenance
  # Bimonthly long-term postprocessing (1st of odd months)
  0 17 1 1,3,5,7,9,11 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh long_term /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_longterm_$(date +\%Y\%m\%d).log 2>&1
  # Yearly skill recalculation (December 31)
  0 1 31 12 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh skill_recalc /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_skillrecalc_$(date +\%Y\%m\%d).log 2>&1
  # Yearly snow norm recalculation (August 31)
  0 2 31 8 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_snownorms_$(date +\%Y\%m\%d).log 2>&1
  ```

  > `[DOC-GAP-11]` ✅ **Resolved** — crontab in `doc/deployment.md` now uses `<data_folder>`/`<env_file>` placeholders and includes a timezone reference table.

- [ ] Verify
  ```bash
  crontab -l
  ```

---

## Phase 11: Monitoring

**Ref:** `doc/monitoring/forecast_tools_monitoring.md`

- [ ] Configure SMTP variables in `<env_file>`:
  ```
  SAPPHIRE_PIPELINE_SMTP_SERVER=<smtp_server>
  SAPPHIRE_PIPELINE_SMTP_PORT=<port>
  SAPPHIRE_PIPELINE_SMTP_USERNAME=<username>
  SAPPHIRE_PIPELINE_SMTP_PASSWORD=<password>
  SAPPHIRE_PIPELINE_SENDER_EMAIL=<sender>
  SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=<recipients>
  ```

- [ ] Set up Docker container monitoring (systemd service)
  - Follow `doc/monitoring/forecast_tools_monitoring.md`

- [ ] Set up dashboard log monitoring (systemd service)
  - Follow `doc/monitoring/forecast_tools_monitoring.md`

- [ ] Test email alerts

---

## Phase 12: Testing & Validation

**Ref:** `doc/deployment.md` > Testing the deployment

> `[DOC-GAP-12]` ✅ **Resolved** — Testing section in `doc/deployment.md` rewritten for headless servers (CLI checks, API curl, log grep, initialize workflow).

### Validation checklist

**Services health:**

- [ ] SAPPHIRE API gateway responds
  ```bash
  curl http://localhost:8000/health
  curl http://localhost:8000/health/ready
  ```

- [ ] Luigi daemon is running
  ```bash
  curl -s http://localhost:8082/ > /dev/null && echo "OK"
  ```

- [ ] Dashboards respond
  ```bash
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5007/forecast_dashboard
  ```

**Pipeline smoke test:**

- [ ] Run gateway preprocessing
  ```bash
  bash bin/run_preprocessing_gateway.sh /data/<data_folder>/config/<env_file>
  ```

- [ ] Run pentadal forecast
  ```bash
  bash bin/run_pentadal_forecasts.sh /data/<data_folder>/config/<env_file>
  ```

- [ ] Check Luigi UI for green tasks

- [ ] Check for errors in logs
  ```bash
  grep -iE "error|critical|exception" /home/ubuntu/logs/sapphire_*.log | tail -20
  ```

**Data verification:**

- [ ] Verify data reached the API
  ```bash
  curl "http://localhost:8000/api/preprocessing/runoff/?limit=5"
  curl "http://localhost:8000/api/postprocessing/forecasts/?limit=5"
  ```

- [ ] Check dashboards display data correctly (via browser or curl)

**Hindcast run (optional but recommended):**

- [ ] Start with restricted station selection (2-3 stations)
- [ ] Run hindcasts from a recent date to verify pipeline end-to-end
- [ ] Once verified, expand to full station selection

---

## Doc-Gap Summary

| ID | Gap | Severity | Where to fix |
|----|-----|----------|--------------|
| ~~DOC-GAP-1~~ | ~~SAPPHIRE services ports not in deployment.md~~ | ~~Low~~ | ✅ Fixed |
| ~~DOC-GAP-2~~ | ~~No locale/translation setup instructions~~ | ~~Medium~~ | ✅ Fixed (rudimentary .po/.mo workflow + add-a-language steps in `doc/configuration.md#localization`) |
| DOC-GAP-3 | ~~**SAPPHIRE services missing from deployment.md entirely**~~ | **Critical** | ✅ Fixed (`doc/deployment.md` new section) |
| ~~DOC-GAP-4~~ | ~~No guidance on sapphire/.env values~~ | ~~Medium~~ | ✅ Fixed (sapphire/.env.example commented + sapphire/README.md Environment setup expanded) |
| ~~DOC-GAP-5~~ | ~~**No "fresh deployment" path for services — DB init required**~~ | ~~**High**~~ | ✅ Fixed (initialize target implemented; Phase 4.3 updated) |
| ~~DOC-GAP-6~~ | ~~**No minimal .env variable reference**~~ | ~~**High**~~ | ✅ Fixed (`.env variable reference` section added to `doc/configuration.md`) |
| ~~DOC-GAP-7~~ | ~~iEasyHydro HF SDK vars undocumented~~ | ~~Medium~~ | ✅ Fixed (covered in `doc/configuration.md` reference table + Add-ons) |
| ~~DOC-GAP-8~~ | ~~Path convention explanation buried~~ | ~~Low~~ | ✅ Fixed (tree diagram + worked example added to `doc/configuration.md`) |
| DOC-GAP-9 | ~~**Deployment order not documented**~~ | **High** | ✅ Fixed (`doc/deployment.md`) |
| ~~DOC-GAP-10~~ | ~~No reverse proxy / HTTPS instructions~~ | ~~Medium~~ | ✅ Fixed (general-recommendations section `doc/deployment.md#reverse-proxy-and-https`; nginx vs Caddy, WebSocket upgrade, cert auto-renew, expose-vs-hide guidance) |
| ~~DOC-GAP-11~~ | ~~Crontab template is deployment-specific~~ | ~~Low~~ | ✅ Fixed (crontab placeholders + TZ reference table in deployment.md) |
| ~~DOC-GAP-12~~ | ~~Testing section outdated~~ | ~~Low~~ | ✅ Fixed (rewrote `doc/deployment.md#testing-the-deployment` for headless server: CLI health checks, API curl, log grep, initialize workflow) |
| ~~DOC-GAP-13~~ | ~~**Deprecated `deploy_sapphire_forecast_tools.sh` still recommended in `doc/deployment.md`**~~ | ~~**Medium**~~ | ✅ Fixed (deprecated-script references removed from deployment.md) |

### Priority for pre-deployment fixes

1. **DOC-GAP-3 + DOC-GAP-9** (critical): Add SAPPHIRE services section and
   deployment order to `doc/deployment.md` — without this, anyone following
   the guide will miss the entire API stack
2. **DOC-GAP-6** (high): Create the minimal .env variable reference
3. Everything else can be fixed during or after deployment

---

## Deployment Log

*Use this section to record issues, deviations, and learnings during the
actual deployment. These notes feed back into doc improvements.*

| Date | Phase | Issue / Observation | Resolution | Doc update needed? |
|------|-------|---------------------|------------|--------------------|
| | | | | |
| | | | | |
| | | | | |
| | | | | |

---

> `[DOC-GAP-13]` ✅ **Resolved** — deprecated `deploy_sapphire_forecast_tools.sh` / `run_sapphire_forecast_tools.sh` references removed from `doc/deployment.md`.

---

## Resolved Doc-Gaps

Changes already applied to the docs as part of deployment preparation.

| ID | What was done | Commit / branch |
|----|---------------|-----------------|
| DOC-GAP-1 | Added structured port table with SAPPHIRE services ports and security guidance to `doc/deployment.md` > "Configuring your server" | `develop_long_term_fix_api_postprocessing_forecasts` |
| DOC-GAP-3 + DOC-GAP-9 | Added "Deployment order" and "SAPPHIRE services (API stack)" sections to doc/deployment.md | <branch> |
| DOC-GAP-4 | Added inline comments and placeholder values to sapphire/.env.example; expanded sapphire/README.md Environment setup section with per-group guidance | <branch> |
| DOC-GAP-5 | Added `initialize` target (see review_gi_draft_infra_new_deployment_initialization.md) + Phase 4.3 updated in this plan | <branch> |
| DOC-GAP-13 | Removed deprecated deploy_sapphire_forecast_tools.sh / run_sapphire_forecast_tools.sh references from doc/deployment.md | <branch> |
| DOC-GAP-11 | Generalised crontab in doc/deployment.md (placeholders + timezone reference table) | <branch> |
| DOC-GAP-6 | Added categorised `.env variable reference` with minimal deployment profile to doc/configuration.md | <branch> |
| DOC-GAP-7 | iEasyHydro HF SDK variables now documented in doc/configuration.md reference table and Add-ons section | <branch> |
| DOC-GAP-8 | Added directory-tree diagram and worked example for the `../../../<data_folder>/` path convention in doc/configuration.md | <branch> |
| DOC-GAP-12 | Rewrote the Testing section in doc/deployment.md for headless server operations (CLI checks, API curl, log grep, initialize workflow for first-time deployments); removed Docker Desktop / GUI / .yaml references and stale hindcast dates | <branch> |
| DOC-GAP-2 | Added locale directory layout, .po/.mo explanation, and an "Adding a new language" workflow (copy → edit msgstr → msgfmt → set ieasyforecast_locale → restart) to doc/configuration.md, emphasising the operational locale lives under `<data_folder>/config/locale/`, not in the repo | <branch> |
| DOC-GAP-10 | General reverse-proxy + HTTPS recommendations in doc/deployment.md (when needed, what to expose vs hide, nginx vs Caddy, Let's Encrypt, WebSocket upgrade requirement, optional API-key/rate-limit gateway hardening, HTTP→HTTPS redirect) | <branch> |

---

## Post-Deployment: Documentation Graduation

After the Uzbek deployment validates this checklist, graduate it from
`doc/plans/` to `doc/prod/` and consolidate with the existing update
checklist. Target structure:

```
doc/prod/
├── deployment_new.md              ← this file (first-time deployment)
├── deployment_update.md           ← current update_deployment_checklist.md
└── reference/
    └── ieasyhydro_hf_connectivity.md   ← extracted shared reference
```

**Design principles:**
- Each checklist is self-contained and sequential — no jumping between docs
- Shared complex procedures (like iEasyHydro HF connectivity) live in
  `reference/` and are linked from both checklists with a brief inline
  summary (the decision tree) sufficient for repeat users
- Non-shared steps stay in each checklist even if they involve the same
  system (e.g., crontab creation vs crontab diffing are different procedures)

**Known issues in `update_deployment_checklist.md` to fix during graduation:**
1. Hardcoded `kghm` paths — replace with `<data_folder>` / `<env_file>`
   placeholders (same issue as DOC-GAP-11)
2. Crontab in §2.5 uses old individual maintenance scripts
   (`daily_update_sapphire_frontend.sh`, `daily_ml_maintenance.sh`, etc.)
   — should use the consolidated `run_daily_maintenance.sh`
3. Section numbering: 2.2 → 2.4 → 2.3 (reordering artifact)
4. `deployment.md` reference doc also needs updates to stay in sync — the
   Prerequisites section has been rewritten; the step-by-step section below
   it still needs work
5. Add cross-references from `deployment.md` to the `doc/prod/` checklists.
   `deployment.md` is the reference doc (what + why); the checklists are the
   action docs (do this now). After graduation, `deployment.md` should link
   to both checklists — e.g., a "Next steps" or "Operational checklists"
   section at the end pointing to `doc/prod/deployment_new.md` for first-time
   setup and `doc/prod/deployment_update.md` for updates
