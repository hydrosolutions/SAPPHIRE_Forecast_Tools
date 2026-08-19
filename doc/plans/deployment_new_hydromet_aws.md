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
  - Outbound SMTP (587/tcp): permitted by default on EC2 ("all outbound"). If the account uses a restricted outbound policy, allow TCP 587 for Phase 11 email alerts. AWS throttles outbound port 25 at the EC2 level — the monitoring scripts use STARTTLS on 587, so this is only a concern if you later customise them.
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
- [ ] Install msmtp (only needed if Phase 11 monitoring email alerts will be enabled; the two monitoring scripts invoke `msmtp` directly and their systemd services fail to start without it)
  ```bash
  sudo apt-get install -y msmtp
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

### 4.1 Note the services-layer variables (file itself is created in Phase 5)

The SAPPHIRE services read their configuration from the same external env file the pipeline uses (`/data/<data_folder>/config/<env_file>`). The file is created in Phase 5 (by copying `apps/config/.env` as a template); this step only inventories the variables the services need so they are filled in when you edit that file. `sapphire/.env.example` is a **reference only** — do not commit edits to it for a deployment.

Services-layer variables to include in `<env_file>` (added to the pipeline variables that Phase 5 copies from `apps/config/.env`):

```bash
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<generate-strong-password>       # openssl rand -base64 24
PREPROCESSING_DB=preprocessing_db
POSTPROCESSING_DB=postprocessing_db
USER_DB=user_db
AUTH_DB=auth_db
JWT_SECRET_KEY=<generate-strong-random-secret>     # openssl rand -hex 32
# Internal Docker-network service URLs — keep as-is unless you run a service outside docker compose
PREPROCESSING_API_URL=http://preprocessing-api:8002
POSTPROCESSING_API_URL=http://postprocessing-api:8003
USER_API_URL=http://user-api:8004
AUTH_API_URL=http://auth-api:8005
```

Per-variable explanation: `sapphire/README.md` > `Environment setup` > `What to set`. Categorised reference (required / required-if / optional): `doc/configuration.md` > `.env variable reference`.

> `[DOC-GAP-4]` ✅ **Resolved** — `sapphire/.env.example` annotated with placeholder values and inline comments; `sapphire/README.md` Environment setup expanded with a per-group walkthrough.

### 4.2 Start the services

This step runs AFTER Phase 5 (`<env_file>` must exist and contain both the pipeline and services-layer variables). Come back here after completing Phase 5.

- [ ] Start all services, passing the external env file explicitly
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools/sapphire
  docker compose --env-file /data/<data_folder>/config/<env_file> up -d
  ```

- [ ] Wait for databases to be healthy
  ```bash
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

**Refs:**
- `doc/configuration.md` > `.env variable reference` — Minimal Profile, full categorised table, Add-ons.
- `sapphire/README.md` > `Environment setup` > `What to set` — services-side guidance.
- `apps/config/.env_develop` — reference example (variable names and structure). **Do not `cp` it to your data folder** — build `<env_file>` from the documented Minimal Profile instead. The in-repo files under `apps/config/` are examples only.

The deployment's real env file is `/data/<data_folder>/config/<env_file>`. It holds **both** pipeline variables and the services-layer variables from Phase 4.1; Phase 4.2 passes the same file to `docker compose --env-file`.

- [ ] Create `<env_file>` from the Minimal Deployment Profile
      (LR-only, manual sites, no iEH HF, no gateway, no ML/CM — ~33 variables) in
      `doc/configuration.md` > `Minimal deployment profile`. Paste the block into
      `/data/<data_folder>/config/<env_file>` and fill in the `<...>` placeholders.

- [ ] Append the services-layer block listed in Phase 4.1 to the same file
      (POSTGRES_USER, POSTGRES_PASSWORD, the four DB names, JWT_SECRET_KEY, the four
      service URLs).

- [ ] Enable the add-ons your deployment needs. For each, follow the corresponding
      bullet under `doc/configuration.md` > `Add-ons — what to flip on when you
      need more`:
      - iEasyHydro HF connectivity (if the hydromet runs iEH HF)
      - SAPPHIRE Data Gateway + ML models
      - Long-term forecasts
      - Conceptual-model (TopoPyScale/FSM)
      - SMTP / email alerts for pipeline monitoring

- [ ] Required variables to set explicitly (double-check before moving on):

  | Variable | What to set | Category |
  |----------|-------------|----------|
  | `ieasyhydroforecast_organization` | `<org>` (e.g. `uzhm`) | Required |
  | All `ieasyforecast_*_path` and `ieasyhydroforecast_*_path` | `../../../<data_folder>/...` — see tree diagram in `doc/configuration.md` | Required |
  | `SAPPHIRE_API_ENABLED` | **`true`** — CSV-only mode is deprecated; almost every pipeline read/write goes through the preprocessing and postprocessing APIs | Required |
  | `SAPPHIRE_API_URL` | `http://localhost:8000` (default) or wherever the api-gateway listens | Required |
  | `ieasyhydroforecast_connect_to_iEH` | `True` if the hydromet has iEasyHydro HF, else `False` | Required |
  | `ieasyhydroforecast_run_ML_models` | `true` / `false` — ML container is optional | Required |
  | `ieasyhydroforecast_run_CM_models` | `true` / `false` — conceptual-model container is optional | Required |
  | `ieasyhydroforecast_locale` | `ru_KG`, `en_CH`, or a locale you added per `doc/configuration.md#localization` | Required |
  | `ieasyforecast_country_borders_file_name` | GADM shapefile for your country (under `<data_folder>/GIS/`) | Required |
  | `ieasyhydroforecast_backend_docker_image_tag` | `local` or `latest` | Required |
  | `ieasyhydroforecast_frontend_docker_image_tag` | `local` or `latest` | Required |
  | `ieasyhydroforecast_START_DATE` | `YYYY-MM-DD` — the Phase 4.3 `initialize` path exits with an error if this is missing. Can be omitted after initialization has run. | Required-if (initialize) |
  | `IEASYHYDROHF_HOST` | iEH HF endpoint (e.g. `http://localhost:<tunnel-port>` with Phase 7's SSH tunnel) | Required-if `connect_to_iEH=True` |
  | `IEASYHYDROHF_USERNAME` | iEH HF user | Required-if `connect_to_iEH=True` |
  | `IEASYHYDROHF_PASSWORD` | iEH HF password | Required-if `connect_to_iEH=True` |
  | `ieasyhydroforecast_ssh_to_iEH` | `false` when the tunnel is managed by systemd (see Phase 7) | Required-if tunnel |
  | `ieasyhydroforecast_API_KEY_GATEAWAY` | SAPPHIRE Data Gateway API key | Required-if gateway used |
  | `SAPPHIRE_PIPELINE_SMTP_*` + `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS` | Alert settings | Required-if monitoring (`doc/monitoring/forecast_tools_monitoring.md`) |

  For the exhaustive list categorised by module see `doc/configuration.md` > `.env variable reference`.

  > `[DOC-GAP-6]` ✅ **Resolved** — categorised `.env variable reference` with minimal deployment profile added to `doc/configuration.md`.

  > `[DOC-GAP-7]` ✅ **Resolved** — `IEASYHYDROHF_HOST`/`USERNAME`/`PASSWORD` covered in the `doc/configuration.md` reference table and Add-ons section.

- [ ] Replace any remaining `<...>` placeholders with your actual values. Quick sanity check:
  ```bash
  grep -n '<[a-z_]\+>' /data/<data_folder>/config/<env_file> || echo "No unresolved placeholders."
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

**Refs:** `doc/deployment.md` > `Deployment order`; `bin/README.md` > `Shell Script Patterns` and `Cron Schedule`.

**Prerequisites from earlier phases** (8.3's smoke test will fail without them — check each before starting):
- Phase 4.2 — SAPPHIRE services up (`curl http://localhost:8000/health/ready` returns 200).
- Phase 4.3 — preprocessing database has historical runoff data (Path A or Path B).
- Phase 5 — external `<env_file>` contains both pipeline and services-layer variables.
- Phase 6 — station config JSON files and discharge Excel files in place.
- Phase 7 — iEH HF tunnel running, only if `ieasyhydroforecast_connect_to_iEH=True`.

### 8.1 Pull Docker images

- [ ] Run the setup script. It reads `ieasyhydroforecast_backend_docker_image_tag` from your env file and pulls images tagged accordingly.
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/setup_docker.sh /data/<data_folder>/config/<env_file>
  ```

  > **DESTRUCTIVE — do not run blindly on an existing deployment.** `setup_docker.sh` first calls `bin/utils/clean_docker.sh --execute`, which removes **every non-nginx Docker container and image on the host**. This includes the SAPPHIRE services stack, the Luigi daemon, the dashboards, and any other Docker workloads running alongside SAPPHIRE. On a brand-new server this is the intended behaviour. On an existing deployment the script will:
  > - stop and remove all running sapphire containers (brief service outage);
  > - delete the cached images, forcing a full re-pull of the tagged versions from Docker Hub;
  > - remove anonymous volumes attached to the removed containers. Named volumes — including the four SAPPHIRE DB volumes — survive, so **DB data is not lost**, but any scratch state kept inside a container is.
  >
  > If you only need to add or refresh specific images on an existing deployment, **skip the script** and pull individually using the tag from your env file:
  > ```bash
  > TAG=$(grep -E '^ieasyhydroforecast_backend_docker_image_tag=' /data/<data_folder>/config/<env_file> | cut -d= -f2)
  > docker pull mabesa/sapphire-<name>:"${TAG:-latest}"
  > ```

- [ ] Confirm the images that should now be on the server:

  | Image | Pulled by `setup_docker.sh`? | Condition |
  |-------|------------------------------|-----------|
  | `mabesa/sapphire-pythonbaseimage` | yes | always |
  | `mabesa/sapphire-preprunoff` | yes | always |
  | `mabesa/sapphire-linreg` | yes | always |
  | `mabesa/sapphire-postprocessing` | yes | always |
  | `mabesa/sapphire-rerun` | yes | always |
  | `mabesa/sapphire-prepgateway` | yes | `run_ML_models=true` OR `run_CM_models=true` |
  | `mabesa/sapphire-ml` | yes | `run_ML_models=true` |
  | `mabesa/sapphire-conceptmod` | yes | `run_CM_models=true` |
  | `mabesa/sapphire-pipeline` | **no** | used by every Luigi-orchestrated service — Docker auto-pulls it on first `docker compose run` in 8.3, provided Docker Hub is reachable |
  | `mabesa/sapphire-lt-forecasting` | **no** | required if you will run `run_long_term_forecasts.sh` — pull it manually (see below) |
  | `mabesa/sapphire-dashboard` | no — belongs to the sapphire stack | pulled by Phase 4.2 / `restart_sapphire_stack.sh` |

  If long-term forecasting is part of this deployment, pull the image explicitly using the tag from your env file:
  ```bash
  TAG=$(grep -E '^ieasyhydroforecast_backend_docker_image_tag=' /data/<data_folder>/config/<env_file> | cut -d= -f2)
  docker pull mabesa/sapphire-lt-forecasting:"${TAG:-latest}"
  ```

  Sanity check:
  ```bash
  docker images 'mabesa/sapphire-*' --format 'table {{.Repository}}\t{{.Tag}}'
  ```

### 8.2 Start the Luigi daemon

- [ ] Start the daemon (persistent — `restart: unless-stopped` in the compose, so it survives host reboots and container crashes):
  ```bash
  docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
  ```

  The daemon's image `sapphire/luigi-daemon` is **built locally** from `bin/luigi-daemon.Dockerfile` on first start — expect the first invocation to take a minute or two. Subsequent starts are instant.

- [ ] Verify the daemon is up and the scheduler API responds:
  ```bash
  docker ps --filter "name=luigi-daemon" --format "table {{.Names}}\t{{.Status}}"
  curl -sf http://localhost:8082/ >/dev/null && echo OK || echo FAIL
  ```
  `OK` from the curl confirms the scheduler is accepting HTTP — the pipeline runners (daily/pentadal/decadal scripts) connect to this endpoint.

### 8.3 Test a pipeline run

This is the first end-to-end check that services, env, station config, and (if applicable) iEH tunnel all work together. Errors here almost always point at one of the Prerequisites above.

- [ ] Run preprocessing as a smoke test. The script uses `docker compose run --rm preprocessing-runoff`, so the pipeline container is one-shot and the Luigi daemon (started in 8.2, left running) orchestrates task dependencies.
  ```bash
  bash bin/run_preprocessing_runoff.sh /data/<data_folder>/config/<env_file>
  echo "Exit code: $?"
  ```
  Exit code `0` means the Luigi task tree completed. A non-zero exit usually indicates:
  - an unset or wrong env variable — re-check against Phase 5;
  - a SAPPHIRE service unreachable — re-run the 4.2 health checks;
  - an iEH HF tunnel that isn't up — re-check Phase 7.

- [ ] Confirm the pipeline actually wrote data to the preprocessing API (the smoke test's real purpose is to exercise the `apps → api-gateway → preprocessing service → DB` path). The gateway gates data endpoints with an API key when `API_KEY_ENABLED=true` in your env file — pass the key in the `x-api-key` header.
  ```bash
  # If API_KEY_ENABLED=false (the default), no auth needed:
  curl -s 'http://localhost:8000/api/preprocessing/runoff/?limit=1'

  # If API_KEY_ENABLED=true:
  API_KEY=$(grep -E '^API_KEY=' /data/<data_folder>/config/<env_file> | cut -d= -f2)
  curl -s -H "x-api-key: ${API_KEY}" 'http://localhost:8000/api/preprocessing/runoff/?limit=1'
  ```
  A non-empty JSON array means `sapphire_api_client` wrote at least one row. An empty array after the runoff script exited 0 usually means your station selection is empty or `SAPPHIRE_API_ENABLED=false` in the env file. A `401 Unauthorized` response means the API key is wrong or the header is missing.

- [ ] Optionally inspect the Luigi task graph. On a headless server the daemon's JSON scheduler API is enough — the curl in 8.2 already confirms it. From a browser-capable workstation on the same network, open `http://<server-ip>:8082/` for a graphical view.

> `[DOC-GAP-9]` ✅ **Resolved** — `## Deployment order` section added to `doc/deployment.md` (services → Luigi → pipeline → dashboards).

---

## Phase 9: Dashboards & Reverse Proxy

**Ref:** `bin/README.md` > docker-compose-dashboards.yml

### 9.1 Start dashboards

**Migration state:** the pentadal dashboard (port 5006) was moved into `sapphire/docker-compose.yml` during PR #332 and comes up with the rest of the sapphire stack (Phase 4.2). Only the decadal dashboard (port 5007) is still served from the legacy `bin/docker-compose-dashboards.yml`. See `doc/deployment.md` > `Dashboards` for the full picture.

- [ ] Confirm the pentadal dashboard is already running (started by Phase 4.2)
  ```bash
  docker ps --filter "name=sapphire-dashboard" --format "table {{.Names}}\t{{.Status}}"
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/forecast_dashboard   # expect 200
  ```

  If it is not running, `bash bin/restart_sapphire_stack.sh /data/<data_folder>/config/<env_file>` brings the integrated stack (including the pentadal dashboard) up cleanly — it stops the legacy dashboards compose first to free port 5006.

- [ ] Start only the decadal dashboard from the legacy compose (specify the service name to avoid starting `pentaddashboard` and colliding on port 5006)
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml \
    --env-file /data/<data_folder>/config/<env_file> up -d decaddashboard
  ```

- [ ] Verify health of both
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/forecast_dashboard   # pentadal, from sapphire stack
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5007/forecast_dashboard   # decadal, from legacy compose
  ```

### 9.2 Reverse proxy & HTTPS

> `[DOC-GAP-10]` ✅ **Resolved** — `### Reverse proxy and HTTPS` general recommendations added to `doc/deployment.md` (nginx vs Caddy, Let's Encrypt, WebSocket upgrade, gateway hardening).

Skip this section if the deployment is LAN-only — users can reach the dashboards directly at `http://<server>:5006/forecast_dashboard` and `http://<server>:5007/forecast_dashboard`. For any internet-facing deployment (AWS and friends), work through the checklist below and cross-reference `doc/deployment.md` > `Reverse proxy and HTTPS` for the detailed recipe.

- [ ] **DNS** — add A records for `fc.pentad.<base_url>` and `fc.decad.<base_url>` pointing at the server's public IP. Both must resolve before certificate acquisition (ACME verifies by connecting back to your host).
- [ ] **Install a reverse proxy** — pick whichever your IT team already supports:
  - `nginx` + `certbot` for TLS (traditional Ubuntu path)
  - `Caddy` (single binary, automatic HTTPS)
- [ ] **Obtain TLS certificates** via Let's Encrypt (`certbot --nginx` or Caddy's built-in ACME). Both tools install renewal timers automatically.
- [ ] **Configure proxy routes** — two hostnames, two backends:
  - `https://fc.pentad.<base_url>/forecast_dashboard` → `localhost:5006` (served by the sapphire stack from Phase 4.2)
  - `https://fc.decad.<base_url>/forecast_dashboard` → `localhost:5007` (served by `bin/docker-compose-dashboards.yml` from 9.1)
- [ ] **Add the WebSocket upgrade** to each route. nginx needs `proxy_http_version 1.1` + `Upgrade` / `Connection` headers on the dashboard location block; Caddy does this by default. Panel/Bokeh dashboards appear loaded but stop responding to clicks without the upgrade.
- [ ] **Set the WebSocket-origin env vars** in `<env_file>` and restart the dashboards so Panel's origin check accepts the new public hostnames:
  ```bash
  ieasyhydroforecast_url_pentad=https://fc.pentad.<base_url>
  ieasyhydroforecast_url_decad=https://fc.decad.<base_url>
  ```
  These can be set directly in the env file, or derived from a base `ieasyhydroforecast_url` by `bin/utils/common_functions.sh` (the derivation hard-codes org prefixes for existing hydromets — check the script before relying on it for a new org). Multiple origins are supported as a comma-separated list.
- [ ] **Force HTTPS** — add a permanent redirect from `http://` to `https://` for both hostnames (one line per proxy flavour — see `doc/deployment.md` > `Reverse proxy and HTTPS` > "Force HTTPS").
- [ ] **Verify end-to-end before announcing the URLs to users**:
  - Load each `https://fc.*.<base_url>/forecast_dashboard` in a browser; confirm a valid TLS certificate is shown.
  - Open DevTools → Network → WS tab, reload the page, confirm the WebSocket connection stays open for at least a minute (no repeated reconnects).
  - Exercise one interactive element (station select, date change) to confirm data round-trips through the proxy end-to-end.

---

## Phase 10: Cron Jobs

**Ref:** `doc/deployment.md` > `Set up cron job` (timezone reference table, log-rotation rationale, full commentary on each scheduled job).

**Prerequisites** (re-confirm the earlier phases' health checks before enabling cron — a failing cron job with a broken prerequisite is much harder to diagnose than a failing manual run):
- Phase 4.2 — SAPPHIRE services healthy: `curl -sf http://localhost:8000/health/ready` returns 200.
- Phase 5 — `<env_file>` populated with both pipeline and services-layer variables.
- Phase 6 — station config JSONs and historical discharge Excel files in place.
- Phase 8.2 — Luigi daemon running: `curl -sf http://localhost:8082/ >/dev/null && echo OK`.
- Phase 7 — iEH HF tunnel up (only if `ieasyhydroforecast_connect_to_iEH=True`).

- [ ] Create log directory
  ```bash
  mkdir -p /home/ubuntu/logs
  ```

  Each cron job below writes to a timestamped file (`sapphire_<job>_YYYYMMDD.log`), and the first cron line deletes anything in `/home/ubuntu/logs/` matching `sapphire_*.log` older than 7 days — this keeps the log directory bounded. If you change the log path or filename pattern, update **both** the `find` command and each job's `>> /home/ubuntu/logs/...` redirect to match.

- [ ] Edit crontab
  ```bash
  crontab -e
  ```

- [ ] Add the schedule below. Times are UTC; use the timezone reference table in `doc/deployment.md` > `Set up cron job` to pick an operational window that fits your hydromet (pentadal forecast usually runs in the morning local time, daily maintenance in the evening local time).
  ```bash
  # m h  dom mon dow   command
  # ---------------------------------------------------------------------------
  # SAPPHIRE Forecast Tools Schedule (Times in UTC)
  # Replace <data_folder> and <env_file> with the values for your deployment.
  # ---------------------------------------------------------------------------

  # Log cleanup: delete logs older than 7 days
  0 2 * * * find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete

  # (1) Gateway Preprocessing at 03:00 UTC
  0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_gateway_$(date +\%Y\%m\%d).log 2>&1

  # (2) Pentadal Forecast at 04:00 UTC
  0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_pentadal_$(date +\%Y\%m\%d).log 2>&1

  # (3) Decadal Forecast at 05:00 UTC
  0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_decadal_$(date +\%Y\%m\%d).log 2>&1

  # (4) Long-term Forecast at 06:00 UTC on THIS deployment's issue day(s).
  # Replace <lt_issue_day> with the "operational_issue_day" values found in
  # <data_folder>/config/long_term_configs/*.json — tjhm = 1, kghm = 10,25.
  # Do NOT copy another deployment's days: a cron day 6-10 days from the issue
  # day is admitted by lt_schedule_query.py (ISSUE_DAY_TOLERANCE = 10, :52) and
  # then refused by lt_utils.py:202 (>5 days), so the run writes nothing and
  # still exits 0 (LTF-007). Scheduling on the issue day itself is the only
  # safe choice.
  0 6 <lt_issue_day> * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_longterm_$(date +\%Y\%m\%d).log 2>&1

  # (5) Daily maintenance via Luigi at 19:00 UTC (replaces individual maintenance cron jobs)
  # Luigi enforces dependency order: PrepRunoff + Gateway → LinReg → ML → PostProcessing → Frontend
  # ML concurrency is limited to 3 via Luigi resources.
  0 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_maintenance_$(date +\%Y\%m\%d).log 2>&1

  # (6) Periodic maintenance
  # Bimonthly long-term postprocessing (1st of odd months)
  0 17 1 1,3,5,7,9,11 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh long_term /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_longterm_$(date +\%Y\%m\%d).log 2>&1
  # Yearly skill recalculation (December 31)
  0 1 31 12 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh skill_recalc /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_skillrecalc_$(date +\%Y\%m\%d).log 2>&1
  # Yearly snow norm recalculation (January 1)
  # Owner decision 2026-08-19: run at the END of the snow year (31 August),
  # before the new accumulation season, rather than 1 January which would move
  # the norms mid-season.
  0 2 31 8 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_snownorms_$(date +\%Y\%m\%d).log 2>&1
  # Yearly monthly discharge norm recalculation from iEH HF SDK (January 1)
  0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh monthly_norms /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_monthlynorms_$(date +\%Y\%m\%d).log 2>&1
  ```

  > `[DOC-GAP-11]` ✅ **Resolved** — crontab in `doc/deployment.md` now uses `<data_folder>`/`<env_file>` placeholders and includes a timezone reference table.

- [ ] Verify
  ```bash
  crontab -l
  ```

- [ ] **Wire cron to your alerting (optional).** If you configure SMTP in Phase 11, failures of any of the jobs scheduled above will email the recipients listed in `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS`. Without SMTP configured, check the per-job log files in `/home/ubuntu/logs/sapphire_*.log` manually (grep for `ERROR`/`CRITICAL` lines from the Testing section's log-grep one-liner).

---

## Phase 11: Monitoring

**Refs:**
- `doc/monitoring/forecast_tools_monitoring.md` — systemd unit templates and operational walkthrough.
- `bin/monitoring/` — the actual scripts: `docker.sh` (container health) and `docker_log_watcher.sh` (dashboard log scanner).

**Phase 11 is optional but strongly recommended.** Without it, cron-job failures (Phase 10) and container crashes are invisible until someone reads the logs. The systemd daemons here and the cron-level alerts from `apps/pipeline/src/notification_manager.py` share the same `SAPPHIRE_PIPELINE_*` SMTP variables — configure once, covers both.

**Prerequisites:**
- Phase 2 — `msmtp` installed: `which msmtp` returns a path.
- Phase 4.2 / 9 — Docker containers running (`docker ps` lists the sapphire containers to monitor).
- Phase 5 — `<env_file>` contains the 6 `SAPPHIRE_PIPELINE_*` variables below.
- `sudo` access — systemd unit installation writes to `/etc/systemd/system/` and `/var/log/docker_monitor/` both need elevation.

### 11.1 Confirm msmtp is installed

```bash
which msmtp && msmtp --version | head -1
```
If missing (Phase 2 should have covered this): `sudo apt-get install -y msmtp`.

### 11.2 Set SMTP variables in `<env_file>`

Append to `/data/<data_folder>/config/<env_file>` (or confirm present):
```
SAPPHIRE_PIPELINE_SMTP_SERVER=<smtp_server>
SAPPHIRE_PIPELINE_SMTP_PORT=587                      # STARTTLS; 465/25 not supported by the scripts as-is
SAPPHIRE_PIPELINE_SMTP_USERNAME=<username>
SAPPHIRE_PIPELINE_SMTP_PASSWORD=<password>
SAPPHIRE_PIPELINE_SENDER_EMAIL=<sender>
SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=<recipient1>,<recipient2>   # comma-separated
```

Notes:
- Port 587 is **required** — `bin/monitoring/docker.sh` and `docker_log_watcher.sh` hardcode `--tls=on --tls-starttls=on`. Providers using implicit TLS (port 465) or no TLS (port 25) need the script edits.
- Gmail as provider: 2FA must be enabled on the account and an App Password generated as the SMTP password (regular Google password will not work).
- All 6 variables are required by the systemd daemons — any missing one causes `exit 1` before the service connects. The pipeline-level `notification_manager.py` is more lenient and silently skips, but that's not the path Phase 11 uses.

### 11.3 Standalone SMTP smoke test

Before touching systemd, confirm `msmtp` can send via your provider. If this fails the systemd daemons will fail the same way:
```bash
set -a; source /data/<data_folder>/config/<env_file>; set +a

printf "From: %s\nTo: %s\nSubject: SAPPHIRE Phase 11 smoke test\n\nFrom %s at %s\n" \
  "$SAPPHIRE_PIPELINE_SENDER_EMAIL" \
  "${SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS%%,*}" \
  "$(hostname)" "$(date -u)" | \
msmtp --host="$SAPPHIRE_PIPELINE_SMTP_SERVER" \
      --port="$SAPPHIRE_PIPELINE_SMTP_PORT" \
      --tls=on --tls-starttls=on --auth=on \
      --user="$SAPPHIRE_PIPELINE_SMTP_USERNAME" \
      --passwordeval="echo \"$SAPPHIRE_PIPELINE_SMTP_PASSWORD\"" \
      --from="$SAPPHIRE_PIPELINE_SENDER_EMAIL" \
      "${SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS%%,*}"
```
The first recipient should receive the message within seconds.

As an alternative, the pipeline's smoke script sends a test through the pipeline notification path (edit the hardcoded recipient first):
```bash
# NOTE: hardcoded recipient — edit apps/pipeline/tests/test_email_notification.py before running
ieasyhydroforecast_env_file_path=/data/<data_folder>/config/<env_file> \
  python apps/pipeline/tests/test_email_notification.py
```

### 11.4 Install the systemd units

Follow `doc/monitoring/forecast_tools_monitoring.md` for the two unit templates. The key items to customise:

- `Environment="DOCKER_MONITOR_ENV_PATH=/data/<data_folder>/config/<env_file>"` — tells the scripts where to find the SMTP vars. Without this, the scripts fall back to `./apps/config/.env` (which does not exist at systemd's working directory) and exit immediately.
- `ExecStart=/data/SAPPHIRE_Forecast_Tools/bin/monitoring/docker.sh` for the container monitor.
- `ExecStart=/data/SAPPHIRE_Forecast_Tools/bin/monitoring/docker_log_watcher.sh` for the log watcher.

Make both scripts executable, then enable + start:
```bash
chmod +x /data/SAPPHIRE_Forecast_Tools/bin/monitoring/docker.sh
chmod +x /data/SAPPHIRE_Forecast_Tools/bin/monitoring/docker_log_watcher.sh

sudo systemctl daemon-reload
sudo systemctl enable --now docker-monitor.service
sudo systemctl enable --now docker-log-watcher.service
```

### 11.5 Sensitive-data caveat

`bin/monitoring/docker_log_watcher.sh` attaches the **last 1000 lines of the affected container's logs** to each alert email. Dashboard containers may log station codes, discharge values, or user session information. Email is an uncontrolled channel for that data. Before enabling the log watcher:
- Confirm with your hydromet that attaching container logs to external email is acceptable for this deployment.
- If not: reduce dashboard log verbosity to keep sensitive data out of the logs, or customise the script to send a message-only notification.

### 11.6 Verify end-to-end

```bash
systemctl status docker-monitor.service
systemctl status docker-log-watcher.service
journalctl -u docker-monitor.service -n 20
journalctl -u docker-log-watcher.service -n 20
```
Both services should report `Active: active (running)`.

Trigger a synthetic alert and watch for the email:
```bash
# Emit an error line into a dashboard container's stderr
docker exec <dashboard-container-name> sh -c 'echo "ERROR: synthetic test, please ignore" >&2'
```
The recipient should receive an alert within ~30 seconds. Alert timestamps use the server's clock (UTC on AWS) — adjust expectations if your hydromet is in a different time zone.

---

## Phase 12: Testing & Validation

**Ref:** `doc/deployment.md` > Testing the deployment

> `[DOC-GAP-12]` ✅ **Resolved** — Testing section in `doc/deployment.md` rewritten for headless servers (CLI checks, API curl, log grep, initialize workflow).

### Validation checklist

Work through these in order. Most checks here are re-runs of health probes from earlier phases — the point is to confirm everything that worked in isolation still works once the whole stack is wired up.

**Services health:**

- [ ] All sapphire containers up
  ```bash
  docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}"
  ```
  Expect the five API containers (`sapphire-api-gateway`, `sapphire-preprocessing-api`, `sapphire-postprocessing-api`, `sapphire-user-api`, `sapphire-auth-api`), the four PostgreSQL containers, `sapphire-dashboard` (pentadal, port 5006), and the decadal dashboard container from `bin/docker-compose-dashboards.yml` all reporting `Up`.

- [ ] SAPPHIRE API gateway responds
  ```bash
  curl -sf http://localhost:8000/health && echo OK
  curl -sf http://localhost:8000/health/ready && echo READY
  ```

- [ ] Luigi daemon responds
  ```bash
  curl -sf http://localhost:8082/ >/dev/null && echo OK
  ```

- [ ] Dashboards return HTTP 200
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/forecast_dashboard   # pentadal, sapphire stack
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5007/forecast_dashboard   # decadal, legacy compose
  ```

**Pipeline smoke test:**

- [ ] Run gateway preprocessing (capture exit code):
  ```bash
  bash bin/run_preprocessing_gateway.sh /data/<data_folder>/config/<env_file>
  echo "Exit: $?"
  ```

- [ ] Run pentadal forecast (capture exit code):
  ```bash
  bash bin/run_pentadal_forecasts.sh /data/<data_folder>/config/<env_file>
  echo "Exit: $?"
  ```
  Both commands should return `0`. A non-zero exit indicates a failed Luigi task — check stderr and the pipeline log below before moving on. (On a browser-capable workstation you can also open `http://<server-ip>:8082/` for a graphical task graph.)

- [ ] Check for errors in pipeline logs
  ```bash
  grep -iE "error|critical|exception" /home/ubuntu/logs/sapphire_*.log | tail -20
  ```
  A handful of transient errors (network hiccups, retried API calls) is expected. Repeated errors from the same module indicate a real problem.

**Data verification:**

- [ ] Verify data reached the preprocessing and postprocessing APIs. Include the API key header if `API_KEY_ENABLED=true` in your env file (same pattern as Phase 8.3):
  ```bash
  # API_KEY_ENABLED=false (the default) — no auth needed:
  curl -s 'http://localhost:8000/api/preprocessing/runoff/?limit=5'
  curl -s 'http://localhost:8000/api/postprocessing/forecasts/?limit=5'

  # API_KEY_ENABLED=true:
  API_KEY=$(grep -E '^API_KEY=' /data/<data_folder>/config/<env_file> | cut -d= -f2)
  curl -s -H "x-api-key: ${API_KEY}" 'http://localhost:8000/api/preprocessing/runoff/?limit=5'
  curl -s -H "x-api-key: ${API_KEY}" 'http://localhost:8000/api/postprocessing/forecasts/?limit=5'
  ```
  Non-empty JSON arrays from both endpoints confirm the pipeline wrote through to the database.

- [ ] Verify forecast bulletins are on disk for the current year:
  ```bash
  ls /data/<data_folder>/reports/bulletins/pentad/$(date +%Y)/
  ```
  Excel bulletins named for the current pentad should appear (e.g., `<year>_04_aprel_<basin>_short_term_forecast_bulletin.xlsx`). If empty, the pentadal forecast step completed but no bulletin was written — check the postprocessing log.

- [ ] Open the dashboards interactively. On a public-facing deployment use the reverse-proxy URLs (`https://fc.pentad.<base_url>/forecast_dashboard`, `https://fc.decad.<base_url>/forecast_dashboard`); on a LAN-only deployment use `http://<server>:5006/forecast_dashboard` and `:5007`. Confirm the forecast chart renders with non-empty data for a known station, and that the WebSocket stays connected during interaction (DevTools → Network → WS tab).

**First-time deployment sanity check:**

- [ ] Confirm the Phase 4.3 `initialize` workflow has run for a fresh deployment. Skill metrics should be present:
  ```bash
  # API_KEY_ENABLED=true variant shown; drop -H for =false
  curl -s -H "x-api-key: ${API_KEY}" 'http://localhost:8000/api/postprocessing/skill-metric/?limit=5'
  ```
  An empty array means Phase 4.3 did not run successfully — go back and run `bash apps/run_locally.sh initialize` (with `ieasyhydroforecast_START_DATE` set in the env file) before announcing the deployment to end users. Do **not** skip this — the dashboards will show a "Current year" curve but no meaningful "Norm" curve until the hindcast has populated multi-year history.

**Alerting end-to-end (only if Phase 11 was configured):**

- [ ] Monitoring daemons healthy:
  ```bash
  systemctl status docker-monitor.service docker-log-watcher.service
  ```
- [ ] Trigger the synthetic log error from Phase 11.6 and confirm the alert email arrives at `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS` within ~30 seconds. This is the only end-to-end proof that container crashes will actually reach a human.

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
