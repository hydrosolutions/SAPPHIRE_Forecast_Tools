# SAPPHIRE Forecast Tools - Update Deployment Checklist

This checklist guides you through a **routine update** of an existing SAPPHIRE Forecast Tools deployment. It covers refreshing the repository, pulling new Docker images, applying schema migrations, restarting the microservices and Luigi stacks, verifying health, and rolling back if needed.

> **First-time deployment?** Use `doc/prod/first_deploy_checklist.md` instead. That doc covers the one-time steps (SSH tunnel setup, initial schema preparation, `RunInitializeWorkflow`, historical backfill) that are *not* part of a routine update.
>

## Operator setup — set these once per session [Required]

Before running any command below, set these variables once in your shell session. Everywhere in this doc that you see `${ORG_SLUG}`, `${DATA_DIR}`, `${ENV_FILE_PATH}`, or `${LOG_DIR}`, those are the values being substituted.

```bash
# Operator: set these variables once per deployment session. The rest of
# this checklist references them. Adapt to your deployment.
export ORG_SLUG=tjhm                                             # one of: kghm, tjhm, uzhm
export DATA_DIR=/data/taj_data_forecast_tools                    # adapt to your data path
export ENV_FILE_PATH="${DATA_DIR}/config/.env_develop_${ORG_SLUG}"
export LOG_DIR=/home/ubuntu/logs                                 # adapt to your logs path
```

**Target Server Paths:**
- Project: `/data/SAPPHIRE_Forecast_Tools`
- Config: `${ENV_FILE_PATH}`
- Logs: `${LOG_DIR}/`
- Luigi daemon port: 8082
- Dashboard port: 5006

---

## 1. PRE-UPDATE PREPARATION [Required]

### 1.1 SSH Access Verification [Required]

- [ ] Verify you have SSH access to the server
  ```bash
  ssh ubuntu@<server-ip>
  ```
- [ ] Confirm you have sudo privileges
  ```bash
  sudo whoami  # Should return: root
  ```
- [ ] Verify you can access the project directory
  ```bash
  ls -la /data/SAPPHIRE_Forecast_Tools
  ```

### 1.2 iEasyHydro HF Connectivity (if applicable) [Required]

If you are setting up this server for the first time, complete the SSH tunnel setup steps in `doc/prod/first_deploy_checklist.md` first. For routine updates, just verify the tunnel is healthy: `sudo systemctl status <tunnel-service>.service`.

### 1.3 Timing Considerations [Required]

- [ ] Check the current time relative to scheduled cron jobs
  ```bash
  crontab -l | grep -v "^#"
  ```
- [ ] **Avoid updating during:**
  - 03:00-06:00 UTC (forecast pipeline runs)
  - 19:00-21:00 UTC (maintenance jobs)
- [ ] Consider notifying stakeholders if dashboards will be temporarily unavailable
- [ ] Plan update during low-usage period (e.g., weekends or early morning local time)

### 1.4 Verify Current State [Required]

**Check running services:**

- [ ] Verify Luigi daemon is running
  ```bash
  docker ps | grep luigi-daemon
  curl -s http://localhost:8082/ > /dev/null && echo "Luigi daemon OK" || echo "Luigi daemon NOT RUNNING"
  ```

- [ ] Verify dashboard containers are running
  ```bash
  docker ps | grep sapphire-dashboard
  ```

- [ ] Check dashboard health status
  ```bash
  docker inspect --format "{{.State.Health.Status}}" sapphire-dashboard
  ```
  TODO carried forward: known Dockerfile healthcheck bug — `sapphire-dashboard` container reports unhealthy despite dashboard being functional. Operator may safely ignore this status; not blocking ops. Fix tracked separately.

- [ ] Verify dashboards are accessible
  ```bash
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard
  ```
  TBD: confirm expected HTTP code on staging (likely 200; possibly 302 if dashboard root redirects to a sub-path). Update this line once verified.

**Check recent pipeline activity:**

- [ ] Review today's pipeline logs for any issues
  ```bash
  ls -lt ${LOG_DIR}/sapphire_*.log | head -5
  tail -50 ${LOG_DIR}/sapphire_pentadal_$(date +%Y%m%d).log
  ```

- [ ] Check Luigi task history for recent failures
  ```bash
  # Open in browser: http://<server-ip>:8082
  # Or check via API:
  curl -s http://localhost:8082/api/task_list | head -20
  ```

- [ ] Note the current git commit/branch
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && git log --oneline -3
  git branch --show-current
  ```

### 1.5 Backup Critical Files [Required]

**Create timestamped backup directory:**

- [ ] Create backup directory
  ```bash
  BACKUP_DIR="/home/ubuntu/backups/sapphire_$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$BACKUP_DIR"
  echo "Backup directory: $BACKUP_DIR"
  ```

**Backup configuration files:**

- [ ] Backup the .env file
  ```bash
  cp ${ENV_FILE_PATH} "$BACKUP_DIR/"
  ```

- [ ] Backup crontab
  ```bash
  crontab -l > "$BACKUP_DIR/crontab_backup.txt"
  ```

- [ ] Backup Luigi daemon compose file (if customized)
  ```bash
  cp /data/SAPPHIRE_Forecast_Tools/bin/docker-compose-luigi.yml "$BACKUP_DIR/" 2>/dev/null || echo "Using default compose file"
  ```

**Backup the four Postgres databases:**

- [ ] Dump preprocessing, postprocessing, user, and auth DBs to timestamped `.dump` files
  ```bash
  export DB_BACKUP_DIR="/var/backups/sapphire/pre_update_$(date +%Y%m%d_%H%M%S)"
  sudo mkdir -p "$DB_BACKUP_DIR"
  sudo chown "$USER" "$DB_BACKUP_DIR"
  bash bin/backup_sapphire_db.sh -e ${ENV_FILE_PATH} -d "$DB_BACKUP_DIR" -r 30
  ```
  The `-e` flag points the backup script at the deployment env file; `-d` must point to an existing writable directory because `bin/backup_sapphire_db.sh` does not create it for you. Use `-r 0` instead of `-r 30` if no retention pruning should happen during this backup.
  This is the supported `pg_dump`-based backup mechanism (`bin/backup_sapphire_db.sh`).
  It writes one `.dump` file per database to the target directory. Required before
  applying schema migrations — DB state is the only state that cannot be regenerated
  from source. See `bin/reset_sapphire_db.sh --help` for the restore counterpart.

**Backup application state:**

- [ ] Backup last successful run timestamp
  ```bash
  cp ${DATA_DIR}/intermediate_data/last_successful_run.txt "$BACKUP_DIR/" 2>/dev/null || echo "File not found"
  ```
  Optional — only required if this deployment uses `ieasyforecast_last_successful_run_file`. Verify with: `grep ieasyforecast_last_successful_run_file ${ENV_FILE_PATH}` before skipping. Retained pending LR-003 cleanup gi_draft.

- [ ] Backup Luigi marker files (required for clean rollback — see §5)
  ```bash
  mkdir -p "$BACKUP_DIR/luigi_markers"
  cp ${DATA_DIR}/intermediate_data/luigi_markers/*.marker "$BACKUP_DIR/luigi_markers/" 2>/dev/null || echo "No marker files"
  ```
  Optional — only required for clean Luigi rollback; can skip if Luigi state was already reset elsewhere.

**Record current Docker image versions:**

- [ ] Document current image tags
  ```bash
  docker images | grep -E "sapphire|hydrosolutions" | tee "$BACKUP_DIR/docker_images.txt"
  ```

**Verify backup completeness:**

- [ ] List backup contents
  ```bash
  ls -la "$BACKUP_DIR"
  ```

- [ ] Record backup location for rollback reference
  ```bash
  echo "Backup complete: $BACKUP_DIR"
  ```

### 1.6 Pre-Update Checklist Summary [Required]

Before proceeding to the update steps, confirm:

- [ ] SSH access verified
- [ ] iEasyHydro HF connectivity verified (same network, SSH tunnel, or cloud config)
- [ ] Timing is appropriate (no cron jobs running soon)
- [ ] All services verified running
- [ ] Recent logs checked for issues
- [ ] .env file backed up
- [ ] Crontab backed up
- [ ] Four Postgres DBs backed up via `bin/backup_sapphire_db.sh`
- [ ] Luigi marker files backed up
- [ ] Current Docker images documented
- [ ] Backup directory location noted: `________________`

---

## 2. CORE UPDATE STEPS [Required]

### 2.1 Stop Services [Required]

Before updating, stop all running SAPPHIRE services to prevent conflicts during the update.

> **Project-name discipline**: the Luigi daemon container is started with `COMPOSE_PROJECT_NAME=sapphire` (see §3.1). Without `-p sapphire`, `docker compose down` derives the project name from `basename $PWD` (= `sapphire_forecast_tools`), which **does not match** and silently leaves `luigi-daemon` running. Always pass `-p sapphire` on the down command, or export `COMPOSE_PROJECT_NAME=sapphire` before invoking compose.

- [ ] **Stop the Luigi daemon and pipeline services** (project-name-safe)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  docker compose -f bin/docker-compose-luigi.yml -p sapphire down
  ```

> The dashboard (`sapphire-dashboard`) is part of the microservices stack defined in `sapphire/docker-compose.yml` and stops with it; no separate dashboard stop is required.

- [ ] **Stop the SAPPHIRE microservices stack** (api-gateway, four backend services, four DBs)
  ```bash
  docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml down
  ```
  This stops the api-gateway (8000), preprocessing-api (8002), postprocessing-api (8003),
  user-api (8004), auth-api (8005), and the four Postgres DB containers. Volumes are preserved.

- [ ] **Verify all SAPPHIRE containers are stopped**
  ```bash
  docker ps | grep sapphire
  # Should return no results
  ```

- [ ] **Optional: Stop any orphaned pipeline containers**
  ```bash
  docker ps -a | grep sapphire-pipeline | awk '{print $1}' | xargs -r docker rm -f
  ```

---

### 2.2 Update Repository [Required]

Pull the latest changes from the repository.

- [ ] **Navigate to project directory**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  ```

- [ ] **Check current branch and status**
  ```bash
  git status
  git branch -v
  ```

- [ ] **Stash any local changes (if needed)**
  ```bash
  git stash
  ```

- [ ] **Fetch and pull latest changes**
  ```bash
  git fetch origin
  git pull origin <branch-name>
  ```
  Replace `<branch-name>` with the target branch (e.g., `main`, `develop`, or a feature branch).

- [ ] **Restore stashed changes (if applicable)**
  ```bash
  git stash pop
  ```

- [ ] **Verify the update**
  ```bash
  git log --oneline -3
  ```

---

### 2.3 Update .env File (BEFORE running containers) [Required]

> **IMPORTANT**: Complete this section BEFORE Section 2.4 (Pull Docker Images). The .env file must be updated before pulling images or running any containers, as scripts read configuration from this file.

#### Step 1: Download server .env to local machine

The server .env and the local repo .env are on different machines, so you need to compare them locally.

- [ ] **Copy server .env to local machine via scp**
  ```bash
  # From your LOCAL machine (not the server)
  # Replace <server> with your server hostname/alias
  # If you connect via a specific user, use user@<server>
  # If you connect via a specific port, add -P <port>
  scp <server>:${ENV_FILE_PATH} \
      ~/Downloads/.env_develop_${ORG_SLUG}_server
  ```

#### Step 2: Compare with local repo .env

- [ ] **Compare the two files locally**
  ```bash
  # On your LOCAL machine
  diff ~/Downloads/.env_develop_${ORG_SLUG}_server \
       /path/to/SAPPHIRE_forecast_tools/apps/config/.env_develop_${ORG_SLUG}
  ```

  Or side-by-side:
  ```bash
  diff -y --suppress-common-lines \
       ~/Downloads/.env_develop_${ORG_SLUG}_server \
       /path/to/SAPPHIRE_forecast_tools/apps/config/.env_develop_${ORG_SLUG}
  ```

#### Step 3: Identify changes needed

- [ ] **New variables to add** (in local repo but not on server)
- [ ] **Variables to update** (different values between server and repo)
- [ ] **Variables to keep unchanged** (server-specific credentials, API keys, paths)

**Key variables to review:**

| Variable | Description | Expected Value |
|----------|-------------|----------------|
| `ieasyhydroforecast_backend_docker_image_tag` | Backend image tag | `local` |
| `ieasyhydroforecast_frontend_docker_image_tag` | Frontend image tag | `local` |
| `ieasyhydroforecast_run_ML_models` | Enable ML forecasting | `true` or `false` |
| `ieasyhydroforecast_run_CM_models` | Enable conceptual models | `true` or `false` |
| `ieasyhydroforecast_organization` | Organization identifier | `${ORG_SLUG}` |

**Variables to preserve** (don't overwrite with repo values):
- `IEASYHYDRO_HOST` - Server-specific API endpoint
- `IEASYHYDRO_PASSWORD` - Credentials
- `ieasyhydroforecast_API_KEY_GATEAWAY` - API keys
- Path variables if customized for server

#### Step 4: Edit the server .env locally

- [ ] **Make a working copy**
  ```bash
  cp ~/Downloads/.env_develop_${ORG_SLUG}_server ~/Downloads/.env_develop_${ORG_SLUG}_updated
  ```

- [ ] **Edit the file locally** (use your preferred editor)
  ```bash
  code ~/Downloads/.env_develop_${ORG_SLUG}_updated
  # Or: nano, vim, etc.
  ```

- [ ] **Add new variables**
- [ ] **Update changed variables**
- [ ] **Verify Docker image tags are set correctly**

#### Step 5: Upload updated .env back to server

- [ ] **Copy updated .env to server via scp**
  ```bash
  # From your LOCAL machine
  scp ~/Downloads/.env_develop_${ORG_SLUG}_updated \
      <server>:${ENV_FILE_PATH}
  ```

- [ ] **Verify on server**
  ```bash
  # On the SERVER
  grep -E "docker_image_tag" ${ENV_FILE_PATH}
  ```
  Expected output:
  ```
  ieasyhydroforecast_backend_docker_image_tag=local
  ieasyhydroforecast_frontend_docker_image_tag=local
  ```

- [ ] **Validate syntax on server** (no trailing spaces, proper quoting)
  ```bash
  grep -n "= " ${ENV_FILE_PATH}  # Spaces after =
  grep -n " $" ${ENV_FILE_PATH}  # Trailing spaces
  ```

---
### 2.4 Pull New Docker Images [Required]

- [ ] **Remove old SAPPHIRE DockerHub images only**
  ```bash
  old_sapphire_images="$(docker images 'mabesa/sapphire-*' --format '{{.Repository}}:{{.Tag}}' | sort -u)"
  printf '%s\n' "$old_sapphire_images"
  read -r -p "Remove only the mabesa/sapphire-* images listed above? [y/N] " confirm
  if [ "$confirm" = "y" ] && [ -n "$old_sapphire_images" ]; then
    printf '%s\n' "$old_sapphire_images" | xargs -r docker rmi
  fi
  ```
  Do not remove `nginx-proxy-manager`, `postgres:15`, locally built microservice images, or any other non-`mabesa/sapphire-*` image.

> **Prerequisite**: Complete Section 2.3 (Update .env File) first!

Pull the updated Docker images with the `:local` tag.

> **Note**: The cron scripts (e.g., `run_pentadal_forecasts.sh`) will pull images automatically when run. You can either:
> - **Option A**: Pull images manually now (see below)
> - **Option B**: Skip manual pull and let the first cron job / manual script run pull the images

**Image inventory — three categories**

The deployment uses three categories of images. **Only the pipeline layer is pulled
from DockerHub**; the microservices layer is built locally via `sapphire/docker-compose.yml`,
and the infrastructure layer mixes a locally-built Luigi daemon with the upstream
`postgres:15` image.

> **Name-collision warning**: the app-side image `mabesa/sapphire-postprocessing` (operational
> postprocessing wrappers, pulled here) is **different** from the service-side image
> `sapphire-postprocessing` (FastAPI service, built locally by the microservices compose).
> They share the suffix `postprocessing` but serve different purposes.

**Set environment variables:**

- [ ] **Export the image tag**
  ```bash
  export ieasyhydroforecast_backend_docker_image_tag=local
  export ieasyhydroforecast_frontend_docker_image_tag=local
  ```

#### 2.4.1 Pipeline images (pulled from DockerHub)

These are the `apps/*` layer images run by Luigi tasks and dashboards.

**Core (required for all deployments):**

- [ ] **Pull base image**
  ```bash
  docker pull mabesa/sapphire-pythonbaseimage:local
  ```

- [ ] **Pull pipeline orchestration image**
  ```bash
  docker pull mabesa/sapphire-pipeline:local
  ```

- [ ] **Pull preprocessing-runoff image**
  ```bash
  docker pull mabesa/sapphire-preprunoff:local
  ```

- [ ] **Pull station-forcing preprocessing image** (new since 2026-01-30)
  ```bash
  docker pull mabesa/sapphire-preprocessing-station-forcing:local
  ```

- [ ] **Pull linear regression forecasting image**
  ```bash
  docker pull mabesa/sapphire-linreg:local
  ```

- [ ] **Pull long-term forecasting image** (new since 2026-01-30)
  ```bash
  docker pull mabesa/sapphire-lt-forecasting:local
  ```

- [ ] **Pull postprocessing image** (operational app — distinct from the service-side image)
  ```bash
  docker pull mabesa/sapphire-postprocessing:local
  ```

- [ ] **Pull dashboard image**
  ```bash
  docker pull mabesa/sapphire-dashboard:local
  ```

**Optional (based on deployment configuration):**

If `ieasyhydroforecast_run_ML_models=true` in your .env file:

- [ ] **Pull gateway preprocessing image**
  ```bash
  docker pull mabesa/sapphire-prepgateway:local
  ```

- [ ] **Pull ML forecasting image**
  ```bash
  docker pull mabesa/sapphire-ml:local
  ```

If `ieasyhydroforecast_run_CM_models=true` in your .env file:

- [ ] **Pull conceptual model image** (rarely used; gated by `ieasyhydroforecast_run_CM_models`)
  ```bash
  docker pull mabesa/sapphire-conceptmod:local
  ```

#### 2.4.2 Microservices images (built locally)

The microservices layer is defined in `sapphire/docker-compose.yml` and is **built
locally** rather than pulled. The services are:

- `sapphire-api-gateway` (port 8000) — built from `sapphire/services/api-gateway/Dockerfile`
- `sapphire-auth` (port 8005) — built from `sapphire/services/auth/Dockerfile`
- `sapphire-user` (port 8004) — built from `sapphire/services/user/Dockerfile`
- `sapphire-preprocessing` (port 8002) — built from `sapphire/services/preprocessing/Dockerfile`
- `sapphire-postprocessing` (port 8003) — built from `sapphire/services/postprocessing/Dockerfile`
  (distinct from the app-side `mabesa/sapphire-postprocessing` above)

The `docker compose ... up -d` in §2.5 will build any missing images automatically.
To force a rebuild after a code update:

- [ ] **Rebuild microservices** (only if Dockerfiles or `requirements.txt` changed)
  ```bash
  docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml build
  ```

#### 2.4.3 Infrastructure images

- `sapphire/luigi-daemon` — built locally from `bin/luigi-daemon.Dockerfile` (see `bin/docker-compose-luigi.yml`).
- `postgres:15` — upstream image used by the four DB containers (`preprocessing-db`,
  `postprocessing-db`, `user-db`, `auth-db`). Pulled automatically on first stack bring-up.

#### 2.4.4 Verify pulled images

- [ ] **List all SAPPHIRE images with `local` tag**
  ```bash
  docker images | grep sapphire | grep local
  ```

---


### 2.4.5 Bring up SAPPHIRE microservices stack [Required]

**Why this step matters:** the cron scripts (`bin/run_pentadal_forecasts.sh`,
`bin/run_decadal_forecasts.sh`, `bin/run_preprocessing_gateway.sh`, etc.) and the
Luigi pipeline tasks read and write through the api-gateway at
`http://localhost:8000`. Without the microservices stack up, every cron command
in §2.6 fails immediately with `Connection refused` to `SAPPHIRE_API_URL` /
`API_GATEWAY_URL`. The dashboards also depend on the api-gateway.

The stack is defined in `sapphire/docker-compose.yml` and includes:

| Service | Host port | Container name |
|---------|-----------|----------------|
| `api-gateway` | 8000 | `sapphire-api-gateway` |
| `preprocessing-api` | 8002 | `sapphire-preprocessing-api` |
| `postprocessing-api` | 8003 | `sapphire-postprocessing-api` |
| `user-api` | 8004 | `sapphire-user-api` |
| `auth-api` | 8005 | `sapphire-auth-api` |
| `preprocessing-db` (postgres:15) | 5433 | `sapphire-preprocessing-db` |
| `postprocessing-db` (postgres:15) | 5434 | `sapphire-postprocessing-db` |
| `user-db` (postgres:15) | 5435 | `sapphire-user-db` |
| `auth-db` (postgres:15) | 5436 | `sapphire-auth-db` |

> **First-time deploy?** If this is the initial deployment on this server, also
> follow `doc/prod/first_deploy_checklist.md` — that doc covers initial schema
> preparation (alembic stamp) and the one-time `RunInitializeWorkflow` task that
> bootstraps the preprocessing DB with historical site data. For routine updates,
> the schema is already populated.

- [ ] **Start the microservices stack**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml up -d
  ```
  Alternatively, the wrapper `bin/restart_sapphire_stack.sh ${ENV_FILE_PATH}` performs
  the same bring-up.

- [ ] **Wait for the api-gateway to be live and ready**
  ```bash
  # Liveness: returns 200 as soon as the gateway process is up
  curl -sf http://localhost:8000/health && echo "PASS: gateway live"

  # Readiness: gateway-side health check that also probes the four backend services
  curl -sf http://localhost:8000/health/ready && echo "PASS: gateway ready"
  ```
  Wait up to 60 s for `/health/ready` to return 200. If it does not, run
  `docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml ps`
  and check `docker logs sapphire-<service>-api` for the unhealthy service.

- [ ] **Verify all nine service+DB containers are running**
  ```bash
  docker ps --filter "name=sapphire-" --format "table {{.Names}}\t{{.Status}}"
  ```

---

### 2.4.6 Apply schema migrations [Required]

The preprocessing and postprocessing services use **Alembic** for schema management.
Pulling new images without applying outstanding migrations can leave the DB schema
behind the code — for example, the snow-stat write path requires migration
`9f1e72108f01` (preprocessing) which adds 12 columns to the `snow` table.

> **Baseline revisions** (for reference / rollback): preprocessing baseline is
> `a6210b339f17`; postprocessing baseline is `34b227f37299`. The latest preprocessing
> migration is `9f1e72108f01` (snow stat columns).

- [ ] **Record current revisions** (for rollback)
  ```bash
  docker exec sapphire-preprocessing-api alembic current  | tee -a "$BACKUP_DIR/alembic_pre_update.txt"
  docker exec sapphire-postprocessing-api alembic current | tee -a "$BACKUP_DIR/alembic_pre_update.txt"
  ```
  Capture the output in your backup directory (`$BACKUP_DIR` from §1.5). You will
  need these revision hashes if a rollback (§5) requires `alembic downgrade`.

- [ ] **Apply preprocessing migrations**
  ```bash
  docker exec sapphire-preprocessing-api alembic upgrade head
  ```
  This brings the preprocessing schema forward to `9f1e72108f01` if not already
  there. The migration adds the 12 nullable snow-stat columns required for the
  snow dashboard write path.

- [ ] **Apply postprocessing migrations**
  ```bash
  docker exec sapphire-postprocessing-api alembic upgrade head
  ```

- [ ] **Confirm new revisions**
  ```bash
  docker exec sapphire-preprocessing-api alembic current
  docker exec sapphire-postprocessing-api alembic current
  ```

> **Note on user/auth services**: both have alembic scaffolding but no
> non-baseline migrations today. They self-stamp on first boot and require no
> manual action during routine updates.

---

### 2.5 Update Crontabs [Required]

Update the cron schedule for automated forecast runs.

> **Note**: The cron scripts handle Docker image pulling automatically. Once crontabs are configured, the first scheduled run will pull the required images.

> **First-time deploy on this server?** Cron installation is covered end-to-end (with backup of any prior crontab, log-dir creation, and the same canonical block as below) in `doc/prod/first_deploy_checklist.md` §12 "Install operational cron schedule". This §2.5 covers cron *updates* on an existing deployment — diffing the running crontab against the canonical block, removing retired entries, and adding new ones introduced since the last update.

The canonical schedule below follows the post-S1-2026 consolidated Luigi-wrapper pattern (one `bin/run_daily_maintenance.sh` for daily maintenance, `bin/run_periodic_maintenance.sh <task>` for bimonthly/yearly tasks). It supersedes the legacy per-app `daily_*` scripts (`daily_preprunoff_maintenance.sh`, `daily_ml_maintenance.sh`, `daily_linreg_maintenance.sh`, `daily_postprc_maintenance.sh`, `daily_gateway_maintenance.sh`, `daily_update_sapphire_frontend.sh`). Those legacy scripts remain on origin for manual debugging only — do **not** schedule them via cron.

- [ ] **Backup current crontab** (if not done in pre-update)
  ```bash
  crontab -l > ~/crontab_backup_$(date +%Y%m%d).txt
  ```

- [ ] **Review the recommended crontab schedule**

  The authoritative source is [deployment.md - Set up cron job](../deployment.md#set-up-cron-job). The block reproduced below is kept in sync with that source.

- [ ] **Diff the running crontab against the canonical block**
  ```bash
  crontab -l > /tmp/crontab_current.txt
  diff /tmp/crontab_current.txt <(sed -n '/# m h  dom mon dow/,/^```$/p' "$(pwd)/doc/prod/update_deployment_checklist.md")
  ```
  Identify legacy entries to remove and new entries to add. The most common change since v0.3.0 is replacing four separate `daily_*_maintenance.sh` lines with one `run_daily_maintenance.sh` line.

- [ ] **Edit crontab**
  ```bash
  crontab -e
  ```

- [ ] **Verify/update the following cron entries:**

  > **Cron does not expand shell variables.** The `${...}` placeholders below are for readability — when you paste these into `crontab -e`, substitute the literal values of `${ENV_FILE_PATH}` and `${LOG_DIR}` (as set in the operator placeholder block at the top of this doc) into each line.

  ```bash
  # m h  dom mon dow   command
  # ---------------------------------------------------------------------------
  # SAPPHIRE Forecast Tools Schedule (Times are in UTC)
  # Cron does not expand shell variables. Before pasting, substitute the
  # literal values of ${ENV_FILE_PATH} and ${LOG_DIR} (see Operator setup at
  # the top of this doc).
  # ---------------------------------------------------------------------------

  # Log cleanup: delete logs older than 7 days (runs daily at 02:00 UTC)
  0 2 * * * find ${LOG_DIR} -name "sapphire_*.log" -mtime +7 -delete

  # Daily DB backup at 01:00 UTC (pg_dump-based, 30-day retention)
  0 1 * * * bash /data/SAPPHIRE_Forecast_Tools/bin/backup_sapphire_db.sh -e ${ENV_FILE_PATH} -d /var/backups/sapphire -r 30 >> ${LOG_DIR}/sapphire_db_backup_$(date +\%Y\%m\%d).log 2>&1

  # (1) Gateway Preprocessing at 03:00 UTC. Independent of daily data.
  0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_gateway_preprocessing_$(date +\%Y\%m\%d).log 2>&1

  # (2) Pentadal Forecast at 04:00 UTC. Luigi triggers runoff preprocessing.
  0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_pentadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (3) Decadal Forecast at 05:00 UTC.
  0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_decadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (4) Long-Term Forecast at 06:00 UTC on the 10th and 25th of each month.
  # The script self-gates via lt_schedule_query.py (only fires on predefined
  # operational forecast dates with ±5 day tolerance), so daily * * * is also
  # safe but wasteful. The 10th/25th schedule matches operational expectations.
  0 6 10,25 * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_long_term_$(date +\%Y\%m\%d).log 2>&1

  # (4b) Bimonthly long-term skill metrics recalculation at 10:00 UTC on the
  # 10th and 25th (4 hours after the long-term forecast). Refreshes MONTHLY,
  # QUARTERLY, and SEASONAL skill metrics so long-term skill tiles on the
  # dashboard do not stay stale for a year. Log-and-continue: one failing
  # mode does not block the others, but the job exits non-zero so errors
  # still surface in the cron log.
  0 10 10,25 * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/bimonthly_long_term_skill_metrics_recalculation.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_bimonthly_lt_skill_recalc_$(date +\%Y\%m\%d).log 2>&1

  # (5) Daily Maintenance at 19:00 UTC (consolidated; replaces legacy
  # daily_preprunoff_maintenance.sh / daily_ml_maintenance.sh /
  # daily_linreg_maintenance.sh / daily_postprc_maintenance.sh /
  # daily_gateway_maintenance.sh / daily_update_sapphire_frontend.sh).
  # Luigi enforces dependency order: PrepRunoff + Gateway → LinReg → ML →
  # PostProcessing → Frontend. ML concurrency limited to 3 via Luigi resources.
  0 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_daily_maintenance_$(date +\%Y\%m\%d).log 2>&1

  # (6) Periodic maintenance tasks (consolidated Luigi wrapper)
  # Bimonthly long-term postprocessing at 22:00 UTC on the 1st of odd months.
  # Supersedes the legacy bin/bimonthly_long_term_postprocessing.sh standalone
  # wrapper (kept on origin for manual / debugging use only).
  0 22 1 1,3,5,7,9,11 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh long_term ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_lt_postproc_$(date +\%Y\%m\%d).log 2>&1

  # (7) Yearly skill metrics recalculation at 01:00 UTC on December 31.
  # Full-history safety net (the bimonthly skill recalc above keeps the
  # dashboard tiles fresh; this is the annual deep recalc).
  0 1 31 12 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh skill_recalc ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_skill_recalc_$(date +\%Y\%m\%d).log 2>&1

  # (8) Yearly snow norm/stat recalculation at 02:00 UTC on August 31.
  # Consolidated Luigi wrapper. Supersedes legacy bin/yearly_snow_norm_recalculation.sh
  # (kept on origin for manual / debugging use only).
  0 2 31 8 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_snow_norm_$(date +\%Y\%m\%d).log 2>&1

  # (9) Yearly runoff hydrograph aggregation at 03:00 UTC on January 1.
  # Replaces the retired YearlyMonthlyNormsRecalculation Luigi task. Builds
  # the long-horizon monthly + April–September seasonal hydrograph view used
  # by the dashboard.
  0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_runoff_hydrograph_$(date +\%Y\%m\%d).log 2>&1
  ```

- [ ] **Verify crontab was saved correctly**
  ```bash
  crontab -l
  ```

- [ ] **Ensure log directory exists**
  ```bash
  mkdir -p ${LOG_DIR}
  ls -la ${LOG_DIR}/
  ```

- [ ] **Verify cron service is running**
  ```bash
  sudo systemctl status cron
  ```

- [ ] **Verify backup target directory exists and is writable** (for the daily DB backup at 01:00 UTC)
  ```bash
  sudo mkdir -p /var/backups/sapphire
  sudo chown $(whoami): /var/backups/sapphire
  ls -ld /var/backups/sapphire
  ```

### 2.6 Test Cron Commands Manually [Required]

> **Prerequisite — the SAPPHIRE microservices stack must be up before any
> cron command below will succeed.** Every cron command reads/writes through
> the api-gateway at `http://localhost:8000`; without the stack up, each
> command fails immediately with `Connection refused`. If you have not
> already brought the stack up in §2.4.5, do so now:
> ```bash
> docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml up -d
> # or, equivalently:
> bash bin/restart_sapphire_stack.sh ${ENV_FILE_PATH}
> ```
> Verify before proceeding:
> ```bash
> curl -sf http://localhost:8000/health/ready && echo "READY"
> ```

After updating crontabs, run each cron command manually (one by one) to verify they work correctly before waiting for scheduled execution. The Luigi daemon starts automatically when needed. The list below mirrors the canonical cron block in §2.5 — run each once to confirm the wrapper script, env file, and microservices stack agree.

- [ ] **Run database backup**
  ```bash
  bash /data/SAPPHIRE_Forecast_Tools/bin/backup_sapphire_db.sh -e ${ENV_FILE_PATH} -d /var/backups/sapphire -r 30
  ```

- [ ] **Run gateway preprocessing**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh ${ENV_FILE_PATH}
  ```

- [ ] **Run pentadal forecast**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh ${ENV_FILE_PATH}
  ```

- [ ] **Run decadal forecast**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh ${ENV_FILE_PATH}
  ```

- [ ] **Run long-term forecast** (self-gates on schedule; safe to run any day for the dry-run path)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh --dry-run ${ENV_FILE_PATH}
  ```
  Drop `--dry-run` once the dry-run succeeds to exercise the full pipeline on a real forecast date.

- [ ] **Run consolidated daily maintenance** (replaces the four legacy `daily_*_maintenance.sh` wrappers)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh ${ENV_FILE_PATH}
  ```

- [ ] **Run periodic maintenance tasks** (one per task type; run only the ones relevant to the current calendar)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh long_term     ${ENV_FILE_PATH}
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh skill_recalc  ${ENV_FILE_PATH}
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms    ${ENV_FILE_PATH}
  ```

- [ ] **Run bimonthly long-term skill metrics recalculation**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/bimonthly_long_term_skill_metrics_recalculation.sh ${ENV_FILE_PATH}
  ```

- [ ] **Run yearly runoff hydrograph aggregation**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH}
  ```

- [ ] **Monitor progress** in Luigi UI at http://localhost:8082
- [ ] **Check logs** for errors after each command completes

---

## 3. POST-UPDATE VERIFICATION [Required]

### 3.1 Start Services [Required]

Start the services in the correct order to ensure proper initialization.

**Order:** microservices stack (already started in §2.4.5) → Luigi daemon → dashboards.

#### Verify SAPPHIRE microservices stack is up (already started in §2.4.5)

- [ ] Confirm the api-gateway and four backend services are healthy:
  ```bash
  curl -sf http://localhost:8000/health/ready && echo "PASS: gateway ready"
  docker ps --filter "name=sapphire-" --format "table {{.Names}}\t{{.Status}}"
  ```
  If the stack is not running, return to §2.4.5 and start it before proceeding.

#### Start Luigi Daemon

The Luigi daemon must be running before any pipeline tasks can execute.

- [ ] Navigate to SAPPHIRE_Forecast_Tools directory:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  ```

- [ ] Set the Compose project name for consistent networking:
  ```bash
  export COMPOSE_PROJECT_NAME=sapphire
  ```

- [ ] Start the Luigi daemon:
  ```bash
  docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
  ```

- [ ] Wait for Luigi daemon to be ready:
  ```bash
  until curl -fsS http://localhost:8082/ >/dev/null; do echo "Waiting for Luigi..."; sleep 2; done
  echo "Luigi daemon is ready"
  ```

#### Dashboard

The `sapphire-dashboard` container is part of the microservices stack and was already started by §2.4.5; no separate dashboard start step is required.

#### Verify Services Started

- [ ] Confirm all containers are running:
  ```bash
  docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  ```

Expected output should include the microservices (`sapphire-api-gateway`,
`sapphire-preprocessing-api`, `sapphire-postprocessing-api`, `sapphire-user-api`,
`sapphire-auth-api`, plus four `sapphire-*-db` containers) and:
- `sapphire-luigi-daemon` (or similar) - Up, port 8082
- `sapphire-dashboard` - Up, port 5006

### 3.2 Verify Services Running [Required]

#### Post-deploy probe suite

Run the full probe suite below. All commands should return HTTP 200 within 5 s.

```bash
# API gateway readiness (gates backend services)
curl -sf http://localhost:8000/health
curl -sf http://localhost:8000/health/ready

# Backend service health
curl -sf http://localhost:8002/health    # preprocessing-api
curl -sf http://localhost:8003/health    # postprocessing-api
curl -sf http://localhost:8004/health    # user-api
curl -sf http://localhost:8005/health    # auth-api

# Luigi UI
curl -sf http://localhost:8082/

# Dashboards
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/forecast_dashboard
```

Acceptance: all `curl -sf` commands exit 0; the dashboard probe returns `200`.
If `/health/ready` fails (200 from `/health` but failure from `/health/ready`),
one or more backend services did not start correctly — inspect
`docker logs sapphire-<service>-api`.

#### Check Luigi UI

- [ ] Open browser to Luigi web interface: `http://<your-server-ip>:8082`
- [ ] Verify the UI loads without errors
- [ ] Check that no tasks are stuck in "RUNNING" state from before the update

#### Check Dashboards

- [ ] Dashboard accessible: `http://<your-server-ip>:5006/forecast_dashboard`
- [ ] Dashboard loads data correctly (charts display, station selector works)

#### Troubleshooting — dashboard cannot reach iEH HF via SSH tunnel

> **If dashboard logs show `iEHHF not available ... Connection refused` on the
> `/api/v1/auth/token-obtain` path:** the dashboard container is on a Docker bridge
> network and cannot reach the host SSH tunnel on `localhost:<port>`. The fix is
> `network_mode: host` on the dashboard service in `sapphire/docker-compose.yml`
> (and in `bin/docker-compose-dashboards.yml` if used).
>
> See `doc/prod/first_deploy_checklist.md` §1.2 Option B / Option C "Dashboard
> tunnel reachability" for the full procedure. Per memory note
> `iehhf_tunnel_docker_bridge_fix.md`, this fix is currently applied server-side
> only on Tajik and is **not** in the committed compose files; expect to apply
> it on any deployment where iEH HF is reached via SSH tunnel.

Diagnostic commands:
```bash
# Confirm the SSH tunnel is listening on the host
ss -tlnp | grep <tunnel-port>

# From inside the dashboard container, try to reach the tunnel
docker exec sapphire-dashboard curl -sf http://localhost:<tunnel-port>/api/v1/
```
If the host probe succeeds but the container probe returns `Connection refused`,
apply the `network_mode: host` fix.

#### Check Container Health

- [ ] Verify container health status:
  ```bash
  docker ps --format "table {{.Names}}\t{{.Status}}"
  ```
  Look for "(healthy)" status on dashboard containers

- [ ] Check for any containers in unhealthy or restarting state:
  ```bash
  docker ps --filter "health=unhealthy" --format "{{.Names}}"
  docker ps --filter "status=restarting" --format "{{.Names}}"
  ```
  Both commands should return empty results

#### Check Container Logs for Errors

- [ ] Review Luigi daemon logs:
  ```bash
  docker logs sapphire-luigi-daemon --tail 50
  ```

- [ ] Review microservices logs:
  ```bash
  docker logs sapphire-api-gateway --tail 50
  docker logs sapphire-preprocessing-api --tail 50
  docker logs sapphire-postprocessing-api --tail 50
  ```

- [ ] Review dashboard logs for startup errors:
  ```bash
  docker logs sapphire-dashboard --tail 50
  ```

### 3.3 Test Forecast Run [Required]

Perform a quick manual test to verify the pipeline works correctly.

#### Quick Test - Run Preprocessing Gateway

This is a lightweight test that verifies the pipeline infrastructure:

- [ ] Run preprocessing gateway task:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/run_preprocessing_gateway.sh ${ENV_FILE_PATH}
  ```

- [ ] Monitor progress in Luigi UI at http://localhost:8082
- [ ] Check logs for successful completion:
  ```bash
  tail -f ${LOG_DIR}/sapphire_gateway_$(date +%Y%m%d).log
  ```

#### Full Test - Run Pentadal Forecast (Optional)

For a comprehensive test, run a full pentadal forecast cycle:

- [ ] Run pentadal forecast:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/run_pentadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_pentadal_$(date +%Y%m%d).log 2>&1
  ```

- [ ] Monitor progress in Luigi UI
- [ ] Verify forecast outputs were generated in the intermediate_data directory

#### Verify in Logs

- [ ] Check that no ERROR or CRITICAL messages appear in logs:
  ```bash
  grep -iE "error|critical|exception" ${LOG_DIR}/sapphire_*.log | tail -20
  ```

- [ ] Verify tasks completed successfully in Luigi UI (all tasks show green checkmarks)

---

### 3.4 Historical Data Migration (one-time, post-update)

**When to run this step:** Only on (a) the initial deployment to a new
server, or (b) a deliberate refresh of the historical archive after a
forecast-tools update that touched table schemas, hook scripts, or the
CSV/API I/O surface. Routine `update_deployment_checklist` runs that
only bump image tags + cron + `.env` do NOT need this step.

**Reference:** [`doc/prod/update_data_migration_runbook.md`](./update_data_migration_runbook.md)

The migration runbook is a self-contained ten-section procedure:

| Section | Purpose                                                |
|---------|--------------------------------------------------------|
| §1      | Purpose, scope, anti-goals                             |
| §2      | Prerequisites and source selection                     |
| §3      | Credentials and secret hygiene                         |
| §4      | P0 diagnostics, backup (`pg_dump`), cron pause, dry-run |
| §5      | CSV-source migrations (runoff DAY, meteo, snow, hydrograph DAY, long forecasts) |
| §6      | Laptop local-export migrations (runoff/hydrograph PENTAD/DECADE, LR + ML forecasts) |
| §7      | Regenerate / gap-backfill hooks (snow stats, hydrograph MONTH/SEASON, skill recalcs) |
| §8      | Acceptance SQL (per-DB consolidated verification)      |
| §9      | Failure recovery and rerun                             |
| §10     | Rollback and cleanup (literal `pg_restore`)            |

**Order of operations relative to this checklist:**

1. Complete §1–§3.3 of THIS checklist first (services up, healthz green,
   test forecast OK). The migration toolkit assumes a healthy stack.
2. Take the §4.1 `pg_dump` backup from the migration runbook BEFORE
   running any migration wrapper. This is a hard gate.
3. Pause cron via the runbook §4.2 procedure (this is in addition to
   any cron changes made in §2.5 of this checklist).
4. Execute §5–§7 migration wrappers per the runbook in order.
5. Run §8 acceptance SQL to verify all data families.
6. Resume cron from the §4.2 snapshot.

**If anything fails:** the runbook's §9 (recovery) and §10 (rollback)
both reference the `pg_dump` backup from step 2 above. Do NOT proceed
with a migration whose `pg_dump` log does not contain `All four dumps
succeeded and verified`.

- [ ] Decided whether this deployment needs historical data migration
      (initial deployment OR forecast-tools schema change since last
      migration). If not, skip to §4.
- [ ] If yes: follow `update_data_migration_runbook.md` end-to-end.
- [ ] Confirmed all `§8` acceptance SQL blocks return expected row
      counts before resuming cron + leaving the host.

---

## 4. LOG CLEANUP [Optional]

Clean up old log files to prevent disk space issues.

### 4.1 Clean Up Pipeline Logs [Optional]

Log files are stored in `${LOG_DIR}/`

- [ ] View current log files and sizes:
  ```bash
  ls -lh ${LOG_DIR}/sapphire_*.log
  ```

- [ ] Delete logs older than 7 days:
  ```bash
  find ${LOG_DIR} -name "sapphire_*.log" -mtime +7 -delete
  ```

- [ ] Verify cleanup (check remaining files):
  ```bash
  ls -lh ${LOG_DIR}/
  ```

### 4.2 Clean Up Docker Logs [Optional]

Docker container logs can also grow large over time.

- [ ] Check intermediate_data/docker_logs if it exists:
  ```bash
  ls -lh ${DATA_DIR}/intermediate_data/docker_logs/ 2>/dev/null || echo "Directory does not exist"
  ```

- [ ] Clean up old Docker logs (if directory exists):
  ```bash
  find ${DATA_DIR}/intermediate_data/docker_logs -name "*.log" -mtime +7 -delete 2>/dev/null
  ```

### 4.3 Prune Docker System [Optional]

Remove unused Docker resources:

- [ ] Remove dangling images and unused containers:
  ```bash
  docker system prune -f
  ```

- [ ] Check disk space recovered:
  ```bash
  docker system df
  ```

---

## 5. ROLLBACK PROCEDURE [Emergency only]

If the update causes issues, follow these steps to revert. The forward path had four
state-changing actions: image pull (§2.4), `.env` edit (§2.3), `alembic upgrade head`
(§2.4.6), and Luigi marker file updates (implicit, via runs). Rollback must reverse
each in the opposite order: stop services → restore `.env` → downgrade schema (if
upgraded forward) → restore Luigi markers → restart with previous image tag.

### 5.1 Stop Current Services [Emergency only]

- [ ] Stop all SAPPHIRE services (project-name-safe `down` on the Luigi compose):
  ```bash
  docker compose -f bin/docker-compose-luigi.yml -p sapphire down
  docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml down
  ```
  Without `-p sapphire` on the Luigi compose, the persistent `luigi-daemon` container
  is silently missed (see §2.1 note). The dashboard stops with the microservices stack.

### 5.2 Restore Previous Docker Images [Emergency only]

- [ ] Pull previous image versions (replace `<previous-tag>` with actual version):
  ```bash
  docker pull mabesa/sapphire-pipeline:<previous-tag>
  docker pull mabesa/sapphire-dashboard:<previous-tag>
  ```

- [ ] Or use locally cached previous images if available:
  ```bash
  docker images | grep sapphire
  ```

### 5.3 Restore .env Backup [Emergency only]

If you backed up your .env file before the update:

- [ ] Restore the backup (use your `$BACKUP_DIR` from §1.5):
  ```bash
  cp ${BACKUP_DIR}/.env_develop_${ORG_SLUG} ${ENV_FILE_PATH}
  ```

### 5.4 Update Image Tags [Emergency only]

- [ ] Edit your .env file to use the previous image tag:
  ```bash
  # Set these variables in your .env file:
  ieasyhydroforecast_backend_docker_image_tag=<previous-tag>
  ieasyhydroforecast_frontend_docker_image_tag=<previous-tag>
  ```

### 5.5 Restore database state [Emergency only]

Apply this section only if the forward path included `alembic upgrade head` (§2.4.6)
or if data was corrupted by the update. Skip the schema-downgrade step if alembic
was not run forward.

#### 5.5.1 Restore from `pg_dump` (only if data corruption is suspected)

The four Postgres DBs were backed up in §1.5 via `bin/backup_sapphire_db.sh` to
`/var/backups/sapphire/pre_update_<timestamp>/`. Restore from those dumps using
`bin/reset_sapphire_db.sh` — see `bash bin/reset_sapphire_db.sh --help` for flags.

> **Caution**: `bin/reset_sapphire_db.sh` is destructive (drops and recreates
> volumes). Run only when data corruption is confirmed; for schema-only rollback,
> prefer `alembic downgrade` in 5.5.2 below.

#### 5.5.2 Downgrade schema with alembic

Use the revisions you recorded in `$BACKUP_DIR/alembic_pre_update.txt` (§2.4.6).
Bring the microservices stack up briefly to run the downgrade:

- [ ] Start the microservices stack (needed so `alembic` can connect to its DB):
  ```bash
  docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml up -d
  curl -sf http://localhost:8000/health/ready && echo "READY"
  ```

- [ ] Downgrade preprocessing (replace `<captured-revision>` with the value from
  `$BACKUP_DIR/alembic_pre_update.txt`):
  ```bash
  docker exec sapphire-preprocessing-api alembic downgrade <captured-revision>
  ```

- [ ] Downgrade postprocessing (same pattern):
  ```bash
  docker exec sapphire-postprocessing-api alembic downgrade <captured-revision>
  ```

> **Note**: downgrading preprocessing past `9f1e72108f01` drops 12 snow-stat
> columns (`count`, `mean`, `std`, `min`, `max`, `q05`, `q25`, `q50`, `q75`,
> `q95`, `previous`, `current`). No historical runoff data is lost, but the snow
> dashboard write path will silently null out stat fields until the schema is
> re-upgraded.

### 5.6 Restore Luigi marker files [Emergency only]

If the rolled-back image expects an earlier marker state, restore from backup. If
markers are not restored, tasks may silently short-circuit on stale completion
markers from the failed forward run.

- [ ] Restore markers from `$BACKUP_DIR/luigi_markers/`:
  ```bash
  cp ${BACKUP_DIR}/luigi_markers/*.marker ${DATA_DIR}/intermediate_data/luigi_markers/
  ```

### 5.7 Restart Services with Previous Version [Emergency only]

The microservices stack may still be up from §5.5.2; only Luigi and dashboards
need to be (re)started.

- [ ] Confirm microservices stack is running:
  ```bash
  curl -sf http://localhost:8000/health/ready && echo "READY"
  ```
  If not running, start it: `docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml up -d`.

- [ ] Start Luigi daemon:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  export COMPOSE_PROJECT_NAME=sapphire
  docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
  ```

- [ ] Wait for Luigi daemon:
  ```bash
  until curl -fsS http://localhost:8082/ >/dev/null; do sleep 2; done
  ```

The dashboard was restarted as part of the microservices stack `up -d` above; no separate dashboard start is required.

- [ ] Verify rollback was successful (repeat Section 3.2 verification steps,
  including the probe suite and the four backend-service health checks)

---

## 6. FINAL CHECKLIST [Required]

Complete this summary checklist before considering the update complete.

### Services Running [Required]

- [ ] Luigi daemon is running and accessible at port 8082
- [ ] Dashboard is running and accessible at port 5006
- [ ] All containers show "healthy" status

### Crontabs Configured [Required]

- [ ] Verify crontab entries are correct:
  ```bash
  crontab -l
  ```

- [ ] Confirm scheduled times are appropriate for your timezone
- [ ] Verify log cleanup job is configured (typically at 02:00 UTC)

### Next Scheduled Run [Required]

- [ ] Identify next scheduled forecast run from crontab
- [ ] Note the expected time: _______________
- [ ] Plan to check logs after next scheduled run to confirm everything works

### Documentation [Required]

- [ ] Note any configuration changes made during this update
- [ ] Update deployment notes if procedures changed
- [ ] Record the new image versions deployed:
  - Backend image tag: _______________
  - Frontend image tag: _______________

### Monitoring (if configured)

- [ ] Verify monitoring services are running:
  ```bash
  sudo systemctl status docker-monitor.service
  sudo systemctl status dashboard-log-watcher.service
  ```

- [ ] Test email alerts are working (optional):
  ```bash
  # Trigger a test by temporarily stopping a dashboard
  docker stop sapphire-dashboard
  # Wait for alert, then restart
  docker start sapphire-dashboard
  ```

---

## Quick Reference Commands

```bash
# Check all SAPPHIRE containers
docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# View recent logs for all SAPPHIRE containers
for c in $(docker ps --filter "name=sapphire" --format "{{.Names}}"); do
  echo "=== $c ==="
  docker logs $c --tail 10
done

# Check Luigi task history
curl -s http://localhost:8082/api/task_list | python3 -m json.tool | head -50

# Check disk usage
df -h /data ${LOG_DIR}

# Check Docker disk usage
docker system df
```

---

*Last updated: 2026-06-04*
