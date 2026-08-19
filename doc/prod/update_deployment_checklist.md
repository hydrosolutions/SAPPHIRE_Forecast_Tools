# SAPPHIRE Forecast Tools - Update Deployment Checklist

This checklist guides you through a **routine update** of an existing SAPPHIRE Forecast Tools deployment. It covers refreshing the repository, pulling new Docker images, applying schema migrations, restarting the microservices and Luigi stacks, verifying health, and rolling back if needed.

> **First-time deployment?** Use `doc/prod/first_deploy_checklist.md` instead. That doc covers the one-time steps (SSH tunnel setup, initial schema preparation, `RunInitializeWorkflow`, historical backfill) that are *not* part of a routine update.
>
> **Deploying long-term (quarter/season) forecast changes?** Use [`doc/prod/long_term_deploy_runbook.md`](long_term_deploy_runbook.md) for the ordered long-term flow (image scope, env/config preflight, full-history recalc, verification, deprecated-model cleanup). It references this checklist for the image-deploy mechanics.
>

## Operator setup — set these once per session [Required]

Before running any command below, set these variables once in your shell session. Everywhere in this doc that you see `${ORG_SLUG}`, `${DATA_DIR}`, `${ENV_FILE_PATH}`, `${LOG_DIR}` or `${LT_ISSUE_DAY}`, those are the values being substituted.

```bash
# Operator: set these variables once per deployment session. The rest of
# this checklist references them. Adapt to your deployment.
export ORG_SLUG=tjhm                                             # one of: kghm, tjhm, uzhm
export DATA_DIR=/data/taj_data_forecast_tools                    # adapt to your data path
export ENV_FILE_PATH="${DATA_DIR}/config/.env_develop_${ORG_SLUG}"
export LOG_DIR=/home/ubuntu/logs                                 # adapt to your logs path

# Long-term forecast issue day(s) — read from THIS deployment's configs, do not
# copy from another deployment. Every file in ${DATA_DIR}/config/long_term_configs/
# carries an "operational_issue_day"; the cron day field must list those values:
#   ls ${DATA_DIR}/config/long_term_configs/*.json |
#     xargs -I{} sh -c 'printf "%-22s " "$(basename {})"; grep -o "\"operational_issue_day\":[^,]*" {}'
# Known values: tjhm = 1 (all modes) | kghm = 10 and 25. A deployment with no
# long_term_configs/ directory does not run long-term forecasts at all — leave
# cron entries (4) and (4b) out entirely.
export LT_ISSUE_DAY=1                                            # e.g. "1" (tjhm) or "10,25" (kghm)
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

### 1.2 iEasyHydro HF Connectivity (if applicable)

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

- [ ] **Pause the operational cron schedule first.** Stopping the stack below while cron is live risks a scheduled forecast/maintenance job firing into a half-updated system (services down, DB schema mid-migration). Your crontab was backed up in §1.5; clear the live schedule now — it is re-installed in §2.5, or restored from backup on rollback (§5.8):
  ```bash
  crontab -l > ~/crontab_pre_update_$(date +%Y%m%d_%H%M%S).txt   # snapshot (redundant if §1.5 done — harmless)
  crontab -r                                                     # pause: no scheduled jobs until re-installed
  crontab -l 2>&1 | head -1                                      # expect: "no crontab for <user>"
  ```
  Do NOT skip the re-install/restore step at the end — a deployment that leaves cron cleared silently stops all forecasts.

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

TODO: remove all unused images
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
| `SAPPHIRE_SKILL_LEAD_AWARE` | Per-lead skill & ensembles — **default OFF, opt-in per deployment**. See [`long_term_deploy_runbook.md`](long_term_deploy_runbook.md) § *Lead-aware skill*. Turning this on (including re-enabling) requires running §3.6's full-history recalc as soon as possible afterward. | `true` **only** if this deployment's long-term configs carry **both** `operational_month_lead_time` **and** `operational_issue_day`, **and** a full recalc is planned; otherwise leave absent/`false`. Per-deployment readiness: see the dated table in [`long_term_deploy_runbook.md`](long_term_deploy_runbook.md) |

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
  - [ ] `PUBLIC_BULLETIN_BASE_URL` — new with the bulletin-share feature (PP-039). Optional (defaults to `http://localhost:8000`), but set it to the deployment's public HTTPS gateway host (e.g. `https://<gateway-host>`) so shareable bulletin links resolve for third parties. If unset, generated links point at localhost and are not externally reachable.
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

TODO: need to set snow data display start date in .env 
# Define the start date for which the snow data should be retrieved. This is relevant for the snow data visualization in the forecast dashboard. The format is MM-DD. Note that the year is not relevant, as this variable defines the day of the year from which on snow data should be visualized in the dashboard. For example, if you set ieasyhydroforecast_SNOW_DISPLAY_START_MMDD to 01-01, snow data will be visualized in the dashboard from January 1st on. If you set it to 15-10, snow data will only be visualized from October 15th on.
ieasyhydroforecast_SNOW_DISPLAY_START_MMDD=09-01
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
 
TODO: No need to pull them manually, they are pulled automatically when testing the crontabs. 
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

- [ ] **Pull preprocessing gateway image** (required for gateway maintenance and yearly snow norm/stat recalculation)
  ```bash
  docker pull mabesa/sapphire-prepgateway:local
  ```

**Optional (based on deployment configuration):**

If `ieasyhydroforecast_run_ML_models=true` in your .env file:

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
TODO: no need to build microservices, we can directly run up -d command further below. 
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

- [ ] **One-time: configure Docker log rotation if not already present.** Without
  `/etc/docker/daemon.json`, per-container `*-json.log` files grow unbounded and
  can fill the disk over time. Check and, if missing, apply it now — the daemon
  restart only affects containers created afterward, so doing it here (stack down
  from §2.1) means the `up -d` below picks up the new limits cleanly:
  ```bash
  if [ ! -f /etc/docker/daemon.json ]; then
    echo '{ "log-driver": "json-file", "log-opts": { "max-size": "10m", "max-file": "3" } }' \
      | sudo tee /etc/docker/daemon.json
    sudo systemctl restart docker
  fi
  docker info --format '{{.LoggingDriver}}'   # expect: json-file
  ```
  This is the retrofit counterpart to `first_deploy_checklist.md` §1.3.

- [ ] **Start the microservices stack** — use the wrapper, which loads the
  deployment config first.
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/restart_sapphire_stack.sh ${ENV_FILE_PATH}
  ```

  > **⚠️ Do NOT bring the stack up with a bare `docker compose --env-file … up -d`.**
  > Several compose variables — `ieasyhydroforecast_data_ref_dir`,
  > `ieasyhydroforecast_container_data_ref_dir`, `ieasyhydroforecast_url_pentad`,
  > … — are **derived and exported by `read_configuration`** (in
  > `bin/utils/common_functions.sh`), **not** literal keys in the `.env`. A bare
  > compose command cannot set them, so they resolve to **blank strings**. That
  > turns the dashboard's volume mount
  > `${ieasyhydroforecast_data_ref_dir}/bin:${ieasyhydroforecast_container_data_ref_dir}/bin`
  > into a literal **`/bin:/bin`**, mounting the **host's** bash over the
  > container's libc and crash-looping the dashboard with
  > `bash: libc.so.6: version 'GLIBC_2.38' not found (required by bash)` — a
  > failure that looks identical to a broken image but is purely this config trap.
  >
  > If you must call `docker compose` directly (e.g. to (re)start a single
  > service), **load the config into your shell first**:
  > ```bash
  > source bin/utils/common_functions.sh
  > read_configuration ${ENV_FILE_PATH}
  > docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml up -d --force-recreate dashboard
  > ```
  > **Watch the output**: if you see
  > `WARN The "ieasyhydroforecast_data_ref_dir" variable is not set. Defaulting to a blank string.`,
  > the config was NOT loaded — stop and fix that first, or the dashboard mount
  > will be `/bin:/bin`.

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

> **This is where cron comes back up** after the §2.1 pause — installing the
> canonical block below re-enables the schedule. If you paused with `crontab -r`,
> the diff step will show an empty current crontab; that is expected, so install
> the full block fresh instead of diffing. Do not leave this section without a
> non-empty `crontab -l`.

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

  # Daily DB backup at 01:00 UTC (pg_dump-based, 3-day retention).
  # The `cd` is REQUIRED: backup_sapphire_db.sh resolves sapphire/docker-compose.yml
  # and its default env file RELATIVE to the working directory (`COMPOSE_DIR="sapphire"`,
  # :47-49) and aborts with "Must run from the repository root" otherwise. Cron runs
  # from $HOME, so a line without the `cd` fails every night and backs up nothing.
  0 1 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/backup_sapphire_db.sh -e ${ENV_FILE_PATH} -d /var/backups/sapphire -r 3 >> ${LOG_DIR}/sapphire_db_backup_$(date +\%Y\%m\%d).log 2>&1

  # Weekly Docker cleanup at 00:30 UTC Sundays: prune dangling images + build
  # cache so /var/lib/docker does not grow unbounded across deploys. Volume-safe:
  # never removes named volumes (the four Postgres DBs) or tagged images. Do NOT
  # add `-a` to image prune or `--volumes` to any prune here.
  30 0 * * 0 docker image prune -f && docker builder prune -f >> ${LOG_DIR}/sapphire_docker_prune_$(date +\%Y\%m\%d).log 2>&1

  # Weekly prune at 01:30 UTC Sundays of stale pre-update DB backup dirs (>30
  # days). The daily backup's retention only prunes top-level dumps, not the
  # pre_update_*/ subdirs created before each deployment update (see §1.5).
  30 1 * * 0 find /var/backups/sapphire -maxdepth 1 -type d -name 'pre_update_*' -mtime +30 -exec rm -rf {} + >> ${LOG_DIR}/sapphire_backup_prune_$(date +\%Y\%m\%d).log 2>&1

  # (1) Gateway Preprocessing at 03:00 UTC. Independent of daily data.
  0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_gateway_preprocessing_$(date +\%Y\%m\%d).log 2>&1

  # (2) Pentadal Forecast at 04:00 UTC. Luigi triggers runoff preprocessing.
  0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_pentadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (3) Decadal Forecast at 05:00 UTC.
  0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_decadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (4) Long-Term Forecast at 06:00 UTC on THIS deployment's issue day(s).
  # ${LT_ISSUE_DAY} MUST equal the "operational_issue_day" values in
  # ${DATA_DIR}/config/long_term_configs/*.json — see Operator setup above.
  # tjhm = 1 | kghm = 10,25. Do NOT copy another deployment's days.
  #
  # Why this is not cosmetic: the run is gated twice, with different widths.
  # lt_schedule_query.py:52 admits a mode when it is within ISSUE_DAY_TOLERANCE
  # (currently 10) days of its issue day, but lt_utils.py:202 refuses to execute
  # a model more than 5 days off. A cron day 6-10 days from the issue day is
  # therefore ADMITTED and then SKIPPED: the run writes nothing and still exits
  # 0. Scheduling on the issue day itself (distance 0) is the only safe choice.
  # This is issue LTF-007; until it is fixed, a wrong cron day fails silently.
  0 6 ${LT_ISSUE_DAY} * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_long_term_$(date +\%Y\%m\%d).log 2>&1

  # (4b) Long-term skill metrics recalculation at 10:00 UTC, four hours after
  # each long-term forecast — so its day field tracks ${LT_ISSUE_DAY} too.
  # Unlike (4) this job does not gate on the issue day, so a mismatched day
  # still runs; keeping the days aligned only preserves the intended
  # forecast-then-score ordering. Refreshes MONTHLY,
  # QUARTERLY, and SEASONAL skill metrics so long-term skill tiles on the
  # dashboard do not stay stale for a year. Log-and-continue: one failing
  # mode does not block the others, but the job exits non-zero so errors
  # still surface in the cron log.
  0 10 ${LT_ISSUE_DAY} * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/bimonthly_long_term_skill_metrics_recalculation.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_bimonthly_lt_skill_recalc_$(date +\%Y\%m\%d).log 2>&1

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
  # Owner decision 2026-08-19: run it at the END of the snow year, before the
  # new accumulation season, rather than on 1 January which would move the
  # norms mid-season. Deployments that previously used 1 January should move
  # to 31 August unless they have a stated reason not to.
  # Consolidated Luigi wrapper. Supersedes legacy bin/yearly_snow_norm_recalculation.sh
  # (kept on origin for manual / debugging use only).
  0 2 31 8 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_snow_norm_$(date +\%Y\%m\%d).log 2>&1

  # (9) Yearly runoff hydrograph aggregation at 03:00 UTC on January 1.
  # Replaces the retired YearlyMonthlyNormsRecalculation Luigi task. Builds
  # the long-horizon monthly + April–September seasonal hydrograph view used
  # by the dashboard.
  0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_runoff_hydrograph_$(date +\%Y\%m\%d).log 2>&1
  ```

- [ ] **Remove retired entries that fail silently** — these were valid in earlier
      versions and are still present on deployments updated from them:

  | Retired entry | What happens if left | Replace with |
  |---|---|---|
  | `run_periodic_maintenance.sh monthly_norms` | **Fails silently, and the cron log looks successful.** The wrapper's argument guard only rejects an *empty* task type (`bin/run_periodic_maintenance.sh:23-28`), so `monthly_norms` passes through to Luigi, where `PeriodicMaintenance.requires()` raises `ValueError: Unknown task_type` (`apps/pipeline/pipeline_docker.py:2049-2057`). The wrapper has no `set -e`, never captures the exit code of `docker compose run`, and ends with two unconditional `echo`s (`:82-90`) — so it **exits 0** and prints "task submitted". Every 1 January the long-horizon hydrograph view is silently not rebuilt. This is **INFRA-023** (`issues/high_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`). | entry **(9)** `bin/yearly_runoff_hydrograph_aggregation.sh` |
  | `daily_preprunoff_maintenance.sh`, `daily_ml_maintenance.sh`, `daily_linreg_maintenance.sh`, `daily_postprc_maintenance.sh`, `daily_gateway_maintenance.sh`, `daily_update_sapphire_frontend.sh` | Duplicates work the Luigi DAG already does, out of dependency order | entry **(5)** `bin/run_daily_maintenance.sh` |

- [ ] **Verify crontab was saved correctly**
  ```bash
  crontab -l
  ```

- [ ] **Verify the long-term cron day matches this deployment's configs** — the
      single most common silent failure on a long-term deployment
  ```bash
  # what the configs say
  grep -h -o '"operational_issue_day":[^,]*' ${DATA_DIR}/config/long_term_configs/*.json | sort -u
  # what cron will do
  crontab -l | grep run_long_term_forecasts
  ```
  The day-of-month field of the cron entry must contain exactly the
  `operational_issue_day` values above. If they differ by 6-10 days the run is
  admitted by the scheduler and then skipped by the model, writing nothing and
  exiting 0 — nothing in the log or the exit code will tell you (LTF-007).

- [ ] **Confirm no line ends in a stray character after `2>&1`.** A trailing `#`
      or space-less comment makes the shell report `1#: ambiguous redirect`, so the
      command never runs while the redirect still creates an empty log file — a
      failure that looks exactly like "the job ran and produced nothing".
  ```bash
  crontab -l | grep -nE '2>&1[^ ]' && echo "^^ FIX THESE" || echo "OK: no malformed redirects"
  ```

- [ ] **Confirm shell variables were substituted.** Cron does not expand them; a
      surviving `${LOG_DIR}` sends the redirect to an unwritable path and the job
      never runs.
  ```bash
  crontab -l | grep -n '\${' && echo "^^ SUBSTITUTE THESE" || echo "OK: no unexpanded variables"
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
> already brought the stack up in §2.4.5, do so now (use the wrapper — it runs
> `read_configuration` first; a bare `docker compose up` leaves the dashboard's
> derived mount vars blank and crash-loops it on `/bin:/bin` — see §2.4.5):
> ```bash
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

- [ ] **Verify snow stat columns after snow norm/stat recalculation**

  The dashboard snow panel uses `mean`, `q05`..`q95`, `previous`, and
  `current`. If a hydrological display window crosses Jan 1 (for example
  `ieasyhydroforecast_SNOW_DISPLAY_START_MMDD=09-01`), the current calendar
  year must have these stat fields populated; otherwise the percentile bands,
  mean, and previous-season line stop at Jan 1 while the current-season line
  continues.

  ```bash
  docker exec sapphire-preprocessing-db psql -U postgres -d preprocessing_db -c \
  "select extract(year from date) y, count(*) rows, count(mean) mean, count(q05) q05, count(previous) prev, count(current) curr from snow where code='19999' and snow_type='SWE' and date between '2025-09-01' and '2026-08-31' group by 1 order by 1;"
  ```

  Replace `19999` and the date range with a non-sensitive representative
  station/window for the deployment. Acceptance: both years in the displayed
  snow window have non-zero `mean`, `q05`, and `prev` counts.

- [ ] **Run bimonthly long-term skill metrics recalculation**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/bimonthly_long_term_skill_metrics_recalculation.sh ${ENV_FILE_PATH}
  ```
  This bare form recalculates only the `current_year − 20` window **when no
  start-year variable is set** — if the deployment `.env` sets
  `SAPPHIRE_SKILL_METRICS_START_YEAR`, that value wins over the default (see
  §3.6's pre-check for how this is verified). If
  this deployment is turning `SAPPHIRE_SKILL_LEAD_AWARE` from off to on —
  including re-enabling it after a rollback or a temporary disablement, not
  just the first enablement — run the **full-history recalc** in §3.6 instead;
  the 20-year window is not sufficient for enablement.

- [ ] **Run yearly runoff hydrograph aggregation**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH}
  ```

- [ ] **Monitor progress** in Luigi UI at http://localhost:8082
- [ ] **Check logs** for errors after each command completes

---

TODO: need to verify that all relevant files are present in the data folder (e.g. bulletin templates). Update if necessary. 

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

### 3.5 Discharge-Aggregation Historical Backfill (one-time, after the iEH HF parity update)

**When to run this step:** Once, after deploying the discharge-aggregation
parity change (PR #406, merged as `c80e74c2`) that made pentad/decad/month/
quarter/season **actuals** match iEasyHydro HF to 3 significant figures. The
parity fix only corrects values written by *new* runs — previously-stored
aggregate rows keep their old (banded / mean-of-days) values until backfilled.
Routine tag-bump-only updates do NOT need this step.

**Prerequisites:** run this only after §3.1–§3.3 are green. This wrapper does
**not** start Docker or open the iEasyHydro HF tunnel — the microservices stack
must be up (`SAPPHIRE_API_URL` reachable, default `http://localhost:8000`) and
the iEasyHydro HF SDK / SSH tunnel must be reachable, because the backfill reads
daily runoff from the preprocessing API and period actuals from the HF SDK.
The wrapper runs the backfill directly on the host via `uv run` (it does not
pull or build a Docker image), so the host must also have `uv` installed and
the `apps/preprocessing_runoff` environment synced (`uv sync` in that
directory) before running it.

**Reference:** general backfill methodology and idempotency model are in
[`doc/prod/historical_backfill_runbook.md`](./historical_backfill_runbook.md).

**Safety rails (built into the tool):** a dry-run computes + diffs without
writing; a live run writes a pre-write snapshot for the rows it can read, writes
per horizon, then re-reads and **raises on any mismatch**. The backfill is
**idempotent** — it upserts the same rows on re-run, so it is safe to re-run if
interrupted. Snapshot reads are best-effort: treat any `snapshot_existing …
treating as no existing rows` warning as an *incomplete* snapshot and
investigate before relying on it for rollback. Always dry-run first and inspect
the diff.

- [ ] Dry-run first (no writes) and review the JSON diff report. Use `--years N`
      for the N most-recent complete years, or `--target-year YYYY` for one year:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/backfill_discharge_aggregation.sh ${ENV_FILE_PATH} --years 3 --dry-run
  # single year instead:
  # bash bin/backfill_discharge_aggregation.sh ${ENV_FILE_PATH} --target-year 2025 --dry-run
  ```
- [ ] Inspect the dry-run diff — a large `changed`/`added` count is EXPECTED (that
      is the parity correction being applied to historical rows).
- [ ] Expect non-fatal iEasyHydro HF **norm** warnings (`No path provided` / SDK
      norm lookup). These skip only the affected station/horizon rows (mostly
      monthly), not the whole run — review the summary counts and spot-check the
      skipped stations rather than treating the warnings as a failure.
- [ ] Live backfill (writes, with snapshot + post-write verification):
  ```bash
  bash bin/backfill_discharge_aggregation.sh ${ENV_FILE_PATH} --years 3
  ```
- [ ] Confirm the run ended with per-year `verification OK`. A verification
      mismatch raises and stops the run — do NOT ignore it.
- [ ] Spot-check a few stations' month/decad values in the dashboard against
      iEasyHydro HF (3 significant figures).

---

### 3.6 Lead-Aware Skill Full-History Recalc (run on every SAPPHIRE_SKILL_LEAD_AWARE off→on transition)

**When to run this step:** On every transition of `SAPPHIRE_SKILL_LEAD_AWARE`
from off to on for this deployment — set in §2.3 of this checklist — not only
the first enablement. Re-enabling after a rollback or a temporary
disablement counts too: rows written while the flag was off are
collapsed/single-lead and would otherwise mix with existing per-lead rows.
Routine tag-bump updates, and deployments leaving the flag off, skip this
step entirely.

Run this **as soon as possible after the flag is enabled** — §2.3 (flag set),
§2.4.5 (stack recreated), and §2.5 (cron re-installed) all run before this
section, so there is a window where the deployment runs flag-ON against a
mixed-convention DB. Until this step completes, the dashboard serves a mix of
single-lead and per-lead skill/ensemble rows.

**Reference:** [`long_term_deploy_runbook.md`](./long_term_deploy_runbook.md) §
*Lead-aware skill & ensembles (`SAPPHIRE_SKILL_LEAD_AWARE`) — opt-in per
deployment* (prerequisites + per-deployment readiness table) and §§ *Phase 3 —
Regenerate* / *Phase 4 — Verify* (recalc + verification queries).

**Hard prerequisite (fail-loud):** every long-term config JSON for this
deployment must carry BOTH `operational_month_lead_time` AND
`operational_issue_day`. Under the flag, the write path raises and aborts if
`operational_issue_day` is missing — by design, so it never silently scores
the wrong rows. Run the prerequisite check in the runbook's *Lead-aware skill*
section before proceeding. The runbook's per-deployment readiness table is
dated — re-verify it against the actual configs on this server before
enabling, do not trust it as-is.

- [ ] Confirmed the prerequisite check passes for this deployment (both fields
      present on every long-term config JSON).

**Backup first.** Same backup helper as §1.5 / Phase 3 of the runbook. This
step may run in a later session than §1.5, so re-create the backup directory
rather than assume `$DB_BACKUP_DIR` is still set. `bin/backup_sapphire_db.sh`
requires the target directory to already exist and be writable — it exits
with an error (loudly, not a silent skip) if it doesn't:

```bash
cd /data/SAPPHIRE_Forecast_Tools
export DB_BACKUP_DIR="/var/backups/sapphire/pre_update_$(date +%Y%m%d_%H%M%S)"
sudo mkdir -p "$DB_BACKUP_DIR"
sudo chown "$USER" "$DB_BACKUP_DIR"
bash bin/backup_sapphire_db.sh -e "${ENV_FILE_PATH}" -d "$DB_BACKUP_DIR" -r 30
```

- [ ] Ran the DB backup above before starting the recalc.

**Cron disable / restore.** The recalc below uses the same wrapper and the
same fixed per-mode container names as the cron-scheduled
`bin/bimonthly_long_term_skill_metrics_recalculation.sh` (§2.5: `0 10
${LT_ISSUE_DAY} * *` UTC), and this run is long enough to overlap a scheduled
firing — do not
assume it will finish before the next slot. Use the same mechanism as §3.4
(runbook §4.2). **This clears the operator's entire crontab, not just the
bimonthly skill entry** — every SAPPHIRE pipeline job stops until the
restore step below runs:

This snapshot is §3.6's own working copy of the **post-update** (§2.5)
schedule — it is a different file from §1.5's `crontab_backup.txt`, which
holds the **pre-update** schedule and is what §5.8 (Emergency rollback)
restores from. Do not confuse the two or hand the wrong one to a rollback.

```bash
SNAPSHOT="$DB_BACKUP_DIR/crontab_before_leadaware_recalc.txt"
crontab -l > "$SNAPSHOT"
echo "Cron snapshot: $SNAPSHOT"   # note this path down — do not rely on $DB_BACKUP_DIR
                                   # surviving to the restore step
if [ -s "$SNAPSHOT" ]; then
  crontab -r
  crontab -l 2>/dev/null || echo "(crontab cleared, verified empty)"
else
  echo "ABORT: snapshot missing or empty at $SNAPSHOT — crontab left untouched."
fi
```
It must be impossible to reach `crontab -r` without a non-empty snapshot
already on disk — the `-s` test is what gates it, not the exit status of the
redirect above it.

- [ ] Cleared and snapshotted cron before starting the run — **noted the
      absolute snapshot path** printed above (the restore step below may run
      in a later shell session where `$DB_BACKUP_DIR` is unset).

> If the recalc fails or is abandoned partway, cron must still be restored
> before leaving the host — do not leave the deployment with an empty
> crontab. If the noted snapshot path is lost, do NOT use
> `update_data_migration_runbook.md` §9.4 — that section recovers snapshots
> written by `bin/initialize_regenerate_hooks.sh` under
> `${ieasyhydroforecast_data_root_dir}/logs/regenerate_hooks/`, a different
> mechanism from this section's manual snapshot. Instead, locate this
> section's file by name under `/var/backups/sapphire/`:
> ```bash
> find /var/backups/sapphire -name crontab_before_leadaware_recalc.txt -printf '%T@ %p\n' \
>   | sort -rn | head -1
> ```
> then `crontab <that path>`. If no match is found, fall back to §2.1's
> `~/crontab_pre_update_*.txt` or §1.5's `$BACKUP_DIR/crontab_backup.txt` —
> both hold the **pre-update** schedule (§5.8's rollback source), not the
> post-update canonical one restored here, so treat them as a last resort,
> not an equivalent substitute.

- [ ] **Confirm `SAPPHIRE_SKILL_METRICS_START_YEAR` cannot silently override
      the start year below.** `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
      reads `SAPPHIRE_SKILL_METRICS_START_YEAR` FIRST and only falls back to
      `SAPPHIRE_RECALC_START_YEAR` if it is unset. The value that matters is
      the one in the **deployment `.env`**, which is loaded inside the
      container — if it sets `SAPPHIRE_SKILL_METRICS_START_YEAR` to a
      different year, the recalc below silently uses THAT window instead of
      `2000`, exits 0, and gives no indication the wrong years were
      recalculated. The shell-level check is a deliberately conservative
      belt-and-braces echo only:
      `bin/utils/run_skill_metrics_recalc.sh` (docker run block, lines
      ~78-93) forwards only `RECALC_START_YEAR_OVERRIDE_ARGS` plus a fixed
      set of `-e` vars — it never forwards `SAPPHIRE_SKILL_METRICS_START_YEAR`
      from the shell into the container, so a shell-only value has no effect
      on this run. Check both:
  ```bash
  grep -n "SAPPHIRE_SKILL_METRICS_START_YEAR" "${ENV_FILE_PATH}"
  echo "shell (belt-and-braces only, not forwarded to the container): ${SAPPHIRE_SKILL_METRICS_START_YEAR:-<unset>}"
  ```
  The `.env` grep must be absent, or already set to the same start year used
  below — that is the check that matters.

- [ ] **Run the full-history recalc.** This differs from the bare §2.6
      invocation ONLY by the explicit start year — the bare form's default
      `current_year − 20` window is NOT sufficient for enablement. `2000` is
      the runbook's conventional safe value; the actual requirement is that
      the start year is at or before this deployment's earliest archived
      forecast year:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  SAPPHIRE_RECALC_START_YEAR=2000 bash bin/bimonthly_long_term_skill_metrics_recalculation.sh "${ENV_FILE_PATH}"
  ```
  An empty or unset `SAPPHIRE_RECALC_START_YEAR` does **not** fail loudly:
  `bin/utils/run_skill_metrics_recalc.sh` only forwards
  `-e SAPPHIRE_RECALC_START_YEAR=...` to the container when the variable is
  non-empty, so an empty value is silently omitted and the container falls
  back to its `current_year − 20` default — exactly the outcome this section
  exists to prevent. Confirm the variable is set and non-empty in the command
  above before running it.

- [ ] **Confirm the start year actually predates the archive.** `2000` is a
      conventional safe value, not a verified one for this deployment.
      Cross-check it against `MIN(date)` from the runbook's Phase 4 aggregate
      query (run as part of "Verify per-lead rows landed" below). Pass/fail
      rule — both failing cases require a re-run with an earlier
      `SAPPHIRE_RECALC_START_YEAR`:
  - **FAIL — `MIN(date)` earlier than the chosen start year** (e.g. archive
    from 1990 with start year 2000): direct proof rows exist outside the
    recalc window and were never recalculated.
  - **FAIL — `MIN(date)` exactly at the start-year boundary** (e.g.
    `2000-01-01`): the archive may extend earlier and is being truncated by
    the window.
  - **PASS — `MIN(date)` comfortably later than the start year**: only this
    case needs no re-run.

- [ ] **Verify per-lead rows landed**, using the runbook's Phase 4
      aggregate-only verification queries:
  - taj: `QUARTER hv=0`, `SEASON hv=0`
  - kyg: `QUARTER hv=1`, `SEASON hv ∈ {3,2,1,0}`
  - month: one row per configured lead (`operational_month_lead_time`)

  Use sentinel station codes (e.g. `19999`) in any ad-hoc SQL — never real
  station codes. SQL must use DB enum names (`ENSEMBLE_MEAN`, `LR_BASE`,
  `LR_SM`, `NAIVE_MEAN`, `SKILLED_MEAN`), never API values such as `EM` or
  `LR_Base`.

  **Limits of this check:** Phase 4 queries `long_forecasts` (bucket
  presence, date span, `EM = mean(LR_BASE, LR_SM)` composition) — it does
  **not** query `skill_metrics` and does not prove monthly skill rows
  converged. A Phase 4 pass is necessary but not sufficient. Do not treat a
  Phase 4 pass alone as proof the skill tables converted.

- [ ] **`skill_metrics` per-lead convergence — explicit outcome required.**
      `skill_metrics` is schema owned by `sapphire/services/` (CLAUDE.md §
      Ownership Boundaries) — do not write ad-hoc SQL against it here.
      Instead, record ONE of the following before closing out §3.6:
  - Confirmed with the service owner that `skill_metrics` rows carry the
    expected per-lead `horizon_value` for this deployment, **or**
  - Not confirmed — residual risk explicitly accepted and left unverified
    for this deployment.

  Tracked as **DOC-005**
  (`doc/plans/issues/mid_prio_gi_draft_doc_long_term_runbook_skill_verification_gap.md`).
  This checkbox exists so the gap is a decision the operator consciously
  makes and records, not a paragraph they can read past.

- [ ] **Restore cron** from the snapshot path noted above, now that the
      recalc and verification are complete. Use the literal absolute path you
      wrote down — `$DB_BACKUP_DIR` only works if this is still the same
      shell session as the snapshot step:
  ```bash
  crontab /var/backups/sapphire/pre_update_<TIMESTAMP>/crontab_before_leadaware_recalc.txt   # the noted path
  # same-session convenience, only if $DB_BACKUP_DIR is still set here:
  # crontab "$DB_BACKUP_DIR/crontab_before_leadaware_recalc.txt"
  crontab -l   # confirm restored
  ```

**Rollback:** setting `SAPPHIRE_SKILL_LEAD_AWARE=false` (or removing the
line) changes how NEW rows are computed and read going forward — it does
**not** delete or collapse the per-lead rows already written while the flag
was on, and it does not touch monthly per-lead skill/ensemble stratification
at all: per `apps/iEasyHydroForecast/skill_lead_aware_flag.py` (lines 11-14),
the PP-038 monthly per-lead stratification is unconditional trunk behavior
and is not gated by this flag. There is no recalc that deletes or collapses
already-persisted per-lead rows. Rollback is therefore **not** a clean
reversal of the data — the DB backup taken above is the actual recovery path
if this enablement needs to be undone.

Skipping this step is the failure mode it exists to prevent: the flag is on,
new rows are written per-lead, old rows stay single-lead, and the
dashboard/skill tiles silently mix the two conventions.

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

> **Now largely automated.** The weekly cron added in §2.5
> (`docker image prune -f && docker builder prune -f`, Sundays 00:30 UTC) keeps
> dangling images and build cache from accumulating across deploys — the usual
> cause of the disk filling on these hosts. Run the manual prune below only if
> the disk is under pressure between weekly runs.

Remove unused Docker resources:

- [ ] Remove dangling images and build cache (volume-safe — never add `--volumes`):
  ```bash
  docker image prune -f && docker builder prune -f
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

- [ ] Start the microservices stack (needed so `alembic` can connect to its DB).
  Use the config-loading wrapper (a bare `docker compose up` blank-mounts the
  dashboard — see §2.4.5):
  ```bash
  bash bin/restart_sapphire_stack.sh ${ENV_FILE_PATH}
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
  If not running, start it with the config-loading wrapper (not a bare
  `docker compose up` — see §2.4.5): `bash bin/restart_sapphire_stack.sh ${ENV_FILE_PATH}`.

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

### 5.8 Restore cron schedule [Emergency only]

The §2.1 pause cleared the live crontab. After a rollback you want the
**pre-update** schedule back (not the new canonical block from §2.5), so restore
from the backup taken in §1.5 / §2.1.

- [ ] Reinstall the pre-update crontab from backup:
  ```bash
  crontab "$BACKUP_DIR/crontab_backup.txt"   # or the ~/crontab_pre_update_*.txt snapshot from §2.1
  ```

- [ ] Verify it is back and the cron service is running:
  ```bash
  crontab -l | grep -v '^#' | head
  sudo systemctl status cron
  ```

---

## 6. FINAL CHECKLIST [Required]

Complete this summary checklist before considering the update complete.

### Services Running [Required]

- [ ] Luigi daemon is running and accessible at port 8082
- [ ] Dashboard is running and accessible at port 5006
- [ ] All containers show "healthy" status

### Crontabs Configured [Required]

- [ ] **Confirm cron was re-enabled after the §2.1 pause** — `crontab -l` is
  non-empty (the §2.5 canonical block on a normal update, or the restored
  pre-update schedule after a rollback). This is the guard against a deployment
  that silently leaves all forecasts disabled.

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

### Monitoring (if configured) [Required]

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
