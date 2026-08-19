# SAPPHIRE Forecast Tools - First Deployment Checklist

This checklist covers one-time setup steps required when deploying SAPPHIRE Forecast Tools to a new server. These steps install permanent services (such as the SSH tunnel systemd unit) and prepare the server for routine forecast operations. Run this checklist once per new deployment; for ongoing updates use `doc/prod/update_deployment_checklist.md`.

> **First deploy vs. update:** "Stop services" and "Backup Critical Files" sections do not apply on a first deploy — there are no prior services to stop and no prior state to preserve. They are covered in `doc/prod/update_deployment_checklist.md`.
>
> **Cron installation** is covered in section 12 of this doc. The same canonical cron block is reproduced in `doc/prod/update_deployment_checklist.md` §2.5 so routine-update operators can diff against the running crontab without consulting two sources.

## Operator setup — set these once per session

Before running any command below, set these variables once in your shell session. Everywhere in this doc that you see `${ORG_SLUG}`, `${DATA_DIR}`, `${ENV_FILE_PATH}`, or `${LOG_DIR}`, those are the values being substituted.

```bash
# Operator: set these variables once per deployment session. The rest of
# this checklist references them. Adapt to your deployment.
export ORG_SLUG=tjhm                                             # one of: kghm, tjhm, uzhm
export DATA_DIR=/data/taj_data_forecast_tools                    # adapt to your data path
export ENV_FILE_PATH="${DATA_DIR}/config/.env_develop_${ORG_SLUG}"
export LOG_DIR=/home/ubuntu/logs                                 # adapt to your logs path

# Long-term forecast issue day(s) — read from THIS deployment's configs:
#   grep -h -o '"operational_issue_day":[^,]*' ${DATA_DIR}/config/long_term_configs/*.json | sort -u
# tjhm = 1 (all modes) | kghm = 10 and 25. No long_term_configs/ directory means
# this deployment does not run long-term forecasts — omit cron entries (4)/(4b).
export LT_ISSUE_DAY=1                                            # e.g. "1" (tjhm) or "10,25" (kghm)
```

---

## 1. Pre-flight server provisioning

These steps prepare the bare server before any SAPPHIRE services are installed.

### 1.1 Verify SSH access and sudo

- [ ] Confirm you have SSH access to the deployment server with a user that has `sudo` privileges:
  ```bash
  ssh <user>@<server-ip>
  sudo -v
  ```
- [ ] Verify a non-root unprivileged user exists for running SAPPHIRE containers (typically `ubuntu`). All cron jobs and Docker containers run as this user.

### 1.2 Verify disk space, hostname, and time

- [ ] Confirm disk space is sufficient for first-deploy. The full Docker image set plus four PostgreSQL volumes typically needs ≥ 30 GB free on `/var/lib/docker` and ≥ 20 GB free on the data directory:
  ```bash
  df -h /var/lib/docker "${DATA_DIR}" "${LOG_DIR}"
  ```
- [ ] Set hostname (for log clarity) and verify:
  ```bash
  hostnamectl
  ```
- [ ] Verify system clock is synchronised (cron schedules depend on this):
  ```bash
  timedatectl
  ```
  Expected: `System clock synchronized: yes` and `NTP service: active`.

### 1.3 Install Docker engine + Docker Compose plugin

If Docker is not already installed, follow the upstream Ubuntu instructions and ensure the non-root user is added to the `docker` group. Confirm:

```bash
docker --version
docker compose version
docker ps
```

The last command must succeed without `sudo` for the deploy user, and without `permission denied` errors.

- [ ] Configure Docker log rotation so container logs cannot grow unbounded.
  Without this, the per-container `*-json.log` files under
  `/var/lib/docker/containers/` grow without limit and will eventually fill the
  disk. Create `/etc/docker/daemon.json`:
  ```json
  {
    "log-driver": "json-file",
    "log-opts": { "max-size": "10m", "max-file": "3" }
  }
  ```
  Then restart Docker and confirm the driver is active:
  ```bash
  sudo systemctl restart docker
  docker info --format '{{.LoggingDriver}}'   # expect: json-file
  ```
  The limits apply only to containers created *after* the restart, so configure
  this before the stack is first brought up (section 7 onward). Retrofitting an
  existing deployment is covered in `doc/prod/update_deployment_checklist.md`.

### 1.4 Clone the repository

- [ ] Clone the repository into a predictable directory (commonly `/data/SAPPHIRE_Forecast_Tools`):
  ```bash
  sudo mkdir -p /data
  sudo chown "${USER}:${USER}" /data
  cd /data
  git clone https://github.com/hydrosolutions/SAPPHIRE_forecast_tools.git SAPPHIRE_Forecast_Tools
  cd SAPPHIRE_Forecast_Tools
  ```
- [ ] Check out the intended deployment branch or tag:
  ```bash
  git fetch --all
  git checkout <deployment-tag-or-branch>
  ```

### 1.5 Prepare the data directory layout

- [ ] Create the data directory layout used by the pipeline (paths align with the operator placeholder block above):
  ```bash
  mkdir -p "${DATA_DIR}/config" \
           "${DATA_DIR}/intermediate_data" \
           "${DATA_DIR}/intermediate_data/luigi_markers" \
           "${DATA_DIR}/templates" \
           "${DATA_DIR}/reports" \
           "${DATA_DIR}/bin" \
           "${DATA_DIR}/help" \
           "${LOG_DIR}"
  ```
- [ ] Copy or create the deployment `.env` at `${ENV_FILE_PATH}`. See section 5 of this doc for the required variables. **Do not commit this file to git.**
- [ ] Place the forecast-dashboard user guides in `${DATA_DIR}/help/` — the files `forecast_dashboard_user_guide_ru.html` and `forecast_dashboard_user_guide_en.html`. The dashboard serves this directory at `/help/` (via `--static-dirs`) for the header **Help** link; the guides embed operational screenshots and therefore live here, **not** in the git repo.

---

## 2. iEasyHydro HF connectivity (if applicable)

SAPPHIRE can run as a standalone forecast tool without iEasyHydro HF. However, certain organization configurations (e.g., `kghm`, `tjhm`) require connectivity to the iEasyHydro HF database for data retrieval. Check your `.env` file for `ieasyhydroforecast_organization` to determine if this applies.

**If your deployment requires iEasyHydro HF**, the configuration depends on your network setup:

**Option A: Same Network with Direct API Access**

If the SAPPHIRE server and iEasyHydro HF server are on the same local network AND the API is accessible from the network:

- [ ] Verify network connectivity to iEasyHydro HF server
  ```bash
  ping -c 3 <ieasyhydro-server-ip>
  ```
- [ ] Verify API is accessible directly (test common ports):
  ```bash
  curl -s http://<ieasyhydro-server-ip>:5555/api/v1/ | head
  curl -s http://<ieasyhydro-server-ip>:8000/api/v1/ | head
  ```
- [ ] If API responds, configure `.env` with direct IP:
  ```
  IEASYHYDROHF_HOST=http://<ieasyhydro-server-ip>:<port>
  ```

**Option B: Same Network but API Only on Localhost (SSH Tunnel Required)**

If iEasyHydro HF API only listens on localhost (common security configuration):

- [ ] Verify ping works but direct API access fails
- [ ] Test manual SSH tunnel:
  ```bash
  ssh -f -N -L <local-port>:localhost:<remote-port> <user>@<ieasyhydro-server-ip>
  curl -s http://localhost:<local-port>/api/v1/ | head
  ```
  Where `<remote-port>` is the port iEasyHydro HF listens on (typically 5555) and `<local-port>` is the port you want to use locally (can be the same or different, e.g., 5554 if 5555 is already in use by another tunnel).
- [ ] Configure `.env` to use localhost:
  ```
  IEASYHYDROHF_HOST=http://localhost:<local-port>
  ```
- [ ] **Set up permanent tunnel with autossh + systemd:**

  1. Install autossh:
     ```bash
     sudo apt-get update && sudo apt-get install -y autossh
     ```

  2. Verify SSH key authentication (no password prompt):
     ```bash
     ssh -o BatchMode=yes <user>@<ieasyhydro-server-ip> echo "OK"
     ```

  3. Create systemd service (`/etc/systemd/system/ieasyhydro-tunnel.service`):
     ```ini
     [Unit]
     Description=SSH Tunnel to iEasyHydro HF Server
     After=network-online.target
     Wants=network-online.target

     [Service]
     Type=simple
     User=<your-user>
     Environment="AUTOSSH_GATETIME=0"
     ExecStart=/usr/bin/autossh -M 0 -N -o "ServerAliveInterval=30" -o "ServerAliveCountMax=3" -o "ExitOnForwardFailure=yes" -L <local-port>:localhost:<remote-port> <user>@<ieasyhydro-server-ip>
     Restart=always
     RestartSec=10

     [Install]
     WantedBy=multi-user.target
     ```

  4. Enable and start:
     ```bash
     sudo systemctl daemon-reload
     sudo systemctl enable ieasyhydro-tunnel.service
     sudo systemctl start ieasyhydro-tunnel.service
     ```

- [ ] Verify permanent tunnel:
  ```bash
  sudo systemctl status ieasyhydro-tunnel.service
  curl -s http://localhost:<local-port>/api/v1/ | head
  ```

**Dashboard tunnel reachability (Docker bridge networking)**

The SSH tunnel binds to host loopback (`127.0.0.1:<local-port>`) for security. Dashboard containers run on a Docker bridge network and have their own loopback — they cannot reach the host loopback. Symptom: dashboard logs `iEHHF not available, falling back to file. Error: ... Connection refused` on the `/api/v1/auth/token-obtain` path. (The misleading hardcoded warning text may mention `hf.ieasyhydro.org` — do not chase that hostname; the real failure is on `localhost:<local-port>` inside the dashboard container.)

Fix: add `network_mode: host` to the dashboard service in `sapphire/docker-compose.yml` (and `bin/docker-compose-dashboards.yml` if its dashboards also call iEH HF). This makes the container share the host network namespace and reach the host loopback tunnel.

**Important:** This fix is **not currently committed** in the repository. Production servers running iEH HF behind a tunnel may have applied it as a server-side override. Coordinate with the team before relying on it; future code may rewrite `IEASYHYDROHF_HOST` for Docker symmetrically with `IEASYHYDRO_HOST` and obviate the workaround.

Verification step (run after the stack is up in section 3 below):
```bash
docker exec sapphire-dashboard curl -sf http://localhost:<local-port>/api/v1/
```
If this returns 200 from inside the dashboard container, the bridge-network workaround is in place. If it fails with `Connection refused` even though the host-side `curl http://localhost:<local-port>/api/v1/` succeeds, the dashboard service still needs `network_mode: host`.

**Option C: Different Networks (SSH Tunnel with Port Forwarding)**

If the SAPPHIRE server and iEasyHydro HF are on different networks (e.g., AWS server connecting to a local installation at a hydromet service), an SSH tunnel with port forwarding is required.

**Prerequisites:**
- SSH access to the iEasyHydro HF server (username, IP, port)
- The remote server's IT team must allow inbound SSH from the SAPPHIRE server IP
- The iEasyHydro HF API must be listening on a known port on the remote server (typically 5555)

**Step 1: Install autossh**

- [ ] Install autossh on the SAPPHIRE server:
  ```bash
  sudo apt-get update && sudo apt-get install -y autossh
  ```

**Step 2: Generate SSH key**

- [ ] Generate a dedicated SSH key for the tunnel:
  ```bash
  ssh-keygen -t ed25519 -f ~/.ssh/<remote-name>_ieh_key -N ""
  ```
  Replace `<remote-name>` with a short identifier (e.g., `tajhm`, `kghm`).

**Step 3: Install public key on remote server**

- [ ] Copy the public key to the remote server:
  ```bash
  ssh-copy-id -i ~/.ssh/<remote-name>_ieh_key.pub -p <ssh-port> <user>@<remote-ip>
  ```
  If `ssh-copy-id` is not available, use the manual approach:
  ```bash
  cat ~/.ssh/<remote-name>_ieh_key.pub | ssh -p <ssh-port> <user>@<remote-ip> \
    "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
  ```
  > **Note:** This step requires either the current password for the remote user, or coordination with the remote IT team to add the key manually.

**Step 4: Add remote server to known_hosts**

- [ ] Add the remote server's host key:
  ```bash
  ssh-keyscan -p <ssh-port> <remote-ip> >> ~/.ssh/known_hosts
  ```

**Step 5: Test the connection**

- [ ] Verify key-based SSH login works:
  ```bash
  ssh -i ~/.ssh/<remote-name>_ieh_key -p <ssh-port> <user>@<remote-ip>
  ```

- [ ] Test port forwarding manually:
  ```bash
  ssh -i ~/.ssh/<remote-name>_ieh_key -p <ssh-port> -L <local-port>:localhost:<remote-port> -N <user>@<remote-ip>
  ```
  In a second terminal, verify the local port is listening:
  ```bash
  ss -tlnp | grep <local-port>
  curl -s http://localhost:<local-port>/api/v1/ | head
  ```
  Press `Ctrl+C` to stop the manual tunnel.

**Step 6: Create systemd service**

- [ ] Create the service file `/etc/systemd/system/<remote-name>-ieh-hf-ssh-tunnel.service`:
  ```bash
  sudo tee /etc/systemd/system/<remote-name>-ieh-hf-ssh-tunnel.service << 'EOF'
  [Unit]
  Description=AutoSSH tunnel to <remote-name> iEH HF
  After=network-online.target
  Wants=network-online.target

  [Service]
  User=<local-user>
  Environment="AUTOSSH_GATETIME=0"
  ExecStart=/usr/bin/autossh -M 0 \
    -o "ServerAliveInterval 30" \
    -o "ServerAliveCountMax 3" \
    -o "ExitOnForwardFailure=yes" \
    -o "StrictHostKeyChecking=no" \
    -N \
    -p <ssh-port> \
    -L <local-port>:localhost:<remote-port> \
    -i /home/<local-user>/.ssh/<remote-name>_ieh_key \
    <user>@<remote-ip>
  Restart=always
  RestartSec=10

  [Install]
  WantedBy=multi-user.target
  EOF
  ```

  Replace all `<placeholders>` with your actual values. Common configurations:

  | Placeholder | Example (same network, non-standard port) | Example (different network) |
  |---|---|---|
  | `<remote-name>` | `kghm` | `tajhm` |
  | `<remote-ip>` | `192.168.1.50` | `195.7.15.46` |
  | `<ssh-port>` | `22` | `56222` |
  | `<user>` | `imomo` | `ieasyhydro` |
  | `<local-port>` | `5555` | `5554` |
  | `<remote-port>` | `5555` | `5555` |
  | `<local-user>` | `ubuntu` | `ubuntu` |

**Step 7: Enable and start the service**

- [ ] Enable and start:
  ```bash
  sudo systemctl daemon-reload
  sudo systemctl enable <remote-name>-ieh-hf-ssh-tunnel
  sudo systemctl start <remote-name>-ieh-hf-ssh-tunnel
  ```

- [ ] Configure `.env` to use the local tunnel port:
  ```
  IEASYHYDROHF_HOST=http://localhost:<local-port>
  ```

**Step 8: Verify**

- [ ] Check service status:
  ```bash
  sudo systemctl status <remote-name>-ieh-hf-ssh-tunnel
  ```
- [ ] Verify tunnel is listening:
  ```bash
  ss -tlnp | grep <local-port>
  ```
- [ ] Test API access:
  ```bash
  curl -s http://localhost:<local-port>/api/v1/ | head
  ```
- [ ] View logs if needed:
  ```bash
  sudo journalctl -u <remote-name>-ieh-hf-ssh-tunnel -f
  ```

> **Troubleshooting:** If the tunnel fails, check: (1) port reachability with `nc -zv <remote-ip> <ssh-port>`, (2) key is in remote `authorized_keys`, (3) no port conflicts with `ss -tlnp | grep <local-port>`, (4) logs with `journalctl -u <remote-name>-ieh-hf-ssh-tunnel --since "1 hour ago"`.

**Dashboard tunnel reachability (Docker bridge networking)**

The SSH tunnel binds to host loopback (`127.0.0.1:<local-port>`) for security. Dashboard containers run on a Docker bridge network and have their own loopback — they cannot reach the host loopback. Symptom: dashboard logs `iEHHF not available, falling back to file. Error: ... Connection refused` on the `/api/v1/auth/token-obtain` path. (The misleading hardcoded warning text may mention `hf.ieasyhydro.org` — do not chase that hostname; the real failure is on `localhost:<local-port>` inside the dashboard container.)

Fix: add `network_mode: host` to the dashboard service in `sapphire/docker-compose.yml` (and `bin/docker-compose-dashboards.yml` if its dashboards also call iEH HF). This makes the container share the host network namespace and reach the host loopback tunnel.

**Important:** This fix is **not currently committed** in the repository. Production servers running iEH HF behind a tunnel may have applied it as a server-side override. Coordinate with the team before relying on it; future code may rewrite `IEASYHYDROHF_HOST` for Docker symmetrically with `IEASYHYDRO_HOST` and obviate the workaround.

Verification step (run after the stack is up in section 3 below):
```bash
docker exec sapphire-dashboard curl -sf http://localhost:<local-port>/api/v1/
```
If this returns 200 from inside the dashboard container, the bridge-network workaround is in place. If it fails with `Connection refused` even though the host-side `curl http://localhost:<local-port>/api/v1/` succeeds, the dashboard service still needs `network_mode: host`.

**Option D: iEasyHydro HF Cloud Version**

If using the iEasyHydro HF cloud version:

- [ ] Configure cloud API endpoint in `.env`:
  ```
  IEASYHYDRO_HOST=<cloud-api-endpoint>
  ```
- [ ] Ensure API credentials are set in `.env`
- [ ] Verify firewall allows outbound HTTPS connections

---

## 3. Environment variable review

Before bringing up the stack, review `${ENV_FILE_PATH}` and confirm all required variables are set. For the authoritative list cross-reference `doc/plans/working/taj_deploy_gap_analysis.md` §5a and `sapphire/.env.example`; the critical first-deploy variables are enumerated inline below.

**Microservices / API integration (required):**

- `SAPPHIRE_API_ENABLED=true` — gates API write path vs. CSV (legacy CSV path is being removed).
- `SAPPHIRE_API_URL=http://localhost:8000` — base URL of the api-gateway.
- `API_GATEWAY_URL=http://localhost:8000` — read by the dashboard and pipeline containers.

**Snow stat dashboard gate (set after backfill):**

- `SAPPHIRE_SNOW_STATS_AVAILABLE=false` until the snow-stat backfill in section 8 has been run successfully; then flip to `true`.
- `SAPPHIRE_SEASON_START_MONTH` / `SAPPHIRE_SEASON_END_MONTH` — integer month numbers defining the hydrological-year window for the snow plot (e.g., `10` / `9`).

**PostgreSQL credentials (required for the four SAPPHIRE databases):**

- `POSTGRES_USER` and `POSTGRES_PASSWORD` — shared credentials for the four DB containers.
- `PREPROCESSING_DB`, `POSTPROCESSING_DB`, `USER_DB`, `AUTH_DB` — database names.

**Organization and image tags (required):**

- `ieasyhydroforecast_organization` — one of `kghm`, `tjhm`, `uzhm`, `demo`. Drives URL routing in `bin/utils/common_functions.sh` and the timezone lookup in `setup_library.py`.
- `ieasyhydroforecast_backend_docker_image_tag` — image tag for `mabesa/sapphire-*` pipeline images (e.g., `local`, `v1.0.0`).
- `ieasyhydroforecast_frontend_docker_image_tag` — image tag for `mabesa/sapphire-dashboard`.

**iEasyHydro HF connectivity (required only if section 2 applies):**

- `IEASYHYDROHF_HOST=http://localhost:<local-port>` — tunnel host:port from section 2 (or direct IP for Option A; cloud endpoint for Option D).
- `IEASYHYDRO_HOST` — cloud endpoint when using the cloud version.
- `ieasyhydroforecast_connect_to_iEH=true` and `ieasyhydroforecast_ssh_to_iEH=true` when a tunnel is in use.

**Auth / JWT (required for the auth-api):**

- `JWT_SECRET_KEY`, `JWT_ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`, `REFRESH_TOKEN_EXPIRE_DAYS`.

**API gateway tuning (required):**

- `REQUEST_TIMEOUT`, `HEALTH_CHECK_TIMEOUT`, `API_KEY_ENABLED`, `API_KEY`, `RATE_LIMIT_ENABLED`.

**Optional integrations:**

- `GOOGLE_SHEETS_ENABLED=false` unless this deployment uses Google Sheets as a discharge source. If `true`, also set `GOOGLE_SHEETS_DISCHARGE_ID`, `GOOGLE_SHEETS_CREDENTIALS_PATH`, `GOOGLE_SHEETS_SITE_CODES`.
- `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS`, `SAPPHIRE_PIPELINE_SMTP_*` — pipeline failure notifications (added 2026-04).

**Sanity check:**

- [ ] Confirm the file exists and is readable only by the deploy user:
  ```bash
  ls -l "${ENV_FILE_PATH}"
  chmod 600 "${ENV_FILE_PATH}"
  ```
- [ ] Confirm no obsolete variables remain. Specifically remove `POSTPROCESSING_GAPFILL_WINDOW_DAYS` if present (removed from code).

---

## 4. Pull (or build) Docker images

The first deploy needs the full image inventory. Image categories:

**Pipeline images** (built/pushed to DockerHub as `mabesa/sapphire-*`; pulled at deploy time):

```bash
TAG="${ieasyhydroforecast_backend_docker_image_tag:-latest}"
FRONTEND_TAG="${ieasyhydroforecast_frontend_docker_image_tag:-latest}"

docker pull mabesa/sapphire-pythonbaseimage:${TAG}
docker pull mabesa/sapphire-pipeline:${TAG}
docker pull mabesa/sapphire-preprunoff:${TAG}
docker pull mabesa/sapphire-prepgateway:${TAG}
docker pull mabesa/sapphire-linreg:${TAG}
docker pull mabesa/sapphire-ml:${TAG}
docker pull mabesa/sapphire-postprocessing:${TAG}
docker pull mabesa/sapphire-lt-forecasting:${TAG}
docker pull mabesa/sapphire-preprocessing-station-forcing:${TAG}
docker pull mabesa/sapphire-dashboard:${FRONTEND_TAG}
```

Conditional / optional:
- `mabesa/sapphire-conceptmod:${TAG}` — only if `ieasyhydroforecast_run_CM_models=true`.

**Microservices images** (built locally from `sapphire/docker-compose.yml`):

The five FastAPI services (`api-gateway`, `preprocessing`, `postprocessing`, `user`, `auth`) build from `sapphire/services/*/Dockerfile`. On first deploy, run the build before the bring-up:

```bash
cd /data/SAPPHIRE_Forecast_Tools/sapphire
docker compose --env-file "${ENV_FILE_PATH}" -f docker-compose.yml build
```

> Naming note: there is an unfortunate name collision between the app image `mabesa/sapphire-postprocessing` (operational postprocessing wrappers built from `apps/postprocessing_forecasts/Dockerfile`) and the service image built from `sapphire/services/postprocessing/Dockerfile` (FastAPI). They are different images for different layers — do not confuse them.

**Infrastructure images:**

- `sapphire/luigi-daemon:${TAG}` — built locally via `bin/luigi-daemon.Dockerfile`. First build:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  docker build -f bin/luigi-daemon.Dockerfile -t sapphire/luigi-daemon:${TAG} .
  ```
- `postgres:15` — pulled from DockerHub on first start of the DB containers; no manual pull required.

---

## 5. First bring-up of the SAPPHIRE microservices stack

The `sapphire/docker-compose.yml` stack is the first thing to start on a fresh deployment. It defines the four PostgreSQL databases, the five FastAPI services, and the pentad dashboard.

**Expected containers (10 total):**

| Container | Service | Host port |
|---|---|---|
| `sapphire-preprocessing-db` | PostgreSQL 15 — preprocessing | 5433 |
| `sapphire-postprocessing-db` | PostgreSQL 15 — postprocessing | 5434 |
| `sapphire-user-db` | PostgreSQL 15 — user | 5435 |
| `sapphire-auth-db` | PostgreSQL 15 — auth | 5436 |
| `sapphire-api-gateway` | FastAPI api-gateway | 8000 |
| `sapphire-preprocessing-api` | FastAPI preprocessing service | 8002 |
| `sapphire-postprocessing-api` | FastAPI postprocessing service | 8003 |
| `sapphire-user-api` | FastAPI user service | 8004 |
| `sapphire-auth-api` | FastAPI auth service | 8005 |
| `sapphire-dashboard` | Pentad dashboard (panel) | 5006 |

**Bring up the stack:**

```bash
cd /data/SAPPHIRE_Forecast_Tools
docker compose --env-file "${ENV_FILE_PATH}" -f sapphire/docker-compose.yml up -d
```

Wait ~30 s for all containers to become healthy, then verify:

```bash
docker ps --filter name=sapphire-
curl -sf http://localhost:8000/health && echo " — api-gateway alive"
curl -sf http://localhost:8000/health/ready && echo " — api-gateway ready (backends OK)"
```

If `/health/ready` fails, one or more backend services did not come up. Check `docker logs sapphire-<service>-api`.

---

## 6. Apply initial database schema (alembic)

The preprocessing and postprocessing services use **Alembic** for schema management. On a fresh DB, run alembic upgrade once each.

**Preprocessing:**

```bash
docker exec sapphire-preprocessing-api alembic current
docker exec sapphire-preprocessing-api alembic upgrade head
```

Baseline revision is `a6210b339f17` (creates `runoffs`, `hydrographs`, `meteo`, `snow` tables). Current head is `9f1e72108f01`, which adds 12 snow-stat columns (`count`, `mean`, `std`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`, `current`) on the `snow` table.

**Postprocessing:**

```bash
docker exec sapphire-postprocessing-api alembic current
docker exec sapphire-postprocessing-api alembic upgrade head
```

Baseline revision is `34b227f37299`. No non-baseline migrations exist yet.

**Auth and user:** both services auto-apply their alembic baselines on first boot; no operator action required.

> If a preprocessing or postprocessing DB pre-existed from a pre-Alembic deployment (created with `create_all`), stamp it at the baseline first to avoid "table already exists" errors:
> ```bash
> docker exec sapphire-preprocessing-api alembic stamp a6210b339f17
> docker exec sapphire-postprocessing-api alembic stamp 34b227f37299
> ```
> Then run `alembic upgrade head` as above.

---

## 7. Start the Luigi daemon

The Luigi daemon runs the pipeline scheduler in a persistent container. **`COMPOSE_PROJECT_NAME=sapphire` must be set** so long-term-forecast tasks can join the network named `sapphire_sapphire-network`.

```bash
cd /data/SAPPHIRE_Forecast_Tools
export COMPOSE_PROJECT_NAME=sapphire
docker compose -f bin/docker-compose-luigi.yml -p sapphire up -d luigi-daemon
```

Wait a few seconds, then verify the Luigi UI is reachable:

```bash
curl -sf http://localhost:8082/ && echo " — Luigi UI alive"
```

---

## 8. Run RunInitializeWorkflow (first-deploy only)

`RunInitializeWorkflow` is the Luigi bootstrap workflow that populates the preprocessing DB with historical site data and seeds the linear-regression model state and skill metrics. **This step is first-deploy only.** The workflow is idempotent (Luigi marker files short-circuit subsequent runs), so it is unnecessary — and skipped automatically — on routine updates documented in `update_deployment_checklist.md`.

**Prerequisites (all must be satisfied first):**

- Sections 1–7 of this checklist completed.
- iEH HF tunnel up and reachable from the host (section 2).
- All four databases healthy and schema upgraded (sections 5–6).
- Luigi daemon running on port 8082 (section 7).

**Run the workflow:**

```bash
cd /data/SAPPHIRE_Forecast_Tools
docker compose -f bin/docker-compose-luigi.yml -p sapphire run --rm \
  --entrypoint "" \
  preprocessing-runoff \
  uv run luigi \
    --scheduler-host luigi-daemon \
    --scheduler-port 8082 \
    --module apps.pipeline.pipeline_docker \
    RunInitializeWorkflow
```

The workflow chain (per `apps/pipeline/pipeline_docker.py:2604`) is:

```
PrepRunoffMaintenance → InitialApiSync → LinRegInitial(PENTAD/DECAD)
                                       → SkillMetricsInitial(PENTAD/DECAD)
                                       → RunInitializeWorkflow
```

On success, a marker file `initial_workflow_complete` is written to the Luigi marker directory inside the data volume. Any later run of `RunInitializeWorkflow` short-circuits on this marker. **Do not delete this marker file** unless you genuinely intend to re-initialise from scratch.

Confirm in the Luigi UI:
```bash
curl -s "http://localhost:8082/api/task_list?upstream_status=&search=RunInitializeWorkflow" | python3 -m json.tool | head -40
```

---

## 9. Historical data backfill (optional)

The following tracked recovery / backfill scripts are useful on first deploy when the operator wants historical years populated beyond what `RunInitializeWorkflow` produces. Run them after section 8 succeeds.

**Snow stat historical backfill** (`bin/backfill_snow_stats_history.sh`):

Populates the 12 snow-stat columns introduced by Alembic revision `9f1e72108f01` for historical years. Snow stats are read by the dashboard snow tab and gated by `SAPPHIRE_SNOW_STATS_AVAILABLE`.

```bash
ieasyhydroforecast_env_file_path="${ENV_FILE_PATH}" \
  bash bin/backfill_snow_stats_history.sh --start-year 2010
```

The script loops years sequentially, writing a progress file so re-runs resume from the last completed year.

> **Known quirk:** a known shell-script issue causes the script to exit with code 1 even on full success (an unbound `$ieasyhydroforecast_ssh_tunnel_pid` in the shared `bin/utils/common_functions.sh` cleanup trap fires on shell exit). Do **not** interpret exit code 1 alone as failure. Inspect the log tail:
> ```bash
> tail -50 "${LOG_DIR}/sapphire_backfill_snow_stats_*.log"
> ```
> A successful run ends with `all years processed`. If you see that string, the backfill succeeded and you can flip `SAPPHIRE_SNOW_STATS_AVAILABLE=true` in `${ENV_FILE_PATH}`. If you do not, treat as a genuine failure and investigate.

**Other tracked periodic wrappers** that may make sense on first deploy:

- `bin/yearly_snow_norm_recalculation.sh "${ENV_FILE_PATH}"` — annual snow-norm recalculation. Run once to seed the current year's norms.
- `bin/yearly_runoff_hydrograph_aggregation.sh "${ENV_FILE_PATH}"` — long-horizon monthly + April–September seasonal hydrograph. Replaces the retired `YearlyMonthlyNormsRecalculation` Luigi task. Run once on first deploy to seed the long-horizon hydrograph view.
- `bin/bimonthly_long_term_postprocessing.sh "${ENV_FILE_PATH}"` — long-term forecast postprocessing.

Recurring schedules for these wrappers belong in the cron installation step, not here.

---

## 10. Start the dashboards

The pentad dashboard is started as part of the SAPPHIRE stack (section 5). The decad dashboard is currently served from the legacy `bin/docker-compose-dashboards.yml`.

> **Known issue — port 5006 dual-binding:** Both `sapphire/docker-compose.yml` (`dashboard` service, port 5006) and `bin/docker-compose-dashboards.yml` (`pentaddashboard` service, port 5006) can bind the same port. The live convention is: pentad dashboard moved to `sapphire/docker-compose.yml`, decad dashboard still served from the legacy compose file. Before starting the legacy compose, comment out or delete the `pentaddashboard` service in `bin/docker-compose-dashboards.yml` to avoid a port conflict. Verify which compose actually exposes 5006 on your deployment.

**Start the decad dashboard from the legacy compose:**

```bash
cd /data/SAPPHIRE_Forecast_Tools
docker compose -f bin/docker-compose-dashboards.yml --env-file "${ENV_FILE_PATH}" up -d decaddashboard
```

If your deployment still uses the legacy compose for the pentad dashboard, add `pentaddashboard` to the `up -d` command — but only one source of truth should bind port 5006.

Verify the dashboards are up:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/forecast_dashboard
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5007/forecast_dashboard
```

Both should return `200`.

- [ ] Verify the header **Help** link resolves (the `help/` static mount is present and populated):
  ```bash
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/help/forecast_dashboard_user_guide_ru.html
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/help/forecast_dashboard_user_guide_en.html
  ```
  Both should return `200`. A `404` means either the guide files are missing from `${DATA_DIR}/help/` (section 1.5) or the running container was created without the `--static-dirs help=…` flag / `help/` volume mount — confirm which compose created it with
  `docker inspect sapphire-dashboard --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}'`
  and recreate it (`docker compose -f <that file> --env-file "${ENV_FILE_PATH}" up -d dashboard`) after the flag and mount are in place.

---

## 11. First smoke test (post-deploy probe suite)

Run the full post-deploy probe suite from `doc/plans/working/taj_deploy_operational_readiness.md` §4. All probes should return HTTP 200.

**Liveness / readiness:**

```bash
curl -sf http://localhost:8000/health        && echo " — api-gateway liveness"
curl -sf http://localhost:8000/health/ready  && echo " — api-gateway readiness (backends ok)"
```

**Backend service health (probes each service directly, bypassing the gateway):**

```bash
curl -sf http://localhost:8002/health && echo " — preprocessing-api"
curl -sf http://localhost:8003/health && echo " — postprocessing-api"
curl -sf http://localhost:8004/health && echo " — user-api"
curl -sf http://localhost:8005/health && echo " — auth-api"
```

**Luigi scheduler UI:**

```bash
curl -sf http://localhost:8082/ && echo " — Luigi scheduler UI"
```

**Dashboards:**

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5006/forecast_dashboard   # pentad
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5007/forecast_dashboard   # decad
```

Both `5006` and `5007` should return `200`.

**Preprocessing read smoke (via api-gateway):**

```bash
curl -s "http://localhost:8000/api/preprocessing/runoff/?limit=1" | python3 -m json.tool | head -20
curl -s "http://localhost:8000/api/preprocessing/snow/?limit=1"   | python3 -m json.tool | head -20
```

Both should return a non-empty JSON array. If the snow endpoint returns rows but the stat columns are all null, the snow-stat backfill (section 9) has not yet been run — leave `SAPPHIRE_SNOW_STATS_AVAILABLE=false` until it has.

**Postprocessing read smoke:**

```bash
curl -s "http://localhost:8000/api/postprocessing/forecasts/?limit=1"     | python3 -m json.tool | head -20
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?limit=1"  | python3 -m json.tool | head -20
```

Empty results here are expected if no forecast has been run yet; they will populate after the first scheduled (or manually triggered) forecast cycle.

**Visual smoke (requires browser or screenshot):**

- Open `http://<server-ip>:5006/forecast_dashboard` — pentad dashboard renders with data.
- Open `http://<server-ip>:5007/forecast_dashboard` — decad dashboard renders.
- Navigate to the **Snow** tab — chart renders. If snow-stat backfill has run and `SAPPHIRE_SNOW_STATS_AVAILABLE=true`, statistical bands (q05–q95, min–max) are visible.

---

## 12. Install operational cron schedule

The microservices stack, Luigi daemon, and dashboards are now running. The final first-deploy step is to put the forecast pipeline on a production cadence by installing the SAPPHIRE crontab. After this section completes, the operational forecasts and the daily/periodic maintenance jobs will run on their own.

This section is the authoritative install procedure. The same canonical cron block is reproduced in `doc/prod/update_deployment_checklist.md` §2.5 so routine-update operators can diff their running crontab against it on later updates without re-reading the first-deploy doc.

> **Prerequisites:**
> - The SAPPHIRE microservices stack is up (section 5). Every cron command reads/writes through the api-gateway at `http://localhost:8000`; without the stack up, each command fails immediately with `Connection refused`. Confirm with `curl -sf http://localhost:8000/health/ready && echo READY`.
> - The first smoke test in section 11 passed.
> - The Luigi daemon is running (section 7). Cron-driven scripts start it automatically if it is not, but verifying once here avoids surprises.

### 12.1 Back up any existing crontab

On a fresh server there is usually no crontab to back up, but on a recycled host there may be. Run this in either case — `crontab -l` returns non-zero if nothing is installed, which is fine:

```bash
crontab -l > ~/crontab_backup_$(date +%Y%m%d).txt 2>/dev/null || true
ls -la ~/crontab_backup_*.txt
```

### 12.2 Install the canonical cron block

The block below is the post-S1-2026 consolidated Luigi-wrapper pattern. The authoritative source is [`doc/deployment.md` §"Set up cron job"](../deployment.md#set-up-cron-job); this section keeps the same block inline for operator convenience.

> **Cron does not expand shell variables.** The `${...}` placeholders below are for readability — when you paste these into `crontab -e`, substitute the literal values of `${ENV_FILE_PATH}` and `${LOG_DIR}` (as set in the operator placeholder block at the top of this doc) into each line.

- [ ] **Edit crontab**
  ```bash
  crontab -e
  ```

- [ ] **Paste the canonical block** (substituting literal values for `${ENV_FILE_PATH}` and `${LOG_DIR}`):

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
  0 1 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/backup_sapphire_db.sh -d /var/backups/sapphire -r 30 >> ${LOG_DIR}/sapphire_db_backup_$(date +\%Y\%m\%d).log 2>&1

  # (1) Gateway Preprocessing at 03:00 UTC. Independent of daily data.
  0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_gateway_preprocessing_$(date +\%Y\%m\%d).log 2>&1

  # (2) Pentadal Forecast at 04:00 UTC. Luigi triggers runoff preprocessing.
  0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_pentadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (3) Decadal Forecast at 05:00 UTC.
  0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_decadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (4) Long-Term Forecast at 06:00 UTC on THIS deployment's issue day(s).
  # ${LT_ISSUE_DAY} MUST equal the "operational_issue_day" values in
  # ${DATA_DIR}/config/long_term_configs/*.json — tjhm = 1, kghm = 10,25.
  # A cron day 6-10 days from the issue day is admitted by
  # lt_schedule_query.py (ISSUE_DAY_TOLERANCE = 10, :52) and then refused by
  # lt_utils.py:202 (>5 days): the run writes nothing and exits 0 (LTF-007).
  0 6 ${LT_ISSUE_DAY} * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_long_term_$(date +\%Y\%m\%d).log 2>&1

  # (4b) Long-term skill metrics recalculation at 10:00 UTC, four hours after
  # each long-term forecast, so its day field tracks ${LT_ISSUE_DAY} too. This
  # job does not gate on the issue day; aligning the days only preserves the
  # forecast-then-score ordering. Keeps the dashboard long-term skill tiles
  # fresh between yearly full recalcs.
  0 10 ${LT_ISSUE_DAY} * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/bimonthly_long_term_skill_metrics_recalculation.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_bimonthly_lt_skill_recalc_$(date +\%Y\%m\%d).log 2>&1

  # (5) Daily Maintenance at 19:00 UTC (consolidated Luigi wrapper).
  # Replaces the legacy individual daily_*_maintenance.sh scripts. Luigi
  # enforces dependency order: PrepRunoff + Gateway → LinReg → ML →
  # PostProcessing → Frontend. ML concurrency limited to 3 via Luigi resources.
  0 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_daily_maintenance_$(date +\%Y\%m\%d).log 2>&1

  # (6) Bimonthly long-term postprocessing at 22:00 UTC on the 1st of odd
  # months (consolidated Luigi wrapper). Supersedes legacy
  # bin/bimonthly_long_term_postprocessing.sh (kept for manual / debugging
  # use only).
  0 22 1 1,3,5,7,9,11 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh long_term ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_lt_postproc_$(date +\%Y\%m\%d).log 2>&1

  # (7) Yearly skill metrics recalculation at 01:00 UTC on December 31
  # (consolidated Luigi wrapper). Full-history safety net.
  0 1 31 12 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh skill_recalc ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_skill_recalc_$(date +\%Y\%m\%d).log 2>&1

  # (8) Yearly snow norm/stat recalculation at 02:00 UTC on January 1
  # (consolidated Luigi wrapper). Supersedes legacy
  # bin/yearly_snow_norm_recalculation.sh (kept for manual / debugging
  # use only).
  # Owner decision 2026-08-19: run at the END of the snow year (31 August),
  # before the new accumulation season, rather than 1 January which would move
  # the norms mid-season.
  0 2 31 8 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_snow_norm_$(date +\%Y\%m\%d).log 2>&1

  # (9) Yearly runoff hydrograph aggregation at 03:00 UTC on January 1.
  # Replaces the retired YearlyMonthlyNormsRecalculation Luigi task. Builds
  # the long-horizon monthly + April–September seasonal hydrograph view
  # used by the dashboard.
  0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH} >> ${LOG_DIR}/sapphire_yearly_runoff_hydrograph_$(date +\%Y\%m\%d).log 2>&1
  ```

  > **Legacy scripts to avoid:** `bin/daily_preprunoff_maintenance.sh`, `bin/daily_ml_maintenance.sh`, `bin/daily_linreg_maintenance.sh`, `bin/daily_postprc_maintenance.sh`, `bin/daily_gateway_maintenance.sh`, and `bin/daily_update_sapphire_frontend.sh` are marked `[Legacy]` in `bin/README.md`. They remain on origin for manual debugging only — do not schedule them via cron. The single `run_daily_maintenance.sh` entry above replaces all of them and enforces dependency order via Luigi.

### 12.3 Verify the crontab installed correctly

- [ ] **Show the installed crontab**
  ```bash
  crontab -l
  ```
  Compare against the block in §12.2.

- [ ] **Ensure the log directory exists**
  ```bash
  mkdir -p ${LOG_DIR}
  ls -ld ${LOG_DIR}
  ```

- [ ] **Verify the cron service is running**
  ```bash
  sudo systemctl status cron
  ```

- [ ] **Verify the database backup target directory exists and is writable**
  ```bash
  sudo mkdir -p /var/backups/sapphire
  sudo chown $(whoami): /var/backups/sapphire
  ls -ld /var/backups/sapphire
  ```

### 12.4 Manual cron tests

> **Prerequisite — the SAPPHIRE microservices stack must be up before any
> cron command below will succeed.** Every cron command reads/writes through
> the api-gateway at `http://localhost:8000`; without the stack up, each
> command fails immediately with `Connection refused`. Verify before
> proceeding:
> ```bash
> curl -sf http://localhost:8000/health/ready && echo "READY"
> ```
> If the stack is not up, start it from section 5 (`docker compose --env-file ${ENV_FILE_PATH} -f sapphire/docker-compose.yml up -d`, or equivalently `bash bin/restart_sapphire_stack.sh ${ENV_FILE_PATH}`).

Run each cron command once to confirm the wrapper script, env file, and microservices stack agree. The Luigi daemon starts automatically when needed. Monitor progress in the Luigi web UI at http://localhost:8082.

- [ ] **Run database backup**
  ```bash
  bash /data/SAPPHIRE_Forecast_Tools/bin/backup_sapphire_db.sh -d /var/backups/sapphire -r 30
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

- [ ] **Run long-term forecast** (dry-run is safe on any date; full run only fires on operational forecast dates)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_long_term_forecasts.sh --dry-run ${ENV_FILE_PATH}
  ```

- [ ] **Run consolidated daily maintenance** (replaces the four legacy `daily_*_maintenance.sh` wrappers)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh ${ENV_FILE_PATH}
  ```

- [ ] **Run periodic maintenance tasks** (one per task type; pick those that match the current calendar)
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

- [ ] **Check logs after each command completes** under `${LOG_DIR}/sapphire_*.log`.

> For *subsequent* updates to the cron schedule (e.g., a new wrapper script added in a later release), follow `doc/prod/update_deployment_checklist.md` §2.5–§2.6 — that section walks through diffing the running crontab against the updated canonical block and re-running the affected manual cron tests.

---

## 13. Next steps

The microservices stack, Luigi daemon, dashboards, and cron schedule are now running. The deployment is on a production cadence.

1. **Routine updates** — for subsequent image-tag bumps, `.env` diffs, microservices restarts, and cron schedule changes, follow `doc/prod/update_deployment_checklist.md`. That checklist also covers stopping services, backups, alembic migrations, and rollback for an existing deployment.

---

*This is the first-deploy checklist. For routine updates see `doc/prod/update_deployment_checklist.md`.*
