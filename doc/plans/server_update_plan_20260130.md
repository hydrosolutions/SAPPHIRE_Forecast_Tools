# Server Update Plan - 2026-01-30

**Purpose**: Document the update of hydromet server deployments from main to local branch (Python 3.12 + uv migration).

**Date**: 2026-01-30
**Performed by**: Bea

---

## Server: sapphire

**Target Server Paths:**
- Project: `/data/SAPPHIRE_Forecast_Tools`
- Config: `/data/kyg_data_forecast_tools/config/.env_develop_kghm`
- Logs: `/home/sapphire/logs/`

---

## 1. PRE-UPDATE PREPARATION

### 1.1 SSH Access Verification

- [x] Verify you have SSH access to the server
- [x] Confirm you have sudo privileges
- [x] Verify you can access the project directory

**Status**: Done

### 1.2 Timing Considerations

- [x] Check the current time relative to scheduled cron jobs
- [x] **Avoid updating during:**
  - 03:00-06:00 UTC (forecast pipeline runs)
  - 19:00-21:00 UTC (maintenance jobs)
- [x] Consider notifying stakeholders if dashboards will be temporarily unavailable
- [x] Plan update during low-usage period

**Status**: Done - Stakeholders notified

### 1.3 Verify Current State

**Check running services:**

- [x] Verify Luigi daemon is running
- [x] Verify dashboard containers are running
- [x] Check dashboard health status
- [x] Verify dashboards are accessible

**Observations**:
- Dashboards were accessible but showed **white page** (rendering issue)
- Pipeline had run without apparent error

**Check recent pipeline activity:**

- [x] Review today's pipeline logs for any issues
- [x] Check Luigi task history for recent failures
- [x] Note the current git commit/branch

**Current state**: Server is on `local` branch

**Status**: Done

### 1.4 Backup Critical Files

**Create timestamped backup directory:**

- [x] Create backup directory

**Backup configuration files:**

- [x] Backup the .env file
- [x] Backup crontab
- [x] Backup Luigi daemon compose file (if customized) - N/A, using default
- [x] Backup dashboard compose file (if customized) - N/A, using default

**Backup application state:**

- [~] Backup last successful run timestamp - Skipped
- [~] Backup Luigi marker files (optional) - Skipped

**Record current Docker image versions:**

- [x] Document current image tags - Previous images were `:local` tag (already deleted)

**Verify backup completeness:**

- [x] List backup contents
- [x] Record backup location: `~/backups/sapphire_20260130`

### 1.5 Pre-Update Checklist Summary

- [x] SSH access verified
- [x] Timing is appropriate
- [x] All services verified running (dashboards show white page)
- [x] Recent logs checked for issues
- [x] .env file backed up
- [x] Crontab backed up
- [x] Current Docker images documented (were `:local`)
- [x] Backup directory location noted

**Status**: Pre-update preparation complete

---

## 2. CORE UPDATE STEPS

### 2.1 Stop Services

- [x] **Stop the Luigi daemon and pipeline services**
- [x] **Stop the forecast dashboards**
- [x] **Verify all SAPPHIRE containers are stopped**
- [x] **Remove old Docker images**

**Status**: All services stopped and images removed

### 2.2 Update Repository

- [x] **Navigate to project directory**
- [x] **Check current branch and status** - on `local` branch
- [x] **Fetch and merge main into local**
- [x] **Pull latest to server**
- [x] **Verify the update**

**Status**: Main merged into local, local pulled to server

### 2.3 Update .env File (BEFORE running containers)

> **IMPORTANT**: The .env file must be updated BEFORE pulling images or running any containers. The scripts read configuration from this file.

**Step 1: Download server .env to local machine**

- [x] **Copy server .env to local machine via scp**
  ```bash
  # From your LOCAL machine (not the server)
  # If you connect via a specific user, use user@<server>
  # If you connect via a specific port, add -P <port>
  scp sapphire:/data/kyg_data_forecast_tools/config/.env_develop_kghm \
      ~/Downloads/.env_develop_kghm_server
  ```

**Step 2: Compare with local repo .env**

- [x] **Compare the two files locally**
  ```bash
  # On your LOCAL machine
  diff ~/Downloads/.env_develop_kghm_server \
       /path/to/SAPPHIRE_forecast_tools/apps/config/.env_develop_kghm
  ```

  Or side-by-side:
  ```bash
  diff -y --suppress-common-lines \
       ~/Downloads/.env_develop_kghm_server \
       /path/to/SAPPHIRE_forecast_tools/apps/config/.env_develop_kghm
  ```

**Step 3: Identify and document changes**

- [x] **New variables to add** (in repo but not on server): None needed
- [x] **Variables to update** (different values): Verified up to date
- [x] **Variables to keep** (server-specific, don't overwrite):
  - Credentials (passwords, API keys)
  - Server-specific paths

**Step 4: Edit the server .env locally**

- [x] **Make a working copy**
- [x] **Edit the file locally**
- [x] **Add new variables** - None needed
- [x] **Update changed variables** - None needed
- [x] **Ensure Docker image tags are set to `local`**

**Key variables to verify:**

| Variable | Expected Value | Verified |
|----------|---------------|----------|
| `ieasyhydroforecast_backend_docker_image_tag` | `local` | [x] |
| `ieasyhydroforecast_frontend_docker_image_tag` | `local` | [x] |
| `ieasyhydroforecast_run_ML_models` | `true` or `false` | [x] |

**Step 5: Upload updated .env back to server**

- [x] **Copy updated .env to server via scp** - Not needed, .env already up to date
- [x] **Verify on server** - Verified

**Status**: .env file verified up to date

### 2.4 Pull New Docker Images

**Note**: The cron scripts (e.g., `run_pentadal_forecasts.sh`) will pull images automatically when run. You can either:
- **Option A**: Pull images manually now (see below)
- **Option B**: Skip manual pull and let the first cron job / manual script run pull the images

**Option A - Manual pull:**

```bash
export ieasyhydroforecast_backend_docker_image_tag=local
export ieasyhydroforecast_frontend_docker_image_tag=local

# Core images
docker pull mabesa/sapphire-pythonbaseimage:local
docker pull mabesa/sapphire-pipeline:local
docker pull mabesa/sapphire-preprunoff:local
docker pull mabesa/sapphire-linreg:local
docker pull mabesa/sapphire-postprocessing:local
docker pull mabesa/sapphire-dashboard:local

# ML images (if enabled)
docker pull mabesa/sapphire-prepgateway:local
docker pull mabesa/sapphire-ml:local
```

- [ ] Images pulled (manual or via script)

**Verify pulled images:**

- [ ] List all SAPPHIRE images with `local` tag
  ```bash
  docker images | grep sapphire | grep local
  ```

### 2.5 Update Crontabs

- [x] **Backup current crontab** (done in 1.4)

- [x] **Edit crontab**

**Crontab for sapphire server** (times in Bishkek local time = UTC+6):

```bash
# m h  dom mon dow   command
# ---------------------------------------------------------------------------
# SAPPHIRE Forecast Tools Schedule (Times in Bishkek = UTC+6)
# ---------------------------------------------------------------------------
# Log cleanup: delete logs older than 7 days (08:00 Bishkek = 02:00 UTC)
0 8 * * * find /home/sapphire/logs -name "sapphire_*.log" -mtime +7 -delete
#
# (1) Gateway Preprocessing (09:00 Bishkek = 03:00 UTC)
0 9 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_gateway_preprocessing_$(date +\%Y\%m\%d).log 2>&1
#
# (2) Pentadal Forecast (10:00 Bishkek = 04:00 UTC)
0 10 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_pentadal_forecast_$(date +\%Y\%m\%d).log 2>&1
#
# (3) Decadal Forecast (11:00 Bishkek = 05:00 UTC)
0 11 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_decadal_forecast_$(date +\%Y\%m\%d).log 2>&1
#
# (4) Maintenance jobs (01:02-02:34 Bishkek = 19:02-20:34 UTC)
2 1 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_update_sapphire_frontend.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_frontend_$(date +\%Y\%m\%d).log 2>&1
4 1 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_preprunoff_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_preprunoff_maintenance_$(date +\%Y\%m\%d).log 2>&1
0 2 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_ml_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_ml_maintenance_$(date +\%Y\%m\%d).log 2>&1
34 2 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_linreg_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/sapphire/logs/sapphire_linreg_maintenance_$(date +\%Y\%m\%d).log 2>&1
```

- [x] **Ensure log directory exists**
- [x] **Verify crontab was saved correctly**
- [x] **Verify cron service is running**
- [x] **Verify connectivity to iEasyHydro HF server**
  - Ping works (same network)
  - **SSH tunnel required** - iEasyHydro HF API only accessible via localhost on remote server

- [ ] **Set up permanent SSH tunnel (autossh + systemd)**

  1. Install autossh:
     ```bash
     sudo apt-get update && sudo apt-get install -y autossh
     ```

  2. Verify SSH key authentication works (no password prompt):
     ```bash
     ssh -o BatchMode=yes <user>@<ieasyhydro-hf-server> echo "OK"
     ```

  3. Create systemd service:
     ```bash
     sudo nano /etc/systemd/system/ieasyhydro-tunnel.service
     ```
     Content:
     ```ini
     [Unit]
     Description=SSH Tunnel to iEasyHydro HF Server
     After=network-online.target
     Wants=network-online.target

     [Service]
     Type=simple
     User=sapphire
     Environment="AUTOSSH_GATETIME=0"
     ExecStart=/usr/bin/autossh -M 0 -N -o "ServerAliveInterval=30" -o "ServerAliveCountMax=3" -o "ExitOnForwardFailure=yes" -L 5555:localhost:5555 <user>@<ieasyhydro-hf-server>
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

  5. Verify:
     ```bash
     sudo systemctl status ieasyhydro-tunnel.service
     curl -s http://localhost:5555/api/v1/ | head
     ```

**Status**: ✅ Complete - Crontabs updated with Bishkek time (UTC+6), cron service running. Permanent SSH tunnel configured via autossh + systemd (`ieasyhydrohf-tunnel.service`).

### 2.6 Test Cron Commands Manually

Run each cron command manually to verify they work correctly. Luigi daemon starts automatically.

**Prerequisites:**
- [x] SSH tunnel to iEasyHydro HF server must be running:
  ```bash
  ssh -f -N -L 5555:localhost:5555 <user>@<ieasyhydro-hf-server>
  ```
- [x] Verify tunnel: `curl -s http://localhost:5555/api/v1/ | head`

**Run commands:**

- [ ] **Run gateway preprocessing**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] **Run pentadal forecast**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] **Run decadal forecast**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] **Run maintenance jobs** (optional)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_update_sapphire_frontend.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_preprunoff_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_ml_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_linreg_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] **Monitor progress** in Luigi UI at http://localhost:8082
- [ ] **Check logs** for errors

**Status**: ✅ Cron commands tested manually. Verify scheduled runs on Monday 2026-02-02.

---

## 3. POST-UPDATE VERIFICATION

### 3.1 Start Services

- [ ] **Start Luigi daemon**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  export COMPOSE_PROJECT_NAME=sapphire
  docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
  ```

- [ ] **Wait for Luigi daemon to be ready**
  ```bash
  until curl -fsS http://localhost:8082/ >/dev/null; do echo "Waiting for Luigi..."; sleep 2; done
  ```

- [ ] **Start dashboards**
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml --env-file /data/kyg_data_forecast_tools/config/.env_develop_kghm up -d
  ```

- [ ] **Confirm all containers are running**
  ```bash
  docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  ```

### 3.2 Verify Services Running

- [ ] Luigi UI accessible at port 8082
- [ ] Pentad dashboard accessible at port 5006
- [ ] Decad dashboard accessible at port 5007
- [ ] Both dashboards load data correctly (no white page!)
- [ ] Container health status shows "healthy"
- [ ] No containers in unhealthy or restarting state

### 3.3 Test Forecast Run

- [ ] Run preprocessing gateway task (quick test)
- [ ] Monitor progress in Luigi UI
- [ ] Check logs for successful completion
- [ ] No ERROR or CRITICAL messages in logs

---

## 4. LOG CLEANUP (Optional)

- [ ] View current log files and sizes
- [ ] Delete logs older than 7 days
- [ ] Check intermediate_data/docker_logs
- [ ] Docker system prune

---

## 5. ISSUES ENCOUNTERED

| Issue | Description | Resolution |
|-------|-------------|------------|
| White page on dashboards | Dashboards accessible but show blank page | TBD |
| iEasyHydro HF connectivity | .env was configured for cloud API (stale data since Oct). Changed to local server but `localhost` doesn't work from Docker containers | Updated .env to use `IEASYHYDROHF_HOST=http://localhost:5555`. Established SSH tunnel: `ssh -f -N -L 5555:localhost:5555 <user>@<ieasyhydro-hf-server>` |
| No WDDA data warnings | Spot-check showed "No WDDA data from API in last 30 days" for stations 15189, 16059 | Caused by stale cloud API config. Resolved by switching to local iEasyHydro HF server via SSH tunnel |
| prepgateway image not pulled | `run_preprocessing_gateway.sh` didn't call `pull_docker_images` | Fixed in commit fe5282f - added missing `pull_docker_images` call |
| Container cleanup error | "No such container: prepgateway" during cleanup | Fixed in commit fe5282f - `stop_and_remove_container` now uses container IDs instead of name patterns |

---

## 6. FINAL CHECKLIST

### Services Running

- [x] Luigi daemon running at port 8082
- [x] Pentad dashboard running at port 5006
- [x] Decad dashboard running at port 5007
- [x] All containers healthy
- [x] Permanent SSH tunnel to iEasyHydro HF server (`ieasyhydrohf-tunnel.service`)

### Crontabs Configured

- [x] Crontab entries verified correct (Bishkek time UTC+6)
- [ ] Log cleanup job configured

### Documentation

- Backend image tag deployed: `local`
- Frontend image tag deployed: `local`
- Git commit on server: `local` branch (review_deployment merged)

### Next Scheduled Run

- Expected time: Gateway 09:00, Pentad 10:30, Decad 12:00 (Bishkek time, Mon-Fri)
- Plan to verify after next run: [x] Monday 2026-02-02

### Deployment Status: ✅ COMPLETE (pending Monday verification)

---

---

# Server: ubuntu (AWS production deployment)

**Target Server Paths:**
- Project: `/data/SAPPHIRE_Forecast_Tools`
- Config: `/data/taj_data_forecast_tools/config/.env_develop_tjhm`
- Logs: `/home/ubuntu/logs/`

---

## 1. PRE-UPDATE PREPARATION

### 1.1 SSH Access Verification

- [x] Verify you have SSH access to the server
- [x] Confirm you have sudo privileges
- [x] Verify you can access the project directory

**Status**: Done

### 1.2 Server OS Update

- [x] Check disk space: `df -h`
- [x] Update apt: `sudo apt update`
- [x] Upgrade server: `sudo apt upgrade`
- [x] Reboot if needed: `sudo reboot`
- [x] Remove stale packages: `sudo apt autoremove && sudo apt clean`

**Observations**: Disk almost full - need to clean old logs and Docker resources

**Status**: Done

### 1.3 Verify Current State

**Check running services:**

- [x] Verify Luigi daemon is running
- [x] Verify dashboard containers are running
- [x] Check dashboard health status
- [x] Verify dashboards are accessible

**Observations**:
- Disk space critical - cleanup required

**Check recent pipeline activity:**

- [x] Review today's pipeline logs for any issues
- [x] Check Luigi task history for recent failures
- [x] Note the current git commit/branch

**Current state**: Services running, disk almost full

**Status**: Done - proceed to cleanup and backup

### 1.4 Backup Critical Files

- [x] Create backup directory: `~/backups/tjhm_20260130`
- [x] Backup the .env file
- [x] Backup crontab: `crontab -l > ~/backups/tjhm_20260130/crontab_backup.txt`
- [~] Document current Docker images - Skipped (images cleaned before documenting)

**Backup location**: `~/backups/tjhm_20260130`

### 1.5 Pre-Update Checklist Summary

- [x] SSH access verified
- [x] Server OS updated
- [x] All services verified running
- [x] Recent logs checked for issues
- [x] .env file backed up
- [x] Crontab backed up
- [~] Current Docker images documented (cleaned before documenting)

**Status**: Pre-update preparation complete

---

## 2. CORE UPDATE STEPS

### 2.1 Stop Services

- [x] **Stop the Luigi daemon and pipeline services**
- [x] **Stop the forecast dashboards**
- [x] **Verify all SAPPHIRE containers are stopped**
- [x] **Remove old Docker images** (done earlier during cleanup)

**Status**: Done

### 2.2 Update Repository

- [x] **Navigate to project directory**
- [x] **Check current branch and status**
- [x] **Fetch and merge main into local** (or pull latest)
- [x] **Verify the update**

**Current branch**: `local` (pulled latest)

**Status**: Done

### 2.3 Update .env File (BEFORE running containers)

- [x] **Download server .env to local machine via scp**
- [x] **Compare with local repo .env**
- [x] **Identify changes needed**
- [x] **Update .env if needed**
- [x] **Upload updated .env back to server**

**Key variables to verify:**

| Variable | Expected Value | Verified |
|----------|---------------|----------|
| `ieasyhydroforecast_backend_docker_image_tag` | `local` | [x] |
| `ieasyhydroforecast_frontend_docker_image_tag` | `local` | [x] |
| `ieasyhydroforecast_run_ML_models` | `true` or `false` | [x] |
| `ieasyhydroforecast_organization` | `tjhm` | [x] |

**Status**: Done - .env verified

### 2.4 iEasyHydro HF Connectivity

- [x] **Check if SSH tunnel is required** - No, using cloud API

**iEasyHydro HF Server**: Cloud API
**Connection method**: [x] Direct API / [ ] SSH Tunnel
**Status**: Done - using cloud iEasyHydro HF server (no SSH tunnel needed)

### 2.5 Update Crontabs

- [x] **Backup current crontab** (done in 1.4)
- [x] **Edit crontab** with correct timezone adjustments
- [x] **Ensure log directory exists**: `mkdir -p /home/ubuntu/logs`
- [x] **Verify crontab was saved correctly**
- [x] **Verify cron service is running**

**Server timezone**: UTC (Tajikistan local time = UTC+5)

**Crontab for ubuntu server** (times in UTC, Tajikistan = UTC+5):

```bash
# m h  dom mon dow   command
# ---------------------------------------------------------------------------
# SAPPHIRE Forecast Tools Schedule (Server in UTC, Tajikistan = UTC+5)
# Forecasts ready by 11:00 AM Tajikistan time
# ---------------------------------------------------------------------------
# Log cleanup: delete logs older than 7 days (02:00 UTC = 07:00 Tajikistan)
0 2 * * * find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete
#
# (1) Gateway Preprocessing (03:00 UTC = 08:00 Tajikistan)
0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_gateway_preprocessing_$(date +\%Y\%m\%d).log 2>&1
#
# (2) Pentadal Forecast (04:00 UTC = 09:00 Tajikistan)
0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_pentadal_forecast_$(date +\%Y\%m\%d).log 2>&1
#
# (3) Decadal Forecast (05:00 UTC = 10:00 Tajikistan)
0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_decadal_forecast_$(date +\%Y\%m\%d).log 2>&1
#
# (4) Maintenance jobs (evening Tajikistan time)
# Frontend update (18:02 UTC = 23:02 Tajikistan)
2 18 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_update_sapphire_frontend.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_frontend_$(date +\%Y\%m\%d).log 2>&1
# Preprunoff maintenance (18:04 UTC = 23:04 Tajikistan)
4 18 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_preprunoff_maintenance.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_preprunoff_maintenance_$(date +\%Y\%m\%d).log 2>&1
# ML maintenance (19:00 UTC = 00:00 Tajikistan next day)
0 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_ml_maintenance.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_ml_maintenance_$(date +\%Y\%m\%d).log 2>&1
# Linreg maintenance (19:34 UTC = 00:34 Tajikistan next day)
34 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_linreg_maintenance.sh /data/taj_data_forecast_tools/config/.env_develop_tjhm >> /home/ubuntu/logs/sapphire_linreg_maintenance_$(date +\%Y\%m\%d).log 2>&1
```

**Status**: Done - crontabs configured

### 2.6 Test Cron Commands Manually

**Prerequisites:**
- [x] SSH tunnel to iEasyHydro HF server - Not required (using cloud API)

**Run commands:**

- [x] **Run gateway preprocessing**
- [x] **Run pentadal forecast**
- [x] **Run decadal forecast**
- [~] **Run maintenance jobs** (optional) - skipped
- [x] **Monitor progress** in Luigi UI
- [x] **Check logs** for errors

**Status**: Done - cron commands tested

---

## 3. POST-UPDATE VERIFICATION

### 3.1 Verify Services Running

- [x] Luigi UI accessible at port 8082
- [x] Pentad dashboard accessible at port 5006
- [x] Decad dashboard accessible at port 5007
- [x] Both dashboards load data correctly - **Issue with data display, see observations**
- [x] All containers healthy

### 3.2 Test Forecast Run

- [x] Run preprocessing gateway task (quick test)
- [x] Monitor progress in Luigi UI
- [x] Check logs for successful completion
- [x] No ERROR or CRITICAL messages in logs - Panel/HoloViews error observed

---

## 4. ISSUES ENCOUNTERED

| Issue | Description | Resolution |
|-------|-------------|------------|
| Panel/HoloViews Markdown error | Dashboard shows "'Markdown' object has no attribute 'opts'" error when no snow data available | See `doc/plans/observations.md` 2026-02-02. Needs code fix in `forecast_dashboard.py` |
| Snow data display | Snow .env variables missing | ✅ Fixed - configured snow paths in .env, dashboard now loads |
| Stale forecast data | Pentad dashboard shows 5th pentad of January, data gaps for 17082 and other sites | Investigating - contacted local contact to check which iEH HF version (cloud vs local) is operational |

---

## 5. FINAL CHECKLIST

### Services Running

- [x] Luigi daemon running at port 8082
- [x] Pentad dashboard running at port 5006
- [x] Decad dashboard running at port 5007
- [x] All containers healthy
- [x] SSH tunnel to iEasyHydro HF - Not required (using cloud API)

### Crontabs Configured

- [x] Crontab entries verified correct (UTC times for Tajikistan UTC+5)
- [x] Log cleanup job configured

### Documentation

- Backend image tag deployed: `local`
- Frontend image tag deployed: `local`
- Git commit on server: `local` branch

### Next Scheduled Run

- Expected time: Gateway 03:00 UTC, Pentad 04:00 UTC, Decad 05:00 UTC
- Plan to verify after next run: [x] Monday 2026-02-03

### Deployment Status: ⚠️ PARTIAL - Services running, dashboard data display issue pending (see observations)

---

*Plan created: 2026-01-30*
