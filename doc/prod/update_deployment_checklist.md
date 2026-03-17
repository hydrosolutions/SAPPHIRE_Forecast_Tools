# SAPPHIRE Forecast Tools - Update Deployment Checklist

This checklist guides you through updating an existing SAPPHIRE Forecast Tools deployment. It covers updating Docker images, configuration files, crontabs, and log cleanup.

**Target Server Paths:**
- Project: `/data/SAPPHIRE_Forecast_Tools`
- Config: `/data/kyg_data_forecast_tools/config/.env_develop_kghm`
- Logs: `/home/ubuntu/logs/`
- Luigi daemon port: 8082
- Dashboard ports: 5006 (pentad), 5007 (decad)

---

## 1. PRE-UPDATE PREPARATION

### 1.1 SSH Access Verification

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
  ssh -f -N -L 5555:localhost:5555 <user>@<ieasyhydro-server-ip>
  curl -s http://localhost:5555/api/v1/ | head
  ```
- [ ] Configure `.env` to use localhost:
  ```
  IEASYHYDROHF_HOST=http://localhost:5555
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
     ExecStart=/usr/bin/autossh -M 0 -N -o "ServerAliveInterval=30" -o "ServerAliveCountMax=3" -o "ExitOnForwardFailure=yes" -L 5555:localhost:5555 <user>@<ieasyhydro-server-ip>
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
  curl -s http://localhost:5555/api/v1/ | head
  ```

**Option C: Different Networks with Local iEasyHydro HF Installation**

If iEasyHydro HF is on a different network:

- [ ] Configure SSH tunnel to iEasyHydro HF server (with port forwarding if needed)
- [ ] Set up automatic SSH tunnel keepalive (e.g., via systemd service or autossh)
- [ ] Verify the tunnel is active and data can be retrieved

**Option C: iEasyHydro HF Cloud Version**

If using the iEasyHydro HF cloud version:

- [ ] Configure cloud API endpoint in `.env`:
  ```
  IEASYHYDRO_HOST=<cloud-api-endpoint>
  ```
- [ ] Ensure API credentials are set in `.env`
- [ ] Verify firewall allows outbound HTTPS connections

### 1.3 Timing Considerations

- [ ] Check the current time relative to scheduled cron jobs
  ```bash
  crontab -l | grep -v "^#"
  ```
- [ ] **Avoid updating during:**
  - 03:00-06:00 UTC (forecast pipeline runs)
  - 19:00-21:00 UTC (maintenance jobs)
- [ ] Consider notifying stakeholders if dashboards will be temporarily unavailable
- [ ] Plan update during low-usage period (e.g., weekends or early morning local time)

### 1.4 Verify Current State

**Check running services:**

- [ ] Verify Luigi daemon is running
  ```bash
  docker ps | grep luigi-daemon
  curl -s http://localhost:8082/ > /dev/null && echo "Luigi daemon OK" || echo "Luigi daemon NOT RUNNING"
  ```

- [ ] Verify dashboard containers are running
  ```bash
  docker ps | grep sapphire-frontend
  ```

- [ ] Check dashboard health status
  ```bash
  docker inspect --format "{{.State.Health.Status}}" sapphire-frontend-forecast-pentad
  docker inspect --format "{{.State.Health.Status}}" sapphire-frontend-forecast-decad
  ```

- [ ] Verify dashboards are accessible
  ```bash
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5007/forecast_dashboard
  ```

**Check recent pipeline activity:**

- [ ] Review today's pipeline logs for any issues
  ```bash
  ls -lt /home/ubuntu/logs/sapphire_*.log | head -5
  tail -50 /home/ubuntu/logs/sapphire_pentadal_$(date +%Y%m%d).log
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

### 1.5 Backup Critical Files

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
  cp /data/kyg_data_forecast_tools/config/.env_develop_kghm "$BACKUP_DIR/"
  ```

- [ ] Backup crontab
  ```bash
  crontab -l > "$BACKUP_DIR/crontab_backup.txt"
  ```

- [ ] Backup Luigi daemon compose file (if customized)
  ```bash
  cp /data/SAPPHIRE_Forecast_Tools/bin/docker-compose-luigi.yml "$BACKUP_DIR/" 2>/dev/null || echo "Using default compose file"
  ```

- [ ] Backup dashboard compose file (if customized)
  ```bash
  cp /data/SAPPHIRE_Forecast_Tools/bin/docker-compose-dashboards.yml "$BACKUP_DIR/" 2>/dev/null || echo "Using default compose file"
  ```

**Backup application state:**

- [ ] Backup last successful run timestamp
  ```bash
  cp /data/kyg_data_forecast_tools/intermediate_data/last_successful_run.txt "$BACKUP_DIR/" 2>/dev/null || echo "File not found"
  ```

- [ ] Backup Luigi marker files (optional, for debugging)
  ```bash
  mkdir -p "$BACKUP_DIR/luigi_markers"
  cp /data/kyg_data_forecast_tools/intermediate_data/luigi_markers/*.marker "$BACKUP_DIR/luigi_markers/" 2>/dev/null || echo "No marker files"
  ```

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

### 1.6 Pre-Update Checklist Summary

Before proceeding to the update steps, confirm:

- [ ] SSH access verified
- [ ] iEasyHydro HF connectivity verified (same network, SSH tunnel, or cloud config)
- [ ] Timing is appropriate (no cron jobs running soon)
- [ ] All services verified running
- [ ] Recent logs checked for issues
- [ ] .env file backed up
- [ ] Crontab backed up
- [ ] Current Docker images documented
- [ ] Backup directory location noted: `________________`

---

## 2. CORE UPDATE STEPS

### 2.1 Stop Services

Before updating, stop all running SAPPHIRE services to prevent conflicts during the update.

- [ ] **Stop the Luigi daemon and pipeline services**
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  docker compose -f bin/docker-compose-luigi.yml down
  ```

- [ ] **Stop the forecast dashboards**
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml down
  ```

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

### 2.2 Update Repository

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

### 2.4 Pull New Docker Images

> **Prerequisite**: Complete Section 2.3 (Update .env File) first!

Pull the updated Docker images with the `:local` tag.

> **Note**: The cron scripts (e.g., `run_pentadal_forecasts.sh`) will pull images automatically when run. You can either:
> - **Option A**: Pull images manually now (see below)
> - **Option B**: Skip manual pull and let the first cron job / manual script run pull the images

**Set environment variables:**

- [ ] **Export the image tag**
  ```bash
  export ieasyhydroforecast_backend_docker_image_tag=local
  export ieasyhydroforecast_frontend_docker_image_tag=local
  ```

**Pull core images (required for all deployments):**

- [ ] **Pull base image**
  ```bash
  docker pull mabesa/sapphire-pythonbaseimage:local
  ```

- [ ] **Pull pipeline orchestration image**
  ```bash
  docker pull mabesa/sapphire-pipeline:local
  ```

- [ ] **Pull preprocessing images**
  ```bash
  docker pull mabesa/sapphire-preprunoff:local
  ```

- [ ] **Pull linear regression forecasting image**
  ```bash
  docker pull mabesa/sapphire-linreg:local
  ```

- [ ] **Pull postprocessing image**
  ```bash
  docker pull mabesa/sapphire-postprocessing:local
  ```

- [ ] **Pull dashboard image**
  ```bash
  docker pull mabesa/sapphire-dashboard:local
  ```

**Pull optional images (based on deployment configuration):**

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

- [ ] **Pull conceptual model image** (if used)
  ```bash
  docker pull mabesa/sapphire-conceptmod:local
  ```

**Utility images:**

- [ ] **Pull rerun utility image** (for hindcasts/reruns from dashboard)
  ```bash
  docker pull mabesa/sapphire-rerun:local
  ```

**Verify pulled images:**

- [ ] **List all SAPPHIRE images with `local` tag**
  ```bash
  docker images | grep sapphire | grep local
  ```

**Alternative: Use the setup script**

The setup script automatically pulls images based on your .env configuration:

```bash
cd /data/SAPPHIRE_Forecast_Tools
bash bin/setup_docker.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
```

---

### 2.3 Update .env File (BEFORE running containers)

> **IMPORTANT**: Complete this section BEFORE Section 2.4 (Pull Docker Images). The .env file must be updated before pulling images or running any containers, as scripts read configuration from this file.

#### Step 1: Download server .env to local machine

The server .env and the local repo .env are on different machines, so you need to compare them locally.

- [ ] **Copy server .env to local machine via scp**
  ```bash
  # From your LOCAL machine (not the server)
  # Replace <server> with your server hostname/alias
  # If you connect via a specific user, use user@<server>
  # If you connect via a specific port, add -P <port>
  scp <server>:/data/kyg_data_forecast_tools/config/.env_develop_kghm \
      ~/Downloads/.env_develop_kghm_server
  ```

#### Step 2: Compare with local repo .env

- [ ] **Compare the two files locally**
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
| `ieasyhydroforecast_organization` | Organization identifier | e.g., `kghm` |

**Variables to preserve** (don't overwrite with repo values):
- `IEASYHYDRO_HOST` - Server-specific API endpoint
- `IEASYHYDRO_PASSWORD` - Credentials
- `ieasyhydroforecast_API_KEY_GATEAWAY` - API keys
- Path variables if customized for server

#### Step 4: Edit the server .env locally

- [ ] **Make a working copy**
  ```bash
  cp ~/Downloads/.env_develop_kghm_server ~/Downloads/.env_develop_kghm_updated
  ```

- [ ] **Edit the file locally** (use your preferred editor)
  ```bash
  code ~/Downloads/.env_develop_kghm_updated
  # Or: nano, vim, etc.
  ```

- [ ] **Add new variables**
- [ ] **Update changed variables**
- [ ] **Verify Docker image tags are set correctly**

#### Step 5: Upload updated .env back to server

- [ ] **Copy updated .env to server via scp**
  ```bash
  # From your LOCAL machine
  scp ~/Downloads/.env_develop_kghm_updated \
      <server>:/data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] **Verify on server**
  ```bash
  # On the SERVER
  grep -E "docker_image_tag" /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```
  Expected output:
  ```
  ieasyhydroforecast_backend_docker_image_tag=local
  ieasyhydroforecast_frontend_docker_image_tag=local
  ```

- [ ] **Validate syntax on server** (no trailing spaces, proper quoting)
  ```bash
  grep -n "= " /data/kyg_data_forecast_tools/config/.env_develop_kghm  # Spaces after =
  grep -n " $" /data/kyg_data_forecast_tools/config/.env_develop_kghm  # Trailing spaces
  ```

---

### 2.5 Update Crontabs

Update the cron schedule for automated forecast runs.

> **Note**: The cron scripts handle Docker image pulling automatically. Once crontabs are configured, the first scheduled run will pull the required images.

> **Important**: Adapt paths for your server. Replace `/home/ubuntu` with your user's home directory (e.g., `/home/sapphire`).

- [ ] **Backup current crontab** (if not done in pre-update)
  ```bash
  crontab -l > ~/crontab_backup_$(date +%Y%m%d).txt
  ```

- [ ] **Review the recommended crontab schedule**

  The full recommended schedule is documented in [deployment.md - Set up cron job](../deployment.md#set-up-cron-job).

- [ ] **Edit crontab**
  ```bash
  crontab -e
  ```

- [ ] **Verify/update the following cron entries:**

  ```bash
  # m h  dom mon dow   command
  # ---------------------------------------------------------------------------
  # SAPPHIRE Forecast Tools Schedule (Times are in UTC)
  # Adapt /home/ubuntu to your server's user home directory
  # ---------------------------------------------------------------------------

  # Log cleanup: delete logs older than 7 days
  0 2 * * * find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete

  # (1) Gateway Preprocessing at 03:00 UTC
  0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_gateway_preprocessing_$(date +\%Y\%m\%d).log 2>&1

  # (2) Pentadal Forecast at 04:00 UTC
  0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_pentadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (3) Decadal Forecast at 05:00 UTC
  0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_decadal_forecast_$(date +\%Y\%m\%d).log 2>&1

  # (4) Maintenance jobs (evening UTC)
  # Frontend update
  2 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_update_sapphire_frontend.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_frontend_$(date +\%Y\%m\%d).log 2>&1

  # Preprocessing runoff maintenance (gap filling)
  4 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_preprunoff_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_preprunoff_maintenance_$(date +\%Y\%m\%d).log 2>&1

  # ML model maintenance (hindcast catch-up)
  0 20 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_ml_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_ml_maintenance_$(date +\%Y\%m\%d).log 2>&1

  # Linear regression maintenance (hindcast catch-up)
  34 20 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_linreg_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_linreg_maintenance_$(date +\%Y\%m\%d).log 2>&1
  ```

- [ ] **Verify crontab was saved correctly**
  ```bash
  crontab -l
  ```

- [ ] **Ensure log directory exists**
  ```bash
  mkdir -p /home/ubuntu/logs
  ls -la /home/ubuntu/logs/
  ```

- [ ] **Verify cron service is running**
  ```bash
  sudo systemctl status cron
  ```

### 2.6 Test Cron Commands Manually

After updating crontabs, run each cron command manually (one by one) to verify they work correctly before waiting for scheduled execution. The Luigi daemon starts automatically when needed.

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

- [ ] **Run maintenance jobs** (optional - run if time permits)
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_update_sapphire_frontend.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_preprunoff_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_ml_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  cd /data/SAPPHIRE_Forecast_Tools && bash bin/daily_linreg_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] **Monitor progress** in Luigi UI at http://localhost:8082
- [ ] **Check logs** for errors after each command completes

---

## 3. POST-UPDATE VERIFICATION

### 3.1 Start Services

Start the services in the correct order to ensure proper initialization.

#### Start Luigi Daemon (First)

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

#### Start Dashboards (Second)

- [ ] Start the pentad and decad dashboards:
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml --env-file /data/kyg_data_forecast_tools/config/.env_develop_kghm up -d
  ```

#### Verify Services Started

- [ ] Confirm all containers are running:
  ```bash
  docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  ```

Expected output should show:
- `sapphire-luigi-daemon` (or similar) - Up, port 8082
- `sapphire-frontend-forecast-pentad` - Up, port 5006
- `sapphire-frontend-forecast-decad` - Up, port 5007

### 3.2 Verify Services Running

#### Check Luigi UI

- [ ] Open browser to Luigi web interface: `http://<your-server-ip>:8082`
- [ ] Verify the UI loads without errors
- [ ] Check that no tasks are stuck in "RUNNING" state from before the update

#### Check Dashboards

- [ ] Pentad dashboard accessible: `http://<your-server-ip>:5006/forecast_dashboard`
- [ ] Decad dashboard accessible: `http://<your-server-ip>:5007/forecast_dashboard`
- [ ] Both dashboards load data correctly (charts display, station selector works)

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

- [ ] Review dashboard logs for startup errors:
  ```bash
  docker logs sapphire-frontend-forecast-pentad --tail 50
  docker logs sapphire-frontend-forecast-decad --tail 50
  ```

### 3.3 Test Forecast Run

Perform a quick manual test to verify the pipeline works correctly.

#### Quick Test - Run Preprocessing Gateway

This is a lightweight test that verifies the pipeline infrastructure:

- [ ] Run preprocessing gateway task:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/run_preprocessing_gateway.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

- [ ] Monitor progress in Luigi UI at http://localhost:8082
- [ ] Check logs for successful completion:
  ```bash
  tail -f /home/ubuntu/logs/sapphire_gateway_$(date +%Y%m%d).log
  ```

#### Full Test - Run Pentadal Forecast (Optional)

For a comprehensive test, run a full pentadal forecast cycle:

- [ ] Run pentadal forecast:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  bash bin/run_pentadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_pentadal_$(date +%Y%m%d).log 2>&1
  ```

- [ ] Monitor progress in Luigi UI
- [ ] Verify forecast outputs were generated in the intermediate_data directory

#### Verify in Logs

- [ ] Check that no ERROR or CRITICAL messages appear in logs:
  ```bash
  grep -iE "error|critical|exception" /home/ubuntu/logs/sapphire_*.log | tail -20
  ```

- [ ] Verify tasks completed successfully in Luigi UI (all tasks show green checkmarks)

---

## 4. LOG CLEANUP (Optional)

Clean up old log files to prevent disk space issues.

### 4.1 Clean Up Pipeline Logs

Log files are stored in `/home/ubuntu/logs/`

- [ ] View current log files and sizes:
  ```bash
  ls -lh /home/ubuntu/logs/sapphire_*.log
  ```

- [ ] Delete logs older than 7 days:
  ```bash
  find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete
  ```

- [ ] Verify cleanup (check remaining files):
  ```bash
  ls -lh /home/ubuntu/logs/
  ```

### 4.2 Clean Up Docker Logs (Optional)

Docker container logs can also grow large over time.

- [ ] Check intermediate_data/docker_logs if it exists:
  ```bash
  ls -lh /data/kyg_data_forecast_tools/intermediate_data/docker_logs/ 2>/dev/null || echo "Directory does not exist"
  ```

- [ ] Clean up old Docker logs (if directory exists):
  ```bash
  find /data/kyg_data_forecast_tools/intermediate_data/docker_logs -name "*.log" -mtime +7 -delete 2>/dev/null
  ```

### 4.3 Prune Docker System (Optional)

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

## 5. ROLLBACK PROCEDURE

If the update causes issues, follow these steps to revert.

### 5.1 Stop Current Services

- [ ] Stop all SAPPHIRE services:
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml down
  docker compose -f bin/docker-compose-luigi.yml down
  ```

### 5.2 Restore Previous Docker Images

- [ ] Pull previous image versions (replace `<previous-tag>` with actual version):
  ```bash
  docker pull mabesa/sapphire-pipeline:<previous-tag>
  docker pull mabesa/sapphire-dashboard:<previous-tag>
  ```

- [ ] Or use locally cached previous images if available:
  ```bash
  docker images | grep sapphire
  ```

### 5.3 Restore .env Backup

If you backed up your .env file before the update:

- [ ] Restore the backup (use your backup directory from Section 1.4):
  ```bash
  cp /home/ubuntu/backups/sapphire_<timestamp>/.env_develop_kghm /data/kyg_data_forecast_tools/config/.env_develop_kghm
  ```

### 5.4 Update Image Tags

- [ ] Edit your .env file to use the previous image tag:
  ```bash
  # Set these variables in your .env file:
  ieasyhydroforecast_backend_docker_image_tag=<previous-tag>
  ieasyhydroforecast_frontend_docker_image_tag=<previous-tag>
  ```

### 5.5 Restart Services with Previous Version

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

- [ ] Start dashboards:
  ```bash
  docker compose -f bin/docker-compose-dashboards.yml --env-file /data/kyg_data_forecast_tools/config/.env_develop_kghm up -d
  ```

- [ ] Verify rollback was successful (repeat Section 3.2 verification steps)

---

## 6. FINAL CHECKLIST

Complete this summary checklist before considering the update complete.

### Services Running

- [ ] Luigi daemon is running and accessible at port 8082
- [ ] Pentad dashboard is running and accessible at port 5006
- [ ] Decad dashboard is running and accessible at port 5007
- [ ] All containers show "healthy" status

### Crontabs Configured

- [ ] Verify crontab entries are correct:
  ```bash
  crontab -l
  ```

- [ ] Confirm scheduled times are appropriate for your timezone
- [ ] Verify log cleanup job is configured (typically at 02:00 UTC)

### Next Scheduled Run

- [ ] Identify next scheduled forecast run from crontab
- [ ] Note the expected time: _______________
- [ ] Plan to check logs after next scheduled run to confirm everything works

### Documentation

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
  docker stop sapphire-frontend-forecast-pentad
  # Wait for alert, then restart
  docker start sapphire-frontend-forecast-pentad
  ```

---

## 7. SERVER OS UPDATES (Periodic Maintenance)

Periodically update the server operating system for security patches and stability.

### 7.1 Pre-Update Checks

- [ ] Check disk space (updates need space):
  ```bash
  df -h
  ```
  Ensure at least 2GB free on `/` and `/var`

- [ ] Note current kernel version (for rollback reference):
  ```bash
  uname -r
  ```

- [ ] Check for held packages:
  ```bash
  apt-mark showhold
  ```

- [ ] Optionally stop SAPPHIRE services before reboot:
  ```bash
  cd /data/SAPPHIRE_Forecast_Tools
  docker compose -f bin/docker-compose-dashboards.yml down
  docker compose -f bin/docker-compose-luigi.yml down
  ```

### 7.2 Update Server

- [ ] Update package lists:
  ```bash
  sudo apt update
  ```

- [ ] Upgrade installed packages:
  ```bash
  sudo apt upgrade
  ```
  Review changes before confirming. Note if kernel update is included.

- [ ] Reboot if required (especially after kernel updates):
  ```bash
  sudo reboot
  ```

- [ ] After reboot, remove stale packages:
  ```bash
  sudo apt autoremove && sudo apt clean
  ```

### 7.3 Post-Reboot Verification

- [ ] Verify SSH tunnel to iEasyHydro HF is running (if configured):
  ```bash
  sudo systemctl status ieasyhydro-tunnel.service
  lsof -i :5555
  curl -s http://localhost:5555/api/v1/ | head
  ```

- [ ] Verify Docker daemon is running:
  ```bash
  sudo systemctl status docker
  docker ps
  ```

- [ ] Verify cron service is running:
  ```bash
  sudo systemctl status cron
  ```

- [ ] Verify time synchronization:
  ```bash
  timedatectl status
  ```

- [ ] Start SAPPHIRE services by running the crontab scripts (if stopped before reboot):
  

- [ ] Verify all SAPPHIRE containers are running:
  ```bash
  docker ps --filter "name=sapphire" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  ```

- [ ] Check dashboards are accessible:
  ```bash
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5007/forecast_dashboard
  ```

### 7.4 Optional: Docker Maintenance

- [ ] Prune unused Docker resources:
  ```bash
  docker system prune -f
  ```

- [ ] Check Docker disk usage:
  ```bash
  docker system df
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
df -h /data /home/ubuntu/logs

# Check Docker disk usage
docker system df
```

---

*Last updated: 2026-01-30*
*Added Section 7: Server OS Updates*
