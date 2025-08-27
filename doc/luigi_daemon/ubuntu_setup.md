## Luigi daemon on Ubuntu (Docker Compose)

This guide sets up the Luigi scheduler (luigid) on Ubuntu using Docker Compose. This is the recommended, production-grade method and matches how the run scripts connect to the daemon.

### Requirements
- Ubuntu Server with Docker and Docker Compose installed
- Port 8082 available on the host (Luigi web UI)
- SAPPHIRE_Forecast_Tools cloned on the server (recommended path: `/data/SAPPHIRE_Forecast_Tools`)

Optional but recommended:
- A stable Compose project name so daemon and tasks share one network

### 1) Remove legacy systemd luigid (if previously installed)
If you ever set up a systemd luigi service, disable it so Docker can bind port 8082 and to avoid conflicts.

```bash
sudo systemctl stop luigid || true
sudo systemctl disable luigid || true
sudo systemctl stop luigi || true
sudo systemctl disable luigi || true
sudo rm -f /etc/systemd/system/luigid.service /etc/systemd/system/luigi.service
sudo systemctl daemon-reload
```

Also stop any old containers holding port 8082:

```bash
docker ps --format '{{.ID}}\t{{.Names}}\t{{.Ports}}' | grep ':8082->8082' | awk '{print $1}' | xargs -r docker stop
```

### 2) Start the Luigi daemon with Compose

From the repository root:

```bash
cd /data/SAPPHIRE_Forecast_Tools

# Use a stable project name so daemon and tasks share the same network
export COMPOSE_PROJECT_NAME=sapphire

# Start the daemon container (idempotent)
docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon

# Wait until the UI is reachable
until curl -fsS http://localhost:8082/ >/dev/null; do sleep 1; done
```

Notes:
- It’s normal to see warnings about missing environment variables when starting only `luigi-daemon`; the service doesn’t use them.
- If you want to silence warnings, you can provide an env file with `--env-file /path/to/.env`.

### 3) Verify the daemon

- Open the UI: http://localhost:8082
- From a task container on the same Compose network:

```bash
docker compose -f bin/docker-compose-luigi.yml run --rm --entrypoint sh preprocessing-gateway -lc \
	'apk add --no-cache curl >/dev/null 2>&1 || true; curl -fsS http://luigi-daemon:8082/ | head -c 80 && echo'
```

### 4) Run pipeline scripts

The provided scripts will connect to the daemon by service name `luigi-daemon` and won’t try to rebuild it if it’s already running:

```bash
bash bin/run_preprocessing_gateway.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm
bash bin/run_pentadal_forecasts.sh    /data/kyg_data_forecast_tools/config/.env_develop_kghm
bash bin/run_decadal_forecasts.sh     /data/kyg_data_forecast_tools/config/.env_develop_kghm
```

They automatically write a `luigi.cfg` with:

```
[core]
scheduler_host = luigi-daemon
scheduler_port = 8082
```

### Troubleshooting
- “port is already allocated”: another process/container is using 8082. Stop it or change that service.
- Task can’t resolve `luigi-daemon`: ensure the same `COMPOSE_PROJECT_NAME` is used for both daemon and task commands so they share one network.
- UI is up but API probe fails: use `/` for readiness; some luigid builds respond 500 on `/api/ping` early in startup.