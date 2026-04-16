#!/bin/bash

# This script runs the daily maintenance workflow via Luigi.
#
# It replaces the individual daily maintenance cron jobs with a single
# Luigi-orchestrated pipeline that enforces dependency order:
#   PrepRunoff + Gateway → LinReg → ML → PostProcessing → Frontend
#
# Usage: bash bin/run_daily_maintenance.sh <env_file_path>
#
# The original individual maintenance scripts (daily_*.sh) remain
# functional for manual invocation.

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running Daily Maintenance via Luigi"

# Read the configuration from the .env file
read_configuration $1

# Always talk to the daemon via its Docker DNS name
LUIGI_SCHEDULER_HOST="luigi-daemon"
LUIGI_SCHEDULER_PORT="8082"
echo "| Luigi scheduler URL set to: http://${LUIGI_SCHEDULER_HOST}:${LUIGI_SCHEDULER_PORT}"

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup EXIT

# Ensure a stable Compose project so services share the same network
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sapphire}"

# Ensure the Luigi daemon container exists and is running
DAEMON_CID=$(docker compose -f bin/docker-compose-luigi.yml ps -q luigi-daemon)
if [ -n "$DAEMON_CID" ] && docker inspect -f '{{.State.Running}}' "$DAEMON_CID" 2>/dev/null | grep -q true; then
    echo "| Luigi daemon (compose) already running; skipping start"
else
    echo "| Starting Luigi daemon via compose"
    docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
fi

# Wait for the daemon to be ready
echo -n "| Waiting for Luigi daemon to be ready"
for i in {1..60}; do
    if curl -fsS "http://localhost:${LUIGI_SCHEDULER_PORT}/" >/dev/null; then
        echo " - ready"
        break
    fi
    echo -n "."
    sleep 1
done

echo "| Starting daily maintenance workflow..."

# Create a luigi.cfg file with explicit scheduler host/port
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}

[worker]
check_complete_on_run = true
EOF

# Run the daily maintenance workflow with ML concurrency limited to 3
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    --user root \
    --rm \
    daily-maintenance

echo "| Daily maintenance task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"

# --- Frontend update (runs on host, not in Luigi DAG) ---
echo "| Updating frontend dashboard..."
bash bin/daily_update_sapphire_frontend.sh "$1"
echo "| Frontend update completed"
