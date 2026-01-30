#!/bin/bash

# This script runs only the GATEWAY preprocessing step for SAPPHIRE forecast tools
# This can run early (10:00 local time) as it doesn't depend on daily runoff data
# Usage: bash bin/run_preprocessing_gateway.sh <env_file_path>

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running GATEWAY PREPROCESSING only"

# Read the configuration from the .env file
read_configuration $1

echo "| Environment configuration loaded from: $1"
echo "| Docker image tag: ${ieasyhydroforecast_backend_docker_image_tag}"

# Always talk to the daemon via its Docker DNS name (portable across macOS/Linux)
LUIGI_SCHEDULER_HOST="luigi-daemon"
LUIGI_SCHEDULER_PORT="8082"
echo "| Luigi scheduler URL set to: http://${LUIGI_SCHEDULER_HOST}:${LUIGI_SCHEDULER_PORT}"

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Ensure a stable Compose project so services share the same network
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sapphire}"

# Ensure the Luigi daemon container exists and is running within this compose project
DAEMON_CID=$(docker compose -f bin/docker-compose-luigi.yml ps -q luigi-daemon)
if [ -n "$DAEMON_CID" ] && docker inspect -f '{{.State.Running}}' "$DAEMON_CID" 2>/dev/null | grep -q true; then
  echo "| Luigi daemon (compose) already running; skipping start"
else
  echo "| Starting Luigi daemon via compose"
  docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
fi

# Wait for the daemon to be ready (use UI endpoint which returns 200)
echo -n "| Waiting for Luigi daemon to be ready"
for i in {1..60}; do
  if curl -fsS "http://localhost:${LUIGI_SCHEDULER_PORT}/" >/dev/null; then
    echo " - ready"
    break
  fi
  echo -n "."
  sleep 1
done

# Set the trap to clean up processes on exit 
trap cleanup_preprocessing_containers EXIT

# Create a minimal Luigi config that uses host/port (avoid default_scheduler_url)
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}
EOF

# Regular command
# Note: PYTHONPATH=/app is set in docker-compose-luigi.yml for Luigi module resolution
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    --user root \
    --rm \
    preprocessing-gateway

echo "| Gateway preprocessing task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"