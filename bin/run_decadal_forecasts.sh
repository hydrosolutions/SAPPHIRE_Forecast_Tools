#!/bin/bash

# This script runs the DECADAL forecasting for SAPPHIRE forecast tools
# Usage: bash bin/run_decadal_forecast.sh <env_file_path>

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running DECADAL forecasting"

# Read the configuration from the .env file
read_configuration $1

# Always talk to the daemon via its Docker DNS name (portable across macOS/Linux)
LUIGI_SCHEDULER_HOST="luigi-daemon"
LUIGI_SCHEDULER_PORT="8082"
echo "| Luigi scheduler URL set to: http://${LUIGI_SCHEDULER_HOST}:${LUIGI_SCHEDULER_PORT}"

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup_decadal_forecasting_containers EXIT

# Ensure a stable Compose project so services share the same network
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sapphire}"

# Start the Luigi daemon only if not already reachable
if curl -fsS "http://localhost:${LUIGI_SCHEDULER_PORT}/" >/dev/null; then
    echo "| Luigi daemon already running; skipping start"
else
    docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
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
fi

# Start the Docker Compose service for decadal forecasting
echo "| Starting decadal forecasting workflow..."
echo "| Luigi daemon will handle dependencies and ensure preprocessing is complete"

# Create a luigi.cfg file with explicit scheduler host/port
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}
EOF

# Run the decadal forecasting with proper configuration
docker compose -f bin/docker-compose-luigi.yml run \
        -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
        -e PYTHONPATH="/home/appuser/.local/lib/python3.11/site-packages:${PYTHONPATH}" \
        -e SAPPHIRE_PREDICTION_MODE=DECAD \
        --user root \
        --rm \
        decadal

echo "| Decadal forecasting task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"