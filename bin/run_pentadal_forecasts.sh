#!/bin/bash

# This script runs the PENTADAL forecasting for SAPPHIRE forecast tools
# Usage: bash bin/run_pentadal_forecast.sh <env_file_path>

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Running PENTADAL forecasting"

# Read the configuration from the .env file
read_configuration $1

# Always talk to the daemon via its Docker DNS name (portable across macOS/Linux)
LUIGI_SCHEDULER_HOST="luigi-daemon"
LUIGI_SCHEDULER_PORT="8082"
echo "| Luigi scheduler URL set to: http://${LUIGI_SCHEDULER_HOST}:${LUIGI_SCHEDULER_PORT}"

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit (closes SSH tunnel)
trap cleanup_pentadal_forecasting_containers EXIT

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

# Start the Docker Compose service for pentadal forecasting
echo "| Starting pentadal forecasting workflow..."
echo "| Luigi daemon will handle dependencies and ensure preprocessing is complete"

# Create a luigi.cfg file with explicit scheduler host/port
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}

[worker]
check_complete_on_run = true
EOF

# Luigi's task-failure return codes default to ZERO (luigi/retcodes.py:
# task_failed, missing_data, already_running, scheduling_error and not_run are
# all 0; only unhandled_exception defaults to 4). A failed task would therefore
# exit 0 and any status this script returns would be meaningless.
#
# LUIGI_CONFIG_PATH is required, not optional: Luigi resolves the bare
# 'luigi.cfg' entry in its default search path relative to the process CWD, and
# the Compose service sets working_dir: /app/apps/pipeline, so the image's own
# apps/pipeline/luigi.cfg wins and the file mounted at /app/luigi.cfg is never
# read. LUIGI_CONFIG_PATH goes through add_config_path(), which APPENDS to the
# search path rather than replacing it, so the image's [core]/[resources]/
# [worker] settings still apply and [retcode] is layered on top.
cat >> temp_luigi.cfg <<'EOF'

[retcode]
unhandled_exception = 4
missing_data = 5
task_failed = 1
already_running = 6
scheduling_error = 7
not_run = 8
EOF
LUIGI_RETCODE_DOCKER_ARGS=(-e LUIGI_CONFIG_PATH=/app/luigi.cfg)

# Run the pentadal forecasting with proper configuration
# Note: PYTHONPATH=/app is set in docker-compose-luigi.yml for Luigi module resolution
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    -e SAPPHIRE_PREDICTION_MODE=PENTAD \
    ${LUIGI_RETCODE_DOCKER_ARGS[@]+"${LUIGI_RETCODE_DOCKER_ARGS[@]}"} \
    --user root \
    --rm \
    pentadal
COMPOSE_STATUS=$?

echo "| Pentadal forecasting task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"

# COMPOSE_STATUS now reflects Luigi's own outcome (see the [retcode] block
# above -- without it a failed task would still exit 0).
if [ "$COMPOSE_STATUS" -eq 0 ]; then
    echo "| Pentadal forecasting: SUCCESS."
else
    echo "| Pentadal forecasting: FAILED (exit ${COMPOSE_STATUS})."
fi

exit "$COMPOSE_STATUS"