#!/bin/bash

# This script runs a periodic maintenance task via Luigi.
#
# Usage: bash bin/run_periodic_maintenance.sh <task_type> <env_file_path>
#
# Task types:
#   long_term      - Bimonthly long-term postprocessing (1st of odd months)
#   skill_recalc   - Yearly full skill metrics recalculation (December 31)
#   snow_norms     - Yearly snow norm recalculation (31 August)
#
# Example:
#   bash bin/run_periodic_maintenance.sh long_term /path/to/config/.env

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Parse task type argument
TASK_TYPE="${1}"
if [ -z "$TASK_TYPE" ]; then
    echo "| Error: task_type argument required."
    echo "| Usage: bash bin/run_periodic_maintenance.sh <task_type> <env_file_path>"
    echo "| Valid task_types: long_term, skill_recalc, snow_norms"
    exit 1
fi
echo "| Running Periodic Maintenance: ${TASK_TYPE}"

# Read the configuration from the .env file (second argument)
read_configuration $2

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

echo "| Starting periodic maintenance workflow (task_type=${TASK_TYPE})..."

# Create a luigi.cfg file with explicit scheduler host/port
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}

[worker]
check_complete_on_run = true
EOF

# Run the periodic maintenance workflow
export MAINTENANCE_TASK_TYPE="${TASK_TYPE}"
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    -e MAINTENANCE_TASK_TYPE=${TASK_TYPE} \
    --user root \
    --rm \
    periodic-maintenance

echo "| Periodic maintenance (${TASK_TYPE}) task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"
