#!/bin/bash

# This script runs the LONG-TERM forecasting for SAPPHIRE forecast tools.
# It submits RunLongTermWorkflow to Luigi, which internally runs
# LTScheduleQuery to determine which modes are active today.
#
# Usage: bash bin/run_long_term_forecasts.sh [--dry-run] <env_file_path>
#
# Flags:
#   --dry-run   Validate configuration and compose file, print what would
#               happen, then exit without starting any containers.

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# ---------------------------------------------------------------------------
# Argument parsing: extract --dry-run before passing remaining args through
# ---------------------------------------------------------------------------
DRY_RUN=false
POSITIONAL_ARGS=()

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true ;;
        *)         POSITIONAL_ARGS+=("$1") ;;
    esac
    shift
done

# Print the banner
print_banner
echo "| Running LONG-TERM forecasting"

if $DRY_RUN; then
    echo "| [DRY RUN] Mode enabled — no containers will be started"
fi

# Read the configuration from the .env file
read_configuration "${POSITIONAL_ARGS[0]:-}"

# ---------------------------------------------------------------------------
# Compose YAML validation (runs in both normal and dry-run mode)
# ---------------------------------------------------------------------------
echo "|"
echo "| Validating docker-compose-luigi.yml ..."
if docker compose -f bin/docker-compose-luigi.yml config >/dev/null 2>&1; then
    echo "| Compose file validation: OK"
else
    echo "| ERROR: docker-compose-luigi.yml validation failed"
    docker compose -f bin/docker-compose-luigi.yml config 2>&1 | head -5
    exit 1
fi

# Always talk to the daemon via its Docker DNS name (portable across macOS/Linux)
LUIGI_SCHEDULER_HOST="luigi-daemon"
LUIGI_SCHEDULER_PORT="8082"
echo "| Luigi scheduler URL set to: http://${LUIGI_SCHEDULER_HOST}:${LUIGI_SCHEDULER_PORT}"

# ---------------------------------------------------------------------------
# Dry-run: print plan and exit
# ---------------------------------------------------------------------------
if $DRY_RUN; then
    echo "|"
    echo "| [DRY RUN] Would submit RunLongTermWorkflow to Luigi."
    echo "| Luigi will run LTScheduleQuery internally to determine active modes."
    echo "|   Schedule query image: mabesa/sapphire-lt-forecasting:${ieasyhydroforecast_backend_docker_image_tag}"
    echo "|   Config: ${ieasyhydroforecast_env_file_path}"
    echo "|   Pipeline submission:"
    echo "|     docker compose -f bin/docker-compose-luigi.yml run --rm long-term"
    echo "|"
    echo "| [DRY RUN] Validation complete. Exiting without starting containers."
    exit 0
fi

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup_long_term_forecasting_containers EXIT

# Ensure a stable Compose project so services share the same network
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sapphire}"

# --- Ensure Luigi daemon is running ---
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

# --- Submit long-term workflow to Luigi ---
echo "| Starting long-term forecasting workflow..."

# Create a luigi.cfg file with explicit scheduler host/port
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}

[worker]
check_complete_on_run = true
EOF

# Run the long-term forecasting with proper configuration
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    --user root \
    --rm \
    long-term

echo "| Long-term forecasting task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"
