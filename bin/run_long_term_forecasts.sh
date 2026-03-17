#!/bin/bash

# This script runs the LONG-TERM forecasting for SAPPHIRE forecast tools.
# It queries lt_schedule_query.py to determine which modes are active today,
# then submits the RunLongTermWorkflow to Luigi with the active modes.
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
    echo "| [DRY RUN] Schedule query command that would run:"
    echo "|   docker run --rm --network host \\"
    echo "|     -v ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config \\"
    echo "|     -v ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data \\"
    echo "|     -e ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path} \\"
    echo "|     -e IN_DOCKER=True \\"
    echo "|     mabesa/sapphire-lt-forecasting:${ieasyhydroforecast_backend_docker_image_tag} \\"
    echo "|     uv run python lt_schedule_query.py"
    echo "|"
    echo "| [DRY RUN] If active modes are found, Luigi submission would run:"
    echo "|   docker compose -f bin/docker-compose-luigi.yml run --rm long-term"
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

# --- Schedule query: determine which long-term modes are active today ---
echo "|"
echo "| Running schedule query to determine active long-term modes..."

SCHEDULE_JSON=$(docker run --rm \
    --network host \
    -v "${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config" \
    -v "${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data" \
    -e "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}" \
    -e "IN_DOCKER=True" \
    "mabesa/sapphire-lt-forecasting:${ieasyhydroforecast_backend_docker_image_tag}" \
    uv run python lt_schedule_query.py 2>/dev/null)

if [ $? -ne 0 ] || [ -z "$SCHEDULE_JSON" ]; then
    echo "| ERROR: Schedule query failed or returned empty result"
    echo "| Exiting without running long-term forecasts"
    exit 1
fi

echo "| Schedule query result: $SCHEDULE_JSON"

# Parse active_modes and skill_metric_types from JSON
LT_ACTIVE_MODES=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(','.join(d['active_modes']))" "$SCHEDULE_JSON")
LT_SKILL_METRIC_TYPES=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(','.join(d['skill_metric_types']))" "$SCHEDULE_JSON")

echo "| Active modes: ${LT_ACTIVE_MODES:-none}"
echo "| Skill metric types: ${LT_SKILL_METRIC_TYPES:-none}"

# If no active modes, exit early
if [ -z "$LT_ACTIVE_MODES" ]; then
    echo "| No long-term forecast modes active today. Exiting."
    exit 0
fi

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
echo "| Active modes: ${LT_ACTIVE_MODES}"

export LT_ACTIVE_MODES
export LT_SKILL_METRIC_TYPES

# Create a luigi.cfg file with explicit scheduler host/port
cat > temp_luigi.cfg <<EOF
[core]
scheduler_host = ${LUIGI_SCHEDULER_HOST}
scheduler_port = ${LUIGI_SCHEDULER_PORT}
EOF

# Run the long-term forecasting with proper configuration
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    --user root \
    --rm \
    long-term

echo "| Long-term forecasting task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"
