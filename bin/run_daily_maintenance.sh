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

# Run the daily maintenance workflow with ML concurrency limited to 3
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    ${LUIGI_RETCODE_DOCKER_ARGS[@]+"${LUIGI_RETCODE_DOCKER_ARGS[@]}"} \
    --user root \
    --rm \
    daily-maintenance
COMPOSE_STATUS=$?

echo "| Daily maintenance task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"

# COMPOSE_STATUS now reflects Luigi's own outcome (see the [retcode] block
# above -- without it a failed task would still exit 0). Report it here, but
# do NOT exit on it: the frontend update below currently runs unconditionally
# after this step and must keep doing so (INFRA-047) -- an early exit here
# would silently stop the frontend from being refreshed on every deployment
# whenever the Luigi step fails.
if [ "$COMPOSE_STATUS" -eq 0 ]; then
    echo "| Daily maintenance (Luigi): SUCCESS."
else
    echo "| Daily maintenance (Luigi): FAILED (exit ${COMPOSE_STATUS})."
fi

# --- Frontend update (runs on host, not in Luigi DAG) ---
# Always run this, regardless of COMPOSE_STATUS above -- see the comment
# there. Its own status is captured immediately and aggregated below (sticky
# aggregate, same pattern as bin/initialize_site_backfill.sh's
# `overall_exit`, main() :594-608): the wrapper exits 0 only if BOTH the
# Luigi step and the frontend update succeeded. No `set -e`, no early exit.
echo "| Updating frontend dashboard..."
bash bin/daily_update_sapphire_frontend.sh "$1"
FRONTEND_STATUS=$?
if [ "$FRONTEND_STATUS" -eq 0 ]; then
    echo "| Frontend update completed"
else
    echo "| Frontend update FAILED (exit ${FRONTEND_STATUS})"
fi

OVERALL_STATUS=0
if [ "$COMPOSE_STATUS" -ne 0 ] || [ "$FRONTEND_STATUS" -ne 0 ]; then
    OVERALL_STATUS=1
fi
exit "$OVERALL_STATUS"
