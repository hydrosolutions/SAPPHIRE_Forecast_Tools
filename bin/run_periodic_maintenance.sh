#!/bin/bash

# This script runs a periodic maintenance task via Luigi.
#
# Usage: bash bin/run_periodic_maintenance.sh <task_type> <env_file_path>
#        bash bin/run_periodic_maintenance.sh lt_recovery <env_file_path> <mode> <YYYY-MM-DD>
#
# Task types:
#   long_term      - Bimonthly long-term postprocessing (1st of odd months)
#   skill_recalc   - Yearly full skill metrics recalculation (December 31)
#   snow_norms     - Yearly snow norm recalculation (31 August)
#   lt_recovery    - Regenerate ONE missed long-term forecast month for ONE mode.
#                    Takes two extra arguments: the forecast mode and the ISO
#                    issue date. Only the current and previous calendar month
#                    can be recovered, and the date must be the mode's
#                    configured issue date (near misses are refused, not
#                    snapped). The run refuses if any member row already exists
#                    for that month, marks recovered rows with flag=1, and reads
#                    the rows back from the database before reporting success.
#                    Unlike the other task types this one RETURNS the Luigi exit
#                    status. Exit 0 means rows were written AND read back.
#                    A non-zero exit means the recovery was NOT CONFIRMED --
#                    it does NOT mean the database is unchanged: only a refusal
#                    (child exit 2) guarantees nothing was written, while a
#                    read-back or forecast failure (child exit 1) can leave
#                    partial or even complete rows behind. Read the container
#                    log before re-running.
#                    Note: it also overwrites the mode's forecast/hindcast CSVs.
#
#                    LIMITATION -- check/write race: the guard and the write are
#                    two separate steps, so a concurrent operational run or a
#                    manual writer that inserts rows between them will be
#                    overwritten by the recovery's upsert. There is no lock.
#                    Run this only when no long-term forecast is in flight.
#
# Example:
#   bash bin/run_periodic_maintenance.sh long_term /path/to/config/.env
#   bash bin/run_periodic_maintenance.sh lt_recovery /path/to/config/.env month_0 2026-08-01

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Parse task type argument
TASK_TYPE="${1}"
if [ -z "$TASK_TYPE" ]; then
    echo "| Error: task_type argument required."
    echo "| Usage: bash bin/run_periodic_maintenance.sh <task_type> <env_file_path>"
    echo "| Valid task_types: long_term, skill_recalc, snow_norms, lt_recovery"
    exit 1
fi
echo "| Running Periodic Maintenance: ${TASK_TYPE}"

# Extra arguments for the dated long-term recovery. Empty for every other
# task type, which is what the compose command and the Luigi task expect.
LT_RECOVERY_MODE=""
LT_RECOVERY_ISSUE_DATE=""
if [ "$TASK_TYPE" = "lt_recovery" ]; then
    LT_RECOVERY_MODE="${3}"
    LT_RECOVERY_ISSUE_DATE="${4}"
    if [ -z "$LT_RECOVERY_MODE" ] || [ -z "$LT_RECOVERY_ISSUE_DATE" ]; then
        echo "| Error: lt_recovery requires a forecast mode and an ISO issue date."
        echo "| Usage: bash bin/run_periodic_maintenance.sh lt_recovery <env_file_path> <mode> <YYYY-MM-DD>"
        echo "| Example: bash bin/run_periodic_maintenance.sh lt_recovery ./config/.env month_0 2026-08-01"
        exit 1
    fi
    if ! echo "$LT_RECOVERY_ISSUE_DATE" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
        echo "| Error: issue date must be ISO YYYY-MM-DD, got '${LT_RECOVERY_ISSUE_DATE}'."
        exit 1
    fi
    echo "| Long-term recovery: mode=${LT_RECOVERY_MODE} issue_date=${LT_RECOVERY_ISSUE_DATE}"
fi

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

# Luigi's task-failure return codes default to ZERO (luigi/retcodes.py:
# task_failed, missing_data, already_running, scheduling_error and not_run are
# all 0; only unhandled_exception defaults to 4). A failed task would therefore
# exit 0 and any status this script returns would be meaningless. Map the
# failures onto non-zero for the recovery path only; the other task types keep
# Luigi's defaults so their behaviour is unchanged.
#
# LUIGI_CONFIG_PATH is required, not optional: Luigi resolves the bare
# 'luigi.cfg' entry in its default search path relative to the process CWD, and
# the Compose service sets working_dir: /app/apps/pipeline, so the image's own
# apps/pipeline/luigi.cfg wins and the file mounted at /app/luigi.cfg is never
# read. LUIGI_CONFIG_PATH goes through add_config_path(), which APPENDS to the
# search path rather than replacing it, so the image's [core]/[resources]/
# [worker] settings still apply and [retcode] is layered on top.
RECOVERY_DOCKER_ARGS=()
if [ "$TASK_TYPE" = "lt_recovery" ]; then
    cat >> temp_luigi.cfg <<'EOF'

[retcode]
unhandled_exception = 4
missing_data = 5
task_failed = 1
already_running = 6
scheduling_error = 7
not_run = 8
EOF
    RECOVERY_DOCKER_ARGS=(-e LUIGI_CONFIG_PATH=/app/luigi.cfg)
fi

# Run the periodic maintenance workflow.
# The exports feed the compose command interpolation; the -e flags put the same
# values inside the container.
export MAINTENANCE_TASK_TYPE="${TASK_TYPE}"
export MAINTENANCE_LT_MODE="${LT_RECOVERY_MODE}"
export MAINTENANCE_LT_ISSUE_DATE="${LT_RECOVERY_ISSUE_DATE}"
docker compose -f bin/docker-compose-luigi.yml run \
    -v $(pwd)/temp_luigi.cfg:/app/luigi.cfg \
    -e MAINTENANCE_TASK_TYPE=${TASK_TYPE} \
    -e MAINTENANCE_LT_MODE="${LT_RECOVERY_MODE}" \
    -e MAINTENANCE_LT_ISSUE_DATE="${LT_RECOVERY_ISSUE_DATE}" \
    ${RECOVERY_DOCKER_ARGS[@]+"${RECOVERY_DOCKER_ARGS[@]}"} \
    --user root \
    --rm \
    periodic-maintenance
COMPOSE_STATUS=$?

echo "| Periodic maintenance (${TASK_TYPE}) task submitted to Luigi daemon"
echo "| Check progress at: http://localhost:${LUIGI_SCHEDULER_PORT}"

# lt_recovery is a repair an operator is waiting on, so its outcome has to
# reach the caller. The other task types keep their historical behaviour
# (status ignored) so this change cannot alter existing cron lines.
if [ "$TASK_TYPE" = "lt_recovery" ]; then
    if [ "$COMPOSE_STATUS" -eq 0 ]; then
        echo "| Long-term recovery for ${LT_RECOVERY_MODE} ${LT_RECOVERY_ISSUE_DATE}:"
        echo "|   SUCCESS - rows were written and read back from the database."
    else
        echo "| Long-term recovery for ${LT_RECOVERY_MODE} ${LT_RECOVERY_ISSUE_DATE}:"
        echo "|   NOT CONFIRMED (exit ${COMPOSE_STATUS}). This does NOT mean the"
        echo "|   database is unchanged. Read the container log:"
        echo "|     'REFUSED' (child exit 2) - nothing ran, no rows were written."
        echo "|     'FAILED'  (child exit 1) - the forecast ran; rows may be"
        echo "|                absent, partial or complete. A read-back error in"
        echo "|                particular says nothing about what was written."
        echo "|   Check the month in the database before re-running: the guard"
        echo "|   will refuse a re-run if any member row now exists."
    fi
    exit "$COMPOSE_STATUS"
fi
