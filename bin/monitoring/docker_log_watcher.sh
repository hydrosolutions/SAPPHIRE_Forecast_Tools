#!/bin/bash

# Monitors logs from specific docker containers for error patterns
# Sends email alerts when errors are detected, including the last 1000 lines of logs
# Uses rate limiting to prevent email flooding

# Configuration
# Override with DASHBOARD_LOG_WATCH_CONTAINERS="name1 name2" for a deployment
# whose dashboard container is named differently. Names that do not exist on
# this host are skipped by the loop below.
read -r -a containers <<< "${DASHBOARD_LOG_WATCH_CONTAINERS:-sapphire-dashboard sapphire-frontend-forecast-pentad sapphire-frontend-forecast-decad}"
pattern="404|ERROR|Exception"
MIN_ALERT_INTERVAL=3600  # Minimum seconds between alerts for the same container

# Signal handling for graceful shutdown
cleanup() {
    echo "Shutting down log watcher script..."
    # Kill any running processes
    jobs -p | xargs -r kill
    exit 0
}

# Set up trap for signals
trap cleanup SIGTERM SIGINT SIGHUP

# Shared SMTP alerting plumbing (env-file resolution, completeness check,
# compose-and-send). See bin/monitoring/lib/mail.sh for what it does and the
# deliberate grep-not-source behaviour change it makes versus the old
# `set -o allexport; source` below. Derive our own directory rather than
# assuming cwd, since this runs under systemd with an absolute ExecStart and
# no particular working directory.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/lib/mail.sh"

# Load configuration from .env file
ENV_FILE="${DOCKER_MONITOR_ENV_PATH:-./apps/config/.env}"
if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi

# Resolves MAIL_SMTP_*, MAIL_SENDER, MAIL_RECIPIENTS and MAIL_ORG (the
# prefer-org-fallback-hostname deployment identifier used in alert subjects)
# from $ENV_FILE -- see bin/monitoring/lib/mail.sh.
mail_resolve_config "$ENV_FILE"

# Test if the SMTP configuration is complete
if ! mail_config_is_complete; then
    echo "[ERROR] SMTP configuration is incomplete. Please check your .env file."
    exit 1
fi

# Track last alert time for each container
declare -A last_alert_time

send_alert() {
    container="$1"
    message="$2"
    
    # Rate limiting: Check if we've sent an alert recently for this container
    current_time=$(date +%s)
    last_time=${last_alert_time[$container]:-0}
    time_diff=$((current_time - last_time))
    
    if [ $time_diff -lt $MIN_ALERT_INTERVAL ]; then
        echo "Rate limiting: Skipping alert for $container (last alert was $time_diff seconds ago)"
        return
    fi
    
    # Update the last alert time
    last_alert_time[$container]=$current_time
    
    # Capture the last 1000 lines of logs from the container
    echo "Capturing last 1000 lines of logs from $container for context..."
    LOG_CONTEXT=$(docker logs --tail 1000 "$container" 2>&1)

    # Pre-render the body exactly as the old inline `{ ... } | msmtp` block
    # did: `echo -e` over $message, but plain `echo` (no escape
    # interpretation) over the log context -- mail_send prints whatever body
    # text it is given verbatim, so that distinction has to stay here; see
    # bin/monitoring/lib/mail.sh for why.
    rendered_body=$(
        echo -e "$message"
        echo
        echo "================= LOG CONTEXT (LAST 1000 LINES) ================="
        echo
        echo "$LOG_CONTEXT"
        echo
        echo "==============================================================="
    )

    mail_send "Dashboard Error Detected ($container)" "$rendered_body" \
        "Content-Type: text/plain; charset=UTF-8"

    echo "Alert sent with log context for container $container"
}

# Check if containers exist before monitoring
for c in "${containers[@]}"; do
    if ! docker ps --format '{{.Names}}' | grep -q "^$c$"; then
        echo "Warning: Container $c not found. Will monitor when/if it starts."
    fi
done

# Start monitoring each container
for c in "${containers[@]}"; do
    ( 
        # Tracks whether the "not running" message has already been logged
        # for the CURRENT absence, so repeated poll iterations stay quiet.
        # Reset to false whenever the container is seen running, so a later
        # disappearance is reported again (log on state CHANGE, not on
        # every poll).
        absent_reported=false
        while true; do
            # Check if container exists and is running
            if docker ps --format '{{.Names}}' | grep -q "^$c$"; then
                absent_reported=false
                # Follow logs from the container. --tail 0 means only output
                # produced from this moment forward is seen -- errors that
                # occurred while the watcher was not running are missed
                # entirely. That is deliberate: without it, every restart of
                # this script (or reconnect after the container restarts)
                # replays the container's whole history and re-alerts on old,
                # already-resolved errors, which trains operators to ignore
                # the alerts -- the exact failure mode this monitoring exists
                # to prevent.
                docker logs -f --tail 0 "$c" 2>&1 | grep --line-buffered -Ei "$pattern" | 
                while read line; do
                    ts=$(date '+%Y-%m-%d %H:%M:%S')
                    send_alert "$c" "[$ts] $line"
                done
            else
                if [[ "$absent_reported" != true ]]; then
                    echo "Container $c not running. Will check again in 60 seconds."
                    absent_reported=true
                fi
                sleep 60
            fi
            
            # Small delay before attempting to reconnect to logs
            sleep 5
        done
    ) &
done

# Wait for all background processes
wait
