#!/bin/bash

# Monitors logs from specific docker containers for error patterns
# Sends email alerts when errors are detected, including the last 1000 lines of logs
# Uses rate limiting to prevent email flooding

# Configuration
containers=("sapphire-frontend-forecast-pentad" "sapphire-frontend-forecast-decad")
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

# Load configuration from .env file
ENV_FILE="${DOCKER_MONITOR_ENV_PATH:-./apps/config/.env}"
if [ -f "$ENV_FILE" ]; then
  set -o allexport
  source "$ENV_FILE"
  set +o allexport
else
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi

SMTP_SERVER=${SAPPHIRE_PIPELINE_SMTP_SERVER}
SMTP_PORT=${SAPPHIRE_PIPELINE_SMTP_PORT}
SMTP_USER=${SAPPHIRE_PIPELINE_SMTP_USERNAME}
SMTP_PASS=${SAPPHIRE_PIPELINE_SMTP_PASSWORD}
SENDER=${SAPPHIRE_PIPELINE_SENDER_EMAIL}
RECIPIENT=${SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS}

# Test if the SMTP configuration is complete
if [[ -z "$SMTP_SERVER" || -z "$SMTP_PORT" || -z "$SMTP_USER" || -z "$SMTP_PASS" || -z "$SENDER" || -z "$RECIPIENT" ]]; then
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
    
    # Use a file for the password instead of exposing it in process arguments
    PASS_FILE=$(mktemp)
    echo "$SMTP_PASS" > "$PASS_FILE"
    chmod 600 "$PASS_FILE"
    
    # Send the email with error and log context
    {
        echo "Subject: Dashboard Error Detected ($container)"
        echo "To: $RECIPIENT"
        echo "From: $SENDER"
        echo "Content-Type: text/plain; charset=UTF-8"
        echo
        echo -e "$message"
        echo
        echo "================= LOG CONTEXT (LAST 1000 LINES) ================="
        echo
        echo "$LOG_CONTEXT"
        echo
        echo "==============================================================="
    } | msmtp --host=$SMTP_SERVER --port=$SMTP_PORT --auth=on \
              --user=$SMTP_USER --passwordeval="cat $PASS_FILE" \
              --tls=on --tls-starttls=on $RECIPIENT
              
    # Clean up temporary password file
    rm -f "$PASS_FILE"
    
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
        while true; do
            # Check if container exists and is running
            if docker ps --format '{{.Names}}' | grep -q "^$c$"; then
                # Follow logs from the container
                docker logs -f "$c" 2>&1 | grep --line-buffered -Ei "$pattern" | 
                while read line; do
                    ts=$(date '+%Y-%m-%d %H:%M:%S')
                    send_alert "$c" "[$ts] $line"
                done
            else
                echo "Container $c not running. Will check again in 60 seconds."
                sleep 60
            fi
            
            # Small delay before attempting to reconnect to logs
            sleep 5
        done
    ) &
done

# Wait for all background processes
wait