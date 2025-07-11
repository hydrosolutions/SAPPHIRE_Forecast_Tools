#!/bin/bash

# Loads SMTP configuration from .env file
# Creates a log directory at /var/log/docker_monitor
# Monitors Docker events for two types of events: 
#  - Container crash (die event)
#  - Container health status change to unhealthy
# When an event is detected, it: 
#  - Captures the container logs (last 1000 lines)
#  - Sends an email alert with the log content

# Configuration
LOG_DIR="/var/log/docker_monitor"
MAX_LOGS=100        # Maximum number of log files to keep
MAX_LOG_DAYS=30     # Maximum age of log files in days

# Create a pipe for the docker events command
EVENT_PIPE="${LOG_DIR}/event_pipe"
[ -p "$EVENT_PIPE" ] || mkfifo "$EVENT_PIPE"

# Signal handling for graceful shutdown
cleanup() {
    echo "Shutting down monitor script..."
    # Kill the docker events process if it's running
    if [[ -n "$DOCKER_PID" ]] && kill -0 "$DOCKER_PID" 2>/dev/null; then
        kill "$DOCKER_PID"
    fi
    
    # Remove the named pipe
    [ -p "$EVENT_PIPE" ] && rm "$EVENT_PIPE"
    
    exit 0
}

# Set up trap for signals
trap cleanup SIGTERM SIGINT SIGHUP

# Log rotation function - removes old logs
rotate_logs() {
    # Delete logs older than MAX_LOG_DAYS days
    find "$LOG_DIR" -name "*.log" -type f -mtime +${MAX_LOG_DAYS} -delete 2>/dev/null
    
    # If we still have too many logs, delete oldest ones
    log_count=$(find "$LOG_DIR" -name "*.log" | wc -l)
    if [[ $log_count -gt $MAX_LOGS ]]; then
        ls -1tr "$LOG_DIR"/*.log | head -n $(($log_count - $MAX_LOGS)) | xargs rm -f
    fi
}

# Load configuration from .env file
ENV_FILE="${DOCKER_MONITOR_ENV_PATH:-./apps/config/.env}"  # fallback if not set
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

mkdir -p "$LOG_DIR"

# Test if the smtp configuration is complete
if [[ -z "$SMTP_SERVER" || -z "$SMTP_PORT" || -z "$SMTP_USER" || -z "$SMTP_PASS" || -z "$SENDER" || -z "$RECIPIENT" ]]; then
    echo "[ERROR] SMTP configuration is incomplete. Please check your .env file."
    exit 1
fi

send_alert() {
    subject="$1"
    body="$2"
    log_file="$3"

    # Use a file for the password instead of exposing it in process arguments
    PASS_FILE=$(mktemp)
    echo "$SMTP_PASS" > "$PASS_FILE"
    chmod 600 "$PASS_FILE"

    {
        echo "Subject: $subject"
        echo "To: $RECIPIENT"
        echo "From: $SENDER"
        echo
        echo -e "$body"
        [ -f "$log_file" ] && echo -e "\n---- Logs ----\n$(cat $log_file)"
    } | msmtp --host=$SMTP_SERVER --port=$SMTP_PORT --auth=on \
              --user=$SMTP_USER --passwordeval="cat $PASS_FILE" \
              --tls=on --tls-starttls=on $RECIPIENT
    
    # Clean up the temporary password file
    rm -f "$PASS_FILE"
}

# Start docker event monitoring in background and redirect to the named pipe
docker events --filter event=die --filter event=health_status:unhealthy > "$EVENT_PIPE" &
DOCKER_PID=$!

# Initial log rotation
rotate_logs

# Counter for periodic log rotation
event_counter=0

# Read from the pipe
while read line; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    container_id=$(echo "$line" | awk '{print $NF}')
    name=$(docker inspect --format='{{.Name}}' "$container_id" | sed 's/\///g' 2>/dev/null || echo "unknown")
    log_file="${LOG_DIR}/${name}_$(date +%Y%m%d%H%M%S).log"

    docker logs --tail 1000 "$container_id" &> "$log_file" 2>/dev/null

    if [[ "$line" == *"die"* ]]; then
        send_alert "Docker ALERT: $name crashed" "Container $name exited at $timestamp" "$log_file"
    elif [[ "$line" == *"unhealthy"* ]]; then
        send_alert "Docker ALERT: $name became unhealthy" "Container $name is unhealthy as of $timestamp" "$log_file"
    fi
    
    # Perform log rotation every 10 events
    ((event_counter++))
    if [[ $event_counter -ge 10 ]]; then
        rotate_logs
        event_counter=0
    fi
done < "$EVENT_PIPE"

# If we exit the loop, clean up
cleanup