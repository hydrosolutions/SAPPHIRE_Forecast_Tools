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

# Containers that are SUPPOSED to stay running. Only these raise alerts;
# without this, every normal pipeline task exit ("die") would alert.
# Override in the environment to suit a deployment. Each entry is matched as
# a plain substring against the full container name (see the matching loop
# below), so a bare service name like "luigi-daemon" matches regardless of
# the Compose project prefix or numeric replica suffix Compose adds
# (e.g. "sapphire-luigi-daemon-1").
MONITORED_CONTAINERS="${DOCKER_MONITOR_CONTAINERS:-sapphire-api-gateway sapphire-preprocessing-api sapphire-postprocessing-api sapphire-user-api sapphire-auth-api sapphire-dashboard sapphire-preprocessing-db sapphire-postprocessing-db sapphire-user-db sapphire-auth-db luigi-daemon}"

# Create a pipe for the docker events command
EVENT_PIPE="${LOG_DIR}/event_pipe"
# LOG_DIR must exist before mkfifo can create the pipe inside it; the later
# mkdir -p below is kept for clarity but this one is what makes a fresh
# install (no /var/log/docker_monitor yet) work.
mkdir -p "$LOG_DIR"
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

    # The To: header keeps the comma-separated form (correct for mail
    # headers), but msmtp takes each recipient as its own argument, so the
    # commas are converted to spaces only for the argument list below.
    RECIPIENT_ARGS=${RECIPIENT//,/ }

    {
        echo "Subject: $subject"
        echo "To: $RECIPIENT"
        echo "From: $SENDER"
        echo
        echo -e "$body"
        [ -f "$log_file" ] && echo -e "\n---- Logs ----\n$(cat $log_file)"
    } | msmtp --host=$SMTP_SERVER --port=$SMTP_PORT --auth=on \
              --user=$SMTP_USER --passwordeval="cat $PASS_FILE" \
              --tls=on --tls-starttls=on --from="$SENDER" $RECIPIENT_ARGS
    
    # Clean up the temporary password file
    rm -f "$PASS_FILE"
}

# NOTE: `docker events` has no "name=" filter key (it is silently ignored,
# not an error) and its "container=" filter only matches an exact container
# name/ID or a *prefix* of one -- not an arbitrary substring -- so it cannot
# express "contains luigi-daemon" once a variable Compose project prefix
# comes first. So the MONITORED_CONTAINERS check is done in the read loop
# below (via true substring matching against the full container name)
# instead of as a `docker events --filter` argument. `docker events` itself
# is therefore left filtering only by event type, same as before.

# Emit ID, action and container name as explicit fields (delimited by "|",
# which cannot appear in a container name) instead of the human-readable
# default line. The default line's last whitespace field is the closing
# "name=<container>)" attribute, not a container ID -- awk '{print $NF}' on
# it never yields a usable ID, so `docker inspect` on that value always
# failed. Reading the name directly out of the event also means we no
# longer need `docker inspect` at all, which cannot look up a --rm
# container that is already gone by the time its die event arrives.
#
# NOTE: "--filter event=health_status:unhealthy" (the value used previously)
# delivers ZERO live events -- verified against a real daemon, it is not
# valid docker events filter syntax and silently matches nothing, so the
# unhealthy-container alert has never fired. The correct filter is plain
# "event=health_status" (matches both healthy and unhealthy transitions);
# the classification below (on $action) already picks out "unhealthy" only.
# Start docker event monitoring in background and redirect to the named pipe
docker events --filter event=die --filter event=health_status \
    --format '{{.Actor.ID}}|{{.Action}}|{{.Actor.Attributes.name}}' > "$EVENT_PIPE" &
DOCKER_PID=$!

# Initial log rotation
rotate_logs

# Counter for periodic log rotation
event_counter=0

# Read from the pipe. The three fields below come straight from the
# --format string on the docker events command above: container ID,
# action ("die" or "health_status: unhealthy"), and container name.
while IFS='|' read -r container_id action name; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    # Only containers matching an entry in MONITORED_CONTAINERS raise an
    # alert; an empty MONITORED_CONTAINERS watches everything (previous
    # behaviour). Matching is a plain substring check against the full
    # container name, so it works regardless of Compose project prefix.
    # If the name could not be determined, fail OPEN (treat as monitored)
    # rather than silently dropping an event we can't identify.
    if [[ -n "$MONITORED_CONTAINERS" && -n "$name" ]]; then
        is_monitored=false
        for _monitored_container in $MONITORED_CONTAINERS; do
            if [[ "$name" == *"$_monitored_container"* ]]; then
                is_monitored=true
                break
            fi
        done
        [[ "$is_monitored" == true ]] || continue
    fi

    log_file="${LOG_DIR}/${name}_$(date +%Y%m%d%H%M%S).log"

    docker logs --tail 1000 "$container_id" &> "$log_file" 2>/dev/null

    if [[ "$action" == *"die"* ]]; then
        send_alert "Docker ALERT: $name crashed" "Container $name exited at $timestamp" "$log_file"
    elif [[ "$action" == *"unhealthy"* ]]; then
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
