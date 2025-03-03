#!/bin/bash

# Set variables, change these to match your setup
REMOTE_USER="username"
REMOTE_HOST="123.456.789.12"
REMOTE_PORT="5678"
LOCAL_PORT="1234"
DESTINATION_HOST="localhost"
DESTINATION_PORT="1234"
SSH_KEY_PATH="~/.ssh/id_ed12345"

# Print SSH_KEY_PATH
echo "SSH_KEY_PATH: $SSH_KEY_PATH"

# Print current key permissions and change them if necessary
echo "Current key permissions:"
ls -l $SSH_KEY_PATH
current_perms=$(stat -c %a "$SSH_KEY_PATH")
if [ "$current_perms" != "400" ]; then
    echo "WARNING: Fixing SSH key permissions (current: $current_perms)"
    chmod 600 "$SSH_KEY_PATH" || {
        echo "ERROR: Failed to set correct permissions on SSH key"
        exit 1
    }
fi

# Check if the tunnel already exists
TUNNEL_PID=$(lsof -ti:$LOCAL_PORT)
TUNNEL_EXISTS=false

if [ -z "$TUNNEL_PID" ]; then
    echo "SSH tunnel does not exist. Creating SSH tunnel..."
    TUNNEL_EXISTS=false
else
    echo "Tunnel already exists. PID: $TUNNEL_PID"
    TUNNEL_EXISTS=true
fi

if [ "$TUNNEL_EXISTS" = false ]; then
    # Create the SSH tunnel
    echo "Creating SSH tunnel..."
    ssh -v -i "$SSH_KEY_PATH" \
        -fNT \
        -o StrictHostKeyChecking=accept-new \
        -o ServerAliveInterval=60 \
        -o ExitOnForwardFailure=yes \
        -o ConnectTimeout=10 \
        -L "$LOCAL_HOST:$LOCAL_PORT:$DESTINATION_HOST:$DESTINATION_PORT" \
        "$REMOTE_USER@$REMOTE_HOST" \
        -p "$REMOTE_PORT"

    SSH_STATUS=$?
    echo "SSH command exit status: $SSH_STATUS"

    if [ $SSH_STATUS -ne 0 ]; then
        echo "ERROR: SSH command failed with status $SSH_STATUS"
        exit 1
    fi

    # Verify that tunnel is successfully created and open
    TUNNEL_PID=$(lsof -ti:$LOCAL_PORT)
    if [ -z "$TUNNEL_PID" ]; then
        echo "ERROR: Failed to create SSH tunnel"
        exit 1
    fi

    # Immediately get the PID after starting
    ieasyhydroforecast_ssh_tunnel_pid=$TUNNEL_PID

    if [ -z "$ieasyhydroforecast_ssh_tunnel_pid" ]; then
        echo "ERROR: Failed to capture tunnel PID"
        exit 1
    fi

    export ieasyhydroforecast_ssh_tunnel_pid

    # Try to verify the connection
    echo "Attempting to verify connection..."
    nc -z localhost $LOCAL_PORT
    if [ $? -eq 0 ]; then
        echo "Port $LOCAL_PORT is accessible"
    else
        echo "WARNING: Port $LOCAL_PORT is not accessible"
    fi

    echo "SSH tunnel created."
fi

# Print a footer for the script
echo "----------------------------------------"
