#!/bin/bash

# Set variables, change these to match your setup
REMOTE_USER="username"
REMOTE_HOST="123.456.789.12"
REMOTE_PORT="5678"
LOCAL_PORT="1234"
DESTINATION_HOST="localhost"
DESTINATION_PORT="1234"
SSH_KEY_PATH="~/.ssh/id_ed12345"

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
    ssh -i $SSH_KEY_PATH -fN -L $LOCAL_PORT:$DESTINATION_HOST:$DESTINATION_PORT $REMOTE_USER@$REMOTE_HOST -p $REMOTE_PORT &
    echo "SSH tunnel created."
fi

