#!/bin/bash

# Simple Luigi daemon runner for development
# Works on both Mac and Linux
#
# Usage: bash dev/run_luigi_dev.sh

echo "Starting Luigi daemon for development..."

# Set Python path
export PYTHONPATH="/data/SAPPHIRE_forecast_tools:$PYTHONPATH"

# Create log directory
mkdir -p logs

# Check if Luigi is installed
if ! command -v luigid &> /dev/null; then
    echo "Luigi not found. Installing..."
    pip install luigi
fi

# Kill any existing Luigi daemon
if [ -f /tmp/luigid.pid ]; then
    echo "Stopping existing Luigi daemon..."
    kill $(cat /tmp/luigid.pid) 2>/dev/null || true
    rm -f /tmp/luigid.pid
fi

# Start Luigi daemon
echo "Starting Luigi daemon on port 8082..."
luigid \
    --port 8082 \
    --pidfile /tmp/luigid.pid \
    --logdir logs \
    --state-path logs/luigi-state.pickle \
    --background

# Wait a moment for startup
sleep 2

# Check if running
if [ -f /tmp/luigid.pid ] && ps -p $(cat /tmp/luigid.pid) > /dev/null; then
    echo "Luigi daemon started successfully!"
    echo "PID: $(cat /tmp/luigid.pid)"
    echo "Web UI: http://localhost:8082"
    echo ""
    echo "To stop: kill $(cat /tmp/luigid.pid)"
else
    echo "Failed to start Luigi daemon"
    exit 1
fi