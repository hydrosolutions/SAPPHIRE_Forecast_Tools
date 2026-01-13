#!/usr/bin/env bash
set -euo pipefail

CONTAINER="sapphire-postprocessing-api"
SCRIPT="python -u app/data_migrator.py"

# Optional: ensure container exists and is running
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Error: container '$CONTAINER' is not running."
  echo "Running containers:"
  docker ps --format '  - {{.Names}}'
  exit 1
fi

run() {
  echo "==> $*"
  docker exec -i "$CONTAINER" bash -lc "$*"
}

# Base run (no type)
run "$SCRIPT"

# Runs by type
run "$SCRIPT --type skillmetric"
run "$SCRIPT --type lrforecast"
run "$SCRIPT --type combinedforecast"
run "$SCRIPT --type forecast"

echo "✅ Done."
