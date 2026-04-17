#!/bin/bash

# This script restarts the full SAPPHIRE stack defined in
# sapphire/docker-compose.yml (4 PostgreSQL databases, 5 FastAPI services,
# and the dashboard service on port 5006).
#
# Usage:
# Run the script in your terminal from the repository root:
# bash bin/restart_sapphire_stack.sh <env_file_path>
#
# argument <env_file_path> is the absolute path to the .env file containing your
# environment variables for the SAPPHIRE forecast tools. The argument is REQUIRED
# (no fallback to an environment variable - we pass it explicitly via --env-file
# to docker compose as well).
#
# Details: The script performs the following tasks:
# 1. Parse the argument <env_file_path> and load env vars via read_configuration
# 2. Take down the LEGACY bin/docker-compose-dashboards.yml stack first to
#    prevent port 5006 conflicts with the new dashboard service defined in
#    sapphire/docker-compose.yml
# 3. Take down the current sapphire/docker-compose.yml stack
# 4. Prune stopped containers
# 5. Pull the latest dashboard image (tag from $ieasyhydroforecast_frontend_docker_image_tag)
# 6. Prune dangling images
# 7. Bring the sapphire stack back up in detached mode
# 8. Show service state with `docker compose ps` so the user can verify health
#
# Note: This script only manages the sapphire stack. It does NOT set up SSH
# tunnels or touch the backend/luigi pipeline. Run this script from the
# repository root so the relative compose paths resolve correctly.
#
# Author: Beatrice Marti

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Read the configuration from the .env file
read_configuration $1

# Taking down the LEGACY dashboards first to free port 5006
echo "|      "
echo "| ------"
echo "| Stopping LEGACY dashboards (bin/docker-compose-dashboards.yml) to free port 5006"
echo "| ------"
docker compose -f bin/docker-compose-dashboards.yml down || true

# Taking down the current sapphire stack
echo "|      "
echo "| ------"
echo "| Stopping the current sapphire stack (sapphire/docker-compose.yml)"
echo "| ------"
docker compose --env-file "$1" -f sapphire/docker-compose.yml down

# Remove unused containers
echo "| Removing unused containers"
docker container prune -f

# Pulling the dashboard image with the tag $ieasyhydroforecast_frontend_docker_image_tag
echo "| Pulling with TAG=$ieasyhydroforecast_frontend_docker_image_tag"
docker pull mabesa/sapphire-dashboard:$ieasyhydroforecast_frontend_docker_image_tag

# Removing old dangling images
echo "| Removing old images"
docker image prune -f

# Bring the sapphire stack back up
echo "|      "
echo "| ------"
echo "| Starting the sapphire stack (sapphire/docker-compose.yml)"
echo "| ------"
docker compose --env-file "$1" -f sapphire/docker-compose.yml up -d

# Wait a few seconds for services to start up, then show state
sleep 5
echo "|      "
echo "| ------"
echo "| Current service state"
echo "| ------"
docker compose --env-file "$1" -f sapphire/docker-compose.yml ps

echo "|      "
echo "| ------"
echo "| Sapphire stack restart completed!"
echo "| Dashboard:    http://localhost:5006/forecast_dashboard"
echo "| API gateway:  http://localhost:8000"
echo "| ------"
