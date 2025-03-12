#!/bin/bash
#==============================================================================
# SAPPHIRE FULL DEPLOYMENT
#==============================================================================
# Description:
#   This script deploys the complete SAPPHIRE forecasting system including both
#   backend processing pipeline and frontend dashboards. It handles Docker image
#   management, container orchestration, network connectivity, and cleanup.
#
# Usage:
#   Interactive mode:
#     bash bin/deploy_sapphire_forecast_tools.sh <env_file_path>
#
#   Background execution:
#     nohup bash bin/deploy_sapphire_forecast_tools.sh <env_file_path> > output.log 2>&1 &
#
# Parameters:
#   env_file_path - Absolute path to the .env configuration file containing
#                   environment variables for the SAPPHIRE forecast tools
#
# Process:
#   1. Reads configuration from the specified .env file
#   2. Performs complete Docker cleanup (all containers and images)
#   3. Pulls and verifies fresh Docker images
#   4. Establishes SSH tunnel for data access (if configured)
#   5. Starts both forecasting pipeline and dashboard services
#   6. Monitors execution and handles cleanup on completion
#
# Performance:
#   Typical execution time: ~4 minutes on standard hardware with good connectivity
#   Resource usage: High during initial deployment, moderate during operation
#
# Directory Structure Requirements:
#   ieasyhydroforecast_data_root_dir/
#   ├── SAPPHIRE_forecast_tools/       # This repository
#   │   ├── apps/                      # Application code
#   │   ├── bin/                       # Scripts directory
#   │   │   ├── utils/                 # Helper functions
#   │   │   └── docker-compose-*.yml   # Docker compose files
#   │   └── ...
#   │
#   └── ieasyhydroforecast_data_ref_dir/
#       ├── config/
#       │   └── .env                   # Environment file
#       ├── bin/
#       │   └── .ssh/                  # SSH configuration
#       └── ...
#
# Dependencies:
#   - utils/common_functions.sh - Core utility functions
#   - utils/clean_docker.sh - Docker environment cleanup
#   - utils/pull_docker_images.sh - Docker image management
#   - docker-compose-luigi.yml - Backend orchestration definition
#   - docker-compose-dashboards.yml - Frontend orchestration definition
#   - .ssh/open_ssh_tunnel.sh - Data access setup
#   - .ssh/close_ssh_tunnel.sh - Network cleanup
#   - Cosign - For container image signature verification
#
# Exit Codes:
#   0 - Success: Deployment completed successfully
#   1 - Failure: Error in setup or execution
#
# Security:
#   - Docker images are cryptographically verified using Cosign before deployment
#   - Required public key: PROJECT_ROOT/keys/cosign.pub
#
# Note:
#   This script performs a COMPLETE rebuild of the Docker environment.
#   For incremental updates, use run_sapphire_forecast_tools.sh and
#   daily_update_sapphire_frontend.sh instead.
#
# Contributors:
#   - Beatrice Marti: Implementation and integration
#   - Developed with assistance from AI pair programming tools
#==============================================================================

# Source the common functions
source "bin/utils/common_functions.sh"

# Print the banner
print_banner "SAPPHIRE Full Deployment"
echo "| Starting deployment at $(date)"

# Read the configuration from the .env file
read_configuration $1

# Clean up the Docker space (note: this will remove all containers and images)
echo "| Performing complete Docker cleanup"
clean_out_docker_space

# Setup cosign for signature verification
echo "| Setting up image verification"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "SCRIPT_DIR=$SCRIPT_DIR"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "PROJECT_ROOT=$PROJECT_ROOT"

# By default, look for the public key in the Git repository
# Check if we already have a variable COSIGN_PUBLIC_KEY set
if [ -z "$COSIGN_PUBLIC_KEY" ]; then
    export COSIGN_PUBLIC_KEY="$PROJECT_ROOT/keys/cosign.pub"
fi
setup_cosign "$COSIGN_PUBLIC_KEY" || exit 1

# pull backend images and verify signatures
pull_docker_images $ieasyhydroforecast_backend_docker_image_tag

# pull frontend images and verify signatures
# Pull and verify the frontend images
REPO="mabesa"
IMAGES="sapphire-configuration sapphire-dashboard"

echo "| Pulling and verifying frontend images with TAG=${TAG}"
all_successful=true

for image in $IMAGES; do
    echo "| Processing $image:${TAG}"
    if ! pull_and_verify_image "${REPO}/${image}:${TAG}" true; then
        echo "| ❌ Failed to pull/verify ${image}:${TAG}"
        all_successful=false
        break
    fi
    echo "| ✅ Successfully pulled and verified ${image}:${TAG}"
done

# Create marker file if all images were pulled and verified successfully
if $all_successful; then
    echo "$(date)" > "${MARKER_FILE}"
    echo "| Frontend images prepared successfully"
    echo "| Marker created at: ${MARKER_FILE}"
else
    echo "| ⚠️ Warning: Not all images were successfully pulled/verified"
    rm -f "${MARKER_FILE}"  # Remove marker file if it exists
fi

# Removing old images
echo "| Removing old images"
docker image prune -f

# Establish SSH tunnel (if required)
establish_ssh_tunnel

# Set the trap to clean up processes on exit
trap cleanup_deployment EXIT

# Start the Docker Compose service for the forecasting pipeline
echo "| Starting backend forecasting pipeline"
start_docker_compose_luigi

# Start the Docker Compose service for the dashboards
echo "| Starting frontend dashboards"
start_docker_compose_dashboards

# Wait for forecasting pipeline to finish
echo "| Monitoring backend service (this may take several minutes)"
wait $DOCKER_COMPOSE_LUIGI_PID
echo "| Backend forecasting pipeline completed"

# Wait for dashboards to finish
echo "| Monitoring dashboard services"
wait $DOCKER_COMPOSE_DASHBOARD_PID
echo "| Dashboard services completed"

echo "| SAPPHIRE deployment completed successfully at $(date)"
exit 0