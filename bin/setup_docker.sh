#!/bin/bash

# setup_docker.sh
# Script to pull necessary docker images for SAPPHIRE forecast tools
# Usage: bash setup_docker.sh <env_file_path>

# Function to display script usage
show_usage() {
  echo "Usage: bash setup_docker.sh <env_file_path>"
  echo "Example: bash setup_docker.sh .env"
}

# Function to check if docker daemon is running
check_docker_daemon() {
  echo "Checking Docker daemon status..."
  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not running. Attempting to start..."
    
    # Check OS and start Docker accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
      echo "Detected macOS, starting Docker Desktop..."
      open -a Docker
      
      # Wait for Docker to start (max 30 seconds)
      for i in {1..30}; do
        echo "Waiting for Docker to start ($i/30)..."
        if docker info >/dev/null 2>&1; then
          echo "Docker successfully started!"
          return 0
        fi
        sleep 1
      done
      echo "Error: Failed to start Docker. Please start Docker manually."
      exit 1
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
      echo "Detected Linux, trying to start Docker service..."
      sudo systemctl start docker
      sleep 5
      if ! docker info >/dev/null 2>&1; then
        echo "Error: Failed to start Docker. Please start Docker manually."
        exit 1
      fi
      echo "Docker successfully started!"
    else
      echo "Error: Unsupported OS for automatic Docker startup. Please start Docker manually."
      exit 1
    fi
  else
    echo "Docker daemon is running."
  fi
}

# Check if environment file is provided
if [ -z "$1" ]; then
  echo "Error: No .env file path provided"
  show_usage
  exit 1
fi

ENV_FILE="$1"

# Check if the provided file exists
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: File $ENV_FILE not found"
  exit 1
fi

# Source the common functions
SCRIPT_DIR=$(dirname "$0")
source "${SCRIPT_DIR}/utils/common_functions.sh"

# Print the banner
print_banner
echo "| Setting up Docker images for SAPPHIRE forecast tools"
echo "| This script pulls necessary Docker images based on the provided .env configuration"

# Check if Docker daemon is running
check_docker_daemon

# Remove any existing Docker containers and images except those related to Nginx
bash "${SCRIPT_DIR}/utils/clean_docker.sh" --execute

# Load the configuration from the provided .env file
read_configuration "$ENV_FILE"

# Set default tag if not defined
TAG="${ieasyhydroforecast_backend_docker_image_tag:-latest}"
ORGANIZATION="${ieasyhydroforecast_organization:-demo}"
RUN_ML_MODELS="${ieasyhydroforecast_run_ML_models:-false}"
RUN_CM_MODELS="${ieasyhydroforecast_run_CM_models:-false}"

echo "Docker image tag: $TAG"
echo "Organization: $ORGANIZATION"
echo "Run ML models: $RUN_ML_MODELS"
echo "Run Conceptual models: $RUN_CM_MODELS"

echo "=== Pulling base Docker images ==="

# Pull common images used in all versions
echo "Pulling common images..."
docker pull mabesa/sapphire-pythonbaseimage:$TAG || { echo "Failed to pull pythonbaseimage"; exit 1; }
docker pull mabesa/sapphire-preprunoff:$TAG || { echo "Failed to pull preprunoff"; exit 1; }
docker pull mabesa/sapphire-linreg:$TAG || { echo "Failed to pull linreg"; exit 1; }
docker pull mabesa/sapphire-postprocessing:$TAG || { echo "Failed to pull postprocessing"; exit 1; }
docker pull mabesa/sapphire-rerun:$TAG || { echo "Failed to pull rerun"; exit 1; }

# Pull organization-specific images
echo "=== Pulling env-specific images ==="

if [ "${RUN_ML_MODELS,,}" = "true" ]; then
    echo "Pulling ML models images..."
    need_gateway_flag=True
    docker pull mabesa/sapphire-ml:$TAG || { echo "Failed to pull ML models image"; exit 1; }
fi

if [ "${RUN_CM_MODELS,,}" = "true" ]; then
    echo "Pulling Conceptual models images..."
    need_gateway_flag=True
    docker pull mabesa/sapphire-conceptmod:$TAG || { echo "Failed to pull CM models image"; exit 1; }
fi

if [ -n "$need_gateway_flag" ]; then
    echo "Pulling gateway image..."
    docker pull mabesa/sapphire-prepgateway:$TAG || { echo "Failed to pull gateway image"; exit 1; }
fi

echo "=== Docker images successfully pulled ==="
echo "You can now run the forecast tools pipeline"

# Display Luigi scheduler information
echo ""
echo "Note: Make sure the Luigi scheduler is running before executing pipeline tasks"
echo "You can start it with: bash dev/run_luigi_dev.sh"
echo ""

# List pulled images
echo "=== SAPPHIRE Docker images ==="
docker images | grep sapphire