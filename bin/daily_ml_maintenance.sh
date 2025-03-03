#!/bin/bash

# Source the common functions
source "$(dirname "$0")/utils/common_functions.sh"

# Print the banner
print_banner

# Read the configuration from the .env file
read_configuration $1

# Validate required environment variables
if [ -z "$ieasyhydroforecast_data_root_dir" ] || [ -z "$ieasyhydroforecast_env_file_path" ] || [ -z "$ieasyhydroforecast_data_ref_dir" ] || [ -z "$ieasyhydroforecast_container_data_ref_dir" ]; then
  echo "Error: Required environment variables are not set. Please check your .env file."
  exit 1
fi

# Create log directory if it doesn't exist
mkdir -p ${ieasyhydroforecast_data_root_dir}/logs/ml_maintenance

# Print log directory path
echo "Log directory: ${ieasyhydroforecast_data_root_dir}/logs/ml_maintenance"

# Set log file path with timestamp
log_file="${ieasyhydroforecast_data_root_dir}/logs/ml_maintenance/run_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log_message() {
  echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$log_file"
}

# Define your variable option sets
MODEL_OPTIONS=("TFT" "TIDE" "TSMIXER" "ARIMA")
HORIZON_OPTIONS=("PENTAD" "DECAD")

log_message "Starting ML maintenance run on Mac M2 Max with 64GB RAM"
log_message "Model options: ${MODEL_OPTIONS[*]}"
log_message "Horizon options: ${HORIZON_OPTIONS[*]}"

# Determine optimal number of parallel jobs based on system memory and models
# On your 64GB system, we can comfortably run 4 containers in parallel
# For each model+horizon combination, allocate ~12GB of memory
MAX_PARALLEL_JOBS=3
MEMORY_PER_CONTAINER="12g"
MEMORY_SWAP_PER_CONTAINER="16g"  # Extra 4GB swap

log_message "Running with max $MAX_PARALLEL_JOBS parallel jobs, $MEMORY_PER_CONTAINER memory per container"

# Create an array to store service names directly
declare -a SERVICE_NAMES

# Create a temporary docker-compose file with memory limits
echo "services:" > maintenance-compose.yml

# Generate service configurations for all combinations
for MODEL in "${MODEL_OPTIONS[@]}"; do
  for HORIZON in "${HORIZON_OPTIONS[@]}"; do
    # Create a unique service name based on the options
    SERVICE_NAME="ml-maintenance-${MODEL}-${HORIZON}"

    # Store service name in array
    SERVICE_NAMES+=("$SERVICE_NAME")

    log_message "Creating service: ${SERVICE_NAME}"

    # Add the service configuration to the compose file
    cat >> maintenance-compose.yml << EOF
  ${SERVICE_NAME}:
    image: sha256:acf31b001d2160312714637b9fda188fdedf2deed5beb22f7afd53f8f71441a1
    environment:
      - PYTHONPATH=/app
      - ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir}
      - ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path}
      - SAPPHIRE_OPDEV_ENV=True
      - IN_DOCKER=True
      - SAPPHIRE_MODEL_TO_USE=${MODEL}
      - SAPPHIRE_PREDICTION_MODE=${HORIZON}
      - RUN_MODE=maintenance
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock # Mount docker socket
      - ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config
      - ${ieasyhydroforecast_data_ref_dir}/daily_runoff:${ieasyhydroforecast_container_data_ref_dir}/daily_runoff
      - ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data
      - ${ieasyhydroforecast_data_ref_dir}/bin:${ieasyhydroforecast_container_data_ref_dir}/bin
    deploy:
      resources:
        limits:
          memory: ${MEMORY_PER_CONTAINER}
          cpus: '2.0'  # 2 cores per container
    mem_swappiness: 60
    mem_limit: ${MEMORY_PER_CONTAINER}
    memswap_limit: ${MEMORY_SWAP_PER_CONTAINER}
    networks:
      - ml-network

EOF
  done
done

# Add the network configuration
cat >> maintenance-compose.yml << EOF
networks:
  ml-network:
    driver: bridge
EOF

log_message "Generated docker-compose file:"
cat maintenance-compose.yml >> "$log_file"

# Instead of running all at once, we'll use a batch approach
log_message "Starting maintenance jobs in batches of $MAX_PARALLEL_JOBS"

# Use the directly stored service names instead of parsing
TOTAL_SERVICES=${#SERVICE_NAMES[@]}
log_message "Total services to run: $TOTAL_SERVICES"

# Process services in batches
for ((i=0; i<TOTAL_SERVICES; i+=$MAX_PARALLEL_JOBS)); do
    # Calculate the end of this batch
    end=$((i + MAX_PARALLEL_JOBS))
    if [ $end -gt $TOTAL_SERVICES ]; then
        end=$TOTAL_SERVICES
    fi

    # Get the current batch of services
    batch_services=("${SERVICE_NAMES[@]:i:end-i}")

    log_message "Starting batch $((i/$MAX_PARALLEL_JOBS + 1)): ${batch_services[*]}"

    # Start just these services
    docker compose -f maintenance-compose.yml up -d "${batch_services[@]}"

    # Check if the docker compose command succeeded
    if [ $? -ne 0 ]; then
        log_message "ERROR: Failed to start services. See error above."
        log_message "Attempting to troubleshoot..."
        log_message "Docker compose file contents:"
        cat maintenance-compose.yml
        log_message "Docker images available:"
        docker images
        exit 1
    fi

    # Wait for this batch to complete
    log_message "Waiting for batch to complete..."

    # Wait until all containers in this batch are done
    TIMEOUT_COUNTER=0
    MAX_TIMEOUT=60  # 30 minutes
    while true; do
        all_done=true
        for service in "${batch_services[@]}"; do
            if docker ps --filter "name=$service" --quiet | grep -q .; then
                all_done=false
                break
            fi
        done

        if [ "$all_done" = true ]; then
            break
        fi

        TIMEOUT_COUNTER=$((TIMEOUT_COUNTER + 1))
        if [ $TIMEOUT_COUNTER -ge $MAX_TIMEOUT ]; then
            log_message "WARNING: Timeout reached (30 minutes). Forcing containers to stop."
            docker compose -f maintenance-compose.yml stop "${batch_services[@]}"
            break
        fi

        log_message "Containers still running, waiting... ($TIMEOUT_COUNTER/$MAX_TIMEOUT)"
        sleep 30
    done

    log_message "Batch $((i/$MAX_PARALLEL_JOBS + 1)) completed"

    # Capture logs for this batch
    for service in "${batch_services[@]}"; do
        log_message "Logs for $service:"
        docker compose -f maintenance-compose.yml logs $service >> "${log_file}" 2>&1
    done
done

log_message "All maintenance jobs completed"

# Clean up
log_message "Cleaning up docker resources"
docker compose -f maintenance-compose.yml down
rm maintenance-compose.yml

log_message "ML maintenance run completed successfully"