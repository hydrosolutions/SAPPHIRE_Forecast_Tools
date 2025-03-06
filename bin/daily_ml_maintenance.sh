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
LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/ml_maintenance"
mkdir -p ${LOG_DIR}

# Print log directory path
echo "Log directory: ${LOG_DIR}"

# Set main log file path with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
log_file="${LOG_DIR}/run_${TIMESTAMP}.log"

# Function to log messages to both console and log file
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
COMPOSE_FILE="maintenance-compose.yml"
echo "services:" > $COMPOSE_FILE

# Generate service configurations for all combinations
for MODEL in "${MODEL_OPTIONS[@]}"; do
  for HORIZON in "${HORIZON_OPTIONS[@]}"; do
    # Create a unique service name based on the options
    SERVICE_NAME="ml-maintenance-${MODEL}-${HORIZON}"

    # Store service name in array
    SERVICE_NAMES+=("$SERVICE_NAME")

    log_message "Creating service: ${SERVICE_NAME}"

    # Add the service configuration to the compose file
    cat >> $COMPOSE_FILE << EOF
  ${SERVICE_NAME}:
    image: mabesa/sapphire-ml:latest
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
cat >> $COMPOSE_FILE << EOF
networks:
  ml-network:
    driver: bridge
EOF

log_message "Generated docker-compose file:"
cat $COMPOSE_FILE >> "$log_file"

# Verify Docker is running
if ! docker info > /dev/null 2>&1; then
  log_message "ERROR: Docker is not running. Please start Docker and try again."
  exit 1
fi

# Check if the Docker image exists
IMAGE_ID="mabesa/sapphire-ml:latest"
if ! docker image inspect $IMAGE_ID > /dev/null 2>&1; then
  docker pull $IMAGE_ID
  #log_message "ERROR: Docker image $IMAGE_ID not found. Please verify the image exists."
  #docker images >> "$log_file"
  #exit 1
fi

log_message "Verifying Docker Compose..."
if ! command -v docker compose &> /dev/null; then
  log_message "ERROR: Docker Compose not found. Please make sure it's installed."
  exit 1
fi

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

    # Start just these services and capture any error output
    docker_output=$(docker compose -f $COMPOSE_FILE up -d "${batch_services[@]}" 2>&1)
    docker_exit_code=$?

    # Log the Docker output regardless of success or failure
    log_message "Docker compose output:"
    echo "$docker_output" | tee -a "$log_file"

    # Check if the docker compose command succeeded
    if [ $docker_exit_code -ne 0 ]; then
        log_message "ERROR: Failed to start services. Exit code: $docker_exit_code"
        log_message "Attempting to troubleshoot..."

        # Check Docker system info
        log_message "Docker system info:"
        docker system df | tee -a "$log_file"

        # Check for any running containers
        log_message "Currently running containers:"
        docker ps | tee -a "$log_file"

        # Check if any of our services actually started
        for service in "${batch_services[@]}"; do
            container_id=$(docker ps -a --filter "name=$service" --format "{{.ID}}")
            if [ -n "$container_id" ]; then
                log_message "Container for $service exists with ID $container_id, checking logs:"
                docker logs $container_id 2>&1 | tee -a "${LOG_DIR}/${service}_${TIMESTAMP}.log"
            else
                log_message "No container found for $service"
            fi
        done

        log_message "Aborting further execution due to startup errors"
        exit 1
    fi

    log_message "Batch started successfully"

    # Wait for this batch to complete
    log_message "Waiting for batch to complete..."

    # Wait until all containers in this batch are done
    TIMEOUT_COUNTER=0
    MAX_TIMEOUT=60  # 30 minutes (60 * 30 seconds)
    while true; do
        all_done=true
        for service in "${batch_services[@]}"; do
            # Get container ID if it exists
            container_id=$(docker ps --filter "name=$service" --quiet)

            if [ -n "$container_id" ]; then
                all_done=false

                # Periodically save logs even while running
                if [ $((TIMEOUT_COUNTER % 10)) -eq 0 ]; then  # Every 5 minutes (10 * 30 seconds)
                    log_message "Capturing interim logs for $service"
                    docker logs $container_id 2>&1 > "${LOG_DIR}/${service}_${TIMESTAMP}.log"
                fi

                break
            fi
        done

        if [ "$all_done" = true ]; then
            log_message "All containers in batch have completed"
            break
        fi

        TIMEOUT_COUNTER=$((TIMEOUT_COUNTER + 1))
        if [ $TIMEOUT_COUNTER -ge $MAX_TIMEOUT ]; then
            log_message "WARNING: Timeout reached (30 minutes). Forcing containers to stop."

            # Capture logs before stopping containers
            for service in "${batch_services[@]}"; do
                container_id=$(docker ps -a --filter "name=$service" --format "{{.ID}}")
                if [ -n "$container_id" ]; then
                    log_message "Capturing final logs for timed-out container $service"
                    docker logs $container_id 2>&1 > "${LOG_DIR}/${service}_${TIMESTAMP}.log"
                fi
            done

            docker compose -f $COMPOSE_FILE stop "${batch_services[@]}"
            break
        fi

        # Only log every 5 iterations to avoid filling the log file
        if [ $((TIMEOUT_COUNTER % 5)) -eq 0 ]; then
            log_message "Containers still running, waiting... ($TIMEOUT_COUNTER/$MAX_TIMEOUT)"
        fi

        sleep 30
    done

    log_message "Batch $((i/$MAX_PARALLEL_JOBS + 1)) completed"

    # Capture logs for this batch into individual files
    for service in "${batch_services[@]}"; do
        service_log_file="${LOG_DIR}/${service}_${TIMESTAMP}.log"
        log_message "Saving logs for $service to $service_log_file"

        # Get the container ID (even if it has exited)
        container_id=$(docker ps -a --filter "name=$service" --format "{{.ID}}")

        if [ -n "$container_id" ]; then
            # Save full logs
            docker logs $container_id 2>&1 > "$service_log_file"

            # Add a summary of container info
            {
                echo "=============== CONTAINER INFO ==============="
                docker inspect $container_id | grep -E "State|Error|ExitCode|Status"
                echo "=============================================="
            } >> "$service_log_file"

            # Add container exit code to main log
            exit_code=$(docker inspect $container_id --format='{{.State.ExitCode}}')
            log_message "$service completed with exit code: $exit_code"

            # Summary in main log
            log_message "Log file for $service: $service_log_file"
        else
            log_message "WARNING: No container found for $service, no logs to save"
        fi
    done
done

log_message "All maintenance jobs completed"

# Clean up
log_message "Cleaning up docker resources"
docker compose -f $COMPOSE_FILE down
rm $COMPOSE_FILE

# Remove old log files
log_message "Removing old log files"
# Find all files in the log directory older than 15 days and delete them
find $LOG_DIR -type f -mtime +15 -delete

log_message "ML maintenance run completed successfully"