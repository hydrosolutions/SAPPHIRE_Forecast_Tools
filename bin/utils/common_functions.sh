# common_functions.sh
# Functions herin are intended to be used in scripts in the bin directory.

print_banner() {
    echo "|   ____    _    ____  ____  _   _ ___ ____  _____                "
    echo "|  / ___|  / \  |  _ \|  _ \| | | |_ _|  _ \| ____|               "
    echo "|  \___ \ / _ \ | |_) | |_) | |_| || || |_) |  _|                 "
    echo "|   ___) / ___ \|  __/|  __/|  _  || ||  _ <| |___                "
    echo "|  |____/_/   \_\_|   |_|   |_| |_|___|_| \_\_____|        _      "
    echo "|  |  ___|__  _ __ ___  ___ __ _ ___| |_  |_   _|__   ___ | |___  "
    echo "|  | |_ / _ \| '__/ _ \/ __/ _\` / __| __|   | |/ _ \ / _ \| / __| "
    echo "|  |  _| (_) | | |  __/ (_| (_| \__ \ |_    | | (_) | (_) | \__ \ "
    echo "|  |_|  \___/|_|  \___|\___\__,_|___/\__|   |_|\___/ \___/|_|___/ "
    echo "|                                                                 "
    echo "| Deploying the SAPPHIRE forecast tools ..."
    echo "| Date: $(date '+%Y-%m-%d %H:%M:%S %Z')"
}

# This function takes a path and returns the last three elements of the path
keep_last_three_elements() {
    local path=$1
    local result=""

    for i in {1..3}; do
        result=$(basename "$path")/$result
        path=$(dirname "$path")
    done

    # Remove the trailing slash
    result=${result%/}
    echo "$result"
}

read_configuration(){
    echo "|       "
    echo "| ------"
    echo "| Reading configuration"
    echo "| ------"
    # If the argument is provided, write it to the environment variable
    # ieasyhydroforecast_env_file_path. If not, check if the environment variable
    # is set. If not, throw an error.
    if [ -n "$1" ];
    then
        env_file_path=$1
        # Derive the path to the .env file inside the container
        container_env_file_path=/$(keep_last_three_elements "$env_file_path")
        # Derive the path to the data reference directory within the container
        container_data_ref_dir=$(dirname "$container_env_file_path")
        container_data_ref_dir=$(dirname "$container_data_ref_dir")
        export ieasyhydroforecast_container_data_ref_dir=$container_data_ref_dir
        # Test if there is a ieasyhydroforecast_env_file_path variable set
        if [ -z "$ieasyhydroforecast_env_file_path" ];
        then
            # Test if the new env_file_path is different from the old one
            if [ "$ieasyhydroforecast_env_file_path" != "$env_file_path" ];
            then
                echo "| WARNING: Updating ieasyhydroforecast_env_file_path"
                echo "|    from $ieasyhydroforecast_env_file_path"
                echo "|    to $container_env_file_path"
            fi
        fi
        # For use by the forecast tools (inside docker containers) we need to know
        # the env file path inside the docker containers as well.
        export ieasyhydroforecast_env_file_path=$container_env_file_path
        echo "| Local path to .env read from argument: $env_file_path"
        echo "| Container path to .env derived: $ieasyhydroforecast_env_file_path"
        # Read the .env file
        if [ -f "$env_file_path" ]; then
            source "$env_file_path"
        else
            echo "| .env file not found at $env_file_path!"
            exit 1
        fi

    else
        echo "| Error: No path to .env file was passed or was found in the environment!"
        exit 1
    fi

    # Derive ieasyhydroforecast_data_root_dir by removing the filename and 2 folder hierarchies
    ieasyhydroforecast_data_root_dir=$(dirname "$env_file_path")
    ieasyhydroforecast_data_ref_dir=$(dirname "$ieasyhydroforecast_data_root_dir")
    ieasyhydroforecast_data_root_dir=$(dirname "$ieasyhydroforecast_data_ref_dir")

    echo "| Local path to data reference directory: $ieasyhydroforecast_data_ref_dir"
    echo "| Container path to data reference directory: $container_data_ref_dir"
    echo "| ieasyhydroforecast_data_root_dir: $ieasyhydroforecast_data_root_dir"
    export ieasyhydroforecast_data_ref_dir
    export ieasyhydroforecast_data_root_dir

    # Load environment variables from the specified .env file
    if [ -f "$env_file_path" ]; then
        source "$env_file_path"
    else
        echo "| .env file not found at $env_file_path!"
        exit 1
    fi

    # If the env. varialbe ieasyhydroforecast_organization is not set, assume "demo"
    if [ -z "$ieasyhydroforecast_organization" ]; then
        echo "| WARNING: ieasyhydroforecast_organization is not set. Assuming 'demo'"
        ieasyhydroforecast_organization="demo"
    fi
    echo "| Deploying the SAPPHIRE forecast tools for organization:"
    echo "|    $ieasyhydroforecast_organization"

}

# Function to remove all Docker containers and images
clean_out_docker_space() {
    echo "|      "
    echo "| ------"
    echo "| Removing all containers and images"
    echo "| ------"
    # Take down the frontend if it is running
    docker compose -f bin/docker-compose-dashboards.yml down
    ieasyhydroforecast_data_root_dir=$ieasyhydroforecast_data_root_dir source ./bin/utils/clean_docker.sh --execute
}

# Function to stop and remove a container if it exists
stop_and_remove_container() {
    container_name=$1
    if [ "$(docker ps -q -f name=$container_name)" ]; then
        docker stop $container_name
    fi
    if [ "$(docker ps -a -q -f name=$container_name)" ]; then
        docker rm $container_name
    fi
}

# Function to clean out the backend for res-running of the forecasts
clean_out_backend() {
    echo "|      "
    echo "| ------"
    echo "| Removing backend containers"
    echo "| ------"
    echo "| Removing all superfluous containers from the backend..."
    docker compose -f bin/docker-compose-luigi.yml down

    # List all containers that may be called in the pipeline
    stop_and_remove_container preprunoff
    stop_and_remove_container prepgateway
    stop_and_remove_container linreg
    stop_and_remove_container postprocessing
    stop_and_remove_container ml_TIDE_PENTAD
    stop_and_remove_container ml_TIDE_DECAD
    stop_and_remove_container ml_TFT_PENTAD
    stop_and_remove_container ml_TFT_DECAD
    stop_and_remove_container ml_TSMIXER_PENTAD
    stop_and_remove_container ml_TSMIXER_DECAD
    stop_and_remove_container ml_ARIMA_PENTAD
    stop_and_remove_container ml_ARIMA_DECAD
    stop_and_remove_container conceptmod
}

# Function to pull Docker images for the forecast tools
pull_docker_images() {
    echo "|      "
    echo "| ------"
    echo "| Pulling images"
    echo "| ------"

    # Pull (deployment mode)
    echo "| Pulling with TAG=$ieasyhydroforecast_backend_docker_image_tag"
    source ./bin/utils/pull_docker_images.sh $ieasyhydroforecast_backend_docker_image_tag
}

# Function to establish an SSH tunnel to the iEasyHydro (HF) server
establish_ssh_tunnel() {
    echo "| ieasyhydroforecast_ssh_to_iEH: $ieasyhydroforecast_ssh_to_iEH"

    # Check if SSH tunnel is required
    if [ "${ieasyhydroforecast_ssh_to_iEH,,}" != "true" ]; then
        echo "| SSH tunnel not required (ieasyhydroforecast_ssh_to_iEH is not set to true)"
        return 0
    fi

    echo "|      "
    echo "| ------"
    echo "| Establishing SSH tunnel to iEasyHydro server"
    echo "| ------"

    echo "| Establishing SSH tunnel to SAPPHIRE server..."
    source $ieasyhydroforecast_data_ref_dir/bin/.ssh/open_ssh_tunnel.sh
    wait  # Wait for the tunnel to be established

}

# Function to start the Docker container to re-set the run date
start_docker_container_reset_run_date() {
  echo "| Starting Docker container to re-set the run date ..."
  docker run -d \
    -e SAPPHIRE_OPDEV_ENV=True \
    --name resetrundate \
    --network host \
    -v $ieasyhydroforecast_data_ref_dir/config:/sensitive_data_forecast_tools/config \
    -v $ieasyhydroforecast_data_ref_dir/intermediate_data:/sensitive_data_forecast_tools/intermediate_data \
    mabesa/sapphire-rerun:latest
  echo "| Docker container started with name resetrundate"
}


# Function to start the Docker Compose service for the backend pipeline
start_docker_compose_luigi() {
    echo "|      "
    echo "| ------"
    echo "| Starting backend services"
    echo "| ------"
    # Auto-detect Docker group ID
    if [ "$(uname)" = "Linux" ]; then
        DOCKER_GID=$(getent group docker | cut -d: -f3 2>/dev/null)
        if [ -z "$DOCKER_GID" ] && [ -S /var/run/docker.sock ]; then
            # Fallback to socket group if docker group not found
            DOCKER_GID=$(stat -c "%g" /var/run/docker.sock 2>/dev/null)
        fi
        echo "| Detected Docker group ID: $DOCKER_GID"
    else
        # Default for macOS/non-Linux
        DOCKER_GID=999
        echo "| Non-Linux OS detected, using default Docker group ID: $DOCKER_GID"
    fi
    # Export for docker-compose
    export DOCKER_GID

    echo "| Starting Docker Compose service for backend ..."
    docker compose -f bin/docker-compose-luigi.yml up -d &
    DOCKER_COMPOSE_LUIGI_PID=$!
    echo "| Docker Compose service started with PID $DOCKER_COMPOSE_LUIGI_PID"

    # Verify Docker socket access
    echo "Verifying Docker socket access..."
    sleep 5  # Give container time to start
    if docker exec sapphire-backend-pipeline sh -c "docker ps" &>/dev/null; then
        echo "✅ Pipeline container has Docker socket access"
    else
        echo "❌ Docker socket access failed. Trying fallback..."
        sudo chmod 666 /var/run/docker.sock
        sleep 2
        if docker exec sapphire-backend-pipeline sh -c "docker ps" &>/dev/null; then
            echo "✅ Access fixed with socket permission change"
        else
            echo "❌ Still cannot access Docker socket"
        fi
    fi
}

# Function to start the Docker Compose service for the dashboards
start_docker_compose_dashboards() {
    echo "|      "
    echo "| ------"
    echo "| Starting frontend services"
    echo "| ------"
    echo "| Starting Docker Compose service for the dashboards..."
    echo "| Deploying dashboard to: ieasyhydroforecast_url: $ieasyhydroforecast_url"
    ieasyhydroforecast_url=$ieasyhydroforecast_url docker compose -f bin/docker-compose-dashboards.yml up -d &
    DOCKER_COMPOSE_DASHBOARD_PID=$!
    echo "| Docker Compose service started with PID $DOCKER_COMPOSE_DASHBOARD_PID"
}

# Clean up processes on script exit (used with trap)
cleanup() {
  echo "|      "
  echo "| ------"
  echo "| Cleaning up"
  echo "| ------"
  if [ -n "$ieasyhydroforecast_ssh_tunnel_pid" ]; then
    kill $ieasyhydroforecast_ssh_tunnel_pid
  fi
}

# Clean up processes on script exit (used with trap)
cleanup_deployment() {
  echo "|      "
  echo "| ------"
  echo "| Cleaning up"
  echo "| ------"
  if [ -n "$ieasyhydroforecast_ssh_tunnel_pid" ]; then
    kill $ieasyhydroforecast_ssh_tunnel_pid
  fi
  echo "|       "
  echo "| ------"
  echo "|       "
  echo "| You have now run the SAPPHIRE forecast tools for the first time!"
  echo "|       "
  echo "| Next steps (follow the docs for more detailed instructions):"
  echo "| 1. Check the logs of the Docker Compose service for any errors."
  echo "| 2. Check if the dashboards are running and displaying as expected."
  echo "| 3. Set up cron jobs for the dashboard services and for the daily run of the forecasting pipeline."
  echo "| "
}

# Setup Cosign for image verification
# Usage: setup_cosign [/path/to/key.pub]
# Returns: Sets VERIFY_SIGNATURES=true/false global variable
setup_cosign() {
    # Default public key location or use passed parameter
    local key_path="${1:-$PROJECT_ROOT/keys/cosign.pub}"
    COSIGN_PUBLIC_KEY="$key_path"
    VERIFY_SIGNATURES=true

    echo "Setting up Cosign for image verification..."

    # Check for Cosign installation AND verify it's working
    if ! command -v cosign &> /dev/null || ! cosign version &> /dev/null; then
        echo "Cosign not found or not working properly. Installing..."

        # Remove any existing broken cosign binary
        if [ -f /usr/local/bin/cosign ]; then
            echo "Removing existing cosign binary..."
            sudo rm -f /usr/local/bin/cosign || rm -f /usr/local/bin/cosign
        fi

        # Determine OS and architecture for download
        local os_lower=$(uname -s | tr '[:upper:]' '[:lower:]')
        local arch=$(uname -m)

        # Handle arm64/aarch64 architecture naming differences
        if [ "$arch" = "aarch64" ]; then
            arch="arm64"
        fi

        # Download appropriate binary
        curl -L "https://github.com/sigstore/cosign/releases/latest/download/cosign-$os_lower-$arch" -o /tmp/cosign

        if [ $? -ne 0 ]; then
            echo "Failed to download cosign. Continuing without verification."
            VERIFY_SIGNATURES=false
            return 1
        fi

        chmod +x /tmp/cosign

        # Move to path with sudo (if available) or directly
        if command -v sudo &> /dev/null; then
            sudo mv /tmp/cosign /usr/local/bin/ || {
                echo "Failed to install cosign. Continuing without verification.";
                VERIFY_SIGNATURES=false;
                return 1;
            }
        else
            mv /tmp/cosign /usr/local/bin/ || {
                echo "Failed to install cosign. Continuing without verification.";
                VERIFY_SIGNATURES=false;
                return 1;
            }
        fi

        echo "Cosign installed successfully."
    else
        echo "Cosign is already installed."
    fi

    # Check for public key
    if [ ! -f "$COSIGN_PUBLIC_KEY" ]; then
        echo "Warning: Cosign public key not found at $COSIGN_PUBLIC_KEY"
        echo "Signature verification will be skipped."
        VERIFY_SIGNATURES=false
        return 1
    else
        echo "Using Cosign public key: $COSIGN_PUBLIC_KEY"
    fi

    return 0
}

# Function to pull and verify an image
pull_and_verify() {
    local image="$REPO/$1:$TAG"
    echo "Pulling image: $image"
    docker pull "$image"

    if $VERIFY_SIGNATURES; then
        echo "Verifying signature for $image..."
        if cosign verify --key "$COSIGN_PUBLIC_KEY" "$image" --insecure-ignore-tlog; then
            echo "✅ Signature verified for $image"
        else
            echo "❌ Failed to verify signature for $image"
            echo "Would you like to continue anyway? (y/n)"
            read -r answer
            if [[ "$answer" != "y" ]]; then
                echo "Aborting due to signature verification failure"
                return 1
            fi
        fi
    fi
    return 0
}

# Function to pull and verify an image
pull_and_verify_image() {
    local image_name="$1"
    local tag="${2:-latest}"
    local full_image="$REPO/$image_name:$tag"

    echo "Pulling image: $full_image"
    docker pull "$full_image"
    pull_status=$?

    if [ $pull_status -ne 0 ]; then
        echo "❌ Failed to pull image: $full_image"
        return 1
    fi

    if $VERIFY_SIGNATURES; then
        echo "Verifying signature for $full_image..."
        if cosign verify --key "$COSIGN_PUBLIC_KEY" "$full_image" --insecure-ignore-tlog; then
            echo "✅ Signature verified for $full_image"
        else
            echo "❌ Failed to verify signature for $full_image"

            # Only prompt if we're in an interactive shell
            if [ -t 0 ]; then
                echo "Would you like to continue anyway? (y/n)"
                read -r answer
                if [[ "$answer" != "y" ]]; then
                    echo "Aborting due to signature verification failure"
                    return 1
                fi
            else
                echo "Non-interactive mode detected, continuing despite verification failure"
            fi
        fi
    fi
    return 0
}

# Function to check if images are prepared
check_backend_images_prepared() {
    local tag="${1:-$ieasyhydroforecast_backend_docker_image_tag}"
    local marker_file="/tmp/sapphire_backend_images_prepared_${tag}"

    if [ -f "${marker_file}" ]; then
        local prep_time=$(cat "${marker_file}")
        echo "| Images were prepared at: ${prep_time}"
        return 0  # Success - images are prepared
    else
        echo "| No prepared images found"
        return 1  # Failure - images are not prepared
    fi
}

check_frontend_images_prepared() {
    local tag="${1:-$ieasyhydroforecast_frontend_docker_image_tag}"
    local marker_file="/tmp/sapphire_backend_images_prepared_${tag}"

    if [ -f "${marker_file}" ]; then
        local prep_time=$(cat "${marker_file}")
        echo "| Images were prepared at: ${prep_time}"
        return 0  # Success - images are prepared
    else
        echo "| No prepared images found"
        return 1  # Failure - images are not prepared
    fi
}

