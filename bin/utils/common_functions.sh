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
    echo "| Starting Docker Compose service for backend ..."
    docker compose -f bin/docker-compose-luigi.yml up -d &
    DOCKER_COMPOSE_LUIGI_PID=$!
    echo "| Docker Compose service started with PID $DOCKER_COMPOSE_LUIGI_PID"
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


