#!/bin/bash

# clean_docker.sh is used to clean up Docker space by stopping and removing all 
# containers and images, except those related to Nginx.

# Usage: ./clean_docker.sh [--execute]
# The --execute flag is optional. If provided, the script will actually perform 
# the cleanup operations.

# Default to dry-run mode
DRY_RUN=true

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --execute) DRY_RUN=false ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Function to execute or simulate a command
run_command() {
    if $DRY_RUN; then
        echo "[DRY RUN] Would execute: $@"
    else
        echo "Executing: $@"
        "$@"
    fi
}

# Function to get IDs of containers not related to Nginx
get_non_nginx_containers() {
    docker ps -a --format '{{.ID}} {{.Image}}' | grep -iv 'nginx|watchtower' | awk '{print $1}'
}

# Function to get IDs of images not related to Nginx
get_non_nginx_images() {
    docker images --format '{{.ID}} {{.Repository}}' | grep -iv 'nginx|watchtower' | awk '{print $1}'
}

# Stop all running containers except Nginx-related ones
running_containers=$(docker ps --format '{{.ID}} {{.Image}}' | grep -iv 'nginx|watchtower' | awk '{print $1}')

if [ -n "$running_containers" ]; then
    echo "Stopping all running containers except Nginx..."
    for container in $running_containers; do
        run_command docker stop $container
    done
else
    echo "No running containers to stop (excluding Nginx)."
fi

# Tear down the Docker Compose service for the backend pipeline
#run_command docker compose -f bin/docker-compose-luigi.yml down

# Remove all stopped containers except Nginx-related ones
stopped_containers=$(docker ps -a --filter "status=exited" --format '{{.ID}} {{.Image}}' | grep -iv 'nginx|watchtower' | awk '{print $1}')

if [ -n "$stopped_containers" ]; then
    echo "Removing stopped containers (excluding Nginx)..."
    for container in $stopped_containers; do
        run_command docker rm $container
    done
else
    echo "No stopped containers to remove (excluding Nginx)."
fi

# Remove all images except Nginx-related ones
image_ids=$(docker images --format '{{.ID}} {{.Repository}}' | grep -iv 'nginx|watchtower' | awk '{print $1}')

if [ -n "$image_ids" ]; then
    echo "Removing images (excluding Nginx)..."
    for image in $image_ids; do
        run_command docker rmi $image -f
    done
else
    echo "No Docker images to remove (excluding Nginx)."
fi

# Prune the build cache to free up disk space
run_command docker builder prune -f

# Cleaning the docker cache
run_command docker system prune -a -f --volumes

if $DRY_RUN; then
    echo "This was a dry run. To actually perform these operations, run the script with --execute"
else
    echo "Cleanup completed."
fi