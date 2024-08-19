#!/bin/bash

# clean_docker.sh is called by run_sapphire_forecast_tools.sh. It is used to
# clean up Docker space by stopping and removing all containers and images.

# Test if there are any running containers
if [ "$(docker ps -q)" ]; then
    echo "Stopping all running containers..."
    # Stop all running containers
    docker stop $(docker ps -a -q)
else
    echo "No running containers found"
fi

# Tear down the Docker Compose service for the backend pipeline
docker compose -f bin/docker-compose-luigi.yml down

# Remove all unused containers
docker container prune -f

# Get the list of image IDs
image_ids=$(docker images -q)

# Check if there are any images to remove
if [ -n "$image_ids" ]; then
    docker rmi $image_ids -f
else
    echo "No Docker images to remove."
fi

# Prune the build cache to free up disk space
docker builder prune -f