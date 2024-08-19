#!/bin/bash

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

# Remove all containers
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