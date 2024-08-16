#!/bin/bash

# Stop all running containers
docker stop $(docker ps -a -q)

# Tear down the Docker Compose service for the backend pipeline
docker compose -f bin/docker-compose-luigi.yml down

# Remove all containers
docker container prune -f
docker rmi $(docker images -q) -f