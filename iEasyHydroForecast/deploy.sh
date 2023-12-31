#!/bin/sh

# Define variables for Docker image names and tags
IMAGE_NAME="mabesa/sapphire"
IMAGE_TAG=$(git rev-parse --short HEAD) # first 7 characters of the current commit hash
TOOL_NAME="iEasyHydroForecast"
IMAGE_TAG="${TOOL_NAME}-${IMAGE_TAG}"

# Build Docker image and tag 
echo "Building Docker image ${IMAGE_NAME}:${IMAGE_TAG}, and tagging as latest"
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" ./iEasyHydroForecast 
#docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${IMAGE_NAME}:latest"

# Push Docker image to Docker Hub
echo "Authenticating and pushing image to Docker Hub"
echo "${DOCKER_PASSWORD}" | docker login -u "${DOCKER_USERNAME}" --password-stdin
docker push "${IMAGE_NAME}:${IMAGE_TAG}"
#docker push "${IMAGE_NAME}:latest"