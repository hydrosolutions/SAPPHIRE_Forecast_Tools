#!/bin/bash
# Run from the root of the project
if test -z "$1"
then
      echo "Usage bash ./bin/build_docker_images.sh TAG"
      echo "No tag was passed! Please pass a tag to the script e.g. latest, or deploy"
      exit 1
fi

TAG=$1
echo "Building with TAG=$TAG"
docker build --no-cache -t mabesa/sapphire-pythonbaseimage:$TAG -f ./apps/docker_base_image/Dockerfile .
docker compose -f bin/docker-compose.yml build --no-cache
docker build --no-cache -t mabesa/sapphire-preprunoff:$TAG -f ./apps/preprocessing_runoff/Dockerfile .
docker build --no-cache -t mabesa/sapphire-linreg:$TAG -f ./apps/linear_regression/Dockerfile .
# There is no rocker shiny base image for ARM architecture
docker pull mabesa/sapphire-configuration:$TAG