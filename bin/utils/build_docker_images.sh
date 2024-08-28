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
# Building
docker build --no-cache -t mabesa/sapphire-pythonbaseimage:$TAG -f ./apps/docker_base_image/Dockerfile .
docker compose -f bin/docker-compose-luigi.yml build --no-cache
docker build --no-cache -t mabesa/sapphire-preprunoff:$TAG -f ./apps/preprocessing_runoff/Dockerfile .
docker build --no-cache -t mabesa/sapphire-linreg:$TAG -f ./apps/linear_regression/Dockerfile .
docker build --no-cache -t mabesa/sapphire-postprocessing:$TAG -f ./apps/postprocessing/Dockerfile .
docker build --no-cache -t mabesa/sapphire-rerun:$TAG -f ./apps/rerun/Dockerfile .
# NOTE: There is no rocker shiny base image for ARM architecture (M-generation chips of more recent macs)
docker pull mabesa/sapphire-configuration:$TAG
docker pull mabesa/sapphire-conceptmod:$TAG

if ["$ieasyhydroforecast_organization" = "kghm"]; then
      docker build --no-cache -t mabesa/sapphire-prepgateway:$TAG -f ./apps/preprocessing_gateway/Dockerfile .
fi