#!/bin/bash
if test -z "$1"
then
      echo "Usage bash ./bin/pull_docker_images.sh TAG"
      echo "No tag was passed! Please pass a tag to the script e.g. latest, or deploy"
      exit 1
fi

TAG=$1
echo "Pulling with TAG=$TAG"
docker pull mabesa/sapphire-pythonbaseimage:$TAG
docker pull mabesa/sapphire-preprunoff:$TAG
docker pull mabesa/sapphire-prepgateway:$TAG
docker pull mabesa/sapphire-linreg:$TAG
docker pull mabesa/sapphire-postprocessing:$TAG
docker pull mabesa/sapphire-dashboard:$TAG

# Build pipeline locally
docker compose -f bin/docker-compose.yml build --no-cache