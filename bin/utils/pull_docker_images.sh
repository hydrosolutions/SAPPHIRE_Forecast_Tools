#!/bin/bash
if test -z "$1"
then
      echo "Usage bash ./bin/pull_docker_images.sh TAG"
      echo "No tag was passed! Please pass a tag to the script e.g. latest, or deploy"
      exit 1
fi

TAG=$1
echo "Pulling with TAG=$TAG"
# Pull images used in the demo version
echo "Pulling images"
docker pull mabesa/sapphire-pythonbaseimage:$TAG
docker pull mabesa/sapphire-preprunoff:$TAG
docker pull mabesa/sapphire-linreg:$TAG
docker pull mabesa/sapphire-postprocessing:$TAG
docker pull mabesa/sapphire-rerun:$TAG

# Pull images used in the KGHM version
if [ "$ieasyhydroforecast_organization" = "kghm" ]; then
      docker pull mabesa/sapphire-prepgateway:$TAG
      docker pull mabesa/sapphire-ml:$TAG
      docker pull mabesa/sapphire-conceptmod:$TAG
elif [ "$ieasyhydroforecast_organization" = "tjhm" ]; then
      docker pull mabesa/sapphire-prepgateway:$TAG
      docker pull mabesa/sapphire-ml:$TAG
else
      echo "No further images to pull for this organization"
fi

# Build forecast pipeline locally
#docker compose -f bin/docker-compose-luigi.yml build --no-cache
