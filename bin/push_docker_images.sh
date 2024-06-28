#!/usr/bin/env bash
# Run from the root of the project
if test -z "$1"
then
      echo "Usage bash ./bin/push_docker_images.sh TAG"
      echo "No tag was passed! Please pass a tag to the script e.g. latest, or deploy"
      exit 1
fi

ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    echo "Running on an Apple Silicon Mac (M1/M2/M3)."
    echo "Currently no pushing to Docker Hub allowed."
    exit 1
fi

TAG=$1
echo "Pushing with TAG=$TAG"
docker push mabesa/sapphire-pythonbaseimage:$TAG
docker push mabesa/sapphire-pipeline:$TAG
docker push mabesa/sapphire-preprunoff:$TAG
docker push mabesa/sapphire-linreg:$TAG