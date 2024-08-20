#!/bin/bash
# Run from the root of the project
# This script is intended for development use only. It is not used in the
# deployment process.
# This script pushes the Docker images to a Docker repository on Docker Hub
# Note that you will need to have push permission to the repository.

usage() {
    echo "Usage: $0 -t TAG -r REPO"
    exit 1
}

# Parse command line options
while getopts ":t:r:" opt; do
  case ${opt} in
    t )
      TAG=$OPTARG
      ;;
    r )
      REPO=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

# Test if we have both arguments
if [ -z "$TAG" ] || [ -z "$REPO" ]; then
    usage
fi

# Test that we are not pushing from an Apple Silicon Mac (M1/M2/M3).
# If you deploy the Forecast Tools on an M1/M2/M3 Mac, you should comment out
# the following if statement.
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    echo "Running on an Apple Silicon Mac (M1/M2/M3)."
    echo "Currently no pushing to Docker Hub allowed."
    exit 1
fi

TAG=$1
echo "Pushing with TAG=$TAG"
docker push $REPO/sapphire-pythonbaseimage:$TAG
docker push $REPO/sapphire-pipeline:$TAG
docker push $REPO/sapphire-preprunoff:$TAG
docker push $REPO/sapphire-linreg:$TAG

# Pull images used in the KGHM version
if ["$ieasyhydroforecast_organization" = "kghm"]; then
      docker push $REPO/sapphire-prepgateway:$TAG
fi