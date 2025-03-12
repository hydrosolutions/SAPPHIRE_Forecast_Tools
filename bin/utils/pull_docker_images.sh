#!/bin/bash

# This script will check if the Cosign binary is installed and if the public key
# is available. If not, it will install the Cosign binary. The user can opt to
# skip signature verification if the public key is not available. The script will
# then pull the Docker images for the specified tag and verify their signatures.
source "bin/utils/common_functions.sh"

if test -z "$1"
then
      echo "Usage bash ./bin/pull_docker_images.sh TAG"
      echo "No tag was passed! Please pass a tag to the script e.g. latest, or deploy"
      exit 1
fi

TAG=$1
REPO="mabesa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../" && pwd)"

# By default, look for the public key in the Git repository
COSIGN_PUBLIC_KEY="${COSIGN_PUBLIC_KEY:-$PROJECT_ROOT/keys/cosign.pub}"

echo "Pulling with TAG=$TAG"

# Check for Cosign installation
setup_cosign "$COSIGN_PUBLIC_KEY" || exit 1

# Define core images
DEMO_IMAGES="sapphire-pythonbaseimage sapphire-preprunoff sapphire-linreg sapphire-postprocessing sapphire-rerun"

# Define KGHM-specific images
KGHM_IMAGES="sapphire-prepgateway sapphire-ml sapphire-conceptmod"

# Pull images used in the demo version
echo "Pulling core images"
for image in $CORE_IMAGES; do
    pull_and_verify "$REPO/$image:$TAG" true || exit 1
done

# Pull images used in the KGHM version
if [ "$ieasyhydroforecast_organization" = "kghm" ]; then
    echo "Pulling KGHM-specific images"
    for image in $KGHM_IMAGES; do
        pull_and_verify "$REPO/$image:$TAG" true || exit 1
    done
fi

echo "Building local pipeline image..."

# Build forecast pipeline locally
docker compose -f "$PROJECT_ROOTbin/docker-compose-luigi.yml" build --no-cache

echo "All images pulled and verified successfully!"