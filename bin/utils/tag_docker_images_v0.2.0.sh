#!/bin/bash
# Script to tag current Docker images with v0.2.0 release tag
# Run this after logging in to DockerHub: docker login

set -e  # Exit on any error

VERSION="v0.2.0"
REGISTRY="mabesa"

IMAGES=(
    "sapphire-ml"
    "sapphire-conceptmod"
    "sapphire-dashboard"
    "sapphire-prepgateway"
    "sapphire-linreg"
    "sapphire-postprocessing"
    "sapphire-preprunoff"
    "sapphire-rerun"
    "sapphire-pipeline"
    "sapphire-pythonbaseimage"
)

echo "=== Tagging Docker images with $VERSION ==="
echo ""

for IMAGE in "${IMAGES[@]}"; do
    FULL_NAME="$REGISTRY/$IMAGE"
    echo "Processing $FULL_NAME..."

    echo "  Pulling :latest..."
    docker pull "$FULL_NAME:latest"

    echo "  Tagging as :$VERSION..."
    docker tag "$FULL_NAME:latest" "$FULL_NAME:$VERSION"

    echo "  Pushing :$VERSION..."
    docker push "$FULL_NAME:$VERSION"

    echo "  ✓ Done with $IMAGE"
    echo ""
done

echo "=== All images tagged with $VERSION ==="
