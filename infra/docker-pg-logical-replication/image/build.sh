#!/usr/bin/env bash

set -e

DOCKER_REPO="$1"
if [ -z "$DOCKER_REPO" ]; then
    echo "Usage: $0 <docker_repo>"
    echo "Example: $0 myusername"
    exit 1
fi

IMAGE_NAME="$DOCKER_REPO/pg-logical-replication"
TAG="17"

# Enable Docker BuildKit
export DOCKER_BUILDKIT=1

# Create a builder instance
docker buildx create --use --name multiarch-builder || true

# Build and push multi-architecture image
echo "Building multi-arch image $IMAGE_NAME:$TAG"
docker buildx build --platform linux/amd64,linux/arm64 \
    -t "$IMAGE_NAME:$TAG" \
    --push \
    .
