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

echo "Building image $IMAGE_NAME:$TAG"
docker build -t "$IMAGE_NAME:$TAG" .

echo "Pushing image $IMAGE_NAME:$TAG"
docker push "$IMAGE_NAME:$TAG"
