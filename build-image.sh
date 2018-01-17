#!/usr/bin/env bash

# Release build: ./build-image.sh <version>

set -eu -o pipefail
TAG="$1"
cd "$(dirname "$0")"
docker build --target release -t "replicator:$TAG" .
