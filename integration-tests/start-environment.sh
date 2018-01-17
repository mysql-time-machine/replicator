#!/usr/bin/env bash
set -eu

NAME="$1"
echo "Starting environment $NAME..."

cd "$(dirname "$0")"

pushd replicator_it
docker-compose -f "$NAME.yml" up -d
popd

DONE="$(docker logs -f replicatorit_replicator_1 | grep -m 1 -e '--- MySQL Ready ---')"

if [ -z "$DONE" ]; then
    echo "Environment did not start"
    exit 1
fi

echo "Started $NAME"
