#!/usr/bin/env bash

STOP=100
while ! nc -w 1 -z mysql 3306; do
    STOP=$((STOP-1))
    if [ ${STOP} == 0 ]; then
        exit 1
    fi
    sleep 1
done

echo "--- MySQL Ready ---"

if [ ! -z "$TIMEOUT" ]; then
    timeout "$TIMEOUT" java -jar ./mysql-replicator.jar --config-path /etc/replicator/replicator.yaml "$@"
else
    java -jar ./mysql-replicator.jar --config-path /etc/replicator/replicator.yaml "$@"
fi