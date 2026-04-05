#!/bin/sh
set -e

mkdir -p "$DAGSTER_HOME"
cp /app/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

exec "$@"
