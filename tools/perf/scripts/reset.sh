#!/bin/sh
set -eu

docker compose -f "$(dirname "$0")/../compose.yaml" down -v --remove-orphans
