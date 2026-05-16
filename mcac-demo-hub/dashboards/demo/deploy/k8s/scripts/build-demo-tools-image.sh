#!/usr/bin/env bash
set -euo pipefail
DEMO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
docker build -t demo-hub/demo-tools:latest -f "$DEMO_ROOT/deploy/docker/demo-tools/Dockerfile" "$DEMO_ROOT/deploy/docker/demo-tools"
echo "Built demo-hub/demo-tools:latest"
