#!/usr/bin/env bash
# Build the sqlcmd image for Job mssql-demo-bootstrap (same stack as Compose mssql-publisher-schema-init).
# There is no public registry image — Kubernetes ImagePullBackOff means this was not built or the cluster
# cannot see your Docker image store (build with the same engine OrbStack/kind uses, then re-apply the Job).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
docker build -t mcac-demo/mssql-tools:22.04 \
  -f "$ROOT/deploy/docker/mssql-kafka/Dockerfile.repl-bootstrap" \
  "$ROOT/deploy/docker/mssql-kafka"
echo "Built mcac-demo/mssql-tools:22.04"
echo "Kubernetes: kubectl delete job -n demo-hub mssql-demo-bootstrap --ignore-not-found"
echo "           kubectl apply -f \"$ROOT/deploy/k8s/generated/62-mssql.yaml\""
echo "  (or run deploy/k8s/scripts/apply-data-bootstrap.sh after Connect is up)"
echo "kind:     kind load docker-image mcac-demo/mssql-tools:22.04"
