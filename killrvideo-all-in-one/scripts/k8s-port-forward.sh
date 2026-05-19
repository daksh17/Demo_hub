#!/usr/bin/env bash
# Same ports as docker-compose-all-in-one.yml (3000, 9091, 50101).
set -euo pipefail

NS=killrvideo

echo "Forwarding (Ctrl+C to stop):"
echo "  Web UI:          http://localhost:3000"
echo "  DataStax Studio: http://localhost:9091"
echo "  Backend gRPC:    localhost:50101"
echo ""

kubectl port-forward -n "$NS" svc/web 3000:3000 &
PF_WEB=$!
kubectl port-forward -n "$NS" svc/studio 9091:9091 &
PF_STUDIO=$!
kubectl port-forward -n "$NS" svc/backend 50101:50101 &
PF_BACKEND=$!

cleanup() {
  kill "$PF_WEB" "$PF_STUDIO" "$PF_BACKEND" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

wait
