#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Applying KillrVideo stack to Kubernetes (namespace: killrvideo)..."
echo "Tip: allocate at least 12 GB RAM to OrbStack/Docker Desktop for DSE Search+Graph."
kubectl apply -k k8s/

echo ""
echo "Waiting for DSE (can take several minutes on first start)..."
kubectl rollout status deployment/dse -n killrvideo --timeout=900s

echo ""
echo "Waiting for schema bootstrap job..."
kubectl wait --for=condition=complete job/dse-config -n killrvideo --timeout=900s

echo ""
echo "Waiting for backend and web..."
kubectl rollout status deployment/backend -n killrvideo --timeout=900s
kubectl rollout status deployment/web -n killrvideo --timeout=300s

echo ""
echo "KillrVideo is starting. First boot can take 3-5 minutes for DSE + sample data."
echo ""
echo "Open the UIs (same ports as Docker Compose):"
echo "  ./scripts/k8s-port-forward.sh"
echo ""
echo "  Web UI:          http://localhost:3000"
echo "  DataStax Studio: http://localhost:9091"
echo ""
echo "Status:"
kubectl get pods,svc -n killrvideo
