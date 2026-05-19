#!/usr/bin/env bash
# Reset DSE data and redeploy (fixes stale localhost / bad config on PVC).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Scaling down DSE..."
kubectl scale deployment/dse -n killrvideo --replicas=0 2>/dev/null || true
kubectl wait --for=delete pod -l app=dse -n killrvideo --timeout=120s 2>/dev/null || true

echo "Deleting DSE PVC and schema job..."
kubectl delete pvc dse-data -n killrvideo --ignore-not-found
kubectl delete job dse-config -n killrvideo --ignore-not-found
kubectl wait --for=delete pvc/dse-data -n killrvideo --timeout=120s 2>/dev/null || true

echo "Applying manifests..."
kubectl apply -k k8s/

echo "Waiting for DSE (first start: 5-10 minutes)..."
kubectl rollout status deployment/dse -n killrvideo --timeout=900s
kubectl wait --for=condition=complete job/dse-config -n killrvideo --timeout=900s
kubectl rollout status deployment/backend -n killrvideo --timeout=900s

echo ""
echo "Done. Run: ./scripts/k8s-port-forward.sh"
kubectl get pods -n killrvideo
