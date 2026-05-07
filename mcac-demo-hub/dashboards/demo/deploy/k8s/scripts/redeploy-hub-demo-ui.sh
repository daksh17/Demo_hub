#!/usr/bin/env bash
# Rebuild mcac-demo/hub-demo-ui:latest from demo-ui sources and restart the Deployment in demo-hub.
# Use after changing app.py, postgres_logical_demo.py, postgres_faker_schema.py, hub_config.py, scenario.py, or requirements.
#
# Usage (from anywhere):
#   ./deploy/k8s/scripts/redeploy-hub-demo-ui.sh
# Environment:
#   NS=demo-hub                      Kubernetes namespace
#   HUB_DEMO_UI_IMAGE=mcac-demo/hub-demo-ui:latest   image tag for docker build / cluster expects this tag in manifests
#   ROLLOUT_TIMEOUT=300s             kubectl rollout status timeout
#   NO_CACHE=1                       docker build --no-cache (avoid stale COPY layers)
set -euo pipefail

NS="${NS:-demo-hub}"
HUB_DEMO_UI_IMAGE="${HUB_DEMO_UI_IMAGE:-mcac-demo/hub-demo-ui:latest}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# dashboards/demo — three levels up from deploy/k8s/scripts
DEMO="$(cd "$SCRIPT_DIR/../../.." && pwd)"
UI="$DEMO/deploy/docker/realtime-orders-search-hub/demo-ui"

CACHE_FLAG=()
[[ "${NO_CACHE:-}" == "1" ]] && CACHE_FLAG=(--no-cache)

echo "Sources for this build (must match the repo where you edited app.py):"
echo "  DEMO=$DEMO"
echo "  UI=$UI"
if [[ ! -f "$UI/app.py" ]]; then
  echo "ERROR: $UI/app.py not found — wrong DEMO path?" >&2
  exit 1
fi

echo "=== docker build $HUB_DEMO_UI_IMAGE ==="
docker build "${CACHE_FLAG[@]}" -t "$HUB_DEMO_UI_IMAGE" -f "$UI/Dockerfile" "$UI"

echo "=== kubectl rollout restart deployment/hub-demo-ui -n $NS ==="
kubectl rollout restart deployment/hub-demo-ui -n "$NS"
kubectl rollout status deployment/hub-demo-ui -n "$NS" --timeout="$ROLLOUT_TIMEOUT"
echo "Done. Open /postgres/logical — Refresh status should show setup_revision from postgres_logical_demo.py; click Run setup once if you changed grants."
echo ""
echo "Kafka UI sanity check (parallel panels marker — expect count ≥ 1):"
echo "  kubectl exec -n $NS deploy/hub-demo-ui -- grep -c kafkaParallelWrap /app/app.py || true"
echo "kind/minikube: if nodes don't see your local image: kind load docker-image $HUB_DEMO_UI_IMAGE"
echo "Stale layers: NO_CACHE=1 ./deploy/k8s/scripts/redeploy-hub-demo-ui.sh"
