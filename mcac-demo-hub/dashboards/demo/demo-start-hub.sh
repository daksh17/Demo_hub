#!/usr/bin/env bash
# One-shot: start the demo-hub stack on Kubernetes (apply + data bootstrap), then register
# all Kafka Connect connectors (Postgres + Mongo + SQL Server) via a temporary port-forward
# to kafka-connect:8083.
#
# Run from this directory (dashboards/demo):
#   ./demo-start-hub.sh
#   REGEN=1 ./demo-start-hub.sh
#   ./demo-start-hub.sh --no-connectors    # stack + bootstrap only; skip connector registration
#
# Environment (optional):
#   NS=demo-hub
#   REGEN=1              — regenerate deploy/k8s/generated before apply (passes to demo-hub.sh)
#   SKIP_BOOTSTRAP=1     — apply manifests only (demo-hub.sh)
#   SKIP_CONNECTORS=1    — same as --no-connectors
#   MSSQL_SA_PASSWORD    — if non-default SA password (connector registration)
#
set -euo pipefail
set +H 2>/dev/null || true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

export NS="${NS:-demo-hub}"
SKIP_CONNECTORS="${SKIP_CONNECTORS:-}"

for arg in "$@"; do
  case "$arg" in
    --help|-h)
      grep '^#' "$0" | head -22 | sed 's/^# \?//'
      exit 0
      ;;
    --no-connectors)
      SKIP_CONNECTORS=1
      ;;
  esac
done

echo "=== Phase 1: Kubernetes (demo-hub.sh start) ==="
"$ROOT/deploy/k8s/scripts/demo-hub.sh" start

if [[ "$SKIP_CONNECTORS" == "1" ]]; then
  echo ""
  echo "Connectors skipped. When ready, port-forward Connect :8083 and run:"
  echo "  DEMO_HUB_K8S=1 $ROOT/deploy/docker/kafka-connect-register/register-all-connectors.sh http://127.0.0.1:8083"
  exit 0
fi

echo ""
echo "=== Phase 2: Port-forward kafka-connect:8083 (background) ==="
kubectl port-forward -n "$NS" svc/kafka-connect 8083:8083 &
PF_PID=$!
cleanup() { kill "$PF_PID" 2>/dev/null || true; }
trap cleanup EXIT

for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8083/connectors >/dev/null 2>&1; then
    echo "Kafka Connect REST is up."
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    echo "Timeout waiting for http://127.0.0.1:8083/connectors" >&2
    exit 1
  fi
  sleep 1
done

echo ""
echo "=== Phase 3: Register all connectors (DEMO_HUB_K8S=1) ==="
export DEMO_HUB_K8S=1
"$ROOT/deploy/docker/kafka-connect-register/register-all-connectors.sh" http://127.0.0.1:8083

echo ""
echo "=== Done ==="
echo "Hub UI (example):  kubectl port-forward -n $NS svc/hub-demo-ui 8888:8888"
echo "Full forwards:     $ROOT/deploy/k8s/scripts/demo-hub.sh port-forward"
