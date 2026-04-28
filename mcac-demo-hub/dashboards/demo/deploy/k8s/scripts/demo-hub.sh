#!/usr/bin/env bash
# Single entry point for the demo-hub Kubernetes stack: ordered start / stop / restart / status.
#
# Usage (from dashboards/demo):
#   ./demo-start-hub.sh                               # apply + bootstrap + register all Kafka Connect connectors (see repo root)
#   ./deploy/k8s/scripts/demo-hub.sh start            # apply manifests + data bootstrap
#   ./deploy/k8s/scripts/demo-hub.sh stop             # delete namespace (full teardown)
#   ./deploy/k8s/scripts/demo-hub.sh restart          # stop then start (clean slate + bootstrap)
#   ./deploy/k8s/scripts/demo-hub.sh status            # quick pod overview
#   ./deploy/k8s/scripts/demo-hub.sh wait-ready        # block until Deployments + Cassandra STS are Ready
#   ./deploy/k8s/scripts/demo-hub.sh port-forward      # wait-ready, then kubectl port-forward (same as port-forward-demo-hub.sh)
#
# Environment (optional):
#   NS=demo-hub              — namespace (default: demo-hub)
#   REGEN=1                  — run gen_demo_hub_k8s.py before apply (start/restart)
#   SKIP_BOOTSTRAP=1         — apply workloads only, skip Jobs (start/restart)
#   DELETE_NS_WAIT_SEC=240   — max seconds to wait for namespace removal on stop/restart (default 240)
#   WITH_PORT_FORWARD=1      — after start/restart bootstrap (or apply if SKIP_BOOTSTRAP): wait-ready + port-forward (blocks)
#   WAIT_READY_TIMEOUT=900s  — kubectl wait timeout (default 900s)
#   SKIP_PROMETHEUS=1        — passed through to port-forward-demo-hub.sh when using port-forward / WITH_PORT_FORWARD
#   APPLY_CASSANDRA_CQL_NODEPORT=1 — after apply + bootstrap: kubectl apply optional-cassandra-0-cql-nodeport.yaml (local DBeaver/CQL)
#
set -euo pipefail

NS="${NS:-demo-hub}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GEN="$ROOT/generated"
ALL_YAML="$GEN/all.yaml"
DELETE_NS_WAIT_SEC="${DELETE_NS_WAIT_SEC:-240}"
WAIT_READY_TIMEOUT="${WAIT_READY_TIMEOUT:-900s}"

usage() {
  cat <<'EOF'
demo-hub.sh — single entry point for the demo-hub Kubernetes stack

Usage (from dashboards/demo):
  ./demo-start-hub.sh                             full stack + connector registration (wrapper)
  ./deploy/k8s/scripts/demo-hub.sh start          apply generated/all.yaml + data bootstrap Jobs
  ./deploy/k8s/scripts/demo-hub.sh stop           delete namespace demo-hub (full teardown)
  ./deploy/k8s/scripts/demo-hub.sh restart        stop then start (clean slate + bootstrap)
  ./deploy/k8s/scripts/demo-hub.sh status           pods / deploy / sts / jobs snapshot
  ./deploy/k8s/scripts/demo-hub.sh wait-ready       wait until labeled Deployments + cassandra STS are Ready
  ./deploy/k8s/scripts/demo-hub.sh port-forward     wait-ready, then localhost port-forwards (see port-forward-demo-hub.sh)

Environment:
  NS=demo-hub              namespace (default: demo-hub)
  REGEN=1                  run gen_demo_hub_k8s.py before apply (start/restart)
  SKIP_BOOTSTRAP=1         apply only, skip bootstrap Jobs (start/restart)
  DELETE_NS_WAIT_SEC=240   max wait for namespace removal on stop/restart
  WITH_PORT_FORWARD=1      after start/restart: wait-ready, then port-forward (blocks; Ctrl+C stops)
  WAIT_READY_TIMEOUT=900s  timeout for wait-ready / port-forward pre-check
  SKIP_PROMETHEUS=1        omit Prometheus forward if its pod is down
  APPLY_CASSANDRA_CQL_NODEPORT=1  apply deploy/k8s/optional-cassandra-0-cql-nodeport.yaml after stack is up (stop-start-all-k8s.sh sets this by default)
EOF
  exit "${1:-0}"
}

regen_manifests() {
  echo "=== Regenerate manifests (gen_demo_hub_k8s.py) ==="
  python3 "$ROOT/scripts/gen_demo_hub_k8s.py"
}

wait_namespace_gone() {
  local max_iter=$((DELETE_NS_WAIT_SEC / 2))
  echo "Waiting for namespace $NS to terminate (up to ${DELETE_NS_WAIT_SEC}s)..."
  for _ in $(seq 1 "$max_iter"); do
    if ! kubectl get ns "$NS" &>/dev/null; then
      echo "Namespace $NS is gone."
      return 0
    fi
    sleep 2
  done
  echo "Namespace $NS still exists after ${DELETE_NS_WAIT_SEC}s." >&2
  echo "Check: kubectl get ns $NS -o yaml | tail -40" >&2
  return 1
}

# Wait until all app.kubernetes.io/part-of=demo-hub Deployments are Available and Cassandra StatefulSet is rolled out.
cmd_wait_ready() {
  if ! kubectl get ns "$NS" &>/dev/null; then
    echo "Namespace $NS not found. Apply manifests first." >&2
    exit 1
  fi
  echo "=== Wait for Deployments (app.kubernetes.io/part-of=demo-hub), timeout ${WAIT_READY_TIMEOUT} ==="
  kubectl wait --for=condition=available deployment -n "$NS" -l app.kubernetes.io/part-of=demo-hub --timeout="${WAIT_READY_TIMEOUT}"
  echo "=== Wait for Cassandra StatefulSet (cassandra) ==="
  kubectl rollout status statefulset/cassandra -n "$NS" --timeout="${WAIT_READY_TIMEOUT}"
  echo "=== Ready: core Deployments + cassandra ring ==="
}

maybe_apply_cassandra_cql_nodeport() {
  [[ "${APPLY_CASSANDRA_CQL_NODEPORT:-}" == "1" ]] || return 0
  local f="$ROOT/optional-cassandra-0-cql-nodeport.yaml"
  if [[ ! -f "$f" ]]; then
    echo "WARN: $f not found — skip optional Cassandra NodePort" >&2
    return 0
  fi
  echo "=== Optional: Cassandra CQL NodePort (DBeaver / local CQL on node :30942) ==="
  kubectl apply -f "$f"
}

cmd_port_forward() {
  cmd_wait_ready
  echo "=== Port-forward (localhost) — Ctrl+C to stop ==="
  exec "$ROOT/scripts/port-forward-demo-hub.sh"
}

cmd_stop() {
  echo "=== STOP: delete namespace $NS ==="
  if ! kubectl get ns "$NS" &>/dev/null; then
    echo "Namespace $NS does not exist — nothing to delete."
    return 0
  fi
  kubectl delete namespace "$NS" --wait=false
  wait_namespace_gone
}

cmd_start() {
  if [[ ! -f "$ALL_YAML" ]]; then
    echo "Missing $ALL_YAML — run: python3 deploy/k8s/scripts/gen_demo_hub_k8s.py" >&2
    exit 1
  fi
  if [[ "${REGEN:-}" == "1" ]]; then
    regen_manifests
  fi

  echo "=== START: kubectl apply (single bundle) ==="
  kubectl apply -f "$ALL_YAML"

  if [[ "${SKIP_BOOTSTRAP:-}" == "1" ]]; then
    echo "SKIP_BOOTSTRAP=1 — skipping apply-data-bootstrap.sh"
    echo "Run later: $ROOT/scripts/apply-data-bootstrap.sh"
    maybe_apply_cassandra_cql_nodeport
    if [[ "${WITH_PORT_FORWARD:-}" == "1" ]]; then
      cmd_wait_ready
      exec "$ROOT/scripts/port-forward-demo-hub.sh"
    fi
    return 0
  fi

  echo "=== Data bootstrap (Postgres / Cassandra / Mongo Jobs) ==="
  "$ROOT/scripts/apply-data-bootstrap.sh"
  maybe_apply_cassandra_cql_nodeport

  if [[ "${WITH_PORT_FORWARD:-}" == "1" ]]; then
    cmd_wait_ready
    exec "$ROOT/scripts/port-forward-demo-hub.sh"
  fi
}

cmd_restart() {
  cmd_stop || exit 1
  cmd_start
}

cmd_status() {
  if ! kubectl get ns "$NS" &>/dev/null; then
    echo "Namespace $NS does not exist. Run: $0 start"
    exit 0
  fi
  echo "=== Namespace $NS ==="
  kubectl get pods -n "$NS" -o wide 2>/dev/null || true
  echo ""
  kubectl get deploy,sts,job -n "$NS" 2>/dev/null || true
}

main() {
  case "${1:-}" in
    start)         shift || true; cmd_start ;;
    stop)          shift || true; cmd_stop ;;
    restart)       shift || true; cmd_restart ;;
    status)        shift || true; cmd_status ;;
    wait-ready)    shift || true; cmd_wait_ready ;;
    port-forward)  shift || true; cmd_port_forward ;;
    -h|--help|help) usage 0 ;;
    "")
      echo "Missing command. Try: $0 start | stop | restart | status | wait-ready | port-forward" >&2
      usage 1
      ;;
    *)
      echo "Unknown command: $1" >&2
      usage 1
      ;;
  esac
}

main "$@"
