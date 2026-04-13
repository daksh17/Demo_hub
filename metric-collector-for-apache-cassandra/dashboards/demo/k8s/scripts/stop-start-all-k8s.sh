#!/usr/bin/env bash
# Full teardown + apply + bootstrap: same as ./k8s/scripts/demo-hub.sh restart, with
# opinionated defaults for a "reset everything" workflow from dashboards/demo.
#
# Defaults (override by exporting before running, e.g. WITH_PORT_FORWARD=1 ./k8s/scripts/stop-start-all-k8s.sh):
#   WITH_PORT_FORWARD=0          — do not run kubectl port-forward after start (non-blocking).
#                                Set to 1 only if you want this terminal to hold localhost forwards
#                                until Ctrl+C. If you use Ingress (96-kubernetes-ops.yaml) or
#                                NodePort for Cassandra, port-forward is usually not required.
#   APPLY_CASSANDRA_CQL_NODEPORT=1 — apply k8s/optional-cassandra-0-cql-nodeport.yaml after bootstrap
#                                (<node-ip>:30942 → cassandra-0 for DBeaver when port-forward is flaky).
#   REGEN=0                      — set to 1 to run gen_demo_hub_k8s.py before apply (pick up YAML changes).
#   SKIP_BOOTSTRAP=0             — set to 1 to apply workloads only, skip Postgres/Cassandra/Mongo Jobs.
#
# Passed through to demo-hub.sh / port-forward-demo-hub.sh when set:
#   NS  DELETE_NS_WAIT_SEC  WAIT_READY_TIMEOUT  SKIP_PROMETHEUS
#
# Examples:
#   ./k8s/scripts/stop-start-all-k8s.sh
#   REGEN=1 ./k8s/scripts/stop-start-all-k8s.sh
#   APPLY_CASSANDRA_CQL_NODEPORT=0 ./k8s/scripts/stop-start-all-k8s.sh
#   WITH_PORT_FORWARD=1 ./k8s/scripts/stop-start-all-k8s.sh
#   WITH_PORT_FORWARD=1 SKIP_PROMETHEUS=1 ./k8s/scripts/stop-start-all-k8s.sh
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Explicit defaults: port-forward is opt-in so restart returns when Jobs/bootstrap finish (or skip).
export WITH_PORT_FORWARD="${WITH_PORT_FORWARD:-0}"
export APPLY_CASSANDRA_CQL_NODEPORT="${APPLY_CASSANDRA_CQL_NODEPORT:-1}"

echo "=== stop-start-all-k8s.sh → demo-hub.sh restart ==="
echo "    WITH_PORT_FORWARD=${WITH_PORT_FORWARD}  APPLY_CASSANDRA_CQL_NODEPORT=${APPLY_CASSANDRA_CQL_NODEPORT}  REGEN=${REGEN:-0}  SKIP_BOOTSTRAP=${SKIP_BOOTSTRAP:-0}"
if [[ "${WITH_PORT_FORWARD}" == "1" ]]; then
  echo "    (this terminal will block on kubectl port-forward — Ctrl+C stops forwards)"
else
  echo "    (no port-forward — use: ./k8s/scripts/port-forward-demo-hub.sh  or Ingress / NodePort)"
fi

exec "$SCRIPT_DIR/demo-hub.sh" restart
