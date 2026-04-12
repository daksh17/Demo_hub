#!/usr/bin/env bash
# Full teardown + apply + bootstrap (same as: ./k8s/scripts/demo-hub.sh restart).
#
# Also applies optional Cassandra CQL NodePort by default so local tools (e.g. DBeaver on OrbStack)
# can use <node-ip>:30942 without a separate kubectl apply. Disable with:
#   APPLY_CASSANDRA_CQL_NODEPORT=0 ./k8s/scripts/stop-start-all-k8s.sh
#
# Optional: REGEN=1  SKIP_BOOTSTRAP=1  WITH_PORT_FORWARD=1  (see demo-hub.sh)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export APPLY_CASSANDRA_CQL_NODEPORT="${APPLY_CASSANDRA_CQL_NODEPORT:-1}"
exec "$SCRIPT_DIR/demo-hub.sh" restart
