#!/usr/bin/env bash
# Open cqlsh against demo-hub ring pod cassandra-0 (no kubectl port-forward).
# Use from any terminal while the cluster is up; does not require ./port-forward-demo-hub.sh.
# Examples:
#   ./deploy/k8s/scripts/demo-hub-cqlsh.sh
#   ./deploy/k8s/scripts/demo-hub-cqlsh.sh -e "DESCRIBE KEYSPACES;"
#   NS=my-ns ./deploy/k8s/scripts/demo-hub-cqlsh.sh
set -euo pipefail
NS="${NS:-demo-hub}"
exec kubectl -n "$NS" exec -it cassandra-0 -c cassandra -- cqlsh 127.0.0.1 9042 "$@"
