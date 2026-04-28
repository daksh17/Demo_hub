#!/usr/bin/env bash
# Clean restart of demo-hub PostgreSQL HA: new pods (emptyDir = fresh data), re-run postgres-demo-bootstrap,
# then you register Kafka Connect (Debezium) yourself with port-forward.
#
# From dashboards/demo:
#   chmod +x deploy/k8s/scripts/restart-postgres-ha-clean-k8s.sh
#   ./deploy/k8s/scripts/restart-postgres-ha-clean-k8s.sh
#
# Environment:
#   NS=demo-hub   (default: demo-hub)
#   SKIP_BOOTSTRAP_JOB=1   — only rollout restart Postgres deployments (no Job delete/apply)
set -euo pipefail

NS="${NS:-demo-hub}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GEN="$DEMO_ROOT/deploy/k8s/generated"

echo "=== PostgreSQL HA clean restart (namespace: $NS) ==="
echo "    WARNING: emptyDir — this wipes primary + replica data. Debezium slot and DBs are recreated by bootstrap."

for d in postgresql-primary postgresql-replica-1 postgresql-replica-2; do
  if ! kubectl get deployment/"$d" -n "$NS" >/dev/null 2>&1; then
    echo "ERROR: deployment/$d not found in $NS" >&2
    exit 1
  fi
done

if [[ "${SKIP_BOOTSTRAP_JOB:-0}" != "1" ]]; then
  echo "=== Delete postgres-demo-bootstrap job (if any) ==="
  kubectl delete job postgres-demo-bootstrap -n "$NS" --ignore-not-found
fi

echo "=== Rollout restart: postgresql-primary, replica-1, replica-2 ==="
for d in postgresql-primary postgresql-replica-1 postgresql-replica-2; do
  kubectl rollout restart "deployment/$d" -n "$NS"
done
for d in postgresql-primary postgresql-replica-1 postgresql-replica-2; do
  kubectl rollout status "deployment/$d" -n "$NS" --timeout=420s
done

if [[ "${SKIP_BOOTSTRAP_JOB:-0}" != "1" ]]; then
  if [[ ! -f "$GEN/45-postgres-bootstrap-job.yaml" ]]; then
    echo "ERROR: missing $GEN/45-postgres-bootstrap-job.yaml (run from repo; gen_demo_hub_k8s.py if needed)" >&2
    exit 1
  fi
  echo "=== Apply postgres-demo-bootstrap job ==="
  kubectl apply -f "$GEN/45-postgres-bootstrap-job.yaml"
  kubectl wait --for=condition=complete job/postgres-demo-bootstrap -n "$NS" --timeout=3600s
  echo "Bootstrap job completed."
fi

echo ""
echo "=== Next: port-forward (if not already): ==="
echo "  cd \"$DEMO_ROOT\""
echo "  ./deploy/k8s/scripts/port-forward-demo-hub.sh"
echo ""
echo "=== Then register Postgres connectors (K8s broker kafka:9092): ==="
echo "  cd \"$DEMO_ROOT\""
echo "  SCHEMA_HISTORY_KAFKA_BOOTSTRAP=kafka:9092 ./deploy/docker/postgres-kafka/register-connectors.sh http://127.0.0.1:8083"
echo ""
echo "Or Postgres + Mongo:"
echo "  DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all.sh http://127.0.0.1:8083"
