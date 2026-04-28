#!/usr/bin/env bash
# Start postgresql-replica-1 / postgresql-replica-2 (physical standbys) after they were stopped via
# stop-postgres-ha-and-connectors.sh (or manual scale/stop). Optionally re-run idempotent physical
# slot SQL on the primary if slots were lost (e.g. primary volume reset).
#
# Compose:
#   ./deploy/docker/postgres-kafka/start-postgres-standbys.sh
#
# Kubernetes:
#   NS=demo-hub ./deploy/docker/postgres-kafka/start-postgres-standbys.sh
#
# Environment:
#   NS=demo-hub              — kubectl mode
#   SKIP_ENSURE_SLOTS=1      — do not run ensure-physical-replication-slots.sql (default: run it)
#   PGPASSWORD / replicator: demo uses replicatorpass for -U replicator
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_ROOT="$(cd "$HERE/../../.." && pwd)"
NS="${NS:-}"
SLOTS_SQL="$HERE/ensure-physical-replication-slots.sql"

ensure_slots() {
  if [[ "${SKIP_ENSURE_SLOTS:-0}" == "1" ]]; then
    echo "SKIP_ENSURE_SLOTS=1 — skipping ensure-physical-replication-slots.sql"
    return 0
  fi
  if [[ ! -f "$SLOTS_SQL" ]]; then
    echo "WARN: missing $SLOTS_SQL" >&2
    return 0
  fi
  echo "=== Primary: ensure physical replication slots (replicator) ==="
  if [[ -n "$NS" ]]; then
    kubectl exec -i -n "$NS" deployment/postgresql-primary -c postgresql-primary -- \
      env PGPASSWORD="${REPLICATOR_PASSWORD:-replicatorpass}" psql -U replicator -d postgres -v ON_ERROR_STOP=1 <"$SLOTS_SQL"
  else
    (cd "$DEMO_ROOT" && docker compose exec -T postgresql-primary \
      env PGPASSWORD="${REPLICATOR_PASSWORD:-replicatorpass}" psql -U replicator -d postgres -v ON_ERROR_STOP=1 <"$SLOTS_SQL")
  fi
}

start_replicas() {
  echo "=== Start standbys ==="
  if [[ -n "$NS" ]]; then
    kubectl scale deployment/postgresql-replica-1 deployment/postgresql-replica-2 -n "$NS" --replicas=1
    kubectl rollout status deployment/postgresql-replica-1 -n "$NS" --timeout=300s
    kubectl rollout status deployment/postgresql-replica-2 -n "$NS" --timeout=300s
    echo "K8s: replicas scaled to 1 and rollout complete."
  else
    if [[ ! -f "$DEMO_ROOT/docker-compose.yml" ]]; then
      echo "ERROR: set NS for Kubernetes or run from demo tree with docker-compose.yml" >&2
      exit 1
    fi
    (cd "$DEMO_ROOT" && docker compose start postgresql-replica-1 postgresql-replica-2)
    echo "Compose: postgresql-replica-1 / postgresql-replica-2 started."
  fi
}

main() {
  ensure_slots
  start_replicas
  echo ""
  echo "Standbys are up. Re-register connectors when ready:"
  echo "  ./deploy/docker/postgres-kafka/register-connectors.sh [connect-url]"
}

main "$@"
