#!/usr/bin/env bash
# Stop Debezium + JDBC connectors and stop physical replicas (standbys). Does **not** drop replication
# slots on the primary — restarting standbys can reuse existing pgdemo_phys_replica_* / debezium slots.
#
# Docker Compose:
#   ./deploy/docker/postgres-kafka/stop-postgres-ha-and-connectors.sh [kafka-connect-url]
#
# Kubernetes:
#   NS=demo-hub ./deploy/docker/postgres-kafka/stop-postgres-ha-and-connectors.sh http://127.0.0.1:8083
#
# Start standbys again:
#   ./deploy/docker/postgres-kafka/start-postgres-standbys.sh
#
# Environment:
#   NS=demo-hub         — if set, kubectl mode; else docker compose
#   SKIP_CONNECTORS=1   — only stop replicas
#   SKIP_REPLICAS=1     — only remove connectors
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_ROOT="$(cd "$HERE/../../.." && pwd)"
CONNECT="${1:-${CONNECT:-http://localhost:8083}}"
CONNECT="${CONNECT%/}"
NS="${NS:-}"
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-5}"
CURL_MAX_TIME="${CURL_MAX_TIME:-30}"
curl_opts=(--connect-timeout "${CURL_CONNECT_TIMEOUT}" --max-time "${CURL_MAX_TIME}" -sS)

remove_connector() {
  local name="$1" code i
  code="$(curl "${curl_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
  if [[ "$code" == "404" ]]; then
    echo "  ${name}: not registered, skip."
    return 0
  fi
  echo "  ${name}: pausing, then DELETE; waiting for unregister (up to ~120s)..."
  curl "${curl_opts[@]}" -o /dev/null -X PUT "$CONNECT/connectors/$name/pause" 2>/dev/null || true
  sleep 2
  curl "${curl_opts[@]}" -o /dev/null -X DELETE "$CONNECT/connectors/$name" 2>/dev/null || true
  for i in $(seq 1 120); do
    code="$(curl "${curl_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
    if [[ "$code" == "404" ]]; then
      echo "  ${name}: removed."
      return 0
    fi
    if (( i % 15 == 0 )); then
      echo "  ${name}: still present after ${i}s (HTTP ${code})."
    fi
    sleep 1
  done
  echo "Timeout: connector ${name} still present (last HTTP ${code})" >&2
  return 1
}

remove_connectors() {
  echo "=== Kafka Connect: remove jdbc-sink-demo, then pg-source-demo ==="
  if ! curl -sf "$CONNECT/connectors" >/dev/null 2>&1; then
    echo "WARN: Connect not reachable at $CONNECT — skip connector removal or fix port-forward." >&2
    return 0
  fi
  remove_connector "jdbc-sink-demo"
  remove_connector "pg-source-demo"
  echo "Connectors removed."
}

stop_replicas() {
  echo "=== Stop physical replicas (streaming standbys) ==="
  if [[ -n "$NS" ]]; then
    kubectl scale deployment/postgresql-replica-1 deployment/postgresql-replica-2 -n "$NS" --replicas=0
    echo "K8s: postgresql-replica-1 / postgresql-replica-2 scaled to 0 in namespace $NS."
  else
    if [[ ! -f "$DEMO_ROOT/docker-compose.yml" ]]; then
      echo "ERROR: no NS= set and no $DEMO_ROOT/docker-compose.yml — set NS for Kubernetes." >&2
      exit 1
    fi
    (cd "$DEMO_ROOT" && docker compose stop postgresql-replica-1 postgresql-replica-2)
    echo "Compose: postgresql-replica-1 / postgresql-replica-2 stopped."
  fi
}

main() {
  if [[ "${SKIP_CONNECTORS:-0}" != "1" ]]; then
    remove_connectors
  else
    echo "SKIP_CONNECTORS=1 — not removing connectors."
  fi

  if [[ "${SKIP_REPLICAS:-0}" != "1" ]]; then
    stop_replicas
  else
    echo "SKIP_REPLICAS=1 — replicas left running."
  fi

  echo ""
  echo "Done. Connectors removed (if reachable); standbys stopped. Slots on primary were not dropped."
  echo "Start standbys: ./deploy/docker/postgres-kafka/start-postgres-standbys.sh"
  echo "Then re-register CDC: ./deploy/docker/postgres-kafka/register-connectors.sh (K8s: SCHEMA_HISTORY / DEMO_HUB_K8S as in README)."
}

main "$@"
