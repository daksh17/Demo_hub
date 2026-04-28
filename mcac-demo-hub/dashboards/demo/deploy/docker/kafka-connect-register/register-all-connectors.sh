#!/usr/bin/env bash
# Register all demo Kafka Connect connectors: Postgres + Mongo (4) + SQL Server (2).
#
# Kubernetes (recommended): set DEMO_HUB_K8S=1 — ensures MSSQL publisher/subscriber have
# database "demo" (idempotent SQL) unless SKIP_MSSQL_SCHEMA_APPLY=1, sets schema-history
# bootstrap to kafka:9092, then registers PG + Mongo + MSSQL.
#
# Compose: set DEMO_HUB_K8S=0 or unset — skips K8s-only MSSQL schema apply; use after
# mssql-publisher-schema-init / subscriber init (or rely on compose one-shot jobs).
#
# Usage:
#   ./register-all-connectors.sh [http://127.0.0.1:8083]
#
# Examples:
#   DEMO_HUB_K8S=1 ./register-all-connectors.sh http://127.0.0.1:8083
#   MSSQL_SA_PASSWORD='…' DEMO_HUB_K8S=1 ./register-all-connectors.sh
#
# Env:
#   DEMO_HUB_K8S — 1 = Kubernetes (apply MSSQL schema from host via kubectl + cluster DNS)
#   SKIP_MSSQL_SCHEMA_APPLY — 1 = do not run apply-mssql-schema-k8s.sh (K8s only)
#   MSSQL_SA_PASSWORD — must match Secret / Compose (default: Demo_hub_Mssql_2025!)
#   NS — namespace for MSSQL schema apply (default: demo-hub)
#
set -euo pipefail
set +H 2>/dev/null || true

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DEMO="$(cd "${HERE}/.." && pwd)"
DEMO_ROOT="$(cd "${HERE}/../../.." && pwd)"
CONNECT="${1:-${KAFKA_CONNECT_URL:-http://127.0.0.1:8083}}"
CONNECT="${CONNECT%/}"
NS="${NS:-demo-hub}"

if [[ "${DEMO_HUB_K8S:-}" == "1" ]]; then
  export SCHEMA_HISTORY_KAFKA_BOOTSTRAP="${SCHEMA_HISTORY_KAFKA_BOOTSTRAP:-kafka:9092}"
else
  export SCHEMA_HISTORY_KAFKA_BOOTSTRAP="${SCHEMA_HISTORY_KAFKA_BOOTSTRAP:-kafka:29092}"
fi

if [[ "${DEMO_HUB_K8S:-}" == "1" && "${SKIP_MSSQL_SCHEMA_APPLY:-}" != "1" ]]; then
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "DEMO_HUB_K8S=1 requires kubectl for MSSQL schema preflight (or set SKIP_MSSQL_SCHEMA_APPLY=1)." >&2
    exit 1
  fi
  echo "=== MSSQL: ensure database demo on publisher + subscriber ($NS) ==="
  NS="$NS" bash "${DEMO_ROOT}/deploy/k8s/scripts/apply-mssql-schema-k8s.sh"
  echo ""
fi

echo "=== Kafka Connect: Postgres + Mongo (Debezium + sinks) ==="
bash "${HERE}/register-all.sh" "${CONNECT}"

echo ""
echo "=== Kafka Connect: SQL Server Debezium + JDBC sink (publisher → subscriber) ==="
bash "${DOCKER_DEMO}/mssql-kafka/register-mssql-connectors.sh" "${CONNECT}"

echo ""
echo "All registration steps finished."
echo "  List:    curl -s ${CONNECT}/connectors"
echo "  Status:  curl -s ${CONNECT}/connectors/<name>/status"
