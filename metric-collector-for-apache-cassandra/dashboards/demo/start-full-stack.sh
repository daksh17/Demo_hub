#!/usr/bin/env bash
# Start the full MCAC demo: Cassandra (all rings), Kafka/Connect, Postgres HA, Mongo sharded,
# Redis, OpenSearch, Prometheus, Grafana, exporters — everything in ./docker-compose.yml.
#
# Do NOT use ../../docker-compose.yaml (dashboards root); that file only runs Prometheus + Grafana.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${SCRIPT_DIR}"

if [[ -z "${PROJECT_VERSION:-}" ]]; then
  PROJECT_VERSION="$(grep '<revision>' "${REPO_ROOT}/pom.xml" | sed -E 's/(<\/?revision>|[[:space:]])//g')"
  export PROJECT_VERSION
fi
if [[ -z "${PROJECT_VERSION}" ]]; then
  echo "error: could not read <revision> from ${REPO_ROOT}/pom.xml; set PROJECT_VERSION manually." >&2
  exit 1
fi

echo "PROJECT_VERSION=${PROJECT_VERSION}"
echo "Building mcac (agent into volume), kafka-connect (Mongo sink), and hub-demo-ui..."
docker compose build mcac kafka-connect hub-demo-ui

echo "Starting all services (this may take several minutes)..."
docker compose up -d

echo ""
echo "Full stack is starting. When healthy:"
echo "  Grafana    http://localhost:3000"
echo "  Prometheus http://localhost:9090"
echo "  Kafka      localhost:9092   Connect REST http://localhost:8083"
echo "  Postgres primary localhost:15432  Mongo mongos localhost:27025"
echo "  Redis      localhost:6379   OpenSearch http://localhost:9200"
echo "  Hub demo UI http://localhost:8888  (writes Postgres/Mongo/Redis/Cassandra/OpenSearch)"
echo ""
echo "Kafka Connect demo connectors (pg + mongo CDC/sink) register via one-shot service **kafka-connect-register**."
echo "Check: docker compose logs kafka-connect-register   |   curl -s http://localhost:8083/connectors"
echo "Re-run manually if needed: ./kafka-connect-register/register-all.sh"
