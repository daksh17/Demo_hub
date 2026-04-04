#!/usr/bin/env bash
# One-shot: register Postgres + Mongo CDC/sink connectors against Kafka Connect in the demo network.
# Full wipe + sink tables + re-register: ../reset-kafka-connect-demo.sh [url]
# Usage (host): ./register-all.sh           → http://localhost:8083 (Compose publishes 8083)
#            ./register-all.sh http://kafka-connect:8083  → only from inside the demo Docker network
# Compose one-shot passes the kafka-connect URL explicitly as $1.
set -euo pipefail

CONNECT="${1:-${KAFKA_CONNECT_URL:-http://127.0.0.1:8083}}"
CONNECT="${CONNECT%/}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO="$(cd "${HERE}/.." && pwd)"

echo "Waiting for Kafka Connect at ${CONNECT} ..."
for _ in $(seq 1 90); do
  if curl -sf "${CONNECT}/connectors" >/dev/null 2>&1; then
    echo "Kafka Connect is up."
    break
  fi
  sleep 2
done
if ! curl -sf "${CONNECT}/connectors" >/dev/null 2>&1; then
  echo "Timeout: ${CONNECT} did not respond." >&2
  exit 1
fi

echo "Registering Postgres Debezium + JDBC sink..."
bash "${DEMO}/postgres-kafka/register-connectors.sh" "${CONNECT}"

echo "Registering MongoDB Debezium + Mongo sink (deletes old connectors; wait ~15–60s, do not interrupt)..."
bash "${DEMO}/mongo-kafka/register-mongo-connectors.sh" "${CONNECT}"

echo "Done. List: curl -s ${CONNECT}/connectors"
