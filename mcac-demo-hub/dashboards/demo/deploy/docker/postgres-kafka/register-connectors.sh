#!/usr/bin/env bash
# Debezium: PostgreSQL -> Kafka (CDC). Debezium JDBC sink: Kafka -> PostgreSQL (separate table).
# On failure, prints Kafka Connect JSON error (older versions used curl -f and hid the message).
#
# Prerequisites: docker compose up -d zookeeper kafka postgresql-primary postgresql-replica-1 postgresql-replica-2 kafka-connect
# Usage:
#   ./register-connectors.sh [kafka-connect-url]           # replace source + sink
#   ./register-connectors.sh [url] jdbc-only               # refresh jdbc-sink-demo only (leave pg-source-demo running)
#   JDBC_SINK_ONLY=1 ./register-connectors.sh [url]      # same as jdbc-only
#
# Kubernetes (demo-hub): broker PLAINTEXT is kafka:9092 only — set
#   SCHEMA_HISTORY_KAFKA_BOOTSTRAP=kafka:9092
# so Debezium schema history can reach Kafka (Compose defaults to kafka:29092).
#
# Optional: after `source deploy/k8s/scripts/export-demo-hub-secrets-from-vault.sh`, PG_SOURCE_*
# and JDBC_SINK_* env vars override connector passwords/hosts (Vault-backed demo-hub).
set -euo pipefail
CONNECT="${1:-http://localhost:8083}"
CONNECT="${CONNECT%/}"
# Debezium schema.history.internal.kafka.bootstrap.servers (Compose: 29092, K8s demo-hub: 9092).
SCHEMA_HISTORY_KAFKA_BOOTSTRAP="${SCHEMA_HISTORY_KAFKA_BOOTSTRAP:-kafka:29092}"
PG_SOURCE_DB_HOST="${PG_SOURCE_DB_HOST:-postgresql-primary}"
PG_SOURCE_DB_PORT="${PG_SOURCE_DB_PORT:-5432}"
PG_SOURCE_DB_USER="${PG_SOURCE_DB_USER:-replicator}"
PG_SOURCE_DB_PASSWORD="${PG_SOURCE_DB_PASSWORD:-replicatorpass}"
PG_SOURCE_DB_NAME="${PG_SOURCE_DB_NAME:-demo}"
JDBC_SINK_URL="${JDBC_SINK_URL:-jdbc:postgresql://postgresql-primary:5432/demo}"
JDBC_SINK_USER="${JDBC_SINK_USER:-demo}"
JDBC_SINK_PASSWORD="${JDBC_SINK_PASSWORD:-demopass}"
if [[ "$CONNECT" == "jdbc-only" ]]; then
  CONNECT="http://127.0.0.1:8083"
  JDBC_SINK_ONLY=1
else
  JDBC_SINK_ONLY="${JDBC_SINK_ONLY:-0}"
  [[ "${2:-}" == "jdbc-only" ]] && JDBC_SINK_ONLY=1
fi
# Avoid indefinite hang if Connect REST stops responding mid-removal.
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-5}"
CURL_MAX_TIME="${CURL_MAX_TIME:-30}"
curl_connect_opts=(--connect-timeout "${CURL_CONNECT_TIMEOUT}" --max-time "${CURL_MAX_TIME}" -sS)

wait_for() {
  echo "Waiting for Kafka Connect..."
  for _ in $(seq 1 60); do
    if curl -sf "$CONNECT/connectors" >/dev/null 2>&1; then
      echo "Kafka Connect is up ($CONNECT/connectors)."
      return 0
    fi
    sleep 2
  done
  echo "Timeout: could not GET $CONNECT/connectors" >&2
  return 1
}

wait_for

# Kafka Connect removes connectors asynchronously; POST before removal finishes → HTTP 409.
remove_connector() {
  local name="$1" code i
  code="$(curl "${curl_connect_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
  if [[ "$code" == "404" ]]; then
    echo "  ${name}: not registered, skip." >&2
    return 0
  fi
  echo "  ${name}: pausing, then DELETE; waiting for Connect to unregister (up to ~120s)..." >&2
  curl "${curl_connect_opts[@]}" -o /dev/null -X PUT "$CONNECT/connectors/$name/pause" 2>/dev/null || true
  sleep 2
  curl "${curl_connect_opts[@]}" -o /dev/null -X DELETE "$CONNECT/connectors/$name" 2>/dev/null || true
  for i in $(seq 1 120); do
    code="$(curl "${curl_connect_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
    if [[ "$code" == "404" ]]; then
      echo "  ${name}: removed." >&2
      return 0
    fi
    if (( i % 15 == 0 )); then
      echo "  ${name}: still present after ${i}s (HTTP ${code})." >&2
    fi
    sleep 1
  done
  echo "Timeout: connector ${name} still present after DELETE (last HTTP ${code})" >&2
  return 1
}

if [[ "$JDBC_SINK_ONLY" == "1" ]]; then
  echo "JDBC sink only: refreshing jdbc-sink-demo (pg-source-demo left as-is)..." >&2
  remove_connector "jdbc-sink-demo"
else
  echo "Removing existing jdbc-sink-demo / pg-source-demo if present..."
  remove_connector "jdbc-sink-demo"
  remove_connector "pg-source-demo"
fi

post_connector() {
  local label="$1"
  local tmp resp code
  tmp="$(mktemp)"
  cat >"$tmp"
  resp="$(mktemp)"
  code="$(curl "${curl_connect_opts[@]}" -o "$resp" -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    --data-binary @"$tmp" \
    "$CONNECT/connectors")"
  rm -f "$tmp"
  if [[ "$code" != "201" && "$code" != "200" ]]; then
    echo "Failed to register ${label} (HTTP ${code}). Response:" >&2
    cat "$resp" >&2
    echo >&2
    rm -f "$resp"
    return 1
  fi
  cat "$resp"
  echo
  rm -f "$resp"
  return 0
}

if [[ "$JDBC_SINK_ONLY" != "1" ]]; then
  post_connector "pg-source-demo" <<JSON
{
  "name": "pg-source-demo",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "${PG_SOURCE_DB_HOST}",
    "database.port": "${PG_SOURCE_DB_PORT}",
    "database.user": "${PG_SOURCE_DB_USER}",
    "database.password": "${PG_SOURCE_DB_PASSWORD}",
    "database.dbname": "${PG_SOURCE_DB_NAME}",
    "topic.prefix": "demopg",
    "table.include.list": "public.demo_items",
    "snapshot.fetch.size": "256",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_demopg",
    "schema.history.internal.kafka.bootstrap.servers": "${SCHEMA_HISTORY_KAFKA_BOOTSTRAP}",
    "schema.history.internal.kafka.topic": "demopg.schema-changes.internal",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "producer.override.max.request.size": "33554432",
    "producer.override.buffer.memory": "67108864"
  }
}
JSON

  echo "Registered pg-source-demo."
fi

post_connector "jdbc-sink-demo" <<JSON
{
  "name": "jdbc-sink-demo",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "demopg.public.demo_items",
    "connection.url": "${JDBC_SINK_URL}",
    "connection.username": "${JDBC_SINK_USER}",
    "connection.password": "${JDBC_SINK_PASSWORD}",
    "dialect.name": "postgresql",
    "insert.mode": "upsert",
    "delete.enabled": "false",
    "primary.key.mode": "record_key",
    "primary.key.fields": "id",
    "table.name.format": "demo_items_from_kafka",
    "auto.create": "true",
    "schema.evolution": "basic",
    "quote.identifiers": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "consumer.override.fetch.max.bytes": "33554432",
    "consumer.override.max.partition.fetch.bytes": "33554432",
    "errors.log.enable": "true"
  }
}
JSON

echo "Registered jdbc-sink-demo."
echo "Check: curl -s $CONNECT/connectors | tr ',' '\n'"
