#!/usr/bin/env bash
# Debezium: PostgreSQL -> Kafka (CDC). Debezium JDBC sink: Kafka -> PostgreSQL (separate table).
# On failure, prints Kafka Connect JSON error (older versions used curl -f and hid the message).
#
# Prerequisites: docker compose up -d zookeeper kafka postgresql-primary postgresql-replica-1 postgresql-replica-2 kafka-connect
# Usage: ./register-connectors.sh [kafka-connect-url]
set -euo pipefail
CONNECT="${1:-http://localhost:8083}"
CONNECT="${CONNECT%/}"

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

curl -s -X DELETE "$CONNECT/connectors/jdbc-sink-demo" >/dev/null 2>&1 || true
curl -s -X DELETE "$CONNECT/connectors/pg-source-demo" >/dev/null 2>&1 || true
sleep 2

post_connector() {
  local label="$1"
  local tmp resp code
  tmp="$(mktemp)"
  cat >"$tmp"
  resp="$(mktemp)"
  code="$(curl -sS -o "$resp" -w '%{http_code}' -X POST \
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

post_connector "pg-source-demo" <<'JSON'
{
  "name": "pg-source-demo",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgresql-primary",
    "database.port": "5432",
    "database.user": "replicator",
    "database.password": "replicatorpass",
    "database.dbname": "demo",
    "topic.prefix": "demopg",
    "table.include.list": "public.demo_items",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_demopg",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "demopg.schema-changes.internal",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true"
  }
}
JSON

echo "Registered pg-source-demo."

post_connector "jdbc-sink-demo" <<'JSON'
{
  "name": "jdbc-sink-demo",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "demopg.public.demo_items",
    "connection.url": "jdbc:postgresql://postgresql-primary:5432/demo",
    "connection.username": "demo",
    "connection.password": "demopass",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "id",
    "table.name.format": "demo_items_from_kafka",
    "auto.create": "true",
    "schema.evolution": "basic",
    "quote.identifiers": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true"
  }
}
JSON

echo "Registered jdbc-sink-demo."
echo "Check: curl -s $CONNECT/connectors | tr ',' '\n'"
