#!/usr/bin/env bash
# Debezium MongoDB source (CDC) -> Kafka; MongoDB Kafka sink -> demo.demo_items_from_kafka.
# Requires: kafka-connect image built from Dockerfile.connect, mongo stack + mongo-kafka-prepare done.
#
# Usage: ./register-mongo-connectors.sh [http://localhost:8083]
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

curl -s -X DELETE "$CONNECT/connectors/mongo-sink-demo" >/dev/null 2>&1 || true
curl -s -X DELETE "$CONNECT/connectors/mongo-source-demo" >/dev/null 2>&1 || true
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

post_connector "mongo-source-demo" <<'JSON'
{
  "name": "mongo-source-demo",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "tasks.max": "1",
    "mongodb.connection.string": "mongodb://mongo-mongos1:27017",
    "topic.prefix": "demomongo",
    "collection.include.list": "demo.demo_items",
    "capture.mode": "change_streams_update_full",
    "snapshot.mode": "initial",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true"
  }
}
JSON

echo "Registered mongo-source-demo (topics like demomongo.demo.demo_items)."

post_connector "mongo-sink-demo" <<'JSON'
{
  "name": "mongo-sink-demo",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "demomongo.demo.demo_items",
    "connection.uri": "mongodb://mongo-mongos1:27017",
    "database": "demo",
    "collection": "demo_items_from_kafka",
    "max.num.retries": "3",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "transforms": "extractDoc",
    "transforms.extractDoc.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
    "transforms.extractDoc.delete.tombstone.handling.mode": "drop",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
    "document.id.strategy.partial.value.projection.type": "AllowList",
    "document.id.strategy.partial.value.projection.list": "_id"
  }
}
JSON

echo "Registered mongo-sink-demo -> demo.demo_items_from_kafka"
echo "Check: curl -s $CONNECT/connectors"
