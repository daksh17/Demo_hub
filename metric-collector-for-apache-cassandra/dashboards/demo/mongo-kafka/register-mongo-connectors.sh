#!/usr/bin/env bash
# Debezium MongoDB source (CDC) -> Kafka; MongoDB Kafka sink -> demo.demo_items_from_kafka.
# Requires: kafka-connect image built from Dockerfile.connect, mongo stack + mongo-kafka-prepare done.
#
# Usage: ./register-mongo-connectors.sh [http://localhost:8083]
set -euo pipefail
CONNECT="${1:-http://localhost:8083}"
CONNECT="${CONNECT%/}"
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

remove_connector() {
  local name="$1" code i
  code="$(curl "${curl_connect_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
  if [[ "$code" == "404" ]]; then
    echo "  ${name}: not registered, skip." >&2
    return 0
  fi
  echo "  ${name}: pausing, then DELETE; waiting for Connect (up to ~120s)..." >&2
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

wait_for

echo "Removing existing mongo-sink-demo / mongo-source-demo if present..."
remove_connector "mongo-sink-demo"
remove_connector "mongo-source-demo"

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
    "value.converter.schemas.enable": "true",
    "producer.override.max.request.size": "33554432",
    "producer.override.buffer.memory": "67108864"
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
    "document.id.strategy.partial.value.projection.list": "_id",
    "consumer.override.fetch.max.bytes": "33554432",
    "consumer.override.max.partition.fetch.bytes": "33554432"
  }
}
JSON

echo "Registered mongo-sink-demo -> demo.demo_items_from_kafka"
echo "Check: curl -s $CONNECT/connectors"
