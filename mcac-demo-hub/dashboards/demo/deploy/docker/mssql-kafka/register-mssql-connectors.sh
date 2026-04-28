#!/usr/bin/env bash
# Debezium SQL Server CDC (publisher) + JDBC sink (subscriber). Requires kafka-connect image with SQL Server plugin.
# Usage: ./register-mssql-connectors.sh [http://kafka-connect:8083]
#
# If registration fails with "Cannot open database \"demo\"", the publisher has no demo DB yet
# (bootstrap not finished) or MSSQL_SA_PASSWORD here does not match the server / K8s secret.
set -euo pipefail
set +H 2>/dev/null || true
CONNECT="${1:-http://localhost:8083}"
CONNECT="${CONNECT%/}"
SCHEMA_HISTORY_KAFKA_BOOTSTRAP="${SCHEMA_HISTORY_KAFKA_BOOTSTRAP:-kafka:29092}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-Demo_hub_Mssql_2025!}"
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-5}"
CURL_MAX_TIME="${CURL_MAX_TIME:-120}"
curl_opts=(--connect-timeout "${CURL_CONNECT_TIMEOUT}" --max-time "${CURL_MAX_TIME}" -sS)

remove_connector() {
  local name="$1" code i
  code="$(curl "${curl_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
  [[ "$code" == "404" ]] && return 0
  curl "${curl_opts[@]}" -o /dev/null -X PUT "$CONNECT/connectors/$name/pause" 2>/dev/null || true
  sleep 2
  curl "${curl_opts[@]}" -o /dev/null -X DELETE "$CONNECT/connectors/$name" 2>/dev/null || true
  for i in $(seq 1 90); do
    code="$(curl "${curl_opts[@]}" -o /dev/null -w '%{http_code}' "$CONNECT/connectors/$name" 2>/dev/null || echo 000)"
    [[ "$code" == "404" ]] && return 0
    sleep 1
  done
  return 1
}

post_connector() {
  local label="$1" tmp resp code
  tmp="$(mktemp)"
  cat >"$tmp"
  resp="$(mktemp)"
  code="$(curl "${curl_opts[@]}" -o "$resp" -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' --data-binary @"$tmp" "$CONNECT/connectors")"
  rm -f "$tmp"
  if [[ "$code" != "201" && "$code" != "200" ]]; then
    echo "Failed to register ${label} (HTTP ${code}). Response:" >&2
    cat "$resp" >&2
    rm -f "$resp"
    return 1
  fi
  cat "$resp"
  echo
  rm -f "$resp"
}

mssql_source_register_hint() {
  cat >&2 <<'HINT'

After "Cannot open database \"demo\"" / login failed when validating mssql-source-demo:
  1) Ensure the publisher has database demo (and CDC): finish Job mssql-demo-bootstrap / Compose
     mssql-publisher-schema-init, or run 01-publisher-schema.sql against mssql-publisher.
  2) MSSQL_SA_PASSWORD in this shell must match the publisher (K8s: demo-hub-credentials key
     mssql-sa-password; default Demo_hub_Mssql_2025!).
  3) Kubernetes: kubectl logs -n demo-hub job/mssql-demo-bootstrap --tail=100
     and: ./deploy/k8s/scripts/verify-mssql-publisher-demo.sh
HINT
}

post_mssql_source_with_retries() {
  local attempt
  for attempt in $(seq 1 8); do
    if post_connector "mssql-source-demo" <<JSON
{
  "name": "mssql-source-demo",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "tasks.max": "1",
    "database.hostname": "mssql-publisher",
    "database.port": "1433",
    "database.user": "sa",
    "database.password": "${MSSQL_SA_PASSWORD}",
    "database.names": "demo",
    "topic.prefix": "demomssql",
    "table.include.list": "dbo.scenario_catalog_mirror_mssql",
    "schema.history.internal.kafka.bootstrap.servers": "${SCHEMA_HISTORY_KAFKA_BOOTSTRAP}",
    "schema.history.internal.kafka.topic": "demomssql.schema-changes.internal",
    "database.encrypt": "false",
    "database.trustServerCertificate": "true",
    "snapshot.mode": "initial",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true"
  }
}
JSON
    then
      return 0
    fi
    if [[ "$attempt" -lt 8 ]]; then
      echo "mssql-source-demo registration failed (attempt ${attempt}/8); retrying in 15s…" >&2
      sleep 15
    fi
  done
  mssql_source_register_hint
  return 1
}

echo "Removing existing mssql-source-demo / mssql-jdbc-sink-demo if present..."
remove_connector "mssql-jdbc-sink-demo" || true
remove_connector "mssql-source-demo" || true

post_mssql_source_with_retries

echo "Registered mssql-source-demo."

post_connector "mssql-jdbc-sink-demo" <<JSON
{
  "name": "mssql-jdbc-sink-demo",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "demomssql.dbo.scenario_catalog_mirror_mssql",
    "connection.url": "jdbc:sqlserver://mssql-subscriber:1433;databaseName=demo;encrypt=false;trustServerCertificate=true",
    "connection.username": "sa",
    "connection.password": "${MSSQL_SA_PASSWORD}",
    "dialect.name": "sqlserver",
    "insert.mode": "upsert",
    "delete.enabled": "false",
    "primary.key.mode": "record_key",
    "primary.key.fields": "id",
    "table.name.format": "dbo.demo_items_from_kafka_mssql",
    "auto.create": "false",
    "schema.evolution": "basic",
    "quote.identifiers": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "drop",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "errors.log.enable": "true"
  }
}
JSON

echo "Registered mssql-jdbc-sink-demo -> mssql-subscriber.demo.dbo.demo_items_from_kafka_mssql"
echo "Check: curl -s $CONNECT/connectors"
