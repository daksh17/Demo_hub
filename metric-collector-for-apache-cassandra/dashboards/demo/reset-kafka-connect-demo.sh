#!/usr/bin/env bash
# Remove every connector from Kafka Connect (sinks first), drop/recreate sink tables, then register
# Postgres + Mongo CDC/sink connectors (same as kafka-connect-register/register-all.sh).
#
# Run from dashboards/demo with the stack up:
#   chmod +x reset-kafka-connect-demo.sh
#   ./reset-kafka-connect-demo.sh
#   ./reset-kafka-connect-demo.sh http://127.0.0.1:8083
#
# Skip dropping Postgres/Mongo sink tables (connectors only):
#   SKIP_CLEAN_SINKS=1 ./reset-kafka-connect-demo.sh
set -euo pipefail

CONNECT="${1:-http://127.0.0.1:8083}"
CONNECT="${CONNECT%/}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-5}"
CURL_MAX_TIME="${CURL_MAX_TIME:-30}"
curl_connect_opts=(--connect-timeout "${CURL_CONNECT_TIMEOUT}" --max-time "${CURL_MAX_TIME}" -sS)

wait_for() {
  echo "Waiting for Kafka Connect..."
  for _ in $(seq 1 90); do
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

remove_all_listed_connectors() {
  local raw
  raw="$(curl "${curl_connect_opts[@]}" -sS "${CONNECT}/connectors" 2>/dev/null || true)"
  if [[ -z "$raw" || "$raw" == "[]" ]]; then
    echo "No connectors registered on ${CONNECT}."
    return 0
  fi
  echo "Removing all connectors reported by GET /connectors (sink names first)..."
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    remove_connector "$name"
  done < <(printf '%s' "$raw" | python3 -c "
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    sys.exit(0)
j = json.loads(raw)
if not j:
    sys.exit(0)
sink = [n for n in j if 'sink' in n.lower()]
rest = [n for n in j if n not in sink]
for n in sink + rest:
    print(n)
")
}

wait_for
remove_all_listed_connectors

if [[ "${SKIP_CLEAN_SINKS:-0}" != "1" ]]; then
  echo "Cleaning sink tables (Postgres + Mongo)..."
  bash "${HERE}/clean-kafka-connect-sinks.sh"
else
  echo "SKIP_CLEAN_SINKS=1 — leaving sink tables as-is." >&2
fi

echo "Registering all demo connectors..."
bash "${HERE}/kafka-connect-register/register-all.sh" "${CONNECT}"

echo "Done. curl -s ${CONNECT}/connectors"
