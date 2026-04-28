#!/usr/bin/env bash
# Resume every connector on this Connect worker (no re-POST of config).
# Use when connectors exist but are PAUSED. Does not create missing connectors.
#
# Usage:
#   ./resume-kafka-connectors.sh [http://127.0.0.1:8083]
#
set -euo pipefail
CONNECT="${1:-${KAFKA_CONNECT_URL:-http://127.0.0.1:8083}}"
CONNECT="${CONNECT%/}"

if ! curl -sf "${CONNECT}/connectors" >/dev/null 2>&1; then
  echo "Kafka Connect not reachable at ${CONNECT}" >&2
  exit 1
fi

list_json="$(curl -sS "${CONNECT}/connectors")"
names="$(echo "$list_json" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(x) for x in d]" 2>/dev/null || true)"
if [[ -z "${names}" ]]; then
  echo "No connectors returned from ${CONNECT}/connectors"
  exit 0
fi

while IFS= read -r name; do
  [[ -z "$name" ]] && continue
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "${CONNECT}/connectors/${name}/resume")"
  echo "resume ${name} → HTTP ${code}"
done <<< "$names"
