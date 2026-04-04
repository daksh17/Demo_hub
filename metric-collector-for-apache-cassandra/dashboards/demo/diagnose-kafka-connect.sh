#!/usr/bin/env bash
# Quick checks when demo_items_from_kafka / demo.demo_items_from_kafka stay empty after workload.
# Run from dashboards/demo with the stack up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

CONNECT="${1:-http://localhost:8083}"
CONNECT="${CONNECT%/}"

echo "=== 1. Kafka Connect REST ($CONNECT) ==="
if ! curl -sf "${CONNECT}/connectors" >/dev/null; then
  echo "FAIL: cannot reach ${CONNECT} (is kafka-connect up and port 8083 published?)"
  exit 1
fi

echo "Connectors:"
curl -s "${CONNECT}/connectors"
echo ""

for name in pg-source-demo jdbc-sink-demo mongo-source-demo mongo-sink-demo; do
  echo ""
  echo "=== 2. Status: $name ==="
  code="$(curl -s -o /tmp/kc-status.json -w '%{http_code}' "${CONNECT}/connectors/${name}/status")"
  if [[ "$code" != "200" ]]; then
    echo "HTTP $code — connector may not be registered. Run: ./kafka-connect-register/register-all.sh"
    continue
  fi
  cat /tmp/kc-status.json
  echo ""
  if grep -q '"state":"FAILED"' /tmp/kc-status.json 2>/dev/null; then
    echo "^^ FAILED — see docker compose logs kafka-connect"
  fi
done
rm -f /tmp/kc-status.json

echo ""
echo "=== 3. Kafka topics (demopg / demomongo) ==="
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -E '^demopg\.|^demomongo\.' || echo "(none — source connectors may not be running or no events yet)"

echo ""
echo "=== 4. Hint ==="
echo "Workload writes public.demo_items and demo.demo_items. Sinks copy from Kafka topics:"
echo "  demopg.public.demo_items  → Postgres demo_items_from_kafka (jdbc-sink-demo)"
echo "  demomongo.demo.demo_items → Mongo demo.demo_items_from_kafka (mongo-sink-demo)"
echo "If connectors list is [] or tasks FAILED, re-register: ./kafka-connect-register/register-all.sh"
echo "RecordTooLargeException on mongo-source / pg-source: restart Kafka + Connect after raising KAFKA_MESSAGE_MAX_BYTES,"
echo "then re-register connectors (see docker-compose.yml + register-*.sh producer.override / consumer.override)."
