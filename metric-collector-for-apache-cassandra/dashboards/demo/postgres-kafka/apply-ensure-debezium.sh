#!/usr/bin/env bash
# Patch primary for CDC when the volume predates replicator grants / publication (run from dashboards/demo).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
docker compose exec -T postgresql-primary \
  env PGPASSWORD=postgres psql -U postgres -d demo -v ON_ERROR_STOP=1 -f - \
  < "$(dirname "$0")/ensure-debezium-cdc.sql"
echo "OK. Next: ./postgres-kafka/register-connectors.sh"
