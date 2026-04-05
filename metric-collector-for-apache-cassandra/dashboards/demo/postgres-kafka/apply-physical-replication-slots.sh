#!/usr/bin/env bash
# Create physical replication slots on an existing primary (volume predates slot init).
# Uses replication user `replicator`: pg_create_physical_replication_slot requires REPLICATION (non-replication roles cannot).
# Run from dashboards/demo after updating compose with primary_slot_name on replicas; then recreate replicas if needed.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
docker compose exec -T postgresql-primary \
  env PGPASSWORD=replicatorpass psql -U replicator -d postgres -v ON_ERROR_STOP=1 -f - \
  < "$(dirname "$0")/ensure-physical-replication-slots.sql"
echo "OK. Verify: docker compose exec -T postgresql-primary env PGPASSWORD=postgres psql -U postgres -d demo -c \"SELECT slot_name, slot_type, active FROM pg_replication_slots WHERE slot_name LIKE 'pgdemo_phys_%';\""
echo "If replicas were already running without slots, recreate: docker compose up -d --force-recreate postgresql-replica-1 postgresql-replica-2"
