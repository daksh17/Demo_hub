#!/usr/bin/env bash
# Run during docker-entrypoint-initdb.d as a shell script so we can use the replication user.
# pg_create_physical_replication_slot requires REPLICATION (or superuser); this hook runs the SQL as `replicator`.
set -euo pipefail
export PGPASSWORD="${POSTGRESQL_REPLICATION_PASSWORD:?}"
exec psql -v ON_ERROR_STOP=1 -U "${POSTGRESQL_REPLICATION_USER:?}" -d postgres \
  -f /tmp/ensure-physical-replication-slots.sql
