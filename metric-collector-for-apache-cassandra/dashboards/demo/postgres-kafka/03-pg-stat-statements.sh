#!/usr/bin/env bash
# docker-entrypoint-initdb.d: enable pg_stat_statements in the maintenance DB `postgres`.
# Requires POSTGRESQL_EXTRA_FLAGS to preload pg_stat_statements (see docker-compose.yml).
set -euo pipefail
export PGPASSWORD="${POSTGRESQL_PASSWORD:?}"
exec psql -v ON_ERROR_STOP=1 -U "${POSTGRESQL_USERNAME:?}" -d postgres \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
