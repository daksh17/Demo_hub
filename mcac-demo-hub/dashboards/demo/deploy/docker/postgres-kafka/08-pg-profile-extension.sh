#!/usr/bin/env bash
# First init only (docker-entrypoint-initdb.d on primary).
# pg_profile: historic workload samples/reports (https://github.com/zubkov-andrei/pg_profile).
# Requires pg_stat_statements (shared_preload_libraries) and dblink.
set -euo pipefail
export PGPASSWORD="${POSTGRESQL_PASSWORD:?}"
PGU="${POSTGRESQL_USERNAME:?}"
psql -v ON_ERROR_STOP=1 -U "$PGU" -d postgres -c "CREATE EXTENSION IF NOT EXISTS dblink;"
psql -v ON_ERROR_STOP=1 -U "$PGU" -d postgres -c "CREATE SCHEMA IF NOT EXISTS profile;"
psql -v ON_ERROR_STOP=1 -U "$PGU" -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_profile SCHEMA profile;"
