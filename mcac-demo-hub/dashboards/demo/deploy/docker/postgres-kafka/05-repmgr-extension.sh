#!/usr/bin/env bash
# First init only (docker-entrypoint-initdb.d). Creates repmgr DB + extension for repmgr CLI / monitoring.
# This demo still uses Bitnami streaming replication; full repmgr failover would need repmgr.conf + node registration.
set -euo pipefail
if ! psql -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'repmgr'" | grep -q 1; then
  psql -v ON_ERROR_STOP=1 -U postgres -d postgres -c "CREATE DATABASE repmgr;"
fi
psql -v ON_ERROR_STOP=1 -U postgres -d repmgr -c "CREATE EXTENSION IF NOT EXISTS repmgr;"
