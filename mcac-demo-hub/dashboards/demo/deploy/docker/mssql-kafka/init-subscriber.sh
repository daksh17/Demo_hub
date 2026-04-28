#!/usr/bin/env bash
set -euo pipefail
PW="${MSSQL_SA_PASSWORD:?missing MSSQL_SA_PASSWORD}"
for i in $(seq 1 60); do
  if sqlcmd -S mssql-subscriber -U sa -P "$PW" -C -Q "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
sqlcmd -S mssql-subscriber -U sa -P "$PW" -C -b -i /scripts/02-subscriber-schema.sql
echo "mssql-subscriber schema OK."
