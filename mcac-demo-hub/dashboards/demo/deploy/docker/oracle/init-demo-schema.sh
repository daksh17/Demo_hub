#!/usr/bin/env bash
set -euo pipefail
DEMO_PW="${ORACLE_DEMO_PASSWORD:?missing ORACLE_DEMO_PASSWORD}"
SYS_PW="${ORACLE_SYS_PASSWORD:-}"
SVC="${ORACLE_HOST:-oracle}"
# Do not use ORACLE_PORT: Kubernetes injects ORACLE_PORT=tcp://<ip>:1521 for Service "oracle".
PORT="${ORACLE_LISTEN_PORT:-1521}"
SERVICE="${ORACLE_SERVICE:-FREEPDB1}"
DEMO_CSTR="demo/${DEMO_PW}@//${SVC}:${PORT}/${SERVICE}"

echo "Waiting for Oracle PDB ${SERVICE} at ${SVC}:${PORT} (user demo)..."
for i in $(seq 1 180); do
  if echo "SELECT 1 FROM DUAL;" | sqlplus -s -L "$DEMO_CSTR" 2>/dev/null | grep -qE '^[[:space:]]*1'; then
    echo "Oracle reachable (attempt $i)"
    break
  fi
  sleep 5
  if [[ "$i" -eq 180 ]]; then
    echo "timeout waiting for ${DEMO_CSTR}" >&2
    exit 1
  fi
done

for script in /scripts/01-demo-schema.sql /scripts/02-hub-scenario-schema.sql; do
  echo "Applying $(basename "$script") as demo..." >&2
  sqlplus -s -L "$DEMO_CSTR" @"$script"
done

if [[ -n "$SYS_PW" ]]; then
  SYS_CSTR="sys/${SYS_PW}@//${SVC}:${PORT}/${SERVICE} as sysdba"
  echo "Applying 03-exporter-grants.sql as SYS..." >&2
  sqlplus -s -L "$SYS_CSTR" @/scripts/03-exporter-grants.sql
else
  echo "ORACLE_SYS_PASSWORD unset; skipping 03-exporter-grants.sql (exporter may show limited metrics)." >&2
fi

echo "oracle-demo-bootstrap done."
