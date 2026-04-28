#!/usr/bin/env bash
# List databases on mssql-publisher (Kubernetes) using sqlcmd inside the pod.
# Use when register-mssql-connectors.sh fails with "Cannot open database \"demo\"".
set -euo pipefail
NS="${NS:-demo-hub}"
kubectl exec -n "$NS" deploy/mssql-publisher -- bash -c '
for c in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
  if [[ -x "$c" ]]; then
    exec "$c" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT name FROM sys.databases ORDER BY name"
  fi
done
echo "sqlcmd not found in container" >&2
exit 1
'
