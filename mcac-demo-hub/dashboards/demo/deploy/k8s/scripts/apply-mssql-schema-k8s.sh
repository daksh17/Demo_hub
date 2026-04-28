#!/usr/bin/env bash
# Apply publisher + subscriber SQL (creates database "demo", mirror table, CDC) when the
# publisher only shows master/model/msdb/tempdb — e.g. mssql-demo-bootstrap never completed.
#
# Usage (from dashboards/demo):
#   ./deploy/k8s/scripts/apply-mssql-schema-k8s.sh
#
# Then register connectors (port-forward Connect :8083):
#   DEMO_HUB_K8S=1 ./deploy/docker/mssql-kafka/register-mssql-connectors.sh http://127.0.0.1:8083
#
set -euo pipefail
NS="${NS:-demo-hub}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PUB_SQL="$ROOT/deploy/docker/mssql-kafka/01-publisher-schema.sql"
SUB_SQL="$ROOT/deploy/docker/mssql-kafka/02-subscriber-schema.sql"

for f in "$PUB_SQL" "$SUB_SQL"; do
  [[ -f "$f" ]] || { echo "Missing $f" >&2; exit 1; }
done

run_sql() {
  local deploy_name="$1" sql_file="$2"
  echo "Applying $(basename "$sql_file") → $deploy_name ($NS)..."
  kubectl exec -i -n "$NS" "deploy/$deploy_name" -- bash -c '
for c in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
  [[ -x "$c" ]] || continue
  exec "$c" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b
done
echo "sqlcmd not found in container" >&2
exit 1
' < "$sql_file"
  echo "OK: $deploy_name"
}

run_sql mssql-publisher "$PUB_SQL"
run_sql mssql-subscriber "$SUB_SQL"
echo "Done. Verify: ./deploy/k8s/scripts/verify-mssql-publisher-demo.sh (expect database demo)."
