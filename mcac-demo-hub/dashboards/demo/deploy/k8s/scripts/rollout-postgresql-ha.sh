#!/usr/bin/env bash
# Restart Bitnami PostgreSQL HA Deployments (e.g. after building mcac-demo/postgresql-repmgr:16.6.0).
set -euo pipefail
NS="${NS:-demo-hub}"
for d in postgresql-primary postgresql-replica-1 postgresql-replica-2; do
  kubectl rollout restart "deployment/${d}" -n "${NS}"
done
for d in postgresql-primary postgresql-replica-1 postgresql-replica-2; do
  kubectl rollout status "deployment/${d}" -n "${NS}" --timeout=300s
done
echo "Rollout complete for ${NS} PostgreSQL HA."
