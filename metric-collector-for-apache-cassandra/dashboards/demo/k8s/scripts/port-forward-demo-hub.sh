#!/usr/bin/env bash
# Expose demo-hub ClusterIP services on localhost (kubectl port-forward).
# Run from anywhere; default namespace demo-hub. Leave this terminal open while you use the UIs/clients.
#
# You do not need this if you already reach the stack via Ingress (see generated/96-kubernetes-ops.yaml)
# or only use in-cluster clients / NodePort (e.g. optional-cassandra-0-cql-nodeport.yaml).
#
# If you see "Connection refused" / socat errors on :9090 (or :3000, …), nothing is listening
# inside that pod yet — usually CrashLoopBackOff or not Ready. Check: kubectl get pods -n demo-hub;
# kubectl logs deploy/prometheus -n demo-hub --tail=50. To forward everything except Prometheus
# while you fix it: SKIP_PROMETHEUS=1 ./k8s/scripts/port-forward-demo-hub.sh
#
# Cassandra: forward **pod/cassandra-0**, not svc/cassandra — the Service load-balances 3 replicas;
# CQL + port-forward (socat) often hits "Connection reset by peer" / lost connection when the
# backend pod changes. One stable pod fixes cqlsh and DBeaver.
#
# Override local ports if something is already bound, e.g.:
#   LOCAL_PG_PORT=15432 LOCAL_PROM_PORT=19090 LOCAL_REDIS_PORT=16379 ./k8s/scripts/port-forward-demo-hub.sh
set -euo pipefail
NS="${NS:-demo-hub}"

LOCAL_PG_PORT="${LOCAL_PG_PORT:-5432}"
LOCAL_CQL_PORT="${LOCAL_CQL_PORT:-9042}"
LOCAL_MONGO_PORT="${LOCAL_MONGO_PORT:-27017}"
LOCAL_REDIS_PORT="${LOCAL_REDIS_PORT:-6379}"
LOCAL_GRAFANA_PORT="${LOCAL_GRAFANA_PORT:-3000}"
LOCAL_PROM_PORT="${LOCAL_PROM_PORT:-9090}"
LOCAL_HUB_UI_PORT="${LOCAL_HUB_UI_PORT:-8888}"
# Hub UI / tools use localhost:9200 / :5601 for OpenSearch REST + Dashboards (same defaults as Compose).
LOCAL_OPENSEARCH_PORT="${LOCAL_OPENSEARCH_PORT:-9200}"
LOCAL_OS_DASHBOARDS_PORT="${LOCAL_OS_DASHBOARDS_PORT:-5601}"

echo "Namespace: $NS"
echo "Open in browser or point clients at 127.0.0.1 — keep this process running (Ctrl+C stops all forwards)."
echo ""
echo "  PostgreSQL    http://127.0.0.1 (psql)     port ${LOCAL_PG_PORT}   user=demo password=demopass db=demo"
echo "  Cassandra CQL cqlsh 127.0.0.1 ${LOCAL_CQL_PORT}   (pod/cassandra-0; keyspace demo_hub after bootstrap Job)"
echo "  MongoDB       mongosh mongodb://127.0.0.1:${LOCAL_MONGO_PORT}/"
echo "  Redis         127.0.0.1:${LOCAL_REDIS_PORT}   (password demoredispass; URI redis://:demoredispass@127.0.0.1:${LOCAL_REDIS_PORT}/0)"
echo "  Grafana       http://127.0.0.1:${LOCAL_GRAFANA_PORT}/"
echo "  Prometheus    http://127.0.0.1:${LOCAL_PROM_PORT}/"
echo "  Hub demo UI   http://127.0.0.1:${LOCAL_HUB_UI_PORT}/"
echo "  OpenSearch    http://127.0.0.1:${LOCAL_OPENSEARCH_PORT}/  (REST API; e.g. curl -s http://127.0.0.1:${LOCAL_OPENSEARCH_PORT}/_cluster/health)"
echo "  OS Dashboards http://127.0.0.1:${LOCAL_OS_DASHBOARDS_PORT}/  (matches “OpenSearch Dashboards” link on hub home)"
echo ""

kubectl -n "$NS" port-forward "svc/postgresql-primary" "${LOCAL_PG_PORT}:5432" &
kubectl -n "$NS" port-forward "pod/cassandra-0" "${LOCAL_CQL_PORT}:9042" &
kubectl -n "$NS" port-forward "svc/mongo-mongos1" "${LOCAL_MONGO_PORT}:27017" &
kubectl -n "$NS" port-forward "svc/redis" "${LOCAL_REDIS_PORT}:6379" &
kubectl -n "$NS" port-forward "svc/grafana" "${LOCAL_GRAFANA_PORT}:3000" &
if [[ "${SKIP_PROMETHEUS:-}" == "1" ]]; then
  echo "SKIP_PROMETHEUS=1 — not forwarding svc/prometheus (fix the pod, then re-run without this)." >&2
else
  kubectl -n "$NS" port-forward "svc/prometheus" "${LOCAL_PROM_PORT}:9090" &
fi
kubectl -n "$NS" port-forward "svc/hub-demo-ui" "${LOCAL_HUB_UI_PORT}:8888" &
kubectl -n "$NS" port-forward "svc/opensearch" "${LOCAL_OPENSEARCH_PORT}:9200" &
kubectl -n "$NS" port-forward "svc/opensearch-dashboards" "${LOCAL_OS_DASHBOARDS_PORT}:5601" &
wait
