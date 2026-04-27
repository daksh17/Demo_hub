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
# while you fix it: SKIP_PROMETHEUS=1 ./deploy/k8s/scripts/port-forward-demo-hub.sh
#
# Cassandra: forward **pod/cassandra-0**, not svc/cassandra — the Service load-balances 3 replicas;
# CQL + port-forward (socat) often hits "Connection reset by peer" / lost connection when the
# backend pod changes. One stable pod fixes cqlsh and DBeaver.
#
# Kafka Connect (REST :8083): the **worker** runs as deploy/kafka-connect; the **four** demo connectors
# (Postgres Debezium + JDBC sink, Mongo Debezium + Mongo sink) are registered via HTTP to Connect’s
# REST API — unlike Compose, K8s does **not** run kafka-connect-register automatically. After forward:
#   curl -s http://127.0.0.1:${LOCAL_KAFKA_CONNECT_PORT:-8083}/connectors
#   curl -s http://127.0.0.1:8083/connectors/<name>/status
# Register from dashboards/demo:
#   DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all.sh http://127.0.0.1:8083
#
# Override local ports if something is already bound, e.g.:
#   LOCAL_PG_PORT=15432 LOCAL_PROM_PORT=19090 LOCAL_REDIS_PORT=16379 LOCAL_KAFKA_CONNECT_PORT=18083 ./deploy/k8s/scripts/port-forward-demo-hub.sh
set -euo pipefail
NS="${NS:-demo-hub}"

LOCAL_PG_PORT="${LOCAL_PG_PORT:-5432}"
LOCAL_PG_REPLICA_1_PORT="${LOCAL_PG_REPLICA_1_PORT:-5433}"
LOCAL_PG_REPLICA_2_PORT="${LOCAL_PG_REPLICA_2_PORT:-5434}"
LOCAL_CQL_PORT="${LOCAL_CQL_PORT:-9042}"
LOCAL_MONGO_PORT="${LOCAL_MONGO_PORT:-27017}"
LOCAL_REDIS_PORT="${LOCAL_REDIS_PORT:-6379}"
LOCAL_GRAFANA_PORT="${LOCAL_GRAFANA_PORT:-3000}"
LOCAL_PROM_PORT="${LOCAL_PROM_PORT:-9090}"
LOCAL_HUB_UI_PORT="${LOCAL_HUB_UI_PORT:-8888}"
LOCAL_KAFKA_CONNECT_PORT="${LOCAL_KAFKA_CONNECT_PORT:-8083}"
# Hub UI / tools use localhost:9200 / :5601 for OpenSearch REST + Dashboards (same defaults as Compose).
LOCAL_OPENSEARCH_PORT="${LOCAL_OPENSEARCH_PORT:-9200}"
LOCAL_OS_DASHBOARDS_PORT="${LOCAL_OS_DASHBOARDS_PORT:-5601}"
LOCAL_VAULT_PORT="${LOCAL_VAULT_PORT:-8200}"

echo "Namespace: $NS"
echo "Open in browser or point clients at 127.0.0.1 — keep this process running (Ctrl+C stops all forwards)."
echo ""
echo "  PostgreSQL primary   127.0.0.1:${LOCAL_PG_PORT}   user=demo password=demopass db=demo  (superuser: postgres / postgres)"
echo "  PostgreSQL replica-1 127.0.0.1:${LOCAL_PG_REPLICA_1_PORT}  (read-only; same users as primary)"
echo "  PostgreSQL replica-2 127.0.0.1:${LOCAL_PG_REPLICA_2_PORT}  (read-only)"
echo "  Cassandra CQL cqlsh 127.0.0.1 ${LOCAL_CQL_PORT}   (pod/cassandra-0; keyspace demo_hub after bootstrap Job)"
echo "  MongoDB       mongosh mongodb://127.0.0.1:${LOCAL_MONGO_PORT}/"
echo "  Redis         127.0.0.1:${LOCAL_REDIS_PORT}   (password demoredispass; URI redis://:demoredispass@127.0.0.1:${LOCAL_REDIS_PORT}/0)"
echo "  Grafana       http://127.0.0.1:${LOCAL_GRAFANA_PORT}/"
echo "  Prometheus    http://127.0.0.1:${LOCAL_PROM_PORT}/"
echo "  Hub demo UI   http://127.0.0.1:${LOCAL_HUB_UI_PORT}/  (Faker + map orders: /scenario step 3)"
echo "  Kafka Connect http://127.0.0.1:${LOCAL_KAFKA_CONNECT_PORT}/  (REST; list: curl -s http://127.0.0.1:${LOCAL_KAFKA_CONNECT_PORT}/connectors)"
echo "  OpenSearch    http://127.0.0.1:${LOCAL_OPENSEARCH_PORT}/  (REST API; e.g. curl -s http://127.0.0.1:${LOCAL_OPENSEARCH_PORT}/_cluster/health)"
echo "  OS Dashboards http://127.0.0.1:${LOCAL_OS_DASHBOARDS_PORT}/  (matches “OpenSearch Dashboards” link on hub home)"
echo "  Vault UI/API http://127.0.0.1:${LOCAL_VAULT_PORT}/  (token: demo-hub-dev-root — demo dev mode only)"
echo ""

# Remote port is always 5432 (container + Service targetPort). Local ports differ so primary + replicas can all bind.
kubectl -n "$NS" port-forward "svc/postgresql-primary" "${LOCAL_PG_PORT}:5432" &
kubectl -n "$NS" port-forward "svc/postgresql-replica-1" "${LOCAL_PG_REPLICA_1_PORT}:5432" &
kubectl -n "$NS" port-forward "svc/postgresql-replica-2" "${LOCAL_PG_REPLICA_2_PORT}:5432" &
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
kubectl -n "$NS" port-forward "svc/kafka-connect" "${LOCAL_KAFKA_CONNECT_PORT}:8083" &
kubectl -n "$NS" port-forward "svc/opensearch" "${LOCAL_OPENSEARCH_PORT}:9200" &
kubectl -n "$NS" port-forward "svc/opensearch-dashboards" "${LOCAL_OS_DASHBOARDS_PORT}:5601" &
kubectl -n "$NS" port-forward "svc/vault" "${LOCAL_VAULT_PORT}:8200" &

wait
