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
# MSSQL pods down: SKIP_MSSQL=1 ./deploy/k8s/scripts/port-forward-demo-hub.sh
# Mongo local port busy (e.g. host Mongo on 27017): script bumps LOCAL_MONGO_PORT until free, or set LOCAL_MONGO_PORT=27027
# Trino not deployed or pod pending: SKIP_TRINO=1 ./deploy/k8s/scripts/port-forward-demo-hub.sh
# Kafka broker PLAINTEXT :9092: forwarded to LOCAL_KAFKA_BROKER_PORT (default 9092). If Docker Compose
# Kafka already uses 9092 on the host, use SKIP_KAFKA_BROKER=1 or LOCAL_KAFKA_BROKER_PORT=19092
#
# Kafka UI / clients (common pitfalls):
# - Broker advertises PLAINTEXT://kafka:9092 — add "127.0.0.1 kafka" to Mac /etc/hosts for host-native clients.
# - Kafka UI running IN Docker must NOT use 127.0.0.1:9092 (that is the container itself). Use bootstrap
#   host.docker.internal:9092 (OrbStack / Docker Desktop). Map kafka to the host so metadata works, e.g.
#   Compose: extra_hosts: ["kafka:host-gateway"]
#
# Cassandra: forward **pod/cassandra-0**, not svc/cassandra — the Service load-balances 3 replicas;
# CQL + port-forward (socat) often hits "Connection reset by peer" / lost connection when the
# backend pod changes. One stable pod fixes cqlsh and DBeaver.
# Forwards bind with **--address $PORT_FORWARD_ADDR** (default 127.0.0.1 only) so clients use IPv4;
# dual-stack [::1] + socat (e.g. OrbStack) can reset CQL mid-handshake. Override: PORT_FORWARD_ADDR=localhost
# Newer kubectl defaults to **WebSocket** port-forward; that path can reset Cassandra native CQL on some
# runtimes. This script sets **KUBECTL_PORT_FORWARD_WEBSOCKETS=false** (pure SPDY) unless you export it to true.
# Two terminals: run this script in terminal 1 and leave it open. Cassandra CQL in terminal 2 should use
# **demo-hub-cqlsh.sh** (exec into the pod) unless you rely on localhost — env vars like SKIP_CASSANDRA_PORT_FORWARD
# apply only when **starting** this script in terminal 1, not when exported in another shell.
# OrbStack-like kubectl contexts skip localhost Cassandra forward by default (native CQL + kubectl PF often resets).
# Force localhost forward: CASSANDRA_KUBE_PORT_FORWARD=1 ./deploy/k8s/scripts/port-forward-demo-hub.sh
# Force skip on any cluster: SKIP_CASSANDRA_PORT_FORWARD=1 …   Force forward despite skip: SKIP_CASSANDRA_PORT_FORWARD=0 …
#
# Kafka Connect (REST :8083): the **worker** runs as deploy/kafka-connect; the **four** demo connectors
# (Postgres Debezium + JDBC sink, Mongo Debezium + Mongo sink) are registered via HTTP to Connect’s
# REST API — unlike Compose, K8s does **not** run kafka-connect-register automatically. After forward:
#   curl -s http://127.0.0.1:${LOCAL_KAFKA_CONNECT_PORT:-8083}/connectors
#   curl -s http://127.0.0.1:8083/connectors/<name>/status
# Register from dashboards/demo:
#   DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all.sh http://127.0.0.1:8083
#
# Local ports default to the **K8s demo-hub** range (16xxx–19xxx) so they do not clash with:
#   - Docker Compose publish ports (e.g. Postgres 15432–15434, Redis 6379, Mongo 27025, hub 8888)
#   - OS services on 5432 / 6379 / 27017 / 1433
# Override any LOCAL_* variable, e.g.:
#   LOCAL_PG_PORT=16432 LOCAL_REDIS_PORT=16379 ./deploy/k8s/scripts/port-forward-demo-hub.sh
# SQL Server: LOCAL_MSSQL_PUBLISHER_PORT / LOCAL_MSSQL_SUBSCRIBER_PORT (defaults 16331 / 16332).
set -euo pipefail
_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${NS:-demo-hub}"
_kctx="$(kubectl config current-context 2>/dev/null || echo "")"
_cassandra_pf_enabled=true
if [[ "${SKIP_CASSANDRA_PORT_FORWARD:-}" == "1" ]]; then
  _cassandra_pf_enabled=false
elif [[ "${SKIP_CASSANDRA_PORT_FORWARD:-}" == "0" ]] || [[ "${CASSANDRA_KUBE_PORT_FORWARD:-}" == "1" ]]; then
  _cassandra_pf_enabled=true
else
  case "$_kctx" in
    *orbstack* | *OrbStack*) _cassandra_pf_enabled=false ;;
  esac
fi
# IPv4-only bind for all forwards (avoids Cassandra cqlsh "Connection reset by peer" / lost connection on some Mac K8s runtimes).
PORT_FORWARD_ADDR="${PORT_FORWARD_ADDR:-127.0.0.1}"
# kubectl 1.31+ defaults to WebSocket port-forward; disable unless explicitly enabled (see header).
KUBECTL_PORT_FORWARD_WEBSOCKETS="${KUBECTL_PORT_FORWARD_WEBSOCKETS:-false}"
export KUBECTL_PORT_FORWARD_WEBSOCKETS

# --- K8s localhost port map (remote ports unchanged inside the cluster) ---
# Service              | Local default | Compose host (if also running) | Client example
# PostgreSQL primary   | 16432         | 15432                          | psql -h 127.0.0.1 -p 16432 -U demo -d demo
# PostgreSQL replica-1 | 16433         | 15433                          |
# PostgreSQL replica-2 | 16434         | 15434                          |
# postgres-sub         | 16435         | —                              |
# Redis                | 16379         | 6379                           | redis-cli -p 16379 -a demoredispass
# Mongo (mongos1)      | 16217         | 27025 (mongos1)                | mongosh mongodb://127.0.0.1:16217/
# SQL Server publisher | 16331         | 14331                          | sqlcmd -S 127.0.0.1,16331 …
# SQL Server subscriber| 16332         | 14332                          |
# Oracle (FREEPDB1)    | 16330         | —                              | sqlplus demo/demopass@//127.0.0.1:16330/FREEPDB1
# Hub demo UI          | 16888         | 8888                           | http://127.0.0.1:16888/
# OpenSearch REST      | 19200         | 9200                           |
# OS Dashboards        | 15601         | 5601                           |
# Cassandra CQL        | 19042         | 19442 (cassandra-1 in compose)   |
# Kafka broker         | 19092         | 9092                           |
# Kafka Connect        | 18083         | 8083                           |
# Trino                | 18088         | —                              |
# Grafana              | 13000         | 3000                           |
# Prometheus           | 19090         | 9090                           |
# Vault                | 18200         | —                              |
LOCAL_PG_PORT="${LOCAL_PG_PORT:-16432}"
LOCAL_PG_REPLICA_1_PORT="${LOCAL_PG_REPLICA_1_PORT:-16433}"
LOCAL_PG_REPLICA_2_PORT="${LOCAL_PG_REPLICA_2_PORT:-16434}"
LOCAL_PG_LOGICAL_SUB_PORT="${LOCAL_PG_LOGICAL_SUB_PORT:-16435}"
LOCAL_CQL_PORT="${LOCAL_CQL_PORT:-19042}"
LOCAL_MONGO_PORT="${LOCAL_MONGO_PORT:-16217}"
# Avoid "bind: address already in use" when the chosen LOCAL_MONGO_PORT is taken.
if command -v lsof >/dev/null 2>&1; then
  _mongo_orig="$LOCAL_MONGO_PORT"
  while lsof -nP -iTCP:"$LOCAL_MONGO_PORT" -sTCP:LISTEN >/dev/null 2>&1 && [[ "$LOCAL_MONGO_PORT" -lt $((_mongo_orig + 50)) ]]; do
    echo "Host port ${LOCAL_MONGO_PORT} busy; using $((LOCAL_MONGO_PORT + 1)) for Mongo forward (override with LOCAL_MONGO_PORT=…)." >&2
    LOCAL_MONGO_PORT=$((LOCAL_MONGO_PORT + 1))
  done
fi
LOCAL_REDIS_PORT="${LOCAL_REDIS_PORT:-16379}"
LOCAL_GRAFANA_PORT="${LOCAL_GRAFANA_PORT:-13000}"
LOCAL_PROM_PORT="${LOCAL_PROM_PORT:-19090}"
LOCAL_HUB_UI_PORT="${LOCAL_HUB_UI_PORT:-16888}"
LOCAL_KAFKA_CONNECT_PORT="${LOCAL_KAFKA_CONNECT_PORT:-18083}"
LOCAL_KAFKA_BROKER_PORT="${LOCAL_KAFKA_BROKER_PORT:-19092}"
LOCAL_OPENSEARCH_PORT="${LOCAL_OPENSEARCH_PORT:-19200}"
LOCAL_OS_DASHBOARDS_PORT="${LOCAL_OS_DASHBOARDS_PORT:-15601}"
LOCAL_VAULT_PORT="${LOCAL_VAULT_PORT:-18200}"
# SQL Server 2022 (publisher / subscriber); remote port always 1433.
LOCAL_MSSQL_PUBLISHER_PORT="${LOCAL_MSSQL_PUBLISHER_PORT:-16331}"
LOCAL_MSSQL_SUBSCRIBER_PORT="${LOCAL_MSSQL_SUBSCRIBER_PORT:-16332}"
LOCAL_ORACLE_PORT="${LOCAL_ORACLE_PORT:-16330}"
LOCAL_TRINO_PORT="${LOCAL_TRINO_PORT:-18088}"
# Matches demo-hub Secret demo-hub-credentials key mssql-sa-password unless you rotated it (override this echo).
MSSQL_SA_PASSWORD_HINT="${MSSQL_SA_PASSWORD_HINT:-Demo_hub_Mssql_2025!}"

echo "Namespace: $NS"
echo "kubectl context: ${_kctx:-unknown}"
echo "Port-forward bind: ${PORT_FORWARD_ADDR} (kubectl --address; override with PORT_FORWARD_ADDR=localhost if needed)"
echo "kubectl port-forward WebSockets: ${KUBECTL_PORT_FORWARD_WEBSOCKETS} (set KUBECTL_PORT_FORWARD_WEBSOCKETS=true for default kubectl behavior)"
echo "Open in browser or point clients at 127.0.0.1 — keep this process running (Ctrl+C stops all forwards)."
echo "Two terminals: use this window for forwards only; open a second shell for clients (see Cassandra line below)."
echo ""
echo "  (K8s forwards — Compose stack uses different host ports: PG 15432–15434, Redis 6379, Mongo 27025, hub 8888)"
echo "  PostgreSQL primary   127.0.0.1:${LOCAL_PG_PORT}   user=demo password=demopass db=demo  (superuser: postgres / postgres)"
echo "       psql \"postgresql://demo:demopass@127.0.0.1:${LOCAL_PG_PORT}/demo\""
echo "  PostgreSQL replica-1 127.0.0.1:${LOCAL_PG_REPLICA_1_PORT}  (read-only; same users as primary)"
echo "  PostgreSQL replica-2 127.0.0.1:${LOCAL_PG_REPLICA_2_PORT}  (read-only)"
echo "  PostgreSQL postgres-sub (logical subscriber pod) 127.0.0.1:${LOCAL_PG_LOGICAL_SUB_PORT}  (writable; db demo_logical_sub after hub setup)"
if [[ "$_cassandra_pf_enabled" == true ]]; then
  echo "  Cassandra CQL  cqlsh 127.0.0.1 ${LOCAL_CQL_PORT}   (localhost forward pod/cassandra-0; keyspace demo_hub after bootstrap Job)"
else
  echo "  Cassandra CQL  ${_script_dir}/demo-hub-cqlsh.sh   (second terminal — no localhost :${LOCAL_CQL_PORT}; OrbStack/kubectl PF often resets native CQL)"
  echo "                  optional: CASSANDRA_KUBE_PORT_FORWARD=1 when starting this script to try 127.0.0.1:${LOCAL_CQL_PORT} forward again"
fi
echo "  MongoDB       mongosh mongodb://127.0.0.1:${LOCAL_MONGO_PORT}/"
echo "  Redis         127.0.0.1:${LOCAL_REDIS_PORT}   (password demoredispass; URI redis://:demoredispass@127.0.0.1:${LOCAL_REDIS_PORT}/0)"
echo "  Grafana       http://127.0.0.1:${LOCAL_GRAFANA_PORT}/"
echo "  Prometheus    http://127.0.0.1:${LOCAL_PROM_PORT}/"
echo "  Hub demo UI   http://127.0.0.1:${LOCAL_HUB_UI_PORT}/  (Faker + map orders: /scenario step 3)"
echo "  Kafka broker    127.0.0.1:${LOCAL_KAFKA_BROKER_PORT}  (bootstrap from Mac; add /etc/hosts: 127.0.0.1 kafka)"
echo "                  Kafka UI in Docker: bootstrap host.docker.internal:${LOCAL_KAFKA_BROKER_PORT} + extra_hosts kafka:host-gateway"
echo "  Kafka Connect http://127.0.0.1:${LOCAL_KAFKA_CONNECT_PORT}/  (REST; list: curl -s http://127.0.0.1:${LOCAL_KAFKA_CONNECT_PORT}/connectors)"
echo "  Trino         http://127.0.0.1:${LOCAL_TRINO_PORT}/  (coordinator UI; federated SQL runner on hub: http://127.0.0.1:${LOCAL_HUB_UI_PORT}/trino)"
echo "  OpenSearch    http://127.0.0.1:${LOCAL_OPENSEARCH_PORT}/  (REST API; e.g. curl -s http://127.0.0.1:${LOCAL_OPENSEARCH_PORT}/_cluster/health)"
echo "  OS Dashboards http://127.0.0.1:${LOCAL_OS_DASHBOARDS_PORT}/  (matches “OpenSearch Dashboards” link on hub home)"
echo "  Vault UI/API http://127.0.0.1:${LOCAL_VAULT_PORT}/  (token: demo-hub-dev-root — demo dev mode only)"
echo "  SQL Server publisher 127.0.0.1:${LOCAL_MSSQL_PUBLISHER_PORT}  login=sa password=${MSSQL_SA_PASSWORD_HINT}  (database demo after bootstrap; encrypt optional)"
echo "  SQL Server subscriber 127.0.0.1:${LOCAL_MSSQL_SUBSCRIBER_PORT}  login=sa password=${MSSQL_SA_PASSWORD_HINT}"
echo "       sqlcmd publisher: sqlcmd -S 127.0.0.1,${LOCAL_MSSQL_PUBLISHER_PORT} -U sa -P \"${MSSQL_SA_PASSWORD_HINT}\" -C -Q \"SELECT name FROM sys.databases\""
echo "       JDBC publisher: jdbc:sqlserver://127.0.0.1:${LOCAL_MSSQL_PUBLISHER_PORT};databaseName=demo;encrypt=false;trustServerCertificate=true"
echo "  Oracle FREEPDB1    127.0.0.1:${LOCAL_ORACLE_PORT}  user=demo password=demopass  (service FREEPDB1; after oracle-demo-bootstrap Job)"
echo "       sqlplus demo/demopass@//127.0.0.1:${LOCAL_ORACLE_PORT}/FREEPDB1"
echo "  Client toolbox     kubectl exec -it -n ${NS} deploy/demo-tools -- bash -l"
echo ""

# Remote port is always 5432 (container + Service targetPort). Local ports differ so primary + replicas can all bind.
pf() { kubectl -n "$NS" port-forward --address "$PORT_FORWARD_ADDR" "$@"; }

pf "svc/postgresql-primary" "${LOCAL_PG_PORT}:5432" &
pf "svc/postgresql-replica-1" "${LOCAL_PG_REPLICA_1_PORT}:5432" &
pf "svc/postgresql-replica-2" "${LOCAL_PG_REPLICA_2_PORT}:5432" &
pf "svc/postgres-sub" "${LOCAL_PG_LOGICAL_SUB_PORT}:5432" &
if [[ "$_cassandra_pf_enabled" != true ]]; then
  echo "Cassandra localhost port-forward disabled (context or SKIP). Use: ${_script_dir}/demo-hub-cqlsh.sh" >&2
else
  (
    set +e
    while true; do
      pf "pod/cassandra-0" "${LOCAL_CQL_PORT}:9042"
      echo "cassandra port-forward exited (rc=$?); restarting in 2s…" >&2
      sleep 2
    done
  ) &
fi
pf "svc/mongo-mongos1" "${LOCAL_MONGO_PORT}:27017" &
pf "svc/redis" "${LOCAL_REDIS_PORT}:6379" &
pf "svc/grafana" "${LOCAL_GRAFANA_PORT}:3000" &
if [[ "${SKIP_PROMETHEUS:-}" == "1" ]]; then
  echo "SKIP_PROMETHEUS=1 — not forwarding svc/prometheus (fix the pod, then re-run without this)." >&2
else
  pf "svc/prometheus" "${LOCAL_PROM_PORT}:9090" &
fi
pf "svc/hub-demo-ui" "${LOCAL_HUB_UI_PORT}:8888" &
if [[ "${SKIP_KAFKA_BROKER:-}" == "1" ]]; then
  echo "SKIP_KAFKA_BROKER=1 — not forwarding svc/kafka:9092 (avoid clash with host Compose Kafka)." >&2
else
  pf "svc/kafka" "${LOCAL_KAFKA_BROKER_PORT}:9092" &
fi
pf "svc/kafka-connect" "${LOCAL_KAFKA_CONNECT_PORT}:8083" &
if [[ "${SKIP_TRINO:-}" == "1" ]]; then
  echo "SKIP_TRINO=1 — not forwarding svc/trino (omit SKIP_TRINO once Trino is applied and Ready)." >&2
else
  pf "svc/trino" "${LOCAL_TRINO_PORT}:8080" &
fi
pf "svc/opensearch" "${LOCAL_OPENSEARCH_PORT}:9200" &
pf "svc/opensearch-dashboards" "${LOCAL_OS_DASHBOARDS_PORT}:5601" &
pf "svc/vault" "${LOCAL_VAULT_PORT}:8200" &
if [[ "${SKIP_MSSQL:-}" == "1" ]]; then
  echo "SKIP_MSSQL=1 — not forwarding svc/mssql-publisher / svc/mssql-subscriber." >&2
else
  pf "svc/mssql-publisher" "${LOCAL_MSSQL_PUBLISHER_PORT}:1433" &
  pf "svc/mssql-subscriber" "${LOCAL_MSSQL_SUBSCRIBER_PORT}:1433" &
fi
if [[ "${SKIP_ORACLE:-}" == "1" ]]; then
  echo "SKIP_ORACLE=1 — not forwarding svc/oracle:1521." >&2
else
  pf "svc/oracle" "${LOCAL_ORACLE_PORT}:1521" &
fi

wait
