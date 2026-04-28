#!/usr/bin/env bash
# Build every image the generated K8s manifests expect that are NOT pulled from public Docker Hub.
# Run from anywhere; uses paths under dashboards/demo (Compose-equivalent builds).
#
# After building (OrbStack / Docker Desktop: same engine as Kubernetes — usually no import step):
#   kubectl rollout restart deploy/kafka-connect deploy/hub-demo-ui deploy/nodetool-exporter -n demo-hub
#   kubectl rollout restart deployment/mssql-publisher deployment/mssql-subscriber deployment/mssql-exporter-publisher deployment/mssql-exporter-subscriber -n demo-hub
#   kubectl rollout restart sts/cassandra -n demo-hub
# kind: load each image — see build-load-nodetool-exporter.sh pattern.
set -euo pipefail
DEMO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$DEMO_ROOT"

echo "=== 1/6 MCAC init (Cassandra initContainer) ==="
"$DEMO_ROOT/deploy/k8s/scripts/build-mcac-init-image.sh"

echo "=== 2/6 Hub demo UI ==="
docker build -t mcac-demo/hub-demo-ui:latest -f deploy/docker/realtime-orders-search-hub/demo-ui/Dockerfile deploy/docker/realtime-orders-search-hub/demo-ui

echo "=== 3/6 Kafka Connect (Debezium + Mongo + SQL Server plugins) ==="
docker build -t mcac-demo/kafka-connect:2.7.3-mongo-sink -f deploy/docker/mongo-kafka/Dockerfile.connect deploy/docker/mongo-kafka

echo "=== 4/6 MSSQL tools (Job mssql-demo-bootstrap — sqlcmd + curl) ==="
"$DEMO_ROOT/deploy/k8s/scripts/build-mssql-tools-image.sh"

echo "=== 5/6 Nodetool exporter ==="
docker build -t demo-hub/nodetool-exporter:latest -f deploy/docker/nodetool-exporter/Dockerfile deploy/docker/nodetool-exporter

echo "=== 6/6 PostgreSQL + repmgr (matches Compose / POSTGRESQL_IMAGE) ==="
docker build -t mcac-demo/postgresql-repmgr:16.6.0 -f deploy/docker/postgres-kafka/Dockerfile.repmgr deploy/docker/postgres-kafka

echo ""
echo "All custom images built. If kind/minikube, load them (example for kind):"
echo "  kind load docker-image mcac-demo/mcac-init:local"
echo "  kind load docker-image mcac-demo/hub-demo-ui:latest"
echo "  kind load docker-image mcac-demo/kafka-connect:2.7.3-mongo-sink"
echo "  kind load docker-image mcac-demo/mssql-tools:22.04"
echo "  kind load docker-image demo-hub/nodetool-exporter:latest"
echo "  kind load docker-image mcac-demo/postgresql-repmgr:16.6.0"
echo "Then: kubectl rollout restart deployment/kafka-connect deployment/hub-demo-ui deployment/nodetool-exporter statefulset/cassandra -n demo-hub"
echo "  deployment/mssql-publisher deployment/mssql-subscriber deployment/mssql-exporter-publisher deployment/mssql-exporter-subscriber -n demo-hub"
echo "  (and rollout restart deployment/postgresql-primary deployment/postgresql-replica-1 deployment/postgresql-replica-2 -n demo-hub after loading Postgres image)"
