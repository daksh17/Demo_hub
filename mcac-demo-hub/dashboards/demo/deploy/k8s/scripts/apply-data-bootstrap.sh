#!/usr/bin/env bash
# After workloads are applied (e.g. `kubectl apply -f deploy/k8s/generated/all.yaml`), run Postgres / Cassandra / Mongo
# bootstrap Jobs — same replication & sharding steps as Docker Compose init scripts.
set -euo pipefail
NS="${NS:-demo-hub}"
GEN="$(cd "$(dirname "$0")/.." && pwd)/generated"

echo "Waiting for PostgreSQL primary..."
echo "  (If this hangs: the primary pod is not Ready — usually Pending scheduling, image pull, or crash. In another terminal: kubectl get pods -n $NS; kubectl describe pod -n $NS -l app.kubernetes.io/name=postgresql-primary)"
kubectl rollout status deployment/postgresql-primary -n "$NS" --timeout=420s

echo "Waiting for logical subscriber postgres-sub..."
kubectl rollout status deployment/postgres-sub -n "$NS" --timeout=420s

echo "Waiting for Cassandra StatefulSet (rollout)..."
echo "  (If this hangs: build MCAC init image first — chmod +x deploy/k8s/scripts/build-mcac-init-image.sh && ./deploy/k8s/scripts/build-mcac-init-image.sh; initContainer uses imagePullPolicy:Never + tag mcac-demo/mcac-init:local)"
kubectl rollout status statefulset/cassandra -n "$NS" --timeout=600s

echo "Waiting for cassandra-0 to be Ready (required before schema Job; avoids hanging on OOM/crashloop)..."
if ! kubectl wait pod/cassandra-0 -n "$NS" --for=condition=Ready --timeout=600s; then
  echo "cassandra-0 is not Ready. Check OOM / limits: kubectl -n $NS describe pod cassandra-0; kubectl logs cassandra-0 -n $NS --tail=80" >&2
  exit 1
fi

echo "Applying Postgres bootstrap Job..."
kubectl apply -f "$GEN/45-postgres-bootstrap-job.yaml"
kubectl wait --for=condition=complete job/postgres-demo-bootstrap -n "$NS" --timeout=3600s

echo "Applying Postgres logical-subscriber bootstrap Job (demo role on postgres-sub)..."
kubectl delete job postgres-sub-bootstrap -n "$NS" --ignore-not-found=true
kubectl apply -f "$GEN/46-postgres-sub-bootstrap-job.yaml"
kubectl wait --for=condition=complete job/postgres-sub-bootstrap -n "$NS" --timeout=600s

echo "Applying Cassandra schema Job (targets cassandra-0 via headless DNS)..."
kubectl delete job cassandra-demo-schema -n "$NS" --ignore-not-found=true
kubectl apply -f "$GEN/35-cassandra-schema-job.yaml"
kubectl wait --for=condition=complete job/cassandra-demo-schema -n "$NS" --timeout=1800s

echo "Waiting for MongoDB Deployments..."
for d in mongo-config1 mongo-config2 mongo-config3 mongo-shard-tic mongo-shard-tac mongo-shard-toe mongo-mongos1 mongo-mongos2 mongo-mongos3; do
  kubectl rollout status deployment/"$d" -n "$NS" --timeout=420s
done

kubectl delete job mongo-demo-bootstrap -n "$NS" --ignore-not-found=true
kubectl apply -f "$GEN/61-mongo-bootstrap-job.yaml"
kubectl wait --for=condition=complete job/mongo-demo-bootstrap -n "$NS" --timeout=7200s

echo "Waiting for Kafka Connect (MSSQL connector registration)..."
kubectl rollout status deployment/kafka-connect -n "$NS" --timeout=600s

require_local_image_mssql_tools() {
  local img="mcac-demo/mssql-tools:22.04"
  if ! command -v docker &>/dev/null; then
    echo "WARN: docker not in PATH — cannot verify $img locally. Job uses imagePullPolicy:Never;" >&2
    echo "      if the Job fails with ErrImageNeverPull, build: deploy/k8s/scripts/build-mssql-tools-image.sh" >&2
    return 0
  fi
  if docker image inspect "$img" &>/dev/null; then
    return 0
  fi
  cat >&2 <<EOF
ERROR: Docker image $img not found locally.

Job mssql-demo-bootstrap uses imagePullPolicy: Never (no registry pull). Build the image on this machine
(local Docker daemon — OrbStack / Docker Desktop Kubernetes reuse those images):

  $(dirname "$0")/build-mssql-tools-image.sh

Or build all demo-hub custom images:

  $(dirname "$0")/build-all-custom-images.sh

Then:

  kubectl delete job mssql-demo-bootstrap -n $NS --ignore-not-found=true
  kubectl apply -f $GEN/62-mssql.yaml
  kubectl wait --for=condition=complete job/mssql-demo-bootstrap -n $NS --timeout=3600s
EOF
  exit 1
}

echo "Applying MSSQL schema + connector registration Job..."
require_local_image_mssql_tools
kubectl delete job mssql-demo-bootstrap -n "$NS" --ignore-not-found=true
kubectl apply -f "$GEN/62-mssql.yaml"
kubectl wait --for=condition=complete job/mssql-demo-bootstrap -n "$NS" --timeout=3600s

echo "Waiting for Oracle (first start can take several minutes)..."
kubectl rollout status deployment/oracle -n "$NS" --timeout=900s

echo "Applying Oracle demo schema Job..."
kubectl delete job oracle-demo-bootstrap -n "$NS" --ignore-not-found=true
kubectl apply -f "$GEN/63-oracle.yaml"
kubectl wait --for=condition=complete job/oracle-demo-bootstrap -n "$NS" --timeout=3600s

echo "Bootstrap jobs complete. kubectl get jobs -n $NS"
