# Data-plane bootstrap (same semantics as Docker Compose)

Generated Jobs (apply **after** workloads are running):

| Job | Purpose |
|-----|---------|
| **vault-demo-hub-seed** | Writes demo credentials + Kafka Connect fields into Vault KV v2 (`secret/demo-hub/...`). Re-run after Vault pod restart (dev mode is in-memory). |
| **postgres-demo-bootstrap** | Debezium user/table/publication + physical replication slots + scenario schema (mirrors `postgres-kafka/*.sql` init). |
| **cassandra-demo-schema** | `demo_hub` keyspace with **RF=3** + placeholder table (ring replication). |
| **mongo-demo-bootstrap** | Config RS → shard RS → addShard → sharded collections (mirrors `mongo-sharded/*.sh` + `prepare-demo-collections.sh`). |

Re-run: `kubectl delete job -n demo-hub <name>` then `kubectl apply -f …` again.

Not generated here: **kafka-connect-register** (use host script against `kafka-connect:8083`). MCAC agent JAR is populated by StatefulSet **initContainer** (`mcac-copy-agent`); build **`mcac-demo/mcac-init:local`** from the repo-root **Dockerfile** (`deploy/k8s/scripts/build-mcac-init-image.sh`).

See **../scripts/apply-data-bootstrap.sh**.
