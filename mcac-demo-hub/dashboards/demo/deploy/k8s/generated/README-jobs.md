# Data-plane bootstrap (same semantics as Docker Compose)

Generated Jobs (apply **after** workloads are running):

| Job | Purpose |
|-----|---------|
| **vault-demo-hub-seed** | Writes demo credentials + Kafka Connect fields into Vault KV v2 (`secret/demo-hub/...`). Re-run after Vault pod restart (dev mode is in-memory). |
| **postgres-demo-bootstrap** | Debezium user/table/publication + physical replication slots + scenario schema (mirrors `postgres-kafka/*.sql` init). |
| **cassandra-demo-schema** | `demo_hub` keyspace with **RF=3** + placeholder table (ring replication). |
| **mongo-demo-bootstrap** | Config RS → shard RS → addShard → sharded collections (mirrors `mongo-sharded/*.sh` + `prepare-demo-collections.sh`). |
| **mssql-demo-bootstrap** | Publisher + subscriber schema (`sqlcmd`), optional replication try, then **register-mssql-connectors.sh** against **kafka-connect:8083** (needs Connect rollout first — see `apply-data-bootstrap.sh`). |

Re-run: `kubectl delete job -n demo-hub <name>` then `kubectl apply -f …` again.

Not generated here: **postgres/mongo kafka-connect-register** beyond MSSQL (use host scripts against `kafka-connect:8083` if needed). MCAC agent JAR is populated by StatefulSet **initContainer** (`mcac-copy-agent`); build **`mcac-demo/mcac-init:local`** from the repo-root **Dockerfile** (`deploy/k8s/scripts/build-mcac-init-image.sh`).

See **../scripts/apply-data-bootstrap.sh**.
