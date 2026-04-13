# MongoDB sharded cluster (tic / tac / toe)

Shell scripts used by **`../docker-compose.yml`** to turn up a **nine-container** topology:

- **3× config servers** — replica set **`configReplSet`**
- **3× shards** — replica sets **`tic`**, **`tac`**, **`toe`** (one data node each in this demo)
- **3× `mongos`** — routers; Debezium and apps should use **`mongo-mongos1`** (or host port **27025** on the first router)

**Kubernetes:** the same topology is generated as Deployments in **`../k8s/generated/60-mongo-sharded.yaml`**; init is the **`61-mongo-bootstrap-job.yaml`** Job. **[`../k8s/README.md`](../k8s/README.md)** describes PDB/NetworkPolicy and other cluster-wide resources.

## Scripts (mounted into one-shot containers)

| File | Runs when | Purpose |
|------|-----------|---------|
| **`init-config-replica-set.sh`** | **`mongo-config-init-rs`** (before shards start) | **`rs.initiate`** for **configReplSet**; waits for a config **PRIMARY**. Avoids a deadlock where shard data already expects a config RS but it is not up yet. |
| **`init-shard-replica-sets.sh`** | **`mongo-shard-init-rs`** | **`rs.initiate`** for **tic** / **tac** / **toe**; waits for primaries. |
| **`init-replica-sets.sh`** | (optional / local) | Wrapper that runs both scripts in order. |
| **`add-shards.sh`** | Service **`mongo-shard-add`** | **`addShard`** for tic, tac, toe via **`mongos`**. |

All **`mongod`** / **`mongos`** processes use **port 27017** inside the network (explicit `--port` avoids MongoDB 7’s default **27019** on config servers).

## Troubleshooting

- **`addShard` / `mongo-shard-add` exit 1** — message like *local database `demo` exists on another shard*: usually leftover `demo` on a shard mongod from a partial run or write before all shards were registered. The **`add-shards.sh`** step now drops stray local **`demo`** on each shard **before** `addShard` (only for shards not yet in `listShards`). Re-run: `docker compose rm -f mongo-shard-add && docker compose up -d mongo-shard-add`.
- **`dependency failed to start: mongo-shard-* is unhealthy`**, logs show **`configReplSet`** / **`FailedToSatisfyReadPreference`** on a shard: usually fixed by this compose order (config RS before shards). If volumes were created under an inconsistent layout, reset Mongo data: `docker compose down` and remove the **`mongo_shard_*`** and **`mongo_cfg*`** volumes, then `up` again.
- **Stale cluster metadata** (config wiped but shards kept): remove shard volumes or run `docker compose down -v` for the demo project (only if you can lose local Mongo data).

## Kafka / Debezium

After **`mongo-shard-add`** succeeds, run **`mongo-kafka-prepare`** and register connectors—see **[`../mongo-kafka/README.md`](../mongo-kafka/README.md)**.

## Further reading

- Main demo index: **[`../README.md`](../README.md)**.
- Mongo CDC + Kafka + diagrams: **[`../mongo-kafka/README.md`](../mongo-kafka/README.md)**.
