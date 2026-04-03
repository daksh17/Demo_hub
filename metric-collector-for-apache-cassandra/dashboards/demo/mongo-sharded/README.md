# MongoDB sharded cluster (tic / tac / toe)

Shell scripts used by **`../docker-compose.yml`** to turn up a **nine-container** topology:

- **3× config servers** — replica set **`configReplSet`**
- **3× shards** — replica sets **`tic`**, **`tac`**, **`toe`** (one data node each in this demo)
- **3× `mongos`** — routers; Debezium and apps should use **`mongo-mongos1`** (or host port **27025** on the first router)

## Scripts (mounted into one-shot containers)

| File | Runs when | Purpose |
|------|-----------|---------|
| **`init-replica-sets.sh`** | Service **`mongo-shard-init-rs`** | **`rs.initiate`** for configReplSet and for tic/tac/toe; waits for primaries. |
| **`add-shards.sh`** | Service **`mongo-shard-add`** | **`addShard`** for tic, tac, toe via **`mongos`**. |

All **`mongod`** / **`mongos`** processes use **port 27017** inside the network (explicit `--port` avoids MongoDB 7’s default **27019** on config servers).

## Kafka / Debezium

After **`mongo-shard-add`** succeeds, run **`mongo-kafka-prepare`** and register connectors—see **[`../mongo-kafka/README.md`](../mongo-kafka/README.md)**.

## Further reading

- Main demo index: **[`../README.md`](../README.md)**.
- End-to-end Mongo + diagrams in **[`../postgres-kafka/README.md`](../postgres-kafka/README.md)** (Mongo section).
