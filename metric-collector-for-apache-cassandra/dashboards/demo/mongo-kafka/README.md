# MongoDB (sharded) + Kafka + Debezium

Part of **`dashboards/demo`**: Mongo services, **`mongo-kafka-prepare`**, **`kafka-connect`** (custom image), and exporters are defined in **`../docker-compose.yml`** (the same file as the PostgreSQL + Debezium demo; shared **Kafka**, **ZooKeeper**, **Kafka Connect**, **Prometheus**, and **Grafana**).

- **PostgreSQL CDC in this compose** (different connectors and scripts): **[`../postgres-kafka/README.md`](../postgres-kafka/README.md)**
- **Cluster bring-up** (config/shard/mongos init): **[Sharded cluster scripts → `../mongo-sharded/README.md`](../mongo-sharded/README.md)**
- **Shared broker:** **[`../kafka/README.md`](../kafka/README.md)**
- **Prometheus / Grafana:** **[`../observability/README.md`](../observability/README.md)**

This guide mirrors the Postgres CDC demo: **Debezium MongoDB source** (CDC via **`mongos`**) → Kafka → **MongoDB Kafka sink** into a separate collection (**no CDC loop**).

## What runs in compose

- **`mongo-kafka-prepare`** (one-shot): enables `sh.enableSharding("demo")`, shards `demo.demo_items` and `demo.demo_items_from_kafka`, inserts two seed documents on `demo.demo_items`.
- **`kafka-connect`**: custom image **`mcac-demo/kafka-connect:2.7.3-mongo-sink`** = Debezium **`connect:2.7.3.Final`** + **`mongo-kafka-connect-1.14.1-all.jar`** from Maven Central (see **`Dockerfile.connect`**).

## Architecture (Mongo + Kafka)

- **Topology (nine data-plane containers):** three **config servers**, three **shard replica sets** (one mongod each in this demo), three **`mongos`** processes. Clients (and Debezium) talk to **`mongos`** on **`mongodb://mongo-mongos1:27017`** inside the Compose network; on the host, **mongos1** is mapped to **27025** (see **`../docker-compose.yml`**).
- **CDC source:** Debezium **`MongoDbConnector`** uses MongoDB **change streams** (**`capture.mode` = `change_streams_update_full`**) via **`mongos`**—the supported path for a **sharded cluster** in current Debezium releases.
- **Kafka topics:** logical prefix **`demomongo`**. Per Debezium naming, captured collection **`demo.demo_items`** produces topic **`demomongo.demo.demo_items`**.
- **Sink:** The official **`MongoSinkConnector`** (fat JAR from Maven Central, bundled in the custom Connect image) consumes that topic and writes **`demo.demo_items_from_kafka`**. The sink applies **`io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`** so the Debezium envelope becomes plain document fields the sink can persist (**do not** use **`ExtractNewRecordState`** here—it expects a JDBC-style `Struct` and fails on Mongo’s JSON-style payloads).
- **Prepare step:** Compose service **`mongo-kafka-prepare`** runs **`prepare-demo-collections.sh`** after **`mongo-shard-add`** completes: **`sh.enableSharding("demo")`**, **`shardCollection`** on **`demo.demo_items`** and **`demo.demo_items_from_kafka`**, seed inserts on **`demo_items`**.
- **Loop safety:** **`collection.include.list`** is only **`demo.demo_items`**. The sink target collection is **not** captured, so **CDC → Kafka → sink** does not feed back into the source stream (same pattern as Postgres **`demo_items_from_kafka`**).

## How the workflow works (short)

1. Applications or **`mongosh`** issue writes to **`demo.demo_items`** through **`mongos`**; documents land on the appropriate shard (tic / tac / toe).
2. **`mongo-source-demo`** reads the **change stream** through **`mongos`**, emits Debezium events, and **produces** to **`demomongo.demo.demo_items`**.
3. **`mongo-sink-demo`** **consumes** that topic, runs **ExtractNewDocumentState**, and **writes** documents into **`demo.demo_items_from_kafka`** via **`mongos`**.
4. **Prometheus** scrapes **`mongodb-exporter`** (targeting **`mongo-mongos1`**) and **kafka-exporter**; **Grafana** can show **`mongodb-tictactoe-detailed.json`** (Mongo sharding, per-shard sizes, dbStats) and **`kafka-cluster-overview.json`** (Kafka exporter metrics); both live under **`../../grafana/generated-dashboards/`**.

### Component diagram

![MongoDB sharded cluster, Kafka, Kafka Connect, prepare job, observability](diagrams/mongo-workflow-components.svg)

_Source: [`diagrams/mongo-workflow-components.mmd`](diagrams/mongo-workflow-components.mmd). Regenerate the SVG (**from this `mongo-kafka` directory**):_

```bash
cd mongo-kafka
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/mongo-workflow-components.mmd -o diagrams/mongo-workflow-components.svg -b transparent
```

### CDC and round-trip sequence

![MongoDB change capture: demo_items → Kafka → demo_items_from_kafka](diagrams/mongo-cdc-sequence.svg)

_Source: [`diagrams/mongo-cdc-sequence.mmd`](diagrams/mongo-cdc-sequence.mmd). Regenerate:_

```bash
cd mongo-kafka
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/mongo-cdc-sequence.mmd -o diagrams/mongo-cdc-sequence.svg -b transparent
```

## Step-by-step walkthrough

1. **Start Mongo dependencies** (from **`dashboards/demo`**): config servers, **`mongo-config-init-rs`**, shard nodes, **`mongo-shard-init-rs`**, **`mongos`** ×3, **`mongo-shard-add`**, then **`mongo-kafka-prepare`**. Ensure **Kafka** and **ZooKeeper** are up if you have not already started another demo stack that includes them.
2. **Build and start Kafka Connect** so the worker loads the **Mongo sink** plugin:

   ```bash
   docker compose build kafka-connect
   docker compose up -d kafka-connect
   ```

3. **Register connectors** (from **`dashboards/demo`**):

   ```bash
   chmod +x mongo-kafka/register-mongo-connectors.sh
   ./mongo-kafka/register-mongo-connectors.sh
   ```

   Optional URL: `./mongo-kafka/register-mongo-connectors.sh http://localhost:8083`

4. **Confirm connector state:**

   ```bash
   curl -s http://localhost:8083/connectors/mongo-source-demo/status
   curl -s http://localhost:8083/connectors/mongo-sink-demo/status
   ```

   Both connectors should show **`RUNNING`** tasks after the initial snapshot.

## Connectors (reference)

| Name | Class | Role |
|------|--------|------|
| **`mongo-source-demo`** | `io.debezium.connector.mongodb.MongoDbConnector` | CDC from **`demo.demo_items`**; **`topic.prefix`** **`demomongo`**; connection **`mongodb://mongo-mongos1:27017`**. |
| **`mongo-sink-demo`** | `com.mongodb.kafka.connect.MongoSinkConnector` | Consumes **`demomongo.demo.demo_items`**; **`connection.uri`** **`mongodb://mongo-mongos1:27017`**; writes **`demo.demo_items_from_kafka`**; **SMT** **`ExtractNewDocumentState`**. |

The sink uses **`io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`** (not **`ExtractNewRecordState`**) so flattened documents work with the Mongo sink.

## Ports (host)

| Service | Port (typical) |
|---------|-----------------|
| mongos 1 | **27025** |
| mongos 2 | **27026** |
| mongos 3 | **27027** |
| Kafka | **9092** |
| Kafka Connect REST | **8083** |
| mongodb-exporter (optional) | **9216** |
| Prometheus / Grafana | **9090** / **3000** |

## Quick verification

```bash
# From dashboards/demo — compare source vs sink collections on mongos
docker compose exec mongo-mongos1 mongosh demo --eval 'db.demo_items.find().limit(3); db.demo_items_from_kafka.find().limit(3)'
```

Insert a test document on the host (example uses **mongos1** port **27025**):

```bash
mongosh "mongodb://127.0.0.1:27025/demo" --eval 'db.demo_items.insertOne({ name: "cdc-test", qty: 42 })'
```

After a short delay, the same logical document (with flattened **`_id`**) should appear in **`demo_items_from_kafka`** if both connectors are healthy.

## Seed bulk documents (optional)

**`prepare-demo-collections.sh`** only inserts two seed docs when **`demo_items`** is empty. To add more rows anytime (similar to **`postgres-kafka/seed-dummy-data.sql`**), run **`seed-demo-items.sh`** from **`dashboards/demo`**:

```bash
chmod +x mongo-kafka/seed-demo-items.sh
./mongo-kafka/seed-demo-items.sh           # default: 30 documents
./mongo-kafka/seed-demo-items.sh 100       # custom count
```

By default the script uses **`mongodb://127.0.0.1:27025`** (mongos1 on the host). If you run it **inside** a container on the demo network, set **`MONGOS_URI`** so the hostname resolves:

```bash
MONGOS_URI="mongodb://mongo-mongos1:27017" ./mongo-kafka/seed-demo-items.sh 30
```

Or run via the mongos container:

```bash
docker compose exec mongo-mongos1 bash -s -- <<'EOF'
mongosh mongodb://127.0.0.1:27017/demo --quiet --eval '
for (let i = 1; i <= 10; i++) {
  db.demo_items.insertOne({ name: "inline-"+i, qty: i });
}
print("done");
'
EOF
```

Each **`insertOne` / `insertMany`** still goes through **mongos**; hashed sharding (below) decides which shard stores each document.

## How shards are created and how data is spread

Rough order (all wired in **`../docker-compose.yml`**; scripts live under **`../mongo-sharded/`**):

1. **Replica sets** — **`mongo-config-init-rs`** runs **`init-config-replica-set.sh`** so **`configReplSet`** has a **PRIMARY** before any shard `mongod` starts. Then **`mongo-shard-init-rs`** runs **`init-shard-replica-sets.sh`** to initialize **`tic`**, **`tac`**, **`toe`** on the three shards. (Legacy **`init-replica-sets.sh`** runs both steps in one shell.)
2. **Routers** — three **`mongos`** processes start; they read **cluster metadata** from the config servers.
3. **Register shards** — **`add-shards.sh`** (service **`mongo-shard-add`**) runs **`addShard`** for **`tic` / `tac` / `toe`** so each shard replica set is a storage node in the cluster (see **`add-shards.sh`**).
4. **Shard the collections** — **`prepare-demo-collections.sh`** runs **`sh.enableSharding("demo")`** and **`sh.shardCollection("demo.demo_items", { _id: "hashed" })`** (and the same for **`demo_items_from_kafka`**). A **hashed shard key on `_id`** means MongoDB hashes each document’s **`_id`** and places it in a **chunk**; chunks are **split** and **balanced** across shards so load is spread roughly evenly as data grows (not “round‑robin per insert”).
5. **Reads/writes** — applications use **mongos** only; mongos routes operations to the right shard using metadata in the **config** database.

To inspect distribution (from **`dashboards/demo`**):

```bash
docker compose exec mongo-mongos1 mongosh --quiet --eval 'sh.status()'
```

Chunk counts per shard often appear under **collections** / **balancer** sections; the Grafana **detailed** dashboard also surfaces **chunks per shard** from the exporter.

### Shard key: what it is

The **shard key** is the field (or compound fields) MongoDB uses to decide **which shard** stores each document. You choose it when you run **`sh.shardCollection`**. It is not “automatically discovered”; it is a schema/cluster decision. In this demo, **`prepare-demo-collections.sh`** uses:

```text
sh.shardCollection("demo.demo_items", { _id: "hashed" })
```

So the shard key is **`_id`**, with a **hashed** strategy (MongoDB hashes the `_id` value for routing). **`demo_items_from_kafka`** is sharded the same way.

Implications:

- **Hashed `_id`** — Good default for **even spread** when `_id` values are uncorrelated (e.g. default **`ObjectId`**). Inserts are not “round robin”; each document’s **hash** falls into some **chunk**, and over time **splitting** and **balancing** keep chunk counts per shard roughly similar.
- **Range shard key** (not used here) — Would use actual key values as ranges; hot spots can appear if many documents share nearby key values.

The shard key is **fixed** for that collection unless you **reshard** (a deliberate migration). Changing which field you “would like” to route on later requires planning.

### How mongos routes data (distribution + lookups)

1. **Metadata** — **Config servers** store which **chunk** (a contiguous range of **hashed** shard key values, or key ranges for range sharding) lives on which **shard**.
2. **Insert / update with shard key** — **mongos** computes the shard key value (here: hash of **`_id`**), finds the chunk and shard, and sends the write to that shard’s primary.
3. **Query by `_id` (equality)** — mongos can target **one** shard (efficient).
4. **Query without the shard key** (e.g. `find({ name: "x" })` with no `_id`) — mongos may run a **scatter‑gather** query (every shard), which still returns correct results but does more work on large clusters.
5. **Balancer** — Background process moves **chunks** between shards when the cluster is imbalanced so data (and read/write load) tends to stay evenly spread for hashed keys.

So “how it finds distribution” is: **hashed shard key → chunk boundaries in config DB → mongos picks the shard(s)** for each operation. **`sh.status()`**, **`db.demo_items.getStats()`** / **`explain`** on mongos, per-shard document counts in monitoring, and the Grafana detailed dashboard’s **chunks per shard** metrics all reflect that layout.

## Troubleshooting

1. **Sink task `FAILED` with “Only Struct objects supported … found: java.lang.String”** — the sink is using **`ExtractNewRecordState`**. Use **`io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`** (as in **`register-mongo-connectors.sh`**).
2. **`mongo-source-demo` cannot connect** — ensure **`mongos`** is healthy and the URI uses **`mongo-mongos1:27017`** from **inside** the Connect container (not `localhost`).
3. **No topics or empty sink** — run **`mongo-kafka-prepare`** successfully once; confirm **`sh.status()`** shows **`demo`** enabled and **`demo_items`** sharded. Re-run **`./mongo-kafka/register-mongo-connectors.sh`** after fixing the cluster.
4. **Connect missing `MongoSinkConnector`** — rebuild the image: **`docker compose build kafka-connect`** and recreate the **`kafka-connect`** container.
5. **`mongo-source-demo` task `FAILED` with `OutOfMemoryError: Java heap space`** during snapshot — the Debezium Connect image defaults to about **2 GiB** heap; Mongo’s initial snapshot (and Postgres CDC on the same worker) can exceed that. The demo compose sets **`KAFKA_HEAP_OPTS: -Xms512m -Xmx8192m`** on **`kafka-connect`**; raise **`Xmx`** further if needed, then **`docker compose up -d kafka-connect`** and fix the connector offset or re-register (`./reset-kafka-connect-demo.sh` or delete + **`register-mongo-connectors.sh`**).

## Files in this directory

| File | Role |
|------|------|
| `Dockerfile.connect` | Extends Debezium Connect + Mongo sink JAR. |
| `prepare-demo-collections.sh` | Sharding + seeds; used by **`mongo-kafka-prepare`**. |
| `seed-demo-items.sh` | Appends **`N`** bulk docs to **`demo.demo_items`** via mongos (default **30**). |
| `register-mongo-connectors.sh` | Registers **`mongo-source-demo`** and **`mongo-sink-demo`**. |
| `diagrams/mongo-workflow-components.mmd` / `.svg` | Component diagram for this README. |
| `diagrams/mongo-cdc-sequence.mmd` / `.svg` | CDC sequence diagram. |
