### Cluster Demo

This directory’s **`docker-compose.yml`** defines **one** stack (Cassandra, ZooKeeper/Kafka/Connect, optional Postgres and Mongo demos, **Redis 7**, **OpenSearch**, Prometheus, Grafana). A plain **`docker compose up`** here starts **every service in that file** (no Compose profiles). Prefer **[`start-full-stack.sh`](start-full-stack.sh)** so `PROJECT_VERSION` is set and **`mcac`** / **`kafka-connect`** images are built first.

**Kafka Connect:** a one-shot **`kafka-connect-register`** service runs **[`kafka-connect-register/register-all.sh`](kafka-connect-register/register-all.sh)** so all four demo connectors are registered (**Postgres** Debezium + JDBC sink, **Mongo** Debezium + Mongo sink). **Hub UI** “Single order” / “Workload” write to **`demo_items`** (Postgres) and **`demo.demo_items`** (Mongo), which those connectors capture. Re-register from the host with **`./kafka-connect-register/register-all.sh`** if Connect was restarted.

**Reset Connect sink copies** (Postgres **`demo_items_from_kafka`**, Mongo **`demo.demo_items_from_kafka`**) before re-testing: [`./clean-kafka-connect-sinks.sh`](clean-kafka-connect-sinks.sh) from this directory (stack must be running).

**Sinks stay empty after Workload / Single order:** the hub only writes the **source** tables (`public.demo_items`, `demo.demo_items`). Rows appear in **`demo_items_from_kafka`** / **`demo.demo_items_from_kafka`** only if **all four connectors are registered** and **RUNNING** (Kafka Connect → topics → sinks). Run **[`./diagnose-kafka-connect.sh`](diagnose-kafka-connect.sh)** from this directory; if connectors are missing or `FAILED`, run **`./kafka-connect-register/register-all.sh`** and check **`docker compose logs kafka-connect`**.

**Note:** **`../docker-compose.yaml`** (parent `dashboards/` folder) runs **only** Prometheus + Grafana on ports **9091** / **3001** and does **not** start Kafka, Postgres, Mongo, Redis, OpenSearch, or Cassandra.

Topic-specific guides live in subfolders:

| Folder | Focus |
|--------|--------|
| **[`cassandra/`](cassandra/README.md)** | Cassandra + MCAC + nodetool-exporter |
| **[`kafka/`](kafka/README.md)** | ZooKeeper, Kafka broker, Kafka Connect image |
| **[`observability/`](observability/README.md)** | Prometheus + Grafana provisioning |
| **[`redis/`](redis/README.md)** | Redis **7** (password, AOF, volume) |
| **[`opensearch/`](opensearch/README.md)** | OpenSearch single-node + Dashboards (dev, HTTP) |
| **[`postgres-kafka/`](postgres-kafka/README.md)** | PostgreSQL HA + Debezium + JDBC sink |
| **[`mongo-kafka/`](mongo-kafka/README.md)** | Sharded Mongo + Debezium + Mongo sink |
| **[`mongo-sharded/`](mongo-sharded/README.md)** | Init scripts for config/shard/mongos (`tic` / `tac` / `toe`) |
| **[`realtime-orders-search-hub/`](realtime-orders-search-hub/README.md)** | **Reference scenario**: Postgres + Mongo CDC, Kafka, Cassandra, Redis, OpenSearch, observability (workflows + Mermaid diagrams) |

### Hub scenario indexes (Multi-DB reference)

The **hub** path (`realtime-orders-search-hub`) creates scenario tables/collections and **indexes** meant for demos and query-shape teaching: PostgreSQL access methods (B-tree, **BRIN**, **GIN**, **GiST**, **HASH**, partial, covering), MongoDB compound/partial/**text** (ESR-oriented compounds), and a Cassandra **secondary index** on a low-cardinality column.

**Where it is defined**

| Store | Source of truth | One-shot / existing stack |
|--------|-----------------|---------------------------|
| PostgreSQL | [`realtime-orders-search-hub/demo-ui/scenario.py`](realtime-orders-search-hub/demo-ui/scenario.py) (`ensure_postgres_scenario_schema`, `_ensure_postgres_scenario_indexes`) + [`postgres-kafka/04-scenario-hub-schema-indexes.sql`](postgres-kafka/04-scenario-hub-schema-indexes.sql) (initdb + parity) | [`postgres-kafka/apply-scenario-hub-schema-indexes.sh`](postgres-kafka/apply-scenario-hub-schema-indexes.sh) |
| MongoDB | [`mongo-kafka/demo-indexes.js`](mongo-kafka/demo-indexes.js) (run from [`mongo-kafka/prepare-demo-collections.sh`](mongo-kafka/prepare-demo-collections.sh) or apply script) | [`mongo-kafka/apply-demo-indexes.sh`](mongo-kafka/apply-demo-indexes.sh) |
| Cassandra | [`realtime-orders-search-hub/demo-ui/scenario.py`](realtime-orders-search-hub/demo-ui/scenario.py) (`ensure_cassandra_scenario_schema`) + [`cassandra/ensure-scenario-hub.cql`](cassandra/ensure-scenario-hub.cql) | [`cassandra/apply-scenario-hub-schema.sh`](cassandra/apply-scenario-hub-schema.sh) |

Requires extension **`pg_trgm`** (created in the same flow) for **GiST** (`gist_trgm_ops`) on `title`.

#### PostgreSQL (`demo`)

| Index name | Table | Access method | Definition / predicate |
|------------|-------|---------------|-------------------------|
| _(implicit)_ | `scenario_catalog_mirror` | B-tree (unique) | `UNIQUE` on `sku` (table constraint) |
| `idx_scenario_catalog_category_price` | `scenario_catalog_mirror` | **B-tree** | `(category, unit_price_cents, sku)` — compound |
| `idx_scenario_catalog_updated_at` | `scenario_catalog_mirror` | **B-tree** | `(updated_at DESC)` |
| `idx_scenario_catalog_brin_updated` | `scenario_catalog_mirror` | **BRIN** | `(updated_at)` — cheap temporal summaries on large time-ordered tables |
| `idx_scenario_catalog_in_stock` | `scenario_catalog_mirror` | **B-tree** (partial) | `(category, sku)` **`WHERE stock_units > 0`** |
| `idx_scenario_catalog_title_trgm` | `scenario_catalog_mirror` | **GiST** | `(title gist_trgm_ops)` — fuzzy / similarity-friendly (with `pg_trgm`) |
| `idx_scenario_catalog_kafka_key_hash` | `scenario_catalog_mirror` | **HASH** | `(kafka_msg_key)` — equality-oriented |
| `idx_scenario_orders_stage_created` | `scenario_orders` | **B-tree** | `(pipeline_stage, created_at DESC)` |
| `idx_scenario_orders_email` | `scenario_orders` | **B-tree** | `(customer_email)` |
| `idx_scenario_orders_created_brin` | `scenario_orders` | **BRIN** | `(created_at)` |
| `idx_scenario_orders_placed_partial` | `scenario_orders` | **B-tree** (partial) | `(created_at DESC)` **`WHERE pipeline_stage = 'placed'`** |
| `idx_scenario_orders_lines_gin` | `scenario_orders` | **GIN** | `(lines jsonb_path_ops)` — containment / path-style JSONB |
| `idx_scenario_orders_stage_cover` | `scenario_orders` | **B-tree** (covering) | `(pipeline_stage)` **`INCLUDE (order_ref, total_cents)`** |
| `idx_scenario_fulfill_order_ref` | `scenario_fulfillment_lines` | **B-tree** | `(order_ref)` |
| `idx_scenario_fulfill_sku` | `scenario_fulfillment_lines` | **B-tree** | `(sku)` |
| `idx_scenario_fulfill_order_sku` | `scenario_fulfillment_lines` | **B-tree** | `(order_ref, sku)` — compound |
| `idx_scenario_fulfill_brin_created` | `scenario_fulfillment_lines` | **BRIN** | `(created_at)` |

#### MongoDB (`demo`)

Compound indexes follow an **ESR** hint: **E**quality-leading keys, then **S**ort, then **R**ange (and tie-breakers such as `sku`) where it matches hub queries.

| Collection | Index name | Type | Keys / options |
|------------|------------|------|----------------|
| `scenario_products` | `esr_sku_unique` | Unique B-tree | `{ sku: 1 }` |
| `scenario_products` | `esr_updated_sku` | Compound B-tree | `{ updated_at: -1, sku: 1 }` |
| `scenario_products` | `esr_category_price_sku` | Compound B-tree | `{ category: 1, unit_price_cents: 1, sku: 1 }` |
| `scenario_products` | `partial_in_stock` | Partial compound B-tree | `{ category: 1, sku: 1 }` with `partialFilterExpression: { stock_units: { $gt: 0 } }` |
| `scenario_products` | `esr_warehouse_sku` | Compound B-tree | `{ warehouse: 1, sku: 1 }` |
| `scenario_products` | `text_title_desc` | **Text** | `title` + `description` (weights: title 10, description 1) |
| `demo_items` | `esr_name_id` | Compound B-tree | `{ name: 1, _id: 1 }` (if collection exists) |
| `demo_items` | `esr_name_qty` | Compound B-tree | `{ name: 1, qty: 1 }` (if collection exists) |
| `demo_items_from_kafka` | `sink_name_id` | Compound B-tree | `{ name: 1, _id: 1 }` (if collection exists) |

#### Cassandra (`demo_hub`)

| Object | Kind | Details |
|--------|------|---------|
| `scenario_timeline` | Table | Partition key `order_ref`, clustering `event_ts DESC`; columns `event_type`, `detail`. |
| `scenario_timeline_event_type` | **Secondary index** | On `event_type` — suitable only for **low-cardinality** demo filters; at scale prefer query-by-partition or a dedicated table. |

**Apply scripts** assume you run them from **`dashboards/demo`** with **`docker compose`** and the matching services up (see paths in the table above).

---

This docker-compose script starts a small cluster with some workloads running and the dashboards
in one easy command!

To use:

  1. **Recommended — full stack from this directory** (agent is built inside the **`mcac`** image; no host Maven required):

     ```bash
     chmod +x start-full-stack.sh
     ./start-full-stack.sh
     ```

  2. **Manual equivalent:** set `PROJECT_VERSION` from the root `pom.xml`, build images, then bring everything up:

     ```bash
     export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
     docker compose build mcac kafka-connect hub-demo-ui
     docker compose up -d
     ```

     Optional: build the agent on the host with `mvn -DskipTests package` before step 2 if you iterate on the agent locally; the **`mcac`** service still populates the shared volume from its image. Browser end-to-end test (**Postgres / Mongo / Redis / Cassandra / OpenSearch**): **http://localhost:8888** (see **`realtime-orders-search-hub/README.md`**).
     
  3. Open your web browser to [http://localhost:3000](http://localhost:3000)
  
  If you want to change the jsonnet dashboards, make your changes then run:

  ```bash
  ../grafana/make-dashboards.sh
  ```
  
  Refresh the browser to see changes.

### Running tlp-stress commands (list, run, etc.)

**Running container (`demo-stress-1`)**  
Started by compose: one **tlp-stress** workload (KeyValue) against the main cluster. You don’t run `list` or `run` inside it; use `docker run` below for ad-hoc tests.

**One-off commands (`docker run --rm ...`)**  
Use a **new** container when you want to run a different workload, or run `list` / `-h` / a short `run` test. The `tlp-stress` binary isn’t on `PATH` when you `docker exec` into the running stress containers, so use `docker run` for these:

  ```bash
  docker run --rm thelastpickle/tlp-stress:latest list
  docker run --rm --network mcac_net -e TLP_STRESS_CASSANDRA_HOST=cassandra thelastpickle/tlp-stress:latest run KeyValue --rate 50 -d 60s
  ```

- **`list`** does not need Cassandra or `--network`; it only prints available workloads.
- **`run ...`** needs `--network mcac_net` and `TLP_STRESS_CASSANDRA_HOST=cassandra` to talk to your cluster.

So you need **both**: the compose **stress** service for continuous demo load, and `docker run` for ad-hoc list/help/run.

### Cluster, datacenter and rack (by service)

This compose file runs a single **3-node “Test Cluster”** (SimpleSnitch, datacenter1/rack1): **`cassandra`**, **`cassandra2`**, **`cassandra3`**. Extra multi-datacenter / Secondary-cluster nodes were removed to reduce CPU and RAM; add them back in your own fork if you need topology labs.

| Service    | Cluster name   | DC           | Rack   |
|------------|----------------|--------------|--------|
| cassandra  | Test Cluster   | datacenter1  | rack1  |
| cassandra2 | Test Cluster   | datacenter1  | rack1  |
| cassandra3 | Test Cluster   | datacenter1  | rack1  |

### Commands you can run (from this directory)

All commands assume you are in `dashboards/demo` (this directory). Replace `demo-` with your project name if you used `-p` with docker compose.

**Start / stop**
```bash
export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
docker compose up -d
docker compose down
docker compose ps -a
```

**Remove leftover Cassandra containers** (from an older compose that had extra nodes / Secondary cluster). Current stack only defines **`cassandra`**, **`cassandra2`**, **`cassandra3`**:
```bash
docker rm -f demo-cassandra4-1 demo-cassandra5-1 demo-cassandra-standalone-1 \
  demo-dc2r2-node1-1 demo-dc2r2-node2-1 demo-dc2r2-node3-1 demo-dc2r2-node4-1 demo-dc1-node1-1 2>/dev/null || true
```

**MongoDB: 1 replica set (rs1) with 3 nodes + Prometheus + Grafana**  
No host ports 27017/27018/27019 (use 27201–27203). Metrics from mongodb-exporter are scraped by Prometheus and can be viewed in Grafana.
```bash
docker compose up -d prometheus grafana \
  mongo-rs1-node1 mongo-rs1-node2 mongo-rs1-node3 mongo-rs1-init mongodb-exporter
```
- **Host ports:** Node1 **27201**, Node2 **27202**, Node3 **27203**; Exporter **9216**; Prometheus **9090**; Grafana **3000**.
- **Connect from host:** `mongosh "mongodb://localhost:27201/?directConnection=true"` (or 27202/27203). For replica set awareness: `mongosh "mongodb://localhost:27201,localhost:27202,localhost:27203/?replicaSet=rs1"`. This is a **replica set**, not a sharded cluster — use **`rs.status()`** to see replica set status; do **not** use `sh.status()` (that is for sharded clusters with mongos).
- **Grafana:** Open [http://localhost:3000](http://localhost:3000), add or use existing Prometheus datasource (URL `http://prometheus:9090`). Query `mongodb_*` or import a MongoDB dashboard (e.g. Grafana.com dashboard 2583).
- **ARM Mac:** Services use `platform: linux/arm64`. On x86, set `platform: linux/amd64` (or remove the line) for mongo and mongodb-exporter in `docker-compose.yml`.
Wait ~2 minutes after `up` for init and exporter to be ready; then check `docker compose ps -a` (mongo-rs1-init should be Exited (0), mongodb-exporter Up). If **mongo-rs1-node3** or **mongo-rs1-init** stay **Created**, start them explicitly:
```bash
docker compose up -d mongo-rs1-node3
# wait ~1 min for node3 to be healthy, then:
docker compose up -d mongo-rs1-init mongodb-exporter
```

**Step-by-step: Remove MongoDB (rs1) only**  
From `dashboards/demo`, stop and remove only the MongoDB replica set and exporter (keeps Grafana, Prometheus, and other services running):
```bash
cd /path/to/metric-collector-for-apache-cassandra/dashboards/demo

docker compose stop mongodb-exporter mongo-rs1-init mongo-rs1-node3 mongo-rs1-node2 mongo-rs1-node1
docker compose rm -f mongodb-exporter mongo-rs1-init mongo-rs1-node3 mongo-rs1-node2 mongo-rs1-node1
```
Verify: `docker compose ps -a` should show no `mongo-rs1*` or `mongodb-exporter` containers.

**MongoDB sharded cluster (config servers + 2 shards + mongos)**  
To run a full sharded cluster so you can use `sh.status()`, `sh.enableSharding()`, `sh.shardCollection()`, etc., start the sharded stack and connect to **mongos** (not a shard node).
```bash
docker compose up -d prometheus grafana \
  mongo-config1 mongo-config2 mongo-config3 mongo-config-init \
  mongo-shard1-node1 mongo-shard1-node2 mongo-shard1-node3 mongo-shard1-init \
  mongo-shard2-node1 mongo-shard2-node2 mongo-shard2-node3 mongo-shard2-init \
  mongos mongo-cluster-init
```
- **Connect to sharded cluster (mongos):** `mongosh "mongodb://localhost:27300"`  
  Use this for all sharding commands: `sh.status()`, `sh.enableSharding("dbName")`, `sh.shardCollection("dbName.collName", { key: "hashed" })`.
- **Host ports:** mongos **27300**; config servers 27301–27303; shard1 27311–27313; shard2 27321–27323.
- Wait ~3–5 minutes for config init, shard inits, and cluster-init (addShard) to complete. Check `docker compose ps -a` (mongo-config-init, mongo-shard1-init, mongo-shard2-init, mongo-cluster-init should be Exited (0), mongos Up).

**Containers created by this compose**

Run from **this directory** (`dashboards/demo`) so compose sees the right project:
```bash
cd /path/to/metric-collector-for-apache-cassandra/dashboards/demo
docker compose ps -a
```
To see only demo containers with status:
```bash
docker ps -a --filter "name=demo-" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
```

**Understanding `docker ps` / `docker compose ps -a` status**

| STATUS | Meaning |
|--------|--------|
| **Up** (healthy) | Container is running and (if it has a healthcheck) healthy. This is what you want for long-lived services (mongod, cassandra, grafana, mongos). |
| **Up** | Container is running (no healthcheck or not yet healthy). |
| **Created** | Container exists but was **never started** (or is waiting for a dependency). Not running. Run `docker compose up -d` to start missing services. |
| **Exited (0)** | Container **finished successfully**. Normal for one-off init jobs (e.g. mongo-config-init, mongo-rs1-init). |
| **Exited (1)** or **Exited (3)** | Container **stopped with an error** (non-zero exit code). Check logs: `docker compose logs <service-name>`. |

- `docker ps` shows only **running** containers.
- `docker compose ps -a` (or `docker ps -a`) shows **all** containers (running, created, exited).  
If many services show **Created**, start them with: `docker compose up -d`.

**Clean up all MongoDB containers and reclaim Docker space**  
Removes every demo mongo container (rs1, config, shards, mongos, inits, exporter), then removes mongo images and prunes unused data to free disk space. Run from any directory:
```bash
# 1. Stop and remove all demo MongoDB-related containers (by name pattern)
docker rm -f $(docker ps -a -q --filter "name=demo-mongo") 2>/dev/null || true

# 2. Remove MongoDB images (optional: omit if you plan to use mongo again soon)
docker rmi mongo:8 percona/mongodb_exporter:0.40 2>/dev/null || true

# 3. Reclaim space: remove unused images, containers, networks (not volumes by default)
docker system prune -af

# 4. (Optional) Also remove unused volumes to reclaim more space
docker volume prune -f
```
**MongoDB rs1:** If `mongosh "mongodb://localhost:27201"` gives ECONNREFUSED, ensure the stack is up and wait ~2 min for mongo-rs1-init to finish. Check `docker compose ps -a` (mongo-rs1-init should be Exited (0), mongo-rs1-node1/2/3 and mongodb-exporter Up). If mongo-rs1-init failed, run `docker compose logs mongo-rs1-init --tail 30`.

**Cassandra – nodetool (3-node Test Cluster)**
```bash
docker exec "$(docker ps -q -f name=demo-cassandra-1)" nodetool status
docker exec "$(docker ps -q -f name=demo-cassandra2-1)" nodetool ring
```
(Replace container names if your project prefix is not `demo-`.)

**Cassandra – cqlsh (host ports: 19442, 19443, 19444 → nodes 1–3)**
```bash
docker exec -it "$(docker ps -q -f name=demo-cassandra-1)" cqlsh
# or from host: cqlsh 127.0.0.1 19442
```

**Shell in a Cassandra node**
```bash
docker exec -it "$(docker ps -q -f name=demo-cassandra-1)" /bin/bash
```

**Install nano in a Cassandra container** (optional; replace container id/name):
```bash
docker exec -u root "$(docker ps -q -f name=demo-cassandra-1)" bash -c 'apt-get update && apt-get install -y nano'
```

**Grafana**
- Open [http://localhost:3000](http://localhost:3000)

**Prometheus**
- Open [http://localhost:9090](http://localhost:9090)

**MongoDB dashboard shows "No data" – get metrics flowing**

Metrics path: **mongodb-exporter** (scraped by Prometheus every 15s) → **Prometheus** → **Grafana** (Prometheus datasource). If the MongoDB dashboard shows "No data", do this:

1. **Confirm the exporter and init completed**
   ```bash
   docker compose ps mongodb-exporter mongo-rs1-init
   ```
   - `mongodb-exporter` must be **running** (not "Exit 0" or missing). It only starts after `mongo-rs1-init` completes successfully.
   - If `mongo-rs1-init` failed, fix it (see "MongoDB rs1" above), then:
     ```bash
     docker compose up -d mongo-rs1-init mongodb-exporter
     ```

2. **Confirm Prometheus is scraping the MongoDB job**
   - Open [http://localhost:9090/targets](http://localhost:9090/targets).
   - Find the **mongodb** job; the target `mongodb-exporter:9216` should be **UP**.
   - If it shows **DOWN** with error **"lookup mongodb-exporter ... no such host"**, the **mongodb-exporter** container is not running (Docker removes its DNS entry when the container stops). Go back to step 1: get `mongo-rs1-init` to succeed and start `mongodb-exporter`, then wait ~30s and refresh the targets page.
   - If the target is still DOWN for another reason, check `docker compose logs mongodb-exporter` and that Prometheus and mongodb-exporter are on the same network (`demo_net` / `mcac_net`).

3. **Confirm Grafana can see MongoDB metrics**
   - In Grafana: **Explore** (compass icon) → choose **Prometheus**.
   - Run: `up{job="mongodb"}`. You should see `1` for `instance="mongodb-exporter:9216"`.
   - Then try: `mongodb_connections` or `mongodb_opcounters_insert_total`. If these return points, metrics are in Prometheus.

4. **Label / instance filter**
   - The MongoDB tic/tac/toe Grafana dashboards select the sharded exporter with **`{job="mongodb",instance="mongodb-exporter:9216"}`** (the service name inside the demo Compose network). That matches metrics even if your Prometheus does not set **`mongo_cluster`** (or uses extra labels like `cluster` / `shard_topology`).
   - If you scrape the same exporter under a **different** `instance` (e.g. `localhost:9216`), panels will be empty until you **edit the dashboard queries** to that instance or relabel in Prometheus to `instance="mongodb-exporter:9216"`.

5. **Dashboard variables (job / instance)**
   - If the dashboard has an **environment**, **job**, or **instance** dropdown, set **job** to `mongodb` and **instance** to `mongodb-exporter:9216` (or "All" if available).

6. **Time range**
   - Use **Last 15 minutes** or **Last 5 minutes** so recent scrapes are included; "Today so far" can miss data right after startup.

**TLP stress – help / list workloads / run (use new container)**
```bash
docker run --rm --network mcac_net -e TLP_STRESS_CASSANDRA_HOST=cassandra thelastpickle/tlp-stress:latest -h
docker run --rm thelastpickle/tlp-stress:latest list
docker run --rm --network mcac_net -e TLP_STRESS_CASSANDRA_HOST=cassandra thelastpickle/tlp-stress:latest run KeyValue --rate 50 -d 60s
```

**Logs**
```bash
docker logs demo-cassandra-1
docker logs demo-cassandra3-1 --tail 100
docker logs demo-stress-1
```
