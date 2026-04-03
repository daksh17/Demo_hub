# PostgreSQL, Debezium, and Kafka (demo stack)

Part of **`dashboards/demo`**: Postgres, Kafka, Connect, and exporters are defined in **`../docker-compose.yml`**. Shared broker docs: **[`../kafka/README.md`](../kafka/README.md)**. Metrics UI: **[`../observability/README.md`](../observability/README.md)**.

This folder documents the **PostgreSQL primary + two streaming replicas**, **Apache Kafka** with **ZooKeeper**, **Debezium Kafka Connect** (CDC in both directions via source + JDBC sink), and **observability** wired into the same Prometheus and Grafana used by the MCAC demo.

## Architecture

- **PostgreSQL (Bitnami, `docker.io/bitnami/postgresql:latest` in compose):** one writable **primary** and two **read replicas** (physical streaming replication). Logical decoding (`wal_level=logical`) is enabled on the primary for Debezium. Bitnami often drops old revision pins; for production pin an image **digest** instead of `:latest`.
- **Kafka:** single broker (Confluent 7.6.1) for demo simplicity; clients inside Docker use `kafka:29092`, clients on the host often use `localhost:9092`.
- **Kafka Connect (Debezium 2.7):** REST API on **8083**.
  - **PostgreSQL → Kafka:** `PostgresConnector` captures changes from `public.demo_items` into topics prefixed with `demopg`.
  - **Kafka → PostgreSQL:** `JdbcSinkConnector` writes to table `demo_items_from_kafka` (avoids feeding the sink back into the same CDC table).
- **Monitoring:** `postgres_exporter` (one per Postgres instance) and `danielqsj/kafka-exporter` expose metrics to **Prometheus**; **Grafana** loads a bundled dashboard that charts connections, throughput, DB size, replication lag, offsets, and consumer lag.

## How the workflow works

1. **Writes** go to the **primary** only (`demo_items`, and any other tables you add to the publication). The primary writes the **WAL** (write-ahead log).
2. **Replicas** apply the WAL over the replication protocol (streaming). They are read-mostly copies of the same data; Debezium still reads CDC from the **primary** because logical decoding runs there.
3. **Debezium Postgres connector** (running inside **Kafka Connect**) connects to the primary as user **`replicator`** (Bitnami replication user with `SELECT` on captured tables), uses publication **`dbz_publication`** and a **replication slot** to read logical change events, and publishes them to Kafka topics (e.g. `demopg.public.demo_items`).
4. **Debezium JDBC sink connector** subscribes to those topics and executes **INSERT/UPSERT** against the primary into **`demo_items_from_kafka`**. That table is intentionally **not** in the CDC publication, so you do not get an infinite loop (sink writes would otherwise be captured again if they went to `demo_items`).
5. **Grafana** queries **Prometheus**, which scrapes **postgres_exporter** (per node) and **kafka_exporter** so you can see connections, replication lag, offsets, and consumer lag.

### Component diagram

![PostgreSQL cluster, Kafka, Kafka Connect, and observability](diagrams/workflow-components.svg)

_Source: [`diagrams/workflow-components.mmd`](diagrams/workflow-components.mmd). Regenerate the SVG (from this directory):_

`npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/workflow-components.mmd -o diagrams/workflow-components.svg -b transparent`

### CDC and round-trip sequence

![Change data capture sequence: demo_items → Kafka → demo_items_from_kafka](diagrams/cdc-sequence.svg)

_Source: [`diagrams/cdc-sequence.mmd`](diagrams/cdc-sequence.mmd). Regenerate:_

`npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/cdc-sequence.mmd -o diagrams/cdc-sequence.svg -b transparent`

## Step-by-step walkthrough (detailed)

The numbered list above is the short version. Below is the same flow in order, with a bit more context.

### 1. Start the infrastructure

- **ZooKeeper** and **Kafka** come up so there is a broker to produce to and consume from.
- The **PostgreSQL primary** starts with `wal_level=logical` (required for Debezium). On first boot, `01-init-debezium.sql` creates **`demo_items`**, grants **`replicator`** for CDC, and publication **`dbz_publication`**.
- The **two replicas** start and **stream WAL from the primary** (physical replication). They lag slightly but carry the same database content for reads.
- **Kafka Connect** (Debezium image) connects to Kafka; you still **register connectors** with `register-connectors.sh` (REST on port **8083**).
- **postgres_exporter** (×3), **kafka-exporter**, **Prometheus**, and **Grafana** provide metrics and dashboards (see below).

### 2. Normal writes (application path)

1. An app or `psql` connects to the **primary** (host **15432**).
2. You run `INSERT` / `UPDATE` / `DELETE` on **`demo_items`** (and any other table you add to the publication).
3. The primary **records the transaction in the WAL** and commits.
4. **Replicas** apply that WAL, so they reflect the same rows on **`demo_items`** (and the rest of the DB), usually with small lag.

### 3. Change capture: PostgreSQL → Kafka

1. The **PostgresConnector** in Kafka Connect opens a **replication connection** to the primary as **`replicator`** (with **`SELECT`** on `demo_items`).
2. It uses a **logical replication slot** and the **publication** so Postgres emits **row-level change events** from the WAL for tables in the publication (here **`demo_items`**).
3. Connect serializes those events and **produces** them to Kafka topics such as **`demopg.public.demo_items`** (topic prefix + schema + table).

So every committed change on captured tables becomes one or more messages on Kafka.

### 4. Optional round-trip: Kafka → PostgreSQL (sink)

1. The **JdbcSinkConnector** consumes from the configured topic(s).
2. It runs **INSERT** / **UPSERT** on the **primary** into **`demo_items_from_kafka`**.
3. That sink table is **not** in **`dbz_publication`**, so those writes are **not** fed back into the same CDC pipeline. That prevents a **loop** (CDC → Kafka → sink → same captured table → CDC again).

### 5. How the diagrams relate

- **Component diagram:** who talks to whom—applications → primary; replicas ← primary; Connect reads the primary and talks to Kafka; sink writes the primary; exporters query Postgres/Kafka; Prometheus scrapes exporters; Grafana queries Prometheus.
- **Sequence diagram:** one write on **`demo_items`** → WAL / publication → Connect poll → Kafka → sink → **`demo_items_from_kafka`**, with the note that the sink table is outside the publication.

### 6. What replicas do not do (in this demo)

- **Debezium reads CDC only from the primary**—logical decoding is tied to the primary WAL.
- Replicas are for **read scaling** and **copies of the same data**, not a second CDC source.

### One-row example

After connectors are running, an insert on the primary flows like this: **`INSERT INTO demo_items …`** → WAL + publication → **PostgresConnector** produces to **`demopg.public.demo_items`** → **JdbcSinkConnector** may write a corresponding row into **`demo_items_from_kafka`**. Replicas show the change in **`demo_items`** via streaming replication; they do not emit Kafka events themselves.

## Ports (host)

| Service | Port |
|--------|------|
| PostgreSQL primary | **15432** |
| PostgreSQL replica 1 | **15433** |
| PostgreSQL replica 2 | **15434** |
| Kafka (PLAINTEXT_HOST listener) | **9092** |
| ZooKeeper | **2181** |
| Kafka Connect REST | **8083** |
| kafka-exporter (optional direct scrape / debugging) | **9308** |
| Prometheus (from main compose) | **9090** |
| Grafana (from main compose) | **3000** |

## Credentials

| Purpose | User | Password |
|--------|------|----------|
| Application / JDBC sink | `demo` | `demopass` |
| Physical replication + Debezium PostgresConnector (CDC) | `replicator` | `replicatorpass` |

Database name: **`demo`**. Demo table: **`demo_items`**; sink table (created by the sink connector): **`demo_items_from_kafka`**.

## Start the stack

From `dashboards/demo` (same directory as `docker-compose.yml`):

```bash
docker compose up -d zookeeper kafka postgresql-primary postgresql-replica-1 postgresql-replica-2 \
  kafka-connect postgres-exporter-primary postgres-exporter-replica-1 postgres-exporter-replica-2 kafka-exporter
```

Start **Prometheus** and **Grafana** if they are not already running (they scrape/listen on the shared Docker network `mc_net`):

```bash
docker compose up -d prometheus grafana
```

The Prometheus config includes jobs **`postgres_pgdemo`** and **`kafka_pgdemo`**. After a minute, confirm targets are up:

```bash
curl -s 'http://localhost:9090/api/v1/targets' | head -c 2000
```

## MongoDB sharded cluster + Kafka (optional)

The same **ZooKeeper**, **Kafka** broker, and **Kafka Connect** instance used for PostgreSQL can run a **second CDC pipeline** against the demo **sharded MongoDB** cluster (config replica set **`configReplSet`**, shards **`tic`**, **`tac`**, **`toe`**, three **`mongos`** routers). Scripts and the custom Connect image live under **`../mongo-kafka/`** (short reference: [mongo-kafka/README.md](../mongo-kafka/README.md)).

### Architecture (Mongo + Kafka)

- **Topology (nine data-plane containers):** three **config servers**, three **shard replica sets** (one mongod each in this demo), three **`mongos`** processes. Clients (and Debezium) talk to **`mongos`** on **`mongodb://mongo-mongos1:27017`** inside the Compose network; on the host, **mongos1** is mapped to **27025** (see main `docker-compose.yml`).
- **CDC source:** Debezium **`MongoDbConnector`** uses MongoDB **change streams** ( **`capture.mode` = `change_streams_update_full`** ) via **`mongos`**—the supported path for a **sharded cluster** in current Debezium releases.
- **Kafka topics:** logical prefix **`demomongo`**. Per Debezium naming, captured collection **`demo.demo_items`** produces topic **`demomongo.demo.demo_items`**.
- **Sink:** The official **`MongoSinkConnector`** ( **`-all`** JAR from Maven Central, bundled in the custom Connect image) consumes that topic and writes **`demo.demo_items_from_kafka`**. The sink applies **`io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`** so the Debezium envelope becomes plain document fields the sink can persist (**do not** use **`ExtractNewRecordState`** here—it expects a JDBC-style `Struct` and fails on Mongo’s JSON-style payloads).
- **Prepare step:** Compose service **`mongo-kafka-prepare`** runs **`prepare-demo-collections.sh`** after **`mongo-shard-add`** completes: **`sh.enableSharding("demo")`**, **`shardCollection`** on **`demo.demo_items`** and **`demo.demo_items_from_kafka`**, seed inserts on **`demo_items`**.
- **Loop safety:** **`collection.include.list`** is only **`demo.demo_items`**. The sink target collection is **not** captured, so **CDC → Kafka → sink** does not feed back into the source stream (same pattern as **`demo_items_from_kafka`** in Postgres).
- **Connect image:** **`docker compose build kafka-connect`** builds **`mcac-demo/kafka-connect:2.7.3-mongo-sink`** from **`../mongo-kafka/Dockerfile.connect`** (Debezium **`connect:2.7.3.Final`** + **`mongo-kafka-connect-1.14.1-all.jar`**).

### How the Mongo workflow works (short)

1. Applications or **`mongosh`** issue writes to **`demo.demo_items`** through **`mongos`**; documents land on the appropriate shard (tic / tac / toe).
2. **`mongo-source-demo`** reads the **change stream** through **`mongos`**, emits Debezium events, and **produces** to **`demomongo.demo.demo_items`**.
3. **`mongo-sink-demo`** **consumes** that topic, runs **ExtractNewDocumentState**, and **writes** documents into **`demo.demo_items_from_kafka`** via **`mongos`**.
4. **Prometheus** scrapes **`mongodb-exporter`** (targeting **`mongo-mongos1`**) and **kafka-exporter**; **Grafana** can show the **MongoDB tic/tac/toe** overview dashboard (`mongodb-tictactoe-overview.json` in `generated-dashboards`).

### Mongo component diagram

![MongoDB sharded cluster, Kafka, Kafka Connect, prepare job, observability](diagrams/mongo-workflow-components.svg)

_Source: [`diagrams/mongo-workflow-components.mmd`](diagrams/mongo-workflow-components.mmd). Regenerate the SVG (from `postgres-kafka`):_

`npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/mongo-workflow-components.mmd -o diagrams/mongo-workflow-components.svg -b transparent`

### Mongo CDC and round-trip sequence

![MongoDB change capture: demo_items → Kafka → demo_items_from_kafka](diagrams/mongo-cdc-sequence.svg)

_Source: [`diagrams/mongo-cdc-sequence.mmd`](diagrams/mongo-cdc-sequence.mmd). Regenerate:_

`npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/mongo-cdc-sequence.mmd -o diagrams/mongo-cdc-sequence.svg -b transparent`

### Mongo step-by-step walkthrough

1. **Start Mongo dependencies** (from `dashboards/demo`): config servers, shard nodes, **`mongo-shard-init-rs`**, **`mongos`** ×3, **`mongo-shard-add`**, then **`mongo-kafka-prepare`**. Ensure **Kafka** and **ZooKeeper** are up if you have not already started the Postgres demo stack.
2. **Build and start Kafka Connect** so the worker loads the **Mongo sink** plugin:  
   `docker compose build kafka-connect`  
   `docker compose up -d kafka-connect`
3. **Register connectors** from the repo (paths relative to `dashboards/demo`):  
   `chmod +x mongo-kafka/register-mongo-connectors.sh`  
   `./mongo-kafka/register-mongo-connectors.sh`  
   Optional URL: `./mongo-kafka/register-mongo-connectors.sh http://localhost:8083`
4. **Confirm connector state:**  
   `curl -s http://localhost:8083/connectors/mongo-source-demo/status`  
   `curl -s http://localhost:8083/connectors/mongo-sink-demo/status`  
   Both connectors should show **`RUNNING`** tasks after the initial snapshot.

### Mongo ports (host)

| Service | Port (typical) |
|---------|-----------------|
| mongos 1 | **27025** |
| mongos 2 | **27026** |
| mongos 3 | **27027** |
| Kafka (same as Postgres flow) | **9092** |
| Kafka Connect REST | **8083** |
| mongodb-exporter (optional) | **9216** |
| Prometheus / Grafana | **9090** / **3000** |

### Mongo connectors (reference)

| Name | Class | Role |
|------|--------|------|
| **`mongo-source-demo`** | `io.debezium.connector.mongodb.MongoDbConnector` | CDC from **`demo.demo_items`**; **`topic.prefix`** **`demomongo`**; connection **`mongodb://mongo-mongos1:27017`**. |
| **`mongo-sink-demo`** | `com.mongodb.kafka.connect.MongoSinkConnector` | Consumes **`demomongo.demo.demo_items`**; **`connection.uri`** **`mongodb://mongo-mongos1:27017`**; writes **`demo.demo_items_from_kafka`**; **SMT** **`ExtractNewDocumentState`**. |

### Mongo quick verification

```bash
# From dashboards/demo — compare source vs sink collections on mongos
docker compose exec mongo-mongos1 mongosh demo --eval 'db.demo_items.find().limit(3); db.demo_items_from_kafka.find().limit(3)'
```

Insert a test document on the host (example uses **mongos1** port **27025**):

```bash
mongosh "mongodb://127.0.0.1:27025/demo" --eval 'db.demo_items.insertOne({ name: "cdc-test", qty: 42 })'
```

After a short delay, the same logical document (with flattened **`_id`**) should appear in **`demo_items_from_kafka`** if both connectors are healthy.

### Mongo troubleshooting

1. **Sink task `FAILED` with “Only Struct objects supported … found: java.lang.String”** — the sink is using **`ExtractNewRecordState`**. Use **`io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`** (as in **`register-mongo-connectors.sh`**).
2. **`mongo-source-demo` cannot connect** — ensure **`mongos`** is healthy and the URI uses **`mongo-mongos1:27017`** from **inside** the Connect container (not `localhost`).
3. **No topics or empty sink** — run **`mongo-kafka-prepare`** successfully once; confirm **`sh.status()`** shows **`demo`** enabled and **`demo_items`** sharded. Re-run **`./mongo-kafka/register-mongo-connectors.sh`** after fixing the cluster.
4. **Connect missing `MongoSinkConnector`** — rebuild the image: **`docker compose build kafka-connect`** and recreate the **`kafka-connect`** container.

## Register Debezium connectors

When **Kafka Connect** answers on [http://localhost:8083](http://localhost:8083):

```bash
chmod +x register-connectors.sh
./register-connectors.sh
# or: ./register-connectors.sh http://localhost:8083
```

The script registers `pg-source-demo` and `jdbc-sink-demo`. To list connectors:

```bash
curl -s http://localhost:8083/connectors
```

To remove and re-register, delete connectors via the REST API (sink first), then run the script again.

### `register-connectors.sh` stops after “Kafka Connect is up” and `curl …/connectors` is `[]`

The script **POST** to create connectors failed. Older versions used `curl -f` and exited **without printing** Kafka Connect’s error body.

1. Run **`./register-connectors.sh` again** from this directory; on failure it now prints **HTTP status and JSON** (for example `password authentication failed for user "replicator"`).
2. **Publication or `replicator` privileges missing** (common on volumes created before init was updated): apply grants + publication, then register again (from `dashboards/demo`):

   ```bash
   chmod +x postgres-kafka/apply-ensure-debezium.sh
   ./postgres-kafka/apply-ensure-debezium.sh
   ./postgres-kafka/register-connectors.sh
   ```

3. **`localhost:8083` is not this project’s Connect** (other tool bound to 8083, or port-forward to the wrong pod): you will see an empty list or odd errors. Check `docker compose ps kafka-connect` and publish mapping **8083**.

## Grafana

1. Open [http://localhost:3000](http://localhost:3000) (anonymous access is enabled in the demo compose).
2. Open **Dashboards → PostgreSQL & Kafka (Debezium demo)**.

The dashboard lives in the repo as:

`dashboards/grafana/generated-dashboards/postgres-kafka-overview.json`

It is picked up automatically because Grafana provisioning mounts the whole `generated-dashboards` directory.

Panels include:

- Active connections and commit rate per **primary / replica** (`pg_role` label).
- Database size and **replication lag** (when exported for replicas).
- **Kafka** topic end offsets and **consumer group lag** (including Connect consumer groups).

For deeper Kafka or Postgres dashboards, you can import community dashboards from [Grafana.com](https://grafana.com/grafana/dashboards/) (search “Kafka exporter”, “PostgreSQL Database”) and point them at the existing **Prometheus** datasource named `prometheus`.

### Grafana shows **No data** on “PostgreSQL & Kafka (Debezium demo)”

1. **Postgres exporter v0.17+** needs `DATA_SOURCE_URI` = `host:port/db?…` only, with **`DATA_SOURCE_USER`** / **`DATA_SOURCE_PASS`** set separately. A full `postgresql://user:pass@…` URI in `DATA_SOURCE_URI` breaks the DSN (`pg_up` stays 0). The compose file in this repo is fixed; recreate exporters:  
   `docker compose up -d postgres-exporter-primary postgres-exporter-replica-1 postgres-exporter-replica-2`
2. **Reload Prometheus** after editing `dashboards/prometheus/prometheus.yaml` (scrape jobs `postgres_pgdemo` / `kafka_pgdemo`):  
   `docker compose restart prometheus`
3. Confirm metrics exist (use `--data-urlencode` so shells do not break `{job="…"}`):  
   `curl -s -G 'http://localhost:9090/api/v1/query' --data-urlencode 'query=pg_up{job="postgres_pgdemo"}'`  
   `curl -s -G 'http://localhost:9090/api/v1/query' --data-urlencode 'query=pg_stat_database_numbackends{job="postgres_pgdemo",datname="demo"}'`
4. In Grafana, set the time range to **Last 1 hour** (or longer) in case you are viewing a gap before scrapers were fixed.

## Dummy / seed data

- **New databases:** `01-init-debezium.sql` inserts **10 named seed rows** (e.g. “Acme Corporation”, “Globex Industries”, …) when the primary volume is first initialized.
- **Already-running primary:** load more rows anytime from this directory:

```bash
psql "postgresql://demo:demopass@127.0.0.1:15432/demo" -f seed-dummy-data.sql
```

That adds **30** additional rows (`bulk-<n>-<time>` style names). Re-run to append more. From the demo compose directory without local `psql`:

```bash
docker compose exec -T postgresql-primary bash -lc 'PGPASSWORD=demopass psql -U demo -d demo -f -' < postgres-kafka/seed-dummy-data.sql
```

## Quick functional test

```bash
psql "postgresql://demo:demopass@127.0.0.1:15432/demo" -c "INSERT INTO demo_items (name) VALUES ('grafana-test');"
psql "postgresql://demo:demopass@127.0.0.1:15433/demo" -c "SELECT * FROM demo_items ORDER BY id DESC LIMIT 3;"
```

After CDC and the sink run, check the sink table on the primary:

```bash
psql "postgresql://demo:demopass@127.0.0.1:15432/demo" -c "SELECT * FROM demo_items_from_kafka ORDER BY id DESC LIMIT 3;"
```

## Resetting Postgres data

If you need a clean primary volume (re-run `01-init-debezium.sql` init):

```bash
docker compose stop postgresql-primary postgresql-replica-1 postgresql-replica-2 kafka-connect \
  postgres-exporter-primary postgres-exporter-replica-1 postgres-exporter-replica-2
docker compose rm -f postgresql-primary postgresql-replica-1 postgresql-replica-2
docker volume rm demo_postgres_primary_data demo_postgres_replica1_data demo_postgres_replica2_data
```

Adjust volume names if your Compose project name is not `demo` (`docker volume ls | grep postgres`).

## Files in this directory

| File | Role |
|------|------|
| `01-init-debezium.sql` | Creates `demo_items`, `replicator` grants, publication `dbz_publication`, and **10 seed rows** on first primary boot. |
| `ensure-debezium-cdc.sql` | Grants + publication for existing volumes (see `apply-ensure-debezium.sh`). |
| `apply-ensure-debezium.sh` | Applies `ensure-debezium-cdc.sql` as `demo` (fixes old DBs missing publication/`replicator` **SELECT**). |
| `seed-dummy-data.sql` | Inserts **30** extra `demo_items` rows (run anytime on an existing DB). |
| `register-connectors.sh` | Registers Debezium PostgreSQL source and JDBC sink via Connect REST API. |
| `diagrams/*.mmd` | Mermaid source for README diagrams (Postgres + Mongo). |
| `diagrams/*.svg` | Rendered diagrams embedded in this README. |
| `diagrams/mongo-workflow-components.mmd` / `.svg` | Mongo sharding + Connect + observability component diagram. |
| `diagrams/mongo-cdc-sequence.mmd` / `.svg` | Mongo CDC → Kafka → sink sequence diagram. |
| `README.md` | This document. |

Mongo connector scripts and **Connect Dockerfile** live in **`../mongo-kafka/`**. **Single** service file: `../docker-compose.yml`. Other area guides: **`../cassandra/README.md`**, **`../kafka/README.md`**, **`../observability/README.md`**, **`../mongo-sharded/README.md`**.
