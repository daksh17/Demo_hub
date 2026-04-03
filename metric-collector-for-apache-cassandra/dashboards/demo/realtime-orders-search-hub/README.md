# Realtime orders & search hub (reference scenario)

This folder describes a **single coherent real-time story** across everything in **`../docker-compose.yml`**:

| System | Role in this scenario |
|--------|------------------------|
| **PostgreSQL** | System of record for **orders** and payments-oriented rows; **Debezium** captures `demo_items`-style tables to Kafka (same pattern as **`../postgres-kafka/`**). |
| **MongoDB (sharded)** | Catalog / documents that change often; **Debezium Mongo** → Kafka (**`../mongo-kafka/`**). |
| **Kafka + Connect** | Event backbone; source and sink connectors you already run. |
| **Cassandra** | High-volume **order events** / timeline (append-heavy, wide rows)—good fit for MCAC metrics and **`../cassandra/`** topology. |
| **Redis** | **Hot cache**: latest order status, product availability snapshot, rate limits (**`../redis/README.md`**). |
| **OpenSearch** | **Full-text search** and customer-facing browse (indexes built from Kafka topics or denormalized writes). |
| **Prometheus + Grafana** | End-to-end observability (**`../observability/`**). |

This is a **reference architecture** you can implement incrementally: the repo already brings up the infrastructure; connectors and app code can follow the flows below.

---

## Scenario in one paragraph

A customer places an order: the **API** persists the order in **Postgres** and updates **Mongo** catalog stock. **Debezium** streams both changes to **Kafka**. A **consumer** (or **sink**) updates **OpenSearch** so the “my orders” and catalog search UI stay current; another path writes an **event** to **Cassandra** for analytics and pushes a **short-TTL cache** entry in **Redis** for the order status API. **Prometheus** scrapes brokers, DBs, Redis, OpenSearch exporters, and **Grafana** gives you one place to see lag, errors, and saturation.

---

## Entire workflow (diagrams)

**If you only see raw `flowchart` / `sequenceDiagram` text:** your preview does not render Mermaid. That is normal in **Cursor / VS Code** unless you add a Mermaid-capable Markdown preview (e.g. extension “Markdown Preview Mermaid Support”). **GitHub** renders Mermaid in `README.md` on the repo website. **Below each heading, the same diagram is also inlined as an SVG** so it shows in any viewer that supports images.

Canonical editable sources: [`.mmd` files in `diagrams/`](diagrams/).

### 1. Component context (stack + data paths)

![Component context — stack and data paths](diagrams/00-component-context.svg)

<details>
<summary>Mermaid source (for GitHub / compatible viewers)</summary>

```mermaid
flowchart TB
  subgraph clients [Clients]
    U[Browser / mobile app]
    API[Your API service]
  end

  subgraph rdbms [PostgreSQL HA demo]
    PG[(Primary :15432)]
    PR1[(Replica 1)]
    PR2[(Replica 2)]
  end

  subgraph mongo [MongoDB sharded]
    MS[mongos :27025 host]
    TIC[tic / tac / toe shards]
  end

  subgraph kafka_stack [Kafka plane]
    ZK[ZooKeeper]
    K[Kafka :9092]
    KC[Kafka Connect :8083]
  end

  subgraph nosql [Cassandra + cache + search]
    CS[(Cassandra cluster)]
    RD[(Redis :6379)]
    OS[(OpenSearch :9200)]
    OSD[Dashboards :5601]
  end

  subgraph obs [Observability]
    PROM[Prometheus :9090]
    GF[Grafana :3000]
  end

  U --> API
  API --> PG
  API --> MS
  API --> CS
  API --> RD
  API --> OS

  PG --> PR1
  PG --> PR2
  MS --> TIC

  PG --> KC
  MS --> KC
  KC --> K
  ZK --> K

  KC -. optional consumers / sinks .-> OS
  KC -. optional consumers .-> RD
  KC -. optional consumers .-> CS

  OSD --> OS

  PG --> PROM
  MS --> PROM
  K --> PROM
  CS --> PROM
  RD --> PROM
  OS --> PROM
  PROM --> GF
```

</details>

### 2. End-to-end sequence (one order)

![End-to-end sequence — one order](diagrams/01-sequence-order-flow.svg)

<details>
<summary>Mermaid source (for GitHub / compatible viewers)</summary>

```mermaid
sequenceDiagram
  autonumber
  participant App as API / worker
  participant PG as Postgres primary
  participant M as Mongos
  participant Connect as Kafka Connect
  participant K as Kafka
  participant C as Cassandra
  participant R as Redis
  participant O as OpenSearch
  participant OS as OpenSearch Dashboards

  Note over App, O: Step A — persist order (OLTP)
  App->>PG: INSERT order + line items
  PG-->>App: OK (order_id)

  Note over App, M: Step B — update catalog / stock (document)
  App->>M: update catalog collection (e.g. sku stockByWarehouse)
  M-->>App: OK

  Note over PG, K: Step C — CDC to Kafka (Debezium sources)
  PG--)Connect: logical replication / WAL events
  Connect->>K: produce demopg.public.*
  M--)Connect: change streams via mongos
  Connect->>K: produce demomongo.demo.*

  Note over App, K: Step D — fan-out (your microservices or Flink-style jobs)
  K--)App: consume order + catalog topics
  App->>C: INSERT INTO order_events PARTITION ...
  App->>R: SET order:status:{id} EX TTL
  App->>O: bulk index orders_search doc

  Note over App, OS: Step E — human visibility
  OS->>O: queries / dev tools
  App-->>OS: (optional) deep links from support UI
```

</details>

### 3. Postgres → Kafka → downstream (stepped)

![Postgres CDC stepped flow](diagrams/02-flowchart-postgres-path.svg)

<details>
<summary>Mermaid source (for GitHub / compatible viewers)</summary>

```mermaid
flowchart TD
  S1[Step 1: Application connects to Postgres primary host 15432]
  S2[Step 2: INSERT UPDATE DELETE on captured tables e.g. demo_items]
  S3[Step 3: WAL committed on primary replicas catch up physically]
  S4[Step 4: Debezium PostgresConnector polls replication slot]
  S5[Step 5: Serialize change event Avro or JSON to Kafka topic]
  S6[Step 6: Topic demopg schema table consumers subscribe]
  S7[Step 7a: JdbcSink writes demo_items_from_kafka not in publication loop safe]
  S8[Step 7b: Your worker updates OpenSearch index]
  S9[Step 7c: Your worker invalidates or updates Redis keys]
  S10[Step 7d: Your worker appends analytics row to Cassandra]

  S1 --> S2 --> S3 --> S4 --> S5 --> S6
  S6 --> S7
  S6 --> S8
  S6 --> S9
  S6 --> S10
```

</details>

### 4. Mongo (sharded) → Kafka → sink (stepped)

![Mongo sharded CDC stepped flow](diagrams/03-flowchart-mongo-path.svg)

<details>
<summary>Mermaid source (for GitHub / compatible viewers)</summary>

```mermaid
flowchart TD
  M1[Step 1: Application connects to mongos e.g. localhost 27025]
  M2[Step 2: Writes to sharded collection demo.demo_items]
  M3[Step 3: Hashed shard key routes doc to tic tac or toe shard]
  M4[Step 4: Debezium MongoDbConnector reads change stream via mongos]
  M5[Step 5: Events published to demomongo.demo.demo_items]
  M6[Step 6: MongoSinkConnector with ExtractNewDocumentState]
  M7[Step 7: Writes to demo.demo_items_from_kafka not captured loop safe]
  M8[Step 8: Optional consumer updates OpenSearch for catalog search]
  M9[Step 9: Optional consumer caches hot SKUs in Redis]

  M1 --> M2 --> M3 --> M4 --> M5 --> M6 --> M7
  M5 --> M8
  M5 --> M9
```

</details>

### 5. Cassandra, Redis, OpenSearch (writes vs reads)

![Cassandra, Redis, OpenSearch write/read paths](diagrams/04-flowchart-cassandra-redis-os.svg)

<details>
<summary>Mermaid source (for GitHub / compatible viewers)</summary>

```mermaid
flowchart LR
  subgraph ingest [From Kafka or API]
    K[Kafka topics]
    A[API direct]
  end

  subgraph write_path [Write paths]
    C1[Step 1: High volume timeline events to Cassandra]
    R1[Step 2: Short TTL read models in Redis]
    O1[Step 3: Denormalized docs bulk to OpenSearch]
  end

  subgraph read_path [Read paths]
    CQ[Analytics CQL time range by partition]
    RQ[Cache aside GET order status]
    OQ[Search browse fuzzy filters]
  end

  K --> C1
  K --> R1
  K --> O1
  A --> C1
  A --> R1
  A --> O1

  C1 --> CQ
  R1 --> RQ
  O1 --> OQ
```

</details>

---

## Phase map (what to build vs what exists)

| Phase | Status in repo | Your work |
|-------|----------------|-----------|
| Infra up | Compose: PG, Mongo sharded, Kafka, Connect, Cassandra, Redis, OpenSearch, Prometheus, Grafana | `docker compose up` from **`dashboards/demo`** per area READMEs |
| Postgres CDC | **`postgres-kafka/register-connectors.sh`** | Tables, publication, sink table not in publication |
| Mongo CDC | **`mongo-kafka/register-mongo-connectors.sh`** + **`mongo-kafka-prepare`** | Ensure topics and sinks match naming |
| Cassandra writes | MCAC agent on cassandra nodes | App or batch job writing order_events |
| Redis | **`redis`** + password | App: SET order:{id} with TTL |
| OpenSearch | **`opensearch`** + Dashboards | Index pipeline from Kafka or REST bulk |
| Metrics | **`prometheus.yaml`** jobs | Restart Prometheus after edits |

---

## Step-by-step workflows (by data plane)

### A. Postgres row → Kafka → (optional) sink / consumers

1. App **INSERT/UPDATE** on primary (e.g. `demo_items` or `orders`) — host port **15432** (`../postgres-kafka/README.md`).
2. WAL + **logical replication** on primary; **PostgresConnector** in Connect consumes slot (**`8083`**).
3. Event on topic (e.g. `demopg.public.demo_items`).
4. **JdbcSinkConnector** (or your consumer) can mirror to another table **not** in the publication to avoid loops.
5. **Your service** (not in compose) consumes the topic → **OpenSearch** bulk index / **Redis** cache invalidate-set / **Cassandra** insert.

### B. Mongo document → Kafka → sink

1. App writes **`demo.demo_items`** via **mongos** (**`../mongo-kafka/README.md`**).
2. **MongoDbConnector** (change streams via mongos) → topic (e.g. `demomongo.demo.demo_items`).
3. **Mongo sink** → `demo.demo_items_from_kafka` (loop-safe; separate collection).
4. Same **your service** pattern: fan-out to **OpenSearch**/ **Redis** / analytics.

### C. Cassandra + Redis + OpenSearch (application pattern)

1. **Cassandra**: append **order_events** (clustering by time, partition by `order_id` or tenant)—see **`../cassandra/README.md`** for CQL access.
2. **Redis**: `SET order:status:{id} {json} EX 300` after each state change (read-mostly API).
3. **OpenSearch**: index **`orders-search-{id}`** with denormalized fields for search; refresh policy per SLA.

### D. Observability (always on)

1. **Prometheus** **http://localhost:9090/targets** — PG exporters, `kafka_pgdemo`, `mongodb`, `redis_demo`, `opensearch_demo`, `mcac`, etc. Restart after **`prometheus.yaml`** changes.
2. **Grafana** **http://localhost:3000** — Kafka, Redis, Mongo, Cassandra dashboards in **`../../grafana/generated-dashboards/`**.

---

## Diagrams (Mermaid sources + rendered SVG)

The same diagrams are **embedded above** under **Entire workflow (diagrams)**. Edit the **`.mmd`** files in [`diagrams/`](diagrams/) as the canonical source, then refresh the README copy if you change structure or labels. Each diagram also has a **`.svg`** for viewers that do not render Mermaid. Regenerate SVGs after editing with the commands in the next section.

| Source | Rendered | Contents |
|--------|----------|-----------|
| [`diagrams/00-component-context.mmd`](diagrams/00-component-context.mmd) | [`diagrams/00-component-context.svg`](diagrams/00-component-context.svg) | All services and external “app” on one diagram |
| [`diagrams/01-sequence-order-flow.mmd`](diagrams/01-sequence-order-flow.mmd) | [`diagrams/01-sequence-order-flow.svg`](diagrams/01-sequence-order-flow.svg) | End-to-end **sequence** (`autonumber`) |
| [`diagrams/02-flowchart-postgres-path.mmd`](diagrams/02-flowchart-postgres-path.mmd) | [`diagrams/02-flowchart-postgres-path.svg`](diagrams/02-flowchart-postgres-path.svg) | **Stepped** Postgres CDC path |
| [`diagrams/03-flowchart-mongo-path.mmd`](diagrams/03-flowchart-mongo-path.mmd) | [`diagrams/03-flowchart-mongo-path.svg`](diagrams/03-flowchart-mongo-path.svg) | **Stepped** Mongo CDC path |
| [`diagrams/04-flowchart-cassandra-redis-os.mmd`](diagrams/04-flowchart-cassandra-redis-os.mmd) | [`diagrams/04-flowchart-cassandra-redis-os.svg`](diagrams/04-flowchart-cassandra-redis-os.svg) | Cassandra, Redis, OpenSearch fan-out |

---

## Regenerate SVGs (from this directory)

```bash
cd realtime-orders-search-hub
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/00-component-context.mmd -o diagrams/00-component-context.svg -b transparent
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/01-sequence-order-flow.mmd -o diagrams/01-sequence-order-flow.svg -b transparent
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/02-flowchart-postgres-path.mmd -o diagrams/02-flowchart-postgres-path.svg -b transparent
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/03-flowchart-mongo-path.mmd -o diagrams/03-flowchart-mongo-path.svg -b transparent
npx --yes @mermaid-js/mermaid-cli@11.4.0 -i diagrams/04-flowchart-cassandra-redis-os.mmd -o diagrams/04-flowchart-cassandra-redis-os.svg -b transparent
```

---

## Related docs

| Topic | Link |
|-------|------|
| Demo index | [`../README.md`](../README.md) |
| Postgres + Kafka | [`../postgres-kafka/README.md`](../postgres-kafka/README.md) |
| Mongo + Kafka | [`../mongo-kafka/README.md`](../mongo-kafka/README.md) |
| Mongo sharding | [`../mongo-sharded/README.md`](../mongo-sharded/README.md) |
| Cassandra | [`../cassandra/README.md`](../cassandra/README.md) |
| Kafka / Connect | [`../kafka/README.md`](../kafka/README.md) |
| Redis | [`../redis/README.md`](../redis/README.md) |
| OpenSearch | [`../opensearch/README.md`](../opensearch/README.md) |
| Observability | [`../observability/README.md`](../observability/README.md) |
