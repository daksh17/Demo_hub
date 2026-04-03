# Multi-DB scenario flow (Faker + pipelines)

This document explains **which diagrams** describe the **http://localhost:8888/scenario** page, **how many integration paths** run, **source vs sink** roles, and **whether the Mongo catalog collection is sharded**.

Implementation: [`demo-ui/scenario.py`](demo-ui/scenario.py) (FastAPI handlers in [`demo-ui/app.py`](demo-ui/app.py)).

---

## 1. Which diagram explains this flow?

| Where | What to look at |
|--------|------------------|
| **Live UI** | **Scenario** → **Pipeline line diagram** (horizontal spine, steps 1–4) and the **vertical “Flow diagram”** in the sidebar (same four stages top → bottom). These match the page you see when you click the numbered buttons. |
| **This repo (static)** | **[`diagrams/01-sequence-order-flow.mmd`](diagrams/01-sequence-order-flow.mmd)** — end-to-end sequence closest to “order + search + side stores.” For **Mongo-heavy** paths see **`03-flowchart-mongo-path.mmd`**, for **Postgres / fulfillment** see **`02-flowchart-postgres-path.mmd`**, for **Cassandra + Redis + OpenSearch** see **`04-flowchart-cassandra-redis-os.mmd`**. Overview: **`00-component-context.mmd`**. Rendered **SVG** versions sit alongside each **`.mmd`** file. |

The hub’s main **[`README.md`](README.md)** also inlines those SVGs and collapsible Mermaid blocks for GitHub.

---

## 2. “Connectors” — what is actually running?

There are **two different meanings** of “connector”:

### A) Kafka Connect JVM connectors (Debezium, JDBC sink, …)

- The **Multi-DB scenario does not require any Kafka Connect connector** to move data. The demo uses **in-process Python**: **`kafka-python`** producers, **PyMongo**, **psycopg**, **HTTP** to OpenSearch, **redis-py**, **Cassandra driver**.
- The stack **can** run **Kafka Connect** for other demos (e.g. **`../mongo-kafka/`** with **`demo.demo_items`**). That is **separate** from the four **Scenario** buttons unless you explicitly register and run those connectors.

### B) Logical “pipelines” triggered from the UI (recommended wording)

When you use **Scenario**, you are running **four explicit operations** (four buttons). Think of them as **four integration steps**, not four Kafka Connect tasks:

| # | Python entry | Role |
|---|----------------|------|
| 1 | `op_seed_catalog` | Load generator → Mongo only |
| 2 | `op_pipeline_mongo_to_postgres_and_kafka` | **Batch sync**: Mongo → Postgres + Kafka + OpenSearch + Redis |
| 3 | `op_place_order` | **OLTP**: Mongo + Postgres read → Postgres + Kafka + OpenSearch + Cassandra + Redis |
| 4 | `op_pipeline_postgres_to_fulfillment_and_kafka` | **Batch sync**: Postgres → Postgres fulfillment + Kafka + OpenSearch + Cassandra (+ Redis summary refresh) |

So: **four** user-driven steps; **two** of them are **multi-store pipeline/sync** functions (steps **2** and **4**). **None** of these are “Kafka Connect connectors” unless you replace them with Connect in production.

---

## 3. Source vs sink (per step)

Full **Kafka** topic names are prefixed with **`scenario.`** in code:

- `scenario.catalog.changes`
- `scenario.orders.events`
- `scenario.pipeline.sync`

**OpenSearch** index for mirrored events: **`hub-scenario-pipeline`** (see `SCENARIO_PIPELINE_OS_INDEX` in `scenario.py`).

### Step 1 — Seed Mongo catalog

| Role | System | Detail |
|------|--------|--------|
| **Source** | **Faker** (in memory) | Synthetic product fields |
| **Sink** | **MongoDB** | `demo.scenario_products` (via **mongos**) |

No other store is written.

### Step 2 — Sync catalog → Postgres + Kafka + OpenSearch + Redis

| Role | System | Detail |
|------|--------|--------|
| **Source** | **MongoDB** | Reads up to **80** docs from `demo.scenario_products` |
| **Sink** | **PostgreSQL** | UPSERT `scenario_catalog_mirror` |
| **Sink** | **Kafka** | Produce `scenario.catalog.changes` |
| **Sink** | **OpenSearch** | Index same logical payload (direction `mongo→kafka+os`) |
| **Sink** | **Redis** | `LPUSH` recent list `scenario:kafka:recent`, refresh `scenario:dashboard:summary` |

### Step 3 — Place random order

| Role | System | Detail |
|------|--------|--------|
| **Source** | **MongoDB** | SKUs (up to 50 read) |
| **Source** | **PostgreSQL** | Prices from `scenario_catalog_mirror` when available |
| **Sink** | **PostgreSQL** | Insert `scenario_orders` |
| **Sink** | **Kafka** | `scenario.orders.events` |
| **Sink** | **OpenSearch** | Mirror payload (direction `api→kafka+os`) |
| **Sink** | **Cassandra** | `demo_hub.scenario_timeline` event `ORDER_PLACED` |
| **Sink** | **Redis** | `scenario:order:latest:<order_ref>` (1h TTL), recent list + dashboard summary |

### Step 4 — Fulfillment rows + Kafka + OpenSearch + Cassandra

| Role | System | Detail |
|------|--------|--------|
| **Source** | **PostgreSQL** | Orders **without** rows in `scenario_fulfillment_lines` yet (up to 20) |
| **Sink** | **PostgreSQL** | Insert `scenario_fulfillment_lines` |
| **Sink** | **Kafka** | `scenario.pipeline.sync` |
| **Sink** | **OpenSearch** | Mirror payload (direction `postgres→kafka+os`) |
| **Sink** | **Cassandra** | `FULFILLMENT_READY` on `scenario_timeline` |
| **Sink** | **Redis** | Dashboard summary refresh only (**no** new recent-list push in this function) |

---

## 4. Is `demo.scenario_products` sharded?

- **`hub-demo-ui`** uses **`MONGO_URI=mongodb://mongo-mongos1:27017`**, i.e. a **sharded-cluster router** (**mongos**), not a single standalone `mongod`.
- The script **`../mongo-kafka/prepare-demo-collections.sh`** enables sharding on database **`demo`** and runs **`sh.shardCollection`** only for:
  - `demo.demo_items`
  - `demo.demo_items_from_kafka`  
  (used by the **mongo-kafka / Debezium** demo path.)

**`demo.scenario_products`** is **created by the Scenario seed** when you first insert documents. It is **not** included in that **shardCollection** script. On a sharded cluster, such a collection is an **unsharded** collection: it lives on the **primary shard** until you explicitly call **`sh.shardCollection("demo.scenario_products", …)`**.

**Summary:** The **cluster** is sharded; the **scenario catalog collection** in this repo is **not configured as a sharded collection**—it is a normal collection accessed through **mongos**. To shard it like `demo_items`, add an idempotent **`shardCollection`** block for `demo.scenario_products` (and optional compound/hashed key) to your init script or run it once via **mongosh**.

---

## 5. Diagram: four-step pipeline (Mermaid)

Renders on **GitHub** and in Mermaid-capable viewers.

```mermaid
flowchart LR
  subgraph s1 [1 · Seed]
    F[ Faker ] --> M[( Mongo demo.scenario_products )]
  end

  subgraph s2 [2 · Sync]
    M2[( Mongo catalog )] --> PG1[( Postgres mirror )]
    M2 --> K1[ Kafka scenario.catalog.changes ]
    M2 --> OS1[ OpenSearch hub-scenario-pipeline ]
    M2 --> R1[ Redis summary + recent ]
  end

  subgraph s3 [3 · Order]
    M3[( Mongo SKUs )] --> PGO[ Postgres scenario_orders ]
    PGm[( Postgres mirror prices )] --> PGO
    PGO --> K2[ Kafka scenario.orders.events ]
    PGO --> OS2[ OpenSearch ]
    PGO --> CA1[( Cassandra ORDER_PLACED )]
    PGO --> R2[ Redis order cache + summary ]
  end

  subgraph s4 [4 · Fulfill]
    PGF[( Postgres orders )] --> PGL[( Postgres fulfillment_lines )]
    PGF --> K3[ Kafka scenario.pipeline.sync ]
    PGF --> OS3[ OpenSearch ]
    PGF --> CA2[( Cassandra FULFILLMENT_READY )]
    PGF --> R3[ Redis summary refresh ]
  end

  M -.->|same cluster| M2
  M2 --> M3
  PG1 --> PGm
  PGO --> PGF
```

---

## 6. Diagram: vertical summary (same as UI sidebar)

```mermaid
flowchart TB
  A[Seed: Faker → Mongo demo.scenario_products]
  B[Sync: Postgres mirror + Kafka + OpenSearch + Redis\nscenario.catalog.changes → hub-scenario-pipeline]
  C[Order: Postgres orders + Kafka + OS + Redis + Cassandra ORDER_PLACED]
  D[Fulfill: Postgres lines + Kafka + OS + Cassandra FULFILLMENT_READY]
  A --> B --> C --> D
```

---

## 7. Quick reference

| Artifact | Value |
|----------|--------|
| UI | `http://localhost:8888/scenario` |
| Catalog collection | `demo.scenario_products` |
| Postgres tables | `scenario_catalog_mirror`, `scenario_orders`, `scenario_fulfillment_lines` |
| Cassandra table | `demo_hub.scenario_timeline` |
| Redis keys | `scenario:dashboard:summary`, `scenario:kafka:recent`, `scenario:order:latest:*` |

For ports, stack startup, and OpenSearch **Discover**, see **[`README.md`](README.md)** and **[`../docker-compose.yml`](../docker-compose.yml)**.
