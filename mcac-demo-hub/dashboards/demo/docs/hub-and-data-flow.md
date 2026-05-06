# Hub UI & data flow (Compose and Kubernetes)

This page describes **application behavior** that is the same in Docker Compose and in the **demo-hub** namespace on Kubernetes (after you reach services via published ports or port-forward).

## Hub UI

| URL | Purpose |
|-----|---------|
| **http://localhost:8888** | Home: single-order demo, links to stores |
| **http://localhost:8888/workload** | Tunable batch load across Postgres, Mongo, Redis, Cassandra, OpenSearch, and optionally **SQL Server** (`demo.dbo.hub_workload_mssql` on the publisher) |
| **http://localhost:8888/scenario** | Multi-DB scenario (Faker, pipelines, Kafka topics from Python — independent of Kafka Connect unless you also register connectors) |
| **http://localhost:8888/kafka** | **Kafka lab:** broker metadata, produce bursts (acks / linger / batch / compression / keys), short consumer polls (explore ordering, groups, offsets) against **`KAFKA_BOOTSTRAP`** |

**Implementation:** [`deploy/docker/realtime-orders-search-hub/demo-ui/`](../deploy/docker/realtime-orders-search-hub/demo-ui/) (FastAPI). **Kafka lab (API, env, curriculum):** [`deploy/docker/realtime-orders-search-hub/demo-ui/README.md`](../deploy/docker/realtime-orders-search-hub/demo-ui/README.md). **Narrative + diagrams:** [`deploy/docker/realtime-orders-search-hub/README.md`](../deploy/docker/realtime-orders-search-hub/README.md), [`scenario-flow/README.md`](../deploy/docker/realtime-orders-search-hub/scenario-flow/README.md).

## Kafka Connect & CDC (Postgres + Mongo + optional SQL Server)

The stack can run **Debezium Postgres source + JDBC sink** and **Debezium Mongo source + Mongo sink** (**four** connectors), and **Debezium SQL Server source + JDBC sink to the subscriber** (**two** more). The hub’s “Single order” / “Workload” paths write **source** rows only:

- Postgres: **`public.demo_items`**
- Mongo: **`demo.demo_items`**
- SQL Server (when registered): publisher **`dbo.scenario_catalog_mirror_mssql`** (and related CDC setup from `mssql-kafka` init)

**Sink** tables/collections (**`demo_items_from_kafka`**, **`demo.demo_items_from_kafka`**, subscriber mirror) fill only when the corresponding connectors are **RUNNING**.

| Action | Command / note |
|--------|------------------|
| **Compose (automatic one-shots)** | **`kafka-connect-register`** runs [`register-all.sh`](../deploy/docker/kafka-connect-register/register-all.sh) (Postgres + Mongo). **`mssql-kafka-connect-register`** runs [`mssql-kafka/register-mssql-connectors.sh`](../deploy/docker/mssql-kafka/register-mssql-connectors.sh) after MSSQL schema jobs (SQL Server uses in-network **`kafka:29092`** for schema history). |
| **Compose / K8s — all six from host** | After Connect is up: [`register-all-connectors.sh`](../deploy/docker/kafka-connect-register/register-all-connectors.sh) — with **`DEMO_HUB_K8S=1`** also runs **[`apply-mssql-schema-k8s.sh`](../deploy/k8s/scripts/apply-mssql-schema-k8s.sh)** (unless **`SKIP_MSSQL_SCHEMA_APPLY=1`**), then **`register-all.sh`** and **`register-mssql-connectors.sh`**. Full cluster bring-up + registration: **[`../demo-start-hub.sh`](../demo-start-hub.sh)**. |
| **Compose (manual — PG+Mongo only)** | `./deploy/docker/kafka-connect-register/register-all.sh` |
| **Kubernetes (PG+Mongo)** | Port-forward Connect REST (see [`deploy/k8s/README.md`](../deploy/k8s/README.md)), then `DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all.sh http://127.0.0.1:8083` |
| **Kubernetes (PG+Mongo+MSSQL)** | Same port-forward, then `DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all-connectors.sh http://127.0.0.1:8083` — or rely on Job **`mssql-demo-bootstrap`** for MSSQL registration after Connect is Ready. |
| **Resume paused connectors** | `./deploy/docker/kafka-connect-register/resume-kafka-connectors.sh http://127.0.0.1:8083` |
| **Diagnose** | `./diagnose-kafka-connect.sh` (from `dashboards/demo`) |
| **Reset sinks / connectors** | `./reset-kafka-connect-demo.sh`, `./clean-kafka-connect-sinks.sh` |

## Typical service ports (host)

Published the same way in Compose; on K8s use **`port-forward-demo-hub.sh`** defaults unless you override **`LOCAL_*`** (see K8s README).

| Service | Default host port |
|---------|-------------------|
| Grafana | 3000 |
| Prometheus | 9090 |
| Hub UI | 8888 |
| Kafka (external) | 9092 |
| Kafka Connect REST | 8083 |
| Postgres primary | 15432 |
| Mongo mongos (first) | 27025 |
| Redis | 6379 |
| OpenSearch | 9200 |
| OpenSearch Dashboards | 5601 |

Cassandra CQL on the host uses mapped ports (**19442** / **19443** / **19444** in Compose); on K8s prefer port-forward to **`cassandra-0:9042`** or the optional NodePort recipe in the K8s README.

## Multi-DB scenario indexes (reference)

The long **per-store index tables** (Postgres BRIN/GIN/…, Mongo ESR, Cassandra secondary index) live in the main demo README so they stay next to the rest of the walkthrough:

→ **[Hub scenario indexes](../README.md#hub-scenario-indexes-multi-db-reference)**  
