# PostgreSQL, Debezium, and Kafka (demo stack)

Part of **`dashboards/demo`**: Postgres, Kafka, Connect, and exporters are defined in **`../docker-compose.yml`**. Shared broker docs: **[`../kafka/README.md`](../kafka/README.md)**. Metrics UI: **[`../observability/README.md`](../observability/README.md)**.

This folder documents the **PostgreSQL primary + two streaming replicas**, **Apache Kafka** with **ZooKeeper**, **Debezium Kafka Connect** (CDC in both directions via source + JDBC sink), and **observability** wired into the same Prometheus and Grafana used by the MCAC demo.

**Debezium concepts (official-doc summary):** see **[`DEBEZIUM.md`](DEBEZIUM.md)** — PostgreSQL source vs JDBC sink, permissions, snapshots, and links to [debezium.io](https://debezium.io/documentation/reference/stable/index.html).

## Architecture

- **PostgreSQL (Bitnami, `docker.io/bitnami/postgresql:latest` in compose):** one writable **primary** and two **read replicas** (physical streaming replication). Logical decoding (`wal_level=logical`) is enabled on the primary for Debezium. Bitnami often drops old revision pins; for production pin an image **digest** instead of `:latest`.
- **`pg_stat_statements`:** Bitnami loads extensions via **`POSTGRESQL_SHARED_PRELOAD_LIBRARIES: pgaudit,pg_stat_statements`** (keep **`pgaudit`** — it is the image default). Tune **`pg_stat_statements.*`** via **`POSTGRESQL_EXTRA_FLAGS`**. On **first** primary init, **[`03-pg-stat-statements.sh`](03-pg-stat-statements.sh)** runs **`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`** in database **`postgres`** only. If you see **`pg_stat_statements must be loaded via shared_preload_libraries`**, restart the container after pulling compose changes and run `SHOW shared_preload_libraries;` — it must include **`pg_stat_statements`**. **Existing volume:** env vars apply when the data dir is (re)initialized; otherwise set `shared_preload_libraries` to include both libraries in server config and **restart** (not reload), then **[`ensure-pg-stat-statements.sql`](ensure-pg-stat-statements.sql)** on **`postgres`**.
- **Kafka:** single broker (Confluent 7.6.1) for demo simplicity; clients inside Docker use `kafka:29092`, clients on the host often use `localhost:9092`.
- **Kafka Connect (Debezium 2.7):** REST API on **8083**.
  - **PostgreSQL → Kafka:** `PostgresConnector` captures changes from `public.demo_items` into topics prefixed with `demopg`.
  - **Kafka → PostgreSQL:** `JdbcSinkConnector` writes to table `demo_items_from_kafka` (avoids feeding the sink back into the same CDC table).
- **Monitoring:** `postgres_exporter` (one per Postgres instance) and `danielqsj/kafka-exporter` expose metrics to **Prometheus**; **Grafana** loads a bundled dashboard that charts connections, throughput, DB size, replication lag, offsets, and consumer lag.

### Physical replication slots (HA standbys, not Debezium)

The two read replicas use **physical** replication slots on the **primary** so PostgreSQL does not recycle WAL until each standby has read it (on top of `wal_keep_size` behaviour). This is **separate** from **logical** slots used by Debezium.

| Item | Value |
|------|--------|
| Slot for `postgresql-replica-1` | `pgdemo_phys_replica_1` |
| Slot for `postgresql-replica-2` | `pgdemo_phys_replica_2` |
| Primary (new volume) | [`02-physical-replication-slots.sh`](02-physical-replication-slots.sh) runs in `docker-entrypoint-initdb.d` as **`replicator`** (needs `REPLICATION` for `pg_create_physical_replication_slot`). |
| Primary (existing volume) | From `dashboards/demo`: `chmod +x postgres-kafka/apply-physical-replication-slots.sh && ./postgres-kafka/apply-physical-replication-slots.sh` |
| Replicas | `POSTGRESQL_EXTRA_FLAGS` sets `-c primary_slot_name=…` per service in **`docker-compose.yml`**. |

After enabling slots on an **old** primary, **recreate** the replica containers (or remove their Docker volumes and let them re-sync) so they connect with `primary_slot_name`.

Check on the primary:

```sql
SELECT slot_name, slot_type, active, restart_lsn
FROM pg_replication_slots
WHERE slot_name LIKE 'pgdemo_phys_%';
```

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
| Superuser (Bitnami bootstrap admin) | `postgres` | `postgres` |
| Application, hub UI, exporters, JDBC sink | `demo` | `demopass` |
| Physical replication + Debezium PostgresConnector (CDC) | `replicator` | `replicatorpass` |

Database name: **`demo`**. Demo table: **`demo_items`**; sink table (created by the sink connector): **`demo_items_from_kafka`**.

**Existing primary volume** from before `demo` existed: as `postgres`, run **[`ensure-demo-app-user.sql`](ensure-demo-app-user.sql)** (see file header), then **`./kafka-connect-register/register-all.sh`** so the JDBC sink uses `demo` again.

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

## Register Debezium connectors

When **Kafka Connect** answers on [http://localhost:8083](http://localhost:8083):

```bash
chmod +x register-connectors.sh
./register-connectors.sh
# or: ./register-connectors.sh http://localhost:8083
```

**`pg-source-demo` already running but `jdbc-sink-demo` missing** (for example the full script was still waiting on removing the source): add the sink without touching the source:

```bash
./register-connectors.sh http://127.0.0.1:8083 jdbc-only
# or: JDBC_SINK_ONLY=1 ./register-connectors.sh http://127.0.0.1:8083
# or default URL: ./register-connectors.sh jdbc-only
```

The script registers `pg-source-demo` and `jdbc-sink-demo`. To list connectors:

```bash
curl -s http://localhost:8083/connectors
```

To remove and re-register, delete connectors via the REST API (sink first), then run the script again.

### `pg-source-demo` task `FAILED` — `OutOfMemoryError` during snapshot (`PgResultSet.getString`)

Usually **`demo_items.name`** contains **very large** `TEXT` (e.g. hub Workload with multi‑MiB pad in a single column). Debezium loads each cell fully during snapshot. **Mitigations:** cap workload names (**`POSTGRES_WORKLOAD_NAME_MAX_CHARS`** on **hub-demo-ui**, default 16 KiB), raise **`KAFKA_HEAP_OPTS`** on **`kafka-connect`** (compose uses **8 GiB**), use **`snapshot.fetch.size`** (see **`register-connectors.sh`**), and/or `TRUNCATE demo_items` / delete oversized rows before re‑snapshot.

### `curl …/connectors` shows only `pg-source-demo`

Usually **`jdbc-sink-demo`** was never created because the script exited or timed out while **removing** `pg-source-demo` (slow Connect shutdown). Register the sink only:

`./register-connectors.sh http://127.0.0.1:8083 jdbc-only`

If removing the source stays stuck for minutes, see Connect logs (`docker compose logs kafka-connect`); restarting the **`kafka-connect`** container and then **`jdbc-only`** is often enough if the source is already healthy.

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
2. **Kafka-only** (this repo): provisioned dashboard **Kafka cluster (demo broker)** — file **`grafana/generated-dashboards/kafka-cluster-overview.json`**, UID **`demo-kafka-cluster`**. It charts **`danielqsj/kafka-exporter`** metrics scraped as **`job="kafka_pgdemo"`**.
3. **PostgreSQL**: there is no bundled Postgres-only dashboard in `generated-dashboards`. Use **Explore** on the **`prometheus`** datasource, or import a community **postgres_exporter** dashboard (e.g. search on [Grafana.com](https://grafana.com/grafana/dashboards/)) and set the job filter to **`postgres_pgdemo`** (and `pg_cluster="pgdemo"` where applicable) to match **`dashboards/prometheus/prometheus.yaml`**.

### Grafana shows **No data** for Postgres exporter metrics

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

### Debezium logs: `permission denied for table demo_items` (snapshot failed)

The **init script used to `GRANT SELECT … TO replicator` before `demo_items` existed**, so on a fresh volume **`replicator` had no `SELECT` on `demo_items`** and the **pg-source** snapshot never finished (no Kafka topic traffic → empty **`demo_items_from_kafka`**).

- **Existing volume:** from `dashboards/demo` run **`./postgres-kafka/apply-ensure-debezium.sh`**, then restart / re-register **`pg-source-demo`** (`./kafka-connect-register/register-all.sh` or delete + `register-connectors.sh`).
- **New volume:** `01-init-debezium.sql` is fixed; recreate the primary volume if you need init to re-run.

### `demo_items_from_kafka` stays empty while Mongo sink has data

1. Check **`jdbc-sink-demo`** status: `curl -s http://localhost:8083/connectors/jdbc-sink-demo/status` — if **`FAILED`**, read **`docker compose logs kafka-connect`**.
2. **Stale sink table schema** (common after iterating connector versions): from `dashboards/demo` run **[`clean-kafka-connect-sinks.sh`](../clean-kafka-connect-sinks.sh)** — it **`DROP`**s `public.demo_items_from_kafka` so **`auto.create`** can rebuild it, then re-run **`./kafka-connect-register/register-all.sh`** (or **`postgres-kafka/register-connectors.sh`**).
3. **`register-connectors.sh`** uses **`io.debezium.connector.jdbc.JdbcSinkConnector`** with **`dialect.name: postgresql`** and **no** unwrap SMT (Debezium JDBC sink reads the native CDC envelope; **`ExtractNewRecordState`** on the sink can prevent rows from being applied). Re-register after pulling the script.

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
| `02-physical-replication-slots.sh` | On first primary boot, runs `ensure-…sql` as **`replicator`** (required for slot DDL). |
| `ensure-physical-replication-slots.sql` / `apply-physical-replication-slots.sh` | Same on existing primary volumes. |
| `seed-dummy-data.sql` | Inserts **30** extra `demo_items` rows (run anytime on an existing DB). |
| `register-connectors.sh` | Registers Debezium PostgreSQL source and JDBC sink via Connect REST API. |
| `DEBEZIUM.md` | Debezium PostgreSQL source + JDBC sink concepts, permissions, snapshots, official doc links. |
| `diagrams/workflow-components.mmd` / `.svg` | Postgres + Kafka component diagram. |
| `diagrams/cdc-sequence.mmd` / `.svg` | Postgres CDC sequence diagram. |
| `README.md` | This document. |

**Single** compose file: **`../docker-compose.yml`**. Other area guides: **`../cassandra/README.md`**, **`../kafka/README.md`**, **`../observability/README.md`**.
