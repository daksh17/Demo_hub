# Debezium with PostgreSQL (source and JDBC sink)

This document summarizes **important concepts from the official Debezium documentation** for the **PostgreSQL source connector** and the **Debezium JDBC sink connector**, plus how they relate to **`dashboards/demo`**. For vendor truth, always refer to the links in [Official documentation](#official-documentation).

---

## What is Debezium?

Debezium is a set of **change data capture (CDC)** tools that record **row-level** changes in databases and expose them as a **change event stream**. Consumers read events in **commit order** and react to inserts, updates, and deletes.

- **Home (stable):** [Debezium Documentation](https://debezium.io/documentation/reference/stable/index.html)
- **Tutorial:** [Debezium tutorial](https://debezium.io/documentation/reference/stable/tutorial.html)

---

## PostgreSQL source connector (`PostgresConnector`)

**Official reference:** [Debezium connector for PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)

### Behaviour

1. **First connection:** takes a **consistent snapshot** of the configured schemas/tables (unless `snapshot.mode` says otherwise).
2. **After snapshot:** continuously captures **row-level** changes committed in PostgreSQL and **emits events to Kafka** (by default, typically one topic per table).

### How it reads the database

| Mechanism | Role |
|-----------|------|
| **Logical decoding** | PostgreSQL feature (9.4+) that reads changes **committed to the transaction log** via an **output plugin**. |
| **Output plugins** | e.g. **`decoderbufs`** (Debezium community) or **`pgoutput`** (built into PostgreSQL 10+ for logical replication; **no extra install** on the server). |
| **Connector (Java)** | Kafka Connect plugin that consumes the replication stream over PostgreSQL’s **streaming replication protocol** (with the **JDBC driver**). |

### Why snapshots exist

The **write-ahead log (WAL)** is **not retained indefinitely**, so the connector **cannot** reconstruct full table history from the WAL alone. The initial **snapshot** provides a full baseline; streaming then continues from the **log position captured during that snapshot** so changes made during the snapshot are not lost.

### Failure recovery

The connector records **WAL positions** (offsets) as it processes events. If it stops (crash, network, Connect rebalance), on restart it **resumes from the last committed offset**. If a snapshot **did not finish**, it **restarts the snapshot** on the next start (per documentation behaviour).

### Documented limitations

- **No DDL change events** through logical decoding in the same way as DML; schema evolution is handled via separate mechanisms (e.g. schema history topics), not as row-level “DDL events.”
- **Replication timing:** in edge cases around **commit**, consumers might briefly observe **inconsistent** states (documentation discusses primary failure and read-after-write semantics).
- **`pgoutput`:** does **not** emit values for **generated columns** (those columns are missing in the event payload).
- **Database encoding:** Debezium expects **UTF-8** for correct string handling.

### Snapshot modes (`snapshot.mode`)

The PostgreSQL connector’s `snapshot.mode` controls when snapshots run. Documented options include (non-exhaustive; see official table for full detail):

| Mode | Summary |
|------|---------|
| **`initial`** (default) | Snapshot when **no** Kafka offsets exist; then stream from the recorded LSN. |
| **`always`** | Snapshot on **every** connector start; then stream. Useful if WAL history was lost or after fail-over. |
| **`initial_only`** | Snapshot **only**, then **no** streaming. |
| **`no_data`** | **Never** snapshot; stream from stored offset or from slot creation point (WAL must still contain needed history). |
| **`when_needed`** | Snapshot only if offsets are missing or point at **unavailable** WAL. |
| **`configuration_based`** / **`custom`** | Advanced / pluggable snapshot behaviour. |

**Ad hoc and incremental snapshots** use **signalling** (inserts into a signal table or messages to a Kafka signal topic). See: [Debezium signalling](https://debezium.io/documentation/reference/stable/configuration/signalling.html).

### PostgreSQL server settings

Logical replication generally requires at least:

- `wal_level=logical`
- Adequate **`max_replication_slots`**, **`max_wal_senders`**, and (where relevant) **`wal_keep_size`**

Replication **slots** retain WAL for the consumer — monitor **disk usage** and **slot lag**; stale slots can cause **catalog bloat** and growth of WAL retention.

### Physical slots for read replicas (same primary, not CDC)

This demo also uses **physical** replication slots (`pgdemo_phys_replica_1` / `_2`) so streaming **standbys** keep a named reservation on the primary’s WAL. Those slots are unrelated to Debezium’s **logical** slot. See **[`README.md` – Physical replication slots](README.md#physical-replication-slots-ha-standbys-not-debezium)** and `02-physical-replication-slots.sh` / `ensure-physical-replication-slots.sql`.

---

## Permissions and security (PostgreSQL source)

**Official section:** *Setting up permissions* in the [PostgreSQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

### Principles

- Prefer a **dedicated replication user** with **minimum** privileges, **not** a superuser.
- PostgreSQL logical replication security is described in: [PostgreSQL — logical replication security](https://www.postgresql.org/docs/current/logical-replication-security.html).

### Typical requirements

| Need | Purpose |
|------|---------|
| **`REPLICATION` + `LOGIN`** | Create or alter user/role so Connect can open a **replication** connection and use a **replication slot**. |
| **`SELECT` on captured tables** | **Snapshot** and ongoing reads of table data as permitted by the connector. |
| **Publication** | Tables must appear in a **publication** consumed by the slot (often **`pgoutput`**). Publications may be **created by a DBA** (recommended in docs) or under specific privileges by the connector. |
| **`CREATE` on database** (if connector creates publication) | Only if Debezium is allowed to **create** publications automatically — follow the doc’s notes on **`publication.autocreate.mode`** and permissions. |
| **Table ownership / replication groups** | If the connector must add tables to a publication, PostgreSQL rules around **table ownership** may require a **shared replication group** pattern (documented in Debezium). |

In **this demo**, the **`replicator`** user (Bitnami) is used for CDC; **`demo`** owns application tables and grants **`SELECT`** to **`replicator`**; publication **`dbz_publication`** is created in `01-init-debezium.sql`. Superuser **`postgres`** / **`postgres`** is also available.

---

## JDBC sink connector (`JdbcSinkConnector`)

**Official reference:** [Debezium connector for JDBC](https://debezium.io/documentation/reference/stable/connectors/jdbc.html)

### Behaviour

- **Kafka Connect sink:** **polls** configured Kafka topics, consumes records, and **writes to a relational database via JDBC**.
- Supports **multiple tasks**, **at-least-once** delivery, **idempotent upserts**, **basic schema evolution**, **primary key modes**, and optional **deletes** (`delete.enabled`).

### Debezium event shape

- The **Debezium JDBC sink** can consume **native Debezium change events** directly — **`ExtractNewRecordState` is not required** (unlike many non-Debezium JDBC sinks).
- For **insert/update**, values are taken from the event’s **`after`** payload as described in the JDBC connector doc.
- **Do not** subscribe the sink to **schema change only** topics; restrict **`topics`** / **`topics.regex`** to **data** topics.

### Primary keys and upsert

- **`primary.key.mode`** (e.g. **`record_key`**, **`record_value`**, **`kafka`**) defines how the sink maps keys for **upsert** and **delete**.
- **`insert.mode: upsert`** yields **idempotent** replays (exact SQL depends on **dialect**).

In **this demo**, the sink writes **`demo_items_from_kafka`** using **`demo`** credentials; that table is **outside** the CDC publication to avoid feedback loops.

---

## Source vs sink (quick comparison)

| | **PostgresConnector (source)** | **JdbcSinkConnector (sink)** |
|---|-------------------------------|------------------------------|
| **Direction** | PostgreSQL → Kafka | Kafka → PostgreSQL |
| **Reads** | Tables (**snapshot**) + **WAL** (**streaming**) | **Kafka topics** |
| **Writes** | **Kafka** (produces events) | **PostgreSQL** (JDBC DML) |
| **Typical DB user** | `REPLICATION` + `LOGIN`, `SELECT` on captured tables | App-like: **`INSERT`/`UPDATE`** on sink table (and **`CREATE`** if `auto.create`) |

---

## Official documentation

| Topic | URL |
|-------|-----|
| Stable documentation home | https://debezium.io/documentation/reference/stable/index.html |
| Tutorial | https://debezium.io/documentation/reference/stable/tutorial.html |
| Connector index | https://debezium.io/documentation/reference/stable/connectors/index.html |
| **PostgreSQL** | https://debezium.io/documentation/reference/stable/connectors/postgresql.html |
| **JDBC** | https://debezium.io/documentation/reference/stable/connectors/jdbc.html |
| **Signalling** | https://debezium.io/documentation/reference/stable/configuration/signalling.html |

---

## Related files in this demo

| File | Role |
|------|------|
| [`01-init-debezium.sql`](01-init-debezium.sql) | Primary DB: `demo_items`, grants, publication |
| [`ensure-debezium-cdc.sql`](ensure-debezium-cdc.sql) | Re-apply grants/publication on existing volumes |
| [`register-connectors.sh`](register-connectors.sh) | REST registration for `pg-source-demo` + `jdbc-sink-demo` |
| [`README.md`](README.md) | Stack-specific setup, ports, troubleshooting |
