# Cassandra (MCAC demo cluster)

This folder documents the **Apache Cassandra** side of the single-stack compose in **`../docker-compose.yml`**. It does **not** define services by itself—the cluster, the MCAC agent image, and **nodetool-exporter** are all declared in that file.

## What lives here vs the repo root

| Item | Location |
|------|----------|
| Compose service definitions | `../docker-compose.yml` |
| **nodetool-exporter** image build context | `../nodetool-exporter/` (`Dockerfile`, `exporter.py`) |
| MCAC agent / collector config | Repo `../../config/`, agent JAR from `../../pom.xml` **`PROJECT_VERSION`** |
| Prometheus file SD for MCAC | `../tg_mcac.json` (mounted into Prometheus) |
| **Kubernetes** (same ring, different wiring) | **[`../k8s/README.md`](../k8s/README.md)** — StatefulSet **`cassandra`** with **PVCs**, headless **`cassandra-headless`**, optional **NodePort** [`../k8s/optional-cassandra-0-cql-nodeport.yaml`](../k8s/optional-cassandra-0-cql-nodeport.yaml); hub UI uses **headless FQDNs** for **`CASSANDRA_HOSTS`** (not the **`cassandra`** ClusterIP service name). |

## Services (names to use in `docker compose`)

- **`mcac`** — builds the metric collector agent from the project root; shared volume **`mcac_data`** supplies the JVM agent under `/mcac`.
- **`cassandra`**, **`cassandra2`**, … — see the cluster / DC / rack table in **[`../README.md`](../README.md)** (“Cluster, datacenter and rack”).
- **`nodetool-exporter`** — HTTP **`9104`**; scrapes `nodetool` on the configured **`CASSANDRA_HOSTS`** and exposes Prometheus metrics.

## Typical commands

From **`dashboards/demo`**:

```bash
export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
docker compose up -d cassandra cassandra2 cassandra3 nodetool-exporter prometheus grafana
```

`nodetool` via Docker (replace host with a running Cassandra container name):

```bash
docker compose exec cassandra nodetool status
```

## Connect from your laptop / apps

The **Cassandra native protocol** (CQL) listens on **9042** inside the Docker network. This demo publishes the **first node** to the host as **19442** (see **`../docker-compose.yml`** — **`cassandra`** → **`19442:9042`**; **`cassandra2`** → **19443**; **`cassandra3`** → **19444**).

| From | Host | Port | Notes |
|------|------|------|--------|
| Host machine (`cqlsh`, apps) | **`127.0.0.1`** | **19442** | First seed node |
| Another container on **`mcac_net`** | **`cassandra`** | **9042** | Any cluster member as contact point |
| Alt nodes on host | `127.0.0.1` | **19443**, **19444** | Second / third container |

Default **`cassandra:4.0`** image: **no password** (authentication off unless you changed it). **No TLS** in this dev stack.

### cqlsh (installed locally)

```bash
cqlsh 127.0.0.1 19442
```

Or via the container (no host port needed):

```bash
docker compose exec -it cassandra cqlsh
```

Hub demo keyspace and tables (after using the UI):

```sql
USE demo_hub;
DESCRIBE TABLES;
SELECT * FROM orders LIMIT 10;
SELECT * FROM scenario_timeline LIMIT 10;
```

### Hub scenario indexes (Cassandra)

**`demo_hub.scenario_timeline`** is keyed by **`(order_ref, event_ts)`** (clustering **`event_ts DESC`**). The hub also creates a **secondary index** **`scenario_timeline_event_type`** on **`event_type`** for low-cardinality demo filters (`ORDER_PLACED`, etc.). Secondary indexes are **not** a general-purpose substitute for partition-key-driven queries at scale.

**Apply without the UI:** from **`dashboards/demo`**, **`./cassandra/apply-scenario-hub-schema.sh`** (runs **[`ensure-scenario-hub.cql`](ensure-scenario-hub.cql)** via **`cqlsh`** on the first node). Schema is also ensured in **`../realtime-orders-search-hub/demo-ui/scenario.py`**.

Full table: **[`../README.md` → Hub scenario indexes](../README.md#hub-scenario-indexes-multi-db-reference)**.

### GUI: DBeaver / Idea

Use a **Cassandra** connection (native driver), **not** a generic JDBC URL. Set contact point **`127.0.0.1`**, port **19442**, no user/password for this demo. If the tool only offers JDBC, use a Cassandra-specific plugin or the **Java**/**Python** drivers below—Cassandra is not an ODBC/JDBC server like Postgres.

### Programmatic

- **Python**: `cassandra-driver` (same as **`hub-demo-ui`**) — `ContactPoints=['127.0.0.1']`, port **19442**, keyspace **`demo_hub`**.
- **Java**: [Apache Cassandra Java driver](https://docs.datastax.com/en/developer/java-driver/latest/) — programmatic **CqlSession** with the same contact point and port.
- **JDBC**: Not first-class on Cassandra. For BI tools, vendors sometimes ship a JDBC/ODBC bridge over CQL; for application code, prefer the **native** drivers above.

### Trouble: `NoNodeAvailableException` (DataStax / Java driver)

The driver opens a control connection but then **refuses to run queries** if it cannot pick a “local” replica. With the stock **`cassandra:4.0`** image (no `CASSANDRA_DC` override in this compose file), nodes report datacenter **`datacenter1`**.

**Checklist:**

1. **Host port from your laptop** — use **19442** (mapped from container **9042**), not **9042** unless you are inside Docker and talking to **`cassandra:9042`**.
2. **Set local datacenter** (Java driver 4.x / many GUIs):
   - Advanced / driver properties: **`basic.load-balancing-policy.local-datacenter=datacenter1`**
   - Or in URL-style config where the tool supports it.
   - If DC is wrong, the driver filters out **all** nodes → **`NoNodeAvailableException`**.
3. **Confirm the cluster is up:**
   ```bash
   docker compose exec cassandra nodetool status
   cqlsh 127.0.0.1 19442 -e "SELECT release_version FROM system.local;"
   ```
4. **Firewall / VPN** blocking localhost high ports (rare).

To **see** the DC name from CQL:

```sql
SELECT data_center FROM system.peers;
SELECT data_center FROM system.local;
```

(`system.local` is the node you connected to; all should show the same DC in a homogenous cluster.)

## Compaction lab: TWCS vs LCS

Two optional tables in **`demo_hub`** illustrate **TimeWindowCompactionStrategy (TWCS)** vs **LeveledCompactionStrategy (LCS)**.

| Table | Strategy | Typical use |
|-------|-----------|-------------|
| **`lab_twcs_sensor`** | TWCS (4-hour windows) | Time-series: partition `(sensor_id, day)`, clustering `event_ts` ASC — inserts ordered in time per partition. |
| **`lab_lcs_lookup`** | LCS (~160 MiB level size) | Read-heavy / random key lookups by **`id`** (UUID). |

**Create tables** (from **`dashboards/demo`**):

```bash
./cassandra/apply-compaction-lab.sh
# optional sample rows:
./cassandra/apply-compaction-lab.sh --with-samples
```

Sources: [`ensure-compaction-lab.cql`](ensure-compaction-lab.cql), [`insert-compaction-lab-samples.cql`](insert-compaction-lab-samples.cql).

**Inspect compaction settings:**

```sql
DESCRIBE TABLE demo_hub.lab_twcs_sensor;
DESCRIBE TABLE demo_hub.lab_lcs_lookup;
```

**Exercise compaction (first node):**

```bash
docker compose exec cassandra nodetool flush demo_hub
docker compose exec cassandra nodetool compactionstats
docker compose exec cassandra nodetool tablestats demo_hub lab_twcs_sensor
docker compose exec cassandra nodetool tablestats demo_hub lab_lcs_lookup
```

**What to look for:** TWCS creates **time windows** of SSTables and drops whole windows when TTL expires (this lab uses **`default_time_to_live = 0`** — set TTL in your tests if you want window drops). LCS **levels** SSTables and minimizes read amplification for wide rows / heavy reads by key. For heavier load, bulk-insert into **`lab_twcs_sensor`** with **monotonic `event_ts`** within each `(sensor_id, day)` partition; for LCS, many **distinct UUID** keys.

If you already created these tables earlier **without** custom compaction, **`CREATE IF NOT EXISTS` will not alter them** — `DROP TABLE` the lab table and re-run **`apply-compaction-lab.sh`**, or `ALTER TABLE ... WITH compaction = { ... }` to switch strategies.

### TWCS 2-minute windows + varying `gc_grace_seconds` (30 / 60 / 90 / 120 s)

Four tables isolate **tombstone / repair grace** while keeping **TWCS** at **2-minute** windows:

| Table | `gc_grace_seconds` |
|-------|---------------------|
| **`lab_twcs_w2m_gc30`** | 30 |
| **`lab_twcs_w2m_gc60`** | 60 |
| **`lab_twcs_w2m_gc90`** | 90 |
| **`lab_twcs_w2m_gc120`** | 120 |

**Schema:** [`ensure-twcs-gc-lab.cql`](ensure-twcs-gc-lab.cql) — apply with **`./cassandra/apply-twcs-gc-lab.sh`**.

**Load (host needs `pip install cassandra-driver`):** [`load_twcs_gc_lab.py`](load_twcs_gc_lab.py) inserts **time-ordered** rows into a single `(sensor_id, day)` partition (default **`loadtest`** + UTC **day**), which matches how TWCS expects writes.

```bash
./cassandra/apply-twcs-gc-lab.sh
# one table:
python3 cassandra/load_twcs_gc_lab.py --gc 60 --rows 30000 --batch-size 100
# or full sweep (same row count for all four):
ROWS=20000 ./cassandra/run-twcs-gc-sweep.sh
```

Use **`--hosts cassandra --port 9042`** when running **inside** the compose network. On the Mac, default is **`127.0.0.1:19442`**.

**Observe:** after load, `nodetool flush` / `nodetool compactionstats` / `nodetool tablestats demo_hub lab_twcs_w2m_gc60`. **`gc_grace_seconds`** affects how long **tombstones** must be retained before they can be purged after compaction — compare behavior when you add **deletes** or **TTL** in a follow-up experiment; pure inserts mainly stress **TWCS windowing** and write path.

## Further reading

- Full demo walkthrough, stress, and topology: **[`../README.md`](../README.md)**.
- Dashboards JSON: `../../grafana/generated-dashboards/`.
- Regenerate Grafana boards after jsonnet edits: `../../grafana/make-dashboards.sh`.
