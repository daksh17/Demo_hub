# Cassandra (MCAC demo cluster)

This folder documents the **Apache Cassandra** side of the single-stack compose in **`../docker-compose.yml`**. It does **not** define services by itself—the cluster, the MCAC agent image, and **nodetool-exporter** are all declared in that file.

## What lives here vs the repo root

| Item | Location |
|------|----------|
| Compose service definitions | `../docker-compose.yml` |
| **nodetool-exporter** image build context | `../nodetool-exporter/` (`Dockerfile`, `exporter.py`) |
| MCAC agent / collector config | Repo `../../config/`, agent JAR from `../../pom.xml` **`PROJECT_VERSION`** |
| Prometheus file SD for MCAC | `../tg_mcac.json` (mounted into Prometheus) |

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

## Further reading

- Full demo walkthrough, stress, and topology: **[`../README.md`](../README.md)**.
- Dashboards JSON: `../../grafana/generated-dashboards/`.
- Regenerate Grafana boards after jsonnet edits: `../../grafana/make-dashboards.sh`.
