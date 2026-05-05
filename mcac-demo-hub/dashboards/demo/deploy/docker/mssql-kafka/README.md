# SQL Server (publisher + subscriber) for the demo hub

## Kubernetes

Generated **`deploy/k8s/generated/62-mssql.yaml`**: **mssql-publisher** and **mssql-subscriber** (same hostnames as Compose), **mssql-exporter-***, and Job **mssql-demo-bootstrap** (runs `init-publisher.sh` / `init-subscriber.sh` / `try-replication-bootstrap.sh`, then **`register-mssql-connectors.sh`** against **`http://kafka-connect:8083`** with **`SCHEMA_HISTORY_KAFKA_BOOTSTRAP=kafka:9092`**). Password: Secret **`demo-hub-credentials`** key **`mssql-sa-password`** (same default as Compose). Build **`mcac-demo/mssql-tools:22.04`** before bootstrap: [`deploy/k8s/scripts/build-mssql-tools-image.sh`](../../k8s/scripts/build-mssql-tools-image.sh) or **`build-all-custom-images.sh`**. The Dockerfile uses **`linux/amd64`** (Microsoft `mssql-tools18` + **`msodbcsql18`**) so builds on **Apple Silicon** use emulation and can take a few minutes; if `apt-get` failed with exit **100**, rebuild after pulling the latest **`Dockerfile.repl-bootstrap`**. **`apply-data-bootstrap.sh`** runs the MSSQL Job **after** Kafka Connect is Ready. Hub UI gets **`MSSQL_HOST=mssql-publisher`** from **`95-hub-demo-ui.yaml`**.

---

Two SQL Server containers run from the root `docker-compose.yml`:

- **mssql-publisher** — internal port `1433`, host **`localhost:14331`**. Holds `demo.dbo.scenario_catalog_mirror_mssql` with **CDC** enabled for Debezium, and **`demo.dbo.hub_workload_mssql`** for the hub **workload** page (not CDC-enabled). If your publisher volume predates that table, re-run **`mssql-publisher-schema-init`** or apply `01-publisher-schema.sql` manually.
- **mssql-subscriber** — internal port `1433`, host **`localhost:14332`**. Holds `demo.dbo.demo_items_from_kafka_mssql` for the **JDBC sink** connector.

Password: `MSSQL_SA_PASSWORD` (default in compose: `Demo_hub_Mssql_2025!`).

## Hub demo UI

`hub-demo-ui` receives `MSSQL_HOST=mssql-publisher` and mirrors catalog rows on **Mongo → Postgres sync** (`mssql_rows_upserted` in the JSON response). View rows under **Scenario → SQL Server** (`/scenario/data/mssql`). On **`/workload`**, optionally target **SQL Server** to insert into **`hub_workload_mssql`**.

If the hub cannot reach SQL Server, set `MSSQL_ENCRYPT=off` on `hub-demo-ui` (default in code) or match your server’s TLS policy.

Rebuild the UI image after adding `pymssql` to `requirements.txt`:

```bash
docker compose build hub-demo-ui
```

## Kafka Connect

The custom image `mcac-demo/kafka-connect` extends Debezium Connect **2.7.3** with the **SQL Server connector** plugin and **mssql-jdbc** in `/kafka/libs`. Rebuild after changing `mongo-kafka/Dockerfile.connect`:

```bash
docker compose build kafka-connect
```

Connectors are registered by **`mssql-kafka-connect-register`** (depends on schema init + optional replication try job). Source topic name follows Debezium’s default (`<server>.dbo.<table>`); adjust `register-mssql-connectors.sh` if your connector name or topic differs.

## Observability

Compose runs **`mssql-exporter-publisher`** and **`mssql-exporter-subscriber`** (`awaragi/prometheus-mssql-exporter`). Prometheus scrapes job **`mssql_demo`**. Grafana: **`grafana/generated-dashboards/mssql-demo-overview.json`** (provisioned with other demo dashboards).

## Transactional replication

`05-transactional-replication-bootstrap.sql` and **`mssql-replication-try`** attempt native transactional replication; **this often fails in Docker** and is non-blocking. The supported demo path is **CDC + Kafka** (publisher → Debezium → topic → JDBC sink → subscriber).

## Troubleshooting (Kafka Connect registration)

**`Cannot open database "demo"` / login failed** when creating **`mssql-source-demo`**: Connect validates by opening **`demo`** on **`mssql-publisher`**. That database is created by **`01-publisher-schema.sql`** (Compose: **`mssql-publisher-schema-init`**; Kubernetes: **`mssql-demo-bootstrap`** Job, first step).

1. Finish schema init before running **`register-mssql-connectors.sh`** / **`register-all-connectors.sh`**.
2. **`MSSQL_SA_PASSWORD`** in your shell must match the publisher (K8s: Secret **`demo-hub-credentials`**, key **`mssql-sa-password`**; default **`Demo_hub_Mssql_2025!`**).
3. Kubernetes: `kubectl logs -n demo-hub job/mssql-demo-bootstrap --tail=120` and  
   [`../../k8s/scripts/verify-mssql-publisher-demo.sh`](../../k8s/scripts/verify-mssql-publisher-demo.sh) — you should see **`demo`** in the database list.

If verify only shows **master / model / msdb / tempdb**, **`demo` was never created** (bootstrap job failed or was skipped). Recover by re-running the Job after fixing **`mcac-demo/mssql-tools`** image pulls, or apply schema manually:

[`../../k8s/scripts/apply-mssql-schema-k8s.sh`](../../k8s/scripts/apply-mssql-schema-k8s.sh) (pipes `01-publisher-schema.sql` / `02-subscriber-schema.sql` into both servers), then register MSSQL connectors again.

The registration script retries the source connector several times to avoid racing a slow SQL Server start.
