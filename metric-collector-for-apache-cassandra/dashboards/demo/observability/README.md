# Prometheus & Grafana

This folder documents **metrics** and **dashboards** for the **`../docker-compose.yml`** stack. Service definitions remain in that compose file; config files live under **`../../prometheus/`** and **`../../grafana/`** relative to **`dashboards/demo`**.

## Services

| Compose service | Port (host) | Config mounted from |
|-----------------|------------|----------------------|
| **`prometheus`** | **9090** | `../../prometheus/prometheus.yaml`, `../tg_mcac.json` |
| **`grafana`** | **3000** | `../../grafana/prometheus-datasource.yaml`, `dashboards.yaml`, `generated-dashboards/`, `grafana.ini` |

## Scrape jobs (overview)

Defined in **`../../prometheus/prometheus.yaml`** (path inside the repo: `dashboards/prometheus/prometheus.yaml`). Highlights:

| Job | Target(s) | Notes |
|-----|-----------|--------|
| `prometheus` | self | |
| `nodetool` | `nodetool-exporter:9104` | Cassandra ring / nodetool-derived metrics |
| `mcac` | file SD `tg_mcac.json` | MCAC / Cassandra detailed metrics |
| `mongodb` | `mongodb-exporter`, optional `mongodb-exporter-local` | Sharded demo + host Mongo |
| `postgres_pgdemo` | postgres exporters ×3 | Bitnami Postgres demo |
| `kafka_pgdemo` | `kafka-exporter` | Broker / consumer lag style metrics |

After editing **`prometheus.yaml`**, reload the stack or restart Prometheus:

```bash
docker compose restart prometheus
```

## Grafana

- Anonymous **Admin** is enabled in **`../../grafana/grafana.ini`** for the demo.
- Dashboards are file-provisioned from **`../../grafana/generated-dashboards/`** (includes Cassandra overview, postgres-kafka overview, Mongo tic-tac-toe overview, etc.).
- Datasource UID **`prometheus`** (see `prometheus-datasource.yaml`).

## Typical commands

From **`dashboards/demo`**:

```bash
docker compose up -d prometheus grafana
open http://localhost:9090/targets
open http://localhost:3000
```

## Further reading

- Cassandra + nodetool exporter: **[`../cassandra/README.md`](../cassandra/README.md)**.
- Kafka broker: **[`../kafka/README.md`](../kafka/README.md)**.
- Full demo index: **[`../README.md`](../README.md)**.
