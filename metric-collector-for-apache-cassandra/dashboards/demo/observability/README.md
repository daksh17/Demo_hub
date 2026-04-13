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
| `nodetool` | `nodetool-exporter:9104` | Cassandra ring / nodetool metrics (sequential nodetool calls; job **`scrape_timeout: 90s`**. Rebuild **`nodetool-exporter`** after changing **`exporter.py`**. HTTP **500** was often caused by **parallel** scrapes racing **`prometheus_client`** — fixed by sequential collection.) |
| `mcac` | file SD `tg_mcac.json` | MCAC / Cassandra detailed metrics |
| `mongodb` | `mongodb-exporter`, optional `mongodb-exporter-local` | Sharded demo + host Mongo |
| `postgres_pgdemo` | postgres exporters ×3 | Bitnami Postgres demo |
| `kafka_pgdemo` | `kafka-exporter-mcac` | Broker / consumer lag style metrics |
| `redis_demo` | `redis-exporter:9121` | Redis 7 via [oliver006/redis_exporter](https://github.com/oliver006/redis_exporter); provisioned **Redis (demo broker)** (`redis-demo-overview.json`, UID **`demo-redis`**). Community IDs **763** / **11835** often need K8s variable fixes — see **`../redis/README.md`** |
| `opensearch_demo` | `opensearch-exporter:9114` | OpenSearch via [prometheuscommunity/elasticsearch-exporter](https://github.com/prometheus-community/elasticsearch-exporter); Grafana IDs **14191**, **2322** — see **`../opensearch/README.md`** |

**Kubernetes:** Prometheus and Grafana are generated into **`../k8s/generated/10-observability-prometheus-grafana.yaml`** (static Prometheus **Deployment**, not the Prometheus Operator). Optional **ServiceMonitor** examples for Operator-based installs: **[`../k8s/optional-prometheus-operator-servicemonitors.example.yaml`](../k8s/optional-prometheus-operator-servicemonitors.example.yaml)**. **Ingress** for Grafana/Prometheus hostnames (requires an Ingress controller): **`../k8s/generated/96-kubernetes-ops.yaml`**. Full list of K8s resources: **[`../k8s/README.md`](../k8s/README.md)**.

After editing **`prometheus.yaml`** or **`tg_mcac.json`**, reload the stack or restart Prometheus:

```bash
docker compose restart prometheus
```

From **`dashboards/demo`**, check whether MCAC scrape hostnames resolve from Prometheus’ network (the official Prometheus image has no `getent`; use **`wget`**):

```bash
docker compose exec prometheus wget -qO- --timeout=3 http://cassandra3:9103/metrics 2>&1 | head -3
```
If you see “bad address” or “Connection refused”, the Cassandra node or port is missing; **`tg_mcac.json`** lists **`cassandra`**, **`cassandra2`**, **`cassandra3`** — all three must be **Up** for **mcac 3/3**.

## Grafana

- Anonymous **Admin** is enabled in **`../../grafana/grafana.ini`** for the demo.
- Dashboards are file-provisioned from **`../../grafana/generated-dashboards/`** (includes Cassandra overview/condensed, **Kafka cluster (demo broker)**, MongoDB tic/tac/toe **detailed**, system metrics, etc.). For PostgreSQL charts, import a **postgres_exporter** dashboard and filter **`job="postgres_pgdemo"`**.
- Datasource UID **`prometheus`** (see `prometheus-datasource.yaml`).

## Typical commands

From **`dashboards/demo`**:

```bash
docker compose up -d prometheus grafana
open http://localhost:9090/targets
open http://localhost:3000
```

## Troubleshooting Prometheus targets

### `lookup <host> on 127.0.0.11:53: no such host`

Prometheus runs **inside** a container (or Pod). Scrape target hostnames must resolve on **that** network—not only on your laptop.

| Symptom | What it means |
|---------|----------------|
| **`mongodb-exporter` DOWN** | Nothing named `mongodb-exporter` is reachable from the Prometheus container. In **`dashboards/demo`**, start the stack service **`mongodb-exporter`** (it appears only after **`mongo-shard-add`** succeeds). |
| **`nodetool-exporter` DOWN** | **Wrong port** is common: use **`nodetool-exporter:9104`** (compose maps **9104:9104**). **`9103`** is the **MCAC** scrape port **on each Cassandra container**, not the nodetool exporter. If the error is **`no such host`**, start **`nodetool-exporter`** and ensure Prometheus is on the same Docker network (`demo_net` / `mcac_net`). |
| **`cassandra-2` DOWN** | Hostnames like **`cassandra-0`** are typical of **Kubernetes**; this repo’s compose uses **`cassandra`**, **`cassandra2`**, **`cassandra3`**. Either deploy matching Services or change **`tg_mcac.json` / scrape_configs** to your real DNS names. |

**Fix (conceptually):** use the same compose/stack as **`prometheus.yaml`**, or edit scrape `targets:` to the FQDNs your orchestrator assigns (e.g. `my-exporter.namespace.svc.cluster.local`).

**Grafana MongoDB dashboard** panels use **`mongo_topology="sharded"`** (set on the **`mongodb-exporter`** scrape in this repo’s **`prometheus.yaml`**). If you customize labels, add the same label to your static config or adjust panel queries in **`mongodb-tictactoe-detailed.json`**.

## Further reading

- Cassandra + nodetool exporter: **[`../cassandra/README.md`](../cassandra/README.md)**.
- Kafka broker: **[`../kafka/README.md`](../kafka/README.md)**.
- Full demo index: **[`../README.md`](../README.md)**.
