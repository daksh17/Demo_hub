# Redis 7 (cache / queue demo)

**Redis 7** is defined in **`../docker-compose.yml`** as service **`redis`** on the shared network **`mcac_net`** (`demo_net` in compose).

## Credentials

| Setting | Default (demo) | Override |
|---------|----------------|----------|
| Password | **`demoredispass`** | Set **`REDIS_PASSWORD`** in the environment or a **`.env`** file next to **`docker-compose.yml`** (same directory as compose). |

There is no separate username; only **`AUTH password`** (Redis ACL default user).

## Host ports (from laptop)

| Port | Role |
|------|------|
| **6379** | Redis protocol |
| **9121** | **redis-exporter** (Prometheus metrics); Prometheus job **`redis_demo`** |

Inside Docker, clients use **`redis:6379`**.

## Start / stop

From **`dashboards/demo`**:

```bash
docker compose up -d redis redis-exporter
docker compose ps redis redis-exporter
```

With a custom password, create **`.env`** in **`dashboards/demo`**:

```bash
echo 'REDIS_PASSWORD=your-secret-here' >> .env
docker compose up -d redis
```

## Quick test

```bash
redis-cli -h 127.0.0.1 -p 6379 -a demoredispass ping
redis-cli -h 127.0.0.1 -p 6379 -a demoredispass SET demo:key "hello"
redis-cli -h 127.0.0.1 -p 6379 -a demoredispass GET demo:key
```

From another container on **`mcac_net`** (default password):

```bash
docker compose exec redis redis-cli -a demoredispass ping
```

## Data

Persistent volume **`redis_data`**; AOF enabled (`--appendonly yes`).

## Grafana

### Bundled dashboard (recommended for this compose)

File provisioning loads **`../../grafana/generated-dashboards/redis-demo-overview.json`** — **Redis (demo broker)**, UID **`demo-redis`**. All queries use **`job="redis_demo"`** (no Kubernetes **Namespace** / **Pod** variables).

Use **`docker compose up -d redis redis-exporter prometheus grafana`**, then open **Dashboards → Redis (demo broker)**. In **Explore**, confirm **`redis_up`** returns **`1`** (optionally filter: `redis_up{job="redis_demo"}` if you use this repo’s **`prometheus.yaml`** unchanged).

If every panel shows **No data**: Grafana’s datasource may not be this stack’s Prometheus, or **`redis-exporter`** cannot reach **`redis`** (password mismatch — use the same **`REDIS_PASSWORD`** in **`.env`** for both). Check **`docker compose ps redis redis-exporter`**.

### **`redis_demo` missing on http://localhost:9090/targets**

That page is **this demo’s** Prometheus only if you use **`dashboards/demo/docker-compose.yml`** (container **`prometheus-mcac`**, port **9090**). After you add or change scrape jobs in **`dashboards/prometheus/prometheus.yaml`**, Prometheus **does not pick them up until you restart** (it does not auto-reload the file):

```bash
cd dashboards/demo
docker compose up -d redis redis-exporter
docker compose restart prometheus
```

Confirm the running container sees the Redis job:

```bash
docker compose exec prometheus grep -A4 'redis_demo' /etc/prometheus/prometheus.yml
```

You should see **`job_name: redis_demo`** and **`redis-exporter:9121`**. If that command prints nothing, your host file is out of date (sync the repo) or the compose project is not **`dashboards/demo`**.

**Note:** If you use **`dashboards/docker-compose.yaml`** instead, Prometheus is on **[http://localhost:9091](http://localhost:9091)**, not 9090, and **`demo_net`** must exist (`docker network create mcac_net` or start **`dashboards/demo`** once first). Prefer running Prometheus from **`dashboards/demo`** with Redis services so DNS names like **`redis-exporter`** resolve.

### Why Grafana.com dashboard **763** often shows “No data” here

ID [763](https://grafana.com/grafana/dashboards/763) is built for **Helm / Kubernetes** Redis: it expects template variables like **Namespace** and **Pod**. On plain Docker those labels are missing, so filters are empty and panels break. **Instance** must match Prometheus’s **`instance`** label (usually **`redis-exporter:9121`**, not a bare host IP like `192.168.10.110` unless that is how the target is configured).

If you still want **763** or **11835**, import them, set **job** to **`redis_demo`**, clear or set **namespace/pod** to match your metrics (or `.*` where the UI allows), and align **instance** with **Explore → `redis_up`**.

| ID | Name | Link |
|----|------|------|
| **763** | Redis (K8s-oriented; often needs variable edits on Docker) | [grafana.com/grafana/dashboards/763](https://grafana.com/grafana/dashboards/763) |
| **11835** | Redis / redis_exporter (Helm-oriented) | [grafana.com/grafana/dashboards/11835](https://grafana.com/grafana/dashboards/11835) |

## Further reading

- Main demo index: **[`../README.md`](../README.md)**  
- **OpenSearch** (search tier in the same stack): **[`../opensearch/README.md`](../opensearch/README.md)**
