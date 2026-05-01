# Kubernetes manifests (demo-hub)

**Shared application docs (same stack as Compose):** [`../../docs/README.md`](../../docs/README.md) · [`../../docs/hub-and-data-flow.md`](../../docs/hub-and-data-flow.md) · [`../../docs/compose-vs-kubernetes.md`](../../docs/compose-vs-kubernetes.md)

Docker Compose for this stack uses **`name: demo-hub`** in [`../../docker-compose.yml`](../../docker-compose.yml). **[`../../start-full-stack.sh`](../../start-full-stack.sh)** is the supported path for a full local run.

If you are new to Kubernetes, read **[Kubernetes basics (for this demo)](#kubernetes-basics-for-this-demo)** first, then **[Basic diagnostics](#basic-diagnostics)** when something fails.

**One command (stack + data bootstrap + all six Kafka Connect connectors):** from **`dashboards/demo`**, run **[`../../demo-start-hub.sh`](../../demo-start-hub.sh)** after custom images are built (Quick start §1). Uses a short-lived **`kubectl port-forward`** to Connect **:8083** and runs **[`register-all-connectors.sh`](../docker/kafka-connect-register/register-all-connectors.sh)** with **`DEMO_HUB_K8S=1`** (includes idempotent MSSQL schema apply). Options: **`REGEN=1`**, **`./demo-start-hub.sh --no-connectors`**.

---

## Quick start (step by step)

All paths below are relative to **`dashboards/demo`** (this repo: `mcac-demo-hub/dashboards/demo`). Namespace is **`demo-hub`** unless you set **`NS`**.

### 1. Build images on your machine (required before first apply)

The cluster **cannot pull** these names from Docker Hub. Build them with Docker (same engine your local Kubernetes uses — OrbStack / Docker Desktop usually share the image store; **kind** / **minikube** need an extra **`kind load docker-image …`** step).

| Image | Role | How it is built |
|-------|------|-----------------|
| **`mcac-demo/mcac-init:local`** | Cassandra **initContainer** copies the MCAC agent into the ring pods (`imagePullPolicy: Never`) | [`deploy/k8s/scripts/build-mcac-init-image.sh`](scripts/build-mcac-init-image.sh) |
| **`mcac-demo/hub-demo-ui:latest`** | Hub FastAPI UI | `docker build -t mcac-demo/hub-demo-ui:latest -f deploy/docker/realtime-orders-search-hub/demo-ui/Dockerfile deploy/docker/realtime-orders-search-hub/demo-ui` |
| **`mcac-demo/kafka-connect:2.7.3-mongo-sink`** | Kafka Connect (Debezium + Mongo sink + SQL Server connector, same [`Dockerfile.connect`](../docker/mongo-kafka/Dockerfile.connect)) | `docker build -t mcac-demo/kafka-connect:2.7.3-mongo-sink -f deploy/docker/mongo-kafka/Dockerfile.connect deploy/docker/mongo-kafka` |
| **`mcac-demo/mssql-tools:22.04`** | **`mssql-demo-bootstrap`** Job (`sqlcmd`, schema + `register-mssql-connectors.sh`) | [`deploy/k8s/scripts/build-mssql-tools-image.sh`](scripts/build-mssql-tools-image.sh) |
| **`demo-hub/nodetool-exporter:latest`** | Scrapes `nodetool` metrics | `docker build -t demo-hub/nodetool-exporter:latest -f deploy/docker/nodetool-exporter/Dockerfile deploy/docker/nodetool-exporter` |
| **`mcac-demo/postgresql-repmgr:16.6.0`** | Bitnami-style Postgres + repmgr (primary/replicas) | See **`build-all-custom-images.sh`** step 6 |

**Public images** (no local build): **`mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04`** (publisher + subscriber), **`awaragi/prometheus-mssql-exporter`**.

**One command** (runs all custom builds):

```bash
cd /path/to/mcac-demo-hub/dashboards/demo
chmod +x deploy/k8s/scripts/build-all-custom-images.sh
./deploy/k8s/scripts/build-all-custom-images.sh
```

After changing Dockerfiles or hub code, rebuild and roll out, for example:

```bash
kubectl rollout restart deployment/kafka-connect deployment/hub-demo-ui deployment/nodetool-exporter \
  deployment/mssql-publisher deployment/mssql-subscriber deployment/mssql-exporter-publisher deployment/mssql-exporter-subscriber \
  statefulset/cassandra -n demo-hub
```

### 2. Generate manifests (if you edit the generator or Prometheus/Grafana assets)

```bash
cd /path/to/mcac-demo-hub/dashboards/demo
python3 deploy/k8s/scripts/gen_demo_hub_k8s.py
```

Committed **`deploy/k8s/generated/all.yaml`** is usually enough; use **`REGEN=1`** with **`demo-hub.sh`** to regenerate before apply.

### 3. Start workloads and bootstrap Jobs

From **`dashboards/demo`**:

```bash
chmod +x deploy/k8s/scripts/demo-hub.sh deploy/k8s/scripts/apply-data-bootstrap.sh
./deploy/k8s/scripts/demo-hub.sh start
```

This **`kubectl apply -f deploy/k8s/generated/all.yaml`** (namespace + everything) and runs **`deploy/k8s/scripts/apply-data-bootstrap.sh`**, which waits for Postgres/Cassandra, then applies/runs the **Postgres**, **Cassandra schema**, and **Mongo** bootstrap **Jobs**.

Useful variants:

| Goal | Command |
|------|---------|
| Full teardown | `./deploy/k8s/scripts/demo-hub.sh stop` |
| Clean restart (optional regen) | `REGEN=1 ./deploy/k8s/scripts/demo-hub.sh restart` |
| Apply YAML only, skip Jobs | `SKIP_BOOTSTRAP=1 ./deploy/k8s/scripts/demo-hub.sh start` |
| Status snapshot | `./deploy/k8s/scripts/demo-hub.sh status` |

### 4. Port-forward from your laptop

ClusterIPs are **not** on `127.0.0.1` until you forward. In a **second terminal**, from **`dashboards/demo`**:

```bash
./deploy/k8s/scripts/port-forward-demo-hub.sh
```

Leave it running. Defaults map **localhost** ports to services/pods in **`demo-hub`** (see script header for **`SKIP_PROMETHEUS=1`** if Prometheus is down, **`SKIP_MSSQL=1`** if SQL Server forwards fail).

**Override local ports** (host already using 9042, 5432, …):

| Environment variable | Default | Remote target |
|---------------------|---------|---------------|
| **`LOCAL_PG_PORT`** | 5432 | `svc/postgresql-primary:5432` |
| **`LOCAL_PG_REPLICA_1_PORT`** | 5433 | `svc/postgresql-replica-1:5432` |
| **`LOCAL_PG_REPLICA_2_PORT`** | 5434 | `svc/postgresql-replica-2:5432` |
| **`LOCAL_CQL_PORT`** | 9042 | **`pod/cassandra-0:9042`** (stable coordinator) |
| **`LOCAL_MONGO_PORT`** | 27017 | `svc/mongo-mongos1:27017` |
| **`LOCAL_REDIS_PORT`** | 6379 | `svc/redis:6379` |
| **`LOCAL_GRAFANA_PORT`** | 3000 | `svc/grafana:3000` |
| **`LOCAL_PROM_PORT`** | 9090 | `svc/prometheus:9090` |
| **`LOCAL_HUB_UI_PORT`** | 8888 | `svc/hub-demo-ui:8888` |
| **`LOCAL_KAFKA_CONNECT_PORT`** | 8083 | `svc/kafka-connect:8083` |
| **`LOCAL_OPENSEARCH_PORT`** | 9200 | `svc/opensearch:9200` |
| **`LOCAL_OS_DASHBOARDS_PORT`** | 5601 | `svc/opensearch-dashboards:5601` |
| **`LOCAL_VAULT_PORT`** | 8200 | `svc/vault:8200` |
| **`LOCAL_MSSQL_PUBLISHER_PORT`** | 14333 | `svc/mssql-publisher:1433` (localhost ↔ publisher; SA password matches Secret **`mssql-sa-password`**, default **`Demo_hub_Mssql_2025!`** — override echo with **`MSSQL_SA_PASSWORD_HINT`**) |
| **`LOCAL_MSSQL_SUBSCRIBER_PORT`** | 14334 | `svc/mssql-subscriber:1433` (subscriber instance) |

Example — use **19042** on the host for CQL:

```bash
LOCAL_CQL_PORT=19042 ./deploy/k8s/scripts/port-forward-demo-hub.sh
# then: cqlsh 127.0.0.1 19042
```

**`NS`** — different namespace: `NS=my-ns ./deploy/k8s/scripts/port-forward-demo-hub.sh` (stack must exist there).

Alternative: **`./deploy/k8s/scripts/demo-hub.sh port-forward`** (waits for Deployments + Cassandra StatefulSet, then runs the same forwards).

### 5. Cassandra (CQL) — port-forward, custom port, and fallbacks

- **Inside the cluster** Cassandra always listens on **9042**. **`LOCAL_CQL_PORT`** only changes the **localhost** side of the forward.
- The script forwards **`pod/cassandra-0`**, not **`svc/cassandra`**, so CQL does not bounce between ring members.
- If **`kubectl port-forward`** logs **socat** / **Connection reset by peer** (common on some local clusters), use one of:
  - **Exec** (no forward): `kubectl exec -it -n demo-hub cassandra-0 -c cassandra -- cqlsh localhost 9042`
  - **NodePort** (DBeaver / GUI): `kubectl apply -f deploy/k8s/optional-cassandra-0-cql-nodeport.yaml` → connect to **`<node-ip>:30942`** (see [optional-cassandra-0-cql-nodeport.yaml](optional-cassandra-0-cql-nodeport.yaml)); delete the Service when finished if you do not want it permanently.

### 6. Kafka Connect connectors (Postgres + Mongo manual on K8s; MSSQL via Job or script)

After **`LOCAL_KAFKA_CONNECT_PORT`** is reachable (default **8083**):

**Postgres + Mongo only (four connectors):**

```bash
cd /path/to/mcac-demo-hub/dashboards/demo
DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all.sh http://127.0.0.1:8083
```

**All six (adds SQL Server Debezium + JDBC sink)** — same port-forward; MSSQL schema must exist (e.g. **`mssql-demo-bootstrap`** completed):

```bash
DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all-connectors.sh http://127.0.0.1:8083
```

**Resume** existing connectors without re-registering: [`resume-kafka-connectors.sh`](../docker/kafka-connect-register/resume-kafka-connectors.sh).

**`DEMO_HUB_K8S=1`** sets Debezium schema-history bootstrap to **`kafka:9092`** (Compose uses **`kafka:29092`**). See [`register-all.sh`](../docker/kafka-connect-register/register-all.sh) and [`register-all-connectors.sh`](../docker/kafka-connect-register/register-all-connectors.sh).

### HashiCorp Vault (demo secrets + connector fields)

The generator emits **`deploy/k8s/generated/02-vault.yaml`**: a **single-replica Vault in `-dev` mode** (in-memory storage — **secrets are lost if the Vault pod restarts**; re-run the seed Job). This is for **local/demo only**, not production HA.

| Item | Value |
|------|--------|
| Service | **`vault`** in **`demo-hub`**, port **8200** |
| Root token | **`demo-hub-dev-root`** (fixed; rotate for anything beyond a lab) |
| KV paths | **`secret/demo-hub/credentials`** (same keys as `demo-hub-credentials` Secret), **`secret/demo-hub/kafka-connect/*`** (Postgres/Mongo connector fields) |

After apply, wait for **`vault-demo-hub-seed`** Job to complete. If the Job ran before Vault was ready, delete and re-apply:

```bash
kubectl delete job -n demo-hub vault-demo-hub-seed --ignore-not-found
kubectl apply -f deploy/k8s/generated/02-vault.yaml
```

**Web UI / API** (with port-forward): open **`http://127.0.0.1:8200`**, sign in with token **`demo-hub-dev-root`**, or use Ingress **`http://vault.demo-hub.local`** if that host resolves.

**Vault CLI:** use the **demo** token on **this** server — a corporate `VAULT_TOKEN` in your shell will yield **403** on reads.

Vault **1.11+** CLI:

```bash
VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=demo-hub-dev-root vault kv get -mount=secret demo-hub/credentials
```

Older **`vault` binaries** (error: `flag provided but not defined: -mount`) — use the full KV v2 path:

```bash
VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=demo-hub-dev-root vault read secret/data/demo-hub/credentials
```

Or the legacy-style path (works on many versions):

```bash
vault kv get secret/demo-hub/credentials
```

**List every secret path** (recursive metadata walk, stdlib Python only):

```bash
chmod +x deploy/k8s/scripts/vault-list-secrets.sh
./deploy/k8s/scripts/vault-list-secrets.sh
./deploy/k8s/scripts/vault-list-secrets.sh --prefix demo-hub --full-paths
```

**Connectors from Vault** — load env vars then register (same scripts as Compose):

```bash
source deploy/k8s/scripts/export-demo-hub-secrets-from-vault.sh
DEMO_HUB_K8S=1 ./deploy/docker/kafka-connect-register/register-all.sh http://127.0.0.1:8083
```

If your shell already exports **`VAULT_ADDR`** for a **corporate Vault**, the export script **ignores** it and uses **`http://127.0.0.1:8200`** (after port-forward). To reuse global `VAULT_ADDR` / `VAULT_TOKEN` instead, run with **`DEMO_HUB_RESPECT_GLOBAL_VAULT=1`**. To point at another instance explicitly: **`DEMO_HUB_VAULT_ADDR=...`** and **`DEMO_HUB_VAULT_TOKEN=...`**.

**Runtime:** Pods (hub UI, Postgres, Redis, etc.) **do not** read Vault. They use **`demo-hub-credentials`** only. Cassandra, Kafka, and OpenSearch are contacted via in-cluster DNS; this demo does not put their connection secrets on a Vault path for the app. Vault mirrors the hub/Postgres/Redis-style credentials for **operators** and **Kafka Connect registration** (`export-demo-hub-secrets-from-vault.sh`). To drive workloads from Vault, add External Secrets / Vault Agent (or similar) and point manifests at the synced Secret—not included here.

### 7. Where things live (reference)

| What | Location |
|------|----------|
| Generated manifests | [`deploy/k8s/generated/`](generated/) — apply **`all.yaml`** once |
| Generator source | [`deploy/k8s/scripts/gen_demo_hub_k8s.py`](scripts/gen_demo_hub_k8s.py) |
| Start / stop / restart | [`deploy/k8s/scripts/demo-hub.sh`](scripts/demo-hub.sh) |
| Port-forward script | [`deploy/k8s/scripts/port-forward-demo-hub.sh`](scripts/port-forward-demo-hub.sh) |
| Bootstrap Jobs orchestration | [`deploy/k8s/scripts/apply-data-bootstrap.sh`](scripts/apply-data-bootstrap.sh) |
| Build all custom images | [`deploy/k8s/scripts/build-all-custom-images.sh`](scripts/build-all-custom-images.sh) |
| Cassandra CQL NodePort (optional) | [`deploy/k8s/optional-cassandra-0-cql-nodeport.yaml`](optional-cassandra-0-cql-nodeport.yaml) |
| Job descriptions | [`deploy/k8s/generated/README-jobs.md`](generated/README-jobs.md) |

---

## Kubernetes basics (for this demo)

Kubernetes (**K8s**) runs **workloads** on one or more **nodes** (machines or VMs). You declare what you want in YAML; the **control plane** (scheduler, API) places **pods** on nodes and keeps them running.

### Core ideas

| Concept | Plain-language meaning |
|--------|-------------------------|
| **Cluster** | The whole K8s installation (API + nodes + add-ons). |
| **Node** | One machine (physical or VM) that runs your containers. Local clusters (Docker Desktop, OrbStack, kind, minikube) often have **one** node. |
| **Namespace** | A logical partition of resources (like a folder). This demo uses **`demo-hub`** so names don’t collide with other apps. Most commands need **`-n demo-hub`**. |
| **Pod** | The smallest runnable unit: usually **one main container** (sometimes + sidecars/init). A pod gets an IP inside the cluster and shares storage/network namespace. |
| **Deployment** | Runs **stateless** apps: keeps **N replicas** of a pod template, rolls out updates, replaces crashed pods. Example here: **Grafana**, **Prometheus**, **Kafka**, many **exporters**. |
| **StatefulSet** | Like a Deployment, but for **stateful** apps: **stable names** (`cassandra-0`, `cassandra-1`, …), ordered start, and often paired with a **headless Service** for DNS per pod. Example: **Cassandra ring**. |
| **Service** | A **stable DNS name + port** that load-balances to matching pods. **ClusterIP** (default here) is only reachable **inside** the cluster unless you **port-forward** or use Ingress/LoadBalancer. |
| **Headless Service** (`clusterIP: None`) | No cluster virtual IP; DNS returns **each pod’s IP** (used for Cassandra peer discovery). |
| **ConfigMap** | Key/value (often files) injected into pods as env vars or mounted files. Example: **Prometheus config**, **Grafana datasources**, **OpenSearch `opensearch.yml`**, **MCAC agent config**. |
| **Job** | Runs a pod **to completion** (migrations, bootstrap SQL). Example: **Postgres/Cassandra/Mongo bootstrap** jobs. |
| **ReplicaSet** | Implementation detail behind Deployments; you rarely edit it directly. |

### Which Kubernetes resource is used for what (this repo)

| K8s resource | Role in *this* demo |
|--------------|---------------------|
| **Namespace** (`demo-hub`) | Isolates all demo objects; use `-n demo-hub` on almost every command. |
| **Deployment** | Long-running **stateless** processes: ZooKeeper, Kafka, Postgres/Redis/Mongo/OpenSearch **pods**, Grafana, Prometheus, Connect, exporters, hub UI, etc. |
| **StatefulSet** | **Cassandra** 3-node ring: predictable pod names and storage identity per replica. |
| **Service (ClusterIP)** | Stable names like **`kafka:9092`**, **`prometheus:9090`**, **`cassandra:9042`** inside the namespace—what apps use instead of pod IPs. |
| **Service (headless)** | **`cassandra-headless`** — DNS for **`cassandra-0.cassandra-headless.demo-hub.svc.cluster.local`** etc., for gossip/seeds and MCAC scrape targets. |
| **ConfigMap** | Injects **Prometheus `prometheus.yaml`**, **`tg_mcac.json`** (MCAC scrape list), Grafana provisioning, OpenSearch config, MCAC agent YAML beside Cassandra. |
| **Job** | One-shot bootstrap: schema, replication setup, Mongo sharding steps (see `README-jobs.md`). |
| **emptyDir volume** | Ephemeral disk for most Deployments (ZooKeeper, Kafka, Postgres, Redis, Mongo, etc.). **Cassandra** uses **PersistentVolumeClaims** (`10Gi` per replica) via [`30-cassandra-ring.yaml`](generated/30-cassandra-ring.yaml); your cluster needs a default **StorageClass** (or set `CASSANDRA_DATA_STORAGE_CLASS` / `CASSANDRA_DATA_STORAGE_SIZE` in [`scripts/gen_demo_hub_k8s.py`](scripts/gen_demo_hub_k8s.py) and regenerate). |
| **Secret** | [`01-demo-hub-credentials.yaml`](generated/01-demo-hub-credentials.yaml) holds demo passwords; Postgres, Redis, hub UI, and postgres-exporter read them via **`valueFrom.secretKeyRef`**. |
| **Ingress / PDB / HPA / CronJob / NetworkPolicy** | [`96-kubernetes-ops.yaml`](generated/96-kubernetes-ops.yaml): multi-host **Ingress** (`demo-hub.local`, `grafana.demo-hub.local`, `prometheus.demo-hub.local` — add `/etc/hosts` and an Ingress controller), **PodDisruptionBudgets**, **HorizontalPodAutoscaler** for hub UI (needs **metrics-server**), a **CronJob** smoke check, and a **NetworkPolicy** (in-cluster + `ingress-nginx` namespace). |
| **ServiceMonitor (Prometheus Operator)** | Not used by the bundled static Prometheus. See [`optional-prometheus-operator-servicemonitors.example.yaml`](optional-prometheus-operator-servicemonitors.example.yaml) if you scrape with the Operator instead. |

**How traffic flows:** A pod calls another **by Service DNS**, e.g. `http://prometheus:9090` from Grafana. Your laptop does **not** see ClusterIP services until you run **`kubectl port-forward`** (see [`scripts/port-forward-demo-hub.sh`](scripts/port-forward-demo-hub.sh)), or you install an **Ingress controller** and use the hosts in `96-kubernetes-ops.yaml`.

### New / extended resources (what was added vs earlier manifests)

The generator ([`scripts/gen_demo_hub_k8s.py`](scripts/gen_demo_hub_k8s.py)) was extended beyond “Deployments + Services + ConfigMaps + Jobs” so the cluster manifest set is closer to a small production layout. **Behavioral summary:**

| Resource | Where | Before (typical older demo YAML) | After (current generated stack) |
|----------|--------|-----------------------------------|----------------------------------|
| **Secret** | [`01-demo-hub-credentials.yaml`](generated/01-demo-hub-credentials.yaml) | Passwords often inlined in Deployment `env` or ConfigMap `data` | **`demo-hub-credentials`** (`Opaque`, `stringData`) — Postgres (primary + replicas), Redis + redis-exporter, hub **`POSTGRES_DSN`** / **`REDIS_URL`**, postgres-exporters use **`valueFrom.secretKeyRef`**. Bootstrap Jobs still embed the same demo passwords in scripts for idempotency; rotate the Secret and scripts together if you change passwords. |
| **Cassandra data disk** | [`30-cassandra-ring.yaml`](generated/30-cassandra-ring.yaml) | **`emptyDir`** under `/var/lib/cassandra` (ephemeral) | **`volumeClaimTemplates`** — **10Gi** `ReadWriteOnce` PVC per ordinal (`data-*`); uses cluster **default StorageClass** unless you set `CASSANDRA_DATA_STORAGE_CLASS` / size in the generator. Requires a **default StorageClass** (or a named class you set). |
| **Hub → Cassandra contact points** | [`95-hub-demo-ui.yaml`](generated/95-hub-demo-ui.yaml) | **`CASSANDRA_HOSTS=cassandra`** (ClusterIP Service) | **Headless FQDNs** for `cassandra-0..2.cassandra-headless...` — avoids **`NoHostAvailable`** when the driver discovers a peer not yet listening on **9042**. App code also **retries** connect (env: `CASSANDRA_CONNECT_MAX_ATTEMPTS`, `CASSANDRA_CONNECT_RETRY_DELAY_SEC`). |
| **Ingress** | [`96-kubernetes-ops.yaml`](generated/96-kubernetes-ops.yaml) | Not present; only ClusterIP + port-forward | **`demo-hub-ingress`** — hosts **`demo-hub.local`** (hub UI), **`grafana.demo-hub.local`**, **`prometheus.demo-hub.local`**. **Does nothing** until an **Ingress controller** is installed and DNS/hosts point to it. **Does not replace** port-forward by default. |
| **PodDisruptionBudget** | same | Not present | **`cassandra-pdb`** (`minAvailable: 2`), **`postgresql-primary-pdb`**, **`hub-demo-ui-pdb`** — limits voluntary disruption during drains/upgrades (when the scheduler respects PDBs). |
| **HorizontalPodAutoscaler** | same | Not present | **`hub-demo-ui`** — CPU **70%**, replicas **1–3**. **Needs [metrics-server](https://github.com/kubernetes-sigs/metrics-server)** in the cluster; without it, the HPA object exists but scaling metrics stay unknown. |
| **NetworkPolicy** | same | Not present | **`demo-hub-namespace-isolation`** — selects pods with **`app.kubernetes.io/part-of: demo-hub`**; **ingress** from namespace **`demo-hub`** and from **`ingress-nginx`** (so the Ingress controller can reach Services); **egress** allowed broadly (`egress: - {}`) so DNS and pulls still work. Adjust if your Ingress runs in another namespace. |
| **CronJob** | same | Not present | **`demo-hub-smoke-curl`** — every **30** minutes, curls **`http://hub-demo-ui:8888/docs`** and **`http://hub-demo-ui:8888/api/scenario/faker-profile`** (in-cluster smoke; not a substitute for monitoring). |
| **ServiceMonitor** | [`optional-prometheus-operator-servicemonitors.example.yaml`](optional-prometheus-operator-servicemonitors.example.yaml) | N/A | **Example only** — for **Prometheus Operator** users. The bundled stack uses a **static Prometheus Deployment** + `scrape_configs`, not the Operator. |

**Optional (not in `all.yaml` by composition logic, separate files):**

| File | Purpose |
|------|---------|
| [`optional-cassandra-0-cql-nodeport.yaml`](optional-cassandra-0-cql-nodeport.yaml) | NodePort **30942** → **`cassandra-0:9042`** for host tools when port-forward is unreliable. **`stop-start-all-k8s.sh`** applies it by default (`APPLY_CASSANDRA_CQL_NODEPORT=1`). |
| [`optional-prometheus-operator-servicemonitors.example.yaml`](optional-prometheus-operator-servicemonitors.example.yaml) | Example **ServiceMonitor** CRs — apply only if you use kube-prometheus-stack / Prometheus Operator. |

**Laptop access:** **Ingress** and **port-forward** are **alternatives** for reaching UIs from your machine; most local clusters still use **[`port-forward-demo-hub.sh`](scripts/port-forward-demo-hub.sh)** unless you install Ingress and configure hosts. See **[`scripts/stop-start-all-k8s.sh`](scripts/stop-start-all-k8s.sh)** — defaults **`WITH_PORT_FORWARD=0`** so restart does not block on forwards.

---

## How pods talk to each other (and how that relates to Postgres, Mongo, and Cassandra)

In Kubernetes, the word **“node”** usually means a **worker machine** (VM or bare metal) that runs containers. **Database nodes** in this demo are **Pods** (or sets of Pods) scheduled onto those machines. They discover and reach each other using **cluster DNS** and **Services**, not by hard-coding Pod IPs (which change when Pods restart).

### Services, DNS, and Pod IPs

| Mechanism | What it gives you |
|-----------|-------------------|
| **Pod IP** | Each Pod gets an IP on the cluster network. It is **ephemeral** — do not bake it into config. |
| **ClusterIP Service** | A **stable virtual IP + port** inside the namespace. **kube-proxy** (or equivalent) forwards traffic to **one of** the Pods that match the Service’s **selector**. Short name: **`postgresql-primary`** resolves to that Service. |
| **Headless Service** (`clusterIP: None`) | No single cluster IP. For a **StatefulSet**, DNS returns **one A record per Pod**, e.g. **`cassandra-0.cassandra-headless.demo-hub.svc.cluster.local`**, so processes can address **specific replicas** by name. |
| **FQDN** | Long form: **`<service>.<namespace>.svc.cluster.local`**. Inside the same namespace you can often use just **`<service>`** or **`<pod>-<ordinal>.<headless-service>`**. |

So: **stateless clients** (Grafana → Prometheus) use a normal Service name. **Stateful clusters** (Cassandra, Mongo replica sets) use **per-Pod DNS** or **Service names** as documented by the image so peers connect over the **same ports as on bare metal** (Postgres **5432**, Cassandra **7000/7001** gossip, **9042** CQL, Mongo **27017**), but the **hostname** is a Kubernetes DNS name.

### PostgreSQL replication (primary + replicas)

The generator ([`40-postgresql-ha.yaml`](generated/40-postgresql-ha.yaml)) runs **three Deployments**: **`postgresql-primary`** (Bitnami **`POSTGRESQL_REPLICATION_MODE=master`**) and **`postgresql-replica-1`**, **`postgresql-replica-2`** (**`slave`**).

- **How replicas find the primary:** Each replica has **`POSTGRESQL_MASTER_HOST=postgresql-primary`**. That is the **Kubernetes Service** name for the primary Pod. DNS resolves it to the primary’s **ClusterIP**; the replica opens a normal **PostgreSQL streaming replication** connection to **port 5432** (WAL stream), using user **`replicator`** / **`replicatorpass`**. Each replica uses a distinct **`primary_slot_name`** (`pgdemo_phys_replica_1` / `_2`) so physical slots line up with the bootstrap SQL.
- **What the primary enables:** **`wal_level=logical`**, **`max_replication_slots`**, **`max_wal_senders`** — required so **logical replication** (Debezium-style) can work after the **[`45-postgres-bootstrap-job.yaml`](generated/45-postgres-bootstrap-job.yaml)** Job runs (publication, replication slots for the demo pipeline). That Job is **not** what creates the Bitnami streaming replicas; it extends the primary for **logical** CDC on top of the running **physical** replica topology.

From another Pod in **`demo-hub`**, you connect to the writer with host **`postgresql-primary`** and to a specific read replica with **`postgresql-replica-1`** / **`postgresql-replica-2`** (each has its own Service).

### Cassandra: ring, seeds, and replication factor

Cassandra runs as a **StatefulSet** ([`30-cassandra-ring.yaml`](generated/30-cassandra-ring.yaml)) with **`replicas: 3`** → Pods **`cassandra-0`**, **`cassandra-1`**, **`cassandra-2`**.

- **Seeds:** **`CASSANDRA_SEEDS`** is set to **`cassandra-0.cassandra-headless.<namespace>.svc.cluster.local`** so new nodes join the ring via stable DNS to the first ordinal.
- **Peer / gossip:** Cassandra uses **inter-node ports** (e.g. **7000** / **7001**) between Pods. The **headless Service** **`cassandra-headless`** exposes those ports and publishes DNS for each Pod so members can resolve one another (gossip does not rely on a single load-balanced VIP for “who is in the cluster”).
- **Client CQL:** The regular Service **`cassandra`** (ClusterIP) balances **CQL :9042** across ready Pods for app-style access.
- **Replication factor:** The keyspace **RF=3** (and schema) is applied by the **`cassandra-demo-schema`** Job ([`35-cassandra-schema-job.yaml`](generated/35-cassandra-schema-job.yaml)), which talks to **`cassandra-0`** via headless DNS — same idea as `nodetool`/CQL against one coordinator.

So in K8s, Cassandra “nodes” are **Pods**; they talk to each other like on VMs, but **addresses are Kubernetes DNS names** on the cluster network.

### MongoDB: config servers, shards, and mongos

The generator ([`60-mongo-sharded.yaml`](generated/60-mongo-sharded.yaml)) matches the Compose layout:

| Role | Deployments | Purpose |
|------|-------------|---------|
| **Config servers** | **`mongo-config1`**, **`mongo-config2`**, **`mongo-config3`** | Run **`mongod --configsvr`**; form replica set **`configReplSet`**. Hold cluster metadata (which chunks live on which shard). |
| **Shards** | **`mongo-shard-tic`**, **`mongo-shard-tac`**, **`mongo-shard-toe`** | Run **`mongod --shardsvr`**; each shard is its own **replica set** (`tic`, `tac`, `toe`). Store actual data. |
| **Routers** | **`mongo-mongos1`**, **`mongo-mongos2`**, **`mongo-mongos3`** | Run **`mongos`** with **`--configdb`** pointing at **`configReplSet/mongo-config1:27017,mongo-config2:27017,mongo-config3:27017`**. Those hostnames are **Kubernetes Service names** in **`demo-hub`**; each resolves to the corresponding config **Pod**. |

Applications and demos should use a **`mongos`** Service (e.g. port-forward to **`mongo-mongos1`**) — **`mongos`** routes queries to the right shard using metadata from the config servers. **Sharding is not automatic** when the processes start: the **[`61-mongo-bootstrap-job.yaml`](generated/61-mongo-bootstrap-job.yaml)** Job runs the same shell steps as Compose (init config replica set, init shard replica sets, **`addShard`**, collection sharding). Until that Job succeeds, you have processes but not a finished sharded cluster.

### Summary

- **K8s networking:** Pods reach each other with **Service DNS** (and **headless** DNS for StatefulSets). **“Nodes”** in the database sense = **Pods** here.
- **Postgres:** Streaming replication from **primary Service** to replicas; bootstrap Job adds **logical** replication artifacts for CDC.
- **Cassandra:** Gossip + CQL across Pods; **seeds** and **headless DNS** replace fixed IP lists.
- **Mongo:** **mongos** → **config servers** + **shard** `mongod`s; bootstrap Job wires replica sets and **`addShard`**.

---

## Basic diagnostics

`kubectl` talks to the cluster API. Always set the namespace for this demo:

```bash
export NS=demo-hub   # optional shorthand
kubectl get pods -n demo-hub
```

### Commands to learn first

| Command | What it tells you |
|--------|-------------------|
| `kubectl get pods -n demo-hub` | List pods and **phase** (`Running`, `Pending`, `Error`, …). |
| `kubectl get pods -n demo-hub -o wide` | Adds **NODE** and **IP** (see which node runs the workload). |
| `kubectl describe pod -n demo-hub <pod-name>` | **Events** at the bottom are critical: scheduling failures, image pull errors, disk pressure, OOM, etc. |
| `kubectl logs -n demo-hub deploy/<name> --tail=100` | Last lines of the **main container** for a Deployment. |
| `kubectl logs -n demo-hub <pod-name> -c <container> --tail=100` | Use **`-c`** when the pod has multiple containers (e.g. init + app). |
| `kubectl get events -n demo-hub --sort-by='.lastTimestamp' \| tail -30` | Recent cluster events (often explains mass `Pending` or evictions). |
| `kubectl get svc -n demo-hub` | Services and **ports** exposed inside the namespace. |
| `kubectl get deploy,sts -n demo-hub` | Deployments and StatefulSets (desired vs ready replicas). |

### Pod phases (what `STATUS` means)

| Phase | Meaning |
|-------|---------|
| **Pending** | Not running yet—often **scheduling** (no CPU/RAM, **disk-pressure** taint, volume issue) or **image pull**. Read **Events** on `kubectl describe pod`. |
| **Running** | At least one container is up (may still be crash-looping if `READY` is `0/1`). |
| **Error** / **CrashLoopBackOff** | Container exits repeatedly—use **`kubectl logs`** on that pod. |
| **Completed** | All containers exited **0**. Normal for **Jobs**; for Deployments it usually means the process exited (misconfig or wrong image). |
| **ImagePullBackOff** | Node cannot download the image (name wrong, private registry, or offline). |

### Node-level issues (why “everything is Pending”)

If **`describe pod`** shows **`FailedScheduling`** and **`disk-pressure`** (or **`Insufficient memory` / `cpu`**):

- **Disk pressure:** Free space on the host (Docker/OrbStack disk, prune unused images/volumes). Until the taint clears, new pods may not schedule.
- **Insufficient resources:** The demo requests a lot of RAM/CPU; a single small node cannot fit every pod. Free resources or run a subset of manifests.

See **[Troubleshooting](#troubleshooting)** for stack-specific issues.

---

## Generated workloads (recommended)

**[`scripts/gen_demo_hub_k8s.py`](scripts/gen_demo_hub_k8s.py)** mirrors Compose **service names, images, env, and ports** into **`generated/`**:

| File | Contents |
|------|----------|
| `00-namespace.yaml` | Copy of [`namespace.yaml`](namespace.yaml) |
| `01-demo-hub-credentials.yaml` | **Secret** `demo-hub-credentials` (demo passwords; workloads use `secretKeyRef`) |
| `10-observability-prometheus-grafana.yaml` | Prometheus + Grafana **Deployments**, **Services**, **ConfigMaps** — embeds [`dashboards/prometheus/prometheus.yaml`](../../prometheus/prometheus.yaml), [`demo/tg_mcac.json`](../tg_mcac.json), [`dashboards/grafana/prometheus-datasource.yaml`](../../grafana/prometheus-datasource.yaml), [`dashboards/grafana/dashboards.yaml`](../../grafana/dashboards.yaml), and all [`dashboards/grafana/generated-dashboards/*.json`](../../grafana/generated-dashboards/) at generate time (same layout as Compose). |
| `20-zookeeper-kafka.yaml` | ZooKeeper + Kafka |
| `30-cassandra-ring.yaml` | Cassandra **StatefulSet** (3 replicas) + **PVCs** (`volumeClaimTemplates`) + headless + client **Service** |
| `35-cassandra-schema-job.yaml` | Job: RF=3 keyspace / placeholder table |
| `40-postgresql-ha.yaml` | Bitnami Postgres primary + 2 replicas (passwords from Secret) |
| `45-postgres-bootstrap-job.yaml` | Job: Debezium + slots + scenario SQL |
| `50-redis.yaml` | Redis + redis-exporter (Redis password from Secret) |
| `60-mongo-sharded.yaml` | Config servers, shard `mongod`s, mongos (same DNS names as Compose) |
| `61-mongo-bootstrap-job.yaml` | Job: sharded cluster init + collections |
| `62-mssql.yaml` | SQL Server **publisher** + **subscriber** (Deployments), **mssql-exporter-***, ConfigMap + **mssql-demo-bootstrap** Job (schema, optional replication try, Debezium/JDBC registration) |
| `70-kafka-connect.yaml` | Kafka Connect |
| `80-exporters.yaml` | Postgres exporters, kafka-exporter, mongodb-exporter |
| `90-opensearch.yaml` | OpenSearch (+ ConfigMap for `opensearch.yml`), Dashboards, elasticsearch-exporter |
| `95-hub-demo-ui.yaml` | Hub demo UI (headless **CASSANDRA_HOSTS**, DSN/Redis from Secret) |
| `96-kubernetes-ops.yaml` | **Ingress**, **PDB**, **HPA**, **NetworkPolicy**, **CronJob** |
| `98-nodetool-stress.yaml` | Nodetool-exporter + tlp-stress |
| `all.yaml` | Single concatenation of all of the above (apply once) |
| `README-jobs.md` | Notes on one-shot **Jobs** |

Labels:

- **`demo-hub.io/group`**: `cassandra-ring`, `postgres-ha`, `mongo-config-servers`, `mongo-shards`, `mongo-mongos`, `kafka`, `mssql`, `observability`, `opensearch`, …
- **`app.kubernetes.io/part-of: demo-hub`**

**Regenerate:**

```bash
python3 scripts/gen_demo_hub_k8s.py
```

**Grafana dashboards & Prometheus (how data flows):**

- **Grafana does not scrape exporters.** It only talks to **Prometheus** (datasource URL `http://prometheus:9090`). Whatever Prometheus has scraped becomes queryable in Grafana.
- **Prometheus** embeds [`dashboards/prometheus/prometheus.yaml`](../../prometheus/prometheus.yaml) with **Kubernetes-specific tweaks** from **`gen_demo_hub_k8s.py`**: MCAC file_sd uses **`cassandra-0..2.cassandra-headless:9103`** (StatefulSet), not Compose hostnames; the **`mongodb-exporter-local`** scrape (Compose-only) is **removed**; MCAC job uses **`scrape_interval: 30s`** and **`scrape_timeout: 28s`** (timeout must not exceed interval — a 30s timeout with a 15s interval breaks Prometheus 2.17). Jobs otherwise match the file: `mcac`, `nodetool`, `mongodb`, `postgres_pgdemo`, `kafka_pgdemo`, `redis_demo`, `opensearch_demo`, self-scrape.
- After editing JSON dashboards, run [`dashboards/grafana/make-dashboards.sh`](../../grafana/make-dashboards.sh) if your workflow regenerates them, then **`python3 scripts/gen_demo_hub_k8s.py`** and re-apply `generated/10-observability-prometheus-grafana.yaml`.

**Apply:**

Prefer **`generated/all.yaml` once** (do **not** also apply every other file in `generated/` — that would apply the same objects twice):

```bash
kubectl apply -f generated/all.yaml
```

(`namespace` is included at the top of `all.yaml`.)

**Connect from your laptop (databases + Grafana + Prometheus + Hub UI):** All of these are **ClusterIP** — nothing is reachable from your Mac/PC except **inside** the cluster until you **port-forward**, use an **Ingress**, or a **LoadBalancer**.

From `dashboards/demo`, run (leave the terminal open):

```bash
chmod +x deploy/k8s/scripts/port-forward-demo-hub.sh
./deploy/k8s/scripts/port-forward-demo-hub.sh
```

Then open or connect locally:

| Service | URL / connection |
|--------|-------------------|
| **Grafana** | http://127.0.0.1:3000/ |
| **Prometheus** | http://127.0.0.1:9090/ |
| **Hub demo UI** | http://127.0.0.1:8888/ · **Faker + map → `scenario_orders`:** http://127.0.0.1:8888/scenario (step **3 · Place order**; `/faker-order` redirects here) |
| **OpenSearch** (REST API) | http://127.0.0.1:9200/ |
| **OpenSearch Dashboards** | http://127.0.0.1:5601/ (same URL the hub UI home page links to) |
| **Postgres** | `psql "postgresql://demo:demopass@127.0.0.1:5432/demo"` |
| **Cassandra** | `cqlsh 127.0.0.1 9042` |
| **MongoDB (mongos)** | `mongosh "mongodb://127.0.0.1:27017/"` |
| **Redis** | `redis-cli -h 127.0.0.1 -p 6379 -a demoredispass` (or URI `redis://:demoredispass@127.0.0.1:6379/0`) |

If a port is already in use locally, override before running the script, e.g. `LOCAL_PROM_PORT=19090 LOCAL_GRAFANA_PORT=13000 LOCAL_REDIS_PORT=16379 ./deploy/k8s/scripts/port-forward-demo-hub.sh`. For OpenSearch use `LOCAL_OPENSEARCH_PORT` / `LOCAL_OS_DASHBOARDS_PORT` (defaults **9200** / **5601**). **Cassandra on the host:** set **`LOCAL_CQL_PORT`** (default **9042**); full **`LOCAL_*`** table: [Quick start, step 4](#4-port-forward-from-your-laptop).

If **`kubectl port-forward`** prints **Connection refused** on **:9090** (often with **socat** in the message), the **Prometheus container is not listening** — usually **CrashLoopBackOff** or not **Ready**. Fix the pod (`kubectl logs deploy/prometheus -n demo-hub`), or temporarily skip that forward so other ports still work: **`SKIP_PROMETHEUS=1 ./deploy/k8s/scripts/port-forward-demo-hub.sh`**.

**Cassandra / `cqlsh` / DBeaver:** [`port-forward-demo-hub.sh`](scripts/port-forward-demo-hub.sh) forwards **`pod/cassandra-0:9042`**, not **`svc/cassandra`** (the Service can bounce between replicas and confuse CQL). If **`kubectl port-forward`** still shows **socat “Connection reset by peer”** / **lost connection to pod** even for **`pod/cassandra-0`**, your runtime (often **OrbStack** / **Docker Desktop** Kubernetes) may not relay Cassandra’s native protocol reliably. **Workaround — run `cqlsh` inside the ring pod** (no port-forward):

```bash
kubectl exec -it -n demo-hub cassandra-0 -c cassandra -- cqlsh localhost 9042
```

Check the cluster from inside: **`kubectl exec -n demo-hub cassandra-0 -c cassandra -- nodetool status`**. For **DBeaver on your Mac** when port-forward keeps failing, from **`dashboards/demo`** run **`kubectl apply -f deploy/k8s/optional-cassandra-0-cql-nodeport.yaml`** (see **[`optional-cassandra-0-cql-nodeport.yaml`](optional-cassandra-0-cql-nodeport.yaml)** — targets **only `cassandra-0`**, NodePort **30942**). Connect DBeaver to **`<node-ip>:30942`** (`kubectl get nodes -o wide` — your node **INTERNAL-IP** was **192.168.139.2** in one OrbStack run; try that or **localhost**). Remove when finished: **`kubectl delete -f deploy/k8s/optional-cassandra-0-cql-nodeport.yaml`**.

**Lifecycle — one script (`start` / `stop` / `restart` / `status` / `wait-ready` / `port-forward`):**

From `dashboards/demo`, use **[`deploy/k8s/scripts/demo-hub.sh`](scripts/demo-hub.sh)** as the single entry point. **Order matters:** `stop` deletes the **`demo-hub`** namespace (full teardown); `start` applies **`generated/all.yaml` once** then runs **`apply-data-bootstrap.sh`** (waits for Postgres/Cassandra, then Jobs). **`restart`** is `stop` then `start` (same as the old **`stop-start-all-k8s.sh`**).

```bash
chmod +x deploy/k8s/scripts/demo-hub.sh

./deploy/k8s/scripts/demo-hub.sh status    # what is running
./deploy/k8s/scripts/demo-hub.sh stop      # tear down everything in the namespace
./deploy/k8s/scripts/demo-hub.sh start     # apply manifests + bootstrap (idempotent apply)

# Clean slate (regenerate YAML optional, then full cycle):
REGEN=1 ./deploy/k8s/scripts/demo-hub.sh restart

# Workloads only (skip Postgres/Cassandra/Mongo Jobs — e.g. data already initialized):
SKIP_BOOTSTRAP=1 ./deploy/k8s/scripts/demo-hub.sh start

# After stack + bootstrap: wait until all part-of=demo-hub Deployments + Cassandra STS are Ready, then port-forward (blocks — Ctrl+C stops):
./deploy/k8s/scripts/demo-hub.sh port-forward

# One shot: bootstrap, wait for workloads Ready, then port-forward (same forwards as port-forward-demo-hub.sh):
WITH_PORT_FORWARD=1 ./deploy/k8s/scripts/demo-hub.sh start
# or: WITH_PORT_FORWARD=1 REGEN=1 ./deploy/k8s/scripts/demo-hub.sh restart

# If Prometheus is down but you want other ports: SKIP_PROMETHEUS=1 ./deploy/k8s/scripts/demo-hub.sh port-forward
```

**`wait-ready`** / **`port-forward`** use `kubectl wait` on **Deployments** labeled **`app.kubernetes.io/part-of=demo-hub`** and **`kubectl rollout status`** on **`statefulset/cassandra`**. Override wait duration with **`WAIT_READY_TIMEOUT`** (default **`900s`**). Legacy alias **`./deploy/k8s/scripts/stop-start-all-k8s.sh`** runs **`demo-hub.sh restart`** with defaults: **`APPLY_CASSANDRA_CQL_NODEPORT=1`** (applies **[`optional-cassandra-0-cql-nodeport.yaml`](optional-cassandra-0-cql-nodeport.yaml)** after bootstrap — DBeaver on **30942**) and **`WITH_PORT_FORWARD=0`** so the script **does not** block on **`kubectl port-forward`** (use Ingress, NodePort, or run **[`port-forward-demo-hub.sh`](scripts/port-forward-demo-hub.sh)** separately when you need localhost ports). Use **`WITH_PORT_FORWARD=1 ./deploy/k8s/scripts/stop-start-all-k8s.sh`** to forward after restart. **`demo-hub.sh restart`** alone does not set **`APPLY_CASSANDRA_CQL_NODEPORT`** unless you export it. Other env: **`REGEN`**, **`SKIP_BOOTSTRAP`**, **`SKIP_PROMETHEUS`**.

**Docker Compose** (not Kubernetes): from `dashboards/demo` run `docker compose down` then `./start-full-stack.sh` or `docker compose up -d`.

**Cassandra:** If you previously applied **legacy** `pods/pod-cassandra*.yaml`, delete those Pods so they do not share the `cassandra` Service with the **StatefulSet** (`kubectl delete pod cassandra cassandra2 cassandra3 -n demo-hub --ignore-not-found`). Generated Services select `demo-hub.io/cassandra-workload: statefulset` so only the ring pods receive traffic.

**Replication & sharding (same steps as Compose):** Deployments/StatefulSets only **start processes**. To run the same init as Docker Compose (Postgres logical + physical slots, Cassandra `demo_hub` RF=3, Mongo config/shard RS + `addShard` + sharded collections), apply Jobs **after** pods are ready:

```bash
chmod +x deploy/k8s/scripts/apply-data-bootstrap.sh
./deploy/k8s/scripts/apply-data-bootstrap.sh
```

Or apply `generated/45-postgres-bootstrap-job.yaml`, `35-cassandra-schema-job.yaml`, `61-mongo-bootstrap-job.yaml`, and (after **Kafka Connect** is Ready) `62-mssql.yaml` for the **mssql-demo-bootstrap** Job — or use **`apply-data-bootstrap.sh`**, which waits for Connect before MSSQL connector registration. To re-run a Job after failure, `kubectl delete job -n demo-hub <job-name>` then apply again (Postgres SQL is mostly idempotent; `CREATE USER` / `CREATE PUBLICATION` may need a fresh DB or hand-edited SQL on repeat).

**Images — custom builds (not on Docker Hub):** Several workloads use **`mcac-demo/*`** and **`demo-hub/nodetool-exporter`** — the cluster cannot pull them until you **build locally** (same sources as Docker Compose). Use the **one-shot** script from `dashboards/demo`:

```bash
chmod +x deploy/k8s/scripts/build-all-custom-images.sh
./deploy/k8s/scripts/build-all-custom-images.sh
```

That builds: **`mcac-demo/mcac-init:local`** (Cassandra MCAC initContainer, **`imagePullPolicy: Never`**), **`mcac-demo/hub-demo-ui:latest`**, **`mcac-demo/kafka-connect:2.7.3-mongo-sink`**, **`mcac-demo/mssql-tools:22.04`**, **`demo-hub/nodetool-exporter:latest`**, **`mcac-demo/postgresql-repmgr:16.6.0`** (Bitnami PostgreSQL 16 + **repmgr** CLI/extension — see [`../docker/postgres-kafka/Dockerfile.repmgr`](../docker/postgres-kafka/Dockerfile.repmgr)). Then restart failing Deployments / StatefulSet, e.g. `kubectl rollout restart deployment/kafka-connect deployment/hub-demo-ui deployment/nodetool-exporter deployment/mssql-publisher deployment/mssql-subscriber deployment/mssql-exporter-publisher deployment/mssql-exporter-subscriber statefulset/cassandra -n demo-hub` (and **`deployment/postgresql-primary`** / replicas if Postgres image was missing). **kind / minikube:** load each image after build (`kind load docker-image …` — see script footer). **OrbStack / Docker Desktop:** the Kubernetes node usually shares the local Docker image store after `docker build`.

**Prometheus** uses the public image **`prom/prometheus:v2.17.1`** — **CrashLoopBackOff** is not an image pull issue; see [Troubleshooting](#troubleshooting) (`kubectl logs deploy/prometheus -n demo-hub`).

**PostgreSQL (Bitnami + repmgr):** Generated manifests use **`mcac-demo/postgresql-repmgr:16.6.0`** — a local build from **`docker.io/bitnamilegacy/postgresql:16.6.0-debian-12-r2`** with **repmgr** (see `POSTGRESQL_IMAGE` in `gen_demo_hub_k8s.py` and `Dockerfile.repmgr`). It is **not** on Docker Hub; run **`build-all-custom-images.sh`** or `docker compose build postgresql-primary` from `dashboards/demo`, then **`kind load docker-image mcac-demo/postgresql-repmgr:16.6.0`** (or push to your registry and retag). **`postgres-demo-bootstrap`** creates the **`repmgr`** database and extension on the primary.

**Nodetool-exporter:** Compose now tags the build as **`demo-hub/nodetool-exporter:latest`** (see `nodetool-exporter` in [`../docker-compose.yml`](../docker-compose.yml)). After `kubectl apply`, if the pod is **ImagePullBackOff**, build and load:

```bash
chmod +x deploy/k8s/scripts/build-load-nodetool-exporter.sh
./deploy/k8s/scripts/build-load-nodetool-exporter.sh
kubectl rollout restart deploy/nodetool-exporter -n demo-hub
```

Or: `docker compose build nodetool-exporter` from `dashboards/demo`, then `kind load docker-image demo-hub/nodetool-exporter:latest`.

**Mongo / Postgres parity:** Compose runs **init scripts** (replica sets, shards, Debezium SQL). The generated manifests **start the processes only** — add **Jobs** with repo scripts (see `README-jobs.md` and [`../docker/mongo-sharded/`](../docker/mongo-sharded/), [`../docker/postgres-kafka/`](../docker/postgres-kafka/)).

**Storage:** Most Deployments still use **`emptyDir`** for portability. **Cassandra** data is **persistent** via **`volumeClaimTemplates`** in **`30-cassandra-ring.yaml`** (see [New / extended resources](#new--extended-resources-what-was-added-vs-earlier-manifests)). Other databases on K8s remain ephemeral unless you extend the generator with PVCs.

**Grafana — imported “OpenSearch Prometheus” dashboards (e.g. [ID 15178](https://grafana.com/grafana/dashboards/15178)) show “No data”:** Panels filter on Prometheus **`job`** and often **`cluster`**. In this repo, [`dashboards/prometheus/prometheus.yaml`](../../prometheus/prometheus.yaml) uses **`job_name: "opensearch_demo"`** (targets **`opensearch-exporter:9114`**). Many community dashboards default the **`job`** variable to **`open_search_demo`** (extra underscore) — **that does not match** and every panel is empty. **Fix:** open the dashboard **Variables** (gear → **Variables**), set **`job`** default / options to **`opensearch_demo`**, or pick **`opensearch_demo`** from the top dropdown. For **`cluster`**, generated OpenSearch config sets **`cluster.name: demo-hub`** — use **`demo-hub`** if the variable lists it (it should match exporter metrics). **Verify in Prometheus** (port-forward **9090**): **Status → Targets** → **`opensearch_demo`** should be **UP**; **Graph** → `up{job="opensearch_demo"}` → **1**. If **DOWN**, check **`kubectl logs -n demo-hub deploy/opensearch-exporter`** and that OpenSearch is reachable at **`http://opensearch:9200`** from the exporter pod.

---

## Legacy Pod skeletons (`pods/`, `services/`)

**`scripts/gen_demo_hub_pods.py`** emits one **Pod** + **Service** per Compose service — useful as a port/image checklist, not a complete port of Compose.

```bash
python3 scripts/gen_demo_hub_pods.py
```

**`scripts/gen_demo_hub_deployments.py`** delegates to **`gen_demo_hub_k8s.py`**.

---

## Troubleshooting

| Symptom | Typical cause |
|--------|----------------|
| **`demo-hub.sh` / `apply-data-bootstrap.sh` stuck** on “Waiting for PostgreSQL primary…” | `kubectl rollout status` waits until the **primary pod is Ready**. If the pod is **Pending** (disk-pressure, insufficient CPU/RAM) or **CrashLoop** / **ImagePullBackOff**, this blocks until timeout (~7 min). Run: `kubectl get pods -n demo-hub -l app.kubernetes.io/name=postgresql-primary`; `kubectl describe pod -n demo-hub -l app.kubernetes.io/name=postgresql-primary` and read **Events**. Fix node disk/resources or image issues first. |
| **PostgreSQL `ErrImagePull` on `mcac-demo/postgresql-repmgr:16.6.0`** | Build the image (`./deploy/k8s/scripts/build-all-custom-images.sh` step 6/6, or `docker build -f deploy/docker/postgres-kafka/Dockerfile.repmgr …`), then load into the cluster (`kind load docker-image mcac-demo/postgresql-repmgr:16.6.0`) or push to a registry the nodes can pull. Restart **`postgresql-primary`** and replicas. |
| **`mssql-demo-bootstrap` ImagePullBackOff** / **ErrImageNeverPull** (`mcac-demo/mssql-tools:22.04`) | This tag is **not** on a public registry. Build [`scripts/build-mssql-tools-image.sh`](scripts/build-mssql-tools-image.sh) (or **`build-all-custom-images.sh`**) with the **same Docker/OrbStack** your cluster uses. **OrbStack** / Docker Desktop K8s normally see locally built images; **kind** needs `kind load docker-image mcac-demo/mssql-tools:22.04`. The Job uses **`imagePullPolicy: IfNotPresent`** (use local image when it exists). **`ErrImageNeverPull`** on older manifests meant **`imagePullPolicy: Never`** and the image was missing locally — build the image, then `kubectl delete job mssql-demo-bootstrap -n demo-hub` → `kubectl apply -f generated/62-mssql.yaml`. |
| **Publisher has no `demo` DB** (verify script lists only master/model/msdb/tempdb) | Schema init never ran or the bootstrap Job failed early. Run [`scripts/apply-mssql-schema-k8s.sh`](scripts/apply-mssql-schema-k8s.sh) from **`dashboards/demo`**, then [`scripts/verify-mssql-publisher-demo.sh`](scripts/verify-mssql-publisher-demo.sh) — **`demo`** should appear. Re-register MSSQL connectors (`register-mssql-connectors.sh` with **`DEMO_HUB_K8S=1`** and port-forward). |
| **SQL Server pods CrashLoop / wrong platform** | Image is **`linux/amd64`**. On **Apple Silicon** clusters without emulation, schedule on **amd64** nodes or use a cluster that runs x86_64 SQL Server. **`mssql-publisher` / `mssql-subscriber`** need ~**2–4Gi** memory per [`62-mssql.yaml`](generated/62-mssql.yaml) limits. |
| **ErrImagePull** / **ImagePullBackOff** (nodetool-exporter) | Not on a public registry — build and load: **`./deploy/k8s/scripts/build-load-nodetool-exporter.sh`** (or `docker compose build nodetool-exporter` then `kind load docker-image demo-hub/nodetool-exporter:latest`). |
| **Kafka / kafka-exporter CrashLoop** | Stack uses **Bitnami Legacy** Kafka + ZooKeeper (`docker.io/bitnamilegacy/kafka`, `bitnamilegacy/zookeeper`) — same **`kafka:9092`** bootstrap as before. Re-apply `generated/20-zookeeper-kafka.yaml`, then **`kubectl delete pod -n demo-hub -l app.kubernetes.io/name=kafka`** (stuck rollouts can leave two Kafka ReplicaSets). Ensure nodes can **pull** `docker.io/bitnamilegacy/*`. |
| **hub-demo-ui ErrImagePull** / **ImagePullBackOff** | Image **`mcac-demo/hub-demo-ui:latest`** is local-only — run **`./deploy/k8s/scripts/build-all-custom-images.sh`** (or build that Dockerfile under `deploy/docker/realtime-orders-search-hub/demo-ui`), then `kubectl rollout restart deploy/hub-demo-ui -n demo-hub`. |
| **hub-demo-ui CrashLoop** (after image pulls OK) | The app **connects to Postgres, Cassandra, Redis, Mongo (mongos), Kafka, and OpenSearch** during FastAPI startup (`lifespan`). The **initContainer** waits for all of those TCP ports before the main container runs. If you still see **BackOff**, confirm dependencies are **Ready** (`kubectl get pods -n demo-hub`) and check **`kubectl logs deploy/hub-demo-ui -n demo-hub -c hub-demo-ui --previous`**. Regenerate/apply **`95-hub-demo-ui.yaml`** if your manifest only waited for Kafka + OpenSearch. |
| **kafka-connect ImagePullBackOff** | Image **`mcac-demo/kafka-connect:2.7.3-mongo-sink`** is built from `deploy/docker/mongo-kafka/Dockerfile.connect` — use **`build-all-custom-images.sh`**, then restart `deploy/kafka-connect`. |
| **Prometheus CrashLoopBackOff** | Check logs: `kubectl logs deploy/prometheus -n demo-hub --tail=80`. If you see **`scrape timeout greater than scrape interval`** for job **`mcac`**, regenerate (`python3 deploy/k8s/scripts/gen_demo_hub_k8s.py`) and re-apply **`10-observability-prometheus-grafana.yaml`** — the generator keeps **`scrape_timeout` ≤ `scrape_interval`** for that job. Other causes: YAML errors, OOM. |
| **Grafana Postgres dashboard — replication slots / lag show 0** | Panels need **`release`** + **`kubernetes_namespace`** (Grafana variables) and metrics from the **primary**. [`prometheus.yaml`](../../prometheus/prometheus.yaml) adds **`release: demo-hub`** and **`kubernetes_namespace: demo-hub`** on **`postgres_pgdemo`** scrapes. **`postgres-exporter-primary`** uses DB user **`postgres`** so **physical** replication slots are visible (role **`demo`** cannot see physical slots in `pg_replication_slots`). Re-apply **`generated/80-exporters.yaml`** + **`10-observability-prometheus-grafana.yaml`**, restart **`deploy/postgres-exporter-primary`** and **`deploy/prometheus`**, pick **Instance** = **`postgres-exporter-primary:9187`** and **Release** = **`demo-hub`**. |
| **Grafana dashboard 15178 / OpenSearch — all “No data”** | **`job`** must be **`opensearch_demo`** (see [`prometheus.yaml`](../../prometheus/prometheus.yaml)), not **`open_search_demo`**. Edit dashboard variables. Confirm **`up{job="opensearch_demo"}==1`** in Prometheus. |
| **stress RunContainerError** | **tlp-stress** must use the image **ENTRYPOINT** — generator uses `args` only (not `command`). Re-apply `98-nodetool-stress.yaml`. |
| **OOMKilled** | Raise limits or node RAM (see kafka-connect `limits.memory`). |
| **Mongo / PG not usable** | Init Jobs not run — replica sets and SQL not applied. |
| **OpenSearch** | ConfigMap supplies single-node `opensearch.yml`; tune for your cluster. |
| **Prometheus `mcac` targets** show **Compose** names (`cassandra`, `cassandra2`, …) **DOWN** | **Stale** `prometheus-tg-mcac` ConfigMap. Regenerate, apply observability YAML, **restart Prometheus**: `python3 deploy/k8s/scripts/gen_demo_hub_k8s.py` → `kubectl apply -f deploy/k8s/generated/10-observability-prometheus-grafana.yaml` → `kubectl rollout restart deployment/prometheus -n demo-hub`. Verify: `kubectl get configmap prometheus-tg-mcac -n demo-hub -o jsonpath='{.data.tg_mcac\.json}'` — must list `cassandra-0.cassandra-headless.demo-hub.svc.cluster.local:9103` (three StatefulSet pods). |
| **Prometheus `mcac` targets** show **correct** FQDNs but **timeout** / **connection refused** on :9103 | Cassandra pods need the **MCAC javaagent** (initContainer + `/mcac` mounts). Use **current** `30-cassandra-ring.yaml` (includes `mcac-copy-agent` init + `mcac-agent-config` ConfigMap), build **`mcac-demo/mcac-init:local`** (`./deploy/k8s/scripts/build-mcac-init-image.sh`), then `kubectl rollout restart statefulset/cassandra -n demo-hub`. Check: `kubectl exec -n demo-hub cassandra-0 -- curl -sS --max-time 2 http://127.0.0.1:9103/metrics \| head` should return Prometheus text. |
| **Cassandra `ErrImagePull` / “access denied” for `mcac-demo/mcac-init`** | That name is **not** on Docker Hub. Build the image locally (**`build-mcac-init-image.sh`**), ensure the tag is **`mcac-demo/mcac-init:local`**, and use the generated manifest (**`imagePullPolicy: Never`** on the initContainer). kind/minikube: load the image into the cluster. |

```bash
kubectl -n demo-hub get deploy,sts,svc
kubectl -n demo-hub describe pod -l app.kubernetes.io/part-of=demo-hub
```
