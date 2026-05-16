# Oracle Database (demo hub)

**Image:** [`gvenzl/oracle-free:23-full-faststart`](https://hub.docker.com/r/gvenzl/oracle-free) (Oracle 23c Free, pre-expanded PDB). Requires **`/dev/shm` ≥ 2Gi** (set in generated manifests).

**Do not** mount `emptyDir` on `/opt/oracle/oradata` for faststart images — it hides the pre-built database.

## Credentials (K8s Secret `demo-hub-credentials`)

| Key | Default | Use |
|-----|---------|-----|
| `oracle-sys-password` | `Demo_hub_Oracle_2025!` | `ORACLE_PASSWORD` (SYS) |
| `oracle-demo-password` | `demopass` | Application user `demo` |

## Connect

In-cluster:

```bash
sqlplus demo/demopass@//oracle.demo-hub.svc.cluster.local:1521/FREEPDB1
```

After port-forward (`LOCAL_ORACLE_PORT`, default **16330**):

```bash
sqlplus demo/demopass@//127.0.0.1:16330/FREEPDB1
```

From **demo-tools** pod: `kubectl exec -it -n demo-hub deploy/demo-tools -- bash -l` (env `DEMO_HUB_ORACLE` is preset).

## Schema

| Script | Purpose |
|--------|---------|
| `01-demo-schema.sql` | Rich demo objects (tables, packages, procedures, MVs, …) |
| `02-hub-scenario-schema.sql` | `scenario_catalog_mirror_oracle`, `hub_workload_oracle` |
| `03-exporter-grants.sql` | `SELECT_CATALOG_ROLE` for **demo** (applied as **SYS**, not demo) |

Bootstrap Job: **`oracle-demo-bootstrap`** (ConfigMap `oracle-scripts`).

### K8s env gotcha

Pods in namespace `demo-hub` get **`ORACLE_PORT=tcp://<cluster-ip>:1521`** from the Service named `oracle`. Bootstrap and apps must use **`ORACLE_LISTEN_PORT=1521`** (or hardcode `1521`), not `ORACLE_PORT`, for the SQL*Net port.

## Hub UI

When `ORACLE_HOST` and `ORACLE_PASSWORD` are set on **hub-demo-ui**:

- Scenario step 2 merges into `scenario_catalog_mirror_oracle` → response field **`oracle_rows_upserted`**
- Workload target **oracle** writes `hub_workload_oracle`
- Data view: `/scenario/data/oracle`

If **`oracle_rows_upserted`** is 0 but connect works, re-run bootstrap:

```bash
kubectl delete job -n demo-hub oracle-demo-bootstrap --ignore-not-found
kubectl apply -f deploy/k8s/generated/63-oracle.yaml
kubectl wait -n demo-hub --for=condition=complete job/oracle-demo-bootstrap --timeout=20m
```

## Metrics

- Prometheus job `oracle_demo` → `oracle-exporter:9161`
- Grafana dashboard **Oracle (demo hub)** (`uid: demo-oracle`)
