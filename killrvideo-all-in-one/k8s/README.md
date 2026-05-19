# KillrVideo on Kubernetes

Kustomize manifests mirroring `docker-compose-all-in-one.yml` (no Kafka by default).

## Requirements

- Kubernetes 1.25+ (tested on OrbStack / Docker Desktop)
- **~12 GB RAM** free for the cluster VM (DSE with Search + Graph is heavy)
- `kubectl` configured

## Deploy

```bash
./scripts/k8s-up.sh
```

If DSE keeps crash-looping or ports fail, reset DSE data and redeploy:

```bash
./scripts/k8s-reset-dse.sh
```

Or:

```bash
kubectl apply -k k8s/
kubectl wait --for=condition=complete job/dse-config -n killrvideo --timeout=900s
kubectl rollout status deployment/dse -n killrvideo --timeout=900s
kubectl rollout status deployment/backend -n killrvideo --timeout=900s
```

## Access

Use the same ports as Docker Compose (`3000`, `9091`):

```bash
./scripts/k8s-port-forward.sh
```

| Service | URL |
|---------|-----|
| Web | http://localhost:3000 |
| Studio | http://localhost:9091 |
| Backend gRPC | — | `kubectl port-forward -n killrvideo svc/backend 50101:50101` |

## Stop and start

| Action | Command |
|--------|---------|
| Start | `./scripts/k8s-up.sh` |
| Stop | `./scripts/k8s-down.sh` |
| Port-forward UIs | `./scripts/k8s-port-forward.sh` |
| Reset DSE data | `./scripts/k8s-reset-dse.sh` |

## Startup order

1. DSE
2. Job `dse-config` (schema bootstrap)
3. Backend (waits for `killrvideo` keyspace)
4. Web, Studio, Generator

## Troubleshooting

### `Collectd start failed` in DSE logs

Harmless in Docker/Kubernetes: DSE Insights tries to start **collectd** for internal metrics and it often cannot run in a container. KillrVideo does not need it. Manifests set `-Dinsights.default_mode=disabled` to reduce log noise. This is **not** why Web/Studio fail to load.

### DSE `OOMKilled` or `CrashLoopBackOff`

DSE auto-tunes from **host** RAM (OrbStack VM size), not the pod limit. Manifests set `JVM_EXTRA_OPTS` to cap heap (~4G) inside a **14Gi** pod.

If DSE still OOMs, raise the pod limit in `dse-deployment.yaml` or give the cluster VM more RAM, then:

```bash
kubectl rollout restart deployment/dse -n killrvideo
```

### DSE not reachable on port 9042 (everything stuck in Init)

Old data on the PVC can keep `listen_address: localhost`. Reset once after upgrading manifests:

```bash
kubectl delete pvc dse-data -n killrvideo
kubectl apply -k k8s/
kubectl rollout restart deployment/dse -n killrvideo
```

### Kafka not starting

Check logs: `kubectl logs -n killrvideo -l app=kafka`

Ensure Zookeeper is ready: `kubectl get pods -n killrvideo -l app=zookeeper`

## Teardown

```bash
./scripts/k8s-down.sh
```

PVC `dse-data` is removed with the namespace only if you delete the namespace; `k8s-down.sh` keeps data unless you delete the PVC manually.
