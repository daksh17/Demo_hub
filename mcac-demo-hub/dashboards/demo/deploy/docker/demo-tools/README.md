# demo-tools (client toolbox pod)

Ubuntu image with **psql**, **mongosh**, **cqlsh**, **redis-cli**, **curl**, **jq**, **opensearch-cli**, and **ora2pg**.

## Build

```bash
docker build -t demo-hub/demo-tools:latest -f deploy/docker/demo-tools/Dockerfile deploy/docker/demo-tools
```

Or: `./deploy/k8s/scripts/build-demo-tools-image.sh`

## K8s

Deployment **`demo-tools`** in namespace `demo-hub` — long-running pod for `kubectl exec`:

```bash
kubectl exec -it -n demo-hub deploy/demo-tools -- bash -l
# then: demo-psql, demo-mongosh, demo-cqlsh, ora2pg --help, opensearch-cli, demo-os-health
```

In-cluster connection hints are in `/etc/profile.d/demo-hub-tools.sh`.

**Note:** This image does not include `sqlplus` (Oracle client is large). Use the **oracle** pod or port-forward + host client for SQL*Plus; **ora2pg** talks to Oracle over the network using the connection string in `DEMO_HUB_ORACLE`.
