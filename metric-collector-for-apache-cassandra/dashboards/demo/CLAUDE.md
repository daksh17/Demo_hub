# MCAC Cassandra Dashboard Project

This project edits Grafana dashboard JSONs for the Metric Collector for Apache Cassandra (MCAC) demo stack.

## Datasource
- UID: `prometheus`
- URL: `http://prometheus:9090`
- Every panel target must use: `"datasource": { "type": "prometheus", "uid": "prometheus" }`

## Metric prefix
All Cassandra metrics use the `mcac_` prefix (MCAC agent, relabeled from JMX).
Never use `cassandra_` or `ic_` prefix.

## Key metric families
| Family | Examples | Labels |
|--------|---------|--------|
| `mcac_table_*` | reads, writes, tombstones, sstables_per_read, live_disk_space_used | keyspace, table |
| `mcac_keyspace_*` | per-keyspace aggregates | keyspace |
| `mcac_client_request_*` | read/write latency, timeouts, unavailables | request_type, cl |
| `mcac_jvm_*` | heap used/max, GC count/time | — |
| `mcac_storage_*` | load bytes, commitlog size | — |
| `mcac_compaction_*` | pending tasks, bytes compacted | — |
| `mcac_thread_pools_*` | pending, active, blocked tasks | pool_type, pool_name |
| `mcac_hints_*` | hints in progress, created | — |
| `mcac_dropped_*` | dropped messages by type | message_type |

## Labels available on all metrics
- `cluster`      — cluster name (e.g. "Test Cluster")
- `datacenter`   — data centre (e.g. "datacenter1")
- `rack`         — rack / AZ
- `host_id`      — node UUID
- `keyspace`     — keyspace name (table-scoped metrics only)
- `table`        — table name (table-scoped metrics only)
- `request_type` — read / write / range_slice (client request metrics)

## Template variables (use in PromQL)
- `$PROMETHEUS_DS` — datasource variable (already defined in dashboards)

## Panel layout rules
- 3 panels per row max: `w=8, h=8`, x positions 0 / 8 / 16
- Always set `"pluginVersion": "11.4.0"`

## Dashboard files (relative to this directory)
- `../grafana/generated-dashboards/cassandra-condensed.json`
- `../grafana/generated-dashboards/system-metrics.json`
- `../grafana/generated-dashboards/overview.json`
- `../grafana/generated-dashboards/kafka-cluster-overview.json`
- `../grafana/generated-dashboards/redis-demo-overview.json`
- `../grafana/generated-dashboards/mongodb-tictactoe-detailed.json`

## Deploying changes
Grafana auto-provisions dashboards from the mounted path — restart the Grafana container:
```bash
docker compose restart grafana
# or reload without restart:
curl -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
```
