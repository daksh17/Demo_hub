# Demo Hub UI (`demo-ui`)

FastAPI service **`hub-demo-ui`** (port **8888**): single-order ingest, workload generator, multi-DB **scenario**, Trino, Postgres logical replication demos, and the **Kafka lab**.

Broader hub narrative: [`README.md`](../README.md) in this folder, [`scenario-flow/README.md`](../scenario-flow/README.md), and [`../../../docs/hub-and-data-flow.md`](../../../docs/hub-and-data-flow.md).

---

## Kafka lab

**Browser:** **`/kafka`** (Compose: **http://localhost:8888/kafka**; Kubernetes: port-forward **`svc/hub-demo-ui`** then same path).

**Code:** [`kafka_lab.py`](kafka_lab.py) (broker calls via **kafka-python**), wired in [`app.py`](app.py).

### Environment

| Variable | Role |
|----------|------|
| **`KAFKA_BOOTSTRAP`** | Comma-separated broker list. **Compose** hub typically uses **`kafka:29092`**. **Kubernetes** manifests set **`kafka:9092`** (in-cluster listener). |
| **`KAFKA_LAB_TOPIC`** | Default topic name when the UI omits one (**default:** **`demo-hub.kafka.lab`**). |

The topic must already exist **unless** the cluster has **`auto.create.topics.enable=true`** (demo stacks often do).

### HTTP API

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/kafka/lab/metadata` | Bootstrap string, approximate broker count, sorted topic list (internal topics filtered, list capped). |
| `GET` | `/api/kafka/lab/hints` | Short JSON cheat-sheet (mirrors on-page hints). |
| `POST` | `/api/kafka/lab/produce` | Produce a **burst** of JSON records with tunable producer knobs. |
| `POST` | `/api/kafka/lab/consume` | Subscribe, poll until **`timeout_ms`** or **`max_messages`**, return records (+ partition / offset / key / value). |

Produce/consume run in a thread pool (**`asyncio.to_thread`**) so request handlers stay non-blocking.

### Produce burst (what the UI exposes)

Payload shape per message: **`{ "lab": true, "seq": i, "ts": …, "pad": "<bytes>" }`** where **`pad`** repeats **`value_pad_kb`** KiB of **`x`** (stress payload size).

| Field | Meaning |
|-------|---------|
| **`topic`** | Target topic (validated: letters, digits, `.`, `_`, `-`; length 1–249). |
| **`count`** | Messages to send (1–20_000). |
| **`key_mode`** | **`none`** / **`fixed`** / **`random`** / **`per_message`** — drives partition choice and ordering experiments. |
| **`fixed_key`** | Used when **`key_mode=fixed`**. |
| **`acks`** | **`0`** / **`1`** / **`all`** (passed through to **`KafkaProducer`**; **`all`** maps to broker **`acks=all`** semantics). |
| **`linger_ms`** | Batching delay (**`linger.ms`**). |
| **`batch_size`** | **`batch.size`** (bytes). |
| **`compression`** | **`none`**, **`gzip`**, **`snappy`**, **`lz4`**, **`zstd`**. |
| **`value_pad_kb`** | Per-record value padding size (capped in API). |
| **`enable_idempotence`** | **`true`** forces **`acks=all`** and enables idempotent producer (session-scoped dedupe on broker). |

Response includes **`elapsed_sec`**, **`approx_throughput_rps`**, **`effective_acks`**, and approximate serialized value size.

### Consume poll (what the UI exposes)

| Field | Meaning |
|-------|---------|
| **`topic`** | Topic to subscribe to. |
| **`group_id`** | Empty → random ephemeral group each request (**`demo-hub-kafka-lab-<suffix>`**). |
| **`max_messages`** | Stop after this many records (1–500). |
| **`timeout_ms`** | Wall-clock budget for assignment + polling (**500–600_000** ms). |
| **`auto_offset_reset`** | **`earliest`** or **`latest`** when no committed offset exists for the group. |
| **`enable_auto_commit`** | **`false`** (default): no offset commit. **`true`**: enables broker auto-commit and a synchronous **`commit()`** before **`close()`**; reuse the same **`group_id`** so the next poll resumes after committed offsets (**`auto_offset_reset`** applies only when there is no committed position). |
| **`parallel_consumers`** | **`1`**–**`3`**. Runs one **`consume_poll`** per thread inside **hub-demo-ui**. **`1`** returns the original flat JSON; **`2`** or **`3`** returns **`consumers`**, **`group_ids_used`**, **`topics_per_consumer`**, **`total_messages_across_consumers`**, etc. |
| **`topic_consumer_2`** | Optional (parallel ≥ **2**). Topic for parallel instance **1** (0-based: instance **0** uses **`topic`**). Blank → same as **`topic`**. |
| **`topic_consumer_3`** | Optional (parallel ≥ **3**). Topic for parallel instance **2**. Blank → same as **`topic`**. |
| **`share_consumer_group`** | **`true`**: all parallel consumers share **one** **`group.id`** (your **`group_id`** or one shared random id) → partitions split across members when the topic has enough partitions. **`false`**: separate **`group.id`** values (**`<your>-inst0`**, **`inst1`**, … when **`group_id`** set; otherwise unrelated random groups — independent reads). |

Implementation notes:

- With **`enable_auto_commit`** off and random **`group_id`**, offsets are **not** stored for the next HTTP poll — compare with **on** + stable **`group_id`** to study resume semantics.
- **`max_messages`** is enforced **per** parallel consumer (possible total up to **`parallel × max_messages`**).
- Partition splitting only matters when the topic has **multiple partitions**; a single-partition topic + shared group leaves extra idle-looking instances until producers spread keys.
- Successful **`consume_poll`** responses include **`assigned_partitions`** (partition IDs assigned after subscribe, before polling) and **`partitions_seen_in_messages`** (unique partitions present in the returned **`messages`** list). Use **`assigned_partitions`** in the UI to see which member owns which partitions when **`share_consumer_group`** is on.
- Values are deserialized as **JSON**; producing from this lab yields JSON-compatible bodies. Binary or non-JSON topics may error on consume.

Use returned **`partition`** / **`offset`** / **`key`** with **`key_mode`** experiments (e.g. fixed key → records stick to one partition → strict ordering per key).

### Continuous until Stop (UI only)

On **`/kafka`**, **Continuous until Stop** runs repeated **`POST /api/kafka/lab/consume`** calls in the browser until **Stop streaming**. Use **`enable_auto_commit`** and a stable **`group_id`** (or an auto-filled **`demo-hub-stream-…`** when the field is empty) so each round can advance offsets. Stop applies after the in-flight request completes. With **parallel consumers**, the page keeps a streaming summary in the top JSON panel and refreshes **one JSON panel per instance** below (each shows **`assigned_partitions`** when the poll succeeds).

### Rebuild / deploy

Image build copies **`kafka_lab.py`** with **`app.py`** — see [`Dockerfile`](Dockerfile). After code changes:

```bash
# from dashboards/demo
./deploy/k8s/scripts/redeploy-hub-demo-ui.sh
```

---

## Deep knowledge map (lab + next steps)

Use the **Kafka lab** for quick knobs and observability in the browser; use **CLI tools**, **Grafana**, and **chaos** for scenarios the UI does not automate.

### Core scenarios (build deliberately)

| Scenario | What you learn |
|----------|----------------|
| Happy path: one topic, **N** partitions, one consumer group scaling to **N** consumers | Partition assignment, throughput vs partition count |
| Keyed vs round-robin producers | Ordering **per partition** vs spreading load |
| Multiple consumer groups on the same topic | Independent offsets; duplicate reads across groups |
| Slow consumer (**`max.poll.interval.ms`**, long processing) | Rebalances; poll loop must finish in time |
| Poison message / crash mid-batch | At-least-once duplicates; idempotent handlers vs EOS |
| Broker / leader bounce | Leadership election; **`min.insync.replicas`**; producer timeouts |
| Broker disk full / replica lagging | ISR shrink; under-replicated partitions |
| Compaction topic | Tombstones; retention of latest key; consumer semantics |

**Map to this repo:** the lab helps with **keys**, **batching**, **compression**, **acks**, **idempotence**, optional **committed offsets** (**`enable_auto_commit`** + fixed **`group_id`**), and **multi-group** mental models. It does **not** configure **`max.poll.interval.ms`**, transactions, or broker topic modes; add **`kafka-configs`**, Strimzi **`KafkaTopic`**, or shell scripts for those.

### Consumer angles to exercise

| Topic | How to practice |
|-------|-----------------|
| At-least-once: commit **after** processing | Duplicates on retry — use app-side counters; turn **`enable_auto_commit`** off to replay without advancing commits. |
| Commit strategies | With **`enable_auto_commit`** on and a fixed **`group_id`**, the lab commits once per poll before **`close()`**; tune **`auto_commit_interval_ms`** (fixed 5000 ms in code) vs manual commit in real apps. |
| Rebalancing | Static vs dynamic membership; scale consumers up/down and watch broker/coordinator logs. |
| Lag | **`kafka-consumer-groups.sh --describe`** under load; relate spikes to **`fetch.max.wait.ms`**, processing time, GC. |
| Isolation | **`read_committed`** vs **`read_uncommitted`** when using transactions — not exposed in lab. |
| Assign vs subscribe | Manual assignment for specialized consumers — compare to lab’s **`subscribe`**. |

### Producer angles (properties that change behaviour)

| Property | Story |
|----------|--------|
| **`acks`**: 0 / 1 / all | Latency vs durability (lab: **`acks`** + **`enable_idempotence`**). |
| **`linger.ms`** + **`batch.size`** | Throughput vs latency (lab: **`linger_ms`**, **`batch_size`**). |
| **`compression.type`** | CPU vs network (**gzip** / **lz4** / **zstd** / **snappy**) — lab exposes **`compression`**. |
| **`enable.idempotence=true`** | Dedupe per producer session; pairs with **`acks=all`** — lab checkbox. |
| Transactional producer | Multi-partition atomic writes + **`read_committed`** consumers — out of scope for the lab page; use Java client or scripted examples. |

### Broker / topic tuning (lab-only toggles)

Worth toggling in a **non-production** cluster:

- **`replication.factor`**, **`min.insync.replicas`**, **`unclean.leader.election.enable`** (dangerous; demo only).
- **`retention.ms`** / **`retention.bytes`** vs **compaction** (`cleanup.policy`).
- **`segment.bytes`**, **`flush.messages`** (observe latency and disk behaviour indirectly).
- **ACLs / SASL / TLS** when training operators — not modeled in default demo manifests.

**This stack:** plain Kafka in Compose/K8s YAML (**not Strimzi** by default). Align any Operator-specific workflows with whatever you deploy.

### Load generation and testing tools

| Tool | Use |
|------|-----|
| **`kafka-producer-perf-test`** / **`kafka-consumer-perf-test`** | Baseline throughput; sweep record size and batches. |
| **kcat** (**kafkacat**) | Quick produce/consume, headers, timestamps from shell. |
| Small loaders in your language | Retry/idempotency logic — easiest with code you own. |
| **Chaos** | Kill broker pod/process, force leader election, kill consumers — scripted failures beat random clicks. |

### What to measure every time

| Role | Signals |
|------|---------|
| Producer | p99 latency, error rate, average batch size, compression ratio |
| Broker | Under-replicated partitions, ISR size, request queue, disk, network |
| Consumer | Lag, rebalance count, poll duration, processing time per batch |

Prometheus + Grafana in this demo include Kafka-oriented dashboards (see parent **`README.md`** → Grafana dashboard list).

### Suggested curriculum order

1. Partitions + consumer groups + lag  
2. Producer batching (**linger** / **batch**) + compression  
3. Durability (**acks=all**, **`min.insync.replicas`**)  
4. Failures (broker kill, slow consumer)  
5. Keys + ordering + compaction  
6. (Optional) Transactions + **`read_committed`**

---

## Other modules (same service)

| Module | Role |
|--------|------|
| [`app.py`](app.py) | Routes, HTML pages, lifespan wiring to Postgres/Cassandra/Redis/Mongo/OpenSearch/Kafka/Trino/MSSQL. |
| [`scenario.py`](scenario.py) | Faker pipeline; emits **`scenario.*`** topics (separate from Kafka lab defaults). |
| [`hub_config.py`](hub_config.py) | Env + runtime session overrides for connection strings. |
| [`postgres_logical_demo.py`](postgres_logical_demo.py), [`postgres_faker_schema.py`](postgres_faker_schema.py), [`postgres_schema_clone.py`](postgres_schema_clone.py) | Postgres demos and DDL helpers. |

OpenAPI: **`/docs`** when the app is running.
