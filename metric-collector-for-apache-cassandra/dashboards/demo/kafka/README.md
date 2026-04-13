# Kafka & ZooKeeper (shared broker)

This folder documents the **Apache Kafka** and **Apache ZooKeeper** (and how **Kafka Connect** attaches to the broker) in **`../docker-compose.yml`**. All definitions stay in that single compose file.

## Services

| Compose service | Role |
|-----------------|------|
| **`zookeeper`** | Coordinates Kafka (demo uses Confluent images). |
| **`kafka`** | Single broker for the demo; inside Docker clients use **`kafka:29092`** (see service `KAFKA_ADVERTISED_LISTENERS`). |
| **`kafka-connect`** | **Debezium 2.7** + **MongoDB Kafka sink** JAR; REST **`8083`**; build context **`../mongo-kafka/`** (`Dockerfile.connect`). |

Host port **9092** is the usual PLAINTEXT listener for tools on your machine.

**Kubernetes:** ZooKeeper + Kafka + Connect are generated as **`../k8s/generated/20-zookeeper-kafka.yaml`**, **`70-kafka-connect.yaml`** (in-cluster bootstrap **`kafka:9092`**). Ops extras (Ingress, etc.): **[`../k8s/README.md`](../k8s/README.md)**.

## Who uses this broker

- **PostgreSQL CDC** — Debezium Postgres source + JDBC sink: **[`../postgres-kafka/README.md`](../postgres-kafka/README.md)** (`register-connectors.sh`).
- **MongoDB CDC** — Debezium Mongo source + Mongo sink: **[`../mongo-kafka/README.md`](../mongo-kafka/README.md)** (`register-mongo-connectors.sh`).
- **Kafka metrics** — **`kafka-exporter-mcac`** (e.g. port **9308**), scraped by Prometheus (job **`kafka_pgdemo`** in **`../../prometheus/prometheus.yaml`**). Grafana: **Kafka cluster (demo broker)** — **`../../grafana/generated-dashboards/kafka-cluster-overview.json`**, UID **`demo-kafka-cluster`**.

## Typical commands

From **`dashboards/demo`**:

```bash
docker compose up -d zookeeper kafka
# Kafka Connect (after image build — see mongo-kafka README)
docker compose build kafka-connect
docker compose up -d kafka-connect
```

List topics (example with a Kafka tools image):

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## Further reading

- Connector registration and topics: **[`../postgres-kafka/README.md`](../postgres-kafka/README.md)**, **[`../mongo-kafka/README.md`](../mongo-kafka/README.md)**.
- Observability: **[`../observability/README.md`](../observability/README.md)**.
