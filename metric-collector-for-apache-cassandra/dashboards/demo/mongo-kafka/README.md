# MongoDB (sharded) + Kafka + Debezium

Part of **`dashboards/demo`**: services are defined in **`../docker-compose.yml`**. For cluster bring-up scripts, see **[`../mongo-sharded/README.md`](../mongo-sharded/README.md)**. For diagrams and a full workflow narrative, see the Mongo section in **[`../postgres-kafka/README.md`](../postgres-kafka/README.md)**.

Mirrors the Postgres demo: **Debezium MongoDB source** (CDC via `mongos`) → Kafka → **MongoDB Kafka sink** into a separate collection (no CDC loop).

## What runs in compose

- **`mongo-kafka-prepare`** (one-shot): enables `sh.enableSharding("demo")`, shards `demo.demo_items` and `demo.demo_items_from_kafka`, inserts two seed documents on `demo.demo_items`.
- **`kafka-connect`**: custom image `mcac-demo/kafka-connect:2.7.3-mongo-sink` = Debezium `connect:2.7.3.Final` + `mongo-kafka-connect-1.14.1-all.jar` from Maven Central.

## Flow

| Connector | Role |
|-----------|------|
| `mongo-source-demo` | `MongoDbConnector`, `mongodb://mongo-mongos1:27017`, topics `demomongo.demo.demo_items` |
| `mongo-sink-demo` | `MongoSinkConnector`, reads that topic, writes `demo.demo_items_from_kafka` |

The sink uses **`io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`** (not `ExtractNewRecordState`) so flattened documents work with the Mongo sink.

## Commands

From `dashboards/demo` (repository path), with Mongo + Kafka + Postgres stack up:

```bash
docker compose build kafka-connect
docker compose up -d mongo-kafka-prepare kafka-connect
chmod +x mongo-kafka/register-mongo-connectors.sh
./mongo-kafka/register-mongo-connectors.sh
```

Host **mongos** for manual tests: `localhost:27025` (see main `docker-compose.yml`).

Verify:

```bash
docker compose exec mongo-mongos1 mongosh demo --eval 'db.demo_items.find(); db.demo_items_from_kafka.find()'
```
