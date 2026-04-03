#!/usr/bin/env bash
# Enable sharding on DB "demo", shard source + sink collections, insert seed docs (via mongos).
set -euo pipefail

MONGOS_URI="${MONGOS_URI:-mongodb://mongo-mongos1:27017}"

wait_mongos() {
  local n=0
  until mongosh "${MONGOS_URI}" --eval 'db.runCommand({ ping: 1 })' --quiet >/dev/null 2>&1; do
    n=$((n + 1))
    if [ "$n" -gt 90 ]; then
      echo "timeout waiting for mongos" >&2
      exit 1
    fi
    sleep 2
  done
}

echo "waiting for mongos..."
wait_mongos

mongosh "${MONGOS_URI}" --quiet <<'EOS'
// Idempotent-ish: enable sharding on demo DB
try {
  sh.enableSharding("demo");
} catch (e) {
  print("enableSharding: " + e);
}

const d = db.getSiblingDB("demo");
if (!d.getCollectionNames().includes("demo_items")) {
  d.createCollection("demo_items");
}
if (!d.getCollectionNames().includes("demo_items_from_kafka")) {
  d.createCollection("demo_items_from_kafka");
}

try {
  sh.shardCollection("demo.demo_items", { _id: "hashed" });
} catch (e) {
  if (!String(e.message).includes("already sharded")) { throw e; }
}
try {
  sh.shardCollection("demo.demo_items_from_kafka", { _id: "hashed" });
} catch (e) {
  if (!String(e.message).includes("already sharded")) { throw e; }
}

const n = d.demo_items.countDocuments({});
if (n === 0) {
  d.demo_items.insertMany([
    { name: "mongo-seed-a", qty: 1 },
    { name: "mongo-seed-b", qty: 2 }
  ]);
  print("inserted seed documents");
} else {
  print("demo_items already has " + n + " documents, skipping seeds");
}
print("demo collections ready for Debezium + Mongo sink");
EOS
