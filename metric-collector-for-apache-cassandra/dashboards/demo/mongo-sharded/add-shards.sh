#!/usr/bin/env bash
# Register shard replica sets tic / tac / toe on the cluster (via mongos). Idempotent.
set -euo pipefail

wait_mongos() {
  local n=0
  until mongosh "mongodb://mongo-mongos1:27017" --eval 'db.runCommand({ ping: 1 })' --quiet >/dev/null 2>&1; do
    n=$((n + 1))
    if [ "$n" -gt 90 ]; then
      echo "timeout waiting for mongo-mongos1"
      exit 1
    fi
    sleep 2
  done
}

echo "waiting for mongos..."
wait_mongos

mongosh "mongodb://mongo-mongos1:27017" --quiet --eval '
const admin = db.getSiblingDB("admin");
function hasShard(id) {
  const r = admin.runCommand({ listShards: 1 });
  if (!r.shards) return false;
  return r.shards.some(s => s._id === id);
}
const shards = [
  { id: "tic", conn: "mongo-shard-tic:27017" },
  { id: "tac", conn: "mongo-shard-tac:27017" },
  { id: "toe", conn: "mongo-shard-toe:27017" }
];
for (const s of shards) {
  if (!hasShard(s.id)) {
    print("adding shard " + s.id);
    printjson(admin.runCommand({ addShard: s.id + "/" + s.conn }));
  } else {
    print("shard " + s.id + " already present");
  }
}
print("done");
'
