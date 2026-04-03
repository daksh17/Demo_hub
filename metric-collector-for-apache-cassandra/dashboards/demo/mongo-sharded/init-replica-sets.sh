#!/usr/bin/env bash
# Init config server replica set (3 nodes) and shard replica sets tic / tac / toe (single node each).
set -euo pipefail

wait_mongo() {
  local host=$1
  local n=0
  until mongosh "mongodb://${host}:27017" --eval 'db.runCommand({ ping: 1 })' --quiet >/dev/null 2>&1; do
    n=$((n + 1))
    if [ "$n" -gt 120 ]; then
      echo "timeout waiting for ${host}"
      exit 1
    fi
    sleep 2
  done
}

for h in mongo-config1 mongo-config2 mongo-config3 mongo-shard-tic mongo-shard-tac mongo-shard-toe; do
  echo "waiting for ${h}..."
  wait_mongo "$h"
done

echo "init configReplSet (if needed)..."
mongosh "mongodb://mongo-config1:27017" --quiet --eval '
const ok = (() => {
  try {
    const s = rs.status();
    return s.ok === 1;
  } catch (e) {
    return false;
  }
})();
if (!ok) {
  rs.initiate({
    _id: "configReplSet",
    configsvr: true,
    members: [
      { _id: 0, host: "mongo-config1:27017" },
      { _id: 1, host: "mongo-config2:27017" },
      { _id: 2, host: "mongo-config3:27017" }
    ]
  });
}
'

echo "waiting for config PRIMARY..."
until mongosh "mongodb://mongo-config1:27017" --eval 'rs.isMaster().ismaster' --quiet 2>/dev/null | grep -q true; do
  sleep 2
done

for spec in "tic:mongo-shard-tic" "tac:mongo-shard-tac" "toe:mongo-shard-toe"; do
  IFS=: read -r rsname host <<<"$spec"
  echo "init replica set ${rsname} on ${host}..."
  mongosh "mongodb://${host}:27017" --quiet --eval '
const rsName = "'"${rsname}"'";
const h = "'"${host}:27017"'";
const ok = (() => {
  try {
    return rs.status().ok === 1;
  } catch (e) {
    return false;
  }
})();
if (!ok) {
  rs.initiate({ _id: rsName, members: [{ _id: 0, host: h }] });
}
'
  echo "waiting for PRIMARY on ${rsname}..."
  until mongosh "mongodb://${host}:27017" --eval 'rs.isMaster().ismaster' --quiet 2>/dev/null | grep -q true; do
    sleep 2
  done
done

echo "replica sets ready."
