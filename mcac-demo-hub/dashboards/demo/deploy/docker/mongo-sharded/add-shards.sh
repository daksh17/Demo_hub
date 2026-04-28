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
const shards = [
  { id: "tic", conn: "mongo-shard-tic:27017" },
  { id: "tac", conn: "mongo-shard-tac:27017" },
  { id: "toe", conn: "mongo-shard-toe:27017" }
];
function hasShard(id) {
  const r = admin.runCommand({ listShards: 1 });
  if (!r.shards) return false;
  return r.shards.some(s => s._id === id);
}
function shardIdentityPresent(conn) {
  try {
    const uri = "mongodb://" + conn;
    const m = new Mongo(uri);
    const doc = m.getDB("config").getCollection("shardIdentity").findOne();
    return doc !== null;
  } catch (e) {
    print("shardIdentity check " + conn + ": " + e.message);
    return false;
  }
}
function chunkCount(rem) {
  if (!rem || rem.chunks == null) return 0;
  const c = rem.chunks;
  if (typeof c === "number") return c;
  try {
    return parseInt(c.toString(), 10);
  } catch (e) {
    return 0;
  }
}
function ensureBalancerRunning() {
  const bs = admin.runCommand({ balancerStart: 1 });
  if (bs.ok !== 1) {
    printjson(bs);
  }
}
function listNonDrainingPeerIds(excludeId) {
  const r = admin.runCommand({ listShards: 1 });
  const out = [];
  for (const s of r.shards || []) {
    if (s._id === excludeId) continue;
    if (s.draining === true) {
      print("listNonDrainingPeerIds: skip " + s._id + " (draining)");
      continue;
    }
    out.push(s._id);
  }
  return out;
}
function pickNonDrainingTarget(excludeId) {
  const candidates = listNonDrainingPeerIds(excludeId);
  if (candidates.length === 0) {
    throw new Error(
      "no non-draining shard for movePrimary/moveChunk; metadata may list multiple draining shards. Delete namespace demo-hub and re-apply for a clean config."
    );
  }
  print("pickNonDrainingTarget: using " + candidates[0]);
  return candidates[0];
}
function tryAbortRemoveShard(id) {
  try {
    const ab = admin.runCommand({ abortRemoveShard: id });
    if (ab.ok === 1) {
      print("abortRemoveShard: ok");
      printjson(ab);
    } else {
      print("abortRemoveShard: " + (ab.errmsg || JSON.stringify(ab)));
    }
  } catch (e) {
    print("abortRemoveShard skipped (unsupported or not applicable): " + e.message);
  }
}
function forceMoveChunksOffShard(fromId, toId) {
  const cfg = db.getSiblingDB("config");
  const list = cfg.chunks.find({ shard: fromId }).toArray();
  print("forceMoveChunksOffShard: " + list.length + " chunk(s) on " + fromId + " -> " + toId);
  for (let i = 0; i < list.length; i++) {
    const ch = list[i];
    if (!ch.ns || ch.min == null || ch.max == null) {
      print("WARN: skip malformed chunk doc at index " + i);
      continue;
    }
    const res = admin.runCommand({ moveChunk: ch.ns, bounds: [ch.min, ch.max], to: toId });
    if (res.ok !== 1) {
      print("WARN moveChunk failed for " + ch.ns + ": " + JSON.stringify(res));
    }
  }
}
function forceMoveChunksOffShardWithFallback(fromId) {
  const targets = listNonDrainingPeerIds(fromId);
  if (targets.length === 0) {
    print("forceMoveChunksOffShardWithFallback: no non-draining peers for " + fromId);
    return;
  }
  const cfg = db.getSiblingDB("config");
  const list = cfg.chunks.find({ shard: fromId }).toArray();
  print(
    "forceMoveChunksOffShardWithFallback: " + list.length + " chunk(s) on " + fromId + " try targets " + JSON.stringify(targets)
  );
  for (let i = 0; i < list.length; i++) {
    const ch = list[i];
    if (!ch.ns || ch.min == null || ch.max == null) continue;
    let moved = false;
    for (let ti = 0; ti < targets.length && !moved; ti++) {
      const res = admin.runCommand({ moveChunk: ch.ns, bounds: [ch.min, ch.max], to: targets[ti] });
      if (res.ok === 1) {
        print("moveChunk ok " + ch.ns + " -> " + targets[ti]);
        moved = true;
      } else {
        print("moveChunk fail " + ch.ns + " -> " + targets[ti] + " " + JSON.stringify(res));
      }
    }
    if (!moved) print("WARN: all targets failed for " + ch.ns);
  }
}
function recoverStuckDrainingShards() {
  const r = admin.runCommand({ listShards: 1 });
  if (!r.shards) return;
  const draining = r.shards.filter((s) => s.draining === true).map((s) => s._id);
  if (draining.length === 0) return;
  print("recoverStuckDrainingShards: found draining=" + JSON.stringify(draining));
  ensureBalancerRunning();
  for (const id of draining) {
    print("recoverStuckDrainingShards: finishing removeShard drain for " + id);
    tryAbortRemoveShard(id);
    ensureBalancerRunning();
    forceMoveChunksOffShardWithFallback(id);
    const maxRounds = 120;
    for (let round = 0; round < maxRounds; round++) {
      forceMoveChunksOffShardWithFallback(id);
      const rs = admin.runCommand({ removeShard: id });
      printjson(rs);
      if (rs.ok !== 1) {
        print("recoverStuckDrainingShards: removeShard failed for " + id + ", stop recovery (fix cluster or delete ns demo-hub)");
        break;
      }
      if (rs.state === "completed") {
        print("recoverStuckDrainingShards: removeShard completed for " + id);
        break;
      }
      if (rs.state === "ongoing" && rs.dbsToMove && rs.dbsToMove.length > 0) {
        const tp = pickNonDrainingTarget(id);
        for (const dbn of rs.dbsToMove) {
          print("movePrimary " + dbn + " to " + tp + " (recover drain " + id + ")");
          const mp = admin.runCommand({ movePrimary: dbn, to: tp });
          printjson(mp);
          if (mp.ok !== 1) {
            print("WARN movePrimary failed: " + JSON.stringify(mp));
          }
        }
      }
      sleep(4000);
    }
  }
}
function removeShardStale(id) {
  print("removing stale shard registration " + id + " (config lists shard but mongod has no shardIdentity — e.g. shard pod emptyDir reset)");
  tryAbortRemoveShard(id);
  ensureBalancerRunning();
  forceMoveChunksOffShardWithFallback(id);
  for (let iter = 1; ; iter++) {
    if (iter > 720) {
      throw new Error(
        "removeShard drain timeout for " + id + " (720 iterations). Chunks may still be migrating; check balancer, shard health, and config.chunks on mongos."
      );
    }
    const r = admin.runCommand({ removeShard: id });
    printjson(r);
    if (r.ok !== 1) {
      throw new Error("removeShard failed for " + id + ": " + JSON.stringify(r));
    }
    if (r.state === "completed") break;
    if (r.state === "ongoing" && r.dbsToMove && r.dbsToMove.length > 0) {
      ensureBalancerRunning();
      const targetMp = pickNonDrainingTarget(id);
      for (const dbn of r.dbsToMove) {
        print("movePrimary " + dbn + " to " + targetMp + " (draining stale shard " + id + ")");
        const mp = admin.runCommand({ movePrimary: dbn, to: targetMp });
        printjson(mp);
        if (mp.ok !== 1) {
          throw new Error("movePrimary failed for " + dbn + ": " + JSON.stringify(mp));
        }
      }
      continue;
    }
    if (r.state === "ongoing") {
      const n = chunkCount(r.remaining);
      ensureBalancerRunning();
      if (n > 0 && iter > 3 && iter % 12 === 0) {
        print("re-try forceMoveChunksOffShardWithFallback (iter " + iter + ", chunks remaining ~" + n + ")");
        forceMoveChunksOffShardWithFallback(id);
      }
      const waitMs = n > 0 ? 8000 : 3000;
      if (n > 0 && iter % 15 === 0) {
        const bst = admin.runCommand({ balancerStatus: 1 });
        print("balancerStatus (every ~2m while " + n + " chunks remain): ");
        printjson(bst);
      }
      sleep(waitMs);
      continue;
    }
    sleep(3000);
  }
}
function dropLocalDemoOnShard(conn) {
  try {
    const uri = "mongodb://" + conn;
    const m = new Mongo(uri);
    const ldb = m.getDB("demo");
    const list = m.getDB("admin").runCommand({ listDatabases: 1 });
    if (!list.databases || !list.databases.some(d => d.name === "demo")) return;
    print("dropping stray local database demo on " + conn + " (required before addShard when demo already exists on another shard)");
    ldb.dropDatabase();
  } catch (e) {
    print("pre-clean on " + conn + ": " + e.message);
  }
}
recoverStuckDrainingShards();
for (const s of shards) {
  let mustAdd = false;
  if (hasShard(s.id)) {
    if (shardIdentityPresent(s.conn)) {
      print("shard " + s.id + " already present");
      continue;
    }
    removeShardStale(s.id);
    mustAdd = true;
  } else {
    mustAdd = true;
  }
  if (mustAdd) {
    dropLocalDemoOnShard(s.conn);
    print("adding shard " + s.id);
    const r = admin.runCommand({ addShard: s.id + "/" + s.conn });
    printjson(r);
    if (r.ok !== 1) {
      throw new Error("addShard failed for " + s.id + ": " + JSON.stringify(r));
    }
  }
}
print("done");
'
