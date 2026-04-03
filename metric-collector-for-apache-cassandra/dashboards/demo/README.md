### Cluster Demo

This directory’s **`docker-compose.yml`** defines **one** stack (Cassandra, ZooKeeper/Kafka/Connect, optional Postgres and Mongo demos, **Redis 7**, **OpenSearch**, Prometheus, Grafana). Topic-specific guides live in subfolders:

| Folder | Focus |
|--------|--------|
| **[`cassandra/`](cassandra/README.md)** | Cassandra + MCAC + nodetool-exporter |
| **[`kafka/`](kafka/README.md)** | ZooKeeper, Kafka broker, Kafka Connect image |
| **[`observability/`](observability/README.md)** | Prometheus + Grafana provisioning |
| **[`redis/`](redis/README.md)** | Redis **7** (password, AOF, volume) |
| **[`opensearch/`](opensearch/README.md)** | OpenSearch single-node + Dashboards (dev, HTTP) |
| **[`postgres-kafka/`](postgres-kafka/README.md)** | PostgreSQL HA + Debezium + JDBC sink |
| **[`mongo-kafka/`](mongo-kafka/README.md)** | Sharded Mongo + Debezium + Mongo sink |
| **[`mongo-sharded/`](mongo-sharded/README.md)** | Init scripts for config/shard/mongos (`tic` / `tac` / `toe`) |

---

This docker-compose script starts a small cluster with some workloads running and the dashboards
in one easy command!

To use:  

  1. From the project root make the agent (in Windows, you will need to do this under WSL):

     ```bash
     mvn -DskipTests package
     ```

  2. From this directory start the system, noting we need to parse the mcac-agent.jar version from the pom (in Windows, do this outside of WSL):

     ```bash
     export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
     docker-compose up
     ```
     
  3. Open your web browser to [http://localhost:3000](http://localhost:3000)
  
  If you want to change the jsonnet dashboards, make your changes then run:

  ```bash
  ../grafana/make-dashboards.sh
  ```
  
  Refresh the browser to see changes.

### Running tlp-stress commands (list, run, etc.)

**Running containers (`demo-stress-1`, `demo-stress2-1`)**  
Started by docker-compose. They run a **fixed workload** continuously (e.g. KeyValue, BasicTimeSeries) to generate load for the demo. You don’t run `list` or `run` inside them; they’re already running. You need these for ongoing load.

**One-off commands (`docker run --rm ...`)**  
Use a **new** container when you want to run a different workload, or run `list` / `-h` / a short `run` test. The `tlp-stress` binary isn’t on `PATH` when you `docker exec` into the running stress containers, so use `docker run` for these:

  ```bash
  docker run --rm thelastpickle/tlp-stress:latest list
  docker run --rm --network mcac_net -e TLP_STRESS_CASSANDRA_HOST=cassandra thelastpickle/tlp-stress:latest run KeyValue --rate 50 -d 60s
  ```

- **`list`** does not need Cassandra or `--network`; it only prints available workloads.
- **`run ...`** needs `--network mcac_net` and `TLP_STRESS_CASSANDRA_HOST=cassandra` to talk to your cluster.

So you need **both**: the compose stress containers for continuous demo load, and `docker run` for ad-hoc list/help/run.

### Cluster, datacenter and rack (by service)

The main cluster and Standalone use the default **SimpleSnitch** (datacenter1/rack1). **cassandra-dc2** is the same cluster as the main but in datacenter2 (GossipingPropertyFileSnitch + `main-cluster-dc2.rackdc.properties`). The Secondary cluster uses **GossipingPropertyFileSnitch** and `secondary-rackdc.properties` (datacenter2/rack2). **dc1-node1** is a standalone node (cluster "StandaloneDC1") using `secondary-dc1.rackdc.properties` (datacenter1/rack1); it can be joined to the Secondary cluster later so the cluster has two DCs.

| Service               | Cluster name   | DC           | Rack   |
|-----------------------|----------------|--------------|--------|
| cassandra             | Test Cluster   | datacenter1  | rack1  |
| cassandra2            | Test Cluster   | datacenter1  | rack1  |
| cassandra3            | Test Cluster   | datacenter1  | rack1  |
| cassandra-dc2         | Test Cluster   | datacenter2  | rack1  |
| cassandra4            | Test Cluster   | datacenter1  | rack1  |
| cassandra5            | Test Cluster   | datacenter1  | rack1  |
| cassandra-standalone  | Standalone     | datacenter1  | rack1  |
| dc2r2-node1           | Secondary      | datacenter2  | rack2  |
| dc2r2-node2           | Secondary      | datacenter2  | rack2  |
| dc2r2-node3           | Secondary      | datacenter2  | rack2  |
| dc2r2-node4           | StandaloneDC2R2 (standalone; same DC as main, rack2)   | datacenter1  | rack2  |
| dc1-node1             | StandaloneDC1 (standalone; can be joined to Secondary later) | datacenter1  | rack1  |

### Commands you can run (from this directory)

All commands assume you are in `dashboards/demo` (this directory). Replace `demo-` with your project name if you used `-p` with docker compose.

**Start / stop**
```bash
export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
docker compose up -d
docker compose down
docker compose ps -a
```

**Start only the Secondary cluster (dc* nodes)**  
If you only want the 5-node Secondary cluster and not the main cluster:
```bash
export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g')
docker compose up -d dc2r2-node1 dc2r2-node2 dc2r2-node3 dc2r2-node4 dc1-node1
```

**Run only dc* cluster + Grafana + Prometheus + one stress (stop and remove everything else)**  
From `dashboards/demo`, stop and remove all services except the dc* nodes, Grafana, Prometheus, and `stress`; then start only those. The **mcac** service runs once to populate the `mcac_data` volume with the MCAC agent (required for dc* nodes); then dc* and stress start.
```bash
cd /path/to/metric-collector-for-apache-cassandra/dashboards/demo

# 1. Stop and remove everything you don't want (main cluster, nodetool-exporter, stress2, etc.)
docker compose stop mcac nodetool-exporter cassandra cassandra2 cassandra3 cassandra4 cassandra5 cassandra-standalone stress2
docker compose rm -f mcac nodetool-exporter cassandra cassandra2 cassandra3 cassandra4 cassandra5 cassandra-standalone stress2

# 2. Start only: mcac (populates agent into mcac_data then exits), dc*, Grafana, Prometheus, stress
#    Ensure PROJECT_VERSION is set (e.g. in .env or: export PROJECT_VERSION=$(grep '<revision>' ../../pom.xml | sed -E 's/(<\/?revision>|[[:space:]])//g'))
docker compose up -d prometheus grafana mcac dc2r2-node1 dc2r2-node2 dc2r2-node3 dc2r2-node4 dc1-node1 stress
```
- **Grafana:** http://localhost:3000 — **Prometheus:** http://localhost:9090 — **Stress:** http://localhost:9500 — **CQL (Secondary):** `cqlsh -u cassandra -p cassandra 127.0.0.1 19450` (node1).
- The `stress` service is configured to use the Secondary cluster (`dc2r2-node1`). Wait for dc* nodes to be healthy before stress runs.
- If **dc2r2-node1** (or other dc*) shows **Exited (3)**, check logs: `docker compose logs dc2r2-node1 --tail 80`. Often the cause is an empty `mcac_data` volume (MCAC agent jar missing). Ensure **mcac** has run at least once (it populates the volume and exits); then start dc* again.

**MongoDB: 1 replica set (rs1) with 3 nodes + Prometheus + Grafana**  
No host ports 27017/27018/27019 (use 27201–27203). Metrics from mongodb-exporter are scraped by Prometheus and can be viewed in Grafana.
```bash
docker compose up -d prometheus grafana \
  mongo-rs1-node1 mongo-rs1-node2 mongo-rs1-node3 mongo-rs1-init mongodb-exporter
```
- **Host ports:** Node1 **27201**, Node2 **27202**, Node3 **27203**; Exporter **9216**; Prometheus **9090**; Grafana **3000**.
- **Connect from host:** `mongosh "mongodb://localhost:27201/?directConnection=true"` (or 27202/27203). For replica set awareness: `mongosh "mongodb://localhost:27201,localhost:27202,localhost:27203/?replicaSet=rs1"`. This is a **replica set**, not a sharded cluster — use **`rs.status()`** to see replica set status; do **not** use `sh.status()` (that is for sharded clusters with mongos).
- **Grafana:** Open [http://localhost:3000](http://localhost:3000), add or use existing Prometheus datasource (URL `http://prometheus:9090`). Query `mongodb_*` or import a MongoDB dashboard (e.g. Grafana.com dashboard 2583).
- **ARM Mac:** Services use `platform: linux/arm64`. On x86, set `platform: linux/amd64` (or remove the line) for mongo and mongodb-exporter in `docker-compose.yml`.
Wait ~2 minutes after `up` for init and exporter to be ready; then check `docker compose ps -a` (mongo-rs1-init should be Exited (0), mongodb-exporter Up). If **mongo-rs1-node3** or **mongo-rs1-init** stay **Created**, start them explicitly:
```bash
docker compose up -d mongo-rs1-node3
# wait ~1 min for node3 to be healthy, then:
docker compose up -d mongo-rs1-init mongodb-exporter
```

**Step-by-step: Remove MongoDB (rs1) only**  
From `dashboards/demo`, stop and remove only the MongoDB replica set and exporter (keeps Grafana, Prometheus, and other services running):
```bash
cd /path/to/metric-collector-for-apache-cassandra/dashboards/demo

docker compose stop mongodb-exporter mongo-rs1-init mongo-rs1-node3 mongo-rs1-node2 mongo-rs1-node1
docker compose rm -f mongodb-exporter mongo-rs1-init mongo-rs1-node3 mongo-rs1-node2 mongo-rs1-node1
```
Verify: `docker compose ps -a` should show no `mongo-rs1*` or `mongodb-exporter` containers.

**MongoDB sharded cluster (config servers + 2 shards + mongos)**  
To run a full sharded cluster so you can use `sh.status()`, `sh.enableSharding()`, `sh.shardCollection()`, etc., start the sharded stack and connect to **mongos** (not a shard node).
```bash
docker compose up -d prometheus grafana \
  mongo-config1 mongo-config2 mongo-config3 mongo-config-init \
  mongo-shard1-node1 mongo-shard1-node2 mongo-shard1-node3 mongo-shard1-init \
  mongo-shard2-node1 mongo-shard2-node2 mongo-shard2-node3 mongo-shard2-init \
  mongos mongo-cluster-init
```
- **Connect to sharded cluster (mongos):** `mongosh "mongodb://localhost:27300"`  
  Use this for all sharding commands: `sh.status()`, `sh.enableSharding("dbName")`, `sh.shardCollection("dbName.collName", { key: "hashed" })`.
- **Host ports:** mongos **27300**; config servers 27301–27303; shard1 27311–27313; shard2 27321–27323.
- Wait ~3–5 minutes for config init, shard inits, and cluster-init (addShard) to complete. Check `docker compose ps -a` (mongo-config-init, mongo-shard1-init, mongo-shard2-init, mongo-cluster-init should be Exited (0), mongos Up).

**Containers created by this compose**

Run from **this directory** (`dashboards/demo`) so compose sees the right project:
```bash
cd /path/to/metric-collector-for-apache-cassandra/dashboards/demo
docker compose ps -a
```
To see only demo containers with status:
```bash
docker ps -a --filter "name=demo-" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
```

**Understanding `docker ps` / `docker compose ps -a` status**

| STATUS | Meaning |
|--------|--------|
| **Up** (healthy) | Container is running and (if it has a healthcheck) healthy. This is what you want for long-lived services (mongod, cassandra, grafana, mongos). |
| **Up** | Container is running (no healthcheck or not yet healthy). |
| **Created** | Container exists but was **never started** (or is waiting for a dependency). Not running. Run `docker compose up -d` to start missing services. |
| **Exited (0)** | Container **finished successfully**. Normal for one-off init jobs (e.g. mongo-config-init, mongo-rs1-init). |
| **Exited (1)** or **Exited (3)** | Container **stopped with an error** (non-zero exit code). Check logs: `docker compose logs <service-name>`. |

- `docker ps` shows only **running** containers.
- `docker compose ps -a` (or `docker ps -a`) shows **all** containers (running, created, exited).  
If many services show **Created**, start them with: `docker compose up -d`.

**Clean up all MongoDB containers and reclaim Docker space**  
Removes every demo mongo container (rs1, config, shards, mongos, inits, exporter), then removes mongo images and prunes unused data to free disk space. Run from any directory:
```bash
# 1. Stop and remove all demo MongoDB-related containers (by name pattern)
docker rm -f $(docker ps -a -q --filter "name=demo-mongo") 2>/dev/null || true

# 2. Remove MongoDB images (optional: omit if you plan to use mongo again soon)
docker rmi mongo:8 percona/mongodb_exporter:0.40 2>/dev/null || true

# 3. Reclaim space: remove unused images, containers, networks (not volumes by default)
docker system prune -af

# 4. (Optional) Also remove unused volumes to reclaim more space
docker volume prune -f
```
**MongoDB rs1:** If `mongosh "mongodb://localhost:27201"` gives ECONNREFUSED, ensure the stack is up and wait ~2 min for mongo-rs1-init to finish. Check `docker compose ps -a` (mongo-rs1-init should be Exited (0), mongo-rs1-node1/2/3 and mongodb-exporter Up). If mongo-rs1-init failed, run `docker compose logs mongo-rs1-init --tail 30`.

**Cassandra – nodetool (main cluster: cassandra, cassandra2, cassandra3, cassandra-dc2 [DC2], cassandra4, cassandra5)**
```bash
docker exec demo-cassandra-1 nodetool status
docker exec demo-cassandra-dc2-1 nodetool status   # 4th node in datacenter2
docker exec demo-cassandra2-1 nodetool ring
```
**Standalone cluster (cluster name "Standalone", single node: cassandra-standalone)**
```bash
docker exec demo-cassandra-standalone-1 nodetool status
```

**Secondary cluster (cluster name "Secondary", datacenter2/rack2): dc2r2-node1, dc2r2-node2, dc2r2-node3, dc2r2-node4**
```bash
docker exec demo-dc2r2-node1-1 nodetool status
docker exec demo-dc2r2-node1-1 nodetool describecluster
```

**Standalone dc1-node1 (cluster "StandaloneDC1", datacenter1/rack1; can be joined to Secondary later for 2-DC setup)**
```bash
docker exec demo-dc1-node1-1 nodetool status
docker exec -it demo-dc1-node1-1 cqlsh   # or cqlsh localhost 19454
```

**Standalone dc2r2-node4 (cluster "StandaloneDC2R2", datacenter1/rack2; can be joined to Secondary later)**
```bash
docker exec demo-dc2r2-node4-1 nodetool status
docker exec -it demo-dc2r2-node4-1 cqlsh   # or cqlsh localhost 19453
```
**Manually add dc2r2-node4 to the Secondary cluster (dc2r2-node1, dc2r2-node2, dc2r2-node3 already running):**

1. **Step 1 – Stop and remove dc2r2-node4.** Run `docker compose stop dc2r2-node4` then `docker compose rm -f dc2r2-node4` (clears data so saved cluster name does not conflict).

2. **Edit `docker-compose.yml`** for `dc2r2-node4`: set `CASSANDRA_CLUSTER_NAME: "Secondary"`, `CASSANDRA_SEEDS: "dc2r2-node1,dc2r2-node2"`, `CASSANDRA_DC: "datacenter2"`, `CASSANDRA_RACK: "rack2"`, and change the rackdc volume to `./secondary-rackdc.properties`.

3. **Start dc2r2-node4:** `docker compose up -d dc2r2-node4` (it will bootstrap from the ring).

4. **Check the ring:** `docker exec demo-dc2r2-node1-1 nodetool status` — you should see 4 nodes in datacenter2, rack2.

5. **(Optional)** Add dc2r2-node4 to the seed list on dc2r2-node1/node2/node3 for discovery.

**Manually add dc1-node1 to the Secondary cluster (so the cluster has 2 DCs: datacenter1 + datacenter2):**

1. **Step 1 – Stop and remove dc1-node1.** Run `docker compose stop dc1-node1` then `docker compose rm -f dc1-node1` (clears data so saved cluster name does not conflict).

2. **Edit `docker-compose.yml`** for the `dc1-node1` service. Change only these two lines in the `environment` block:
   - `CASSANDRA_CLUSTER_NAME: "StandaloneDC1"` → `CASSANDRA_CLUSTER_NAME: "Secondary"`
   - `CASSANDRA_SEEDS: "dc1-node1"` → `CASSANDRA_SEEDS: "dc2r2-node1,dc2r2-node2"`
   Leave `CASSANDRA_DC`, `CASSANDRA_RACK`, and the volume `./secondary-dc1.rackdc.properties` unchanged.

3. **Start dc1-node1:** `docker compose up -d dc1-node1`. The service already has `-Dcassandra.auto_bootstrap=false` (like `dc2r2-node4.yaml`), so the node joins the ring without streaming data.

4. **Check the ring:** `docker exec demo-dc2r2-node1-1 nodetool status` — you should see datacenter2 (4 nodes) and datacenter1 (1 node).

5. **(Optional)** Add dc1-node1 to the seed list on Secondary nodes for discovery.

**Add dc1-node1 to the Secondary cluster as datacenter2** (5th node in DC2; cluster stays single-DC):

The compose mounts `dc1-node1.yaml` (copy of `dc2r2-node4.yaml` with `auto_bootstrap: false`) so the node joins without streaming. The image often does not apply the JVM option `-Dcassandra.auto_bootstrap=false`, so the yaml is used instead.

1. **Stop and remove dc1-node1** (clears data so saved cluster name does not conflict):
   ```bash
   docker compose stop dc1-node1
   docker compose rm -f dc1-node1
   ```

2. **Edit `docker-compose.yml`** for the `dc1-node1` service. In the `environment` block, set:
   - `CASSANDRA_CLUSTER_NAME: "Secondary"`
   - `CASSANDRA_SEEDS: "dc2r2-node1,dc2r2-node2"`
   - `CASSANDRA_DC: "datacenter2"`
   - `CASSANDRA_RACK: "rack2"`
   In the `volumes` section, change the rackdc mount from `./secondary-dc1.rackdc.properties` to `./secondary-rackdc.properties`.

3. **Start dc1-node1:** `docker compose up -d dc1-node1`. The service mounts `dc1-node1.yaml` (same content as `dc2r2-node4.yaml`) so `auto_bootstrap: false` is set in `cassandra.yaml`; the image entrypoint still overwrites seeds and cluster_name from env, so the node joins the ring without streaming.

4. **Check the ring:** `docker exec demo-dc2r2-node1-1 nodetool status` — you should see **datacenter2** with 5 nodes (dc2r2-node1–4 plus dc1-node1), all in rack2.

5. **(Optional)** Add dc1-node1 to the seed list on other Secondary nodes for discovery.

**Cassandra – cqlsh (host ports: main 19442–19446, cassandra-dc2 19448; Standalone 19447; Secondary 19450–19453; dc1-node1 19454)**
```bash
docker exec -it demo-cassandra-1 cqlsh
docker exec -it demo-cassandra-dc2-1 cqlsh         # or cqlsh localhost 19448 (main cluster, DC2)
# Standalone cluster (single node):
docker exec -it demo-cassandra-standalone-1 cqlsh   # or cqlsh localhost 19447
# Secondary cluster:
docker exec -it demo-dc2r2-node1-1 cqlsh           # or cqlsh localhost 19450/19451/19452
# Standalone dc1-node1 (datacenter1/rack1):
docker exec -it demo-dc1-node1-1 cqlsh            # or cqlsh localhost 19454
```

**Shell in a Cassandra node**
```bash
docker exec -it demo-cassandra-1 /bin/bash
docker exec -it demo-cassandra2-1 /bin/bash
```

**Install nano (or vim) in a Cassandra container**  
The image does not include an editor by default. From the host (replace container name as needed):
```bash
docker exec -u root demo-dc2r2-node4-1 apt-get update && docker exec -u root demo-dc2r2-node4-1 apt-get install -y nano
```
Or from inside the container (as root):
```bash
docker exec -it -u root demo-dc2r2-node4-1 /bin/bash
# then inside:
apt-get update && apt-get install -y nano
exit
```
Then use `nano /etc/cassandra/cassandra.yaml` (or other file) inside that container.

**Grafana**
- Open [http://localhost:3000](http://localhost:3000)

**Prometheus**
- Open [http://localhost:9090](http://localhost:9090)

**MongoDB dashboard shows "No data" – get metrics flowing**

Metrics path: **mongodb-exporter** (scraped by Prometheus every 15s) → **Prometheus** → **Grafana** (Prometheus datasource). If the MongoDB dashboard shows "No data", do this:

1. **Confirm the exporter and init completed**
   ```bash
   docker compose ps mongodb-exporter mongo-rs1-init
   ```
   - `mongodb-exporter` must be **running** (not "Exit 0" or missing). It only starts after `mongo-rs1-init` completes successfully.
   - If `mongo-rs1-init` failed, fix it (see "MongoDB rs1" above), then:
     ```bash
     docker compose up -d mongo-rs1-init mongodb-exporter
     ```

2. **Confirm Prometheus is scraping the MongoDB job**
   - Open [http://localhost:9090/targets](http://localhost:9090/targets).
   - Find the **mongodb** job; the target `mongodb-exporter:9216` should be **UP**.
   - If it shows **DOWN** with error **"lookup mongodb-exporter ... no such host"**, the **mongodb-exporter** container is not running (Docker removes its DNS entry when the container stops). Go back to step 1: get `mongo-rs1-init` to succeed and start `mongodb-exporter`, then wait ~30s and refresh the targets page.
   - If the target is still DOWN for another reason, check `docker compose logs mongodb-exporter` and that Prometheus and mongodb-exporter are on the same network (`demo_net` / `mcac_net`).

3. **Confirm Grafana can see MongoDB metrics**
   - In Grafana: **Explore** (compass icon) → choose **Prometheus**.
   - Run: `up{job="mongodb"}`. You should see `1` for `instance="mongodb-exporter:9216"`.
   - Then try: `mongodb_connections` or `mongodb_opcounters_insert_total`. If these return points, metrics are in Prometheus.

4. **Label / instance filter**
   - The MongoDB tic/tac/toe Grafana dashboards select the sharded exporter with **`{job="mongodb",instance="mongodb-exporter:9216"}`** (the service name inside the demo Compose network). That matches metrics even if your Prometheus does not set **`mongo_cluster`** (or uses extra labels like `cluster` / `shard_topology`).
   - If you scrape the same exporter under a **different** `instance` (e.g. `localhost:9216`), panels will be empty until you **edit the dashboard queries** to that instance or relabel in Prometheus to `instance="mongodb-exporter:9216"`.

5. **Dashboard variables (job / instance)**
   - If the dashboard has an **environment**, **job**, or **instance** dropdown, set **job** to `mongodb` and **instance** to `mongodb-exporter:9216` (or "All" if available).

6. **Time range**
   - Use **Last 15 minutes** or **Last 5 minutes** so recent scrapes are included; "Today so far" can miss data right after startup.

**TLP stress – help / list workloads / run (use new container)**
```bash
docker run --rm --network mcac_net -e TLP_STRESS_CASSANDRA_HOST=cassandra thelastpickle/tlp-stress:latest -h
docker run --rm thelastpickle/tlp-stress:latest list
docker run --rm --network mcac_net -e TLP_STRESS_CASSANDRA_HOST=cassandra thelastpickle/tlp-stress:latest run KeyValue --rate 50 -d 60s
```

**Logs**
```bash
docker logs demo-cassandra-1
docker logs demo-cassandra5-1 --tail 100
docker logs demo-stress-1
```
