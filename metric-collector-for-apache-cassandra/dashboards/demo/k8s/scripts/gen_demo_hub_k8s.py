#!/usr/bin/env python3
"""Generate Kubernetes manifests from dashboards/demo/docker-compose.yml (demo-hub stack).

Outputs under k8s/generated/: Deployments, StatefulSets, Services, ConfigMaps, bootstrap **Jobs**
(Postgres/Cassandra/Mongo — same scripts as Compose), **Secret** (`demo-hub-credentials`), and ops extras
(Ingress, PDB, NetworkPolicy, HPA, CronJob). Run **scripts/apply-data-bootstrap.sh** after workloads are up.

Usage:
  python3 k8s/scripts/gen_demo_hub_k8s.py

Apply:
  kubectl apply -f k8s/namespace.yaml -f k8s/generated/
  ./k8s/scripts/apply-data-bootstrap.sh
"""
from __future__ import annotations

import hashlib
import json
import re
import textwrap
from pathlib import Path

SCRIPT = Path(__file__).resolve()
K8S_ROOT = SCRIPT.parents[1]
DEMO = K8S_ROOT.parent
DASHBOARDS = DEMO.parent
REPO_ROOT = DASHBOARDS.parent
OUT = K8S_ROOT / "generated"
NS = "demo-hub"
# Opaque Secret with demo passwords (same values as Compose); workloads use valueFrom.secretKeyRef.
SECRET_NAME = "demo-hub-credentials"
SK_POSTGRESQL_PASSWORD = "postgresql-password"
SK_POSTGRESQL_REPLICATION_PASSWORD = "postgresql-replication-password"
SK_REDIS_PASSWORD = "redis-password"
SK_DEMO_USER_PASSWORD = "demo-user-password"
SK_HUB_POSTGRES_DSN = "hub-postgres-dsn"
SK_HUB_REDIS_URL = "hub-redis-url"
# Cassandra data disk: use default StorageClass when set to "" (cluster must provide one).
CASSANDRA_DATA_STORAGE_CLASS = ""
CASSANDRA_DATA_STORAGE_SIZE = "10Gi"
# Local-only image (not on Docker Hub). Tag is NOT :latest so the kubelet does not always try to pull.
# Build: `docker build -t mcac-demo/mcac-init:local <repo-root>` (see k8s/scripts/build-mcac-init-image.sh).
MCAC_INIT_IMAGE = "mcac-demo/mcac-init:local"
# `docker.io/bitnami/postgresql:16` often 404s ("manifest unknown"); legacy repo has multi-arch pins (same line as ZooKeeper/Kafka).
POSTGRESQL_IMAGE = "docker.io/bitnamilegacy/postgresql:16.6.0-debian-12-r2"


def fqdn_service(svc: str) -> str:
    """Kubernetes DNS name for in-cluster clients (avoids ambiguous short-name resolution)."""
    return f"{svc}.{NS}.svc.cluster.local"


def cassandra_headless_contact_points() -> str:
    """Per-pod headless DNS for the Python driver's contact points.

    Using the ClusterIP Service name alone can yield NoHostAvailable: the driver discovers
    peer IPs from gossip; a replica may not yet accept CQL on 9042 during startup. Headless
    FQDNs match nodetool-exporter / stable CQL patterns for this StatefulSet.
    """
    return ",".join(
        f"cassandra-{i}.cassandra-headless.{NS}.svc.cluster.local" for i in range(3)
    )


def tcp_readiness_probe(
    port: int,
    *,
    initial_delay: int = 10,
    period: int = 5,
    timeout: int = 3,
    failure_threshold: int = 12,
) -> str:
    return f"""          readinessProbe:
            tcpSocket:
              port: {port}
            initialDelaySeconds: {initial_delay}
            periodSeconds: {period}
            timeoutSeconds: {timeout}
            failureThreshold: {failure_threshold}
"""


def join_docs(*parts: str) -> str:
    return "\n---\n\n".join(p.strip() for p in parts if p.strip()) + "\n"


def read_repo(rel: str) -> str | None:
    p = DEMO / rel
    if not p.is_file():
        return None
    return p.read_text(encoding="utf-8")


def cm_data_block(files: dict[str, str]) -> str:
    parts: list[str] = []
    for key, content in files.items():
        body = content.rstrip("\n") + "\n"
        indented = "".join("    " + ln + "\n" for ln in body.splitlines())
        parts.append(f"  {key}: |\n{indented}")
    return "\n".join(parts)


def configmap(name: str, group: str, files: dict[str, str]) -> str:
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {NS}
  labels:
    demo-hub.io/group: {group}
    app.kubernetes.io/part-of: demo-hub
data:
{cm_data_block(files)}
"""


def grafana_dash_cm_name(fname: str) -> str:
    """Unique DNS-1123 name per dashboard file (one ConfigMap each — avoids kubectl apply annotation limit)."""
    base = re.sub(r"[^a-z0-9]+", "-", Path(fname).name.lower()).strip("-") or "dash"
    name = f"grafana-db-{base}"
    if len(name) > 63:
        short = hashlib.sha256(fname.encode()).hexdigest()[:10]
        name = f"grafana-db-{short}"
    return name[:63]


def batch_job(
    name: str,
    group: str,
    image: str,
    command: list[str],
    configmap_name: str,
    mount_path: str = "/scripts",
    ttl_seconds: int = 86400,
    active_deadline_seconds: int | None = 7200,
) -> str:
    cmd = "\n".join(f"            - {jdump(c)}" for c in command)
    deadline = ""
    if active_deadline_seconds is not None:
        deadline = f"  activeDeadlineSeconds: {active_deadline_seconds}\n"
    return f"""apiVersion: batch/v1
kind: Job
metadata:
  name: {name}
  namespace: {NS}
  labels:
    demo-hub.io/group: {group}
    app.kubernetes.io/part-of: demo-hub
spec:
{deadline}  ttlSecondsAfterFinished: {ttl_seconds}
  backoffLimit: 4
  template:
    metadata:
      labels:
        demo-hub.io/group: {group}
        app.kubernetes.io/part-of: demo-hub
    spec:
      restartPolicy: Never
      containers:
        - name: run
          image: {image}
          imagePullPolicy: IfNotPresent
          command:
{cmd}
          volumeMounts:
            - name: scripts
              mountPath: {mount_path}
              readOnly: true
      volumes:
        - name: scripts
          configMap:
            name: {configmap_name}
            defaultMode: 0755
"""


def jdump(s: str) -> str:
    return json.dumps(s)


def lbl(group: str, name: str, extra: dict[str, str] | None = None) -> tuple[str, str]:
    meta = {
        "app.kubernetes.io/name": name,
        "app.kubernetes.io/part-of": "demo-hub",
        "demo-hub.io/group": group,
    }
    if extra:
        meta.update(extra)
    ml = "\n".join(f"    {k}: {v}" for k, v in meta.items())
    pl = "\n".join(f"        {k}: {v}" for k, v in meta.items())
    return ml, pl


def env_lines(
    pairs: list[tuple[str, str]],
    secret_refs: list[tuple[str, str, str]] | None = None,
    indent: str = "            ",
) -> str:
    lines = ["          env:"]
    for k, v in pairs:
        lines.append(f"{indent}- name: {k}")
        lines.append(f"{indent}  value: {jdump(v)}")
    for env_name, sec_name, key in secret_refs or ():
        lines.append(f"{indent}- name: {env_name}")
        lines.append(f"{indent}  valueFrom:")
        lines.append(f"{indent}    secretKeyRef:")
        lines.append(f"{indent}      name: {sec_name}")
        lines.append(f"{indent}      key: {key}")
    return "\n".join(lines) + "\n"


# Init: Kafka must not start until ZooKeeper is *ready* (Service has endpoints). Use full DNS
# name; short names can race before CoreDNS search path is ready.
def _kafka_wait_zk() -> str:
    host = fqdn_service("zookeeper")
    return f"""      initContainers:
        - name: wait-zookeeper
          image: alpine:3.19
          command:
            - /bin/sh
            - -c
            - |
              apk add --no-cache netcat-openbsd >/dev/null
              for i in $(seq 1 120); do
                if nc -z -w 2 {host} 2181 2>/dev/null; then exit 0; fi
                sleep 2
              done
              echo "timeout waiting for {host}:2181" >&2
              exit 1
"""


def _kafka_wait_broker() -> str:
    host = fqdn_service("kafka")
    return f"""      initContainers:
        - name: wait-kafka
          image: alpine:3.19
          command:
            - /bin/sh
            - -c
            - |
              apk add --no-cache netcat-openbsd >/dev/null
              for i in $(seq 1 150); do
                if nc -z -w 2 {host} 9092 2>/dev/null; then exit 0; fi
                sleep 2
              done
              echo "timeout waiting for {host}:9092" >&2
              exit 1
"""


def _hub_wait_upstream() -> str:
    # Must match app.py lifespan(): Cluster(cassandra), psycopg, httpx→OpenSearch. Without these,
    # uvicorn exits immediately → CrashLoopBackOff (init used to wait only kafka+opensearch).
    kh = fqdn_service("kafka")
    oh = fqdn_service("opensearch")
    pg = fqdn_service("postgresql-primary")
    cs = fqdn_service("cassandra")
    rd = fqdn_service("redis")
    mg = fqdn_service("mongo-mongos1")
    return f"""      initContainers:
        - name: wait-hub-deps
          image: alpine:3.19
          command:
            - /bin/sh
            - -c
            - |
              apk add --no-cache netcat-openbsd >/dev/null
              for i in $(seq 1 200); do
                if nc -z -w 2 {kh} 9092 2>/dev/null \\
                  && nc -z -w 2 {oh} 9200 2>/dev/null \\
                  && nc -z -w 2 {pg} 5432 2>/dev/null \\
                  && nc -z -w 2 {cs} 9042 2>/dev/null \\
                  && nc -z -w 2 {rd} 6379 2>/dev/null \\
                  && nc -z -w 2 {mg} 27017 2>/dev/null; then
                  exit 0
                fi
                sleep 2
              done
              echo "timeout waiting for kafka:9092, opensearch:9200, postgresql-primary:5432," >&2
              echo "  cassandra:9042, redis:6379, mongo-mongos1:27017" >&2
              exit 1
"""


def deployment(
    name: str,
    group: str,
    image: str,
    replicas: int,
    ports: list[tuple[int, str]],
    env: list[tuple[str, str]],
    command: list[str] | None = None,
    args: list[str] | None = None,
    extra_labels: dict[str, str] | None = None,
    data_mount: str | None = None,
    resources: str | None = None,
    init_before_containers: str | None = None,
    fs_group: int | None = None,
    strategy_recreate: bool = False,
    readiness_probe: str | None = None,
    env_secret_refs: list[tuple[str, str, str]] | None = None,
) -> str:
    ml, pl = lbl(group, name, extra_labels)
    port_block = "\n".join(
        f"            - containerPort: {p}\n              name: {n}" for p, n in ports
    )
    svc_ports = "\n".join(
        f"    - port: {p}\n      targetPort: {p}\n      name: {n}" for p, n in ports
    )
    cmd = ""
    if command:
        cmd = "          command:\n" + "\n".join(f"            - {jdump(c)}" for c in command) + "\n"
    arg = ""
    if args:
        arg = "          args:\n" + "\n".join(f"            - {jdump(a)}" for a in args) + "\n"
    ev = ""
    if env or env_secret_refs:
        ev = env_lines(env, env_secret_refs)
    vol_mount = ""
    vol = ""
    if data_mount:
        vol_mount = f"""          volumeMounts:
            - name: data
              mountPath: {data_mount}
"""
        vol = """      volumes:
        - name: data
          emptyDir: {}
"""
    res = resources or ""
    rprobe = readiness_probe or ""
    cname = name.replace("_", "-")
    init = init_before_containers or ""
    sec = ""
    if fs_group is not None:
        sec = f"""      securityContext:
        fsGroup: {fs_group}
"""
    strat = ""
    if strategy_recreate:
        strat = """  strategy:
    type: Recreate
"""
    dep = f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {NS}
  labels:
{ml}
spec:
  replicas: {replicas}
{strat}  selector:
    matchLabels:
      app.kubernetes.io/name: {name}
  template:
    metadata:
      labels:
{pl}
    spec:
{sec}{init}      containers:
        - name: {cname}
          image: {image}
          imagePullPolicy: IfNotPresent
{cmd}{arg}{ev}{res}{vol_mount}          ports:
{port_block}
{rprobe}{vol}"""
    svc = f"""apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {NS}
  labels:
{ml}
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: {name}
  ports:
{svc_ports}
"""
    return join_docs(dep, svc)


def demo_hub_credentials_secret() -> str:
    return f"""apiVersion: v1
kind: Secret
metadata:
  name: {SECRET_NAME}
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: credentials
type: Opaque
stringData:
  {SK_POSTGRESQL_PASSWORD}: postgres
  {SK_POSTGRESQL_REPLICATION_PASSWORD}: replicatorpass
  {SK_REDIS_PASSWORD}: demoredispass
  {SK_DEMO_USER_PASSWORD}: demopass
  {SK_HUB_POSTGRES_DSN}: postgresql://demo:demopass@postgresql-primary:5432/demo
  {SK_HUB_REDIS_URL}: redis://:demoredispass@redis:6379/0
"""


def kubernetes_ops_extras() -> str:
    """Ingress, PDB, NetworkPolicy, HPA, CronJob — production-style ops (demo values)."""
    np = f"""apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: demo-hub-namespace-isolation
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/part-of: demo-hub
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {NS}
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ingress-nginx
  egress:
    - {{}}
"""
    ing = f"""apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: demo-hub-ingress
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  rules:
    - host: demo-hub.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: hub-demo-ui
                port:
                  number: 8888
    - host: grafana.demo-hub.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: grafana
                port:
                  number: 3000
    - host: prometheus.demo-hub.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: prometheus
                port:
                  number: 9090
"""
    pdb_c = f"""apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cassandra-pdb
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: cassandra
      demo-hub.io/cassandra-workload: statefulset
"""
    pdb_pg = f"""apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: postgresql-primary-pdb
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: postgresql-primary
"""
    pdb_hub = f"""apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: hub-demo-ui-pdb
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: hub-demo-ui
"""
    hpa = f"""apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: hub-demo-ui
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: hub-demo-ui
  minReplicas: 1
  maxReplicas: 3
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
"""
    cron = f"""apiVersion: batch/v1
kind: CronJob
metadata:
  name: demo-hub-smoke-curl
  namespace: {NS}
  labels:
    app.kubernetes.io/part-of: demo-hub
    demo-hub.io/group: kubernetes-ops
spec:
  schedule: "*/30 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      ttlSecondsAfterFinished: 600
      template:
        metadata:
          labels:
            app.kubernetes.io/part-of: demo-hub
            demo-hub.io/group: kubernetes-ops
        spec:
          restartPolicy: Never
          containers:
            - name: curl
              image: curlimages/curl:8.5.0
              imagePullPolicy: IfNotPresent
              args:
                - -sf
                - http://hub-demo-ui:8888/docs
"""
    return join_docs(np, ing, pdb_c, pdb_pg, pdb_hub, hpa, cron)


def read_mcac_revision() -> str:
    """Align javaagent JAR name with Maven output (`datastax-mcac-agent-<revision>.jar`)."""
    pom = REPO_ROOT / "pom.xml"
    raw = read_text(pom) if pom.is_file() else None
    if raw:
        m = re.search(r"<revision>([^<]+)</revision>", raw)
        if m:
            return m.group(1).strip()
    return "0.3.3"


def mcac_agent_configmap() -> str:
    """MCAC agent config (same paths as Compose: /mcac/config/...)."""
    cfg = REPO_ROOT / "config"
    mcy = read_text(cfg / "metric-collector.yaml")
    cct = read_text(cfg / "collectd.conf.tmpl")
    if not mcy or not cct:
        raise SystemExit(
            f"MCAC config files missing under {cfg} (need metric-collector.yaml, collectd.conf.tmpl)."
        )
    return configmap(
        "mcac-agent-config",
        "cassandra-ring",
        {"metric-collector.yaml": mcy, "collectd.conf.tmpl": cct},
    )


def cassandra_statefulset() -> str:
    seed = f"cassandra-0.cassandra-headless.{NS}.svc.cluster.local"
    rev = read_mcac_revision()
    agent_jar = f"/mcac/lib/datastax-mcac-agent-{rev}.jar"
    jvm_extra = (
        f"-javaagent:{agent_jar} "
        "-Dcassandra.consistent.rangemovement=false -Dcassandra.ring_delay_ms=100 "
        "-Dcassandra.jmx.remote.authenticate=false -Dcom.sun.management.jmxremote.authenticate=false "
        "-Dcom.sun.management.jmxremote.ssl=false"
    )
    env = [
        ("MAX_HEAP_SIZE", "500M"),
        ("HEAP_NEWSIZE", "100M"),
        ("LOCAL_JMX", "no"),
        ("JVM_EXTRA_OPTS", jvm_extra),
        ("CASSANDRA_NUM_TOKENS", "8"),
        ("CASSANDRA_SEEDS", seed),
    ]
    ev = "\n".join(["          env:"] + [f"            - name: {k}\n              value: {jdump(v)}" for k, v in env])
    headless = f"""apiVersion: v1
kind: Service
metadata:
  name: cassandra-headless
  namespace: {NS}
  labels:
    demo-hub.io/group: cassandra-ring
    app.kubernetes.io/name: cassandra
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app.kubernetes.io/name: cassandra
    demo-hub.io/cassandra-workload: statefulset
  ports:
    - port: 9042
      name: cql
      targetPort: 9042
    - port: 7000
      name: intra
      targetPort: 7000
    - port: 7001
      name: tls-intra
      targetPort: 7001
    - port: 9103
      name: jmx
      targetPort: 9103
    - port: 9501
      name: mcac
      targetPort: 9501
"""
    client = f"""apiVersion: v1
kind: Service
metadata:
  name: cassandra
  namespace: {NS}
  labels:
    demo-hub.io/group: cassandra-ring
    app.kubernetes.io/name: cassandra
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: cassandra
    demo-hub.io/cassandra-workload: statefulset
  ports:
    - port: 9042
      name: cql
      targetPort: 9042
"""
    sc = CASSANDRA_DATA_STORAGE_CLASS.strip()
    sc_yaml = ""
    if sc:
        sc_yaml = f"\n        storageClassName: {jdump(sc)}"
    sts = f"""apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cassandra
  namespace: {NS}
  labels:
    demo-hub.io/group: cassandra-ring
    app.kubernetes.io/name: cassandra
spec:
  serviceName: cassandra-headless
  replicas: 3
  podManagementPolicy: OrderedReady
  selector:
    matchLabels:
      app.kubernetes.io/name: cassandra
      demo-hub.io/cassandra-workload: statefulset
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cassandra
        app.kubernetes.io/part-of: demo-hub
        demo-hub.io/group: cassandra-ring
        demo-hub.io/cassandra-workload: statefulset
    spec:
      initContainers:
        - name: mcac-copy-agent
          image: {MCAC_INIT_IMAGE}
          imagePullPolicy: Never
          command:
            - sh
            - -c
            - mkdir -p /mcac/lib && cp -r /agent/lib/. /mcac/lib/
          volumeMounts:
            - name: mcac-agent
              mountPath: /mcac
      containers:
        - name: cassandra
          image: cassandra:4.0
          imagePullPolicy: IfNotPresent
{ev}
          resources:
            requests:
              memory: "1536Mi"
            limits:
              memory: "3Gi"
          ports:
            - containerPort: 9042
              name: cql
            - containerPort: 7000
              name: intra
            - containerPort: 9103
              name: jmx
            - containerPort: 9501
              name: mcac
          volumeMounts:
            - name: data
              mountPath: /var/lib/cassandra
            - name: mcac-agent
              mountPath: /mcac
            - name: mcac-config
              mountPath: /mcac/config
              readOnly: true
      volumes:
        - name: mcac-agent
          emptyDir: {{}}
        - name: mcac-config
          configMap:
            name: mcac-agent-config
  volumeClaimTemplates:
    - metadata:
        name: data
        labels:
          app.kubernetes.io/name: cassandra
          demo-hub.io/group: cassandra-ring
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: {jdump(CASSANDRA_DATA_STORAGE_SIZE)}{sc_yaml}
"""
    return join_docs(mcac_agent_configmap(), headless, client, sts)


def opensearch_configmap() -> str:
    yml = textwrap.dedent(
        """\
        cluster.name: demo-hub
        network.host: 0.0.0.0
        discovery.type: single-node
        plugins.security.disabled: true
        cluster.routing.allocation.disk.threshold_enabled: false
        """
    )
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: opensearch-config
  namespace: {NS}
  labels:
    demo-hub.io/group: opensearch
data:
  opensearch.yml: |
{textwrap.indent(yml, "    ")}
"""


def opensearch_stack() -> str:
    cm = opensearch_configmap()
    dep = f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: opensearch
  namespace: {NS}
  labels:
    demo-hub.io/group: opensearch
    app.kubernetes.io/name: opensearch
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: opensearch
  template:
    metadata:
      labels:
        app.kubernetes.io/name: opensearch
        demo-hub.io/group: opensearch
    spec:
      containers:
        - name: opensearch
          image: opensearchproject/opensearch:2.11.1
          imagePullPolicy: IfNotPresent
          env:
            - name: OPENSEARCH_JAVA_OPTS
              value: "-Xms512m -Xmx512m -XX:MaxDirectMemorySize=256m"
            - name: DISABLE_SECURITY_PLUGIN
              value: "true"
            - name: DISABLE_INSTALL_DEMO_CONFIG
              value: "true"
          volumeMounts:
            - name: config
              mountPath: /usr/share/opensearch/config/opensearch.yml
              subPath: opensearch.yml
            - name: data
              mountPath: /usr/share/opensearch/data
          ports:
            - containerPort: 9200
              name: http
            - containerPort: 9600
              name: metrics
      volumes:
        - name: config
          configMap:
            name: opensearch-config
        - name: data
          emptyDir: {{}}
"""
    svc = f"""apiVersion: v1
kind: Service
metadata:
  name: opensearch
  namespace: {NS}
  labels:
    app.kubernetes.io/name: opensearch
    demo-hub.io/group: opensearch
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: opensearch
  ports:
    - port: 9200
      name: http
      targetPort: 9200
    - port: 9600
      name: metrics
      targetPort: 9600
"""
    dash = deployment(
        "opensearch-dashboards",
        "opensearch",
        "opensearchproject/opensearch-dashboards:2.11.1",
        1,
        [(5601, "http")],
        [
            ("OPENSEARCH_HOSTS", '["http://opensearch:9200"]'),
            ("DISABLE_SECURITY_DASHBOARDS_PLUGIN", "true"),
        ],
    )
    exp = deployment(
        "opensearch-exporter",
        "opensearch",
        "quay.io/prometheuscommunity/elasticsearch-exporter:v1.7.0",
        1,
        [(9114, "metrics")],
        [],
        args=["--es.uri=http://opensearch:9200", "--es.all", "--es.indices", "--es.timeout=30s"],
    )
    return join_docs(cm, dep, svc, dash, exp)


def read_text(p: Path) -> str | None:
    try:
        return p.read_text(encoding="utf-8")
    except OSError:
        return None


def k8s_prometheus_for_demo_hub(prom_yml: str) -> str:
    """Tune shared dashboards/prometheus/prometheus.yaml for this K8s stack (not Docker Compose)."""
    # `mongodb-exporter-local` exists only in Compose; omit so the target is not permanently DOWN.
    prom_yml = re.sub(
        r"\n      - targets:\n          - mongodb-exporter-local:9216\n        labels:\n          mongo_cluster: \"host-standalone\"\n          mongo_topology: \"standalone\"\n",
        "\n",
        prom_yml,
    )
    # MCAC scrapes can be slow; need scrape_timeout up to ~30s. Prometheus requires timeout <= interval.
    prom_yml = prom_yml.replace(
        '  - job_name: "mcac"\n    scrape_interval: 15s\n    scrape_timeout:  15s',
        '  - job_name: "mcac"\n    scrape_interval: 30s\n    scrape_timeout:  28s',
        1,
    )
    return prom_yml


def k8s_tg_mcac_json() -> str:
    """MCAC scrape targets: Cassandra StatefulSet pods (headless), not compose names (cassandra2, …)."""
    # Fully-qualified DNS avoids relying on ndots/search path in every cluster.
    targets = [
        f"cassandra-{i}.cassandra-headless.{NS}.svc.cluster.local:9103" for i in range(3)
    ]
    return json.dumps([{"targets": targets, "labels": {}}], indent=2) + "\n"


def prometheus_stack() -> str:
    prom_yml = read_text(DASHBOARDS / "prometheus" / "prometheus.yaml")
    if not prom_yml:
        prom_yml = textwrap.dedent(
            """\
            global:
              scrape_interval: 15s
            scrape_configs:
              - job_name: prometheus
                static_configs:
                  - targets: ['localhost:9090']
            """
        )
    else:
        prom_yml = k8s_prometheus_for_demo_hub(prom_yml)
    tg = k8s_tg_mcac_json()
    cm_prom = f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: {NS}
  labels:
    demo-hub.io/group: observability
data:
  prometheus.yaml: |
{textwrap.indent(prom_yml.rstrip(), "    ")}
"""
    cm_tg = f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-tg-mcac
  namespace: {NS}
  labels:
    demo-hub.io/group: observability
data:
  tg_mcac.json: |
{textwrap.indent(tg.rstrip(), "    ")}
"""
    dep = f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: {NS}
  labels:
    demo-hub.io/group: observability
    app.kubernetes.io/name: prometheus
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: prometheus
  template:
    metadata:
      labels:
        app.kubernetes.io/name: prometheus
        demo-hub.io/group: observability
    spec:
      containers:
        - name: prometheus
          image: prom/prometheus:v2.17.1
          imagePullPolicy: IfNotPresent
          args:
            - --config.file=/etc/prometheus/prometheus.yaml
            - --storage.tsdb.path=/prometheus
          volumeMounts:
            - name: cfg
              mountPath: /etc/prometheus/prometheus.yaml
              subPath: prometheus.yaml
            - name: tg
              mountPath: /etc/prometheus/tg_mcac.json
              subPath: tg_mcac.json
            - name: data
              mountPath: /prometheus
          ports:
            - containerPort: 9090
              name: http
      volumes:
        - name: cfg
          configMap:
            name: prometheus-config
        - name: tg
          configMap:
            name: prometheus-tg-mcac
        - name: data
          emptyDir: {{}}
"""
    svc = f"""apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: {NS}
  labels:
    app.kubernetes.io/name: prometheus
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: prometheus
  ports:
    - port: 9090
      targetPort: 9090
      name: http
"""
    return join_docs(cm_prom, cm_tg, dep, svc)


def grafana_stack() -> str:
    """Grafana: same provisioning as Compose — datasource file + dashboards.yaml + generated-dashboards/*.json."""
    grafana_dir = DASHBOARDS / "grafana"
    ds_default = textwrap.dedent(
        """\
        apiVersion: 1
        datasources:
          - name: prometheus
            uid: prometheus
            type: prometheus
            url: http://prometheus:9090
            access: proxy
            editable: true
            isDefault: true
        """
    )
    ds_path = grafana_dir / "prometheus-datasource.yaml"
    ds_text = read_text(ds_path) or ds_default
    dash_prov_path = grafana_dir / "dashboards.yaml"
    dash_prov_default = textwrap.dedent(
        """\
        apiVersion: 1
        providers:
          - name: 'default'
            orgId: 1
            type: file
            editable: true
            options:
              path: /var/lib/grafana/dashboards
        """
    )
    dash_prov_text = read_text(dash_prov_path) or dash_prov_default
    json_dir = grafana_dir / "generated-dashboards"
    dash_json: dict[str, str] = {}
    if json_dir.is_dir():
        for p in sorted(json_dir.glob("*.json")):
            t = read_text(p)
            if t:
                dash_json[p.name] = t
    if not dash_json:
        dash_json["README.json"] = (
            '{"title":"No generated-dashboards JSON — run dashboards/grafana/make-dashboards.sh",'
            '"panels":[],"schemaVersion":39}\n'
        )

    cm_ds = configmap(
        "grafana-datasources",
        "observability",
        {"prometheus-datasource.yaml": ds_text},
    )
    cm_prov = configmap(
        "grafana-dashboard-provider",
        "observability",
        {"dashboards.yaml": dash_prov_text},
    )
    dash_cm_docs: list[str] = []
    proj_sources: list[str] = []
    for fname in sorted(dash_json.keys()):
        content = dash_json[fname]
        cm_name = grafana_dash_cm_name(fname)
        dash_cm_docs.append(configmap(cm_name, "observability", {fname: content}))
        k = json.dumps(fname)
        proj_sources.append(
            f"""              - configMap:
                  name: {cm_name}
                  items:
                    - key: {k}
                      path: {k}"""
        )
    projected_sources_yaml = "\n".join(proj_sources)
    dep = f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  namespace: {NS}
  labels:
    demo-hub.io/group: observability
    app.kubernetes.io/name: grafana
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: grafana
  template:
    metadata:
      labels:
        app.kubernetes.io/name: grafana
        demo-hub.io/group: observability
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:11.4.0
          imagePullPolicy: IfNotPresent
          env:
            - name: GF_INSTALL_PLUGINS
              value: grafana-polystat-panel
            - name: GF_AUTH_ANONYMOUS_ENABLED
              value: "true"
            - name: GF_AUTH_ANONYMOUS_ORG_ROLE
              value: Admin
          volumeMounts:
            - name: grafana-ds
              mountPath: /etc/grafana/provisioning/datasources/prometheus-datasource.yaml
              subPath: prometheus-datasource.yaml
            - name: grafana-dash-prov
              mountPath: /etc/grafana/provisioning/dashboards/dashboards.yaml
              subPath: dashboards.yaml
            - name: grafana-dash-json
              mountPath: /var/lib/grafana/dashboards
          ports:
            - containerPort: 3000
              name: http
      volumes:
        - name: grafana-ds
          configMap:
            name: grafana-datasources
        - name: grafana-dash-prov
          configMap:
            name: grafana-dashboard-provider
        - name: grafana-dash-json
          projected:
            defaultMode: 420
            sources:
{projected_sources_yaml}
"""
    svc = f"""apiVersion: v1
kind: Service
metadata:
  name: grafana
  namespace: {NS}
  labels:
    app.kubernetes.io/name: grafana
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: grafana
  ports:
    - port: 3000
      targetPort: 3000
      name: http
"""
    return join_docs(cm_ds, cm_prov, *dash_cm_docs, dep, svc)


def zookeeper_kafka() -> str:
    # Bitnami Legacy (Apache Kafka + ZooKeeper): Confluent cp-kafka/cp-zookeeper often CrashLoop on K8s
    # (log dirs / permissions / image quirks). Same bootstrap address: kafka:9092.
    # Images: https://hub.docker.com/u/bitnamilegacy — UID 1001, data under /bitnami/*
    zk = deployment(
        "zookeeper",
        "kafka",
        "docker.io/bitnamilegacy/zookeeper:3.9.3-debian-12-r0",
        1,
        [(2181, "client")],
        [
            ("ZOO_PORT_NUMBER", "2181"),
            ("ZOO_TICK_TIME", "2000"),
            # Bitnami: ZOO_ENABLE_AUTH does *not* mean "disable auth" — without this, setup exits with ERROR.
            ("ALLOW_ANONYMOUS_LOGIN", "yes"),
            # Ensures peer/client listeners work when the chart expects broader binding (dev/demo).
            ("ZOO_LISTEN_ALLIPS_ENABLED", "yes"),
        ],
        data_mount="/bitnami/zookeeper",
        fs_group=1001,
        strategy_recreate=True,
        readiness_probe=tcp_readiness_probe(2181, initial_delay=15, period=5, failure_threshold=18),
    )
    # Single PLAINTEXT listener on 9092 (matches kafka-connect / hub-demo-ui / exporters).
    kafka_env = [
        ("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181"),
        ("ALLOW_PLAINTEXT_LISTENER", "yes"),
        ("KAFKA_CFG_LISTENERS", "PLAINTEXT://:9092"),
        ("KAFKA_CFG_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092"),
        ("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT"),
        ("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"),
        ("KAFKA_CFG_BROKER_ID", "1"),
        ("KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
        ("KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
        ("KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
        ("KAFKA_CFG_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_CFG_MESSAGE_MAX_BYTES", "33554432"),
        ("KAFKA_CFG_REPLICA_FETCH_MAX_BYTES", "33554432"),
        ("KAFKA_CFG_SOCKET_REQUEST_MAX_BYTES", "33554432"),
        ("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m"),
    ]
    kf = deployment(
        "kafka",
        "kafka",
        "docker.io/bitnamilegacy/kafka:3.7.1-debian-12-r0",
        1,
        [(9092, "broker")],
        kafka_env,
        resources="""          resources:
            requests:
              memory: "1Gi"
            limits:
              memory: "2Gi"
""",
        init_before_containers=_kafka_wait_zk(),
        data_mount="/bitnami/kafka",
        fs_group=1001,
        strategy_recreate=True,
        readiness_probe=tcp_readiness_probe(9092, initial_delay=45, period=10, failure_threshold=24),
    )
    return join_docs(zk, kf)


def postgres_ha() -> str:
    pg_secret = [
        ("POSTGRESQL_REPLICATION_PASSWORD", SECRET_NAME, SK_POSTGRESQL_REPLICATION_PASSWORD),
        ("POSTGRESQL_PASSWORD", SECRET_NAME, SK_POSTGRESQL_PASSWORD),
    ]
    primary = [
        ("POSTGRESQL_REPLICATION_MODE", "master"),
        ("POSTGRESQL_REPLICATION_USER", "replicator"),
        ("POSTGRESQL_USERNAME", "postgres"),
        ("POSTGRESQL_DATABASE", "demo"),
        ("POSTGRESQL_SHARED_PRELOAD_LIBRARIES", "pgaudit,pg_stat_statements"),
        (
            "POSTGRESQL_EXTRA_FLAGS",
            "-c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 "
            "-c pg_stat_statements.max=10000 -c pg_stat_statements.track=all",
        ),
    ]
    docs = [
        deployment(
            "postgresql-primary",
            "postgres-ha",
            POSTGRESQL_IMAGE,
            1,
            [(5432, "pg")],
            primary,
            data_mount="/bitnami/postgresql",
            env_secret_refs=pg_secret,
        )
    ]
    rep_base = [
        ("POSTGRESQL_REPLICATION_MODE", "slave"),
        ("POSTGRESQL_REPLICATION_USER", "replicator"),
        ("POSTGRESQL_MASTER_HOST", "postgresql-primary"),
        ("POSTGRESQL_MASTER_PORT_NUMBER", "5432"),
        ("POSTGRESQL_SHARED_PRELOAD_LIBRARIES", "pgaudit,pg_stat_statements"),
    ]
    for slot, rname in ((1, "postgresql-replica-1"), (2, "postgresql-replica-2")):
        env = rep_base + [
            (
                "POSTGRESQL_EXTRA_FLAGS",
                f"-c primary_slot_name=pgdemo_phys_replica_{slot} "
                f"-c pg_stat_statements.max=10000 -c pg_stat_statements.track=all",
            ),
        ]
        docs.append(
            deployment(
                rname,
                "postgres-ha",
                POSTGRESQL_IMAGE,
                1,
                [(5432, "pg")],
                env,
                data_mount="/bitnami/postgresql",
                env_secret_refs=pg_secret,
            )
        )
    return join_docs(*docs)


def redis_stack() -> str:
    r = deployment(
        "redis",
        "redis",
        "redis:7.4-alpine",
        1,
        [(6379, "redis")],
        [],
        command=["sh", "-c", 'exec redis-server --appendonly yes --requirepass "$REDIS_PASSWORD"'],
        data_mount="/data",
        env_secret_refs=[("REDIS_PASSWORD", SECRET_NAME, SK_REDIS_PASSWORD)],
    )
    rx = deployment(
        "redis-exporter",
        "redis",
        "oliver006/redis_exporter:v1.62.0",
        1,
        [(9121, "metrics")],
        [("REDIS_ADDR", "redis:6379")],
        env_secret_refs=[("REDIS_PASSWORD", SECRET_NAME, SK_REDIS_PASSWORD)],
    )
    return join_docs(r, rx)


def _mongo_config_deployments() -> str:
    cfg_cmd = ["mongod", "--port", "27017", "--configsvr", "--replSet", "configReplSet", "--bind_ip_all"]
    docs: list[str] = []
    for i in (1, 2, 3):
        docs.append(
            deployment(
                f"mongo-config{i}",
                "mongo-config-servers",
                "mongo:7.0",
                1,
                [(27017, "mongo")],
                [],
                command=cfg_cmd,
                extra_labels={"demo-hub.io/mongo-role": "configsvr"},
                data_mount="/data/db",
            )
        )
    return join_docs(*docs)


def _mongo_shard_deployments() -> str:
    docs: list[str] = []
    for svc, rs in (
        ("mongo-shard-tic", "tic"),
        ("mongo-shard-tac", "tac"),
        ("mongo-shard-toe", "toe"),
    ):
        cmd = ["mongod", "--port", "27017", "--shardsvr", "--replSet", rs, "--bind_ip_all"]
        docs.append(
            deployment(
                svc,
                "mongo-shards",
                "mongo:7.0",
                1,
                [(27017, "mongo")],
                [],
                command=cmd,
                extra_labels={"demo-hub.io/mongo-role": "shardsvr", "demo-hub.io/shard-rs": rs},
                data_mount="/data/db",
            )
        )
    return join_docs(*docs)


def _mongo_mongos_deployments() -> str:
    cfg = "configReplSet/mongo-config1:27017,mongo-config2:27017,mongo-config3:27017"
    docs: list[str] = []
    for i in (1, 2, 3):
        docs.append(
            deployment(
                f"mongo-mongos{i}",
                "mongo-mongos",
                "mongo:7.0",
                1,
                [(27017, "mongo")],
                [],
                command=["mongos", "--configdb", cfg, "--bind_ip_all"],
                extra_labels={"demo-hub.io/mongo-role": "mongos"},
            )
        )
    return join_docs(*docs)


def mongo_sharded_all_deployments() -> str:
    """Single file: all mongo workloads (Compose applies with depends_on; K8s uses Jobs + waits)."""
    return join_docs(_mongo_config_deployments(), _mongo_shard_deployments(), _mongo_mongos_deployments())


def postgres_bootstrap_job() -> str:
    """Bitnami primary: same SQL/slot flow as docker-entrypoint-initdb.d in Compose."""
    sql01 = read_repo("postgres-kafka/01-init-debezium.sql")
    sql_slots = read_repo("postgres-kafka/ensure-physical-replication-slots.sql")
    sql04 = read_repo("postgres-kafka/04-scenario-hub-schema-indexes.sql")
    if not sql01 or not sql_slots or not sql04:
        placeholder = "# MISSING: check out repo files under dashboards/demo/postgres-kafka/\n"
        sql01 = sql01 or placeholder
        sql_slots = sql_slots or placeholder
        sql04 = sql04 or placeholder
    bootstrap = f"""#!/usr/bin/env bash
set -euo pipefail
export PGHOST=postgresql-primary
export PGPORT=5432
until pg_isready -h "$PGHOST" -p "$PGPORT" -U postgres; do sleep 3; done
export PGPASSWORD=postgres
psql -h "$PGHOST" -U postgres -d demo -v ON_ERROR_STOP=1 -f /scripts/01-init-debezium.sql
export PGPASSWORD=replicatorpass
psql -h "$PGHOST" -U replicator -d postgres -v ON_ERROR_STOP=1 -f /scripts/ensure-physical-replication-slots.sql
export PGPASSWORD=postgres
psql -h "$PGHOST" -U postgres -d postgres -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
psql -h "$PGHOST" -U postgres -d demo -v ON_ERROR_STOP=1 -f /scripts/04-scenario-hub-schema-indexes.sql
echo "postgres bootstrap done."
"""
    cm = configmap(
        "postgres-bootstrap-sql",
        "postgres-ha",
        {
            "bootstrap.sh": bootstrap,
            "01-init-debezium.sql": sql01,
            "ensure-physical-replication-slots.sql": sql_slots,
            "04-scenario-hub-schema-indexes.sql": sql04,
        },
    )
    job = batch_job(
        "postgres-demo-bootstrap",
        "postgres-ha",
        POSTGRESQL_IMAGE,
        ["/bin/bash", "/scripts/bootstrap.sh"],
        "postgres-bootstrap-sql",
    )
    return join_docs(cm, job)


def cassandra_schema_job() -> str:
    """RF=3 keyspace to mirror a 3-node ring (Compose TWCS lab uses RF=1; hub UI uses demo_hub)."""
    cql = textwrap.dedent(
        """\
        CREATE KEYSPACE IF NOT EXISTS demo_hub
          WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

        -- Minimal table for hub path checks (scenario may add more)
        CREATE TABLE IF NOT EXISTS demo_hub.hub_orders_placeholder (
          id uuid PRIMARY KEY,
          created_at timestamp
        );
        """
    )
    cql_host = f"cassandra-0.cassandra-headless.{NS}.svc.cluster.local"
    bootstrap = f"""#!/usr/bin/env bash
set -euo pipefail
CQL_HOST="{cql_host}"
for i in $(seq 1 120); do
  if cqlsh "$CQL_HOST" 9042 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; then
    break
  fi
  sleep 3
done
exec cqlsh "$CQL_HOST" 9042 -f /scripts/schema.cql
"""
    cm = configmap(
        "cassandra-schema-sql",
        "cassandra-ring",
        {"schema.cql": cql, "bootstrap.sh": bootstrap},
    )
    job = batch_job(
        "cassandra-demo-schema",
        "cassandra-ring",
        "cassandra:4.0",
        ["/bin/bash", "/scripts/bootstrap.sh"],
        "cassandra-schema-sql",
    )
    return join_docs(cm, job)


def mongo_sharded_scripts_and_jobs() -> str:
    """Same order as Compose: config RS → shard RS → add shards → prepare (single Job chain)."""
    icfg = read_repo("mongo-sharded/init-config-replica-set.sh")
    ishard = read_repo("mongo-sharded/init-shard-replica-sets.sh")
    iadd = read_repo("mongo-sharded/add-shards.sh")
    prep = read_repo("mongo-kafka/prepare-demo-collections.sh")
    idx = read_repo("mongo-kafka/demo-indexes.js")
    if not icfg:
        icfg = 'echo "missing init-config-replica-set.sh"\nexit 1\n'
    if not ishard:
        ishard = 'echo "missing init-shard-replica-sets.sh"\nexit 1\n'
    if not iadd:
        iadd = 'echo "missing add-shards.sh"\nexit 1\n'
    if not prep:
        prep = 'echo "missing prepare-demo-collections.sh"\nexit 1\n'
    if not idx:
        idx = "// empty\n"
    chain = """#!/usr/bin/env bash
set -euo pipefail
bash /scripts/init-config-replica-set.sh
bash /scripts/init-shard-replica-sets.sh
bash /scripts/add-shards.sh
export MONGOS_URI=mongodb://mongo-mongos1:27017
bash /scripts/prepare-demo-collections.sh
echo "mongo sharded bootstrap done."
"""
    cm = configmap(
        "mongo-sharded-scripts",
        "mongo-sharded",
        {
            "chain.sh": chain,
            "init-config-replica-set.sh": icfg,
            "init-shard-replica-sets.sh": ishard,
            "add-shards.sh": iadd,
            "prepare-demo-collections.sh": prep,
            "demo-indexes.js": idx,
        },
    )
    job = batch_job(
        "mongo-demo-bootstrap",
        "mongo-sharded",
        "mongo:7.0",
        ["bash", "/scripts/chain.sh"],
        "mongo-sharded-scripts",
        active_deadline_seconds=7200,
    )
    return join_docs(cm, job)


def kafka_connect() -> str:
    env = [
        ("BOOTSTRAP_SERVERS", "kafka:9092"),
        ("GROUP_ID", "pgdemo-connect-cluster"),
        ("CONFIG_STORAGE_TOPIC", "pgdemo_connect_configs"),
        ("OFFSET_STORAGE_TOPIC", "pgdemo_connect_offsets"),
        ("STATUS_STORAGE_TOPIC", "pgdemo_connect_statuses"),
        ("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter"),
        ("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter"),
        ("KAFKA_HEAP_OPTS", "-Xms512m -Xmx8192m"),
    ]
    return deployment(
        "kafka-connect",
        "kafka-connect",
        "mcac-demo/kafka-connect:2.7.3-mongo-sink",
        1,
        [(8083, "http")],
        env,
        resources="""          resources:
            requests:
              memory: "1Gi"
            limits:
              memory: "10Gi"
""",
    )


def exporters() -> str:
    # Primary: use Bitnami superuser so pg_replication_slots / physical slots appear in exporter
    # (role "demo" cannot read physical slots per PostgreSQL visibility rules).
    def pe(host: str, name: str, *, user: str, pwd_key: str) -> str:
        return deployment(
            name,
            "exporters",
            "prometheuscommunity/postgres-exporter:v0.17.1",
            1,
            [(9187, "metrics")],
            [
                ("DATA_SOURCE_URI", f"{host}:5432/demo?sslmode=disable"),
                ("DATA_SOURCE_USER", user),
            ],
            env_secret_refs=[("DATA_SOURCE_PASS", SECRET_NAME, pwd_key)],
        )

    parts = [
        pe("postgresql-primary", "postgres-exporter-primary", user="postgres", pwd_key=SK_POSTGRESQL_PASSWORD),
        pe("postgresql-replica-1", "postgres-exporter-replica-1", user="demo", pwd_key=SK_DEMO_USER_PASSWORD),
        pe("postgresql-replica-2", "postgres-exporter-replica-2", user="demo", pwd_key=SK_DEMO_USER_PASSWORD),
        deployment(
            "kafka-exporter-mcac",
            "exporters",
            "danielqsj/kafka-exporter:v1.7.0",
            1,
            [(9308, "metrics")],
            [],
            args=["--kafka.server=kafka:9092"],
            init_before_containers=_kafka_wait_broker(),
            strategy_recreate=True,
        ),
        deployment(
            "mongodb-exporter",
            "exporters",
            "percona/mongodb_exporter:0.40",
            1,
            [(9216, "metrics")],
            [("MONGODB_URI", "mongodb://mongo-mongos1:27017")],
            args=["--collect-all", "--compatible-mode"],
        ),
    ]
    return join_docs(*parts)


def nodetool_stress() -> str:
    fq = f"cassandra-0.cassandra-headless.{NS}.svc.cluster.local,cassandra-1.cassandra-headless.{NS}.svc.cluster.local,cassandra-2.cassandra-headless.{NS}.svc.cluster.local"
    nt = deployment(
        "nodetool-exporter",
        "cassandra-ring",
        "demo-hub/nodetool-exporter:latest",
        1,
        [(9104, "metrics")],
        [("CASSANDRA_HOSTS", fq), ("NODETOOL_EXPORTER_PORT", "9104")],
    )
    st = deployment(
        "stress",
        "cassandra-ring",
        "thelastpickle/tlp-stress:latest",
        1,
        [(9500, "http")],
        [("TLP_STRESS_CASSANDRA_HOST", "cassandra")],
        args=["run", "KeyValue", "--rate", "30", "-d", "1d", "-r", ".8"],
    )
    return join_docs(nt, st)


def hub_demo_ui() -> str:
    return deployment(
        "hub-demo-ui",
        "hub-demo-ui",
        "mcac-demo/hub-demo-ui:latest",
        1,
        [(8888, "http")],
        [
            ("MONGO_URI", "mongodb://mongo-mongos1:27017"),
            ("CASSANDRA_HOSTS", cassandra_headless_contact_points()),
            ("CASSANDRA_KEYSPACE", "demo_hub"),
            ("OPENSEARCH_URL", "http://opensearch:9200"),
            ("OPENSEARCH_INDEX", "hub-orders"),
            ("KAFKA_BOOTSTRAP", "kafka:9092"),
            ("CASSANDRA_WORKLOAD_REQUEST_TIMEOUT_SECONDS", "300"),
            ("CASSANDRA_WORKLOAD_INTER_BATCH_SLEEP_MS", "25"),
            ("CASSANDRA_WORKLOAD_WRITE_RETRIES", "3"),
        ],
        init_before_containers=_hub_wait_upstream(),
        strategy_recreate=True,
        env_secret_refs=[
            ("POSTGRES_DSN", SECRET_NAME, SK_HUB_POSTGRES_DSN),
            ("REDIS_URL", SECRET_NAME, SK_HUB_REDIS_URL),
        ],
    )


def jobs_readme() -> str:
    return f"""# Data-plane bootstrap (same semantics as Docker Compose)

Generated Jobs (apply **after** workloads are running):

| Job | Purpose |
|-----|---------|
| **postgres-demo-bootstrap** | Debezium user/table/publication + physical replication slots + scenario schema (mirrors `postgres-kafka/*.sql` init). |
| **cassandra-demo-schema** | `demo_hub` keyspace with **RF=3** + placeholder table (ring replication). |
| **mongo-demo-bootstrap** | Config RS → shard RS → addShard → sharded collections (mirrors `mongo-sharded/*.sh` + `prepare-demo-collections.sh`). |

Re-run: `kubectl delete job -n {NS} <name>` then `kubectl apply -f …` again.

Not generated here: **kafka-connect-register** (use host script against `kafka-connect:8083`). MCAC agent JAR is populated by StatefulSet **initContainer** (`mcac-copy-agent`); build **`mcac-demo/mcac-init:local`** from the repo-root **Dockerfile** (`k8s/scripts/build-mcac-init-image.sh`).

See **../scripts/apply-data-bootstrap.sh**.
"""


def copy_namespace() -> str:
    p = K8S_ROOT / "namespace.yaml"
    return p.read_text(encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    bundles: list[tuple[str, str]] = [
        ("00-namespace.yaml", copy_namespace()),
        ("01-demo-hub-credentials.yaml", demo_hub_credentials_secret()),
        ("10-observability-prometheus-grafana.yaml", join_docs(prometheus_stack(), grafana_stack())),
        ("20-zookeeper-kafka.yaml", zookeeper_kafka()),
        ("30-cassandra-ring.yaml", cassandra_statefulset()),
        ("35-cassandra-schema-job.yaml", cassandra_schema_job()),
        ("40-postgresql-ha.yaml", postgres_ha()),
        ("45-postgres-bootstrap-job.yaml", postgres_bootstrap_job()),
        ("50-redis.yaml", redis_stack()),
        ("60-mongo-sharded.yaml", mongo_sharded_all_deployments()),
        ("61-mongo-bootstrap-job.yaml", mongo_sharded_scripts_and_jobs()),
        ("70-kafka-connect.yaml", kafka_connect()),
        ("80-exporters.yaml", exporters()),
        ("90-opensearch.yaml", opensearch_stack()),
        ("95-hub-demo-ui.yaml", hub_demo_ui()),
        ("96-kubernetes-ops.yaml", kubernetes_ops_extras()),
        ("98-nodetool-stress.yaml", nodetool_stress()),
    ]
    all_parts: list[str] = []
    for fname, body in bundles:
        (OUT / fname).write_text(body, encoding="utf-8")
        all_parts.append(body)
        print((OUT / fname).relative_to(DEMO))
    (OUT / "README-jobs.md").write_text(jobs_readme(), encoding="utf-8")
    (OUT / "all.yaml").write_text("\n---\n\n".join(p.strip() for p in all_parts if p.strip()) + "\n", encoding="utf-8")
    print((OUT / "all.yaml").relative_to(DEMO))


if __name__ == "__main__":
    main()
