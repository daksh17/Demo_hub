#!/usr/bin/env python3
"""Emit Pod + Service manifests under deploy/k8s/pods/ and deploy/k8s/services/ (legacy skeleton).

For Deployments / StatefulSets / ConfigMaps aligned with docker-compose.yml, use
**gen_demo_hub_k8s.py** (writes **deploy/k8s/generated/**).
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _postgresql_image_from_generator() -> str:
    """Same pin as gen_demo_hub_k8s.POSTGRESQL_IMAGE (Bitnami + repmgr)."""
    path = Path(__file__).resolve().with_name("gen_demo_hub_k8s.py")
    spec = importlib.util.spec_from_file_location("_gen_demo_hub_k8s", path)
    mod = importlib.util.module_from_spec(spec)
    loader = spec.loader
    assert loader is not None
    loader.exec_module(mod)
    return str(mod.POSTGRESQL_IMAGE)


POSTGRESQL_IMAGE = _postgresql_image_from_generator()
OUT = ROOT / "pods"
OUT_SVC = ROOT / "services"
# Align with gen_demo_hub_k8s.hub_demo_ui (headless per-pod DNS for the driver).
HUB_CASSANDRA_HOSTS_K8S = ",".join(
    f"cassandra-{i}.cassandra-headless.demo-hub.svc.cluster.local" for i in range(3)
)

# (compose_service_name, image, container_ports, optional_command, optional_args)
SPECS: list[tuple[str, str, list[int], list[str] | None, list[str] | None]] = [
    ("prometheus", "prom/prometheus:v2.17.1", [9090], None, None),
    ("grafana", "grafana/grafana:11.4.0", [3000], None, None),
    ("mcac", "busybox:1.36", [], ["sh", "-c", "echo replace-with-built-mcac-init; sleep 3600"], None),
    ("nodetool-exporter", "demo-hub/nodetool-exporter:latest", [9104], None, None),
    ("mongodb-exporter-local", "percona/mongodb_exporter:0.40", [9216], None, ["--collect-all", "--compatible-mode"]),
    ("cassandra", "cassandra:4.0", [9042, 7000, 9103, 9501], None, None),
    ("cassandra2", "cassandra:4.0", [9042, 7000, 9103, 9501], None, None),
    ("cassandra3", "cassandra:4.0", [9042, 7000, 9103, 9501], None, None),
    ("stress", "thelastpickle/tlp-stress:latest", [9500], ["run", "KeyValue", "--rate", "30", "-d", "1d", "-r", ".8"], None),
    ("zookeeper", "confluentinc/cp-zookeeper:7.6.1", [2181], None, None),
    ("kafka", "confluentinc/cp-kafka:7.6.1", [9092, 29092], None, None),
    ("postgresql-primary", POSTGRESQL_IMAGE, [5432], None, None),
    ("postgresql-replica-1", POSTGRESQL_IMAGE, [5432], None, None),
    ("postgresql-replica-2", POSTGRESQL_IMAGE, [5432], None, None),
    ("kafka-connect", "mcac-demo/kafka-connect:2.7.3-mongo-sink", [8083], None, None),
    ("kafka-connect-register", "alpine:3.19", [], ["sh", "-c", "echo use-Kubernetes-Job-for-one-shot"], None),
    ("postgres-exporter-primary", "prometheuscommunity/postgres-exporter:v0.17.1", [9187], None, None),
    ("postgres-exporter-replica-1", "prometheuscommunity/postgres-exporter:v0.17.1", [9187], None, None),
    ("postgres-exporter-replica-2", "prometheuscommunity/postgres-exporter:v0.17.1", [9187], None, None),
    ("kafka-exporter-mcac", "danielqsj/kafka-exporter:v1.7.0", [9308], None, ["--kafka.server=kafka:29092"]),
    ("mongo-config1", "mongo:7.0", [27017], None, None),
    ("mongo-config2", "mongo:7.0", [27017], None, None),
    ("mongo-config3", "mongo:7.0", [27017], None, None),
    ("mongo-config-init-rs", "mongo:7.0", [], ["bash", "-c", "echo use-Kubernetes-Job"], None),
    ("mongo-shard-tic", "mongo:7.0", [27017], None, None),
    ("mongo-shard-tac", "mongo:7.0", [27017], None, None),
    ("mongo-shard-toe", "mongo:7.0", [27017], None, None),
    ("mongo-shard-init-rs", "mongo:7.0", [], ["bash", "-c", "echo use-Kubernetes-Job"], None),
    ("mongo-mongos1", "mongo:7.0", [27017], None, None),
    ("mongo-mongos2", "mongo:7.0", [27017], None, None),
    ("mongo-mongos3", "mongo:7.0", [27017], None, None),
    ("mongo-shard-add", "mongo:7.0", [], ["bash", "-c", "echo use-Kubernetes-Job"], None),
    ("mongodb-exporter", "percona/mongodb_exporter:0.40", [9216], None, ["--collect-all", "--compatible-mode"]),
    ("mongo-kafka-prepare", "mongo:7.0", [], ["bash", "-c", "echo use-Kubernetes-Job"], None),
    ("redis", "redis:7.4-alpine", [6379], ["redis-server", "--appendonly", "yes", "--requirepass", "demoredispass"], None),
    ("redis-exporter", "oliver006/redis_exporter:v1.62.0", [9121], None, None),
    ("opensearch", "opensearchproject/opensearch:2.11.1", [9200, 9600], None, None),
    ("opensearch-dashboards", "opensearchproject/opensearch-dashboards:2.11.1", [5601], None, None),
    ("opensearch-exporter", "quay.io/prometheuscommunity/elasticsearch-exporter:v1.7.0", [7979], None, [
        "--es.uri=http://opensearch:9200",
        "--es.all",
        "--es.indices",
        "--es.timeout=30s",
    ]),
    ("hub-demo-ui", "mcac-demo/hub-demo-ui:latest", [8888], None, None),
]

ENV_BY_SERVICE: dict[str, list[tuple[str, str]]] = {
    "zookeeper": [
        ("ZOOKEEPER_CLIENT_PORT", "2181"),
        ("ZOOKEEPER_TICK_TIME", "2000"),
    ],
    "kafka": [
        ("KAFKA_BROKER_ID", "1"),
        ("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181"),
        ("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"),
        ("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://kafka:9092"),
        ("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092"),
        ("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_MESSAGE_MAX_BYTES", "33554432"),
        ("KAFKA_REPLICA_FETCH_MAX_BYTES", "33554432"),
        ("KAFKA_SOCKET_REQUEST_MAX_BYTES", "33554432"),
    ],
    "grafana": [
        ("GF_INSTALL_PLUGINS", "grafana-polystat-panel"),
        ("GF_AUTH_ANONYMOUS_ENABLED", "true"),
        ("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin"),
    ],
    "redis-exporter": [
        ("REDIS_ADDR", "redis:6379"),
        ("REDIS_PASSWORD", "demoredispass"),
    ],
    "opensearch-dashboards": [
        ("OPENSEARCH_HOSTS", '["http://opensearch:9200"]'),
        ("DISABLE_SECURITY_DASHBOARDS_PLUGIN", "true"),
    ],
    "postgres-exporter-primary": [
        ("DATA_SOURCE_URI", "postgresql-primary:5432/demo?sslmode=disable"),
        ("DATA_SOURCE_USER", "demo"),
        ("DATA_SOURCE_PASS", "demopass"),
    ],
    "postgres-exporter-replica-1": [
        ("DATA_SOURCE_URI", "postgresql-replica-1:5432/demo?sslmode=disable"),
        ("DATA_SOURCE_USER", "demo"),
        ("DATA_SOURCE_PASS", "demopass"),
    ],
    "postgres-exporter-replica-2": [
        ("DATA_SOURCE_URI", "postgresql-replica-2:5432/demo?sslmode=disable"),
        ("DATA_SOURCE_USER", "demo"),
        ("DATA_SOURCE_PASS", "demopass"),
    ],
    "mongodb-exporter": [
        ("MONGODB_URI", "mongodb://mongo-mongos1:27017"),
    ],
    "mongodb-exporter-local": [
        ("MONGODB_URI", "mongodb://mongo-mongos1:27017"),
    ],
    "stress": [
        ("TLP_STRESS_CASSANDRA_HOST", "cassandra"),
    ],
    "hub-demo-ui": [
        ("POSTGRES_DSN", "postgresql://demo:demopass@postgresql-primary:5432/demo"),
        ("MONGO_URI", "mongodb://mongo-mongos1:27017"),
        ("REDIS_URL", "redis://:demoredispass@redis:6379/0"),
        ("CASSANDRA_HOSTS", HUB_CASSANDRA_HOSTS_K8S),
        ("CASSANDRA_KEYSPACE", "demo_hub"),
        ("OPENSEARCH_URL", "http://opensearch:9200"),
        ("OPENSEARCH_INDEX", "hub-orders"),
        ("KAFKA_BOOTSTRAP", "kafka:29092"),
        ("CASSANDRA_WORKLOAD_REQUEST_TIMEOUT_SECONDS", "300"),
        ("CASSANDRA_WORKLOAD_INTER_BATCH_SLEEP_MS", "25"),
        ("CASSANDRA_WORKLOAD_WRITE_RETRIES", "3"),
    ],
}

OPENSEARCH_ENV: list[tuple[str, str]] = [
    ("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m -XX:MaxDirectMemorySize=256m"),
    ("DISABLE_SECURITY_PLUGIN", "true"),
    ("DISABLE_INSTALL_DEMO_CONFIG", "true"),
]

# WAL archive is configured on PostgreSQL Deployments via conf.d (see gen_demo_hub_k8s.postgres_ha), not here —
# POSTGRESQL_EXTRA_FLAGS is space-split by Bitnami and breaks archive_command.

POSTGRES_PRIMARY_ENV: list[tuple[str, str]] = [
    ("POSTGRESQL_REPLICATION_MODE", "master"),
    ("POSTGRESQL_REPLICATION_USER", "replicator"),
    ("POSTGRESQL_REPLICATION_PASSWORD", "replicatorpass"),
    ("POSTGRESQL_USERNAME", "postgres"),
    ("POSTGRESQL_PASSWORD", "postgres"),
    ("POSTGRESQL_DATABASE", "demo"),
    ("POSTGRESQL_SHARED_PRELOAD_LIBRARIES", "repmgr,pgaudit,pg_stat_statements"),
    (
        "POSTGRESQL_EXTRA_FLAGS",
        "-c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 "
        "-c pg_stat_statements.max=10000 -c pg_stat_statements.track=all",
    ),
]

POSTGRES_REPLICA_ENV: list[tuple[str, str]] = [
    ("POSTGRESQL_REPLICATION_MODE", "slave"),
    ("POSTGRESQL_REPLICATION_USER", "replicator"),
    ("POSTGRESQL_REPLICATION_PASSWORD", "replicatorpass"),
    ("POSTGRESQL_USERNAME", "postgres"),
    ("POSTGRESQL_PASSWORD", "postgres"),
    ("POSTGRESQL_SHARED_PRELOAD_LIBRARIES", "repmgr,pgaudit,pg_stat_statements"),
]


def container_name(service: str) -> str:
    return service.replace("_", "-")


def env_lines(pairs: list[tuple[str, str]]) -> list[str]:
    out: list[str] = ["      env:"]
    for k, v in pairs:
        if "." in k:
            continue
        out.append("        - name: " + k)
        out.append("          value: " + json.dumps(v))
    return out


def cassandra_resources() -> list[str]:
    return [
        "      resources:",
        "        requests:",
        "          memory: \"768Mi\"",
        "        limits:",
        "          memory: \"1536Mi\"",
    ]


def pod_yaml(
    name: str,
    image: str,
    ports: list[int],
    command: list[str] | None,
    args: list[str] | None,
    extra_env: list[tuple[str, str]],
    cassandra_mem: bool,
) -> str:
    cname = container_name(name)
    lines: list[str] = [
        "# Generated by deploy/k8s/scripts/gen_demo_hub_pods.py — see deploy/k8s/README.md",
        "apiVersion: v1",
        "kind: Pod",
        "metadata:",
        f"  name: {name}",
        "  namespace: demo-hub",
        "  labels:",
        f"    app.kubernetes.io/name: {name}",
        "    app.kubernetes.io/part-of: demo-hub",
        "spec:",
        "  containers:",
        f"    - name: {cname}",
        f"      image: {image}",
        "      imagePullPolicy: IfNotPresent",
    ]
    if command:
        lines.append("      command:")
        for c in command:
            lines.append(f"        - {json.dumps(c)}")
    if args:
        lines.append("      args:")
        for a in args:
            lines.append(f"        - {json.dumps(a)}")

    env_pairs = list(extra_env)
    if name == "opensearch":
        env_pairs.extend(OPENSEARCH_ENV)
    if name == "postgresql-primary":
        env_pairs.extend(POSTGRES_PRIMARY_ENV)
    if name == "postgresql-replica-1":
        env_pairs.extend(POSTGRES_REPLICA_ENV)
        env_pairs.extend(
            [
                ("POSTGRESQL_MASTER_HOST", "postgresql-primary"),
                ("POSTGRESQL_MASTER_PORT_NUMBER", "5432"),
            ]
        )
        env_pairs.append(
            (
                "POSTGRESQL_EXTRA_FLAGS",
                "-c wal_level=replica -c primary_slot_name=pgdemo_phys_replica_1 "
                "-c pg_stat_statements.max=10000 -c pg_stat_statements.track=all",
            )
        )
    if name == "postgresql-replica-2":
        env_pairs.extend(POSTGRES_REPLICA_ENV)
        env_pairs.extend(
            [
                ("POSTGRESQL_MASTER_HOST", "postgresql-primary"),
                ("POSTGRESQL_MASTER_PORT_NUMBER", "5432"),
            ]
        )
        env_pairs.append(
            (
                "POSTGRESQL_EXTRA_FLAGS",
                "-c wal_level=replica -c primary_slot_name=pgdemo_phys_replica_2 "
                "-c pg_stat_statements.max=10000 -c pg_stat_statements.track=all",
            )
        )

    if env_pairs:
        lines.extend(env_lines(env_pairs))

    if cassandra_mem:
        lines.extend(cassandra_resources())

    if ports:
        lines.append("      ports:")
        for p in ports:
            lines.append(f"        - containerPort: {p}")
    return "\n".join(lines) + "\n"


def service_yaml(name: str, ports: list[int], for_bundle: bool = False) -> str:
    if not ports:
        return ""
    plines = []
    for p in ports:
        plines.append(f"    - port: {p}\n      targetPort: {p}\n      name: p-{p}")
    ports_block = "\n".join(plines)
    suffix = "\n---\n" if for_bundle else "\n"
    return (
        f"# Generated — ClusterIP ({name})\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n"
        f"  name: {name}\n  namespace: demo-hub\n  labels:\n    app.kubernetes.io/name: {name}\n"
        "spec:\n  type: ClusterIP\n  selector:\n"
        f"    app.kubernetes.io/name: {name}\n  ports:\n{ports_block}{suffix}"
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    OUT_SVC.mkdir(parents=True, exist_ok=True)

    combined_svc: list[str] = []
    for name, image, ports, cmd, args in SPECS:
        extra = ENV_BY_SERVICE.get(name, [])
        cassandra_mem = name in ("cassandra", "cassandra2", "cassandra3")

        body = pod_yaml(name, image, ports, cmd, args, extra, cassandra_mem)
        rel = OUT / f"pod-{name}.yaml"
        rel.write_text(body, encoding="utf-8")
        print(rel.relative_to(ROOT.parent))

        sy = service_yaml(name, ports, for_bundle=False)
        sy_bundle = service_yaml(name, ports, for_bundle=True)
        if sy:
            combined_svc.append(sy_bundle)
            (OUT_SVC / f"svc-{name}.yaml").write_text(sy, encoding="utf-8")

    (OUT_SVC / "all-services.yaml").write_text("\n".join(combined_svc).rstrip() + "\n", encoding="utf-8")
    print((OUT_SVC / "all-services.yaml").relative_to(ROOT.parent))
    print(f"Wrote {len(SPECS)} pods + services under {OUT_SVC}")


if __name__ == "__main__":
    main()
