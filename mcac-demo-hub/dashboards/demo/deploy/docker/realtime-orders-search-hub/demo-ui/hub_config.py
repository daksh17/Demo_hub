"""
Optional hub connection overrides (browser session). Defaults always come from environment.
"""
from __future__ import annotations

import os
import re
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

_KEYSPACE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,47}$")


def keyspace_valid(s: str) -> bool:
    return bool(_KEYSPACE_RE.match(s.strip()))

# Session keys (Starlette session dict)
SK_POSTGRES_DSN = "hub_pg_dsn"
SK_MONGO_URI = "hub_mongo_uri"
SK_REDIS_URL = "hub_redis_url"
SK_OS_URL = "hub_os_url"
SK_CASSANDRA_HOSTS = "hub_cassandra_hosts"
SK_CASSANDRA_KEYSPACE = "hub_cassandra_keyspace"
SK_OS_INDEX = "hub_os_index"
SK_OS_WORKLOAD_INDEX = "hub_os_workload_index"
SK_SCENARIO_OS_INDEX = "hub_scenario_os_index"


def _env_cassandra_hosts() -> tuple[str, ...]:
    raw = os.environ.get("CASSANDRA_HOSTS", "cassandra")
    if not (raw or "").strip():
        raw = "cassandra"
    return tuple(h.strip() for h in raw.split(",") if h.strip())


@dataclass(frozen=True)
class HubRuntimeConfig:
    postgres_dsn: str
    """Application user DSN for ``demo`` on primary (inserts, row counts)."""
    postgres_admin_dsn: str
    """Superuser DSN on primary (logical publisher DDL)."""
    postgres_replica_read_dsn: str
    """Optional: primary standby ``demo_logical_pub`` — publisher lag/compare."""
    postgres_logical_sub_dsn: str
    """Application ``demo`` DSN base on logical subscriber pod (``postgres-sub`` in K8s)."""
    postgres_logical_sub_admin_dsn: str
    """Superuser DSN on logical subscriber pod."""
    mongo_uri: str
    redis_url: str
    opensearch_url: str
    cassandra_hosts: tuple[str, ...]
    cassandra_keyspace: str
    opensearch_index: str
    opensearch_workload_index: str
    scenario_opensearch_index: str
    workload_redis_prefix: str
    trino_http_url: str
    """Trino coordinator HTTP base (e.g. ``http://trino:8080``). Empty disables hub SQL runner."""

    @classmethod
    def from_env(cls) -> HubRuntimeConfig:
        pg_dsn = os.environ.get(
            "POSTGRES_DSN",
            "postgresql://demo:demopass@postgresql-primary:5432/demo",
        )
        pg_admin = os.environ.get(
            "POSTGRES_ADMIN_DSN",
            "postgresql://postgres:postgres@postgresql-primary:5432/postgres",
        )
        sub_dsn = os.environ.get("POSTGRES_LOGICAL_SUB_DSN", "").strip()
        sub_admin = os.environ.get("POSTGRES_LOGICAL_SUB_ADMIN_DSN", "").strip()
        return cls(
            postgres_dsn=pg_dsn,
            postgres_admin_dsn=pg_admin,
            postgres_replica_read_dsn=os.environ.get(
                "POSTGRES_REPLICA_READ_DSN", ""
            ).strip(),
            postgres_logical_sub_dsn=sub_dsn or pg_dsn,
            postgres_logical_sub_admin_dsn=sub_admin or pg_admin,
            mongo_uri=os.environ.get("MONGO_URI", "mongodb://mongo-mongos1:27017"),
            redis_url=os.environ.get(
                "REDIS_URL", "redis://:demoredispass@redis:6379/0"
            ),
            opensearch_url=os.environ.get(
                "OPENSEARCH_URL", "http://opensearch:9200"
            ).rstrip("/"),
            cassandra_hosts=_env_cassandra_hosts(),
            cassandra_keyspace=os.environ.get("CASSANDRA_KEYSPACE", "demo_hub"),
            opensearch_index=os.environ.get("OPENSEARCH_INDEX", "hub-orders"),
            opensearch_workload_index=os.environ.get(
                "OPENSEARCH_WORKLOAD_INDEX", "hub-workload"
            ),
            scenario_opensearch_index=os.environ.get(
                "SCENARIO_PIPELINE_OS_INDEX", "hub-scenario-pipeline"
            ),
            workload_redis_prefix=os.environ.get(
                "WORKLOAD_REDIS_PREFIX", "hub:wl:"
            ),
            trino_http_url=os.environ.get("TRINO_HTTP", "").strip(),
        )

    def merge_session(self, session: dict[str, Any]) -> HubRuntimeConfig:
        def pick(key: str, cur: str) -> str:
            v = session.get(key)
            if v is None:
                return cur
            if isinstance(v, str) and not v.strip():
                return cur
            return str(v).strip()

        def pick_hosts(cur: tuple[str, ...]) -> tuple[str, ...]:
            v = session.get(SK_CASSANDRA_HOSTS)
            if v is None or (isinstance(v, str) and not v.strip()):
                return cur
            hosts = tuple(
                h.strip() for h in str(v).split(",") if h.strip()
            )
            return hosts if hosts else cur

        def pick_ks(cur: str) -> str:
            v = session.get(SK_CASSANDRA_KEYSPACE)
            if v is None or (isinstance(v, str) and not v.strip()):
                return cur
            s = str(v).strip()
            if not _KEYSPACE_RE.match(s):
                return cur
            return s

        return HubRuntimeConfig(
            postgres_dsn=pick(SK_POSTGRES_DSN, self.postgres_dsn),
            postgres_admin_dsn=self.postgres_admin_dsn,
            postgres_replica_read_dsn=self.postgres_replica_read_dsn,
            postgres_logical_sub_dsn=self.postgres_logical_sub_dsn,
            postgres_logical_sub_admin_dsn=self.postgres_logical_sub_admin_dsn,
            mongo_uri=pick(SK_MONGO_URI, self.mongo_uri),
            redis_url=pick(SK_REDIS_URL, self.redis_url),
            opensearch_url=pick(SK_OS_URL, self.opensearch_url).rstrip("/"),
            cassandra_hosts=pick_hosts(self.cassandra_hosts),
            cassandra_keyspace=pick_ks(self.cassandra_keyspace),
            opensearch_index=pick(SK_OS_INDEX, self.opensearch_index),
            opensearch_workload_index=pick(
                SK_OS_WORKLOAD_INDEX, self.opensearch_workload_index
            ),
            scenario_opensearch_index=pick(
                SK_SCENARIO_OS_INDEX, self.scenario_opensearch_index
            ),
            workload_redis_prefix=self.workload_redis_prefix,
            trino_http_url=self.trino_http_url,
        )

    def is_default_cassandra(self, env_base: HubRuntimeConfig) -> bool:
        return (
            self.cassandra_hosts == env_base.cassandra_hosts
            and self.cassandra_keyspace == env_base.cassandra_keyspace
        )

    def session_overrides_only(self, env_base: HubRuntimeConfig) -> dict[str, bool]:
        """Which fields differ from env defaults (for UI status)."""
        return {
            "postgres_dsn": self.postgres_dsn != env_base.postgres_dsn,
            "mongo_uri": self.mongo_uri != env_base.mongo_uri,
            "redis_url": self.redis_url != env_base.redis_url,
            "opensearch_url": self.opensearch_url != env_base.opensearch_url,
            "cassandra_hosts": self.cassandra_hosts != env_base.cassandra_hosts,
            "cassandra_keyspace": self.cassandra_keyspace
            != env_base.cassandra_keyspace,
            "opensearch_index": self.opensearch_index != env_base.opensearch_index,
            "opensearch_workload_index": self.opensearch_workload_index
            != env_base.opensearch_workload_index,
            "scenario_opensearch_index": self.scenario_opensearch_index
            != env_base.scenario_opensearch_index,
        }


_ENV_BASE: HubRuntimeConfig | None = None
_ctx: ContextVar[HubRuntimeConfig | None] = ContextVar(
    "hub_runtime_config", default=None
)


def env_base_config() -> HubRuntimeConfig:
    global _ENV_BASE
    if _ENV_BASE is None:
        _ENV_BASE = HubRuntimeConfig.from_env()
    return _ENV_BASE


def get_runtime_config() -> HubRuntimeConfig:
    c = _ctx.get()
    if c is not None:
        return c
    return env_base_config()


def set_runtime_config_token(cfg: HubRuntimeConfig):
    return _ctx.set(cfg)


def reset_runtime_config_token(token) -> None:
    _ctx.reset(token)


def runtime_config_from_request_session(session: dict[str, Any]) -> HubRuntimeConfig:
    return env_base_config().merge_session(session)


def mask_connection_hint(s: str, max_head: int = 24) -> str:
    if not s:
        return ""
    if len(s) <= max_head + 6:
        return "•••• (hidden)"
    return s[:max_head] + "…••••"
