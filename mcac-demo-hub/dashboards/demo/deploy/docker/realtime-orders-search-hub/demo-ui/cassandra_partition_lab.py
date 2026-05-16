"""
Custom Cassandra tables: composite partition + clustering columns, Faker seed data,
topology (system.peers / system.local), and partition token + replica hints via driver token_map.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Sequence

from cassandra.cluster import Session
from faker import Faker

from cassandra_storage_lab import _rows

_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_TABLE_SUFFIX = re.compile(r"^[a-z][a-z0-9_]{0,40}$")

ALLOWED_TYPES = frozenset(
    {
        "text",
        "int",
        "bigint",
        "boolean",
        "double",
        "timestamp",
        "uuid",
        "timeuuid",
        "tinyint",
        "inet",
    }
)


def _safe_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"invalid identifier: {name!r}")
    return name


def _table_name(suffix: str) -> str:
    if not _TABLE_SUFFIX.match(suffix):
        raise ValueError("table_suffix must match ^[a-z][a-z0-9_]{0,40}$")
    return f"hub_cass_user_{suffix}"


def _primary_key_clause(partition: list[str], clustering: list[str]) -> str:
    if not partition:
        raise ValueError("partition_key must be non-empty")
    p = [_safe_ident(x) for x in partition]
    c = [_safe_ident(x) for x in clustering]
    if len(p) == 1 and not c:
        return f"({p[0]})"
    if len(p) == 1:
        return f"({p[0]}, {', '.join(c)})"
    inner = ", ".join(p)
    if not c:
        return f"(({inner}))"
    return f"(({inner}), {', '.join(c)})"


def _clustering_order_clause(
    clustering: list[str], order_spec: list[tuple[str, str]] | None
) -> str:
    if not clustering:
        return ""
    spec: dict[str, str] = {}
    if order_spec:
        for name, direction in order_spec:
            n = _safe_ident(name)
            d = direction.upper()
            if d not in ("ASC", "DESC"):
                raise ValueError(f"bad clustering order for {name!r}: {direction!r}")
            spec[n] = d
    parts = []
    for n in [_safe_ident(x) for x in clustering]:
        parts.append(f"{n} {spec.get(n, 'ASC')}")
    return "WITH CLUSTERING ORDER BY (" + ", ".join(parts) + ")"


def _normalize_clustering_order(
    clustering_key: list[tuple[str, str]],
    clustering_order: list[tuple[str, str]] | None,
) -> list[tuple[str, str]] | None:
    if not clustering_key:
        return None
    order_by_name: dict[str, str] = {}
    if clustering_order:
        for name, d in clustering_order:
            n = _safe_ident(name)
            u = d.upper()
            if u not in ("ASC", "DESC"):
                raise ValueError(f"invalid clustering direction {d!r}")
            order_by_name[n] = u
    return [(n, order_by_name.get(n, "ASC")) for n, _ in clustering_key]


def build_create_cql(
    ks: str,
    table: str,
    partition: list[tuple[str, str]],
    clustering: list[tuple[str, str]],
    extra: list[tuple[str, str]],
    clustering_order: list[tuple[str, str]] | None,
) -> str:
    for n, t in partition + clustering + extra:
        _safe_ident(n)
        if t not in ALLOWED_TYPES:
            raise ValueError(f"unsupported type {t!r} for column {n!r}")
    cols: list[str] = []
    for n, t in partition + clustering + extra:
        cols.append(f"{_safe_ident(n)} {t}")
    pk = _primary_key_clause([n for n, _ in partition], [n for n, _ in clustering])
    cco = _clustering_order_clause([n for n, _ in clustering], clustering_order)
    tail = f" {cco}" if cco else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {_safe_ident(ks)}.{_safe_ident(table)} "
        f"( {', '.join(cols)}, PRIMARY KEY {pk} ){tail}"
    )


def create_table(
    session: Session,
    ks: str,
    *,
    table_suffix: str,
    partition_key: list[tuple[str, str]],
    clustering_key: list[tuple[str, str]],
    extra_columns: list[tuple[str, str]],
    clustering_order: list[tuple[str, str]] | None,
    replace_if_exists: bool,
) -> dict[str, Any]:
    table = _table_name(table_suffix)
    co = _normalize_clustering_order(clustering_key, clustering_order)
    cql = build_create_cql(
        ks, table, partition_key, clustering_key, extra_columns, co
    )
    session.set_keyspace(ks)
    if replace_if_exists:
        session.execute(f"DROP TABLE IF EXISTS {_safe_ident(table)}")
    session.execute(cql)
    tq = _safe_ident(table)
    kq = _safe_ident(ks)
    return {
        "ok": True,
        "keyspace": ks,
        "table": table,
        "cql": cql.strip(),
        "sample_select_cql": f"SELECT * FROM {kq}.{tq} LIMIT 20;",
        "sample_select_after_use_cql": (
            f"-- after: USE {kq};\nSELECT * FROM {tq} LIMIT 20;"
        ),
        "cqlsh_note": (
            "If cqlsh reports 'not found in keyspace None' right after CREATE, the Python driver's "
            "schema cache in that cqlsh session is stale: exit cqlsh and reconnect (e.g. cqlsh … -k "
            f"{kq}), or run cqlsh against another node. DESCRIBE can succeed while SELECT still uses old metadata."
        ),
        "partition_key": [list(x) for x in partition_key],
        "clustering_key": [list(x) for x in clustering_key],
        "extra_columns": [list(x) for x in extra_columns],
    }


def _faker_value(fake: Faker, cql_type: str, seq: int) -> Any:
    t = cql_type.lower()
    if t == "text":
        return fake.pystr(min_chars=4, max_chars=24) if seq % 3 else fake.lexify("?" * 8)
    if t == "int":
        return int(fake.random_int(1, 2_000_000_000))
    if t == "bigint":
        return int(fake.random_int(1, 9_000_000_000))
    if t == "boolean":
        return bool(seq % 2)
    if t == "double":
        return float(fake.pyfloat(left_digits=3, right_digits=4, positive=True))
    if t == "timestamp":
        return datetime.now(timezone.utc)
    if t == "uuid":
        return uuid.uuid4()
    if t == "timeuuid":
        return uuid.uuid1()
    if t == "tinyint":
        return int(fake.random_int(0, 255))
    if t == "inet":
        return fake.ipv4(network=False, private=False)
    raise ValueError(t)


def seed_faker(
    session: Session,
    ks: str,
    table: str,
    *,
    partition_key: list[tuple[str, str]],
    clustering_key: list[tuple[str, str]],
    extra_columns: list[tuple[str, str]],
    n_rows: int,
    partitions_wide: int,
    seed: int | None,
) -> dict[str, Any]:
    if n_rows < 1 or n_rows > 5000:
        raise ValueError("n_rows must be 1..5000")
    if partitions_wide < 1 or partitions_wide > 2000:
        raise ValueError("partitions_wide must be 1..2000")
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)

    pnames = [n for n, _ in partition_key]
    cnames = [n for n, _ in clustering_key]
    enames = [n for n, _ in extra_columns]
    all_cols = pnames + cnames + enames
    placeholders = ", ".join(["?"] * len(all_cols))
    q = f"INSERT INTO {_safe_ident(ks)}.{_safe_ident(table)} ({', '.join(_safe_ident(c) for c in all_cols)}) VALUES ({placeholders})"
    session.set_keyspace(ks)
    prep = session.prepare(q)
    inserted = 0

    # Build reusable partition key value tuples (stable per slot)
    part_slots: list[list[Any]] = []
    for i in range(partitions_wide):
        part_slots.append(
            [_faker_value(fake, t, seq=i * 1000 + j) for j, (_, t) in enumerate(partition_key)]
        )

    if not clustering_key:
        for i in range(n_rows):
            pvals = list(part_slots[i % len(part_slots)])
            evals = [_faker_value(fake, t, i) for _, t in extra_columns]
            session.execute(prep, tuple(pvals + evals))
            inserted += 1
    else:
        ci = 0
        while inserted < n_rows:
            pi = ci % partitions_wide
            pvals = list(part_slots[pi % len(part_slots)])
            cvals = [
                _faker_value(fake, typ, inserted + ci * 17)
                for _, typ in clustering_key
            ]
            evals = [_faker_value(fake, typ, inserted) for _, typ in extra_columns]
            session.execute(prep, tuple(pvals + cvals + evals))
            inserted += 1
            ci += 1

    tq = _safe_ident(table)
    kq = _safe_ident(ks)
    return {
        "ok": True,
        "inserted": inserted,
        "keyspace": ks,
        "table": table,
        "sample_select_cql": f"SELECT * FROM {kq}.{tq} LIMIT 20;",
        "sample_select_after_use_cql": (
            f"-- after: USE {kq};\nSELECT * FROM {tq} LIMIT 20;"
        ),
        "cqlsh_note": (
            "If cqlsh reports 'not found in keyspace None', reconnect cqlsh so the driver reloads "
            f"schema (e.g. cqlsh … -k {kq}). DESCRIBE may work before SELECT does."
        ),
    }


def _token_args(partition: list[str]) -> str:
    p = [_safe_ident(x) for x in partition]
    if len(p) == 1:
        return p[0]
    return ", ".join(p)


def partition_token_summary(
    session: Session,
    ks: str,
    table: str,
    partition_columns: Sequence[str],
) -> dict[str, Any]:
    p = [_safe_ident(c) for c in partition_columns]
    tok_expr = f"token({_token_args(p)}) AS tok_murmur3"
    sel = ", ".join([tok_expr] + p)
    group = ", ".join(p)
    cql = f"SELECT {sel} FROM {_safe_ident(ks)}.{_safe_ident(table)} GROUP BY {group}"
    rs = session.execute(cql)
    rows = _rows(rs)
    cluster = session.cluster
    tm = getattr(getattr(cluster, "metadata", None), "token_map", None)
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        tok_v = rr.pop("tok_murmur3", None)
        if tok_v is None:
            for k in list(rr.keys()):
                if str(k).lower() == "tok_murmur3":
                    tok_v = rr.pop(k)
                    break
        if tok_v is None:
            for k in list(rr.keys()):
                if "token" in str(k).lower():
                    tok_v = rr.pop(k)
                    break
        tok_i = int(tok_v) if tok_v is not None else None
        rep: list[str] | None = None
        if tm is not None and tok_i is not None:
            try:
                from cassandra.metadata import Murmur3Token

                hosts = tm.get_replicas(ks, Murmur3Token(tok_i))
                rep = []
                for h in hosts or []:
                    addr = getattr(h, "broadcast_address", None) or getattr(
                        h, "address", None
                    )
                    rep.append(str(addr) if addr else str(h))
            except Exception:
                rep = None
        row_out = dict(rr)
        if tok_i is not None:
            row_out["token_murmur3"] = tok_i
        if rep is not None:
            row_out["replicas_hint"] = rep
        out_rows.append(row_out)

    return {"cql": cql.strip(), "partitions": out_rows, "count": len(out_rows)}


def _metadata_host_count(meta: Any) -> int | None:
    """cassandra-driver 3.29+ uses all_hosts(); older code used .hosts dict."""
    if meta is None:
        return None
    all_hosts = getattr(meta, "all_hosts", None)
    if callable(all_hosts):
        try:
            return len(all_hosts())
        except Exception:
            return None
    hosts = getattr(meta, "hosts", None)
    if hosts is None:
        return None
    try:
        return len(hosts)
    except TypeError:
        return len(list(hosts))


def fetch_topology(session: Session) -> dict[str, Any]:
    out: dict[str, Any] = {"nodes": [], "errors": []}
    cluster = session.cluster
    meta = getattr(cluster, "metadata", None)
    out["cluster_name"] = getattr(meta, "cluster_name", None) if meta else None
    out["partitioner"] = getattr(meta, "partitioner", None) if meta else None
    out["num_hosts_metadata"] = _metadata_host_count(meta)

    def run(cql: str) -> list[dict[str, Any]]:
        rs = session.execute(cql)
        return _rows(rs)

    for cql, label in (
        (
            "SELECT peer, data_center, rack, host_id, rpc_address, tokens FROM system.peers",
            "peers",
        ),
        (
            "SELECT broadcast_address, cluster_name, data_center, rack, host_id, "
            "partitioner, tokens FROM system.local",
            "local",
        ),
    ):
        try:
            out[label] = run(cql)
        except Exception as e:
            out["errors"].append({"query": cql, "error": f"{type(e).__name__}: {e}"})

    # Token ring summary from driver (if populated)
    tm = getattr(meta, "token_map", None) if meta else None
    if tm is not None and getattr(tm, "ring", None):
        try:
            ring = list(tm.ring)
            out["ring_tokens_sample"] = [str(t) for t in ring[: min(32, len(ring))]]
            out["ring_size"] = len(ring)
        except Exception as e:
            out["errors"].append({"ring": str(e)})
    return out


def drop_user_table(session: Session, ks: str, table_suffix: str) -> dict[str, Any]:
    t = _table_name(table_suffix)
    session.set_keyspace(ks)
    session.execute(f"DROP TABLE IF EXISTS {_safe_ident(t)}")
    return {"ok": True, "dropped": t}


def user_table_name(table_suffix: str) -> str:
    """Public: full table name for hub_cass_user_* tables."""
    return _table_name(table_suffix)
