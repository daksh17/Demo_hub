"""
Teaching tables + queries for Cassandra partition key vs clustering key vs RDBMS intuition.
Uses the hub keyspace (default demo_hub); table names are prefixed hub_cass_lab_*.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from cassandra.cluster import Session

T_EVENTS_ASC = "hub_cass_lab_events_asc"
T_EVENTS_DESC = "hub_cass_lab_events_desc"
T_ORDERS_BY_USER = "hub_cass_lab_orders_by_user"


def _row_to_dict(row: Any, column_names: tuple[str, ...] | None) -> dict[str, Any]:
    """Driver may return Row, namedtuple, or plain tuple — avoid row['col'] on tuples."""
    if row is None:
        return {}
    if isinstance(row, (list, tuple)) and column_names:
        return {
            column_names[i]: row[i]
            for i in range(min(len(column_names), len(row)))
        }
    try:
        return dict(row)
    except Exception:
        pass
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    if column_names and len(column_names) == 1:
        return {column_names[0]: row}
    return {}


def _scalar_int(row: Any, *, alias: str = "c", column_names: tuple[str, ...] | None = None) -> int:
    if row is None:
        return 0
    if isinstance(row, (list, tuple)):
        if len(row) == 0:
            return 0
        return int(row[0])
    d = _row_to_dict(row, column_names)
    if alias in d:
        return int(d[alias])
    if d:
        return int(next(iter(d.values())))
    try:
        return int(getattr(row, alias))
    except AttributeError:
        return int(row[0])


def _rows(rs) -> list[dict[str, Any]]:
    colnames: tuple[str, ...] | None = getattr(rs, "column_names", None)
    out: list[dict[str, Any]] = []
    for row in rs:
        d = _row_to_dict(row, colnames)
        for k, v in list(d.items()):
            if hasattr(v, "date"):
                d[k] = str(v.date())
            elif hasattr(v, "isoformat") and not isinstance(v, str):
                try:
                    d[k] = v.isoformat()
                except Exception:
                    pass
        out.append(d)
    return out


def ensure_lab_tables(session: Session, ks: str) -> list[str]:
    session.set_keyspace(ks)
    steps: list[str] = []
    stmts = [
        f"""
        CREATE TABLE IF NOT EXISTS {T_EVENTS_ASC} (
          tenant_id text,
          day date,
          seq int,
          note text,
          PRIMARY KEY ((tenant_id, day), seq)
        )
        WITH CLUSTERING ORDER BY (seq ASC)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {T_EVENTS_DESC} (
          tenant_id text,
          day date,
          seq int,
          note text,
          PRIMARY KEY ((tenant_id, day), seq)
        )
        WITH CLUSTERING ORDER BY (seq DESC)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {T_ORDERS_BY_USER} (
          user_id text,
          order_id text,
          sku text,
          cents int,
          PRIMARY KEY (user_id, order_id)
        )
        WITH CLUSTERING ORDER BY (order_id ASC)
        """,
    ]
    for cql in stmts:
        session.execute(cql)
    steps.append(f"Ensured teaching tables in {ks!r}: {T_EVENTS_ASC}, {T_EVENTS_DESC}, {T_ORDERS_BY_USER}.")
    return steps


def _truncate_lab(session: Session, ks: str) -> None:
    session.set_keyspace(ks)
    for t in (T_EVENTS_ASC, T_EVENTS_DESC, T_ORDERS_BY_USER):
        session.execute(f"TRUNCATE {t}")


def seed_lab(session: Session, ks: str, *, replace: bool) -> list[str]:
    steps: list[str] = []
    ensure_lab_tables(session, ks)
    if replace:
        _truncate_lab(session, ks)
        steps.append("Truncated prior lab rows for re-seed.")
    else:
        chk = session.execute(
            f"SELECT COUNT(*) AS c FROM {T_EVENTS_ASC} WHERE tenant_id = 'acme' AND day = '2024-06-01'"
        )
        row = chk.one()
        cn = getattr(chk, "column_names", None)
        n = _scalar_int(row, alias="c", column_names=cn)
        if n > 0:
            steps.append("Lab data already present (use replace=true to re-seed).")
            return steps

    d1 = date(2024, 6, 1)
    d2 = date(2024, 6, 2)
    inserts_asc = [
        ("acme", d1, 3, "inserted third seq (write order ≠ read order)"),
        ("acme", d1, 1, "first by clustering order"),
        ("acme", d1, 2, "second by clustering order"),
        ("globex", d1, 1, "different partition (tenant + day)"),
        ("acme", d2, 1, "different partition (different day)"),
    ]
    ins = session.prepare(
        f"INSERT INTO {T_EVENTS_ASC} (tenant_id, day, seq, note) VALUES (?, ?, ?, ?)"
    )
    for row in inserts_asc:
        session.execute(ins, row)
    insd = session.prepare(
        f"INSERT INTO {T_EVENTS_DESC} (tenant_id, day, seq, note) VALUES (?, ?, ?, ?)"
    )
    for row in inserts_asc:
        session.execute(insd, row)

    orders = [
        ("user-42", "ord-a", "sku-1", 100),
        ("user-42", "ord-b", "sku-2", 250),
        ("user-42", "ord-c", "sku-1", 100),
        ("user-99", "ord-x", "sku-9", 999),
    ]
    io = session.prepare(
        f"INSERT INTO {T_ORDERS_BY_USER} (user_id, order_id, sku, cents) VALUES (?, ?, ?, ?)"
    )
    for row in orders:
        session.execute(io, row)

    steps.append(
        "Inserted sample rows: same logical writes into ASC/DESC clustering tables; "
        "two users in orders_by_user (user-42 has three rows in one partition)."
    )
    return steps


def run_demo(session: Session, ks: str) -> dict[str, Any]:
    steps = seed_lab(session, ks, replace=False)
    panels: list[dict[str, Any]] = []

    def add(title: str, cql: str, caption: str) -> None:
        rs = session.execute(cql)
        panels.append(
            {
                "title": title,
                "cql": cql.strip(),
                "rows": _rows(rs),
                "caption": caption,
            }
        )

    add(
        "One partition (acme, 2024-06-01) — clustering ASC on seq",
        f"SELECT tenant_id, day, seq, note FROM {T_EVENTS_ASC} "
        f"WHERE tenant_id = 'acme' AND day = '2024-06-01'",
        "All rows share the same partition key (tenant_id, day). On disk Cassandra groups them in "
        "clustering order (seq ASC): 1, 2, 3 — not insertion order.",
    )
    add(
        "Same partition — clustering DESC on seq",
        f"SELECT tenant_id, day, seq, note FROM {T_EVENTS_DESC} "
        f"WHERE tenant_id = 'acme' AND day = '2024-06-01'",
        "Same data model, opposite clustering order: reads return 3, 2, 1. Clustering only "
        "sorts inside a partition; it does not choose which node holds the data.",
    )
    add(
        "Two partitions for same tenant, different days",
        f"SELECT tenant_id, day, seq, note FROM {T_EVENTS_ASC} WHERE tenant_id = 'acme' ALLOW FILTERING",
        "ALLOW FILTERING scans all partitions for tenant_id=acme (fine for tiny demo; avoid in prod). "
        "Notice rows for 2024-06-01 vs 2024-06-02 live in different partitions.",
    )
    add(
        "Wide partition: many orders for one user_id",
        f"SELECT user_id, order_id, sku, cents FROM {T_ORDERS_BY_USER} WHERE user_id = 'user-42'",
        "Partition key is only user_id — all orders for that user sit in one partition, clustered by order_id. "
        "Contrast RDBMS: rows with the same user_id are not necessarily adjacent on disk unless an index/cluster says so.",
    )

    return {
        "keyspace": ks,
        "tables": [T_EVENTS_ASC, T_EVENTS_DESC, T_ORDERS_BY_USER],
        "steps": steps,
        "panels": panels,
    }


def setup_lab(session: Session, ks: str, *, replace: bool) -> dict[str, Any]:
    steps = ensure_lab_tables(session, ks)
    steps.extend(seed_lab(session, ks, replace=replace))
    return {"ok": True, "keyspace": ks, "steps": steps}
