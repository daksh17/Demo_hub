"""
Bulk read/write benchmark with configurable Cassandra consistency levels (demo teaching).
Table: hub_cass_cl_bench — partition key bench_id, clustering seq.
"""
from __future__ import annotations

import os
import time
import uuid
from typing import Any

from cassandra import ConsistencyLevel, consistency_value_to_name
from cassandra.cluster import Session
from cassandra.query import BatchStatement, SimpleStatement

LAB_VERSION = "2026-05-15-v5-diagnostics-fix"

TABLE = "hub_cass_cl_bench"

_REQUEST_TIMEOUT = float(os.environ.get("CASSANDRA_REQUEST_TIMEOUT_SECONDS", "120"))

# ALL levels exposed for writes; reads exclude write-only / LWT-only levels.
CL_NAMES: tuple[str, ...] = (
    "ANY",
    "ONE",
    "TWO",
    "THREE",
    "QUORUM",
    "ALL",
    "LOCAL_ONE",
    "LOCAL_QUORUM",
    "EACH_QUORUM",
)
READ_DISALLOWED: frozenset[str] = frozenset({"ANY", "SERIAL", "LOCAL_SERIAL"})
READ_CL_NAMES: tuple[str, ...] = tuple(n for n in CL_NAMES if n not in READ_DISALLOWED)


def parse_consistency(name: str, *, for_read: bool = False) -> int:
    key = str(name or "").strip().upper().replace("-", "_")
    if for_read and key in READ_DISALLOWED:
        raise ValueError(
            f"{key} is not valid for reads; use e.g. ONE, LOCAL_ONE, QUORUM, LOCAL_QUORUM, ALL"
        )
    try:
        return int(ConsistencyLevel.name_to_value[key])
    except KeyError:
        allowed = ", ".join(READ_CL_NAMES if for_read else CL_NAMES)
        raise ValueError(f"unknown consistency {name!r}; use one of: {allowed}") from None


def consistency_label(cl: int) -> str:
    return consistency_value_to_name(int(cl))


def consistency_choices() -> dict[str, Any]:
    return {
        "write_levels": [{"name": n, "value": n} for n in CL_NAMES],
        "read_levels": [{"name": n, "value": n} for n in READ_CL_NAMES],
    }


def lab_info() -> dict[str, str]:
    return {"lab_version": LAB_VERSION, "table": TABLE}


def module_diagnostics() -> dict[str, Any]:
    """Inspect on-disk module source (detect stale image vs mounted file)."""
    from pathlib import Path

    path = Path(__file__).resolve()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        return {"path": str(path), "lab_version": LAB_VERSION, "read_error": str(e)}
    # Never spell buggy patterns as contiguous literals in this function — they would match our own source.
    obsolete_execute = "session.execute(" + "prep" + ", (bench_id, seq), "
    obsolete_kw = "consistency_level=" + "read_cl"
    stale = obsolete_execute in text and obsolete_kw in text
    ss_needle = "SimpleStatement(select_tpl, consistency_level=" + "read_cl)"
    uses_ss = ss_needle in text
    return {
        "path": str(path),
        "lab_version": LAB_VERSION,
        "stale_execute_cl_kwarg": stale,
        "uses_simple_statement_read": uses_ss,
        "ok": not stale and uses_ss,
    }


def ensure_table(session: Session, ks: str, *, replace: bool) -> list[str]:
    session.set_keyspace(ks)
    steps: list[str] = []
    if replace:
        session.execute(f"DROP TABLE IF EXISTS {TABLE}", timeout=_REQUEST_TIMEOUT)
        steps.append(f"Dropped {TABLE} (if existed).")
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          bench_id text,
          seq int,
          payload text,
          PRIMARY KEY (bench_id, seq)
        )
        """,
        timeout=_REQUEST_TIMEOUT,
    )
    steps.append(f"Ensured {ks}.{TABLE}.")
    return steps


def _write_bulk(
    session: Session,
    ks: str,
    bench_id: str,
    n_rows: int,
    batch_size: int,
    write_cl: int,
) -> dict[str, Any]:
    session.set_keyspace(ks)
    prep = session.prepare(
        f"INSERT INTO {TABLE} (bench_id, seq, payload) VALUES (?, ?, ?)"
    )
    payload = "x" * 64
    t0 = time.perf_counter()
    written = 0
    bs_cap = max(1, min(batch_size, 256))
    seq = 0
    while seq < n_rows:
        batch = BatchStatement(consistency_level=write_cl)
        n_in_batch = 0
        while seq < n_rows and n_in_batch < bs_cap:
            batch.add(prep, (bench_id, seq, f"{payload}-{seq}"))
            seq += 1
            n_in_batch += 1
        session.execute(batch, timeout=_REQUEST_TIMEOUT)
        written += n_in_batch
    elapsed = time.perf_counter() - t0
    return _phase_stats("write", written, elapsed, write_cl)


def _read_bulk(
    session: Session,
    ks: str,
    bench_id: str,
    n_rows: int,
    read_cl: int,
) -> dict[str, Any]:
    session.set_keyspace(ks)
    # SimpleStatement carries consistency_level; Session.execute() does not accept that kwarg.
    select_tpl = (
        f"SELECT seq, payload FROM {TABLE} WHERE bench_id = %s AND seq = %s"
    )
    t0 = time.perf_counter()
    read = 0
    for seq in range(n_rows):
        stmt = SimpleStatement(select_tpl, consistency_level=read_cl)
        session.execute(stmt, (bench_id, seq), timeout=_REQUEST_TIMEOUT)
        read += 1
    elapsed = time.perf_counter() - t0
    return _phase_stats("read", read, elapsed, read_cl)


def _phase_stats(phase: str, count: int, elapsed_s: float, cl: int) -> dict[str, Any]:
    ops = count / elapsed_s if elapsed_s > 0 else 0.0
    return {
        "phase": phase,
        "count": count,
        "elapsed_ms": round(elapsed_s * 1000, 2),
        "ops_per_sec": round(ops, 1),
        "avg_ms_per_op": round((elapsed_s * 1000 / count), 3) if count else 0.0,
        "consistency": consistency_label(cl),
    }


def run_benchmark(
    session: Session,
    ks: str,
    *,
    write_cl_name: str,
    read_cl_name: str,
    n_rows: int,
    batch_size: int,
    replace_table: bool = False,
) -> dict[str, Any]:
    if n_rows < 1 or n_rows > 50_000:
        raise ValueError("n_rows must be between 1 and 50000")
    if batch_size < 1 or batch_size > 256:
        raise ValueError("batch_size must be between 1 and 256")
    write_cl = parse_consistency(write_cl_name, for_read=False)
    read_cl = parse_consistency(read_cl_name, for_read=True)
    steps = ensure_table(session, ks, replace=replace_table)
    bench_id = str(uuid.uuid4())
    try:
        write_stats = _write_bulk(session, ks, bench_id, n_rows, batch_size, write_cl)
    except Exception as e:
        raise RuntimeError(f"write phase failed ({consistency_label(write_cl)}): {e}") from e
    try:
        read_stats = _read_bulk(session, ks, bench_id, n_rows, read_cl)
    except Exception as e:
        raise RuntimeError(f"read phase failed ({consistency_label(read_cl)}): {e}") from e
    return {
        "keyspace": ks,
        "table": TABLE,
        "bench_id": bench_id,
        "n_rows": n_rows,
        "batch_size": batch_size,
        "write_consistency": consistency_label(write_cl),
        "read_consistency": consistency_label(read_cl),
        "steps": steps,
        "write": write_stats,
        "read": read_stats,
        "lab_version": LAB_VERSION,
    }


def run_compare_matrix(
    session: Session,
    ks: str,
    *,
    n_rows: int,
    batch_size: int,
    write_levels: list[str],
    read_levels: list[str],
    replace_table: bool = False,
) -> dict[str, Any]:
    if len(write_levels) * len(read_levels) > 36:
        raise ValueError("too many combinations (max 36)")
    steps = ensure_table(session, ks, replace=replace_table)
    results: list[dict[str, Any]] = []
    for wname in write_levels:
        for rname in read_levels:
            try:
                row = run_benchmark(
                    session,
                    ks,
                    write_cl_name=wname,
                    read_cl_name=rname,
                    n_rows=n_rows,
                    batch_size=batch_size,
                    replace_table=False,
                )
                results.append(
                    {
                        "write_consistency": wname,
                        "read_consistency": rname,
                        "write_ops_per_sec": row["write"]["ops_per_sec"],
                        "read_ops_per_sec": row["read"]["ops_per_sec"],
                        "write_elapsed_ms": row["write"]["elapsed_ms"],
                        "read_elapsed_ms": row["read"]["elapsed_ms"],
                        "write_avg_ms": row["write"]["avg_ms_per_op"],
                        "read_avg_ms": row["read"]["avg_ms_per_op"],
                        "bench_id": row["bench_id"],
                    }
                )
            except Exception as e:
                results.append(
                    {
                        "write_consistency": wname,
                        "read_consistency": rname,
                        "error": f"{type(e).__name__}: {e}",
                    }
                )
    return {
        "keyspace": ks,
        "table": TABLE,
        "n_rows": n_rows,
        "batch_size": batch_size,
        "steps": steps,
        "results": results,
        "lab_version": LAB_VERSION,
    }
