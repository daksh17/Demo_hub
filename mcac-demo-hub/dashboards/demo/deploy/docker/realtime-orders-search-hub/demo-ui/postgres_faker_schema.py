"""
Generate assorted PostgreSQL catalog objects using Faker for identifiers/layout only.

DDL is fixed-template and identifier-validated — not arbitrary SQL from the client.
"""
from __future__ import annotations

import secrets
from typing import Any

import psycopg
from psycopg import sql

from postgres_logical_demo import (
    PUB_DB,
    SUB_DB,
    _with_db,
    faker_generate_custom_columns,
    logical_custom_create_table,
)

_MAX_TABLES = 3
_MAX_SEQUENCES = 6
_MAX_VIEWS = 4
_MAX_MVIEWS = 3
_MAX_FUNCTIONS = 4
_MAX_PROCEDURES = 4
_MAX_SCHEMAS_BATCH = 8
_MAX_TABLES_PER_SCHEMA_BATCH = 4


def _uniq(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(3)}"


def apply_faker_schema_objects(
    *,
    publisher_admin_dsn: str,
    subscriber_admin_dsn: str,
    database: str,
    preset: str,
    seed: int | None,
    tables: int,
    sequences: int,
    views: int,
    materialized_views: int,
    functions: int,
    procedures: int,
) -> dict[str, Any]:
    """Create hub-safe tables/views/etc. on ``demo_logical_pub`` or ``demo_logical_sub``."""
    if database not in (PUB_DB, SUB_DB):
        raise ValueError(f"unsupported database {database!r}")
    target = "publisher" if database == PUB_DB else "subscriber"

    tables = max(0, min(int(tables), _MAX_TABLES))
    sequences = max(0, min(int(sequences), _MAX_SEQUENCES))
    views = max(0, min(int(views), _MAX_VIEWS))
    materialized_views = max(0, min(int(materialized_views), _MAX_MVIEWS))
    functions = max(0, min(int(functions), _MAX_FUNCTIONS))
    procedures = max(0, min(int(procedures), _MAX_PROCEDURES))

    total_obj = tables + sequences + views + materialized_views + functions + procedures
    if total_obj == 0:
        raise ValueError(
            "choose at least one object type with a non-zero count "
            "(tables, sequences, views, materialized_views, functions, procedures)"
        )

    out: dict[str, Any] = {
        "ok": True,
        "database": database,
        "preset": preset,
        "created": {
            "tables": [],
            "sequences": [],
            "views": [],
            "materialized_views": [],
            "functions": [],
            "procedures": [],
        },
    }

    anchor_schema_table: tuple[str, str, list[str]] | None = None
    # (schema, table, select_cols)

    # --- Tables (reuse logical replication DDL helper; commits per table)
    for i in range(tables):
        col_seed = None if seed is None else int(seed) + i * 997
        plan = faker_generate_custom_columns(
            preset=preset,
            column_count=min(6, 16),
            seed=col_seed,
        )
        tn = plan["table_name_suggestion"]
        cols = plan["columns"]
        logical_custom_create_table(
            publisher_admin_dsn,
            subscriber_admin_dsn,
            tn,
            cols,
            target=target,
            include_bigserial_pk=True,
            replica_identity_full=False,
        )
        out["created"]["tables"].append({"name": tn, "columns": cols})
        if anchor_schema_table is None:
            sel_cols: list[str] = ["id"]
            for c in cols[:3]:
                sel_cols.append(str(c["name"]))
            anchor_schema_table = ("public", tn, sel_cols)

    needs_anchor = views > 0 or materialized_views > 0
    if needs_anchor and anchor_schema_table is None:
        anchor_tn = _uniq("fs_anchor")
        logical_custom_create_table(
            publisher_admin_dsn,
            subscriber_admin_dsn,
            anchor_tn,
            [{"name": "payload", "pg_type": "TEXT", "not_null": False}],
            target=target,
            include_bigserial_pk=True,
            replica_identity_full=False,
        )
        out["created"]["tables"].append(
            {
                "name": anchor_tn,
                "columns": [{"name": "payload", "pg_type": "TEXT", "not_null": False}],
                "note": "auto-created anchor for views/materialized views",
            }
        )
        anchor_schema_table = ("public", anchor_tn, ["id", "payload"])

    adm = (
        publisher_admin_dsn
        if database == PUB_DB
        else subscriber_admin_dsn
    )
    ci = _with_db(adm, database)

    def grant_demo_sequence(ident: sql.Composable) -> sql.Composed:
        return sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {} TO demo").format(ident)

    def grant_demo_table(ident: sql.Composable) -> sql.Composed:
        return sql.SQL("GRANT SELECT ON TABLE {} TO demo").format(ident)

    with psycopg.connect(ci, autocommit=False) as conn:
        with conn.cursor() as cur:
            for _ in range(sequences):
                seq = _uniq("fs_seq")
                cur.execute(
                    sql.SQL(
                        "CREATE SEQUENCE IF NOT EXISTS {} AS bigint START WITH 1 INCREMENT BY 1"
                    ).format(sql.Identifier("public", seq))
                )
                cur.execute(
                    grant_demo_sequence(sql.Identifier("public", seq))
                )
                out["created"]["sequences"].append(seq)

            sch, tbl, acols = (
                anchor_schema_table
                if anchor_schema_table
                else ("public", "skipped", ["id"])
            )

            idents = [
                sql.Identifier(sch, tbl),
                sql.SQL(", ").join(sql.Identifier(c) for c in acols),
            ]

            for _ in range(views):
                vn = _uniq("fs_vw")
                cur.execute(
                    sql.SQL(
                        "CREATE OR REPLACE VIEW {} AS SELECT {} FROM {} LIMIT 500"
                    ).format(
                        sql.Identifier("public", vn),
                        idents[1],
                        idents[0],
                    )
                )
                cur.execute(grant_demo_table(sql.Identifier("public", vn)))
                out["created"]["views"].append(vn)

            for _ in range(materialized_views):
                mvn = _uniq("fs_mv")
                cur.execute(
                    sql.SQL(
                        "CREATE MATERIALIZED VIEW IF NOT EXISTS {} AS SELECT {} FROM {}"
                    ).format(
                        sql.Identifier("public", mvn),
                        idents[1],
                        idents[0],
                    )
                )
                cur.execute(grant_demo_table(sql.Identifier("public", mvn)))
                out["created"]["materialized_views"].append(mvn)

            for _ in range(functions):
                fn = _uniq("fs_fn")
                cur.execute(
                    sql.SQL(
                        """
                        CREATE OR REPLACE FUNCTION {}(a integer)
                        RETURNS integer
                        LANGUAGE sql
                        IMMUTABLE
                        AS $$ SELECT a + 1 $$
                        """
                    ).format(sql.Identifier("public", fn))
                )
                out["created"]["functions"].append(fn)

            for _ in range(procedures):
                pn = _uniq("fs_proc")
                cur.execute(
                    sql.SQL(
                        """
                        CREATE OR REPLACE PROCEDURE {}()
                        LANGUAGE plpgsql
                        AS $$
                        BEGIN
                          PERFORM 1;
                        END;
                        $$
                        """
                    ).format(sql.Identifier("public", pn))
                )
                out["created"]["procedures"].append(pn)

        # Grants referencing catalog names must run after DDL is visible
        with conn.cursor() as cur:
            for fn in out["created"]["functions"]:
                cur.execute(
                    sql.SQL("GRANT EXECUTE ON FUNCTION {}(integer) TO demo").format(
                        sql.Identifier("public", fn)
                    )
                )
            for pn in out["created"]["procedures"]:
                cur.execute(
                    sql.SQL("GRANT EXECUTE ON PROCEDURE {}() TO demo").format(
                        sql.Identifier("public", pn)
                    )
                )

        conn.commit()

    out["hint"] = (
        "Faker only influences table column layouts/names; views/MVs reference the first "
        "generated table (or a tiny anchor table). Destructive DROP is not performed — "
        "objects accumulate across runs."
    )
    return out


def apply_multi_schema_meaningful_tables(
    *,
    publisher_admin_dsn: str,
    subscriber_admin_dsn: str,
    database: str,
    schemas: list[str],
    tables_per_schema: int,
    preset: str,
    seed: int | None,
) -> dict[str, Any]:
    """Create multiple schemas (if needed) and Faker tables with readable names under each."""
    if database not in (PUB_DB, SUB_DB):
        raise ValueError(f"unsupported database {database!r}")
    target = "publisher" if database == PUB_DB else "subscriber"

    norm: list[str] = []
    seen: set[str] = set()
    for s in schemas:
        ns = str(s).strip().lower()
        if not ns:
            continue
        if ns in seen:
            continue
        seen.add(ns)
        norm.append(ns)
    if not norm:
        raise ValueError("at least one schema name is required")
    if len(norm) > _MAX_SCHEMAS_BATCH:
        raise ValueError(f"too many schemas (max {_MAX_SCHEMAS_BATCH})")

    tps = max(1, min(int(tables_per_schema), _MAX_TABLES_PER_SCHEMA_BATCH))

    out: dict[str, Any] = {
        "ok": True,
        "database": database,
        "preset": preset,
        "tables_per_schema": tps,
        "schemas": [],
    }

    idx = 0
    for schema in norm:
        entry: dict[str, Any] = {"schema": schema, "tables": []}
        for _ti in range(tps):
            col_seed = None if seed is None else int(seed) + idx
            idx += 1
            plan = faker_generate_custom_columns(
                preset=preset,
                column_count=min(6, 16),
                seed=col_seed,
                meaningful_table_name=True,
                schema_slug=schema,
            )
            tn = str(plan["table_name_suggestion"])
            logical_custom_create_table(
                publisher_admin_dsn,
                subscriber_admin_dsn,
                tn,
                plan["columns"],
                target=target,
                include_bigserial_pk=True,
                replica_identity_full=False,
                schema_name=schema,
            )
            entry["tables"].append(
                {
                    "table": f"{schema}.{tn}",
                    "table_name": tn,
                    "columns": plan["columns"],
                }
            )
        out["schemas"].append(entry)

    out["hint"] = (
        "Schemas are created if missing and granted to demo. Add tables to your publication "
        "manually if you need them on the replication stream."
    )
    return out
