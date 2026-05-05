"""
PostgreSQL logical replication demo: publisher DB ``demo_logical_pub`` on **postgresql-primary**
and subscriber DB ``demo_logical_sub`` on **postgres-sub** (separate pod). The subscription on
``postgres-sub`` pulls logical changes from the primary.

**postgresql-replica-1** / **postgresql-replica-2** remain physical standbys of the primary only; they replicate
``demo_logical_pub`` (not ``demo_logical_sub``). Optional ``POSTGRES_REPLICA_READ_DSN`` should
point at a replica database ``demo_logical_pub`` to compare publisher row counts with the primary.

Hub: ``/postgres`` → ``/postgres/logical``.

Env (Kubernetes): ``POSTGRES_ADMIN_DSN`` / ``POSTGRES_DSN`` (primary), ``POSTGRES_LOGICAL_SUB_ADMIN_DSN`` /
``POSTGRES_LOGICAL_SUB_DSN`` (subscriber pod). If logical-sub env vars are unset, fall back to primary DSNs
(single-node publisher+subscriber, backwards compatible).
"""
from __future__ import annotations

import re
import secrets
from datetime import timezone
from typing import Any
from urllib.parse import urlparse

import psycopg
from faker import Faker
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

PUB_DB = "demo_logical_pub"
SUB_DB = "demo_logical_sub"
TABLE = "logical_demo_events"
# BIGSERIAL ``id`` owns this sequence; ``demo`` needs USAGE/SELECT to insert (not only TABLE privileges).
TABLE_ID_SEQ = f"{TABLE}_id_seq"
PUBLICATION = "pub_logical_demo"
SUBSCRIPTION = "sub_logical_demo"
# Bump when setup logic changes (shown in API JSON — confirms hub picked up new module).
SETUP_REVISION = 8

_ENSURE_DEMO_ROLE_SQL = """
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'demo') THEN
    CREATE ROLE demo WITH LOGIN PASSWORD 'demopass';
  END IF;
END $$;
"""


def _ensure_demo_app_user(admin_dsn: str, label: str, step) -> None:
    """Ensure ``demo`` / ``demopass`` exists (same idea as postgres-demo-bootstrap / postgres-sub-bootstrap).

    Without this, ``GRANT CONNECT ... TO demo`` on ``postgres-sub`` fails with
    ``role "demo" does not exist`` if the bootstrap Job was skipped or not finished.
    """
    pg_ci = _with_db(admin_dsn, "postgres")
    with psycopg.connect(pg_ci, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(_ENSURE_DEMO_ROLE_SQL)
        step(f"ensure role demo ({label})", True)
    with psycopg.connect(pg_ci, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("GRANT CONNECT ON DATABASE demo TO demo")
            cur.execute("GRANT pg_monitor TO demo")
    demo_ci = _with_db(admin_dsn, "demo")
    with psycopg.connect(demo_ci, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("GRANT USAGE, CREATE ON SCHEMA public TO demo")


def _with_db(dsn: str, dbname: str) -> str:
    """Resolve DB name reliably (URI query strings / odd paths break naive urlparse path edits)."""
    opts = conninfo_to_dict(dsn)
    opts["dbname"] = dbname
    return make_conninfo(**opts)


def parse_host_port(
    dsn: str, default_host: str = "postgresql-primary", default_port: int = 5432
) -> tuple[str, int]:
    p = urlparse(dsn)
    return (p.hostname or default_host), (p.port or default_port)


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.{tbl} (
  id BIGSERIAL PRIMARY KEY,
  note TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def logical_demo_setup(
    publisher_admin_dsn: str,
    subscriber_admin_dsn: str,
    primary_host: str,
    primary_port: int,
    postgres_password: str,
) -> dict[str, Any]:
    """
    Reset logical replication objects and recreate publication + subscription.

    Publisher objects live on ``publisher_admin_dsn`` (primary); subscriber objects on
    ``subscriber_admin_dsn`` (``postgres-sub`` in Kubernetes).

    Each run drops the subscription (subscriber pod) and publication (primary), then
    rebuilds them. That clears broken catalog state that surfaces as
    ``relation "public.logical_demo_events" does not exist`` during ``CREATE SUBSCRIPTION``.
    Publisher table data is kept; subscriber table is truncated before resync.
    """
    out: dict[str, Any] = {"ok": True, "setup_revision": SETUP_REVISION, "steps": []}

    def step(name: str, ok: bool, detail: str | None = None) -> None:
        out["steps"].append({"name": name, "ok": ok, "detail": detail})

    try:
        _ensure_demo_app_user(publisher_admin_dsn, "primary", step)
        _ensure_demo_app_user(subscriber_admin_dsn, "subscriber pod", step)

        with psycopg.connect(publisher_admin_dsn, autocommit=True) as aconn:
            with aconn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (PUB_DB,),
                )
                if cur.fetchone():
                    step(f"database {PUB_DB} (primary)", True, "exists")
                else:
                    cur.execute(f'CREATE DATABASE "{PUB_DB}"')
                    step(f"CREATE DATABASE {PUB_DB} (primary)", True)
                cur.execute(f'GRANT CONNECT ON DATABASE "{PUB_DB}" TO demo')

        with psycopg.connect(subscriber_admin_dsn, autocommit=True) as aconn:
            with aconn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (SUB_DB,),
                )
                if cur.fetchone():
                    step(f"database {SUB_DB} (subscriber pod)", True, "exists")
                else:
                    cur.execute(f'CREATE DATABASE "{SUB_DB}"')
                    step(f"CREATE DATABASE {SUB_DB} (subscriber pod)", True)
                cur.execute(f'GRANT CONNECT ON DATABASE "{SUB_DB}" TO demo')

        pub_adm = _with_db(publisher_admin_dsn, PUB_DB)
        sub_adm = _with_db(subscriber_admin_dsn, SUB_DB)

        # Tear down subscription first (drops replication slot on publisher), then publication.
        with psycopg.connect(sub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SUBSCRIPTION IF EXISTS "{SUBSCRIPTION}"')
            step("DROP SUBSCRIPTION IF EXISTS", True)

        with psycopg.connect(pub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS "{PUBLICATION}" CASCADE')
            step("DROP PUBLICATION IF EXISTS", True)

        with psycopg.connect(pub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL.format(tbl=TABLE))
                cur.execute(f"ALTER TABLE public.{TABLE} REPLICA IDENTITY FULL")
                cur.execute(
                    f"GRANT SELECT, INSERT, DELETE ON TABLE public.{TABLE} TO demo"
                )
                cur.execute(
                    f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE_ID_SEQ} TO demo"
                )
            step("publisher table", True)

        with psycopg.connect(pub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'CREATE PUBLICATION "{PUBLICATION}" FOR TABLE public.{TABLE}'
                )
            step("CREATE PUBLICATION FOR TABLE", True)

        with psycopg.connect(sub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(_TABLE_DDL.format(tbl=TABLE))
                cur.execute(f"ALTER TABLE public.{TABLE} REPLICA IDENTITY FULL")
                cur.execute(f"TRUNCATE public.{TABLE} RESTART IDENTITY")
                cur.execute(
                    f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE_ID_SEQ} TO demo"
                )
            step("subscriber table + truncate (clean sync)", True)

        with psycopg.connect(sub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                conninfo = (
                    f"host={primary_host} port={primary_port} "
                    f"user=postgres password={postgres_password} dbname={PUB_DB}"
                )
                ci_lit = conninfo.replace("\\", "\\\\").replace("'", "''")
                cur.execute(
                    f"""
                    CREATE SUBSCRIPTION "{SUBSCRIPTION}"
                    CONNECTION '{ci_lit}'
                    PUBLICATION "{PUBLICATION}"
                    WITH (copy_data = true, create_slot = true)
                    """
                )
            step("CREATE SUBSCRIPTION", True)

        try:
            with psycopg.connect(sub_adm) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"GRANT SELECT ON TABLE public.{TABLE} TO demo")
                conn.commit()
                step("subscriber SELECT grant for demo", True)
        except Exception as ex:
            step(
                "subscriber SELECT grant (optional)",
                True,
                f"skipped — run setup again after sync if needed: {ex}",
            )

    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
        step("failed", False, str(e))

    return out


PG_DOCUMENTATION_LINKS: dict[str, dict[str, str]] = {
    "logical_replication": {
        "title": "Logical replication",
        "url": "https://www.postgresql.org/docs/current/logical-replication.html",
        "hint": "Publisher WAL is decoded into rows applied on subscriber databases. Tables must exist on both sides before they participate.",
    },
    "create_table": {
        "title": "CREATE TABLE",
        "url": "https://www.postgresql.org/docs/current/sql-createtable.html",
        "hint": "Subscriber DDL should match the publisher (including replica identity rules for UPDATE/DELETE).",
    },
    "create_publication": {
        "title": "CREATE PUBLICATION",
        "url": "https://www.postgresql.org/docs/current/sql-createpublication.html",
        "hint": "Only tables listed in the publication send logical changes; adding tables requires subscriber DDL + subscription refresh.",
    },
    "alter_subscription": {
        "title": "ALTER SUBSCRIPTION",
        "url": "https://www.postgresql.org/docs/current/sql-altersubscription.html",
        "hint": "REFRESH PUBLICATION pulls new publication tables; each target relation must already exist on the subscriber.",
    },
    "replica_identity": {
        "title": "Replica identity",
        "url": "https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY",
        "hint": "REPLICA IDENTITY FULL helps replicate UPDATE/DELETE when no suitable primary key exists.",
    },
}

_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_RESERVED_CUSTOM_TABLE_NAMES = frozenset({"logical_demo_events"})
_ALLOWED_PG_TYPES: dict[str, sql.SQL] = {
    "TEXT": sql.SQL("TEXT"),
    "INTEGER": sql.SQL("INTEGER"),
    "BIGINT": sql.SQL("BIGINT"),
    "BOOLEAN": sql.SQL("BOOLEAN"),
    "NUMERIC": sql.SQL("NUMERIC"),
    "DOUBLE PRECISION": sql.SQL("DOUBLE PRECISION"),
    "TIMESTAMPTZ": sql.SQL("TIMESTAMPTZ"),
}

_CUSTOM_TABLE_MAX_COLUMNS = 16
_CUSTOM_INSERT_MAX_ROWS = 80
_MAX_UNIQUE_CONSTRAINTS = 8
_MAX_FOREIGN_KEYS = 8
_MAX_CATALOG_FK_DEPTH = 14

_RESERVED_HUB_SCHEMAS = frozenset({"pg_catalog", "information_schema", "pg_toast"})

_FAKER_PRESETS: dict[str, list[tuple[str, str, bool]]] = {
    "mixed": [
        ("sku", "TEXT", True),
        ("label", "TEXT", False),
        ("qty", "INTEGER", False),
        ("unit_price", "NUMERIC", False),
        ("tax_rate", "DOUBLE PRECISION", False),
        ("is_active", "BOOLEAN", True),
        ("note", "TEXT", False),
        ("external_ref", "BIGINT", False),
        ("created_at", "TIMESTAMPTZ", False),
        ("region_code", "TEXT", True),
        ("priority", "INTEGER", False),
        ("score", "DOUBLE PRECISION", False),
        ("approved", "BOOLEAN", False),
        ("description", "TEXT", False),
        ("batch_id", "BIGINT", False),
        ("checksum", "TEXT", False),
    ],
    "people": [
        ("full_name", "TEXT", True),
        ("email", "TEXT", True),
        ("phone", "TEXT", False),
        ("city", "TEXT", False),
        ("country", "TEXT", True),
        ("age_years", "INTEGER", False),
        ("verified", "BOOLEAN", True),
        ("joined_at", "TIMESTAMPTZ", False),
        ("bio_snippet", "TEXT", False),
        ("loyalty_points", "INTEGER", False),
        ("latitude", "DOUBLE PRECISION", False),
        ("longitude", "DOUBLE PRECISION", False),
        ("external_id", "BIGINT", False),
        ("locale", "TEXT", False),
        ("nickname", "TEXT", False),
        ("account_tier", "INTEGER", False),
    ],
    "commerce": [
        ("order_code", "TEXT", True),
        ("buyer_email", "TEXT", True),
        ("sku", "TEXT", True),
        ("qty", "INTEGER", False),
        ("unit_price", "NUMERIC", False),
        ("tax_rate", "DOUBLE PRECISION", False),
        ("free_shipping", "BOOLEAN", False),
        ("placed_at", "TIMESTAMPTZ", False),
        ("warehouse", "TEXT", False),
        ("tracking_id", "BIGINT", False),
        ("memo", "TEXT", False),
        ("discount_pct", "NUMERIC", False),
        ("gift_wrap", "BOOLEAN", False),
        ("region", "TEXT", False),
        ("line_seq", "INTEGER", False),
        ("checksum", "TEXT", False),
    ],
}


def _faker_instance(seed: int | None) -> Faker:
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)
    return fake


def _fake_cell(fake: Faker, col_name: str, pg_type: str) -> Any:
    """Scalar JSON-friendly value for logical_custom_insert_rows."""
    n = col_name.lower()
    pt = pg_type.strip().upper().replace("  ", " ")
    if pt == "DOUBLE":
        pt = "DOUBLE PRECISION"

    if "email" in n or n.endswith("_email"):
        return fake.email()[:320]
    if "phone" in n or "fax" in n:
        return fake.phone_number()[:40]
    if "city" in n:
        return fake.city()[:120]
    if "country" in n:
        return fake.country()[:120]
    if "region" in n or n.endswith("_code"):
        return (fake.country_code() + fake.bothify(text="??")).strip()[:16]
    if "sku" in n:
        return fake.bothify(text="SKU-###??", letters="ABCDEFGHJKLMNPQRSTUVWXYZ")[:48]
    if "checksum" in n:
        return fake.sha256()[:64]
    if "order_code" in n or "tracking" in n or "batch_id" in n or "external_ref" in n:
        if pt == "BIGINT":
            return fake.random_int(1, 9_000_000_000)
        return fake.bothify(text="??########", letters="ABCDEFGHJKLMNPQRSTUVWXYZ")[:32]
    if "name" in n or "nickname" in n or "label" in n or "warehouse" in n:
        return fake.company()[:160] if "warehouse" in n else fake.name()[:200]
    if "memo" in n or "note" in n or "bio" in n or "description" in n:
        return fake.text(max_nb_chars=140).replace("\n", " ").strip()[:400]
    if "url" in n or "uri" in n:
        return fake.url()[:400]
    if "locale" in n:
        return fake.locale()[:16]

    if pt == "TEXT":
        return fake.pystr(min_chars=4, max_chars=48)
    if pt == "INTEGER":
        return fake.random_int(0, min(2_147_483_647, 10**9))
    if pt == "BIGINT":
        return fake.random_int(1, 9_000_000_000)
    if pt == "BOOLEAN":
        return fake.boolean()
    if pt == "NUMERIC":
        return round(fake.random.uniform(0, 99_999.9999), 4)
    if pt == "DOUBLE PRECISION":
        return round(fake.random.uniform(-180.0, 180.0), 8)
    if pt == "TIMESTAMPTZ":
        dt = fake.date_time_between(
            start_date="-3y", end_date="now", tzinfo=timezone.utc
        )
        return dt.isoformat()
    return fake.pystr(min_chars=3, max_chars=40)


def _slug_pg_ident_fragment(s: str, max_len: int = 22) -> str:
    """Lowercase snake-ish fragment safe to embed in an unquoted PostgreSQL identifier."""
    t = re.sub(r"[^a-z0-9]+", "_", str(s).lower().strip())
    t = re.sub(r"_+", "_", t).strip("_")
    return (t or "x")[:max_len]


def _faker_meaningful_table_name(
    fake: Faker, *, preset: str, schema_slug: str
) -> str:
    """Readable table name (still validated server-side on create)."""
    sch = _slug_pg_ident_fragment(schema_slug, 14)
    if preset == "people":
        a = _slug_pg_ident_fragment(fake.job(), 18)
        b = _slug_pg_ident_fragment(fake.last_name(), 18)
    elif preset == "commerce":
        a = _slug_pg_ident_fragment(fake.catch_phrase(), 18)
        b = _slug_pg_ident_fragment(fake.company(), 18)
    else:
        a = _slug_pg_ident_fragment(fake.word(), 18)
        b = _slug_pg_ident_fragment(fake.word(), 18)
    tail = secrets.token_hex(2)
    base = f"{a}_{b}_{sch}_{tail}"
    if not re.match(r"^[a-z]", base):
        base = f"t_{base}"
    return base[:63]


def faker_generate_custom_columns(
    *,
    preset: str,
    column_count: int,
    seed: int | None,
    meaningful_table_name: bool = False,
    schema_slug: str = "pub",
) -> dict[str, Any]:
    """Build column definitions for custom logical tables using Faker-backed presets."""
    if preset not in _FAKER_PRESETS:
        raise ValueError(f"unknown preset {preset!r}; use one of: {sorted(_FAKER_PRESETS)}")
    n = max(1, min(column_count, _CUSTOM_TABLE_MAX_COLUMNS))
    blueprint = _FAKER_PRESETS[preset][:n]
    fake = _faker_instance(seed)
    if meaningful_table_name:
        ss = _validate_pg_identifier(str(schema_slug or "pub"), ctx="schema (for table name)")
        table_name_suggestion = _faker_meaningful_table_name(
            fake, preset=preset, schema_slug=ss
        )
    else:
        table_name_suggestion = f"f_{preset}_{secrets.token_hex(4)}"
    # Touch Faker so seeded runs differ from totally idle RNG where relevant
    _ = fake.pystr(max_chars=1)
    columns = [
        {"name": name, "pg_type": pg_t, "not_null": nn} for name, pg_t, nn in blueprint
    ]
    return {
        "ok": True,
        "preset": preset,
        "column_count": len(columns),
        "seed": seed,
        "table_name_suggestion": table_name_suggestion,
        "meaningful_table_name": meaningful_table_name,
        "columns": columns,
    }


def faker_generate_custom_rows(
    *,
    columns: list[dict[str, Any]],
    row_count: int,
    seed: int | None,
) -> dict[str, Any]:
    """Generate insert rows matching custom column names/types (omit ``id`` — BIGSERIAL)."""
    if not columns:
        raise ValueError("columns required")
    if len(columns) > _CUSTOM_TABLE_MAX_COLUMNS:
        raise ValueError(f"too many columns (max {_CUSTOM_TABLE_MAX_COLUMNS})")
    k = max(1, min(row_count, _CUSTOM_INSERT_MAX_ROWS))
    fake = _faker_instance(seed)
    rows: list[dict[str, Any]] = []
    for _ in range(k):
        row: dict[str, Any] = {}
        for raw in columns:
            cn = str(raw["name"])
            if cn == "id":
                raise ValueError(
                    'omit generated column "id" from columns when using BIGSERIAL — '
                    "remove it from Columns JSON for Faker rows"
                )
            pg_type = str(raw["pg_type"])
            row[cn] = _fake_cell(fake, cn, pg_type)
        rows.append(row)
    return {"ok": True, "rows": rows, "row_count": len(rows), "seed": seed}


def _map_information_schema_type(
    data_type: str | None, udt_name: str | None
) -> str | None:
    """Map ``information_schema.columns`` types to hub Faker whitelist labels."""
    dt = (data_type or "").lower().strip()
    udt = (udt_name or "").lower().strip()
    if dt in ("character varying", "character", "text") or udt in ("varchar", "bpchar", "text"):
        return "TEXT"
    if dt == "integer" or udt == "int4":
        return "INTEGER"
    if dt == "smallint" or udt == "int2":
        return "INTEGER"
    if dt == "bigint" or udt == "int8":
        return "BIGINT"
    if dt == "boolean" or udt == "bool":
        return "BOOLEAN"
    if dt == "numeric" or udt == "numeric":
        return "NUMERIC"
    if dt in ("double precision", "real") or udt in ("float8", "float4"):
        return "DOUBLE PRECISION"
    if dt == "timestamp with time zone" or udt == "timestamptz":
        return "TIMESTAMPTZ"
    if dt == "timestamp without time zone" or udt == "timestamp":
        return "TIMESTAMPTZ"
    return None


def _skip_catalog_insert_column(
    *,
    column_default: str | None,
    is_identity: str | None,
    is_generated: str | None,
) -> bool:
    """Columns handled by PostgreSQL (identity, serial, generated)."""
    if (is_identity or "").upper() == "YES":
        return True
    gen = (is_generated or "").upper()
    if gen and gen != "NEVER":
        return True
    d = column_default or ""
    if "nextval(" in d.lower():
        return True
    return False


def _catalog_column_has_non_serial_default(column_default: str | None) -> bool:
    if not column_default or not str(column_default).strip():
        return False
    if "nextval(" in str(column_default).lower():
        return False
    return True


def introspect_table_catalog_summary(
    demo_dsn_base: str,
    database: str,
    schema_name: str,
    table_name: str,
) -> dict[str, Any]:
    """Describe columns (hub types) and outgoing FKs for one table — ``demo`` role SELECT only."""
    sch = _validate_pg_identifier(schema_name or "public", ctx="schema")
    tn = _validate_pg_identifier(table_name, ctx="table_name")
    if sch in _RESERVED_HUB_SCHEMAS:
        raise ValueError(f"schema {sch!r} is reserved")
    if database not in (PUB_DB, SUB_DB):
        raise ValueError(f"database must be {PUB_DB!r} or {SUB_DB!r}")
    meta = _fetch_table_catalog_meta(demo_dsn_base, database, sch, tn)
    return {
        "ok": True,
        "database": database,
        "schema": sch,
        "table": tn,
        "columns": meta["columns_display"],
        "foreign_keys": meta["foreign_keys"],
        "insert_without_catalog": meta["user_fill_preview"],
    }


def _fetch_table_catalog_meta(
    demo_dsn_base: str, database: str, schema_name: str, table_name: str
) -> dict[str, Any]:
    ci = _with_db(demo_dsn_base, database)
    with psycopg.connect(ci) as conn:
        with conn.cursor() as cur:
            return _read_table_catalog_meta(cur, schema_name, table_name)


def _read_table_catalog_meta(
    cur: psycopg.Cursor, schema_name: str, table_name: str
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT column_name, data_type, udt_name, is_nullable,
               column_default, is_identity, is_generated
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema_name, table_name),
    )
    raw_cols = cur.fetchall()
    if not raw_cols:
        raise ValueError(
            f"no columns found for {schema_name}.{table_name} — check schema/name and demo privileges"
        )

    cur.execute(
        """
        SELECT tc.constraint_name, kcu.ordinal_position,
               kcu.column_name AS fk_column,
               ccu.table_schema AS ref_schema,
               ccu.table_name AS ref_table,
               ccu.column_name AS ref_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_catalog = kcu.constraint_catalog
          AND tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.referential_constraints rc
          ON rc.constraint_catalog = tc.constraint_catalog
          AND rc.constraint_schema = tc.constraint_schema
          AND rc.constraint_name = tc.constraint_name
        JOIN information_schema.key_column_usage ccu
          ON ccu.constraint_catalog = rc.unique_constraint_catalog
          AND ccu.constraint_schema = rc.unique_constraint_schema
          AND ccu.constraint_name = rc.unique_constraint_name
          AND ccu.ordinal_position = kcu.ordinal_position
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'FOREIGN KEY'
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """,
        (schema_name, table_name),
    )
    fk_raw = cur.fetchall()
    fk_groups: dict[str, dict[str, Any]] = {}
    for conname, ord_pos, fk_col, ref_sch, ref_tbl, ref_col in fk_raw:
        g = fk_groups.setdefault(
            conname,
            {
                "pairs": [],
                "ref_schema": ref_sch,
                "ref_table": ref_tbl,
            },
        )
        g["pairs"].append((ord_pos, fk_col, ref_col))
    foreign_keys: list[dict[str, Any]] = []
    fk_local_columns: set[str] = set()
    for _cn, g in sorted(fk_groups.items()):
        pairs = sorted(g["pairs"], key=lambda z: z[0])
        lc = [p[1] for p in pairs]
        rc = [p[2] for p in pairs]
        fk_local_columns.update(lc)
        foreign_keys.append(
            {
                "columns": [_validate_pg_identifier(str(c), ctx="FK column") for c in lc],
                "ref_schema": _validate_pg_identifier(str(g["ref_schema"]), ctx="FK ref schema"),
                "ref_table": _validate_pg_identifier(str(g["ref_table"]), ctx="FK ref table"),
                "ref_columns": [_validate_pg_identifier(str(c), ctx="FK ref column") for c in rc],
            }
        )

    columns_display: list[dict[str, Any]] = []
    user_fill: list[dict[str, Any]] = []
    user_fill_preview: list[str] = []

    for (
        col_name,
        data_type,
        udt_name,
        is_nullable,
        column_default,
        is_identity,
        is_generated,
    ) in raw_cols:
        cn = _validate_pg_identifier(str(col_name), ctx="column")
        mapped = _map_information_schema_type(data_type, udt_name)
        skip_auto = _skip_catalog_insert_column(
            column_default=column_default,
            is_identity=is_identity,
            is_generated=is_generated,
        )
        has_other_default = _catalog_column_has_non_serial_default(column_default)
        nullable = (is_nullable or "").upper() == "YES"

        entry = {
            "name": cn,
            "data_type": data_type,
            "hub_pg_type": mapped,
            "nullable": nullable,
            "skip_insert": skip_auto,
            "uses_database_default": has_other_default and not skip_auto,
            "is_foreign_key_column": cn in fk_local_columns,
        }
        columns_display.append(entry)

        if skip_auto:
            continue
        if cn in fk_local_columns:
            continue
        if has_other_default:
            continue
        if mapped is None:
            raise ValueError(
                f'column {cn!r} has type {data_type!r} — hub Faker catalog insert supports only: '
                f'{", ".join(sorted(_ALLOWED_PG_TYPES))}'
            )
        user_fill.append({"name": cn, "pg_type": mapped, "not_null": not nullable})
        user_fill_preview.append(cn)

    return {
        "foreign_keys": foreign_keys,
        "fk_local_columns": fk_local_columns,
        "user_fill": user_fill,
        "columns_display": columns_display,
        "user_fill_preview": user_fill_preview,
    }


def _catalog_row_count(
    cur: psycopg.Cursor, schema_name: str, table_name: str
) -> int:
    cur.execute(
        sql.SQL("SELECT COUNT(*) FROM {}").format(
            sql.Identifier(schema_name, table_name)
        )
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _sample_parent_tuple(
    cur: psycopg.Cursor,
    ref_schema: str,
    ref_table: str,
    ref_columns: list[str],
) -> tuple[Any, ...] | None:
    sel = sql.SQL(", ").join(sql.Identifier(c) for c in ref_columns)
    q = sql.SQL("SELECT {} FROM {} ORDER BY random() LIMIT 1").format(
        sel,
        sql.Identifier(ref_schema, ref_table),
    )
    cur.execute(q)
    row = cur.fetchone()
    return tuple(row) if row else None


def _build_catalog_faker_rows(
    cur: psycopg.Cursor,
    meta: dict[str, Any],
    row_count: int,
    seed: int | None,
) -> list[dict[str, Any]]:
    fake = _faker_instance(seed)
    fills = meta["user_fill"]
    fk_groups = meta["foreign_keys"]
    rows_out: list[dict[str, Any]] = []
    for _ in range(row_count):
        row: dict[str, Any] = {}
        for fk in fk_groups:
            tup = _sample_parent_tuple(
                cur, fk["ref_schema"], fk["ref_table"], fk["ref_columns"]
            )
            if tup is None:
                raise ValueError(
                    f'cannot sample foreign key: referenced table {fk["ref_schema"]}.{fk["ref_table"]} '
                    "has no rows"
                )
            if len(tup) != len(fk["columns"]):
                raise ValueError("internal FK shape mismatch")
            for lc, val in zip(fk["columns"], tup):
                row[lc] = val
        for col in fills:
            nm = col["name"]
            if nm in row:
                continue
            row[nm] = _fake_cell(fake, nm, str(col["pg_type"]))
        rows_out.append(row)
    return rows_out


def faker_catalog_insert_rows(
    publisher_demo_dsn: str,
    subscriber_demo_dsn: str,
    database: str,
    schema_name: str,
    table_name: str,
    row_count: int,
    seed: int | None,
    *,
    ensure_parent_rows: bool = True,
    _stack: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Generate INSERT rows using live catalog metadata: FK columns sampled from parents."""
    if database not in (PUB_DB, SUB_DB):
        raise ValueError(f"database must be {PUB_DB!r} or {SUB_DB!r}")
    sch = _validate_pg_identifier(schema_name or "public", ctx="schema")
    tn = _validate_pg_identifier(table_name, ctx="table_name")
    if sch in _RESERVED_HUB_SCHEMAS:
        raise ValueError(f"schema {sch!r} is reserved")
    if tn in _RESERVED_CUSTOM_TABLE_NAMES and sch == "public":
        raise ValueError(f"use the hub Insert button for reserved table {tn!r}")

    k = max(1, min(int(row_count), _CUSTOM_INSERT_MAX_ROWS))
    demo_base = publisher_demo_dsn if database == PUB_DB else subscriber_demo_dsn
    ci = _with_db(demo_base, database)
    here = f"{sch}.{tn}"
    if here in _stack:
        raise ValueError(
            f"circular foreign-key path involving {here} — insert a root row manually "
            "(e.g. nullable self-FK) or break the cycle"
        )
    if len(_stack) > _MAX_CATALOG_FK_DEPTH:
        raise ValueError("foreign-key dependency chain too deep for this demo helper")

    touched: list[str] = []

    with psycopg.connect(ci) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            meta = _read_table_catalog_meta(cur, sch, tn)

            for fk in meta["foreign_keys"]:
                rs, rt = fk["ref_schema"], fk["ref_table"]
                rkey = f"{rs}.{rt}"
                if rs == sch and rt == tn:
                    nself = _catalog_row_count(cur, sch, tn)
                    if nself < 1:
                        raise ValueError(
                            f"table {here} has a self-referencing foreign key and is empty — "
                            "insert one root row first (e.g. leave the FK column NULL if allowed), "
                            "then use catalog insert again."
                        )
                    continue

                n = _catalog_row_count(cur, rs, rt)
                if n >= 1:
                    continue
                if not ensure_parent_rows:
                    raise ValueError(
                        f"table {here} references {rkey}, which has no rows — "
                        "seed the parent first or set ensure_parent_rows=true"
                    )
                sub = faker_catalog_insert_rows(
                    publisher_demo_dsn,
                    subscriber_demo_dsn,
                    database,
                    rs,
                    rt,
                    1,
                    seed,
                    ensure_parent_rows=True,
                    _stack=(*_stack, here),
                )
                touched.append(rkey)
                if not sub.get("ok"):
                    return sub

            rows = _build_catalog_faker_rows(cur, meta, k, seed)

    ins = logical_custom_insert_rows_for_database(
        publisher_demo_dsn,
        subscriber_demo_dsn,
        database,
        tn,
        rows,
        schema_name=sch,
    )
    if ins.get("ok"):
        ins["catalog"] = {
            "from_catalog": True,
            "ensure_parent_rows": ensure_parent_rows,
            "parents_materialized": touched,
            "foreign_keys": meta["foreign_keys"],
            "filled_columns": meta["user_fill_preview"],
        }
    return ins


def pg_documentation_links() -> dict[str, dict[str, str]]:
    """Stable URLs + short hints (paraphrased; see official docs for normative text)."""
    return dict(PG_DOCUMENTATION_LINKS)


def _validate_pg_identifier(name: str, *, ctx: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"invalid {ctx}: {name!r} — use lowercase letter then letters/digits/underscore (≤63 chars)"
        )
    return name


def ensure_schema_for_hub(admin_dsn_for_db: str, dbname: str, schema: str) -> None:
    """Create schema if missing and grant USAGE to ``demo`` (skip ``public``)."""
    sch = _validate_pg_identifier(schema, ctx="schema")
    if sch in _RESERVED_HUB_SCHEMAS:
        raise ValueError(f"schema {sch!r} is reserved — choose another name")
    if sch == "public":
        return
    adm = _with_db(admin_dsn_for_db, dbname)
    with psycopg.connect(adm, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(sch)))
            cur.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO demo").format(sql.Identifier(sch))
            )


def logical_custom_create_table(
    publisher_admin_dsn: str,
    subscriber_admin_dsn: str,
    table_name: str,
    columns: list[dict[str, Any]],
    *,
    target: str,
    include_bigserial_pk: bool,
    replica_identity_full: bool,
    unique_constraints: list[list[str]] | None = None,
    foreign_keys: list[dict[str, Any]] | None = None,
    schema_name: str = "public",
) -> dict[str, Any]:
    """CREATE TABLE IF NOT EXISTS on publisher and/or subscriber with safe, whitelisted types."""
    tn = _validate_pg_identifier(table_name, ctx="table_name")
    sch = _validate_pg_identifier(schema_name or "public", ctx="schema")
    if sch in _RESERVED_HUB_SCHEMAS:
        raise ValueError(f"schema {sch!r} is reserved — choose another name")
    if tn in _RESERVED_CUSTOM_TABLE_NAMES and sch == "public":
        raise ValueError(
            f"table name {tn!r} is reserved for hub setup — choose another name"
        )
    out: dict[str, Any] = {"ok": True, "table": f"{sch}.{tn}", "schema": sch, "target": target}
    if len(columns) > _CUSTOM_TABLE_MAX_COLUMNS:
        raise ValueError(f"too many columns (max {_CUSTOM_TABLE_MAX_COLUMNS})")
    seen: set[str] = set()
    parts: list[sql.Composable] = []
    if include_bigserial_pk:
        parts.append(sql.SQL("id BIGSERIAL PRIMARY KEY"))
        seen.add("id")
    for raw in columns:
        cn = _validate_pg_identifier(str(raw["name"]), ctx="column name")
        if cn in seen:
            raise ValueError(f"duplicate column: {cn}")
        seen.add(cn)
        pg_type = str(raw["pg_type"]).strip().upper().replace("  ", " ")
        if pg_type == "DOUBLE":
            pg_type = "DOUBLE PRECISION"
        if pg_type not in _ALLOWED_PG_TYPES:
            raise ValueError(
                f"unsupported type {raw['pg_type']!r}; allowed: {', '.join(sorted(_ALLOWED_PG_TYPES))}"
            )
        frag = sql.SQL("{} {}").format(
            sql.Identifier(cn), _ALLOWED_PG_TYPES[pg_type]
        )
        if raw.get("not_null"):
            frag += sql.SQL(" NOT NULL")
        parts.append(frag)
    if not parts:
        raise ValueError("no columns — add columns or enable include_bigserial_pk")

    uq_list = unique_constraints or []
    if len(uq_list) > _MAX_UNIQUE_CONSTRAINTS:
        raise ValueError(f"too many UNIQUE constraints (max {_MAX_UNIQUE_CONSTRAINTS})")
    for gi, colgroup in enumerate(uq_list):
        if not colgroup:
            raise ValueError(f"UNIQUE constraint {gi}: need at least one column")
        if len(colgroup) > _CUSTOM_TABLE_MAX_COLUMNS:
            raise ValueError("too many columns in one UNIQUE constraint")
        uq_idents: list[sql.Identifier] = []
        for c in colgroup:
            cq = _validate_pg_identifier(str(c), ctx="UNIQUE column")
            if cq not in seen:
                raise ValueError(f"UNIQUE references unknown column {cq!r}")
            uq_idents.append(sql.Identifier(cq))
        uq_name = _validate_pg_identifier(f"{tn}_uq_{gi}", ctx="constraint name")
        parts.append(
            sql.SQL("CONSTRAINT {} UNIQUE ({})").format(
                sql.Identifier(uq_name),
                sql.SQL(", ").join(uq_idents),
            )
        )

    fk_list = foreign_keys or []
    if len(fk_list) > _MAX_FOREIGN_KEYS:
        raise ValueError(f"too many FOREIGN KEY constraints (max {_MAX_FOREIGN_KEYS})")
    for gi, fkraw in enumerate(fk_list):
        fk_cols = fkraw.get("columns")
        ref_table = fkraw.get("ref_table")
        ref_cols = fkraw.get("ref_columns")
        if not fk_cols or not ref_table or not ref_cols:
            raise ValueError(
                f"FK {gi}: requires columns, ref_table, and ref_columns lists"
            )
        if len(fk_cols) != len(ref_cols):
            raise ValueError(f"FK {gi}: columns and ref_columns must have same length")
        rt = _validate_pg_identifier(str(ref_table), ctx="referenced table")
        ref_sch = _validate_pg_identifier(
            str(fkraw.get("ref_schema") or "public"), ctx="FK referenced schema"
        )
        if ref_sch in _RESERVED_HUB_SCHEMAS:
            raise ValueError(f"FK {gi}: referenced schema {ref_sch!r} is reserved")
        fk_name = _validate_pg_identifier(f"{tn}_fk_{gi}", ctx="constraint name")
        loc_idents: list[sql.Identifier] = []
        ref_idents: list[sql.Identifier] = []
        for c in fk_cols:
            cq = _validate_pg_identifier(str(c), ctx="FK column")
            if cq not in seen:
                raise ValueError(f"FK column {cq!r} is not defined on this table")
            loc_idents.append(sql.Identifier(cq))
        for c in ref_cols:
            rc = _validate_pg_identifier(str(c), ctx="referenced column")
            ref_idents.append(sql.Identifier(rc))
        parts.append(
            sql.SQL("CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})").format(
                sql.Identifier(fk_name),
                sql.SQL(", ").join(loc_idents),
                sql.Identifier(ref_sch, rt),
                sql.SQL(", ").join(ref_idents),
            )
        )

    create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(sch, tn),
        sql.SQL(", ").join(parts),
    )

    def grants_and_identity(admin_dsn_for_db: str, dbname: str) -> None:
        adm = _with_db(admin_dsn_for_db, dbname)
        with psycopg.connect(adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                ensure_schema_for_hub(admin_dsn_for_db, dbname, sch)
                cur.execute(create_stmt)
                if replica_identity_full:
                    cur.execute(
                        sql.SQL(
                            "ALTER TABLE {} REPLICA IDENTITY FULL"
                        ).format(sql.Identifier(sch, tn))
                    )
                cur.execute(
                    sql.SQL(
                        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {} TO demo"
                    ).format(sql.Identifier(sch, tn))
                )
                if include_bigserial_pk:
                    seq = sql.Identifier(sch, f"{tn}_id_seq")
                    cur.execute(
                        sql.SQL(
                            "GRANT USAGE, SELECT ON SEQUENCE {} TO demo"
                        ).format(seq)
                    )

    targets = []
    if target in ("publisher", "both"):
        grants_and_identity(publisher_admin_dsn, PUB_DB)
        targets.append("publisher")
    if target in ("subscriber", "both"):
        grants_and_identity(subscriber_admin_dsn, SUB_DB)
        targets.append("subscriber")
    out["applied_to"] = targets
    out["hint"] = (
        "After CREATE TABLE on publisher: ALTER PUBLICATION … ADD TABLE … then "
        "subscriber CREATE TABLE + ALTER SUBSCRIPTION … REFRESH PUBLICATION "
        "(see PostgreSQL docs via info icons)."
    )
    return out


def logical_custom_insert_rows_for_database(
    publisher_demo_dsn: str,
    subscriber_demo_dsn: str,
    database: str,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    schema_name: str = "public",
) -> dict[str, Any]:
    """Parameterized INSERT using ``demo`` into ``demo_logical_pub`` or ``demo_logical_sub``."""
    if database not in (PUB_DB, SUB_DB):
        raise ValueError(
            f"database must be {PUB_DB!r} or {SUB_DB!r}, not {database!r}"
        )
    tn = _validate_pg_identifier(table_name, ctx="table_name")
    sch = _validate_pg_identifier(schema_name or "public", ctx="schema")
    if sch in _RESERVED_HUB_SCHEMAS:
        raise ValueError(f"schema {sch!r} is reserved — choose another name")
    if tn in _RESERVED_CUSTOM_TABLE_NAMES:
        raise ValueError(
            f"use the hub Insert button for reserved table {tn!r}"
        )
    if not rows:
        raise ValueError("rows required")
    if len(rows) > _CUSTOM_INSERT_MAX_ROWS:
        raise ValueError(f"too many rows (max {_CUSTOM_INSERT_MAX_ROWS})")

    cols = sorted(rows[0].keys())
    for k in cols:
        _validate_pg_identifier(str(k), ctx="column key")
    for i, row in enumerate(rows):
        if set(row.keys()) != set(cols):
            raise ValueError(f"row {i}: keys must match first row")
        for ck, val in row.items():
            if isinstance(val, (dict, list)):
                raise ValueError(f"row {i} column {ck}: only scalar JSON types allowed")

    base = publisher_demo_dsn if database == PUB_DB else subscriber_demo_dsn
    ci = _with_db(base, database)

    try:
        with psycopg.connect(ci) as conn:
            with conn.cursor() as cur:
                if not cols:
                    default_stmt = sql.SQL("INSERT INTO {} DEFAULT VALUES").format(
                        sql.Identifier(sch, tn)
                    )
                    for _ in rows:
                        cur.execute(default_stmt)
                else:
                    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(cols))
                    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(sch, tn),
                        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                        placeholders,
                    )
                    batch = [tuple(row[c] for c in cols) for row in rows]
                    cur.executemany(insert_stmt, batch)
            conn.commit()
        return {
            "ok": True,
            "inserted": len(rows),
            "table": f"{sch}.{tn}",
            "schema": sch,
            "database": database,
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "hint": (
                "Ensure the table exists on this database, demo has INSERT (+ sequence USAGE "
                "if BIGSERIAL), column names and FK/UNIQUE rules match."
            ),
        }


def logical_custom_insert_rows(
    publisher_demo_dsn: str,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    schema_name: str = "public",
) -> dict[str, Any]:
    """Parameterized INSERT into publisher logical DB for arbitrary user-defined table."""
    return logical_custom_insert_rows_for_database(
        publisher_demo_dsn,
        publisher_demo_dsn,
        PUB_DB,
        table_name,
        rows,
        schema_name=schema_name,
    )


def logical_demo_insert_row(demo_dsn_base: str, note: str) -> dict[str, Any]:
    """Insert using ``demo`` user into publisher DB."""
    pub = _with_db(demo_dsn_base, PUB_DB)
    try:
        with psycopg.connect(pub) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO public.{TABLE} (note) VALUES (%s) RETURNING id, created_at",
                    (note[:4000],),
                )
                rid, ts = cur.fetchone()
            conn.commit()
        return {"ok": True, "id": rid, "created_at": ts.isoformat() if ts else None}
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "hint": "Run setup again if grants changed; demo needs INSERT on the table and USAGE on the BIGSERIAL sequence.",
        }


def logical_demo_status(
    subscriber_admin_dsn: str,
    publisher_demo_dsn: str,
    subscriber_demo_dsn: str,
    replica_read_dsn: str | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ok": True,
        "setup_revision": SETUP_REVISION,
        "publisher_database": PUB_DB,
        "subscriber_database": SUB_DB,
        "table": f"public.{TABLE}",
    }
    pub = _with_db(publisher_demo_dsn, PUB_DB)
    sub = _with_db(subscriber_demo_dsn, SUB_DB)

    try:
        with psycopg.connect(pub) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM public.{TABLE}")
                out["publisher_rows"] = cur.fetchone()[0]
    except Exception as e:
        out["publisher_rows"] = None
        out["publisher_error"] = str(e)

    try:
        with psycopg.connect(sub) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM public.{TABLE}")
                out["subscriber_rows"] = cur.fetchone()[0]
    except Exception as e:
        out["subscriber_rows"] = None
        out["subscriber_error"] = str(e)

    if replica_read_dsn and replica_read_dsn.strip():
        try:
            with psycopg.connect(replica_read_dsn.strip()) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM public.{TABLE}")
                    out["replica_read_rows"] = cur.fetchone()[0]
                out["replica_read_hint"] = (
                    "Read from POSTGRES_REPLICA_READ_DSN (physical standby of primary); "
                    "expects database ``demo_logical_pub`` — compare replica vs primary publisher row counts."
                )
        except Exception as e:
            out["replica_read_error"] = str(e)

    try:
        sub_adm = _with_db(subscriber_admin_dsn, SUB_DB)
        with psycopg.connect(sub_adm, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT subname, subenabled FROM pg_subscription WHERE subname = %s",
                    (SUBSCRIPTION,),
                )
                row = cur.fetchone()
                out["subscription"] = (
                    {"name": row[0], "enabled": bool(row[1])} if row else None
                )
    except Exception as e:
        out["subscription_error"] = str(e)

    return out
