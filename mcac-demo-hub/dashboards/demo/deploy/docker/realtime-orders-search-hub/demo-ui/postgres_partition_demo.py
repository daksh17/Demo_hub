"""
Declarative partitioning playground on database ``demo``: RANGE + pg_partman + pg_cron,
plus LIST and HASH examples with manual child partitions and Faker seed rows.
"""
from __future__ import annotations

import re
import textwrap
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.errors as pg_errors
from psycopg import sql
from faker import Faker

PartitionKind = Literal["range", "list", "hash"]

_CRON_JOB = "hub_demo_partman_maint"
_PARTMAN_CRON_CMD_MARKER = "run_maintenance_proc"


def _cron_jobs_where_clause() -> str:
    """Match hub demo jobs whether scheduled by name or legacy INSERT (no jobname)."""
    return """
        jobname = %s
        OR (
          COALESCE(database, '') = 'demo'
          AND position(%s in command) > 0
        )
    """
_RANGE_PARENT = "public.hub_part_range_events"
_LIST_PARENT = "public.hub_part_list_orders"
_HASH_PARENT = "public.hub_part_hash_metrics"

_RANGE_PARENT_DDL = textwrap.dedent(
    """
    CREATE TABLE public.hub_part_range_events (
      id bigint GENERATED ALWAYS AS IDENTITY,
      event_day date NOT NULL,
      customer_email text,
      region text,
      amount_cents integer NOT NULL DEFAULT 0,
      notes text,
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (id, event_day)
    ) PARTITION BY RANGE (event_day);
    """
).strip()

_LIST_PARENT_DDL = textwrap.dedent(
    """
    CREATE TABLE public.hub_part_list_orders (
      id bigint GENERATED ALWAYS AS IDENTITY,
      channel text NOT NULL,
      order_ref text NOT NULL,
      amount_cents integer NOT NULL DEFAULT 0,
      sku text,
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (id, channel)
    ) PARTITION BY LIST (channel)
    """
).strip()

_LIST_CHILD_STMTS: tuple[str, ...] = (
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_list_orders_online
          PARTITION OF public.hub_part_list_orders FOR VALUES IN ('online')
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_list_orders_retail
          PARTITION OF public.hub_part_list_orders FOR VALUES IN ('retail')
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_list_orders_partner
          PARTITION OF public.hub_part_list_orders FOR VALUES IN ('partner')
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_list_orders_wholesale
          PARTITION OF public.hub_part_list_orders FOR VALUES IN ('wholesale')
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_list_orders_default
          PARTITION OF public.hub_part_list_orders DEFAULT
        """
    ).strip(),
)

_HASH_PARENT_DDL = textwrap.dedent(
    """
    CREATE TABLE public.hub_part_hash_metrics (
      id bigint GENERATED ALWAYS AS IDENTITY,
      shard_key integer NOT NULL,
      metric_name text NOT NULL,
      metric_value double precision,
      observed_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (shard_key, id)
    ) PARTITION BY HASH (shard_key)
    """
).strip()

_HASH_CHILD_STMTS: tuple[str, ...] = (
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_hash_metrics_p0
          PARTITION OF public.hub_part_hash_metrics
          FOR VALUES WITH (MODULUS 4, REMAINDER 0)
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_hash_metrics_p1
          PARTITION OF public.hub_part_hash_metrics
          FOR VALUES WITH (MODULUS 4, REMAINDER 1)
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_hash_metrics_p2
          PARTITION OF public.hub_part_hash_metrics
          FOR VALUES WITH (MODULUS 4, REMAINDER 2)
        """
    ).strip(),
    textwrap.dedent(
        """
        CREATE TABLE public.hub_part_hash_metrics_p3
          PARTITION OF public.hub_part_hash_metrics
          FOR VALUES WITH (MODULUS 4, REMAINDER 3)
        """
    ).strip(),
)


def _quote_ident_sql(ident: str) -> str:
    """Double-quote a PostgreSQL identifier for display scripts."""
    return '"' + ident.replace('"', '""') + '"'


_PARTMAN_SQL_PREVIEW_PLACEHOLDER = "<partman_schema>"


def sql_create_parent_statement(partman_schema: str) -> str:
    if partman_schema == _PARTMAN_SQL_PREVIEW_PLACEHOLDER:
        hint = "  -- replace partman if extension is in another schema (status → partman.extension_schema)"
        return textwrap.dedent(
            f"""
            SELECT partman.create_parent(
                p_parent_table := 'public.hub_part_range_events',
                p_control := 'event_day',
                p_interval := '1 month',
                p_type := 'range',
                p_premake := 4
            );{hint}
            """
        ).strip()
    sch_q = _quote_ident_sql(partman_schema)
    return textwrap.dedent(
        f"""
        SELECT {sch_q}.create_parent(
            p_parent_table := 'public.hub_part_range_events',
            p_control := 'event_day',
            p_interval := '1 month',
            p_type := 'range',
            p_premake := 4
        );
        """
    ).strip()


def sql_call_run_maintenance_statement(partman_schema: str) -> str:
    if partman_schema == _PARTMAN_SQL_PREVIEW_PLACEHOLDER:
        return (
            "CALL partman.run_maintenance_proc();  "
            "-- replace partman if extension is in another schema (status → partman.extension_schema)"
        )
    sch = _quote_ident_sql(partman_schema)
    return f"CALL {sch}.run_maintenance_proc();"


def _sql_dollar_quote_literal(body: str) -> str:
    """Return a PostgreSQL dollar-quoted literal containing ``body`` (safe for embedded quotes)."""
    if "$" not in body:
        return "$$" + body + "$$"
    i = 0
    while True:
        tag = f"p{i}"
        delim = f"${tag}$"
        if delim not in body:
            return delim + body + delim
        i += 1


def sql_cron_schedule_examples(partman_schema: str, schedule: str, jobname: str = _CRON_JOB) -> str:
    """Commands the hub tries when scheduling maintenance (pg_cron metadata DB is usually ``postgres``)."""
    maint = sql_call_run_maintenance_statement(partman_schema)
    sch = jobname.replace("'", "''")
    sched = schedule.replace("'", "''")
    dq = _sql_dollar_quote_literal(maint)
    lines = [
        "-- pg_cron opens a second libpq connection to the job database. Default nodename is localhost (often ::1);",
        "-- if job_run_details shows connection failed, set nodename to '' (Unix socket), e.g.:",
        "-- UPDATE cron.job SET nodename = '' WHERE database = 'demo' AND command LIKE '%run_maintenance_proc%';",
        "",
        "-- Connect to database postgres (superuser). Command runs against database demo:",
        f"SELECT cron.schedule('{sch}', '{sched}', {dq}, 'demo');",
        "",
        "-- If 4-argument cron.schedule is unavailable, INSERT (with jobname):",
        textwrap.dedent(
            f"""
            INSERT INTO cron.job (jobname, schedule, command, database, username)
            VALUES ('{sch}', '{sched}', {dq}, 'demo', current_user);
            """
        ).strip(),
        "",
        "-- Older pg_cron without jobname column:",
        textwrap.dedent(
            f"""
            INSERT INTO cron.job (schedule, command, database, username)
            VALUES ('{sched}', {dq}, 'demo', current_user);
            """
        ).strip(),
        "",
        "-- If schedule must omit database (uses cron.database_name from postgresql.conf):",
        f"SELECT cron.schedule('{sch}', '{sched}', {dq});",
        "",
        f"-- Remove: SELECT cron.unschedule(jobid) FROM cron.job WHERE jobname = '{sch}';",
    ]
    return "\n".join(lines)


_GRANT_DEMO_SQL = (
    "GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO demo;"
)


def partition_demo_sql_blueprint(
    *,
    partition_kind: PartitionKind,
    cron_schedule: str,
    schedule_cron: bool,
    partman_schema: str | None,
) -> dict[str, Any]:
    """
    Human-readable SQL matching ``partition_demo_setup`` / cron helpers.

    ``partman_schema`` is the schema holding pg_partman (from ``pg_extension``); if unknown,
    pass ``None`` and the preview uses a placeholder.
    """
    psch = partman_schema if partman_schema else _PARTMAN_SQL_PREVIEW_PLACEHOLDER
    sections: list[dict[str, Any]] = []

    needs_partman = partition_kind == "range" or schedule_cron
    if needs_partman:
        sections.append(
            {
                "id": "extensions",
                "title": "Extensions (demo DB, superuser)",
                "sql": "CREATE EXTENSION IF NOT EXISTS pg_partman;",
                "note": "RANGE setup and pg_cron maintenance both require pg_partman on database demo.",
            }
        )

    if partition_kind == "range":
        sections.append({"id": "parent", "title": "RANGE parent table", "sql": _RANGE_PARENT_DDL})
        sections.append(
            {
                "id": "partman_register",
                "title": "pg_partman: register parent",
                "sql": sql_create_parent_statement(psch),
                "note": f"Replace {psch!r} with your extension schema (see partition-demo status JSON: partman.extension_schema).",
            }
        )
        sections.append(
            {
                "id": "partman_maint",
                "title": "pg_partman: run maintenance once",
                "sql": sql_call_run_maintenance_statement(psch),
                "note": "Creates premade child partitions; same as the hub Run maintenance now button.",
            }
        )
    elif partition_kind == "list":
        parts_sql = "\n\n".join([_LIST_PARENT_DDL, *_LIST_CHILD_STMTS])
        sections.append(
            {
                "id": "parent_and_children",
                "title": "LIST parent + fixed partitions",
                "sql": parts_sql,
                "note": "partman is not used for this table in the hub demo.",
            }
        )
    else:
        parts_sql = "\n\n".join([_HASH_PARENT_DDL, *_HASH_CHILD_STMTS])
        sections.append(
            {
                "id": "parent_and_children",
                "title": "HASH parent + modulus partitions",
                "sql": parts_sql,
                "note": "partman is not used for this table in the hub demo.",
            }
        )

    if schedule_cron:
        sections.append(
            {
                "id": "pg_cron",
                "title": "pg_cron: schedule partman maintenance",
                "sql": sql_cron_schedule_examples(psch, cron_schedule.strip() or "*/5 * * * *"),
                "note": "Run scheduling statements against the postgres database. The hub unschedules matching jobs before inserting.",
            }
        )

    sections.append(
        {
            "id": "grants",
            "title": "Grants (demo role)",
            "sql": _GRANT_DEMO_SQL,
            "note": "Applied after DDL so role demo can run Faker seed inserts.",
        }
    )

    blocks: list[str] = []
    for s in sections:
        hdr = f"-- === {s['title']} ==="
        nb = s.get("note")
        if nb:
            hdr += f"\n-- {nb}"
        blocks.append(hdr + "\n" + s["sql"])

    return {
        "ok": True,
        "partition_kind": partition_kind,
        "schedule_cron": schedule_cron,
        "cron_schedule": cron_schedule.strip() or "*/5 * * * *",
        "partman_schema_resolved": partman_schema,
        "sections": sections,
        "full_script": "\n\n".join(blocks),
    }


def partition_demo_sql_preview_for_admin(
    admin_dsn: str | None,
    *,
    partition_kind: PartitionKind,
    cron_schedule: str,
    schedule_cron: bool,
) -> dict[str, Any]:
    """Resolve partman schema when ``admin_dsn`` works; otherwise blueprint with placeholder."""
    resolved: str | None = None
    if admin_dsn and (partition_kind == "range" or schedule_cron):
        try:
            demo_admin = _admin_dsn_for_db(admin_dsn, "demo")
            with psycopg.connect(demo_admin) as conn:
                resolved = _partman_extension_schema(conn)
        except (ValueError, psycopg.Error):
            resolved = None
    out = partition_demo_sql_blueprint(
        partition_kind=partition_kind,
        cron_schedule=cron_schedule,
        schedule_cron=schedule_cron,
        partman_schema=resolved,
    )
    if resolved is None and (partition_kind == "range" or schedule_cron):
        out["partman_schema_hint"] = (
            "Could not read pg_partman schema from demo (extension missing or DSN unreachable). "
            "Preview uses <partman_schema>; after CREATE EXTENSION, use GET partition-demo/status "
            "→ partman.extension_schema (often public or partman)."
        )
    return out


fake = Faker()
Faker.seed(43)


def _admin_dsn_for_db(admin_dsn: str, database: str) -> str:
    admin_dsn = (admin_dsn or "").strip()
    database = database.strip()
    if not admin_dsn or not database:
        raise ValueError("admin DSN and database are required")
    m = re.match(
        r"^(postgresql://[^/]+/)([^/?]+)(.*)$",
        admin_dsn,
        re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)}{database}{m.group(3)}"
    if admin_dsn.endswith("/"):
        return f"{admin_dsn.rstrip('/')}/{database}"
    raise ValueError("POSTGRES_ADMIN_DSN must be a postgresql:// URI with a database segment")


_PARTMAN_UNAVAILABLE_MSG = (
    'Database "demo" does not have the pg_partman extension. RANGE partitioning and pg_cron '
    "maintenance require it. Fix: install with a superuser "
    '`psql -d demo -c "CREATE EXTENSION pg_partman;"` '
    "(optional schema: `CREATE EXTENSION pg_partman SCHEMA partman;`). "
    "Kubernetes: use image mcac-demo/postgresql-repmgr:16.6.0 and complete postgres-demo-bootstrap. "
    "Until then use LIST or HASH without scheduling pg_cron."
)


def _partman_extension_schema(conn: psycopg.Connection) -> str | None:
    """Schema holding pg_partman objects (often ``partman`` or ``public`` depending on install)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname::text
            FROM pg_extension e
            JOIN pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname = 'pg_partman'
            """
        )
        row = cur.fetchone()
        return row[0] if row else None


def _require_partman_schema(conn: psycopg.Connection) -> str:
    s = _partman_extension_schema(conn)
    if not s:
        raise ValueError(_PARTMAN_UNAVAILABLE_MSG)
    return s


def _grant_demo_public_tables(demo_admin: str) -> None:
    """Apply after DDL/maintenance so role ``demo`` can INSERT into hub_part_* before Faker seed."""
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        conn.execute(
            "GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO demo"
        )


def _sql_call_run_maintenance(psch: str) -> sql.Composed:
    """pg_partman 5 registers ``run_maintenance_proc`` as a PROCEDURE — PostgreSQL requires CALL."""
    return sql.SQL("CALL {}.run_maintenance_proc()").format(sql.Identifier(psch))


def _json_safe_scalar(v: Any) -> Any:
    """Values suitable for FastAPI/Starlette JSONResponse (no timedelta/Decimal surprises)."""
    if v is None:
        return None
    if isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, memoryview):
        return bytes(v).decode("utf-8", errors="replace")
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, Decimal):
        return float(v)
    return str(v)


def _ensure_pg_partman(demo_admin: str) -> None:
    """Install pg_partman on database demo if missing; raise if the server has no extension."""
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_partman")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman')"
            )
            present = cur.fetchone()[0]
    if not present:
        raise ValueError(_PARTMAN_UNAVAILABLE_MSG)


def _drop_hub_partitions(admin_dsn: str, qualified_parent: str) -> None:
    demo_admin = _admin_dsn_for_db(admin_dsn, "demo")
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        pschema = _partman_extension_schema(conn)
        if pschema:
            try:
                del_sql = sql.SQL("DELETE FROM {}.part_config WHERE parent_table = %s").format(
                    sql.Identifier(pschema)
                )
                conn.execute(del_sql, (qualified_parent,))
            except psycopg.Error:
                pass
        conn.execute(f"DROP TABLE IF EXISTS {qualified_parent} CASCADE")


def partition_demo_status(admin_dsn: str) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True, "demo": {}, "cron": {}, "partman": {}}
    demo_admin = _admin_dsn_for_db(admin_dsn, "demo")
    with psycopg.connect(demo_admin) as conn:
        with conn.cursor() as cur:
            for label, rel in [
                ("range_parent", _RANGE_PARENT),
                ("list_parent", _LIST_PARENT),
                ("hash_parent", _HASH_PARENT),
            ]:
                cur.execute(
                    """
                    SELECT c.relname, pg_get_expr(c.relpartbound, c.oid, true)
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relispartition
                      AND EXISTS (
                        SELECT 1 FROM pg_inherits i
                        JOIN pg_class p ON p.oid = i.inhparent
                        JOIN pg_namespace pn ON pn.oid = p.relnamespace
                        WHERE i.inhrelid = c.oid
                          AND pn.nspname || '.' || p.relname = %s
                      )
                    ORDER BY 1
                    """,
                    (rel,),
                )
                parts = [{"child": r[0], "bound": r[1]} for r in cur.fetchall()]
                cur.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname || '.' || c.relname = %s AND c.relkind = 'p'
                    )
                    """,
                    (rel,),
                )
                exists = cur.fetchone()[0]
                out["demo"][label] = {"exists": bool(exists), "partitions": parts}

            part_schema = _partman_extension_schema(conn)
            out["partman"]["extension_schema"] = part_schema
            out["partman"]["schema_present"] = bool(part_schema)
            if part_schema:
                cfg_sql = sql.SQL(
                    """
                    SELECT parent_table, partition_type, partition_interval,
                           premake, datetime_string, automatic_maintenance
                    FROM {}.part_config
                    WHERE parent_table = ANY(%s)
                    ORDER BY 1
                    """
                ).format(sql.Identifier(part_schema))
                cur.execute(cfg_sql, ([_RANGE_PARENT, _LIST_PARENT, _HASH_PARENT],))
                cols = [d[0] for d in cur.description]
                out["partman"]["config_rows"] = [
                    {
                        k: _json_safe_scalar(val)
                        for k, val in zip(cols, row, strict=True)
                    }
                    for row in cur.fetchall()
                ]

    pg_meta = _admin_dsn_for_db(admin_dsn, "postgres")
    out["cron"]["jobs"] = []
    try:
        with psycopg.connect(pg_meta) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT jobid, jobname, schedule, database, command, active, nodename
                    FROM cron.job
                    WHERE ({_cron_jobs_where_clause().strip()})
                    ORDER BY jobid
                    """,
                    (_CRON_JOB, _PARTMAN_CRON_CMD_MARKER),
                )
                cols = [d[0] for d in cur.description]
                out["cron"]["jobs"] = [
                    {
                        k: _json_safe_scalar(val)
                        for k, val in zip(cols, row, strict=True)
                    }
                    for row in cur.fetchall()
                ]
    except psycopg.Error as e:
        out["cron"]["error"] = str(e)
    out["hint"] = (
        "pg_cron metadata DB is postgres. Scheduling tries cron.schedule(4-arg), then 3-arg, "
        "then INSERT INTO cron.job(database=demo). Maintenance uses CALL run_maintenance_proc() "
        "(pg_partman 5+). If cron.job_run_details shows connection failed for database=demo, "
        "ensure cron.job.nodename is '' (Unix socket), not localhost — see hub scheduling fix."
    )
    return out


def _unschedule_cron(admin_dsn: str) -> None:
    pg_meta = _admin_dsn_for_db(admin_dsn, "postgres")
    with psycopg.connect(pg_meta, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT jobid FROM cron.job
                WHERE ({_cron_jobs_where_clause().strip()})
                """,
                (_CRON_JOB, _PARTMAN_CRON_CMD_MARKER),
            )
            for (jid,) in cur.fetchall():
                conn.execute("SELECT cron.unschedule(%s)", (jid,))


def _cron_job_use_unix_socket(conn: psycopg.Connection, jobid: int) -> None:
    """pg_cron defaults nodename to localhost; TCP to ::1 often fails — Unix socket is reliable locally."""
    try:
        conn.execute("UPDATE cron.job SET nodename = '' WHERE jobid = %s", (jobid,))
    except psycopg.Error:
        pass


def _schedule_partman_cron(admin_dsn: str, schedule: str) -> dict[str, Any]:
    pg_meta = _admin_dsn_for_db(admin_dsn, "postgres")
    demo_admin = _admin_dsn_for_db(admin_dsn, "demo")
    with psycopg.connect(demo_admin) as dconn:
        psch = _require_partman_schema(dconn)
    maint_sql = sql.SQL("CALL {}.run_maintenance_proc();").format(sql.Identifier(psch))
    _unschedule_cron(admin_dsn)

    tries_log: list[str] = []
    with psycopg.connect(pg_meta, autocommit=True) as conn:
        cmd_text = maint_sql.as_string(conn)
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT cron.schedule(%s::text, %s::text, %s::text, %s::text)
                    """,
                    (_CRON_JOB, schedule, cmd_text, "demo"),
                )
                jid = cur.fetchone()[0]
                _cron_job_use_unix_socket(conn, jid)
                return {
                    "cron_job_id": jid,
                    "jobname": _CRON_JOB,
                    "schedule": schedule,
                    "database": "demo",
                    "schedule_via": "cron.schedule(job, schedule, command, database)",
                }
            except psycopg.Error as e:
                tries_log.append(f"schedule(4-arg): {e}")

            try:
                cur.execute(
                    """
                    INSERT INTO cron.job (jobname, schedule, command, database, username, nodename)
                    VALUES (%s, %s, %s, %s, current_user, '')
                    RETURNING jobid
                    """,
                    (_CRON_JOB, schedule, cmd_text, "demo"),
                )
                jid = cur.fetchone()[0]
                _cron_job_use_unix_socket(conn, jid)
                return {
                    "cron_job_id": jid,
                    "jobname": _CRON_JOB,
                    "schedule": schedule,
                    "database": "demo",
                    "schedule_via": "INSERT INTO cron.job",
                    "hint": (
                        "Older pg_cron without schedule(..., database): worker runs command "
                        "against cron.job.database = demo."
                    ),
                }
            except psycopg.Error as e1:
                tries_log.append(f"INSERT(with nodename): {e1}")
                try:
                    cur.execute(
                        """
                        INSERT INTO cron.job (jobname, schedule, command, database, username)
                        VALUES (%s, %s, %s, %s, current_user)
                        RETURNING jobid
                        """,
                        (_CRON_JOB, schedule, cmd_text, "demo"),
                    )
                    jid = cur.fetchone()[0]
                    _cron_job_use_unix_socket(conn, jid)
                    return {
                        "cron_job_id": jid,
                        "jobname": _CRON_JOB,
                        "schedule": schedule,
                        "database": "demo",
                        "schedule_via": "INSERT INTO cron.job",
                        "hint": (
                            "Older pg_cron without schedule(..., database): worker runs command "
                            "against cron.job.database = demo."
                        ),
                    }
                except psycopg.Error as e2:
                    tries_log.append(f"INSERT(with jobname): {e2}")
                    if isinstance(e2, pg_errors.UndefinedColumn):
                        try:
                            cur.execute(
                                """
                                INSERT INTO cron.job (schedule, command, database, username, nodename)
                                VALUES (%s, %s, %s, current_user, '')
                                RETURNING jobid
                                """,
                                (schedule, cmd_text, "demo"),
                            )
                            jid = cur.fetchone()[0]
                            _cron_job_use_unix_socket(conn, jid)
                            return {
                                "cron_job_id": jid,
                                "jobname": None,
                                "schedule": schedule,
                                "database": "demo",
                                "schedule_via": "INSERT INTO cron.job (no jobname)",
                                "hint": (
                                    "Legacy cron.job row; matched by database=demo and "
                                    "run_maintenance_proc in command."
                                ),
                            }
                        except psycopg.Error:
                            try:
                                cur.execute(
                                    """
                                    INSERT INTO cron.job (schedule, command, database, username)
                                    VALUES (%s, %s, %s, current_user)
                                    RETURNING jobid
                                    """,
                                    (schedule, cmd_text, "demo"),
                                )
                                jid = cur.fetchone()[0]
                                _cron_job_use_unix_socket(conn, jid)
                                return {
                                    "cron_job_id": jid,
                                    "jobname": None,
                                    "schedule": schedule,
                                    "database": "demo",
                                    "schedule_via": "INSERT INTO cron.job (no jobname)",
                                    "hint": (
                                        "Legacy cron.job row; matched by database=demo and "
                                        "run_maintenance_proc in command."
                                    ),
                                }
                            except psycopg.Error as e3:
                                tries_log.append(f"INSERT(no jobname): {e3}")

            try:
                cur.execute(
                    """
                    SELECT cron.schedule(%s::text, %s::text, %s::text)
                    """,
                    (_CRON_JOB, schedule, cmd_text),
                )
                jid = cur.fetchone()[0]
                _cron_job_use_unix_socket(conn, jid)
                return {
                    "cron_job_id": jid,
                    "jobname": _CRON_JOB,
                    "schedule": schedule,
                    "database": "cron.database_name",
                    "schedule_via": "cron.schedule(job, schedule, command)",
                    "warning": (
                        "Runs only in cron.database_name from postgresql.conf; set to \"demo\" "
                        "or use a pg_cron build with schedule(..., database)."
                    ),
                }
            except psycopg.Error as e:
                tries_log.append(f"schedule(3-arg): {e}")

    return {
        "cron_job_id": None,
        "jobname": _CRON_JOB,
        "schedule": schedule,
        "error": " | ".join(tries_log),
        "hint": (
            "Could not schedule via cron.schedule or INSERT cron.job. "
            "Use Run maintenance manually, or rebuild Postgres image with newer pg_cron "
            "(demo Dockerfile pins PG_CRON_VERSION)."
        ),
    }


def partition_demo_run_maintenance(admin_dsn: str) -> dict[str, Any]:
    demo_admin = _admin_dsn_for_db(admin_dsn, "demo")
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        psch = _require_partman_schema(conn)
        conn.execute(_sql_call_run_maintenance(psch))
    return {"ok": True, "ran": "CALL run_maintenance_proc() on database demo"}


def _setup_range(demo_admin: str) -> None:
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        conn.execute(_RANGE_PARENT_DDL)
        psch = _require_partman_schema(conn)
        parent_sql = sql.SQL(
            """
            SELECT {}.create_parent(
                p_parent_table := 'public.hub_part_range_events',
                p_control := 'event_day',
                p_interval := '1 month',
                p_type := 'range',
                p_premake := 4
            )
            """
        ).format(sql.Identifier(psch))
        conn.execute(parent_sql)


def _setup_list(demo_admin: str) -> None:
    stmts = [_LIST_PARENT_DDL, *_LIST_CHILD_STMTS]
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        for s in stmts:
            conn.execute(s)


def _setup_hash(demo_admin: str) -> None:
    stmts = [_HASH_PARENT_DDL, *_HASH_CHILD_STMTS]
    with psycopg.connect(demo_admin, autocommit=True) as conn:
        for s in stmts:
            conn.execute(s)


def _faker_seed(app_dsn: str, kind: PartitionKind, n: int) -> int:
    inserted = 0
    channels = ["online", "retail", "partner", "wholesale"]
    with psycopg.connect(app_dsn, autocommit=True) as conn:
        if kind == "range":
            today = date.today()
            for _ in range(n):
                d = today - timedelta(days=fake.random_int(0, 120))
                conn.execute(
                    """
                    INSERT INTO public.hub_part_range_events
                      (event_day, customer_email, region, amount_cents, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        d,
                        fake.email(),
                        fake.random_element(["NA", "EU", "APAC"]),
                        fake.random_int(100, 99999),
                        fake.sentence(nb_words=6)[:240],
                    ),
                )
                inserted += 1
        elif kind == "list":
            for _ in range(n):
                conn.execute(
                    """
                    INSERT INTO public.hub_part_list_orders
                      (channel, order_ref, amount_cents, sku)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        fake.random_element(channels),
                        f"ORD-{uuid.uuid4().hex[:10]}",
                        fake.random_int(500, 50000),
                        f"SKU-{uuid.uuid4().hex[:8].upper()}",
                    ),
                )
                inserted += 1
        else:
            for _ in range(n):
                conn.execute(
                    """
                    INSERT INTO public.hub_part_hash_metrics
                      (shard_key, metric_name, metric_value)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        fake.random_int(1, 10_000_000),
                        fake.random_element(["latency_ms", "cpu_pct", "errors"]),
                        round(fake.random.uniform(0.1, 999.9), 4),
                    ),
                )
                inserted += 1
    return inserted


def partition_demo_setup(
    admin_dsn: str,
    app_dsn: str,
    *,
    partition_kind: PartitionKind,
    replace_existing: bool,
    seed_rows: int,
    cron_schedule: str,
    schedule_cron: bool,
) -> dict[str, Any]:
    seed_rows = max(0, min(seed_rows, 200))
    demo_admin = _admin_dsn_for_db(admin_dsn, "demo")

    if partition_kind == "range" or schedule_cron:
        _ensure_pg_partman(demo_admin)

    steps: list[str] = []
    cron_info: dict[str, Any] | None = None

    if replace_existing:
        for q in (_RANGE_PARENT, _LIST_PARENT, _HASH_PARENT):
            _drop_hub_partitions(admin_dsn, q)
        steps.append("Dropped prior hub_part_* demo tables (if any).")

    if partition_kind == "range":
        _setup_range(demo_admin)
        steps.append(
            "Created RANGE parent public.hub_part_range_events + partman.create_parent (type range, interval 1 month, premake 4)."
        )
        with psycopg.connect(demo_admin, autocommit=True) as conn:
            psch = _require_partman_schema(conn)
            conn.execute(_sql_call_run_maintenance(psch))
        steps.append("Called run_maintenance_proc() once to materialize child partitions.")
        if schedule_cron:
            cron_info = _schedule_partman_cron(admin_dsn, cron_schedule)
            steps.append(
                f"Scheduled pg_cron job {_CRON_JOB!r} on schedule {cron_schedule!r} (database demo)."
            )
    elif partition_kind == "list":
        _setup_list(demo_admin)
        steps.append(
            "Created LIST parent public.hub_part_list_orders with fixed channel partitions + DEFAULT."
        )
        if schedule_cron:
            cron_info = _schedule_partman_cron(admin_dsn, cron_schedule)
            steps.append(
                "Scheduled pg_cron maintenance on demo (partman no-op for this LIST table)."
            )
    else:
        _setup_hash(demo_admin)
        steps.append(
            "Created HASH parent public.hub_part_hash_metrics with four modulus partitions."
        )
        if schedule_cron:
            cron_info = _schedule_partman_cron(admin_dsn, cron_schedule)
            steps.append(
                "Scheduled pg_cron maintenance on demo (partman no-op for this HASH table)."
            )

    _grant_demo_public_tables(demo_admin)

    inserted = 0
    if seed_rows > 0:
        inserted = _faker_seed(app_dsn, partition_kind, seed_rows)
        steps.append(f"Inserted {inserted} Faker row(s) as role demo.")

    out: dict[str, Any] = {
        "ok": True,
        "partition_kind": partition_kind,
        "steps": steps,
        "rows_inserted": inserted,
        "cron": cron_info,
        "tables": {
            "range": _RANGE_PARENT,
            "list": _LIST_PARENT,
            "hash": _HASH_PARENT,
        },
    }
    out["status"] = partition_demo_status(admin_dsn)
    return out


def partition_demo_extra_seed(app_dsn: str, kind: PartitionKind, n: int) -> dict[str, Any]:
    n = max(1, min(n, 200))
    ins = _faker_seed(app_dsn, kind, n)
    return {"ok": True, "partition_kind": kind, "rows_inserted": ins}


def partition_demo_remove_cron(admin_dsn: str) -> dict[str, Any]:
    _unschedule_cron(admin_dsn)
    return {"ok": True, "unscheduled": _CRON_JOB}


def partition_demo_repair_pg_cron_nodename(admin_dsn: str) -> dict[str, Any]:
    """
    Set ``cron.job.nodename`` to empty string for hub partman maintenance rows.

    pg_cron opens a new libpq connection per run; default ``nodename`` is ``localhost``,
    which often resolves to IPv6 while PostgreSQL listens on IPv4 only → ``connection failed``.
    An empty ``nodename`` uses a Unix-domain socket to the same postmaster.
    """
    pg_meta = _admin_dsn_for_db(admin_dsn, "postgres")
    where_inner = _cron_jobs_where_clause().strip()
    try:
        with psycopg.connect(pg_meta, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE cron.job
                    SET nodename = ''
                    WHERE ({where_inner})
                      AND (nodename IS DISTINCT FROM '')
                    RETURNING jobid
                    """,
                    (_CRON_JOB, _PARTMAN_CRON_CMD_MARKER),
                )
                jobids = [row[0] for row in cur.fetchall()]
    except psycopg.Error as e:
        if isinstance(e, pg_errors.UndefinedColumn) and "nodename" in str(e).lower():
            return {
                "ok": False,
                "error": str(e),
                "hint": "This pg_cron build has no nodename column; upgrade pg_cron or set host in postgresql.conf.",
            }
        raise
    return {
        "ok": True,
        "updated": len(jobids),
        "jobids": jobids,
        "note": (
            "Past rows in cron.job_run_details stay as failed until new runs succeed. "
            "Optional: TRUNCATE cron.job_run_details; (superuser) to clear history."
        ),
    }
