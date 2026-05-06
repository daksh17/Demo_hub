"""
Browser UI + API: single-order ingest and configurable multi-DB workload generator.
"""
import asyncio
import decimal
import json
import os
import re
import secrets
from urllib.parse import unquote, urlparse
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal, Self

import httpx
import psycopg
import redis
from cassandra.cluster import Cluster, UnresolvableContactPoints
from cassandra.query import BatchStatement, ConsistencyLevel
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from trino.dbapi import connect as trino_connect
from trino.exceptions import TrinoConnectionError, TrinoUserError

import postgres_logical_demo as pg_logical
import postgres_faker_schema as pg_faker_schema
import postgres_schema_clone as pg_schema_clone
import kafka_lab
import scenario
from hub_config import (
    SK_CASSANDRA_HOSTS,
    SK_CASSANDRA_KEYSPACE,
    SK_MONGO_URI,
    SK_OS_INDEX,
    SK_OS_URL,
    SK_OS_WORKLOAD_INDEX,
    SK_POSTGRES_DSN,
    SK_REDIS_URL,
    SK_SCENARIO_OS_INDEX,
    env_base_config,
    get_runtime_config,
    keyspace_valid,
    mask_connection_hint,
    reset_runtime_config_token,
    runtime_config_from_request_session,
    set_runtime_config_token,
)

_CONNECTION_SESSION_KEYS: tuple[tuple[str, str], ...] = (
    ("postgres_dsn", SK_POSTGRES_DSN),
    ("mongo_uri", SK_MONGO_URI),
    ("redis_url", SK_REDIS_URL),
    ("opensearch_url", SK_OS_URL),
    ("cassandra_hosts", SK_CASSANDRA_HOSTS),
    ("cassandra_keyspace", SK_CASSANDRA_KEYSPACE),
    ("opensearch_index", SK_OS_INDEX),
    ("opensearch_workload_index", SK_OS_WORKLOAD_INDEX),
    ("scenario_opensearch_index", SK_SCENARIO_OS_INDEX),
)

WORKLOAD_SUSTAIN_MAX_SECONDS = int(
    os.environ.get("WORKLOAD_SUSTAIN_MAX_SECONDS", str(9 * 3600))
)
# Max payload pad per record (KB in API). 16 MiB = 16384 KiB.
PAYLOAD_KB_MAX = 16 * 1024
# Nominal MiB for one workload wave (total_records × payload); guards huge single requests.
WORKLOAD_MAX_WAVE_NOMINAL_MB = int(
    os.environ.get("WORKLOAD_MAX_WAVE_NOMINAL_MB", str(2_000_000))
)
# Upper bound on est_mb × estimated_wave_count for sustain (total nominal MiB across waves).
WORKLOAD_SUSTAIN_NOMINAL_CAP_MB = int(
    os.environ.get("WORKLOAD_SUSTAIN_NOMINAL_CAP_MB", str(50_000_000))
)

def _connect_cassandra_cluster(hosts: list[str] | None = None):
    """Create Cluster + Session; retry while the ring is still opening CQL (common on K8s)."""
    if hosts is None:
        hosts = list(env_base_config().cassandra_hosts)
    if not hosts:
        raise ValueError(
            "CASSANDRA_HOSTS has no hostnames after parsing. "
            "Example local: export CASSANDRA_HOSTS=127.0.0.1 "
            "(with kubectl port-forward to Cassandra :9042)."
        )
    max_attempts = int(os.environ.get("CASSANDRA_CONNECT_MAX_ATTEMPTS", "45"))
    delay_sec = float(os.environ.get("CASSANDRA_CONNECT_RETRY_DELAY_SEC", "2"))
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        cluster = Cluster(hosts, connect_timeout=15)
        try:
            return cluster, cluster.connect()
        except UnresolvableContactPoints as e:
            last_err = e
            try:
                cluster.shutdown()
            except Exception:
                pass
            raise RuntimeError(
                "Cassandra contact points could not be resolved (DNS). "
                f"hosts={hosts!r}. On the host (uvicorn), use a resolvable address, e.g. "
                "CASSANDRA_HOSTS=127.0.0.1 after port-forwarding CQL 9042; "
                "inside Compose/K8s, use service DNS like cassandra."
            ) from e
        except Exception as e:
            last_err = e
            try:
                cluster.shutdown()
            except Exception:
                pass
            if attempt < max_attempts:
                time.sleep(delay_sec)
    assert last_err is not None
    raise last_err
# Workload sustain + many batches can exceed the driver default (~10s) when the node is busy.
CASSANDRA_WORKLOAD_REQUEST_TIMEOUT = float(
    os.environ.get("CASSANDRA_WORKLOAD_REQUEST_TIMEOUT_SECONDS", "120")
)
# Pause between Cassandra batch executes (sustain + large payloads can overload a 500M demo node).
CASSANDRA_WORKLOAD_INTER_BATCH_SLEEP_MS = float(
    os.environ.get("CASSANDRA_WORKLOAD_INTER_BATCH_SLEEP_MS", "0")
)
CASSANDRA_WORKLOAD_WRITE_RETRIES = int(
    os.environ.get("CASSANDRA_WORKLOAD_WRITE_RETRIES", "2")
)
WORKLOAD_REDIS_PREFIX = os.environ.get("WORKLOAD_REDIS_PREFIX", "hub:wl:")
# OpenSearch default http.max_content_length is often 100 MiB; stay under to avoid HTTP 413 on /_bulk.
OPENSEARCH_BULK_MAX_BYTES = int(
    os.environ.get("OPENSEARCH_BULK_MAX_BYTES", str(48 * 1024 * 1024))
)
# Debezium Postgres snapshot loads full TEXT per row into heap; multi‑MiB names OOM Connect (default ~2–4G).
POSTGRES_WORKLOAD_NAME_MAX_CHARS = int(
    os.environ.get("POSTGRES_WORKLOAD_NAME_MAX_CHARS", str(16 * 1024))
)
# Read-back page: max rows per store per /api/workload/read request (Cassandra IN, Redis MGET, OS size, etc.).
WORKLOAD_READ_SAMPLE_LIMIT_MAX = max(
    1, int(os.environ.get("WORKLOAD_READ_SAMPLE_LIMIT_MAX", "500"))
)
# Read-back UI: max parallel browser→API requests (each request hits all selected targets).
WORKLOAD_READ_PARALLEL_MAX = max(
    1, min(64, int(os.environ.get("WORKLOAD_READ_PARALLEL_MAX", "32")))
)

_cassandra_session = None
_cassandra_insert_prep = None

ALLOWED_TARGETS = frozenset(
    {"postgres", "mongo", "redis", "cassandra", "opensearch", "mssql"}
)

TRINO_ALLOWED_CATALOGS = frozenset(
    {"demo_pg", "demo_mongo", "demo_es", "demo_sqlserver", "memory"}
)
TRINO_ROW_CAP = max(1, min(2000, int(os.environ.get("TRINO_QUERY_ROW_CAP", "500"))))
_TRINO_FORBIDDEN_KW = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\b",
    re.I,
)
_TRINO_ALLOWED_START = re.compile(
    r"^\s*(SELECT\b|WITH\b|SHOW\b|DESCRIBE\b|DESC\b|TABLE\b|EXPLAIN\b)",
    re.I | re.DOTALL,
)


def _trino_sql_guard(sql: str) -> None:
    core = sql.strip().rstrip(";").strip()
    if not core:
        raise HTTPException(status_code=400, detail="empty SQL")
    parts = [p.strip() for p in sql.split(";") if p.strip()]
    if len(parts) != 1:
        raise HTTPException(
            status_code=400, detail="exactly one SQL statement (no multiple ; chunks)",
        )
    if _TRINO_FORBIDDEN_KW.search(core):
        raise HTTPException(
            status_code=400,
            detail="mutating SQL keywords are not allowed from this hub",
        )
    if not _TRINO_ALLOWED_START.match(core):
        raise HTTPException(
            status_code=400,
            detail="only SELECT, WITH, SHOW, DESCRIBE, TABLE, or EXPLAIN …",
        )


def _trino_cell_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", errors="replace")
    if isinstance(v, (list, dict)):
        return json.loads(json.dumps(v, default=str))
    return v


def _parse_trino_http(base: str) -> tuple[str, int, str]:
    u = urlparse(base.strip())
    if u.scheme not in ("http", "https") or not u.hostname:
        raise HTTPException(
            status_code=500,
            detail="TRINO_HTTP must be like http://trino:8080",
        )
    host = u.hostname
    assert host is not None
    if u.port is not None:
        port = u.port
    else:
        port = 443 if u.scheme == "https" else 8080
    return host, port, u.scheme


def _execute_trino_query(
    cfg,
    sql: str,
    catalog: str | None,
    schema: str | None,
) -> dict[str, Any]:
    if not (cfg.trino_http_url or "").strip():
        raise HTTPException(
            status_code=503,
            detail="Trino not configured (TRINO_HTTP is empty). Kubernetes demo-hub sets it to http://trino:8080.",
        )
    cat = (catalog or "demo_pg").strip()
    if cat not in TRINO_ALLOWED_CATALOGS:
        raise HTTPException(
            status_code=400,
            detail=f"catalog must be one of {sorted(TRINO_ALLOWED_CATALOGS)}",
        )
    sch_raw = (schema or "").strip()
    sch = sch_raw or None
    if sch is not None and not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", sch):
        raise HTTPException(status_code=400, detail="invalid schema identifier")

    host, port, scheme = _parse_trino_http(cfg.trino_http_url)
    conn = trino_connect(
        host=host,
        port=port,
        user="hub-demo-ui",
        catalog=cat,
        schema=sch,
        http_scheme=scheme,
        request_timeout=120.0,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        truncated = False
        rows: list[list[Any]] = []
        while len(rows) < TRINO_ROW_CAP:
            batch = cur.fetchmany(min(256, TRINO_ROW_CAP - len(rows)))
            if not batch:
                break
            for r in batch:
                rows.append([_trino_cell_json(c) for c in r])
                if len(rows) >= TRINO_ROW_CAP:
                    truncated = True
                    break
            if truncated:
                break
        if truncated:
            while cur.fetchmany(512):
                pass
        return {
            "ok": True,
            "catalog": cat,
            "schema": sch,
            "columns": cols,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }
    except TrinoUserError as e:
        raise HTTPException(status_code=400, detail=e.message) from e
    except TrinoConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _make_pad(payload_kb: int) -> str:
    if payload_kb <= 0:
        return ""
    cap = PAYLOAD_KB_MAX * 1024
    n = min(payload_kb * 1024, cap)
    return "x" * n


def _duration_seconds(value: int, unit: Literal["seconds", "minutes", "hours"]) -> float:
    if unit == "seconds":
        return float(value)
    if unit == "minutes":
        return float(value * 60)
    return float(value * 3600)


def _cassandra_rows_per_batch(pad: str, requested_batch_cap: int) -> int:
    """Cap rows per BatchStatement so total size stays under Cassandra's batch limit.

    Workload rows put ``pad`` in ``label``; multi-partition unlogged batches fail above
    ~50 KiB by default (``Batch too large``). This scales row count down when ``payload_kb``
    is large even if the UI ``batch_size`` is small.
    """
    cass_label_max = 60_000
    label_len = min(len(pad) + 48, cass_label_max)
    est_row_bytes = max(256, int(220 + label_len * 1.08))
    budget = 35_000
    max_by_server = max(1, budget // est_row_bytes)
    return max(1, min(requested_batch_cap, 50, max_by_server))


def _opensearch_bulk_chunk_size(pad: str, requested_bs: int) -> int:
    """How many workload docs per ``/_bulk`` request.

    Large ``pad`` (payload_kb) makes each NDJSON line huge; sending ``batch_size`` docs at
    once can exceed ``http.max_content_length`` and yield **413 Request Entity Too Large**.
    """
    if requested_bs <= 1:
        return 1
    # Per doc: index directive line + JSON body (pad dominates; json.dumps uses UTF-8).
    index_line_est = 96
    body_overhead = 160  # run_id, seq, created_at, JSON structure
    est_doc_bytes = index_line_est + body_overhead + max(8, len(pad.encode("utf-8")) * 2)
    cap = max(1, OPENSEARCH_BULK_MAX_BYTES // est_doc_bytes)
    return max(1, min(requested_bs, cap))


def _ensure_cassandra_schema(session, keyspace: str) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(keyspace)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
          order_id text PRIMARY KEY,
          label text,
          created_at timestamp
        )
        """
    )


def _ensure_opensearch_index(client: httpx.Client, cfg) -> None:
    specs = [
        (
            cfg.opensearch_index,
            {
                "mappings": {
                    "properties": {
                        "order_id": {"type": "keyword"},
                        "label": {"type": "text"},
                        "source": {"type": "keyword"},
                        "created_at": {"type": "date"},
                    }
                }
            },
        ),
        (
            cfg.opensearch_workload_index,
            {
                "mappings": {
                    "properties": {
                        "run_id": {"type": "keyword"},
                        "seq": {"type": "long"},
                        "pad": {"type": "text"},
                        "created_at": {"type": "date"},
                    }
                }
            },
        ),
    ]
    for idx, body in specs:
        r = client.head(f"{cfg.opensearch_url}/{idx}")
        if r.status_code == 200:
            continue
        r = client.put(f"{cfg.opensearch_url}/{idx}", json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"OpenSearch create index {idx}: {r.status_code} {r.text}")


_os_index_bootstrapped: set[str] = set()


def _ensure_hub_opensearch_for_cfg(client: httpx.Client, cfg) -> None:
    k = f"{cfg.opensearch_url}\0{cfg.opensearch_index}\0{cfg.opensearch_workload_index}"
    if k in _os_index_bootstrapped:
        return
    _ensure_opensearch_index(client, cfg)
    _os_index_bootstrapped.add(k)


_cass_override_lock = threading.Lock()
_cass_override: dict[str, tuple[Cluster, object, object]] = {}


def _cass_override_key(cfg) -> str:
    return ",".join(cfg.cassandra_hosts) + "#" + cfg.cassandra_keyspace


def get_hub_cassandra_handles(cfg):
    """Return (session, prepared_insert) for hub orders table; uses env cluster when unmodified."""
    boot = env_base_config()
    if cfg.is_default_cassandra(boot):
        return _cassandra_session, _cassandra_insert_prep
    key = _cass_override_key(cfg)
    with _cass_override_lock:
        if key not in _cass_override:
            cluster, sess = _connect_cassandra_cluster(list(cfg.cassandra_hosts))
            _ensure_cassandra_schema(sess, cfg.cassandra_keyspace)
            scenario.ensure_cassandra_scenario_schema(
                sess, cfg.cassandra_keyspace
            )
            prep = sess.prepare(
                f"INSERT INTO {cfg.cassandra_keyspace}.orders "
                "(order_id, label, created_at) VALUES (?, ?, ?)"
            )
            _cass_override[key] = (cluster, sess, prep)
        _cluster, sess, prep = _cass_override[key]
    return sess, prep


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cassandra_session, _cassandra_insert_prep
    boot = env_base_config()
    cluster, _cassandra_session = _connect_cassandra_cluster(
        list(boot.cassandra_hosts)
    )
    try:
        _ensure_cassandra_schema(_cassandra_session, boot.cassandra_keyspace)
        _cassandra_insert_prep = _cassandra_session.prepare(
            f"INSERT INTO {boot.cassandra_keyspace}.orders "
            "(order_id, label, created_at) VALUES (?, ?, ?)"
        )
        scenario.ensure_cassandra_scenario_schema(
            _cassandra_session, boot.cassandra_keyspace
        )
        with psycopg.connect(boot.postgres_dsn) as conn:
            scenario.ensure_postgres_scenario_schema(conn)
            conn.commit()
        with httpx.Client(timeout=120.0) as hc:
            _ensure_opensearch_index(hc, boot)
            scenario.ensure_scenario_os_index(hc)
        yield
    finally:
        cluster.shutdown()
        for cl, _s, _p in _cass_override.values():
            try:
                cl.shutdown()
            except Exception:
                pass
        _cass_override.clear()


app = FastAPI(title="Realtime hub demo UI", lifespan=lifespan)


class HubRuntimeConfigMiddleware(BaseHTTPMiddleware):
    """Runs *inside* SessionMiddleware so ``request.session`` is available."""

    async def dispatch(self, request: Request, call_next):
        rt = runtime_config_from_request_session(dict(request.session))
        tok = set_runtime_config_token(rt)
        try:
            return await call_next(request)
        finally:
            reset_runtime_config_token(tok)


_SESSION_SECRET = os.environ.get("HUB_SESSION_SECRET", "").strip() or secrets.token_hex(32)
# Starlette inserts each add_middleware at index 0: register Hub first, then Session so
# the stack is Session(Hub(app)) — session cookie is parsed before Hub runs.
app.add_middleware(HubRuntimeConfigMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=_SESSION_SECRET,
    max_age=14 * 24 * 3600,
    same_site="lax",
)

NAV = """
  <nav style="margin-bottom:1rem;font-size:0.95rem;">
    <a href="/">Single order</a> · <a href="/workload">Workload</a> ·
    <a href="/reads">Read-back</a> · <a href="/scenario">Scenario</a> ·
    <a href="/kafka">Kafka lab</a> ·
    <a href="/postgres">Postgres</a> · <a href="/trino">Trino</a> ·
    <a href="/connections">External DBs</a>
  </nav>
"""

# Shown under main NAV on Postgres-specific pages (breadcrumb + sub-links).
POSTGRES_BAR = """
  <div style="margin:0 0 1rem;font-size:0.88rem;color:#8899a6;line-height:1.5;">
    <span style="color:#71767b">Postgres hub</span>
    · <a href="/postgres">Overview</a>
    · <a href="/postgres/logical">Logical replication</a>
    · <a href="/postgres/faker-schema">Faker schema objects</a>
  </div>
"""

PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Realtime orders-search hub — demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 52rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; }}
    a {{ color: #6cb5f4; }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.6rem 1.25rem; font-size: 1rem; font-weight: 600; cursor: pointer;
    }}
    button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    pre {{
      background: #16181c; border: 1px solid #2f3336; border-radius: 8px;
      padding: 1rem; overflow: auto; font-size: 0.8rem;
    }}
    .links {{ margin: 1rem 0; display: flex; flex-wrap: wrap; gap: 0.75rem 1.25rem; }}
    .hint {{ color: #71767b; font-size: 0.9rem; margin-top: 1.5rem; }}
    .ok {{ color: #7af87a; }}
    .err {{ color: #f66; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Hub demo — write one order everywhere</h1>
  <p>Pushes the same event to <strong>Postgres</strong> (<code>demo_items</code>), <strong>Mongo</strong>
    (<code>demo.demo_items</code>), <strong>Redis</strong>, <strong>Cassandra</strong> (<code>demo_hub.orders</code>),
    and <strong>OpenSearch</strong> (<code>hub-orders</code>). With the full stack, Compose runs <strong>kafka-connect-register</strong>
    so <strong>Kafka Connect</strong> loads four connectors: Postgres Debezium + JDBC sink, Mongo Debezium + Mongo sink
    (topics like <code>demopg.public.demo_items</code>, <code>demomongo.demo.demo_items</code>; sinks
    <code>demo_items_from_kafka</code> on Postgres and <code>demo.demo_items_from_kafka</code> in Mongo). Re-register:
    <code>./deploy/docker/kafka-connect-register/register-all.sh</code> from <code>dashboards/demo</code>.</p>
  <button type="button" id="go">Create demo order</button>
  <p id="status"></p>
  <pre id="out">Click the button to see JSON verification.</pre>
  <div class="links">
    <a href="http://localhost:3000" target="_blank" rel="noopener">Grafana</a>
    <a href="http://localhost:3000/dashboards" target="_blank" rel="noopener">Dashboards</a>
    <a href="http://localhost:9090/targets" target="_blank" rel="noopener">Prometheus targets</a>
    <a href="http://localhost:5601" target="_blank" rel="noopener">OpenSearch Dashboards</a>
    <a href="http://localhost:8083/connectors" target="_blank" rel="noopener">Kafka Connect</a>
  </div>
  <p class="hint"><strong>OpenSearch:</strong> in Dashboards → Dev Tools or
    <code>GET /hub-orders/_search?pretty</code>. Index pattern <code>hub-orders*</code> in Discover.</p>
  <p class="hint"><strong>CLI:</strong> <code>cqlsh 127.0.0.1 19442</code> → <code>SELECT * FROM demo_hub.orders LIMIT 10;</code></p>
  <script>
    const go = document.getElementById("go");
    const out = document.getElementById("out");
    const statusEl = document.getElementById("status");
    go.addEventListener("click", async () => {{
      go.disabled = true;
      statusEl.textContent = "Working…";
      statusEl.className = "";
      out.textContent = "";
      try {{
        const r = await fetch("/api/ingest", {{ method: "POST" }});
        const data = await r.json();
        out.textContent = JSON.stringify(data, null, 2);
        if (r.ok && data.ok) {{
          statusEl.textContent = "All backends returned OK.";
          statusEl.className = "ok";
        }} else {{
          statusEl.textContent = "Some steps failed — see JSON.";
          statusEl.className = "err";
        }}
      }} catch (e) {{
        statusEl.textContent = String(e);
        statusEl.className = "err";
      }} finally {{
        go.disabled = false;
      }}
    }});
  </script>
</body>
</html>
"""


WORKLOAD_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Workload generator — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 44rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    a {{ color: #6cb5f4; }}
    label {{ display: block; margin: 0.75rem 0 0.25rem; font-size: 0.9rem; color: #8899a6; }}
    input[type="number"] {{ width: 8rem; padding: 0.4rem; border-radius: 6px; border: 1px solid #38444d; background: #16181c; color: #e7e9ea; }}
    fieldset {{ border: 1px solid #38444d; border-radius: 8px; margin: 1rem 0; padding: 0.75rem 1rem; }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.6rem 1.25rem; font-size: 1rem; font-weight: 600; cursor: pointer; margin-top: 1rem;
    }}
    button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    pre {{ background: #16181c; border: 1px solid #2f3336; border-radius: 8px; padding: 1rem; overflow: auto; font-size: 0.78rem; max-height: 28rem; }}
    .ok {{ color: #7af87a; }} .err {{ color: #f66; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Workload generator</h1>
  <p>Writes synthetic rows in <strong>batches</strong>. <strong>Payload block size (KB)</strong> repeats filler bytes per record (larger = heavier writes).
    <strong>Postgres</strong> stores the pad inside <code>demo_items.name</code> with a <strong>CDC‑safe max length</strong> (default 16&nbsp;KiB; override <code>POSTGRES_WORKLOAD_NAME_MAX_CHARS</code>) so Debezium snapshot does not OOM Kafka Connect on huge pads.     <strong>Cassandra</strong> puts the pad in <code>label</code> (truncated); each batch row count is <strong>capped automatically</strong> so size stays under Cassandra&apos;s limit (prevents <code>Batch too large</code>). Sustained + large payloads can overload demo nodes (coordinator timeout / code 1100)—raise <code>CASSANDRA_WORKLOAD_REQUEST_TIMEOUT_SECONDS</code>, set <code>CASSANDRA_WORKLOAD_INTER_BATCH_SLEEP_MS</code> (e.g. 15–40), or lower payload / sustain duration.
    OpenSearch uses index <code>hub-workload</code>. <strong>SQL Server</strong> (when <code>MSSQL_HOST</code> is set on the hub) inserts into <code>demo.dbo.hub_workload_mssql</code> on the <strong>publisher</strong> — same <code>wl-&lt;run_id&gt;-&lt;seq&gt;</code> naming; not enabled for CDC (keeps Debezium topics focused on the catalog mirror). REST: <code>http://localhost:9200</code>, Dashboards: <code>http://localhost:5601</code>. Grafana: <strong>SQL Server (demo hub)</strong> dashboard (awaragi exporter) plus Mongo, Redis, Kafka, Cassandra, ….</p>
  <form id="f">
    <label>Total records (1–100000)</label>
    <input type="number" name="total_records" value="200" min="1" max="100000"/>
    <label>Batch size (1–2000; internally capped per engine)</label>
    <input type="number" name="batch_size" value="50" min="1" max="2000"/>
    <label>Payload block size (KB per record, 0–{PAYLOAD_KB_MAX} = up to {PAYLOAD_KB_MAX // 1024} MiB)</label>
    <input type="number" name="payload_kb" value="0" min="0" max="{PAYLOAD_KB_MAX}"/>
    <fieldset>
      <legend>Sustain load (repeat until time elapses)</legend>
      <label><input type="checkbox" name="sustain" id="sustain" /> Keep running the workload for…</label>
      <label>Duration (1–9)</label>
      <select name="duration_value" id="dur_val" style="padding:0.4rem;border-radius:6px;background:#16181c;color:#e7e9ea;border:1px solid #38444d">
        <option value="1">1</option><option value="2">2</option><option value="3">3</option>
        <option value="4">4</option><option value="5" selected>5</option><option value="6">6</option>
        <option value="7">7</option><option value="8">8</option><option value="9">9</option>
      </select>
      <label>Unit</label>
      <select name="duration_unit" id="dur_unit" style="padding:0.4rem;border-radius:6px;background:#16181c;color:#e7e9ea;border:1px solid #38444d">
        <option value="seconds">seconds</option>
        <option value="minutes" selected>minutes</option>
        <option value="hours">hours</option>
      </select>
      <p class="hint" style="color:#71767b;font-size:0.82rem;margin:0.5rem 0 0">Each wave writes <strong>total records</strong> per target; waves repeat until the window ends. Long runs can fill disks — lower counts or disable sustain.</p>
    </fieldset>
    <fieldset>
      <legend>Targets</legend>
      <label><input type="checkbox" name="tg" value="postgres" checked /> Postgres <code>demo_items</code></label>
      <label><input type="checkbox" name="tg" value="mongo" checked /> Mongo <code>demo.demo_items</code></label>
      <label><input type="checkbox" name="tg" value="redis" checked /> Redis keys <code>hub:wl:*</code></label>
      <label><input type="checkbox" name="tg" value="cassandra" checked /> Cassandra <code>demo_hub.orders</code></label>
      <label><input type="checkbox" name="tg" value="opensearch" checked /> OpenSearch <code>hub-workload</code></label>
      <label><input type="checkbox" name="tg" value="mssql" /> SQL Server <code>demo.dbo.hub_workload_mssql</code> (publisher)</label>
    </fieldset>
    <button type="submit" id="run">Run workload</button>
  </form>
  <p id="st"></p>
  <pre id="out">Submit the form to see timing and per-target counts.</pre>
  <script>
    document.getElementById("f").addEventListener("submit", async (ev) => {{
      ev.preventDefault();
      const fd = new FormData(ev.target);
      const targets = [...document.querySelectorAll('input[name="tg"]:checked')].map((x) => x.value);
      const sustainEl = document.getElementById("sustain");
      const body = {{
        total_records: Number(fd.get("total_records")),
        batch_size: Number(fd.get("batch_size")),
        payload_kb: Number(fd.get("payload_kb")),
        targets,
        sustain: sustainEl.checked
      }};
      if (sustainEl.checked) {{
        body.duration_value = Number(document.getElementById("dur_val").value);
        body.duration_unit = document.getElementById("dur_unit").value;
      }}
      const btn = document.getElementById("run");
      const st = document.getElementById("st");
      const out = document.getElementById("out");
      btn.disabled = true;
      st.textContent = sustainEl.checked ? "Running sustained workload (request may take a long time)…" : "Running…";
      st.className = "";
      out.textContent = "";
      try {{
        const r = await fetch("/api/workload", {{ method: "POST", headers: {{ "Content-Type": "application/json" }}, body: JSON.stringify(body) }});
        const data = await r.json();
        out.textContent = JSON.stringify(data, null, 2);
        st.textContent = r.ok && data.ok ? "Done." : "Finished with errors — see JSON.";
        st.className = r.ok && data.ok ? "ok" : "err";
        if (r.ok && data.run_id) {{
          try {{ localStorage.setItem("hub_last_run_id", data.run_id); }} catch (e) {{}}
        }}
      }} catch (e) {{
        st.textContent = String(e);
        st.className = "err";
      }} finally {{
        btn.disabled = false;
      }}
    }});
  </script>
</body>
</html>
"""


READS_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Read-back — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 52rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    a {{ color: #6cb5f4; }}
    label {{ display: block; margin: 0.6rem 0 0.2rem; font-size: 0.88rem; color: #8899a6; }}
    input[type="text"], input[type="number"] {{
      width: 100%; max-width: 22rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea; box-sizing: border-box;
    }}
    fieldset {{ border: 1px solid #38444d; border-radius: 8px; margin: 1rem 0; padding: 0.75rem 1rem; }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.55rem 1.1rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin: 0.4rem 0.5rem 0 0;
    }}
    button.secondary {{ background: #38444d; }}
    button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    pre {{
      background: #16181c; border: 1px solid #2f3336; border-radius: 8px;
      padding: 1rem; overflow: auto; font-size: 0.75rem; max-height: 36rem; white-space: pre-wrap;
    }}
    .row {{ display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 1rem; margin: 0.5rem 0; }}
    .hint {{ color: #71767b; font-size: 0.88rem; margin-top: 1.2rem; border-left: 3px solid #38444d; padding-left: 0.75rem; }}
    .ok {{ color: #7af87a; }} .err {{ color: #f66; }}
    #tick {{ font-size: 0.85rem; color: #71767b; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Read workload data (Redis + others)</h1>
  <p>Poll the same stores the workload uses. <strong>Redis</strong> reads keys <code>hub:wl:&lt;run_id&gt;:0 .. n-1</code>.
    <strong>SQL Server</strong> returns recent rows from <code>dbo.hub_workload_mssql</code> for the given <code>run_id</code>.
    Paste the <code>run_id</code> from a workload response (the page saves the last one in the browser), or generate a random id.
    Cap per store is <code>{WORKLOAD_READ_SAMPLE_LIMIT_MAX}</code> (raise with env <code>WORKLOAD_READ_SAMPLE_LIMIT_MAX</code> on hub-demo-ui).</p>
  <div class="row">
    <div style="flex:1;min-width:12rem">
      <label>run_id</label>
      <input type="text" id="run_id" placeholder="e.g. a1b2c3d4" autocomplete="off"/>
    </div>
    <div style="align-self:flex-end">
      <button type="button" class="secondary" id="rand_rid" title="8 hex chars, same shape as workload run_id">Random run_id</button>
    </div>
  </div>
  <label>Rows per store (1–{WORKLOAD_READ_SAMPLE_LIMIT_MAX})</label>
  <input type="number" id="limit" value="10" min="1" max="{WORKLOAD_READ_SAMPLE_LIMIT_MAX}"/>
  <label>Parallel API requests (1–{WORKLOAD_READ_PARALLEL_MAX}) — simulates N clients reading at once</label>
  <input type="number" id="parallel" value="1" min="1" max="{WORKLOAD_READ_PARALLEL_MAX}"/>
  <div class="row" style="margin-top:0.5rem">
    <label><input type="checkbox" id="random_per_req"/> Random <code>run_id</code> per request (ignores field; use with parallel to fan out random keys)</label>
  </div>
  <fieldset>
    <legend>Targets</legend>
    <div class="row">
      <label><input type="checkbox" class="tg" value="postgres" checked/> Postgres</label>
      <label><input type="checkbox" class="tg" value="mongo" checked/> Mongo</label>
      <label><input type="checkbox" class="tg" value="redis" checked/> Redis</label>
      <label><input type="checkbox" class="tg" value="cassandra" checked/> Cassandra</label>
      <label><input type="checkbox" class="tg" value="opensearch" checked/> OpenSearch</label>
      <label><input type="checkbox" class="tg" value="mssql"/> SQL Server</label>
    </div>
  </fieldset>
  <div class="row">
    <label style="margin:0">Poll every
      <select id="interval" style="margin-left:0.35rem;padding:0.35rem;border-radius:6px;background:#16181c;color:#e7e9ea;border:1px solid #38444d">
        <option value="0">Manual only</option>
        <option value="1000">1 s</option>
        <option value="2000" selected>2 s</option>
        <option value="5000">5 s</option>
      </select>
    </label>
  </div>
  <button type="button" id="read">Read now</button>
  <button type="button" class="secondary" id="toggle">Start continuous</button>
  <span id="tick"></span>
  <p id="st"></p>
  <pre id="out">Configure run_id and click Read now, or start continuous polling.</pre>
  <div class="hint">
    <strong>OpenSearch (documents, not container logs):</strong> workload rows live in index <code>hub-workload</code>.
    Open <a href="http://localhost:5601" target="_blank" rel="noopener">Dashboards</a> → <strong>Dev Tools</strong> and run
    <code>GET hub-workload/_search?q=run_id:YOUR_RUN_ID&amp;pretty</code>, or create an index pattern <code>hub-workload*</code> → <strong>Discover</strong>.
    To ship <em>application / Docker logs</em> into OpenSearch you would add a log collector (e.g. Fluent Bit) — this demo only indexes JSON docs from the hub UI.
  </div>
  <script>
    const SAMPLE_CAP = {WORKLOAD_READ_SAMPLE_LIMIT_MAX};
    const PARALLEL_CAP = {WORKLOAD_READ_PARALLEL_MAX};
    function randomRunId() {{
      const a = new Uint8Array(4);
      crypto.getRandomValues(a);
      return [...a].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 8);
    }}
    const out = document.getElementById("out");
    const st = document.getElementById("st");
    const tick = document.getElementById("tick");
    let timer = null;
    let nread = 0;
    try {{
      const x = localStorage.getItem("hub_last_run_id");
      if (x) document.getElementById("run_id").value = x;
    }} catch (e) {{}}
    document.getElementById("rand_rid").addEventListener("click", () => {{
      document.getElementById("run_id").value = randomRunId();
    }});
    async function doRead() {{
      const randomPer = document.getElementById("random_per_req").checked;
      const runField = document.getElementById("run_id").value.trim();
      if (!randomPer && !runField) {{
        st.textContent = "Set run_id or enable Random run_id per request.";
        st.className = "err";
        return;
      }}
      let limit = Number(document.getElementById("limit").value);
      if (!Number.isFinite(limit)) limit = 10;
      limit = Math.min(SAMPLE_CAP, Math.max(1, limit));
      const targets = [...document.querySelectorAll(".tg:checked")].map((x) => x.value);
      if (!targets.length) {{
        st.textContent = "Pick at least one target.";
        st.className = "err";
        return;
      }}
      let parallel = Number(document.getElementById("parallel").value);
      if (!Number.isFinite(parallel)) parallel = 1;
      parallel = Math.min(PARALLEL_CAP, Math.max(1, parallel));

      const bodies = [];
      for (let i = 0; i < parallel; i++) {{
        const run_id = randomPer ? randomRunId() : runField;
        bodies.push({{ run_id, sample_limit: limit, targets }});
      }}

      const started = performance.now();
      const results = await Promise.all(
        bodies.map((body) =>
          fetch("/api/workload/read", {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify(body),
          }}).then(async (r) => ({{
            http_ok: r.ok,
            status: r.status,
            data: await r.json(),
          }}))
        )
      );
      const ms = Math.round(performance.now() - started);

      nread += 1;
      tick.textContent = "batches: " + nread + " · parallel=" + parallel + " · " + ms + "ms · " + new Date().toISOString();

      const allApiOk = results.every((x) => x.http_ok && x.data && x.data.ok);
      const payload = {{
        parallel,
        random_run_id_per_request: randomPer,
        sample_limit: limit,
        wall_ms: ms,
        results: results.map((r, i) => ({{
          index: i,
          run_id: bodies[i].run_id,
          http_ok: r.http_ok,
          status: r.status,
          response: r.data,
        }})),
      }};
      out.textContent = JSON.stringify(payload, null, 2);
      st.textContent = allApiOk
        ? "OK (" + parallel + " request(s), " + ms + " ms wall)"
        : "Some requests failed — see results[].http_ok / response.errors.";
      st.className = allApiOk ? "ok" : "err";
    }}
    document.getElementById("read").addEventListener("click", () => doRead());
    document.getElementById("toggle").addEventListener("click", () => {{
      const btn = document.getElementById("toggle");
      if (timer) {{
        clearInterval(timer);
        timer = null;
        btn.textContent = "Start continuous";
        return;
      }}
      const ms = Number(document.getElementById("interval").value);
      if (!ms) {{
        st.textContent = "Choose a poll interval &gt; Manual only for continuous.";
        st.className = "err";
        return;
      }}
      btn.textContent = "Stop continuous";
      doRead();
      timer = setInterval(doRead, ms);
    }});
  </script>
</body>
</html>
"""

_SCENARIO_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Multi-DB scenario — hub demo</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
  <style>
    :root { font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }
    body { margin: 0; padding: 1rem 1.25rem 2rem; line-height: 1.55; }
    a { color: #6cb5f4; }
    h1 { font-size: 1.35rem; margin-top: 0; }
    h2 { font-size: 1.05rem; margin-top: 1.25rem; color: #8899a6; }
    h3 { font-size: 0.95rem; margin: 1rem 0 0.35rem; color: #c4cfd6; }
    .layout {
      display: grid;
      grid-template-columns: 1fr minmax(280px, 400px);
      gap: 1.5rem;
      align-items: start;
      max-width: 75rem;
      margin: 0 auto;
    }
    @media (max-width: 960px) {
      .layout { grid-template-columns: 1fr; }
      .diagram-aside { position: static !important; max-height: none !important; }
    }
    .main-col { min-width: 0; }
    .diagram-aside {
      position: sticky;
      top: 0.75rem;
      background: #16181c;
      border: 1px solid #38444d;
      border-radius: 10px;
      padding: 0.75rem;
      max-height: calc(100vh - 1.5rem);
      overflow: auto;
    }
    .diagram-aside h2 { margin-top: 0; font-size: 0.95rem; color: #8899a6; }
    .flow-svg { width: 100%; height: auto; display: block; }
    .flow-svg text { font-family: ui-sans-serif, system-ui, sans-serif; fill: #e7e9ea; }
    .flow-svg .muted { fill: #71767b; font-size: 10px; }
    .flow-svg .box { fill: #252a35; stroke: #6cb5f4; stroke-width: 1.25; }
    .flow-svg .step { fill: #1d9bf0; font-size: 11px; font-weight: 700; }
    .flow-svg .arrow { stroke: #8899a6; stroke-width: 1.5; fill: none; marker-end: url(#ah); }
    button {
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.5rem 1.1rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin: 0.35rem 0.35rem 0 0;
    }
    button.secondary { background: #38444d; }
    button:disabled { opacity: 0.5; cursor: not-allowed; }
    pre {
      background: #16181c; border: 1px solid #2f3336; border-radius: 8px;
      padding: 1rem; overflow: auto; font-size: 0.72rem; max-height: 18rem;
    }
    .grid { display: flex; flex-wrap: wrap; gap: 0.5rem; margin: 0.75rem 0; }
    .ok { color: #7af87a; } .err { color: #f66; }
    .hint { color: #71767b; font-size: 0.88rem; }
    details.behind { margin: 0.5rem 0; border-left: 3px solid #38444d; padding-left: 0.75rem; }
    details.behind summary { cursor: pointer; color: #6cb5f4; font-weight: 600; }
    code { font-size: 0.88em; background: #252a30; padding: 0.12em 0.35em; border-radius: 4px; }
    .line-flow-wrap {
      background: #16181c;
      border: 1px solid #38444d;
      border-radius: 10px;
      padding: 0.75rem 0.5rem 1rem;
      margin: 1rem 0 1.25rem;
      overflow-x: auto;
    }
    .line-flow-wrap h2 { margin: 0 0 0.5rem; font-size: 1rem; color: #8899a6; }
    .line-flow { width: 100%; min-width: 560px; height: auto; display: block; }
    .line-flow .spine { stroke: #6cb5f4; stroke-width: 2.5; fill: none; }
    .line-flow .node { fill: #1d9bf0; stroke: #e7e9ea; stroke-width: 1.5; }
    .line-flow .lbl { font-size: 11px; fill: #e7e9ea; font-weight: 600; }
    .line-flow .sub { font-size: 9px; fill: #71767b; }
    .line-flow .fan { stroke: #4a5f78; stroke-width: 1.2; fill: none; }
    .flow-svg .fan { stroke: #4a5f78; stroke-width: 1.2; fill: none; }
    .order-step3 {
      background: #16181c;
      border: 1px solid #38444d;
      border-radius: 10px;
      padding: 1rem 1rem 1.25rem;
      margin: 1rem 0 1.25rem;
    }
    .order-step3 label { display: block; margin: 0.65rem 0 0.2rem; font-size: 0.88rem; color: #8899a6; }
    .order-step3 input[type="text"], .order-step3 input[type="email"], .order-step3 input[type="number"] {
      width: 100%; max-width: 28rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea; box-sizing: border-box;
    }
    .order-step3 select {
      max-width: 28rem; width: 100%; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
    }
    #scenario-map { height: 260px; border-radius: 10px; border: 1px solid #38444d; margin: 0.75rem 0 0.25rem; }
    .order-latlon { display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: flex-end; margin-top: 0.5rem; }
    .order-latlon > div { flex: 1; min-width: 8rem; max-width: 12rem; }
  </style>
</head>
<body>
  @@NAV@@
  <div class="layout">
    <div class="main-col">
      <h1>Multi-DB scenario (Faker + pipelines)</h1>
      <div class="line-flow-wrap">
        <h2>Pipeline line diagram</h2>
        <svg class="line-flow" viewBox="0 0 620 125" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Horizontal pipeline: four connected steps">
          <defs>
            <marker id="line-arr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
              <path d="M0,0 L6,3 L0,6 Z" fill="#6cb5f4"/>
            </marker>
          </defs>
          <line class="spine" x1="82" y1="52" x2="172" y2="52" marker-end="url(#line-arr)"/>
          <line class="spine" x1="202" y1="52" x2="292" y2="52" marker-end="url(#line-arr)"/>
          <line class="spine" x1="322" y1="52" x2="412" y2="52" marker-end="url(#line-arr)"/>
          <line class="spine" x1="442" y1="52" x2="532" y2="52" marker-end="url(#line-arr)"/>
          <circle class="node" cx="70" cy="52" r="10"/>
          <circle class="node" cx="190" cy="52" r="10"/>
          <circle class="node" cx="310" cy="52" r="10"/>
          <circle class="node" cx="430" cy="52" r="10"/>
          <circle class="node" cx="550" cy="52" r="10"/>
          <text x="70" y="28" text-anchor="middle" class="lbl">1 · Seed</text>
          <text x="190" y="28" text-anchor="middle" class="lbl">2 · Sync</text>
          <text x="310" y="28" text-anchor="middle" class="lbl">3 · Order</text>
          <text x="430" y="28" text-anchor="middle" class="lbl">4 · Fulfill</text>
          <text x="550" y="28" text-anchor="middle" class="lbl">◆</text>
          <text x="70" y="78" text-anchor="middle" class="sub">Faker→Mongo</text>
          <text x="190" y="78" text-anchor="middle" class="sub">PG+MS+K+OS+R</text>
          <text x="310" y="78" text-anchor="middle" class="sub">PG+K+OS+R+C*</text>
          <text x="430" y="78" text-anchor="middle" class="sub">PG+K+OS+C*</text>
          <text x="550" y="78" text-anchor="middle" class="sub">end</text>
          <text x="300" y="108" text-anchor="middle" class="sub">C* = Cassandra · K = Kafka · MS = SQL Server (publisher) · OS = hub-scenario-pipeline · R = Redis · PG = Postgres</text>
        </svg>
      </div>
      <p class="hint"><strong>Mongo</strong> is the <em>catalog service</em>: rich product docs in <code>demo.scenario_products</code>.
        <strong>Postgres</strong> holds a <em>relational mirror</em> (<code>scenario_catalog_mirror</code>), <em>orders</em> (<code>scenario_orders</code>), and <em>fulfillment lines</em> (<code>scenario_fulfillment_lines</code>).
        <strong>SQL Server</strong> (when <code>MSSQL_HOST</code> + <code>MSSQL_SA_PASSWORD</code> are set — Compose/K8s: <code>mssql-publisher</code>) gets a second mirror <code>dbo.scenario_catalog_mirror_mssql</code> on step 2 for <em>Debezium CDC</em> → Kafka → JDBC sink to the subscriber; workload generator can also write <code>dbo.hub_workload_mssql</code>.
        <strong>Kafka</strong> gets event payloads for integration testing; the same JSON is written to <strong>OpenSearch</strong> index <code>hub-scenario-pipeline</code> (simulating what a Kafka→OpenSearch sink would index).
        <strong>Redis</strong> stores a small dashboard summary + a rolling list of recent pipeline events + per-order cache keys.
        <strong>Cassandra</strong> appends an <em>order timeline</em> (<code>demo_hub.scenario_timeline</code>) for steps 3–4.</p>

      <h2>Behind the scenes (each button)</h2>
      <details class="behind" open>
        <summary>1 · Seed Mongo catalog (Faker)</summary>
        <p class="hint">Runs <code>scenario.op_seed_catalog</code>: <strong>Faker</strong> generates titles, categories, prices, stock, warehouse, description. Inserts <strong>one document per product</strong> into MongoDB <code>demo.scenario_products</code> (unique <code>sku</code>). No other database is touched yet.</p>
      </details>
      <details class="behind">
        <summary>2 · Sync catalog → Postgres + Kafka + OpenSearch</summary>
        <p class="hint">Runs <code>op_pipeline_mongo_to_postgres_and_kafka</code>: reads up to 80 products from Mongo, <strong>UPSERTs</strong> into Postgres <code>scenario_catalog_mirror</code>. When <code>MSSQL_HOST</code> is set (Compose: <code>mssql-publisher</code>), each product is also <strong>MERGE</strong>d into SQL Server <code>dbo.scenario_catalog_mirror_mssql</code> for Debezium CDC → Kafka → JDBC sink to the subscriber. For each row it sends a message to Kafka topic <code>scenario.catalog.changes</code> (if the broker is reachable) and <strong>indexes the same payload</strong> into OpenSearch <code>hub-scenario-pipeline</code> with direction <code>mongo→kafka+os</code>. Pushes a short entry onto Redis list <code>scenario:kafka:recent</code> and refreshes <code>scenario:dashboard:summary</code> (counts from Postgres + Mongo).</p>
      </details>
      <details class="behind">
        <summary>3 · Place order (Faker + map)</summary>
        <p class="hint"><strong>Form</strong> → <code>POST /api/scenario/order/custom</code>: same as <code>op_place_order</code> but with your <strong>customer name, email</strong>, and <strong>ship_lat / ship_lon / ship_label</strong> (preset city, map click, or Faker). <strong>Quick random</strong> → <code>POST /api/scenario/order</code> (fully server-side Faker for customer + lines). Both paths insert <code>scenario_orders</code>, emit Kafka, OpenSearch, Redis, Cassandra timeline.</p>
      </details>
      <details class="behind">
        <summary>4 · Fulfillment rows + Kafka + OS + Cassandra</summary>
        <p class="hint">Runs <code>op_pipeline_postgres_to_fulfillment_and_kafka</code>: finds Postgres orders that have <strong>no</strong> rows in <code>scenario_fulfillment_lines</code> yet, expands each order’s <code>lines</code> JSON into fulfillment rows, produces <code>scenario.pipeline.sync</code> on Kafka, indexes OpenSearch with <code>postgres→kafka+os</code>, appends <code>FULFILLMENT_READY</code> on Cassandra timeline, commits Postgres.</p>
      </details>

      <h2>Run pipelines (order matters the first time)</h2>
      <p class="hint">You need <strong>catalog in Mongo</strong> before sync; <strong>mirror in Postgres</strong> helps pricing on step 3; step 4 needs <strong>orders</strong> in Postgres that are not yet fulfilled.</p>
      <div>
        <button type="button" id="b_seed">1 · Seed Mongo catalog (Faker)</button>
        <button type="button" id="b_sync">2 · Sync catalog → Postgres + Kafka + OpenSearch</button>
      </div>

      <h2>3 · Place order</h2>
      <p class="hint">Fill customer + shipping (Faker button, preset city, or map). Submits to <code>scenario_orders</code> with <code>ship_lat</code> / <code>ship_lon</code> / <code>ship_label</code>. Or use <strong>Quick random</strong> for an all-server-side demo row.</p>
      <div class="order-step3">
        <button type="button" class="secondary" id="btn_faker_scenario">Fill form with Faker (server)</button>
        <form id="order_form">
          <label>Customer name</label>
          <input type="text" name="customer_name" id="customer_name" required autocomplete="name"/>
          <label>Email</label>
          <input type="email" name="customer_email" id="customer_email" required autocomplete="email"/>
          <label>Preset location</label>
          <select id="loc_preset">
            <option value="">Custom — use map / Faker only</option>
            <option value="60.1699,24.9384,Helsinki, Finland">Helsinki, Finland</option>
            <option value="59.4370,24.7536,Tallinn, Estonia">Tallinn, Estonia</option>
            <option value="51.5074,-0.1278,London, UK">London, UK</option>
            <option value="52.5200,13.4050,Berlin, Germany">Berlin, Germany</option>
            <option value="48.8566,2.3522,Paris, France">Paris, France</option>
            <option value="40.7128,-74.0060,New York, USA">New York, USA</option>
            <option value="37.7749,-122.4194,San Francisco, USA">San Francisco, USA</option>
            <option value="19.0760,72.8777,Mumbai, India">Mumbai, India</option>
            <option value="35.6762,139.6503,Tokyo, Japan">Tokyo, Japan</option>
          </select>
          <label>Location label (optional)</label>
          <input type="text" name="ship_label" id="ship_label" placeholder="e.g. pinned address" maxlength="500"/>
          <p class="hint" style="margin:0.5rem 0 0">Map: click or drag marker. Coordinates are stored on the order.</p>
          <div id="scenario-map"></div>
          <div class="order-latlon">
            <div><label>Latitude</label><input type="text" name="ship_lat" id="ship_lat" required readonly style="opacity:0.95"/></div>
            <div><label>Longitude</label><input type="text" name="ship_lon" id="ship_lon" required readonly style="opacity:0.95"/></div>
          </div>
          <label>Line items (SKUs from Mongo catalog)</label>
          <input type="number" name="lines_count" id="lines_count" value="3" min="1" max="10"/>
          <div style="margin-top:1rem">
            <button type="submit" id="btn_submit_order">Place order with this profile</button>
          </div>
        </form>
        <p class="hint" style="margin:0.75rem 0 0">No form — one click, random customer + lines entirely on the server:</p>
        <button type="button" class="secondary" id="b_order_quick">Quick random order (server-side)</button>
      </div>

      <div>
        <button type="button" id="b_fulfill">4 · Fulfillment rows + Kafka + OS + Cassandra</button>
      </div>
      <p id="st"></p>
      <pre id="out">Click a step or submit the order form to see JSON.</pre>
      <h2>View data per store</h2>
      <div class="grid">
        <a href="/scenario/data/postgres">Postgres</a>
        <a href="/scenario/data/mongo">Mongo</a>
        <a href="/scenario/data/redis">Redis</a>
        <a href="/scenario/data/cassandra">Cassandra</a>
        <a href="/scenario/data/opensearch">OpenSearch</a>
        <a href="/scenario/data/kafka">Kafka (meta)</a>
        <a href="/scenario/data/mssql">SQL Server</a>
      </div>
    </div>
    <aside class="diagram-aside">
      <h2>Vertical line (detail)</h2>
      <p class="hint" style="margin-top:0">Spine + branches. Same steps as the horizontal line above.</p>
      <svg class="flow-svg" viewBox="0 0 340 560" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Vertical scenario timeline">
        <defs>
          <marker id="ah" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
            <path d="M0,0 L8,4 L0,8 Z" fill="#8899a6"/>
          </marker>
        </defs>
        <line x1="40" y1="28" x2="40" y2="428" stroke="#6cb5f4" stroke-width="3" stroke-linecap="round"/>
        <circle class="node" cx="40" cy="40" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <circle class="node" cx="40" cy="148" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <circle class="node" cx="40" cy="296" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <circle class="node" cx="40" cy="416" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <path class="arrow" d="M55 40 L115 40"/>
        <text x="120" y="44" font-size="11px" fill="#e7e9ea" font-weight="700">① Faker → Mongo</text>
        <text x="120" y="58" class="muted">demo.scenario_products</text>
        <path class="arrow" d="M55 148 L115 148"/>
        <text x="120" y="136" font-size="11px" fill="#e7e9ea" font-weight="700">② Sync catalog</text>
        <text x="120" y="150" class="muted">PG+MS+K+OS+R · mirrors → bus</text>
        <text x="118" y="164" class="muted" font-size="10px">MS · dbo.scenario_catalog_mirror_mssql (Debezium CDC)</text>
        <text x="118" y="178" class="muted" font-size="10px">K · scenario.catalog.changes</text>
        <text x="118" y="192" class="muted" font-size="10px">OS · index hub-scenario-pipeline</text>
        <path class="arrow" d="M55 296 L115 296"/>
        <text x="120" y="288" font-size="11px" fill="#e7e9ea" font-weight="700">③ New order</text>
        <text x="120" y="302" class="muted">PG+K+OS+R+C* · scenario_orders</text>
        <path class="arrow" d="M55 416 L115 416"/>
        <text x="120" y="408" font-size="11px" fill="#e7e9ea" font-weight="700">④ Fulfillment</text>
        <text x="120" y="422" class="muted">PG+K+OS+C* · scenario_fulfillment_lines</text>
        <text x="16" y="452" font-size="10px" fill="#8899a6">Key · C* Cassandra · K Kafka · MS SQL Server · OS OpenSearch · R Redis · PG Postgres</text>
        <text x="16" y="468" class="muted" font-size="10px">MS hub_workload on publisher (workload page) · pymssql + FreeTDS in hub image</text>
        <text x="16" y="484" class="muted" font-size="10px">PG workload SQL · pg_stat_statements on DB postgres</text>
        <text x="16" y="500" class="muted" font-size="10px">ORDER_PLACED (step ③) · FULFILLMENT_READY (step ④)</text>
        <text x="16" y="518" font-size="10px" fill="#8899a6">Kafka topics</text>
        <text x="16" y="532" class="muted" font-size="10px">scenario.catalog.changes · scenario.orders.events</text>
        <text x="16" y="546" class="muted" font-size="10px">scenario.pipeline.sync</text>
      </svg>
    </aside>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <script>
    async function call(path, st, out) {
      st.textContent = "…";
      st.className = "";
      out.textContent = "";
      try {
        const r = await fetch(path, { method: "POST" });
        const data = await r.json();
        out.textContent = JSON.stringify(data, null, 2);
        st.textContent = r.ok && data.ok !== false ? "OK." : "See JSON.";
        st.className = r.ok && data.ok !== false ? "ok" : "err";
      } catch (e) {
        st.textContent = String(e);
        st.className = "err";
      }
    }
    const st = document.getElementById("st");
    const out = document.getElementById("out");
    document.getElementById("b_seed").onclick = () => call("/api/scenario/seed?count=12", st, out);
    document.getElementById("b_sync").onclick = () => call("/api/scenario/pipeline/mongo-sync", st, out);
    document.getElementById("b_order_quick").onclick = () => call("/api/scenario/order", st, out);
    document.getElementById("b_fulfill").onclick = () => call("/api/scenario/pipeline/fulfill", st, out);

    const latEl = document.getElementById("ship_lat");
    const lonEl = document.getElementById("ship_lon");
    const scenMap = L.map("scenario-map").setView([60.1699, 24.9384], 11);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap",
    }).addTo(scenMap);
    let scenMarker = L.marker([60.1699, 24.9384], { draggable: true }).addTo(scenMap);
    function setShipPos(lat, lng) {
      latEl.value = lat.toFixed(6);
      lonEl.value = lng.toFixed(6);
      scenMarker.setLatLng([lat, lng]);
    }
    setShipPos(60.1699, 24.9384);
    scenMap.on("click", (e) => {
      document.getElementById("loc_preset").value = "";
      setShipPos(e.latlng.lat, e.latlng.lng);
    });
    scenMarker.on("dragend", (e) => {
      document.getElementById("loc_preset").value = "";
      const p = e.target.getLatLng();
      setShipPos(p.lat, p.lng);
    });
    document.getElementById("loc_preset").addEventListener("change", (ev) => {
      const v = ev.target.value;
      if (!v) return;
      const parts = v.split(",");
      if (parts.length < 4) return;
      const lat = Number(parts[0]);
      const lon = Number(parts[1]);
      const ship = parts.slice(2).join(",");
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
      document.getElementById("ship_label").value = ship;
      setShipPos(lat, lon);
      scenMap.setView([lat, lon], 8);
    });
    document.getElementById("btn_faker_scenario").addEventListener("click", async () => {
      st.textContent = "Loading…";
      st.className = "";
      try {
        const r = await fetch("/api/scenario/faker-profile");
        const d = await r.json();
        if (!r.ok) throw new Error(d.detail || JSON.stringify(d));
        document.getElementById("loc_preset").value = "";
        document.getElementById("customer_name").value = d.customer_name || "";
        document.getElementById("customer_email").value = d.customer_email || "";
        document.getElementById("ship_label").value = d.ship_label || "";
        if (typeof d.ship_lat === "number" && typeof d.ship_lon === "number") {
          setShipPos(d.ship_lat, d.ship_lon);
          scenMap.setView([d.ship_lat, d.ship_lon], 6);
        }
        st.textContent = "Form filled.";
        st.className = "ok";
      } catch (e) {
        st.textContent = String(e);
        st.className = "err";
      }
    });
    document.getElementById("order_form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const btn = document.getElementById("btn_submit_order");
      btn.disabled = true;
      st.textContent = "Placing order…";
      st.className = "";
      out.textContent = "";
      const body = {
        customer_name: document.getElementById("customer_name").value.trim(),
        customer_email: document.getElementById("customer_email").value.trim(),
        ship_label: document.getElementById("ship_label").value.trim() || null,
        ship_lat: Number(latEl.value),
        ship_lon: Number(lonEl.value),
        lines_count: Number(document.getElementById("lines_count").value),
      };
      try {
        const r = await fetch("/api/scenario/order/custom", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await r.json();
        out.textContent = JSON.stringify(data, null, 2);
        st.textContent = r.ok && data.ok ? "OK — see scenario_orders + other stores." : "See JSON.";
        st.className = r.ok && data.ok ? "ok" : "err";
      } catch (e) {
        st.textContent = String(e);
        st.className = "err";
      } finally {
        btn.disabled = false;
      }
    });
    setTimeout(() => { scenMap.invalidateSize(); }, 100);
  </script>
</body>
</html>
"""

SCENARIO_PAGE = _SCENARIO_PAGE_TEMPLATE.replace("@@NAV@@", NAV)

SCENARIO_DATA_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Scenario data view — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 58rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    a {{ color: #6cb5f4; }}
    pre {{ background: #16181c; border: 1px solid #2f3336; border-radius: 8px; padding: 1rem; overflow: auto; font-size: 0.76rem; max-height: 36rem; white-space: pre-wrap; }}
  </style>
</head>
<body>
  {NAV}
  <h1 id="title">Loading…</h1>
  <p><a href="/scenario">← Scenario hub</a></p>
  <pre id="out"></pre>
  <script>
    const store = location.pathname.split("/").pop();
    document.getElementById("title").textContent = "Data: " + store;
    fetch("/api/scenario/view/" + encodeURIComponent(store))
      .then((r) => r.json())
      .then((d) => {{ document.getElementById("out").textContent = JSON.stringify(d, null, 2); }})
      .catch((e) => {{ document.getElementById("out").textContent = String(e); }});
  </script>
</body>
</html>
"""


CONNECTIONS_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>External databases — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 44rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    a {{ color: #6cb5f4; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; }}
    p.note {{ color: #8899a6; font-size: 0.9rem; }}
    label {{ display: block; margin: 0.85rem 0 0.25rem; font-size: 0.88rem; color: #c8d0d8; }}
    input[type="text"] {{
      width: 100%; box-sizing: border-box; padding: 0.45rem 0.6rem;
      border-radius: 6px; border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
      font-family: ui-monospace, monospace; font-size: 0.82rem;
    }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.55rem 1.2rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin: 1rem 0.75rem 0 0;
    }}
    button.secondary {{ background: #38444d; }}
    #status {{ margin-top: 1rem; font-size: 0.9rem; }}
    #status.ok {{ color: #7af87a; }}
    #status.err {{ color: #f66; }}
    pre {{ background: #16181c; border: 1px solid #2f3336; border-radius: 8px; padding: 1rem; overflow: auto; font-size: 0.78rem; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Optional external connections</h1>
  <p class="note">The stack still runs with the in-cluster (or Compose) databases from environment variables.
    Use this page only when you want <strong>this browser session</strong> to send demo writes and scenario traffic
    to your own MongoDB, Postgres, Redis, Cassandra, or OpenSearch instead. Leave a field empty and save to clear
    that override. Secrets are stored in a signed session cookie — use HTTPS in production; for shared demos set
    <code>HUB_SESSION_SECRET</code> on the server.</p>
  <form id="connf">
    <label>Postgres DSN <span style="color:#71767b">(postgresql://…)</span></label>
    <input type="text" name="postgres_dsn" autocomplete="off" placeholder="Leave blank for env default"/>
    <label>MongoDB URI</label>
    <input type="text" name="mongo_uri" autocomplete="off" placeholder="mongodb://… or mongodb+srv://…"/>
    <label>Redis URL</label>
    <input type="text" name="redis_url" autocomplete="off" placeholder="redis://…"/>
    <label>Cassandra contact points <span style="color:#71767b">(comma-separated hosts)</span></label>
    <input type="text" name="cassandra_hosts" autocomplete="off" placeholder="host1,host2"/>
    <label>Cassandra keyspace <span style="color:#71767b">(letters, digits, underscore; default demo_hub)</span></label>
    <input type="text" name="cassandra_keyspace" autocomplete="off" placeholder="demo_hub"/>
    <label>OpenSearch base URL</label>
    <input type="text" name="opensearch_url" autocomplete="off" placeholder="https://search-….amazonaws.com"/>
    <label>Hub single-order / CDC index name</label>
    <input type="text" name="opensearch_index" autocomplete="off" placeholder="hub-orders"/>
    <label>Workload index name</label>
    <input type="text" name="opensearch_workload_index" autocomplete="off" placeholder="hub-workload"/>
    <label>Scenario pipeline index name</label>
    <input type="text" name="scenario_opensearch_index" autocomplete="off" placeholder="hub-scenario-pipeline"/>
    <button type="submit">Save session overrides</button>
    <button type="button" class="secondary" id="clearbtn">Clear all overrides</button>
  </form>
  <p id="status"></p>
  <pre id="out">Loading status…</pre>
  <script>
    async function refresh() {{
      const r = await fetch("/api/connections");
      const d = await r.json();
      document.getElementById("out").textContent = JSON.stringify(d, null, 2);
    }}
    document.getElementById("connf").addEventListener("submit", async (ev) => {{
      ev.preventDefault();
      const st = document.getElementById("status");
      const fd = new FormData(ev.target);
      const body = {{}};
      for (const [k, v] of fd.entries()) {{
        const s = String(v).trim();
        if (s) body[k] = s;
        else body[k] = "";
      }}
      st.textContent = "Saving…";
      st.className = "";
      try {{
        const r = await fetch("/api/connections", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body),
        }});
        const d = await r.json();
        st.textContent = r.ok ? "Saved. Overrides apply to this browser session only." : (d.detail || r.statusText);
        st.className = r.ok ? "ok" : "err";
        await refresh();
      }} catch (e) {{
        st.textContent = String(e);
        st.className = "err";
      }}
    }});
    document.getElementById("clearbtn").addEventListener("click", async () => {{
      const st = document.getElementById("status");
      st.textContent = "Clearing…";
      try {{
        const r = await fetch("/api/connections/clear", {{ method: "POST" }});
        st.textContent = r.ok ? "Cleared — using env defaults again." : "Clear failed";
        st.className = r.ok ? "ok" : "err";
        document.getElementById("connf").reset();
        await refresh();
      }} catch (e) {{
        st.textContent = String(e);
        st.className = "err";
      }}
    }});
    refresh();
  </script>
</body>
</html>
"""


KAFKA_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Kafka lab — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 46rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    a {{ color: #6cb5f4; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; }}
    h2 {{ font-size: 1.05rem; margin-top: 1.75rem; color: #c8d0d8; }}
    p.note {{ color: #8899a6; font-size: 0.88rem; }}
    label {{ display: block; margin: 0.55rem 0 0.2rem; font-size: 0.82rem; color: #c8d0d8; }}
    input[type="text"], input[type="number"], select {{
      width: 100%; max-width: 28rem; box-sizing: border-box; padding: 0.4rem 0.55rem;
      border-radius: 6px; border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
      font-family: ui-monospace, monospace; font-size: 0.82rem;
    }}
    fieldset {{ border: 1px solid #38444d; border-radius: 8px; margin: 1rem 0; padding: 0.75rem 1rem; }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.55rem 1.15rem; font-size: 0.92rem; font-weight: 600; cursor: pointer;
      margin: 0.5rem 0.65rem 0 0;
    }}
    button.secondary {{ background: #38444d; }}
    button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    #status {{ margin-top: 0.85rem; font-size: 0.88rem; }}
    #status.ok {{ color: #7af87a; }} #status.err {{ color: #f66; }}
    pre {{ background: #16181c; border: 1px solid #2f3336; border-radius: 8px;
           padding: 1rem; overflow: auto; font-size: 0.74rem; max-height: 28rem; }}
    pre.kafka-pre {{ margin-top: 0.35rem; }}
    h3.kafka-out-h {{ font-size: 0.98rem; font-weight: 600; margin: 1.25rem 0 0.35rem; color: #c8d0d8; }}
    p.kafka-status {{ margin: 0.25rem 0 0; font-size: 0.88rem; min-height: 1.2em; }}
    p.kafka-status.ok {{ color: #7af87a; }}
    p.kafka-status.err {{ color: #f66; }}
    p.kafka-out-note {{ margin: 0.4rem 0 0.75rem; }}
    code {{ font-size: 0.82rem; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Kafka lab</h1>
  <p class="note">Uses <code>KAFKA_BOOTSTRAP</code> from the hub container (<strong>{kafka_lab.KAFKA_BOOTSTRAP}</strong>).
    Produce bursts with tunable <strong>acks</strong>, <strong>linger</strong>, <strong>batch_size</strong>, <strong>compression</strong>, and key strategy (ordering demos).
    Short-lived consumers use ephemeral <strong>group_id</strong> by default so each poll can read from <strong>earliest</strong> without resetting offsets manually.
    <strong>latest</strong> only sees records appended <em>after</em> that consumer joins — produce first and use <strong>earliest</strong> to read an existing backlog.</p>

  <h2>Producer load</h2>
  <fieldset>
    <form id="prod">
      <label>Topic</label>
      <input type="text" name="topic" value="{kafka_lab.DEFAULT_LAB_TOPIC}" autocomplete="off"/>
      <label>Messages (1–20000)</label>
      <input type="number" name="count" value="200" min="1" max="20000"/>
      <label>Key mode</label>
      <select name="key_mode">
        <option value="per_message">per_message (spread partitions)</option>
        <option value="fixed">fixed key (single partition)</option>
        <option value="random">random key</option>
        <option value="none">none</option>
      </select>
      <label>Fixed key (when mode=fixed)</label>
      <input type="text" name="fixed_key" value="lab-key"/>
      <label>acks</label>
      <select name="acks">
        <option value="0">0</option>
        <option value="1" selected>1</option>
        <option value="all">all</option>
      </select>
      <label>linger_ms</label>
      <input type="number" name="linger_ms" value="5" min="0" max="30000"/>
      <label>batch_size (bytes)</label>
      <input type="number" name="batch_size" value="16384" min="1024" max="5000000"/>
      <label>compression</label>
      <select name="compression">
        <option value="none">none</option>
        <option value="gzip">gzip</option>
        <option value="snappy">snappy</option>
        <option value="lz4">lz4</option>
        <option value="zstd">zstd</option>
      </select>
      <label>Value pad (KiB per message — increases payload)</label>
      <input type="number" name="value_pad_kb" value="0" min="0" max="512"/>
      <label style="display:flex;align-items:center;gap:0.5rem;max-width:28rem;">
        <input type="checkbox" name="enable_idempotence" style="width:auto"/> enable_idempotence (forces acks=all)
      </label>
      <button type="submit">Produce</button>
    </form>
  </fieldset>

  <h3 class="kafka-out-h">Producer response (JSON)</h3>
  <p id="statusProduce" class="kafka-status"></p>
  <pre id="outProduce" class="kafka-pre">Submit <strong>Produce</strong> — expect <code>ok</code>, <code>elapsed_sec</code>, <code>approx_throughput_rps</code>, <code>effective_acks</code>, …</pre>

  <h2>Consumer poll</h2>
  <p class="note kafka-out-note"><strong>Broker topics</strong> and <strong>Hints</strong> write JSON to the <strong>Consumer / broker</strong> panel under the form.</p>
  <button type="button" id="btnMeta">Broker topics (metadata)</button>
  <button type="button" class="secondary" id="btnHints">Hints cheat-sheet</button>

  <fieldset>
    <form id="cons">
      <label>Topic</label>
      <input type="text" name="topic" value="{kafka_lab.DEFAULT_LAB_TOPIC}" autocomplete="off"/>
      <label>group_id (optional — empty = random ephemeral group)</label>
      <input type="text" name="group_id" value="" placeholder="demo-hub-my-group" autocomplete="off"/>
      <label>max_messages</label>
      <input type="number" name="max_messages" value="30" min="1" max="500"/>
      <label>timeout_ms</label>
      <input type="number" name="timeout_ms" value="15000" min="500" max="120000"/>
      <label>auto_offset_reset</label>
      <select name="auto_offset_reset">
        <option value="earliest">earliest</option>
        <option value="latest">latest</option>
      </select>
      <button type="submit">Consume poll</button>
    </form>
  </fieldset>

  <h3 class="kafka-out-h">Consumer / broker JSON</h3>
  <p id="statusConsume" class="kafka-status"></p>
  <pre id="outConsume" class="kafka-pre"><strong>Consume poll</strong> → <code>messages</code>, <code>count</code>, <code>auto_offset_reset</code>, <code>group_id</code>. <strong>latest</strong> + backlog already produced → often <code>count: 0</code>; use <strong>earliest</strong> to read older data.</pre>

  <script>
    const statusProduce = document.getElementById("statusProduce");
    const outProduce = document.getElementById("outProduce");
    const statusConsume = document.getElementById("statusConsume");
    const outConsume = document.getElementById("outConsume");

    async function readJSON(url, opts) {{
      const r = await fetch(url, opts);
      const text = await r.text();
      if (!r.ok) {{
        throw new Error(`HTTP ${{r.status}} ${{r.statusText || ""}}: ${{text.slice(0, 1200)}}`);
      }}
      try {{
        return JSON.parse(text);
      }} catch (parseErr) {{
        throw new Error(`Expected JSON from ${{url}}; body starts with: ${{text.slice(0, 400)}}`);
      }}
    }}

    async function showPanel(statusEl, outEl, fetchFn) {{
      statusEl.textContent = "Working…";
      statusEl.className = "kafka-status";
      outEl.textContent = "";
      try {{
        const d = await fetchFn();
        outEl.textContent = JSON.stringify(d, null, 2);
        const errMsg = d && typeof d === "object" && d.ok === false ? (d.error || "failed") : null;
        statusEl.textContent = errMsg || "OK";
        statusEl.className = errMsg ? "kafka-status err" : "kafka-status ok";
      }} catch (e) {{
        const msg = String(e);
        statusEl.textContent = msg;
        statusEl.className = "kafka-status err";
        outEl.textContent = msg;
      }}
    }}

    document.getElementById("btnMeta").onclick = () =>
      showPanel(statusConsume, outConsume, async () => readJSON("/api/kafka/lab/metadata"));

    document.getElementById("btnHints").onclick = () =>
      showPanel(statusConsume, outConsume, async () => readJSON("/api/kafka/lab/hints"));

    function fdJSON(fd, producerForm) {{
      const o = {{}};
      for (const [k, v] of fd.entries()) {{
        if (k === "enable_idempotence") continue;
        if (v === "") continue;
        const numKeys = new Set(["count", "linger_ms", "batch_size", "value_pad_kb", "max_messages", "timeout_ms"]);
        o[k] = numKeys.has(k) ? Number(v) : String(v);
      }}
      if (producerForm) {{
        const cb = document.querySelector("#prod input[name=enable_idempotence]");
        o.enable_idempotence = !!(cb && cb.checked);
      }}
      return o;
    }}

    document.getElementById("prod").onsubmit = async (ev) => {{
      ev.preventDefault();
      const body = fdJSON(new FormData(ev.target), true);
      await showPanel(statusProduce, outProduce, async () =>
        readJSON("/api/kafka/lab/produce", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body),
        }}));
    }};

    document.getElementById("cons").onsubmit = async (ev) => {{
      ev.preventDefault();
      const body = fdJSON(new FormData(ev.target), false);
      await showPanel(statusConsume, outConsume, async () =>
        readJSON("/api/kafka/lab/consume", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body),
        }}));
    }};
  </script>
</body>
</html>
"""


TRINO_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Trino SQL — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 52rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
    a {{ color: #6cb5f4; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; }}
    p.note {{ color: #8899a6; font-size: 0.9rem; }}
    label {{ display: block; margin: 0.5rem 0 0.25rem; font-size: 0.85rem; color: #c8d0d8; }}
    select, textarea {{
      width: 100%; box-sizing: border-box; border-radius: 8px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
      font-family: ui-monospace, monospace; font-size: 0.82rem;
    }}
    textarea {{ min-height: 9rem; padding: 0.65rem; resize: vertical; }}
    select {{ padding: 0.45rem 0.5rem; }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.55rem 1.2rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin-top: 0.85rem;
    }}
    button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    #status {{ margin-top: 0.75rem; font-size: 0.9rem; }}
    #status.ok {{ color: #7af87a; }}
    #status.err {{ color: #f66; }}
    pre {{ background: #16181c; border: 1px solid #2f3336; border-radius: 8px; padding: 1rem; overflow: auto; font-size: 0.76rem; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Trino (federated SQL)</h1>
  <p class="note">Kubernetes stacks expose catalogs <code>demo_pg</code>, <code>demo_mongo</code>,
    <code>demo_es</code> (OpenSearch via Elasticsearch connector), <code>demo_sqlserver</code>, and <code>memory</code>.
    The hub only allows read-only statements (<code>SELECT</code>, <code>SHOW</code>, <code>DESCRIBE</code>, …).
    Set env <code>TRINO_HTTP</code> (empty in plain Docker Compose unless you add Trino).</p>
  <label>Catalog</label>
  <select id="catalog">
    <option value="demo_pg">demo_pg (Postgres)</option>
    <option value="demo_mongo">demo_mongo</option>
    <option value="demo_es">demo_es</option>
    <option value="demo_sqlserver">demo_sqlserver</option>
    <option value="memory">memory</option>
  </select>
  <label>Schema <span style="color:#71767b">(optional, e.g. public)</span></label>
  <input id="schema" type="text" placeholder="public" autocomplete="off"
    style="width:100%;box-sizing:border-box;padding:0.45rem 0.55rem;border-radius:8px;border:1px solid #38444d;background:#16181c;color:#e7e9ea;font-family:ui-monospace,monospace;font-size:0.82rem;"/>
  <label>SQL</label>
  <textarea id="sql" spellcheck="false">SELECT table_schema, table_name FROM demo_pg.information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1,2 LIMIT 30</textarea>
  <button type="button" id="run">Run</button>
  <p id="status"></p>
  <pre id="out">Results appear here.</pre>
  <script>
    document.getElementById("run").onclick = async () => {{
      const st = document.getElementById("status");
      const out = document.getElementById("out");
      const sql = document.getElementById("sql").value;
      const catalog = document.getElementById("catalog").value;
      const schema = document.getElementById("schema").value.trim();
      st.textContent = "Running…";
      st.className = "";
      out.textContent = "";
      try {{
        const r = await fetch("/api/trino/query", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ sql, catalog, schema: schema || null }}),
        }});
        const data = await r.json().catch(() => ({{ detail: r.statusText }}));
        if (!r.ok) {{
          st.textContent = data.detail || r.statusText;
          st.className = "err";
          out.textContent = JSON.stringify(data, null, 2);
          return;
        }}
        st.textContent = data.truncated ? `OK (${{data.row_count}} rows, truncated)` : `OK (${{data.row_count}} rows)`;
        st.className = "ok";
        out.textContent = JSON.stringify(data, null, 2);
      }} catch (e) {{
        st.textContent = String(e);
        st.className = "err";
      }}
    }};
  </script>
</body>
</html>
"""


POSTGRES_LANDING_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Postgres — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 48rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.55; }}
    a {{ color: #6cb5f4; }}
    h1 {{ font-size: 1.4rem; font-weight: 600; margin-bottom: 0.35rem; }}
    p.lead {{ color: #8899a6; font-size: 0.95rem; margin: 0 0 1.25rem; }}
    .tiles {{ display: grid; gap: 1rem; grid-template-columns: 1fr; }}
    @media (min-width: 560px) {{ .tiles {{ grid-template-columns: 1fr 1fr; }} }}
    .tile {{
      display: block; padding: 1rem 1.15rem; border-radius: 12px;
      border: 1px solid #38444d; background: #16181c; text-decoration: none; color: inherit;
      transition: border-color 0.15s ease, background 0.15s ease;
    }}
    .tile:hover {{ border-color: #6cb5f4; background: #1a1f26; }}
    .tile h2 {{ font-size: 1.05rem; margin: 0 0 0.35rem; color: #e7e9ea; }}
    .tile p {{ margin: 0; font-size: 0.88rem; color: #8899a6; line-height: 1.45; }}
    .tile .tag {{ display: inline-block; margin-top: 0.65rem; font-size: 0.72rem; letter-spacing: 0.03em;
      text-transform: uppercase; color: #6cb5f4; }}
    ul.more {{ margin: 1.5rem 0 0; padding-left: 1.15rem; color: #8899a6; font-size: 0.9rem; }}
    ul.more li {{ margin: 0.35rem 0; }}
    code {{ font-size: 0.88em; background: #252a30; padding: 0.1em 0.35em; border-radius: 4px; }}
  </style>
</head>
<body>
  {NAV}
  <h1>Postgres</h1>
  <p class="lead">Demos and shortcuts that only involve PostgreSQL in this stack (HA primary + replicas, Debezium elsewhere on <code>demo</code>).</p>
  <div class="tiles">
    <a class="tile" href="/postgres/logical">
      <h2>Logical replication</h2>
      <p>Publisher on <code>postgresql-primary</code> (<code>demo_logical_pub</code>), subscriber on <code>postgres-sub</code>
        (<code>demo_logical_sub</code>); streaming replicas follow the primary only. Browser inserts + optional replica publisher counts.</p>
      <span class="tag">Publisher / subscriber</span>
    </a>
    <a class="tile" href="/scenario/data/postgres">
      <h2>Scenario · Postgres data</h2>
      <p>Peek at relational mirror + orders + fulfillment tables populated by the multi-DB scenario pipeline.</p>
      <span class="tag">Read-only JSON</span>
    </a>
    <a class="tile" href="/postgres/faker-schema">
      <h2>Faker · Schema objects</h2>
      <p>Create templated tables, sequences, views, materialized views, SQL functions, and procedures — names/layout driven by Faker presets on <code>demo_logical_pub</code> or <code>demo_logical_sub</code>.</p>
      <span class="tag">DDL playground</span>
    </a>
  </div>
  <ul class="more">
    <li><a href="/">Single order</a> — writes <code>demo_items</code> (plus other backends).</li>
    <li><a href="/workload">Workload</a> — batch load <code>demo_items</code> when Postgres is selected.</li>
    <li><a href="/reads">Read-back</a> — sample workload rows from Postgres among other stores.</li>
    <li><a href="/connections">External DBs</a> — session override for <code>POSTGRES_DSN</code>.</li>
  </ul>
</body>
</html>
"""


POSTGRES_FAKER_SCHEMA_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Postgres — Faker schema objects</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 46rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.55; }}
    a {{ color: #6cb5f4; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; }}
    p.note {{ color: #8899a6; font-size: 0.9rem; }}
    label {{ display: block; margin: 0.65rem 0 0.25rem; font-size: 0.85rem; color: #8899a6; }}
    input[type="number"] {{
      width: 4.5rem; padding: 0.35rem 0.4rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
    }}
    select {{
      max-width: 28rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
    }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.55rem 1.15rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin-top: 1rem;
    }}
    button.secondary {{ background: #38444d; }}
    pre {{
      background: #16181c; border: 1px solid #2f3336; border-radius: 8px;
      padding: 1rem; overflow: auto; font-size: 0.78rem; max-height: 28rem; white-space: pre-wrap;
    }}
    .ok {{ color: #7af87a; }} .err {{ color: #f66; }}
    #st {{ margin-top: 0.75rem; font-size: 0.9rem; }}
    .grid {{
      display: grid; gap: 0.35rem 1.25rem; grid-template-columns: 1fr auto;
      align-items: center; max-width: 28rem; margin-top: 0.35rem;
    }}
    .grid label {{ margin: 0; }}
    code {{ font-size: 0.88em; background: #252a30; padding: 0.08em 0.32em; border-radius: 4px; }}
    hr.sep {{ border: 0; border-top: 1px solid #2f3336; margin: 1.35rem 0; }}
    h2.sec {{ font-size: 1.05rem; font-weight: 600; margin: 0 0 0.35rem; }}
    textarea.input-lg {{
      width: 100%; max-width: 36rem; min-height: 6.5rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
      font-family: ui-monospace, monospace; font-size: 0.78rem; box-sizing: border-box;
    }}
    input[type="text"].tbl-name {{
      width: 100%; max-width: 28rem; padding: 0.4rem 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea; box-sizing: border-box;
    }}
    .chk-row {{ margin: 0.45rem 0; font-size: 0.88rem; color: #e7e9ea; }}
    .chk-row input {{ margin-right: 0.35rem; vertical-align: middle; }}
    .faker-row {{
      display: flex; flex-wrap: wrap; gap: 0.65rem 1rem; align-items: flex-end;
    }}
    .faker-row label {{ margin: 0; font-size: 0.82rem; color: #8899a6; }}
    .faker-row input[type="number"] {{
      width: 5.25rem; padding: 0.35rem 0.4rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea; box-sizing: border-box;
    }}
  </style>
</head>
<body>
  {NAV}
  {POSTGRES_BAR}
  <h1>Faker-driven schema objects</h1>
  <p class="note"><strong>Quick bundle:</strong> preset tables + sequences/views/MVs/functions/procedures.
    <strong>Custom table:</strong> define column types in JSON, optional <code>UNIQUE</code> groups and <code>FOREIGN KEY</code>s (reference existing tables). <strong>Faker inserts:</strong> generate sample rows into the selected DB using the demo user.</p>
  <p class="note">Runs as the hub admin connection for DDL and <code>demo</code> for inserts. Objects use random names where noted and <strong>accumulate</strong> across runs (no drop).</p>
  <p class="note">Logical replication is unaffected unless you add new tables to your publication yourself.</p>
  <label for="fsDb">Database</label>
  <select id="fsDb" aria-label="Target database">
    <option value="demo_logical_pub">demo_logical_pub (publisher primary)</option>
    <option value="demo_logical_sub">demo_logical_sub (logical subscriber)</option>
  </select>
  <label for="fsPreset">Table preset (when tables &gt; 0)</label>
  <select id="fsPreset">
    <option value="mixed">mixed</option>
    <option value="people">people</option>
    <option value="commerce">commerce</option>
  </select>
  <label for="fsSeed">Seed (optional, tables only)</label>
  <input type="number" id="fsSeed" placeholder="random" style="max-width:10rem"/>
  <p class="note" style="margin-top:1rem;margin-bottom:0.25rem">How many objects to create this run (caps enforced server-side):</p>
  <div class="grid">
    <label for="fsTables">Tables</label><input type="number" id="fsTables" min="0" max="3" value="1"/>
    <label for="fsSeq">Sequences</label><input type="number" id="fsSeq" min="0" max="6" value="0"/>
    <label for="fsViews">Views</label><input type="number" id="fsViews" min="0" max="4" value="0"/>
    <label for="fsMv">Materialized views</label><input type="number" id="fsMv" min="0" max="3" value="0"/>
    <label for="fsFn">Functions (SQL)</label><input type="number" id="fsFn" min="0" max="4" value="0"/>
    <label for="fsProc">Procedures (plpgsql)</label><input type="number" id="fsProc" min="0" max="4" value="0"/>
  </div>
  <button type="button" id="fsApply">Create quick bundle</button>

  <hr class="sep"/>
  <h2 class="sec">Multi-schema tables (readable names)</h2>
  <p class="note">Uses the <strong>Database</strong> and <strong>Table preset</strong> above. Creates each schema if missing, then adds <code>tables_per_schema</code> Faker tables per schema (names like <code>phrase_company_sales_ab12</code>, capped at 63 chars). Duplicates in the list are ignored. Server caps: 8 schemas, 4 tables per schema.</p>
  <label for="fsMultiSchemas">Schema names (one per line)</label>
  <textarea id="fsMultiSchemas" class="input-lg" spellcheck="false">sales
inventory</textarea>
  <div class="faker-row" style="margin-top:0.5rem">
    <label>Tables per schema<br/><input type="number" id="fsMultiTps" min="1" max="4" value="1"/></label>
    <button type="button" class="secondary" id="fsMultiApply">Create multi-schema tables</button>
  </div>

  <hr class="sep"/>
  <h2 class="sec">Clone / export schema (DDL for another cluster)</h2>
  <p class="note">Uses the <strong>hub admin</strong> connection to read <code>pg_catalog</code> and emit a SQL script: schema, sequences, tables (dependency order when possible), constraints, optional views/MVs/functions/procedures. Copy and run on a target cluster after reviewing extensions, roles, and cross-schema FKs. Empty table list = whole schema; tables-only export skips views/MVs (they often reference other tables).</p>
  <label for="fsCloneSch">Schema to export</label>
  <input type="text" class="tbl-name" id="fsCloneSch" value="public" maxlength="63" autocomplete="off"/>
  <label for="fsCloneTbls">Tables (optional — comma or newline separated; leave empty for all base tables)</label>
  <textarea id="fsCloneTbls" class="input-lg" spellcheck="false" style="min-height:4rem" placeholder="e.g. parents, line_items"></textarea>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneSeq" checked/> Sequences</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneTbl" checked/> Tables</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneVw" checked/> Views</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneMv" checked/> Materialized views</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneFn" checked/> Functions</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneProc" checked/> Procedures</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCloneGrants"/> Table grants (demo-specific roles)</label></div>
  <div class="faker-row" style="margin-top:0.5rem">
    <button type="button" class="secondary" id="fsCloneExport">Generate clone SQL</button>
    <button type="button" class="secondary" id="fsCloneCopy">Copy SQL to clipboard</button>
  </div>
  <label for="fsCloneSqlOut">Generated SQL</label>
  <textarea id="fsCloneSqlOut" class="input-lg" spellcheck="false" readonly style="min-height:14rem;opacity:0.95" placeholder="Export appears here…"></textarea>

  <hr class="sep"/>
  <h2 class="sec">Custom table — column types + PK / UNIQUE / FK</h2>
  <p class="note">Uses the same whitelisted types as logical replication: <code>TEXT</code>, <code>INTEGER</code>, <code>BIGINT</code>, <code>BOOLEAN</code>, <code>NUMERIC</code>, <code>DOUBLE PRECISION</code>, <code>TIMESTAMPTZ</code>.
    <strong>PK</strong> defaults to <code>BIGSERIAL id</code> when enabled. <strong>FK</strong> targets must already exist in the chosen database (create parent table first). Default schema is <code>public</code>; optional <code>ref_schema</code> on each FK (defaults to <code>public</code>).</p>
  <label for="fsCustSch">Schema</label>
  <input type="text" class="tbl-name" id="fsCustSch" value="public" maxlength="63" autocomplete="off"/>
  <label for="fsCustTbl">Table name</label>
  <input type="text" class="tbl-name" id="fsCustTbl" placeholder="e.g. line_items" maxlength="63" autocomplete="off"/>
  <div class="chk-row"><label><input type="checkbox" id="fsCustPk" checked/> <code>BIGSERIAL</code> surrogate primary key (<code>id</code>)</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsCustRif"/> <code>REPLICA IDENTITY FULL</code></label></div>
  <label for="fsCustCols">Columns JSON</label>
  <textarea id="fsCustCols" class="input-lg" spellcheck="false">[
  {{"name": "sku", "pg_type": "TEXT", "not_null": true}},
  {{"name": "qty", "pg_type": "INTEGER", "not_null": false}},
  {{"name": "parent_id", "pg_type": "BIGINT", "not_null": false}}
]</textarea>
  <label for="fsCustUq"><code>UNIQUE</code> constraints (JSON array of column lists)</label>
  <textarea id="fsCustUq" class="input-lg" spellcheck="false">[
  ["sku"]
]</textarea>
  <label for="fsCustFk"><code>FOREIGN KEY</code> definitions (JSON array)</label>
  <textarea id="fsCustFk" class="input-lg" spellcheck="false">[]</textarea>
  <p class="note" style="margin-top:0.25rem">FK example (parent <code>parents</code> with <code>id</code> in <code>public</code>): <code>[{{"columns":["parent_id"],"ref_schema":"public","ref_table":"parents","ref_columns":["id"]}}]</code></p>
  <button type="button" class="secondary" id="fsCustApply">Create custom table</button>

  <hr class="sep"/>
  <h2 class="sec">Faker inserts (demo role)</h2>
  <p class="note"><strong>Manual columns:</strong> Columns JSON drives value shapes; omit <code>id</code> when using <code>BIGSERIAL</code>.
    <strong>Catalog mode:</strong> the hub reads the table from Postgres, discovers outgoing foreign keys, pulls matching keys from referenced tables (existing rows or auto-seeded parents), and Faker-fills only the remaining writable columns.</p>
  <div class="chk-row"><label><input type="checkbox" id="fsInsCatalog"/> Use live catalog (<code>information_schema</code>) — FK-aware; Columns JSON ignored</label></div>
  <div class="chk-row"><label><input type="checkbox" id="fsInsEnsureParents" checked/> In catalog mode: insert minimal rows into empty referenced tables (recursive)</label></div>
  <button type="button" class="secondary" id="fsInsPreview">Preview columns + FKs</button>
  <label for="fsInsSch">Schema</label>
  <input type="text" class="tbl-name" id="fsInsSch" value="public" maxlength="63" autocomplete="off"/>
  <label for="fsInsTbl">Table name</label>
  <input type="text" class="tbl-name" id="fsInsTbl" placeholder="same as custom table" maxlength="63" autocomplete="off"/>
  <label for="fsInsCols">Columns JSON (for Faker value shapes)</label>
  <textarea id="fsInsCols" class="input-lg" spellcheck="false">[
  {{"name": "sku", "pg_type": "TEXT", "not_null": true}},
  {{"name": "qty", "pg_type": "INTEGER", "not_null": false}}
]</textarea>
  <div class="faker-row" style="margin-top:0.5rem">
    <label># rows<br/><input type="number" id="fsInsRows" min="1" max="80" value="8"/></label>
    <label>Seed<br/><input type="number" id="fsInsSeed" placeholder="random"/></label>
    <button type="button" class="secondary" id="fsFakerIns">Insert Faker rows</button>
  </div>

  <p id="st"></p>
  <pre id="fsOut">{{}}</pre>
  <script>
    const st = document.getElementById("st");
    const fsOut = document.getElementById("fsOut");
    async function j(method, url, body) {{
      const opt = {{ method, headers: {{}} }};
      if (body !== undefined) {{
        opt.headers["Content-Type"] = "application/json";
        opt.body = JSON.stringify(body);
      }}
      const r = await fetch(url, opt);
      const data = await r.json().catch(() => ({{ detail: r.statusText }}));
      return {{ r, data }};
    }}
    document.getElementById("fsApply").addEventListener("click", async () => {{
      const body = {{
        database: document.getElementById("fsDb").value,
        preset: document.getElementById("fsPreset").value,
        tables: Number(document.getElementById("fsTables").value),
        sequences: Number(document.getElementById("fsSeq").value),
        views: Number(document.getElementById("fsViews").value),
        materialized_views: Number(document.getElementById("fsMv").value),
        functions: Number(document.getElementById("fsFn").value),
        procedures: Number(document.getElementById("fsProc").value),
      }};
      const rawSeed = document.getElementById("fsSeed").value.trim();
      if (rawSeed !== "") {{
        const n = Number(rawSeed);
        if (!Number.isFinite(n)) {{
          st.textContent = "Seed must be empty or a number.";
          st.className = "err";
          return;
        }}
        body.seed = Math.trunc(n);
      }}
      st.textContent = "Applying…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres/faker-schema/apply", body);
      fsOut.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Done." : "Failed — see JSON.";
      st.className = ok ? "ok" : "err";
    }});
    document.getElementById("fsMultiApply").addEventListener("click", async () => {{
      const raw = document.getElementById("fsMultiSchemas").value.trim();
      const schemas = raw.split(/\\r?\\n/).map((s) => s.trim()).filter(Boolean);
      if (schemas.length === 0) {{
        st.textContent = "Enter at least one schema name (one per line).";
        st.className = "err";
        return;
      }}
      if (schemas.length > 8) {{
        st.textContent = "At most 8 schema names.";
        st.className = "err";
        return;
      }}
      const body = {{
        database: document.getElementById("fsDb").value,
        preset: document.getElementById("fsPreset").value,
        schemas,
        tables_per_schema: Number(document.getElementById("fsMultiTps").value),
      }};
      if (!fsAppendSeed(body, document.getElementById("fsSeed"))) return;
      st.textContent = "Creating multi-schema tables…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres/faker-schema/multi-schema-tables", body);
      fsOut.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Multi-schema tables OK." : "Multi-schema failed — see JSON.";
      st.className = ok ? "ok" : "err";
    }});
    function fsParseTableNames(raw) {{
      const parts = String(raw || "").split(/[\\n,]+/).map((s) => s.trim()).filter(Boolean);
      return parts.length ? parts : null;
    }}
    document.getElementById("fsCloneExport").addEventListener("click", async () => {{
      const schema_name = document.getElementById("fsCloneSch").value.trim() || "public";
      const table_names = fsParseTableNames(document.getElementById("fsCloneTbls").value);
      const body = {{
        database: document.getElementById("fsDb").value,
        schema_name,
        table_names,
        include_sequences: document.getElementById("fsCloneSeq").checked,
        include_tables: document.getElementById("fsCloneTbl").checked,
        include_views: document.getElementById("fsCloneVw").checked,
        include_materialized_views: document.getElementById("fsCloneMv").checked,
        include_functions: document.getElementById("fsCloneFn").checked,
        include_procedures: document.getElementById("fsCloneProc").checked,
        include_grants: document.getElementById("fsCloneGrants").checked,
      }};
      st.textContent = "Generating clone SQL…";
      st.className = "";
      const sqlEl = document.getElementById("fsCloneSqlOut");
      const {{ r, data }} = await j("POST", "/api/postgres/faker-schema/clone-export", body);
      const ok = r.ok && data && data.ok === true && data.sql;
      if (ok) {{
        sqlEl.value = data.sql;
        const summary = {{ ...data }};
        delete summary.sql;
        fsOut.textContent = JSON.stringify(summary, null, 2);
        st.textContent = "Clone SQL ready — large script is in the textarea; metadata below.";
        st.className = "ok";
      }} else {{
        sqlEl.value = "";
        fsOut.textContent = JSON.stringify(data, null, 2);
        st.textContent = "Clone export failed — see JSON.";
        st.className = "err";
      }}
    }});
    document.getElementById("fsCloneCopy").addEventListener("click", async () => {{
      const t = document.getElementById("fsCloneSqlOut").value;
      if (!t) {{
        st.textContent = "Generate SQL first.";
        st.className = "err";
        return;
      }}
      try {{
        await navigator.clipboard.writeText(t);
        st.textContent = "SQL copied to clipboard.";
        st.className = "ok";
      }} catch (e) {{
        st.textContent = "Clipboard unavailable — select the SQL textarea manually.";
        st.className = "err";
      }}
    }});
    function fsAppendSeed(body, el) {{
      const raw = el.value.trim();
      if (raw === "") return true;
      const n = Number(raw);
      if (!Number.isFinite(n)) {{
        st.textContent = "Seed must be empty or a number.";
        st.className = "err";
        return false;
      }}
      body.seed = Math.trunc(n);
      return true;
    }}
    document.getElementById("fsCustApply").addEventListener("click", async () => {{
      const table_name = document.getElementById("fsCustTbl").value.trim();
      let columns, unique_constraints, foreign_keys;
      try {{
        columns = JSON.parse(document.getElementById("fsCustCols").value);
        unique_constraints = JSON.parse(document.getElementById("fsCustUq").value);
        foreign_keys = JSON.parse(document.getElementById("fsCustFk").value);
      }} catch (e) {{
        st.textContent = "Custom table JSON parse error: " + e;
        st.className = "err";
        return;
      }}
      if (!Array.isArray(columns)) {{
        st.textContent = "Columns JSON must be an array.";
        st.className = "err";
        return;
      }}
      if (!Array.isArray(unique_constraints)) {{
        st.textContent = "UNIQUE constraints must be a JSON array.";
        st.className = "err";
        return;
      }}
      if (!Array.isArray(foreign_keys)) {{
        st.textContent = "FK definitions must be a JSON array.";
        st.className = "err";
        return;
      }}
      const body = {{
        database: document.getElementById("fsDb").value,
        schema_name: (document.getElementById("fsCustSch").value.trim() || "public"),
        table_name,
        columns,
        include_bigserial_pk: document.getElementById("fsCustPk").checked,
        replica_identity_full: document.getElementById("fsCustRif").checked,
        unique_constraints,
        foreign_keys,
      }};
      st.textContent = "Creating custom table…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres/faker-schema/custom-table", body);
      fsOut.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Custom table OK." : "Custom table failed — see JSON.";
      st.className = ok ? "ok" : "err";
      if (ok && table_name) {{
        document.getElementById("fsInsTbl").value = table_name;
        document.getElementById("fsInsSch").value = (document.getElementById("fsCustSch").value.trim() || "public");
        document.getElementById("fsInsCols").value = document.getElementById("fsCustCols").value;
      }}
    }});
    document.getElementById("fsInsPreview").addEventListener("click", async () => {{
      const table_name = document.getElementById("fsInsTbl").value.trim();
      if (!table_name) {{
        st.textContent = "Table name required for catalog preview.";
        st.className = "err";
        return;
      }}
      const body = {{
        database: document.getElementById("fsDb").value,
        schema_name: (document.getElementById("fsInsSch").value.trim() || "public"),
        table_name,
      }};
      st.textContent = "Loading catalog…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres/faker-schema/table-catalog", body);
      fsOut.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Catalog loaded." : "Catalog preview failed — see JSON.";
      st.className = ok ? "ok" : "err";
    }});
    document.getElementById("fsFakerIns").addEventListener("click", async () => {{
      const table_name = document.getElementById("fsInsTbl").value.trim();
      const from_catalog = document.getElementById("fsInsCatalog").checked;
      let columns = [];
      if (!from_catalog) {{
        try {{
          columns = JSON.parse(document.getElementById("fsInsCols").value);
        }} catch (e) {{
          st.textContent = "Insert columns JSON parse error: " + e;
          st.className = "err";
          return;
        }}
        if (!Array.isArray(columns) || columns.length === 0) {{
          st.textContent = "Insert columns must be a non-empty array (or enable catalog mode).";
          st.className = "err";
          return;
        }}
      }}
      const body = {{
        database: document.getElementById("fsDb").value,
        schema_name: (document.getElementById("fsInsSch").value.trim() || "public"),
        table_name,
        row_count: Number(document.getElementById("fsInsRows").value),
        from_catalog,
        ensure_parent_rows: document.getElementById("fsInsEnsureParents").checked,
      }};
      if (!from_catalog) body.columns = columns;
      if (!fsAppendSeed(body, document.getElementById("fsInsSeed"))) return;
      st.textContent = "Inserting Faker rows…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres/faker-schema/faker-insert", body);
      fsOut.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Faker insert OK." : "Faker insert failed — see JSON.";
      st.className = ok ? "ok" : "err";
    }});
  </script>
</body>
</html>
"""


POSTGRES_LOGICAL_PAGE = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Postgres logical replication — hub demo</title>
  <style>
    :root {{ font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ max-width: 46rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.55; }}
    a {{ color: #6cb5f4; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; }}
    p.note {{ color: #8899a6; font-size: 0.9rem; }}
    label {{ display: block; margin: 0.75rem 0 0.25rem; font-size: 0.88rem; color: #8899a6; }}
    input[type="text"] {{
      width: 100%; max-width: 28rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea; box-sizing: border-box;
    }}
    button {{
      background: #1d9bf0; color: #fff; border: 0; border-radius: 9999px;
      padding: 0.55rem 1.15rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin: 0.35rem 0.5rem 0 0;
    }}
    button.secondary {{ background: #38444d; }}
    button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
    pre {{
      background: #16181c; border: 1px solid #2f3336; border-radius: 8px;
      padding: 1rem; overflow: auto; font-size: 0.78rem; max-height: 28rem; white-space: pre-wrap;
    }}
    .ok {{ color: #7af87a; }} .err {{ color: #f66; }}
    #st {{ margin-top: 0.75rem; font-size: 0.9rem; }}
    h2.sec {{ font-size: 1.05rem; font-weight: 600; margin: 1.5rem 0 0.35rem; }}
    hr.sep {{ border: 0; border-top: 1px solid #2f3336; margin: 1.5rem 0; }}
    textarea.input-lg {{
      width: 100%; max-width: 36rem; min-height: 7rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
      font-family: ui-monospace, monospace; font-size: 0.78rem; box-sizing: border-box;
    }}
    select {{
      display: block; max-width: 28rem; padding: 0.45rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea;
    }}
    .hint-toolbar {{ display: flex; flex-wrap: wrap; gap: 0.35rem; align-items: center; margin: 0.35rem 0 0.5rem; }}
    button.info-icon {{
      cursor: help; border: 1px solid #38444d; background: #16181c; color: #6cb5f4;
      border-radius: 9999px; width: 1.65rem; height: 1.65rem; padding: 0; font-size: 0.95rem;
      line-height: 1; margin: 0 0.15rem 0 0; font-weight: 600;
    }}
    .hint-pop {{
      display: none; font-size: 0.82rem; color: #c8cdd2; margin: 0 0 0.85rem;
      padding: 0.55rem 0.65rem; border-left: 3px solid #38444d; background: #16181c; border-radius: 0 6px 6px 0;
    }}
    .hint-pop.open {{ display: block; }}
    .chk-row {{ margin: 0.5rem 0; font-size: 0.88rem; color: #e7e9ea; }}
    .chk-row input {{ margin-right: 0.35rem; vertical-align: middle; }}
    .faker-row {{
      display: flex; flex-wrap: wrap; gap: 0.65rem 1rem; align-items: flex-end;
      margin: 0.25rem 0 0.65rem;
    }}
    .faker-row label {{ margin: 0; font-size: 0.82rem; color: #8899a6; }}
    .faker-row input[type="number"] {{
      width: 5.25rem; padding: 0.35rem 0.4rem; border-radius: 6px;
      border: 1px solid #38444d; background: #16181c; color: #e7e9ea; box-sizing: border-box;
    }}
  </style>
</head>
<body>
  {NAV}
  {POSTGRES_BAR}
  <h1>PostgreSQL logical replication</h1>
  <p class="note"><strong>Publisher</strong> database <code>demo_logical_pub</code> lives on <code>postgresql-primary</code>;
    <strong>subscriber</strong> database <code>demo_logical_sub</code> on <code>postgres-sub</code> (logical subscription pulls from the primary).
    <code>postgresql-replica-*</code> are physical standbys of the primary only (they replicate <code>demo_logical_pub</code>, not <code>demo_logical_sub</code>).
    Optional <code>POSTGRES_REPLICA_READ_DSN</code> should target database <code>demo_logical_pub</code> on a replica to compare publisher row counts vs the primary.</p>
  <p class="note" style="margin-top:0.5rem"><strong>Setup can take 1–4 minutes</strong> — Postgres may wait for open transactions before creating the replication slot. If the button stays busy longer than that, it&apos;s waiting on the server; we abort the HTTP request after 4 minutes so you get an error instead of a frozen UI (check primary logs).</p>
  <div>
    <button type="button" id="setup">Run setup (wait for completion)</button>
    <button type="button" class="secondary" id="statusBtn">Refresh status</button>
  </div>
  <label>Note for next insert</label>
  <input type="text" id="note" placeholder="optional label" maxlength="4000"/>
  <div style="margin-top:0.75rem">
    <button type="button" id="insert">Insert row (publisher)</button>
  </div>
  <hr class="sep"/>
  <h2 class="sec">Custom table</h2>
  <p class="note">Extra tables with whitelisted types on publisher and/or subscriber. Hints link to the current PostgreSQL manual (paraphrased summaries).</p>
  <div class="hint-toolbar">
    <span style="font-size:0.82rem;color:#8899a6;">Hints (PostgreSQL docs):</span>
    <button type="button" class="info-icon" title="Logical replication" aria-label="Logical replication hint" onclick="toggleHint('logical_replication')">ℹ</button>
    <button type="button" class="info-icon" title="CREATE TABLE" aria-label="CREATE TABLE hint" onclick="toggleHint('create_table')">ℹ</button>
    <button type="button" class="info-icon" title="CREATE PUBLICATION" aria-label="CREATE PUBLICATION hint" onclick="toggleHint('create_publication')">ℹ</button>
    <button type="button" class="info-icon" title="ALTER SUBSCRIPTION" aria-label="ALTER SUBSCRIPTION hint" onclick="toggleHint('alter_subscription')">ℹ</button>
    <button type="button" class="info-icon" title="Replica identity" aria-label="Replica identity hint" onclick="toggleHint('replica_identity')">ℹ</button>
  </div>
  <div id="hint-slot-logical_replication" class="hint-pop"></div>
  <div id="hint-slot-create_table" class="hint-pop"></div>
  <div id="hint-slot-create_publication" class="hint-pop"></div>
  <div id="hint-slot-alter_subscription" class="hint-pop"></div>
  <div id="hint-slot-replica_identity" class="hint-pop"></div>
  <label for="customTbl">Table name</label>
  <input type="text" id="customTbl" placeholder="e.g. zoo_counts" maxlength="63" autocomplete="off"/>
  <label for="customTarget">Apply <code>CREATE TABLE</code> on</label>
  <select id="customTarget">
    <option value="publisher">Publisher only (<code>demo_logical_pub</code>)</option>
    <option value="subscriber">Subscriber only (<code>demo_logical_sub</code>)</option>
    <option value="both">Both</option>
  </select>
  <div class="chk-row"><label><input type="checkbox" id="customPk" checked/> Add <code>BIGSERIAL</code> primary key column <code>id</code></label></div>
  <div class="chk-row"><label><input type="checkbox" id="customRif" checked/> <code>REPLICA IDENTITY FULL</code></label></div>
  <label for="customCols">Columns JSON</label>
  <div class="faker-row">
    <label>Preset<br/>
      <select id="fakerPreset" aria-label="Faker preset for columns">
        <option value="mixed">mixed</option>
        <option value="people">people</option>
        <option value="commerce">commerce</option>
      </select>
    </label>
    <label># columns<br/><input type="number" id="fakerColCount" min="1" max="16" value="5"/></label>
    <label>Seed<br/><input type="number" id="fakerSeedCols" placeholder="random" aria-label="Optional RNG seed"/></label>
    <button type="button" class="secondary" id="fakerFillCols">Fill columns (Faker)</button>
  </div>
  <textarea id="customCols" class="input-lg" spellcheck="false">[
  {{"name": "species", "pg_type": "TEXT", "not_null": true}},
  {{"name": "cnt", "pg_type": "INTEGER", "not_null": false}}
]</textarea>
  <div style="margin-top:0.55rem">
    <button type="button" class="secondary" id="customCreate">Create table (IF NOT EXISTS)</button>
  </div>
  <hr class="sep"/>
  <h2 class="sec">Custom insert (publisher)</h2>
  <p class="note">Runs as <code>demo</code> on <code>demo_logical_pub</code>. Keys must match table columns (omit <code>id</code> to let <code>BIGSERIAL</code> default).</p>
  <label for="customInsTbl">Table name</label>
  <input type="text" id="customInsTbl" placeholder="same as table above" maxlength="63" autocomplete="off"/>
  <label for="customRows">Rows JSON</label>
  <div class="faker-row">
    <label># rows<br/><input type="number" id="fakerRowCount" min="1" max="80" value="6"/></label>
    <label>Seed<br/><input type="number" id="fakerSeedRows" placeholder="random" aria-label="Optional RNG seed for rows"/></label>
    <button type="button" class="secondary" id="fakerFillRows">Fill rows (Faker)</button>
  </div>
  <p class="note" style="margin-bottom:0.35rem">Row filler uses <strong>Columns JSON</strong> above; omit <code>id</code> when the table uses the generated <code>BIGSERIAL</code> PK.</p>
  <textarea id="customRows" class="input-lg" spellcheck="false">[
  {{"species": "cat", "cnt": 1}},
  {{"species": "dog", "cnt": 2}}
]</textarea>
  <div style="margin-top:0.55rem">
    <button type="button" class="secondary" id="customInsert">Insert rows</button>
  </div>
  <p id="st"></p>
  <pre id="out">Click Run setup once, then Insert or Refresh status.</pre>
  <script>
    const out = document.getElementById("out");
    const st = document.getElementById("st");
    function escapeHtml(s) {{
      return String(s)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }}
    function toggleHint(key) {{
      const el = document.getElementById("hint-slot-" + key);
      if (!el) return;
      const wasOpen = el.classList.contains("open");
      document.querySelectorAll(".hint-pop").forEach((e) => e.classList.remove("open"));
      if (!wasOpen) el.classList.add("open");
    }}
    async function j(method, url, body) {{
      const opt = {{ method, headers: {{}} }};
      if (body !== undefined) {{
        opt.headers["Content-Type"] = "application/json";
        opt.body = JSON.stringify(body);
      }}
      const r = await fetch(url, opt);
      const data = await r.json().catch(() => ({{ detail: r.statusText }}));
      return {{ r, data }};
    }}
    async function loadPgDocs() {{
      const {{ r, data }} = await j("GET", "/api/postgres-logical/docs");
      if (!r.ok || !data) return;
      for (const [k, v] of Object.entries(data)) {{
        const slot = document.getElementById("hint-slot-" + k);
        if (!slot || !v || !v.hint) continue;
        slot.innerHTML =
          "<p>" +
          escapeHtml(v.hint) +
          '</p><p><a href="' +
          escapeHtml(v.url) +
          '" target="_blank" rel="noopener">' +
          escapeHtml(v.title) +
          " — PostgreSQL Documentation</a></p>";
      }}
    }}
    async function refreshStatus() {{
      st.textContent = "Loading…";
      st.className = "";
      const {{ r, data }} = await j("GET", "/api/postgres-logical/status");
      out.textContent = JSON.stringify(data, null, 2);
      st.textContent = r.ok ? "Status OK." : (data.detail || "Error");
      st.className = r.ok ? "ok" : "err";
    }}
    document.getElementById("setup").addEventListener("click", async () => {{
      const btn = document.getElementById("setup");
      const SETUP_MS = 240000;
      btn.disabled = true;
      st.textContent = "Running setup… (often 30s–3min; Postgres may wait on snapshots)";
      st.className = "";
      const ac = new AbortController();
      const kill = setTimeout(() => ac.abort(), SETUP_MS);
      try {{
        const r = await fetch("/api/postgres-logical/setup", {{
          method: "POST",
          signal: ac.signal,
        }});
        let data;
        try {{
          data = await r.json();
        }} catch (e) {{
          data = {{ detail: await r.text().catch(() => r.statusText), parse_error: String(e) }};
        }}
        out.textContent = JSON.stringify(data, null, 2);
        const ok = r.ok && data && data.ok;
        st.textContent = ok ? "Setup finished." : "Setup reported issues — see JSON.";
        st.className = ok ? "ok" : "err";
      }} catch (e) {{
        const aborted = e && (e.name === "AbortError" || e.message === "signal is aborted without reason");
        const msg = aborted
          ? "Stopped waiting after " + (SETUP_MS / 1000) + "s — Postgres is still running CREATE SUBSCRIPTION (often blocked by open transactions). Check primary logs / terminate old sessions, then retry."
          : String(e);
        st.textContent = msg;
        st.className = "err";
        out.textContent = JSON.stringify({{
          ok: false,
          client_error: msg,
          hint: "CREATE SUBSCRIPTION blocks until logical decoding can take a snapshot; idle-in-transaction backends delay this.",
        }}, null, 2);
      }} finally {{
        clearTimeout(kill);
        btn.disabled = false;
      }}
    }});
    document.getElementById("insert").addEventListener("click", async () => {{
      const note = document.getElementById("note").value.trim();
      st.textContent = "Inserting…";
      const {{ r, data }} = await j("POST", "/api/postgres-logical/insert", {{ note }});
      out.textContent = JSON.stringify(data, null, 2);
      const insertOk = data && data.ok === true;
      st.textContent = insertOk ? "Insert OK." : "Insert failed — see JSON.";
      st.className = insertOk ? "ok" : "err";
    }});
    function appendOptionalSeed(body, inputEl) {{
      const raw = inputEl.value.trim();
      if (raw === "") return true;
      const n = Number(raw);
      if (!Number.isFinite(n)) {{
        st.textContent = "Seed must be empty or a valid number.";
        st.className = "err";
        return false;
      }}
      body.seed = Math.trunc(n);
      return true;
    }}
    document.getElementById("fakerFillCols").addEventListener("click", async () => {{
      const column_count = Number(document.getElementById("fakerColCount").value);
      const body = {{
        preset: document.getElementById("fakerPreset").value,
        column_count: Number.isFinite(column_count) ? column_count : 5,
      }};
      if (!appendOptionalSeed(body, document.getElementById("fakerSeedCols"))) return;
      st.textContent = "Generating columns…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres-logical/faker-columns", body);
      out.textContent = JSON.stringify(data, null, 2);
      if (r.ok && data.columns) {{
        document.getElementById("customCols").value = JSON.stringify(data.columns, null, 2);
        const tn = document.getElementById("customTbl").value.trim();
        if (!tn && data.table_name_suggestion) {{
          document.getElementById("customTbl").value = data.table_name_suggestion;
        }}
        st.textContent = "Faker columns filled.";
        st.className = "ok";
      }} else {{
        st.textContent = "Faker columns failed — see JSON.";
        st.className = "err";
      }}
    }});
    document.getElementById("fakerFillRows").addEventListener("click", async () => {{
      let columns;
      try {{
        columns = JSON.parse(document.getElementById("customCols").value);
      }} catch (e) {{
        st.textContent = "Fix Columns JSON before generating rows: " + e;
        st.className = "err";
        return;
      }}
      if (!Array.isArray(columns) || columns.length === 0) {{
        st.textContent = "Columns JSON must be a non-empty array.";
        st.className = "err";
        return;
      }}
      const row_count = Number(document.getElementById("fakerRowCount").value);
      const body = {{
        columns,
        row_count: Number.isFinite(row_count) ? row_count : 6,
      }};
      if (!appendOptionalSeed(body, document.getElementById("fakerSeedRows"))) return;
      st.textContent = "Generating rows…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres-logical/faker-rows", body);
      out.textContent = JSON.stringify(data, null, 2);
      if (r.ok && data.rows) {{
        document.getElementById("customRows").value = JSON.stringify(data.rows, null, 2);
        const ins = document.getElementById("customInsTbl").value.trim();
        const tbl = document.getElementById("customTbl").value.trim();
        if (!ins && tbl) document.getElementById("customInsTbl").value = tbl;
        st.textContent = "Faker rows filled.";
        st.className = "ok";
      }} else {{
        st.textContent = "Faker rows failed — see JSON.";
        st.className = "err";
      }}
    }});
    document.getElementById("customCreate").addEventListener("click", async () => {{
      const table_name = document.getElementById("customTbl").value.trim();
      let columns;
      try {{
        columns = JSON.parse(document.getElementById("customCols").value);
      }} catch (e) {{
        st.textContent = "Columns JSON parse error: " + e;
        st.className = "err";
        return;
      }}
      if (!Array.isArray(columns)) {{
        st.textContent = "Columns JSON must be an array.";
        st.className = "err";
        return;
      }}
      const body = {{
        table_name,
        columns,
        target: document.getElementById("customTarget").value,
        include_bigserial_pk: document.getElementById("customPk").checked,
        replica_identity_full: document.getElementById("customRif").checked,
      }};
      st.textContent = "Creating table…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres-logical/custom-table", body);
      out.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Custom table OK." : "Custom table failed — see JSON.";
      st.className = ok ? "ok" : "err";
    }});
    document.getElementById("customInsert").addEventListener("click", async () => {{
      const table_name = document.getElementById("customInsTbl").value.trim();
      let rows;
      try {{
        rows = JSON.parse(document.getElementById("customRows").value);
      }} catch (e) {{
        st.textContent = "Rows JSON parse error: " + e;
        st.className = "err";
        return;
      }}
      if (!Array.isArray(rows)) {{
        st.textContent = "Rows JSON must be an array.";
        st.className = "err";
        return;
      }}
      st.textContent = "Inserting…";
      st.className = "";
      const {{ r, data }} = await j("POST", "/api/postgres-logical/custom-insert", {{ table_name, rows }});
      out.textContent = JSON.stringify(data, null, 2);
      const ok = r.ok && data && data.ok === true;
      st.textContent = ok ? "Custom insert OK." : "Custom insert failed — see JSON.";
      st.className = ok ? "ok" : "err";
    }});
    document.getElementById("statusBtn").addEventListener("click", refreshStatus);
    loadPgDocs();
    refreshStatus();
  </script>
</body>
</html>
"""


def _pg_password_from_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    return unquote(p.password) if p.password else ""


def _normalize_hub_pg_identifier(v: object) -> object:
    """Lowercase + strip so camelCase UI input matches PostgreSQL unquoted rules."""
    if isinstance(v, str):
        return v.strip().lower()
    return v


class TrinoQueryBody(BaseModel):
    sql: str = Field(..., min_length=1, max_length=96_000)
    catalog: str | None = Field(None, max_length=64)
    schema: str | None = Field(None, max_length=128)


class KafkaLabProduceBody(BaseModel):
    topic: str = Field(default="", max_length=249)
    count: int = Field(default=100, ge=1, le=20_000)
    key_mode: Literal["none", "fixed", "random", "per_message"] = "per_message"
    fixed_key: str = Field(default="lab", max_length=200)
    acks: Literal["0", "1", "all"] = "1"
    linger_ms: int = Field(default=5, ge=0, le=30_000)
    batch_size: int = Field(default=16384, ge=1024, le=5_000_000)
    compression: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "none"
    value_pad_kb: int = Field(default=0, ge=0, le=512)
    enable_idempotence: bool = False

    @model_validator(mode="after")
    def _normalize_topic(self) -> Self:
        t = self.topic.strip() or kafka_lab.DEFAULT_LAB_TOPIC
        object.__setattr__(self, "topic", kafka_lab.validate_topic(t))
        return self


class KafkaLabConsumeBody(BaseModel):
    topic: str = Field(default="", max_length=249)
    group_id: str = Field(default="", max_length=240)
    max_messages: int = Field(default=20, ge=1, le=500)
    timeout_ms: int = Field(default=15_000, ge=500, le=120_000)
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"

    @model_validator(mode="after")
    def _normalize_topic_consume(self) -> Self:
        t = self.topic.strip() or kafka_lab.DEFAULT_LAB_TOPIC
        object.__setattr__(self, "topic", kafka_lab.validate_topic(t))
        return self


class PostgresLogicalInsertBody(BaseModel):
    note: str = ""


class PostgresLogicalCustomColumn(BaseModel):
    name: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")
    pg_type: Literal[
        "TEXT",
        "INTEGER",
        "BIGINT",
        "BOOLEAN",
        "NUMERIC",
        "DOUBLE PRECISION",
        "TIMESTAMPTZ",
    ]
    not_null: bool = False

    @field_validator("name", mode="before")
    @classmethod
    def normalize_column_name(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)


class PostgresLogicalCustomTableBody(BaseModel):
    table_name: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")
    columns: list[PostgresLogicalCustomColumn] = Field(default_factory=list, max_length=16)
    target: Literal["publisher", "subscriber", "both"] = "publisher"
    include_bigserial_pk: bool = True
    replica_identity_full: bool = True

    @field_validator("table_name", mode="before")
    @classmethod
    def normalize_table_name(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @model_validator(mode="after")
    def pk_or_columns_and_no_id_conflict(self) -> "PostgresLogicalCustomTableBody":
        if not self.include_bigserial_pk and not self.columns:
            raise ValueError(
                "either include_bigserial_pk or at least one column is required"
            )
        if self.include_bigserial_pk:
            for c in self.columns:
                if c.name == "id":
                    raise ValueError(
                        'column name "id" conflicts with include_bigserial_pk; '
                        "rename the column or disable the generated PK"
                    )
        return self


class PostgresLogicalCustomInsertBody(BaseModel):
    table_name: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")
    rows: list[dict[str, Any]] = Field(..., min_length=1, max_length=80)

    @field_validator("table_name", mode="before")
    @classmethod
    def normalize_table_name(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)


class PostgresLogicalFakerColumnsBody(BaseModel):
    preset: Literal["mixed", "people", "commerce"] = "mixed"
    column_count: int = Field(5, ge=1, le=16)
    seed: int | None = Field(None, ge=0, le=2_147_483_647)


class PostgresLogicalFakerRowsBody(BaseModel):
    columns: list[PostgresLogicalCustomColumn] = Field(..., min_length=1, max_length=16)
    row_count: int = Field(6, ge=1, le=80)
    seed: int | None = Field(None, ge=0, le=2_147_483_647)


class PostgresFakerSchemaBody(BaseModel):
    database: Literal["demo_logical_pub", "demo_logical_sub"] = "demo_logical_pub"
    preset: Literal["mixed", "people", "commerce"] = "mixed"
    seed: int | None = Field(None, ge=0, le=2_147_483_647)
    tables: int = Field(0, ge=0, le=3)
    sequences: int = Field(0, ge=0, le=6)
    views: int = Field(0, ge=0, le=4)
    materialized_views: int = Field(0, ge=0, le=3)
    functions: int = Field(0, ge=0, le=4)
    procedures: int = Field(0, ge=0, le=4)

    @model_validator(mode="after")
    def nonempty_counts(self) -> "PostgresFakerSchemaBody":
        if (
            self.tables
            + self.sequences
            + self.views
            + self.materialized_views
            + self.functions
            + self.procedures
        ) == 0:
            raise ValueError("set at least one object count above zero")
        return self


class PostgresFakerSchemaForeignKeyBody(BaseModel):
    columns: list[str] = Field(..., min_length=1, max_length=8)
    ref_schema: str = Field("public", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    ref_table: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")
    ref_columns: list[str] = Field(..., min_length=1, max_length=8)

    @field_validator("columns", "ref_columns", mode="before")
    @classmethod
    def normalize_key_lists(cls, v: object) -> object:
        if isinstance(v, list):
            return [_normalize_hub_pg_identifier(str(x)) for x in v]
        return v

    @field_validator("ref_schema", mode="before")
    @classmethod
    def normalize_ref_schema(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @field_validator("ref_table", mode="before")
    @classmethod
    def normalize_ref_table(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)


class PostgresFakerSchemaCustomTableBody(BaseModel):
    database: Literal["demo_logical_pub", "demo_logical_sub"]
    schema_name: str = Field("public", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    table_name: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")
    columns: list[PostgresLogicalCustomColumn] = Field(default_factory=list, max_length=16)
    include_bigserial_pk: bool = True
    replica_identity_full: bool = False
    unique_constraints: list[list[str]] = Field(default_factory=list, max_length=8)
    foreign_keys: list[PostgresFakerSchemaForeignKeyBody] = Field(
        default_factory=list, max_length=8
    )

    @field_validator("schema_name", mode="before")
    @classmethod
    def normalize_custom_schema(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @field_validator("table_name", mode="before")
    @classmethod
    def normalize_table_name(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @field_validator("unique_constraints", mode="before")
    @classmethod
    def normalize_unique_constraints(cls, v: object) -> object:
        if not isinstance(v, list):
            return v
        out: list[list[str]] = []
        for group in v:
            if not isinstance(group, list):
                raise ValueError("unique_constraints must be a list of column lists")
            out.append([_normalize_hub_pg_identifier(str(x)) for x in group])
        return out

    @model_validator(mode="after")
    def pk_and_constraint_columns(self) -> "PostgresFakerSchemaCustomTableBody":
        if not self.include_bigserial_pk and not self.columns:
            raise ValueError(
                "either include_bigserial_pk or at least one column is required"
            )
        if self.include_bigserial_pk:
            for c in self.columns:
                if c.name == "id":
                    raise ValueError(
                        'column name "id" conflicts with include_bigserial_pk'
                    )
        names = {c.name for c in self.columns}
        if self.include_bigserial_pk:
            names.add("id")
        for i, uq in enumerate(self.unique_constraints):
            if not uq:
                raise ValueError(f"unique_constraints[{i}] cannot be empty")
            for col in uq:
                if col not in names:
                    raise ValueError(
                        f"unique_constraints[{i}] references unknown column {col!r}"
                    )
        for i, fk in enumerate(self.foreign_keys):
            for col in fk.columns:
                if col not in names:
                    raise ValueError(
                        f"foreign_keys[{i}] references unknown column {col!r}"
                    )
        return self


class PostgresFakerSchemaFakerInsertBody(BaseModel):
    database: Literal["demo_logical_pub", "demo_logical_sub"]
    schema_name: str = Field("public", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    table_name: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")
    columns: list[PostgresLogicalCustomColumn] = Field(default_factory=list, max_length=16)
    row_count: int = Field(8, ge=1, le=80)
    seed: int | None = Field(None, ge=0, le=2_147_483_647)
    from_catalog: bool = False
    ensure_parent_rows: bool = True

    @model_validator(mode="after")
    def columns_required_without_catalog(self) -> "PostgresFakerSchemaFakerInsertBody":
        if self.from_catalog:
            return self
        if len(self.columns) < 1:
            raise ValueError(
                "columns must be a non-empty JSON array unless from_catalog is true"
            )
        return self

    @field_validator("schema_name", mode="before")
    @classmethod
    def normalize_insert_schema(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @field_validator("table_name", mode="before")
    @classmethod
    def normalize_table_name_insert(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)


class PostgresFakerSchemaTableCatalogBody(BaseModel):
    database: Literal["demo_logical_pub", "demo_logical_sub"]
    schema_name: str = Field("public", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    table_name: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,62}$")

    @field_validator("schema_name", mode="before")
    @classmethod
    def normalize_tc_schema(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @field_validator("table_name", mode="before")
    @classmethod
    def normalize_tc_table(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)


class PostgresSchemaCloneExportBody(BaseModel):
    database: Literal["demo_logical_pub", "demo_logical_sub"]
    schema_name: str = Field("public", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    table_names: list[str] | None = Field(default=None, max_length=48)
    include_sequences: bool = True
    include_tables: bool = True
    include_views: bool = True
    include_materialized_views: bool = True
    include_functions: bool = True
    include_procedures: bool = True
    include_grants: bool = False

    @field_validator("schema_name", mode="before")
    @classmethod
    def normalize_clone_schema(cls, v: object) -> object:
        return _normalize_hub_pg_identifier(v)

    @field_validator("table_names", mode="before")
    @classmethod
    def normalize_clone_tables(cls, v: object) -> object:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("table_names must be null or an array of table names")
        out: list[str] = []
        seen: set[str] = set()
        for item in v:
            s = _normalize_hub_pg_identifier(str(item))
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out


class PostgresFakerSchemaMultiSchemaBody(BaseModel):
    database: Literal["demo_logical_pub", "demo_logical_sub"] = "demo_logical_pub"
    preset: Literal["mixed", "people", "commerce"] = "mixed"
    seed: int | None = Field(None, ge=0, le=2_147_483_647)
    schemas: list[str] = Field(..., min_length=1, max_length=8)
    tables_per_schema: int = Field(1, ge=1, le=4)

    @field_validator("schemas", mode="before")
    @classmethod
    def normalize_multi_schemas(cls, v: object) -> object:
        if not isinstance(v, list):
            raise ValueError("schemas must be a non-empty JSON array of strings")
        out: list[str] = []
        seen: set[str] = set()
        for item in v:
            s = _normalize_hub_pg_identifier(str(item))
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        if not out:
            raise ValueError("provide at least one non-empty schema name")
        return out


class WorkloadRequest(BaseModel):
    total_records: int = Field(100, ge=1, le=100_000)
    batch_size: int = Field(50, ge=1, le=2000)
    payload_kb: int = Field(0, ge=0, le=PAYLOAD_KB_MAX)
    targets: list[str] = Field(
        default_factory=lambda: ["postgres", "mongo", "redis", "cassandra", "opensearch"]
    )
    sustain: bool = False
    duration_value: int | None = Field(None, ge=1, le=9)
    duration_unit: Literal["seconds", "minutes", "hours"] | None = None

    @model_validator(mode="after")
    def sustain_needs_duration(self) -> "WorkloadRequest":
        if self.sustain:
            if self.duration_value is None or self.duration_unit is None:
                raise ValueError(
                    "sustain=true requires duration_value (1–9) and duration_unit (seconds|minutes|hours)"
                )
        return self

    @field_validator("targets")
    @classmethod
    def targets_ok(cls, v: list[str]) -> list[str]:
        s = set(v)
        bad = s - ALLOWED_TARGETS
        if bad:
            raise ValueError(f"unknown targets: {bad}")
        if not s:
            raise ValueError("at least one target required")
        return v

    def budget_ok(self) -> None:
        est_mb = (self.total_records * max(0, self.payload_kb)) // 1024
        if est_mb > WORKLOAD_MAX_WAVE_NOMINAL_MB:
            raise ValueError(
                f"total_records × payload_kb too large (~{est_mb} MiB nominal); "
                f"lower counts or payload_kb (max wave {WORKLOAD_MAX_WAVE_NOMINAL_MB} MiB, "
                f"or set WORKLOAD_MAX_WAVE_NOMINAL_MB)"
            )
        if self.sustain and self.duration_value and self.duration_unit:
            dur = _duration_seconds(self.duration_value, self.duration_unit)
            if dur > WORKLOAD_SUSTAIN_MAX_SECONDS:
                raise ValueError(
                    f"sustain duration exceeds max {WORKLOAD_SUSTAIN_MAX_SECONDS}s "
                    f"(set WORKLOAD_SUSTAIN_MAX_SECONDS to raise)"
                )
            # Upper bound on wave count: assume at least ~5s wall per wave on average so
            # we do not multiply est_mb by duration×2 (that rejected realistic sustained runs).
            wave_cap = min(50_000, max(1, int(dur / 5) + 1))
            total_nom = est_mb * wave_cap
            if total_nom > WORKLOAD_SUSTAIN_NOMINAL_CAP_MB:
                raise ValueError(
                    "sustained workload nominal size too large; "
                    "lower total_records, payload_kb, or duration "
                    f"(estimated ~{total_nom} MiB vs cap {WORKLOAD_SUSTAIN_NOMINAL_CAP_MB}, "
                    "set WORKLOAD_SUSTAIN_NOMINAL_CAP_MB to raise)"
                )


def _execute_workload_wave(
    *,
    cfg,
    total_records: int,
    batch_size_req: int,
    targets: set[str],
    run_id: str,
    pad: str,
    now: datetime,
    seq_base: int,
    bs: int,
    c_batches: int,
    cassandra_session,
    cassandra_prep,
    redis_client: redis.Redis | None = None,
) -> tuple[dict[str, int], dict[str, str]]:
    """One pass writing total_records rows per target (indices seq_base .. seq_base+total_records-1)."""
    counts: dict[str, int] = {k: 0 for k in targets}
    errors: dict[str, str] = {}

    if "postgres" in targets:
        try:
            n = 0
            with psycopg.connect(cfg.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    for start in range(0, total_records, bs):
                        chunk = []
                        for j in range(start, min(start + bs, total_records)):
                            i = seq_base + j
                            lim = max(64, POSTGRES_WORKLOAD_NAME_MAX_CHARS)
                            name = (f"wl-{run_id}-{i}|{pad}")[:lim]
                            chunk.append((name,))
                            n += 1
                        cur.executemany("INSERT INTO demo_items (name) VALUES (%s)", chunk)
                conn.commit()
            counts["postgres"] = n
        except Exception as e:
            errors["postgres"] = str(e)

    if "mongo" in targets:
        try:
            m = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=120_000)
            coll = m["demo"]["demo_items"]
            n = 0
            for start in range(0, total_records, bs):
                docs = []
                for j in range(start, min(start + bs, total_records)):
                    i = seq_base + j
                    docs.append(
                        {
                            "name": f"wl-{run_id}-{i}",
                            "run_id": run_id,
                            "seq": i,
                            "source": "hub-workload",
                            "pad": pad,
                            "batch_size": batch_size_req,
                            "created_at": now,
                        }
                    )
                    n += 1
                coll.insert_many(docs, ordered=False)
            counts["mongo"] = n
        except Exception as e:
            errors["mongo"] = str(e)

    if "redis" in targets:
        try:
            own_redis = False
            r = redis_client
            if r is None:
                r = redis.from_url(cfg.redis_url, decode_responses=False)
                own_redis = True
            n = 0
            try:
                for start in range(0, total_records, bs):
                    pipe = r.pipeline(transaction=False)
                    for j in range(start, min(start + bs, total_records)):
                        i = seq_base + j
                        key = f"{cfg.workload_redis_prefix}{run_id}:{i}"
                        payload = json.dumps(
                            {
                                "run_id": run_id,
                                "seq": i,
                                "pad": pad,
                            },
                            separators=(",", ":"),
                        ).encode()
                        pipe.setex(key, 86400, payload)
                        n += 1
                    pipe.execute()
                counts["redis"] = n
            finally:
                if own_redis:
                    r.close()
        except Exception as e:
            errors["redis"] = str(e)

    if "cassandra" in targets:
        try:
            sess = cassandra_session
            cass_label_max = 60000
            n = 0
            sleep_s = max(0.0, CASSANDRA_WORKLOAD_INTER_BATCH_SLEEP_MS / 1000.0)
            retries = max(0, CASSANDRA_WORKLOAD_WRITE_RETRIES)
            for start in range(0, total_records, c_batches):
                batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE)
                for j in range(start, min(start + c_batches, total_records)):
                    i = seq_base + j
                    # Deterministic PK so read-back can SELECT ... WHERE order_id IN (...) (no SASI on label).
                    oid = f"wl-{run_id}-{i}"
                    lab = (f"wl-{run_id}-{i}|{pad}")[:cass_label_max]
                    batch.add(cassandra_prep, (oid, lab, now))
                    n += 1
                attempt = 0
                while True:
                    try:
                        sess.execute(batch, timeout=CASSANDRA_WORKLOAD_REQUEST_TIMEOUT)
                        break
                    except Exception as ex:
                        msg = str(ex).lower()
                        transient = (
                            "1100" in msg
                            or "timed out" in msg
                            or "timeout" in msg
                            or "no hosts available" in msg
                        )
                        if transient and attempt < retries:
                            attempt += 1
                            time.sleep(0.08 * (2 ** (attempt - 1)))
                            continue
                        raise
                if sleep_s > 0.0:
                    time.sleep(sleep_s)
            counts["cassandra"] = n
        except Exception as e:
            errors["cassandra"] = str(e)

    if "opensearch" in targets:
        try:
            n = 0
            os_chunk = _opensearch_bulk_chunk_size(pad, bs)
            with httpx.Client(timeout=120.0) as hc:
                _ensure_hub_opensearch_for_cfg(hc, cfg)
                for start in range(0, total_records, os_chunk):
                    lines: list[str] = []
                    for j in range(start, min(start + os_chunk, total_records)):
                        i = seq_base + j
                        doc_id = f"{run_id}-{i}"
                        lines.append(
                            json.dumps(
                                {
                                    "index": {
                                        "_index": cfg.opensearch_workload_index,
                                        "_id": doc_id,
                                    }
                                }
                            )
                        )
                        lines.append(
                            json.dumps(
                                {
                                    "run_id": run_id,
                                    "seq": i,
                                    "pad": pad,
                                    "created_at": now.isoformat(),
                                }
                            )
                        )
                        n += 1
                    body = "\n".join(lines) + "\n"
                    resp = hc.post(
                        f"{cfg.opensearch_url}/_bulk",
                        content=body.encode(),
                        headers={"Content-Type": "application/x-ndjson"},
                    )
                    resp.raise_for_status()
                    bulk = resp.json()
                    if bulk.get("errors"):
                        raise RuntimeError(
                            "bulk item failure: " + json.dumps(bulk, default=str)[:2000]
                        )
            counts["opensearch"] = n
        except Exception as e:
            errors["opensearch"] = str(e)

    if "mssql" in targets:
        try:
            n, err = scenario.workload_mssql_batch(
                run_id, seq_base, total_records, bs, pad
            )
            if err:
                errors["mssql"] = err
            else:
                counts["mssql"] = n
        except Exception as e:
            errors["mssql"] = str(e)

    return counts, errors


class WorkloadReadRequest(BaseModel):
    run_id: str = Field(..., min_length=1, max_length=32)
    sample_limit: int = Field(10, ge=1, le=WORKLOAD_READ_SAMPLE_LIMIT_MAX)
    targets: list[str] = Field(
        default_factory=lambda: ["postgres", "mongo", "redis", "cassandra", "opensearch"]
    )

    @field_validator("targets")
    @classmethod
    def targets_ok(cls, v: list[str]) -> list[str]:
        s = set(v)
        bad = s - ALLOWED_TARGETS
        if bad:
            raise ValueError(f"unknown targets: {bad}")
        if not s:
            raise ValueError("at least one target required")
        return v


def _workload_read_sample(req: WorkloadReadRequest) -> dict:
    """Fetch a small slice of workload-shaped rows from each selected store."""
    cfg = get_runtime_config()
    out: dict = {"ok": True, "run_id": req.run_id, "samples": {}, "errors": {}}
    lim = req.sample_limit
    tg = set(req.targets)
    like_prefix = f"wl-{req.run_id}-%"

    if "postgres" in tg:
        try:
            with psycopg.connect(cfg.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, name, created_at FROM demo_items WHERE name LIKE %s ORDER BY id DESC LIMIT %s",
                        (like_prefix, lim),
                    )
                    rows = cur.fetchall()
            out["samples"]["postgres"] = {
                "count": len(rows),
                "rows": [
                    {
                        "id": r[0],
                        "name": r[1][:200] + ("…" if r[1] and len(r[1]) > 200 else ""),
                        "created_at": r[2].isoformat() if r[2] else None,
                    }
                    for r in rows
                ],
            }
        except Exception as e:
            out["ok"] = False
            out["errors"]["postgres"] = str(e)

    if "mongo" in tg:
        try:
            m = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=30_000)
            coll = m["demo"]["demo_items"]
            cur = (
                coll.find(
                    {"run_id": req.run_id},
                    projection={"_id": 1, "name": 1, "seq": 1, "run_id": 1, "source": 1},
                )
                .sort("_id", -1)
                .limit(lim)
            )
            docs = []
            for d in cur:
                d["_id"] = str(d["_id"])
                docs.append(d)
            out["samples"]["mongo"] = {"count": len(docs), "documents": docs}
        except Exception as e:
            out["ok"] = False
            out["errors"]["mongo"] = str(e)

    if "redis" in tg:
        try:
            r = redis.from_url(cfg.redis_url, decode_responses=True)
            keys = [f"{cfg.workload_redis_prefix}{req.run_id}:{i}" for i in range(lim)]
            raw = r.mget(keys)
            entries = []
            for k, vzip in zip(keys, raw):
                if vzip is None:
                    entries.append({"key": k, "value": None})
                else:
                    try:
                        entries.append({"key": k, "value": json.loads(vzip)})
                    except json.JSONDecodeError:
                        entries.append({"key": k, "value_raw": vzip[:500]})
            hits = sum(
                1 for e in entries if e.get("value") is not None or "value_raw" in e
            )
            out["samples"]["redis"] = {
                "keys_checked": keys,
                "hits": hits,
                "entries": entries,
            }
        except Exception as e:
            out["ok"] = False
            out["errors"]["redis"] = str(e)

    if "cassandra" in tg:
        try:
            sess, _ = get_hub_cassandra_handles(cfg)
            # Same key convention as workload writes: order_id = wl-{run_id}-{seq} (partition key lookup, no LIKE).
            order_ids = [f"wl-{req.run_id}-{i}" for i in range(lim)]
            # Bind one placeholder per IN value; a single tuple bound to IN %s can yield invalid CQL on some stacks.
            ph = ", ".join(["%s"] * len(order_ids))
            rows = sess.execute(
                f"SELECT order_id, label, created_at FROM {cfg.cassandra_keyspace}.orders "
                f"WHERE order_id IN ({ph})",
                tuple(order_ids),
            )
            cass_rows = []
            for row in rows:
                cass_rows.append(
                    {
                        "order_id": row.order_id,
                        "label": (row.label or "")[:200],
                        "created_at": row.created_at.isoformat() if row.created_at else None,
                    }
                )
            cass_rows.sort(key=lambda r: r["order_id"])
            out["samples"]["cassandra"] = {"count": len(cass_rows), "rows": cass_rows}
        except Exception as e:
            out["ok"] = False
            out["errors"]["cassandra"] = str(e)

    if "opensearch" in tg:
        try:
            query = {
                "query": {"term": {"run_id": req.run_id}},
                "size": lim,
                "sort": [{"seq": "desc"}],
            }
            with httpx.Client(timeout=30.0) as hc:
                _ensure_hub_opensearch_for_cfg(hc, cfg)
                resp = hc.post(
                    f"{cfg.opensearch_url}/{cfg.opensearch_workload_index}/_search",
                    json=query,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                body = resp.json()
            hits = body.get("hits", {}).get("hits", [])
            out["samples"]["opensearch"] = {
                "total": body.get("hits", {}).get("total", {}),
                "hits": [
                    {"_id": h.get("_id"), "_score": h.get("_score"), "source": h.get("_source")}
                    for h in hits
                ],
            }
        except Exception as e:
            out["ok"] = False
            out["errors"]["opensearch"] = str(e)

    if "mssql" in tg:
        try:
            sm = scenario.fetch_workload_sample_mssql(req.run_id, lim)
            if sm.get("ok"):
                out["samples"]["mssql"] = {
                    "count": sm.get("count", 0),
                    "rows": sm.get("rows", []),
                }
            else:
                out["ok"] = False
                out["errors"]["mssql"] = sm.get("error", "unknown")
        except Exception as e:
            out["ok"] = False
            out["errors"]["mssql"] = str(e)

    return out


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(PAGE)


@app.get("/workload", response_class=HTMLResponse)
async def workload_page():
    return HTMLResponse(WORKLOAD_PAGE)


@app.get("/reads", response_class=HTMLResponse)
async def reads_page():
    return HTMLResponse(READS_PAGE)


@app.get("/scenario", response_class=HTMLResponse)
async def scenario_page():
    return HTMLResponse(SCENARIO_PAGE)


@app.get("/kafka", response_class=HTMLResponse)
async def kafka_lab_page():
    return HTMLResponse(KAFKA_PAGE)


@app.get("/connections", response_class=HTMLResponse)
async def connections_page():
    return HTMLResponse(CONNECTIONS_PAGE)


@app.get("/api/kafka/lab/metadata")
async def api_kafka_lab_metadata():
    return await asyncio.to_thread(kafka_lab.metadata)


@app.get("/api/kafka/lab/hints")
async def api_kafka_lab_hints():
    return kafka_lab.describe_snippet()


@app.post("/api/kafka/lab/produce")
async def api_kafka_lab_produce(body: KafkaLabProduceBody):
    def _run() -> dict[str, Any]:
        return kafka_lab.produce_burst(**body.model_dump())

    return await asyncio.to_thread(_run)


@app.post("/api/kafka/lab/consume")
async def api_kafka_lab_consume(body: KafkaLabConsumeBody):
    def _run() -> dict[str, Any]:
        return kafka_lab.consume_poll(**body.model_dump())

    return await asyncio.to_thread(_run)


@app.get("/trino", response_class=HTMLResponse)
async def trino_page():
    return HTMLResponse(TRINO_PAGE)


@app.get("/postgres", response_class=HTMLResponse)
async def postgres_hub_page():
    """Landing tab for Postgres-only demos (logical replication, links to scenario PG view, etc.)."""
    return HTMLResponse(POSTGRES_LANDING_PAGE)


@app.get("/postgres/logical", response_class=HTMLResponse)
async def postgres_logical_page():
    return HTMLResponse(POSTGRES_LOGICAL_PAGE)


@app.get("/postgres/faker-schema", response_class=HTMLResponse)
async def postgres_faker_schema_page():
    return HTMLResponse(POSTGRES_FAKER_SCHEMA_PAGE)


@app.get("/postgres-logical")
async def postgres_logical_legacy_redirect():
    """Old path; keep bookmarks working."""
    return RedirectResponse(url="/postgres/logical", status_code=307)


@app.post("/api/trino/query")
async def api_trino_query(body: TrinoQueryBody):
    cfg = get_runtime_config()
    _trino_sql_guard(body.sql)
    return await asyncio.to_thread(
        _execute_trino_query,
        cfg,
        body.sql.strip(),
        body.catalog,
        body.schema,
    )


@app.post("/api/postgres-logical/setup")
async def api_postgres_logical_setup():
    cfg = get_runtime_config()
    pwd = _pg_password_from_dsn(cfg.postgres_admin_dsn)
    if not pwd:
        raise HTTPException(
            500,
            "POSTGRES_ADMIN_DSN must include a password (postgresql://user:pass@host/db)",
        )
    host, port = pg_logical.parse_host_port(cfg.postgres_dsn)
    return pg_logical.logical_demo_setup(
        cfg.postgres_admin_dsn,
        cfg.postgres_logical_sub_admin_dsn,
        host,
        port,
        pwd,
    )


@app.post("/api/postgres-logical/insert")
async def api_postgres_logical_insert(body: PostgresLogicalInsertBody):
    cfg = get_runtime_config()
    return pg_logical.logical_demo_insert_row(cfg.postgres_dsn, body.note)


@app.get("/api/postgres-logical/status")
async def api_postgres_logical_status():
    cfg = get_runtime_config()
    rep = cfg.postgres_replica_read_dsn.strip() or None
    return pg_logical.logical_demo_status(
        cfg.postgres_logical_sub_admin_dsn,
        cfg.postgres_dsn,
        cfg.postgres_logical_sub_dsn,
        rep,
    )


@app.get("/api/postgres-logical/docs")
async def api_postgres_logical_docs():
    return pg_logical.pg_documentation_links()


@app.post("/api/postgres-logical/custom-table")
async def api_postgres_logical_custom_table(body: PostgresLogicalCustomTableBody):
    cfg = get_runtime_config()
    try:
        cols = [c.model_dump() for c in body.columns]
        return pg_logical.logical_custom_create_table(
            cfg.postgres_admin_dsn,
            cfg.postgres_logical_sub_admin_dsn,
            body.table_name,
            cols,
            target=body.target,
            include_bigserial_pk=body.include_bigserial_pk,
            replica_identity_full=body.replica_identity_full,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres-logical/custom-insert")
async def api_postgres_logical_custom_insert(body: PostgresLogicalCustomInsertBody):
    cfg = get_runtime_config()
    try:
        return pg_logical.logical_custom_insert_rows(
            cfg.postgres_dsn,
            body.table_name,
            body.rows,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres-logical/faker-columns")
async def api_postgres_logical_faker_columns(body: PostgresLogicalFakerColumnsBody):
    try:
        return pg_logical.faker_generate_custom_columns(
            preset=body.preset,
            column_count=body.column_count,
            seed=body.seed,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres-logical/faker-rows")
async def api_postgres_logical_faker_rows(body: PostgresLogicalFakerRowsBody):
    try:
        cols = [c.model_dump() for c in body.columns]
        return pg_logical.faker_generate_custom_rows(
            columns=cols,
            row_count=body.row_count,
            seed=body.seed,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres/faker-schema/apply")
async def api_postgres_faker_schema_apply(body: PostgresFakerSchemaBody):
    cfg = get_runtime_config()
    try:
        return pg_faker_schema.apply_faker_schema_objects(
            publisher_admin_dsn=cfg.postgres_admin_dsn,
            subscriber_admin_dsn=cfg.postgres_logical_sub_admin_dsn,
            database=body.database,
            preset=body.preset,
            seed=body.seed,
            tables=body.tables,
            sequences=body.sequences,
            views=body.views,
            materialized_views=body.materialized_views,
            functions=body.functions,
            procedures=body.procedures,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except psycopg.Error as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres/faker-schema/custom-table")
async def api_postgres_faker_schema_custom_table(body: PostgresFakerSchemaCustomTableBody):
    cfg = get_runtime_config()
    target = (
        "publisher" if body.database == pg_logical.PUB_DB else "subscriber"
    )
    try:
        cols = [c.model_dump() for c in body.columns]
        fks = [fk.model_dump() for fk in body.foreign_keys]
        return pg_logical.logical_custom_create_table(
            cfg.postgres_admin_dsn,
            cfg.postgres_logical_sub_admin_dsn,
            body.table_name,
            cols,
            target=target,
            include_bigserial_pk=body.include_bigserial_pk,
            replica_identity_full=body.replica_identity_full,
            unique_constraints=body.unique_constraints,
            foreign_keys=fks,
            schema_name=body.schema_name,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres/faker-schema/faker-insert")
async def api_postgres_faker_schema_faker_insert(body: PostgresFakerSchemaFakerInsertBody):
    cfg = get_runtime_config()
    try:
        if body.from_catalog:
            ins = pg_logical.faker_catalog_insert_rows(
                cfg.postgres_dsn,
                cfg.postgres_logical_sub_dsn,
                body.database,
                body.schema_name,
                body.table_name,
                body.row_count,
                body.seed,
                ensure_parent_rows=body.ensure_parent_rows,
            )
            gen_info = {
                "from_catalog": True,
                "seed": body.seed,
                "ensure_parent_rows": body.ensure_parent_rows,
            }
            if ins.get("inserted") is not None:
                gen_info["rows_generated"] = ins["inserted"]
            return {"faker": gen_info, **ins}

        cols = [c.model_dump() for c in body.columns]
        plan = pg_logical.faker_generate_custom_rows(
            columns=cols,
            row_count=body.row_count,
            seed=body.seed,
        )
        ins = pg_logical.logical_custom_insert_rows_for_database(
            cfg.postgres_dsn,
            cfg.postgres_logical_sub_dsn,
            body.database,
            body.table_name,
            plan["rows"],
            schema_name=body.schema_name,
        )
        return {
            "faker": {
                "from_catalog": False,
                "seed": body.seed,
                "rows_generated": plan["row_count"],
            },
            **ins,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres/faker-schema/table-catalog")
async def api_postgres_faker_schema_table_catalog(body: PostgresFakerSchemaTableCatalogBody):
    cfg = get_runtime_config()
    try:
        demo = (
            cfg.postgres_dsn
            if body.database == pg_logical.PUB_DB
            else cfg.postgres_logical_sub_dsn
        )
        return pg_logical.introspect_table_catalog_summary(
            demo,
            body.database,
            body.schema_name,
            body.table_name,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres/faker-schema/clone-export")
async def api_postgres_faker_schema_clone_export(body: PostgresSchemaCloneExportBody):
    cfg = get_runtime_config()
    adm = (
        cfg.postgres_admin_dsn
        if body.database == pg_logical.PUB_DB
        else cfg.postgres_logical_sub_admin_dsn
    )
    try:
        return pg_schema_clone.export_schema_clone_sql(
            adm,
            body.database,
            body.schema_name,
            table_names=body.table_names,
            include_sequences=body.include_sequences,
            include_tables=body.include_tables,
            include_views=body.include_views,
            include_materialized_views=body.include_materialized_views,
            include_functions=body.include_functions,
            include_procedures=body.include_procedures,
            include_grants=body.include_grants,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/postgres/faker-schema/multi-schema-tables")
async def api_postgres_faker_schema_multi_schema(body: PostgresFakerSchemaMultiSchemaBody):
    cfg = get_runtime_config()
    try:
        return pg_faker_schema.apply_multi_schema_meaningful_tables(
            publisher_admin_dsn=cfg.postgres_admin_dsn,
            subscriber_admin_dsn=cfg.postgres_logical_sub_admin_dsn,
            database=body.database,
            schemas=body.schemas,
            tables_per_schema=body.tables_per_schema,
            preset=body.preset,
            seed=body.seed,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except psycopg.Error as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.get("/api/connections")
async def api_connections_get(request: Request):
    base = env_base_config()
    rt = runtime_config_from_request_session(dict(request.session))
    ov = rt.session_overrides_only(base)
    return {
        "session_overrides_active": any(ov.values()),
        "per_field": ov,
        "masked_effective": {
            "postgres_dsn": mask_connection_hint(rt.postgres_dsn),
            "mongo_uri": mask_connection_hint(rt.mongo_uri),
            "redis_url": mask_connection_hint(rt.redis_url),
            "opensearch_url": mask_connection_hint(rt.opensearch_url),
            "cassandra_hosts": ", ".join(rt.cassandra_hosts),
            "cassandra_keyspace": rt.cassandra_keyspace,
            "opensearch_index": rt.opensearch_index,
            "opensearch_workload_index": rt.opensearch_workload_index,
            "scenario_opensearch_index": rt.scenario_opensearch_index,
            "trino_http_url": mask_connection_hint(rt.trino_http_url)
            if rt.trino_http_url
            else "(disabled)",
        },
    }


@app.post("/api/connections")
async def api_connections_post(request: Request):
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(400, "expected JSON object")
    s = request.session
    for py, sk in _CONNECTION_SESSION_KEYS:
        if py not in body:
            continue
        raw = body[py]
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            s.pop(sk, None)
            continue
        val = str(raw).strip()
        if py == "cassandra_keyspace" and not keyspace_valid(val):
            raise HTTPException(
                400,
                "cassandra_keyspace must match [a-zA-Z_][a-zA-Z0-9_]{0,47}",
            )
        s[sk] = val
    return {"ok": True}


@app.post("/api/connections/clear")
async def api_connections_clear(request: Request):
    for _py, sk in _CONNECTION_SESSION_KEYS:
        request.session.pop(sk, None)
    with _cass_override_lock:
        for cl, _s, _p in _cass_override.values():
            try:
                cl.shutdown()
            except Exception:
                pass
        _cass_override.clear()
    return {"ok": True}


@app.get("/faker-order")
async def faker_order_redirect():
    """Old path: Faker + map order UI now lives under /scenario (step 3)."""
    return RedirectResponse(url="/scenario", status_code=307)


@app.get("/api/scenario/faker-profile")
async def api_scenario_faker_profile():
    return scenario.build_faker_customer_bundle()


class ScenarioCustomOrderRequest(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=200)
    customer_email: str = Field(..., min_length=3, max_length=320)
    ship_lat: float = Field(..., ge=-90, le=90)
    ship_lon: float = Field(..., ge=-180, le=180)
    ship_label: str | None = Field(None, max_length=500)
    lines_count: int = Field(3, ge=1, le=10)


@app.post("/api/scenario/order/custom")
async def api_scenario_order_custom(req: ScenarioCustomOrderRequest):
    cass, _ = get_hub_cassandra_handles(get_runtime_config())
    return scenario.op_place_order(
        req.lines_count,
        cassandra_session=cass,
        customer_email=req.customer_email,
        customer_name=req.customer_name,
        ship_lat=req.ship_lat,
        ship_lon=req.ship_lon,
        ship_label=req.ship_label,
    )


@app.get("/scenario/data/{store}", response_class=HTMLResponse)
async def scenario_data_page(store: str):
    _ = store  # rendered client-side from path
    return HTMLResponse(SCENARIO_DATA_PAGE)


@app.post("/api/scenario/seed")
async def api_scenario_seed(count: int = 12):
    out = scenario.op_seed_catalog(min(max(count, 1), 50))
    if not out.get("ok", True):
        raise HTTPException(503, detail=out)
    return out


@app.post("/api/scenario/pipeline/mongo-sync")
async def api_scenario_mongo_sync():
    return scenario.op_pipeline_mongo_to_postgres_and_kafka()


@app.post("/api/scenario/order")
async def api_scenario_order():
    cass, _ = get_hub_cassandra_handles(get_runtime_config())
    return scenario.op_place_order(cassandra_session=cass)


@app.post("/api/scenario/pipeline/fulfill")
async def api_scenario_fulfill():
    cass, _ = get_hub_cassandra_handles(get_runtime_config())
    return scenario.op_pipeline_postgres_to_fulfillment_and_kafka(cass)


@app.get("/api/scenario/view/{store}")
async def api_scenario_view(store: str):
    key = store.lower().strip()
    try:
        if key == "postgres":
            return scenario.fetch_view_postgres()
        if key == "mongo":
            return scenario.fetch_view_mongo()
        if key == "redis":
            return scenario.fetch_view_redis()
        if key == "cassandra":
            cass, _ = get_hub_cassandra_handles(get_runtime_config())
            return scenario.fetch_view_cassandra(cass)
        if key == "opensearch":
            return scenario.fetch_view_opensearch()
        if key == "kafka":
            return scenario.fetch_view_kafka_meta()
        if key == "mssql":
            return scenario.fetch_view_mssql()
    except Exception as e:
        raise HTTPException(500, str(e)) from e
    raise HTTPException(404, f"unknown store: {store}")


@app.post("/api/workload/read")
async def api_workload_read(req: WorkloadReadRequest):
    return _workload_read_sample(req)


@app.post("/api/workload")
async def api_workload(req: WorkloadRequest):
    try:
        req.budget_ok()
    except ValueError as e:
        raise HTTPException(400, str(e)) from e

    cfg = get_runtime_config()
    run_id = str(uuid.uuid4())[:8]
    pad = _make_pad(req.payload_kb)
    now = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    bs = min(req.batch_size, 500)
    c_batches = _cassandra_rows_per_batch(pad, min(req.batch_size, 50))
    targets = set(req.targets)
    cass_sess, cass_prep = get_hub_cassandra_handles(cfg)

    sustain_deadline: float | None = None
    if req.sustain:
        sustain_deadline = t0 + _duration_seconds(
            req.duration_value, req.duration_unit
        )

    seq_base = 0
    waves = 0
    counts: dict[str, int] = {k: 0 for k in req.targets}
    errors: dict[str, str] = {}

    wl_redis: redis.Redis | None = None
    try:
        if "redis" in targets:
            # One connection for all sustain waves: opening a new TCP client every wave
            # can overwhelm Redis / hit connection limits ("Connection reset by peer").
            wl_redis = redis.from_url(
                cfg.redis_url,
                decode_responses=False,
                socket_keepalive=True,
                health_check_interval=30,
                retry_on_timeout=True,
            )
        while True:
            w_counts, w_err = _execute_workload_wave(
                cfg=cfg,
                total_records=req.total_records,
                batch_size_req=req.batch_size,
                targets=targets,
                run_id=run_id,
                pad=pad,
                now=now,
                seq_base=seq_base,
                bs=bs,
                c_batches=c_batches,
                cassandra_session=cass_sess,
                cassandra_prep=cass_prep,
                redis_client=wl_redis,
            )
            waves += 1
            for k, v in w_counts.items():
                counts[k] = counts.get(k, 0) + v
            errors.update(w_err)
            seq_base += req.total_records

            if errors:
                break
            if not req.sustain:
                break
            if sustain_deadline is not None and time.perf_counter() >= sustain_deadline:
                break
    finally:
        if wl_redis is not None:
            try:
                wl_redis.close()
            except Exception:
                pass

    elapsed = time.perf_counter() - t0
    ok = not errors
    rates = {}
    for k in counts:
        rates[k] = round(counts[k] / elapsed, 2) if elapsed > 0 else 0.0

    out: dict = {
        "ok": ok,
        "run_id": run_id,
        "seconds": round(elapsed, 3),
        "records_requested": req.total_records,
        "batch_size": req.batch_size,
        "payload_kb": req.payload_kb,
        "sustain": req.sustain,
        "waves": waves,
        "counts": counts,
        "errors": errors,
        "rates_per_s": rates,
    }
    if req.sustain and req.duration_value and req.duration_unit:
        out["sustain_seconds"] = _duration_seconds(
            req.duration_value, req.duration_unit
        )
    if "cassandra" in targets:
        out["cassandra_rows_per_batch"] = c_batches
    return out


@app.post("/api/ingest")
async def ingest():
    cfg = get_runtime_config()
    order_id = str(uuid.uuid4())
    label = f"hub-ui-order-{order_id[:8]}"
    now = datetime.now(timezone.utc)
    created_iso = now.isoformat()
    steps: dict = {}
    ok = True

    try:
        with psycopg.connect(cfg.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO demo_items (name) VALUES (%s) RETURNING id, created_at",
                    (label,),
                )
                row = cur.fetchone()
                conn.commit()
        steps["postgres"] = {
            "ok": True,
            "table": "public.demo_items",
            "id": row[0],
            "name": label,
            "created_at": row[1].isoformat() if row[1] else None,
        }
    except Exception as e:
        ok = False
        steps["postgres"] = {"ok": False, "error": str(e)}

    try:
        m = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=10_000)
        coll = m["demo"]["demo_items"]
        ins = coll.insert_one(
            {
                "name": label,
                "order_id": order_id,
                "source": "hub-demo-ui",
                "qty": 1,
                "created_at": now,
            }
        )
        steps["mongo"] = {
            "ok": True,
            "collection": "demo.demo_items",
            "inserted_id": str(ins.inserted_id),
        }
    except Exception as e:
        ok = False
        steps["mongo"] = {"ok": False, "error": str(e)}

    try:
        r = redis.from_url(cfg.redis_url, decode_responses=True)
        payload = json.dumps(
            {"order_id": order_id, "label": label, "created_at": created_iso},
            separators=(",", ":"),
        )
        r.setex(f"hub:order:{order_id}", 3600, payload)
        steps["redis"] = {
            "ok": True,
            "key": f"hub:order:{order_id}",
            "ttl_sec": 3600,
            "read_back": r.get(f"hub:order:{order_id}"),
        }
    except Exception as e:
        ok = False
        steps["redis"] = {"ok": False, "error": str(e)}

    try:
        sess, _ = get_hub_cassandra_handles(cfg)
        sess.execute(
            f"INSERT INTO {cfg.cassandra_keyspace}.orders (order_id, label, created_at) VALUES (%s, %s, %s)",
            (order_id, label, now),
        )
        row = sess.execute(
            f"SELECT order_id, label, created_at FROM {cfg.cassandra_keyspace}.orders WHERE order_id = %s",
            (order_id,),
        ).one()
        steps["cassandra"] = {
            "ok": True,
            "keyspace": cfg.cassandra_keyspace,
            "table": "orders",
            "row": {
                "order_id": row.order_id,
                "label": row.label,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            },
        }
    except Exception as e:
        ok = False
        steps["cassandra"] = {"ok": False, "error": str(e)}

    try:
        doc = {
            "order_id": order_id,
            "label": label,
            "source": "hub-demo-ui",
            "created_at": created_iso,
        }
        with httpx.Client(timeout=30.0) as hc:
            _ensure_hub_opensearch_for_cfg(hc, cfg)
            resp = hc.post(
                f"{cfg.opensearch_url}/{cfg.opensearch_index}/_doc/{order_id}",
                json=doc,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            body = resp.json()
        steps["opensearch"] = {
            "ok": True,
            "index": cfg.opensearch_index,
            "id": order_id,
            "result": body.get("result"),
        }
    except Exception as e:
        ok = False
        steps["opensearch"] = {"ok": False, "error": str(e)}

    return {
        "ok": ok,
        "order_id": order_id,
        "label": label,
        "steps": steps,
    }
