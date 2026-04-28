"""
Browser UI + API: single-order ingest and configurable multi-DB workload generator.
"""
import json
import os
import secrets
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

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
    <a href="/connections">External DBs</a>
  </nav>
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


@app.get("/connections", response_class=HTMLResponse)
async def connections_page():
    return HTMLResponse(CONNECTIONS_PAGE)


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
