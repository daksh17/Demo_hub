"""
Browser UI + API: single-order ingest and configurable multi-DB workload generator.
"""
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import httpx
import psycopg
import redis
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient

import scenario

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

PG_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://demo:demopass@postgresql-primary:5432/demo",
)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo-mongos1:27017")
REDIS_URL = os.environ.get("REDIS_URL", "redis://:demoredispass@redis:6379/0")
CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra").split(",")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://opensearch:9200").rstrip("/")
HUB_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "demo_hub")
OS_INDEX = os.environ.get("OPENSEARCH_INDEX", "hub-orders")
WORKLOAD_REDIS_PREFIX = os.environ.get("WORKLOAD_REDIS_PREFIX", "hub:wl:")
OS_WORKLOAD_INDEX = os.environ.get("OPENSEARCH_WORKLOAD_INDEX", "hub-workload")

_cassandra_session = None
_cassandra_insert_prep = None

ALLOWED_TARGETS = frozenset({"postgres", "mongo", "redis", "cassandra", "opensearch"})


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


def _ensure_cassandra_schema(session):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {HUB_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(HUB_KEYSPACE)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
          order_id text PRIMARY KEY,
          label text,
          created_at timestamp
        )
        """
    )


def _ensure_opensearch_index(client: httpx.Client) -> None:
    specs = [
        (
            OS_INDEX,
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
            OS_WORKLOAD_INDEX,
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
        r = client.head(f"{OPENSEARCH_URL}/{idx}")
        if r.status_code == 200:
            continue
        r = client.put(f"{OPENSEARCH_URL}/{idx}", json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"OpenSearch create index {idx}: {r.status_code} {r.text}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cassandra_session, _cassandra_insert_prep
    cluster = Cluster(CASSANDRA_HOSTS)
    _cassandra_session = cluster.connect()
    _ensure_cassandra_schema(_cassandra_session)
    _cassandra_insert_prep = _cassandra_session.prepare(
        f"INSERT INTO {HUB_KEYSPACE}.orders (order_id, label, created_at) VALUES (?, ?, ?)"
    )
    scenario.ensure_cassandra_scenario_schema(_cassandra_session)
    with psycopg.connect(PG_DSN) as conn:
        scenario.ensure_postgres_scenario_schema(conn)
        conn.commit()
    with httpx.Client(timeout=120.0) as hc:
        _ensure_opensearch_index(hc)
        scenario.ensure_scenario_os_index(hc)
    yield
    cluster.shutdown()


app = FastAPI(title="Realtime hub demo UI", lifespan=lifespan)

NAV = """
  <nav style="margin-bottom:1rem;font-size:0.95rem;">
    <a href="/">Single order</a> · <a href="/workload">Workload</a> ·
    <a href="/reads">Read-back</a> · <a href="/scenario">Scenario</a>
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
    and <strong>OpenSearch</strong> (<code>hub-orders</code>). Postgres &amp; Mongo rows are also visible
    to Kafka Connect if connectors run (CDC).</p>
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
    <strong>Cassandra</strong> puts the pad in <code>label</code>; each batch row count is <strong>capped automatically</strong> so size stays under Cassandra&apos;s limit (prevents <code>Batch too large</code>).
    OpenSearch uses index <code>hub-workload</code>. REST: <code>http://localhost:9200</code>, Dashboards: <code>http://localhost:5601</code>. Grafana: separate dashboards per subsystem (Mongo, Redis, Kafka, Cassandra, …).</p>
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
    Paste the <code>run_id</code> from a workload response (the page saves the last one in the browser).</p>
  <label>run_id</label>
  <input type="text" id="run_id" placeholder="e.g. a1b2c3d4" autocomplete="off"/>
  <label>Rows per store (1–50)</label>
  <input type="number" id="limit" value="10" min="1" max="50"/>
  <fieldset>
    <legend>Targets</legend>
    <div class="row">
      <label><input type="checkbox" class="tg" value="postgres" checked/> Postgres</label>
      <label><input type="checkbox" class="tg" value="mongo" checked/> Mongo</label>
      <label><input type="checkbox" class="tg" value="redis" checked/> Redis</label>
      <label><input type="checkbox" class="tg" value="cassandra" checked/> Cassandra</label>
      <label><input type="checkbox" class="tg" value="opensearch" checked/> OpenSearch</label>
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
    const out = document.getElementById("out");
    const st = document.getElementById("st");
    const tick = document.getElementById("tick");
    let timer = null;
    let nread = 0;
    try {{
      const x = localStorage.getItem("hub_last_run_id");
      if (x) document.getElementById("run_id").value = x;
    }} catch (e) {{}}
    async function doRead() {{
      const run_id = document.getElementById("run_id").value.trim();
      if (!run_id) {{
        st.textContent = "Set run_id first.";
        st.className = "err";
        return;
      }}
      const limit = Number(document.getElementById("limit").value);
      const targets = [...document.querySelectorAll(".tg:checked")].map((x) => x.value);
      if (!targets.length) {{
        st.textContent = "Pick at least one target.";
        st.className = "err";
        return;
      }}
      const r = await fetch("/api/workload/read", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify({{ run_id, sample_limit: limit, targets }}),
      }});
      const data = await r.json();
      nread += 1;
      tick.textContent = "reads: " + nread + " · " + new Date().toISOString();
      out.textContent = JSON.stringify(data, null, 2);
      st.textContent = r.ok && data.ok ? "OK" : "Some reads failed — see errors in JSON.";
      st.className = r.ok && data.ok ? "ok" : "err";
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
          <text x="190" y="78" text-anchor="middle" class="sub">PG+K+OS+R</text>
          <text x="310" y="78" text-anchor="middle" class="sub">PG+K+OS+R+C*</text>
          <text x="430" y="78" text-anchor="middle" class="sub">PG+K+OS+C*</text>
          <text x="550" y="78" text-anchor="middle" class="sub">end</text>
          <text x="300" y="108" text-anchor="middle" class="sub">C* = Cassandra · K = Kafka · OS = hub-scenario-pipeline · R = Redis · PG = Postgres</text>
        </svg>
      </div>
      <p class="hint"><strong>Mongo</strong> is the <em>catalog service</em>: rich product docs in <code>demo.scenario_products</code>.
        <strong>Postgres</strong> holds a <em>relational mirror</em> (<code>scenario_catalog_mirror</code>), <em>orders</em> (<code>scenario_orders</code>), and <em>fulfillment lines</em> (<code>scenario_fulfillment_lines</code>).
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
        <p class="hint">Runs <code>op_pipeline_mongo_to_postgres_and_kafka</code>: reads up to 80 products from Mongo, <strong>UPSERTs</strong> into Postgres <code>scenario_catalog_mirror</code>. For each row it sends a message to Kafka topic <code>scenario.catalog.changes</code> (if the broker is reachable) and <strong>indexes the same payload</strong> into OpenSearch <code>hub-scenario-pipeline</code> with direction <code>mongo→kafka+os</code>. Pushes a short entry onto Redis list <code>scenario:kafka:recent</code> and refreshes <code>scenario:dashboard:summary</code> (counts from Postgres + Mongo).</p>
      </details>
      <details class="behind">
        <summary>3 · Place random order</summary>
        <p class="hint">Runs <code>op_place_order</code>: loads SKUs from Mongo, loads prices from <code>scenario_catalog_mirror</code> when possible (otherwise random cents). Inserts one row into <code>scenario_orders</code> with JSON <code>lines</code>, customer fields, <code>order_ref</code>. Produces <code>scenario.orders.events</code> on Kafka, mirrors to OpenSearch, writes <code>ORDER_PLACED</code> to Cassandra <code>scenario_timeline</code>, sets Redis key <code>scenario:order:latest:&lt;order_ref&gt;</code> (1h TTL), updates recent list + dashboard summary.</p>
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
        <button type="button" id="b_order">3 · Place random order</button>
        <button type="button" id="b_fulfill">4 · Fulfillment rows + Kafka + OS + Cassandra</button>
      </div>
      <p id="st"></p>
      <pre id="out">Click a step to see JSON.</pre>
      <h2>View data per store</h2>
      <div class="grid">
        <a href="/scenario/data/postgres">Postgres</a>
        <a href="/scenario/data/mongo">Mongo</a>
        <a href="/scenario/data/redis">Redis</a>
        <a href="/scenario/data/cassandra">Cassandra</a>
        <a href="/scenario/data/opensearch">OpenSearch</a>
        <a href="/scenario/data/kafka">Kafka (meta)</a>
      </div>
    </div>
    <aside class="diagram-aside">
      <h2>Vertical line (detail)</h2>
      <p class="hint" style="margin-top:0">Spine + branches. Same steps as the horizontal line above.</p>
      <svg class="flow-svg" viewBox="0 0 300 560" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Vertical scenario timeline">
        <defs>
          <marker id="ah" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
            <path d="M0,0 L8,4 L0,8 Z" fill="#8899a6"/>
          </marker>
        </defs>
        <line x1="40" y1="28" x2="40" y2="480" stroke="#6cb5f4" stroke-width="3" stroke-linecap="round"/>
        <circle class="node" cx="40" cy="40" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <circle class="node" cx="40" cy="140" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <circle class="node" cx="40" cy="260" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <circle class="node" cx="40" cy="380" r="9" fill="#1d9bf0" stroke="#e7e9ea" stroke-width="1.5"/>
        <path class="arrow" d="M55 40 L115 40"/>
        <text x="120" y="44" font-size="11px" fill="#e7e9ea" font-weight="700">① Faker → Mongo</text>
        <text x="120" y="58" class="muted">demo.scenario_products</text>
        <path class="arrow" d="M55 140 L115 140"/>
        <text x="120" y="132" font-size="11px" fill="#e7e9ea" font-weight="700">② Mongo → bus</text>
        <text x="120" y="146" class="muted">PG mirror · Kafka · OS · Redis</text>
        <path class="fan" stroke="#4a5f78" d="M115 152 L115 168 L200 168 M115 168 L230 152 M115 168 L260 180"/>
        <text x="205" y="176" class="muted">catalog.changes</text>
        <text x="240" y="190" class="muted">hub-scenario-pipeline</text>
        <path class="arrow" d="M55 260 L115 260"/>
        <text x="120" y="252" font-size="11px" fill="#e7e9ea" font-weight="700">③ New order</text>
        <text x="120" y="266" class="muted">scenario_orders · events · OS · Redis · C*</text>
        <path class="arrow" d="M55 380 L115 380"/>
        <text x="120" y="372" font-size="11px" fill="#e7e9ea" font-weight="700">④ Fulfillment</text>
        <text x="120" y="386" class="muted">fulfillment_lines · pipeline.sync · OS · C*</text>
        <text x="55" y="430" font-size="10px" fill="#8899a6">*C* = Cassandra timeline</text>
        <text x="55" y="448" class="muted">ORDER_PLACED · FULFILLMENT_READY</text>
        <text x="55" y="472" font-size="10px" fill="#8899a6">Topics</text>
        <text x="55" y="488" class="muted">scenario.catalog / .orders / .pipeline</text>
      </svg>
    </aside>
  </div>
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
    document.getElementById("b_order").onclick = () => call("/api/scenario/order", st, out);
    document.getElementById("b_fulfill").onclick = () => call("/api/scenario/pipeline/fulfill", st, out);
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
    total_records: int,
    batch_size_req: int,
    targets: set[str],
    run_id: str,
    pad: str,
    now: datetime,
    seq_base: int,
    bs: int,
    c_batches: int,
    cassandra_prep,
) -> tuple[dict[str, int], dict[str, str]]:
    """One pass writing total_records rows per target (indices seq_base .. seq_base+total_records-1)."""
    counts: dict[str, int] = {k: 0 for k in targets}
    errors: dict[str, str] = {}

    if "postgres" in targets:
        try:
            n = 0
            with psycopg.connect(PG_DSN) as conn:
                with conn.cursor() as cur:
                    for start in range(0, total_records, bs):
                        chunk = []
                        for j in range(start, min(start + bs, total_records)):
                            i = seq_base + j
                            name = (f"wl-{run_id}-{i}|{pad}")[:1048576]
                            chunk.append((name,))
                            n += 1
                        cur.executemany("INSERT INTO demo_items (name) VALUES (%s)", chunk)
                conn.commit()
            counts["postgres"] = n
        except Exception as e:
            errors["postgres"] = str(e)

    if "mongo" in targets:
        try:
            m = MongoClient(MONGO_URI, serverSelectionTimeoutMS=120_000)
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
            r = redis.from_url(REDIS_URL, decode_responses=False)
            n = 0
            for start in range(0, total_records, bs):
                pipe = r.pipeline(transaction=False)
                for j in range(start, min(start + bs, total_records)):
                    i = seq_base + j
                    key = f"{WORKLOAD_REDIS_PREFIX}{run_id}:{i}"
                    payload = json.dumps(
                        {"run_id": run_id, "seq": i, "pad": pad}, separators=(",", ":")
                    ).encode()
                    pipe.setex(key, 86400, payload)
                    n += 1
                pipe.execute()
            counts["redis"] = n
        except Exception as e:
            errors["redis"] = str(e)

    if "cassandra" in targets:
        try:
            sess = _cassandra_session
            cass_label_max = 60000
            n = 0
            for start in range(0, total_records, c_batches):
                batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE)
                for j in range(start, min(start + c_batches, total_records)):
                    i = seq_base + j
                    # Deterministic PK so read-back can SELECT ... WHERE order_id IN (...) (no SASI on label).
                    oid = f"wl-{run_id}-{i}"
                    lab = (f"wl-{run_id}-{i}|{pad}")[:cass_label_max]
                    batch.add(cassandra_prep, (oid, lab, now))
                    n += 1
                sess.execute(batch)
            counts["cassandra"] = n
        except Exception as e:
            errors["cassandra"] = str(e)

    if "opensearch" in targets:
        try:
            n = 0
            with httpx.Client(timeout=120.0) as hc:
                for start in range(0, total_records, bs):
                    lines: list[str] = []
                    for j in range(start, min(start + bs, total_records)):
                        i = seq_base + j
                        doc_id = f"{run_id}-{i}"
                        lines.append(
                            json.dumps(
                                {"index": {"_index": OS_WORKLOAD_INDEX, "_id": doc_id}}
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
                        f"{OPENSEARCH_URL}/_bulk",
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

    return counts, errors


class WorkloadReadRequest(BaseModel):
    run_id: str = Field(..., min_length=1, max_length=32)
    sample_limit: int = Field(10, ge=1, le=50)
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
    out: dict = {"ok": True, "run_id": req.run_id, "samples": {}, "errors": {}}
    lim = req.sample_limit
    tg = set(req.targets)
    like_prefix = f"wl-{req.run_id}-%"

    if "postgres" in tg:
        try:
            with psycopg.connect(PG_DSN) as conn:
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
            m = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
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
            r = redis.from_url(REDIS_URL, decode_responses=True)
            keys = [f"{WORKLOAD_REDIS_PREFIX}{req.run_id}:{i}" for i in range(lim)]
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
            sess = _cassandra_session
            # Same key convention as workload writes: order_id = wl-{run_id}-{seq} (partition key lookup, no LIKE).
            order_ids = tuple(f"wl-{req.run_id}-{i}" for i in range(lim))
            rows = sess.execute(
                (
                    f"SELECT order_id, label, created_at FROM {HUB_KEYSPACE}.orders "
                    "WHERE order_id IN %s"
                ),
                (order_ids,),
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
                resp = hc.post(
                    f"{OPENSEARCH_URL}/{OS_WORKLOAD_INDEX}/_search",
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


@app.get("/scenario/data/{store}", response_class=HTMLResponse)
async def scenario_data_page(store: str):
    _ = store  # rendered client-side from path
    return HTMLResponse(SCENARIO_DATA_PAGE)


@app.post("/api/scenario/seed")
async def api_scenario_seed(count: int = 12):
    return scenario.op_seed_catalog(min(max(count, 1), 50))


@app.post("/api/scenario/pipeline/mongo-sync")
async def api_scenario_mongo_sync():
    return scenario.op_pipeline_mongo_to_postgres_and_kafka()


@app.post("/api/scenario/order")
async def api_scenario_order():
    return scenario.op_place_order(cassandra_session=_cassandra_session)


@app.post("/api/scenario/pipeline/fulfill")
async def api_scenario_fulfill():
    return scenario.op_pipeline_postgres_to_fulfillment_and_kafka(_cassandra_session)


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
            return scenario.fetch_view_cassandra(_cassandra_session)
        if key == "opensearch":
            return scenario.fetch_view_opensearch()
        if key == "kafka":
            return scenario.fetch_view_kafka_meta()
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

    run_id = str(uuid.uuid4())[:8]
    pad = _make_pad(req.payload_kb)
    now = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    bs = min(req.batch_size, 500)
    c_batches = _cassandra_rows_per_batch(pad, min(req.batch_size, 50))
    targets = set(req.targets)
    prep = _cassandra_insert_prep

    sustain_deadline: float | None = None
    if req.sustain:
        sustain_deadline = t0 + _duration_seconds(
            req.duration_value, req.duration_unit
        )

    seq_base = 0
    waves = 0
    counts: dict[str, int] = {k: 0 for k in req.targets}
    errors: dict[str, str] = {}

    while True:
        w_counts, w_err = _execute_workload_wave(
            total_records=req.total_records,
            batch_size_req=req.batch_size,
            targets=targets,
            run_id=run_id,
            pad=pad,
            now=now,
            seq_base=seq_base,
            bs=bs,
            c_batches=c_batches,
            cassandra_prep=prep,
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
    order_id = str(uuid.uuid4())
    label = f"hub-ui-order-{order_id[:8]}"
    now = datetime.now(timezone.utc)
    created_iso = now.isoformat()
    steps: dict = {}
    ok = True

    try:
        with psycopg.connect(PG_DSN) as conn:
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
        m = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
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
        r = redis.from_url(REDIS_URL, decode_responses=True)
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
        sess = _cassandra_session
        sess.execute(
            f"INSERT INTO {HUB_KEYSPACE}.orders (order_id, label, created_at) VALUES (%s, %s, %s)",
            (order_id, label, now),
        )
        row = sess.execute(
            f"SELECT order_id, label, created_at FROM {HUB_KEYSPACE}.orders WHERE order_id = %s",
            (order_id,),
        ).one()
        steps["cassandra"] = {
            "ok": True,
            "keyspace": HUB_KEYSPACE,
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
            resp = hc.post(
                f"{OPENSEARCH_URL}/{OS_INDEX}/_doc/{order_id}",
                json=doc,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            body = resp.json()
        steps["opensearch"] = {
            "ok": True,
            "index": OS_INDEX,
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
