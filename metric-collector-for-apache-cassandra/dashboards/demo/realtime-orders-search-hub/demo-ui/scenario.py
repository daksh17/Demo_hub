"""
Multi-store realistic demo: Faker catalog in Mongo, transactional orders in Postgres,
Kafka-style events (real broker + mirrored into OpenSearch as a Connect-style log),
Redis dashboards, Cassandra timeline. Pipelines are explicit steps you trigger from the UI
(Production would use long-running consumers / Kafka Connect).
"""
from __future__ import annotations

import json
import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
import psycopg
import redis
from cassandra.cluster import Session as CassandraSession
from faker import Faker
from pymongo import MongoClient

fake = Faker()
Faker.seed(42)

PG_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://demo:demopass@postgresql-primary:5432/demo",
)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo-mongos1:27017")
REDIS_URL = os.environ.get("REDIS_URL", "redis://:demoredispass@redis:6379/0")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://opensearch:9200").rstrip("/")
HUB_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "demo_hub")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
SCENARIO_OS_INDEX = os.environ.get("SCENARIO_PIPELINE_OS_INDEX", "hub-scenario-pipeline")

MONGO_COLL = "scenario_products"
REDIS_DASH_KEY = "scenario:dashboard:summary"
REDIS_KAFKA_RECENT = "scenario:kafka:recent"
TOPIC_CATALOG = "scenario.catalog.changes"
TOPIC_ORDERS = "scenario.orders.events"
TOPIC_PIPELINE = "scenario.pipeline.sync"


def _kafka_producer():
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        return None
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: (k or "").encode("utf-8"),
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
    except Exception:
        return None


_producer = None


def _producer_send(topic: str, key: str, payload: dict) -> bool:
    global _producer
    if _producer is None:
        _producer = _kafka_producer()
    if _producer is None:
        return False
    try:
        _producer.send(topic, key=key, value=payload)
        _producer.flush(timeout=3)
        return True
    except Exception:
        return False


def ensure_postgres_scenario_schema(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_catalog_mirror (
          id SERIAL PRIMARY KEY,
          sku TEXT NOT NULL UNIQUE,
          title TEXT NOT NULL,
          category TEXT,
          unit_price_cents INT NOT NULL,
          stock_units INT NOT NULL DEFAULT 0,
          source_mongo_id TEXT,
          kafka_msg_key TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_orders (
          id SERIAL PRIMARY KEY,
          order_ref TEXT NOT NULL UNIQUE,
          customer_email TEXT NOT NULL,
          customer_name TEXT,
          lines JSONB NOT NULL,
          total_cents INT NOT NULL,
          pipeline_stage TEXT NOT NULL DEFAULT 'placed',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        "ALTER TABLE scenario_orders ADD COLUMN IF NOT EXISTS ship_lat DOUBLE PRECISION"
    )
    conn.execute(
        "ALTER TABLE scenario_orders ADD COLUMN IF NOT EXISTS ship_lon DOUBLE PRECISION"
    )
    conn.execute(
        "ALTER TABLE scenario_orders ADD COLUMN IF NOT EXISTS ship_label TEXT"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_fulfillment_lines (
          id SERIAL PRIMARY KEY,
          order_ref TEXT NOT NULL REFERENCES scenario_orders(order_ref) ON DELETE CASCADE,
          sku TEXT NOT NULL,
          qty INT NOT NULL,
          notes TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    _ensure_postgres_scenario_indexes(conn)


def _ensure_postgres_scenario_indexes(conn: psycopg.Connection) -> None:
    """B-tree, GIN (JSONB), BRIN (time), HASH (equality), partial, compound — demo-friendly set."""
    conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    stmts = [
        # --- scenario_catalog_mirror ---
        "CREATE INDEX IF NOT EXISTS idx_scenario_catalog_category_price ON scenario_catalog_mirror (category, unit_price_cents, sku)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_catalog_updated_at ON scenario_catalog_mirror (updated_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_catalog_brin_updated ON scenario_catalog_mirror USING BRIN (updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_catalog_in_stock ON scenario_catalog_mirror (category, sku) WHERE stock_units > 0",
        "CREATE INDEX IF NOT EXISTS idx_scenario_catalog_title_trgm ON scenario_catalog_mirror USING GIST (title gist_trgm_ops)",
        # HASH: equality-only lookups on message key (postgres requires btree-compatible types; HASH is OK for text)
        "CREATE INDEX IF NOT EXISTS idx_scenario_catalog_kafka_key_hash ON scenario_catalog_mirror USING HASH (kafka_msg_key)",
        # --- scenario_orders ---
        "CREATE INDEX IF NOT EXISTS idx_scenario_orders_stage_created ON scenario_orders (pipeline_stage, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_orders_email ON scenario_orders (customer_email)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_orders_created_brin ON scenario_orders USING BRIN (created_at)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_orders_placed_partial ON scenario_orders (created_at DESC) WHERE pipeline_stage = 'placed'",
        "CREATE INDEX IF NOT EXISTS idx_scenario_orders_lines_gin ON scenario_orders USING GIN (lines jsonb_path_ops)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_orders_stage_cover ON scenario_orders (pipeline_stage) INCLUDE (order_ref, total_cents)",
        # --- scenario_fulfillment_lines ---
        "CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_order_ref ON scenario_fulfillment_lines (order_ref)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_sku ON scenario_fulfillment_lines (sku)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_order_sku ON scenario_fulfillment_lines (order_ref, sku)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_brin_created ON scenario_fulfillment_lines USING BRIN (created_at)",
    ]
    for sql in stmts:
        conn.execute(sql)


def ensure_cassandra_scenario_schema(session: CassandraSession) -> None:
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HUB_KEYSPACE}.scenario_timeline (
          order_ref text,
          event_ts timestamp,
          event_type text,
          detail text,
          PRIMARY KEY (order_ref, event_ts)
        ) WITH CLUSTERING ORDER BY (event_ts DESC)
        """
    )
    # Secondary index: use only for low-cardinality filters in demos (ORDER_PLACED vs FULFILLMENT_READY).
    # High-cardinality or large partitions: prefer model + query by partition key or a dedicated table.
    session.execute(
        f"CREATE INDEX IF NOT EXISTS scenario_timeline_event_type ON {HUB_KEYSPACE}.scenario_timeline (event_type)"
    )


def ensure_scenario_os_index(client: httpx.Client) -> None:
    body = {
        "mappings": {
            "properties": {
                "topic": {"type": "keyword"},
                "msg_key": {"type": "keyword"},
                "direction": {"type": "keyword"},
                "payload": {"type": "object", "enabled": True},
                "ts": {"type": "date"},
            }
        }
    }
    r = client.head(f"{OPENSEARCH_URL}/{SCENARIO_OS_INDEX}")
    if r.status_code == 200:
        return
    r = client.put(f"{OPENSEARCH_URL}/{SCENARIO_OS_INDEX}", json=body)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"OpenSearch {SCENARIO_OS_INDEX}: {r.status_code} {r.text}")


def _mirror_to_opensearch(
    client: httpx.Client, *, topic: str, key: str, direction: str, payload: dict
) -> None:
    doc = {
        "topic": topic,
        "msg_key": key,
        "direction": direction,
        "payload": payload,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    _id = f"{key}-{uuid.uuid4().hex[:12]}"
    r = client.put(
        f"{OPENSEARCH_URL}/{SCENARIO_OS_INDEX}/_doc/{_id}",
        json=doc,
        headers={"Content-Type": "application/json"},
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"OpenSearch indexOS: {r.status_code} {r.text}")


def _redis_push_recent(r: redis.Redis, rec: dict) -> None:
    r.lpush(REDIS_KAFKA_RECENT, json.dumps(rec, default=str))
    r.ltrim(REDIS_KAFKA_RECENT, 0, 99)


def _redis_refresh_summary(r: redis.Redis) -> None:
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM scenario_catalog_mirror")
            n_mir = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_orders")
            n_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_fulfillment_lines")
            n_ful = cur.fetchone()[0]
    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    n_mongo = mc["demo"][MONGO_COLL].count_documents({})
    mc.close()
    summary = {
        "postgres_catalog_mirror_rows": n_mir,
        "postgres_orders": n_ord,
        "postgres_fulfillment_lines": n_ful,
        "mongo_catalog_docs": n_mongo,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    r.set(REDIS_DASH_KEY, json.dumps(summary))


# --- API-facing operations ---


def op_seed_catalog(count: int = 10) -> dict[str, Any]:
    """Service A: product catalog documents in Mongo (rich semi-realistic attributes)."""
    mc: MongoClient | None = None
    try:
        mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
        coll = mc["demo"][MONGO_COLL]
        inserted = []
        cats = ["electronics", "home", "apparel", "grocery", "sports"]
        for _ in range(count):
            sku = f"SKU-{uuid.uuid4().hex[:8].upper()}"
            doc = {
                "sku": sku,
                "title": fake.catch_phrase(),
                "category": fake.random_element(cats),
                "unit_price_cents": fake.random_int(499, 49999),
                "stock_units": fake.random_int(0, 500),
                "warehouse": fake.random_element(["east-1", "west-2", "eu-1"]),
                "description": fake.text(max_nb_chars=200),
                "source": "scenario-seed",
                "updated_at": datetime.now(timezone.utc),
            }
            coll.insert_one(doc)
            inserted.append({"sku": sku, "title": doc["title"][:60]})
        return {"ok": True, "mongo_inserted": len(inserted), "samples": inserted[:5]}
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "mongo_uri": MONGO_URI,
            "hint": "K8s: ensure Job mongo-demo-bootstrap is Complete (sharding + demo DB). "
            "Check: kubectl logs -n demo-hub job/mongo-demo-bootstrap --tail=80 ; "
            "kubectl logs -n demo-hub deploy/mongo-mongos1 --tail=50",
        }
    finally:
        if mc is not None:
            mc.close()


def op_pipeline_mongo_to_postgres_and_kafka() -> dict[str, Any]:
    """
    Consume-from-Mongo pattern (here: synchronous pull). Writes mirror rows in Postgres,
    emits Kafka events, mirrors same payload to OpenSearch (stand-in for Kafka→OS sink).
    """
    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    coll = mc["demo"][MONGO_COLL]
    docs = list(coll.find().sort("updated_at", -1).limit(80))
    mc.close()
    mirrored = 0
    kafka_ok = 0
    with httpx.Client(timeout=60.0) as hc:
        with psycopg.connect(PG_DSN) as conn:
            ensure_postgres_scenario_schema(conn)
            for d in docs:
                sku = d.get("sku")
                if not sku:
                    continue
                msg_key = f"catalog-{sku}"
                conn.execute(
                    """
                    INSERT INTO scenario_catalog_mirror
                      (sku, title, category, unit_price_cents, stock_units, source_mongo_id, kafka_msg_key)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (sku) DO UPDATE SET
                      title = EXCLUDED.title,
                      category = EXCLUDED.category,
                      unit_price_cents = EXCLUDED.unit_price_cents,
                      stock_units = EXCLUDED.stock_units,
                      source_mongo_id = EXCLUDED.source_mongo_id,
                      kafka_msg_key = EXCLUDED.kafka_msg_key,
                      updated_at = NOW()
                    """,
                    (
                        sku,
                        d.get("title", "")[:512],
                        d.get("category"),
                        int(d.get("unit_price_cents", 0)),
                        int(d.get("stock_units", 0)),
                        str(d.get("_id", "")),
                        msg_key,
                    ),
                )
                mirrored += 1
                payload = {
                    "action": "catalog.synced_from_mongo",
                    "sku": sku,
                    "title": d.get("title"),
                    "warehouse": d.get("warehouse"),
                }
                if _producer_send(TOPIC_CATALOG, msg_key, payload):
                    kafka_ok += 1
                _mirror_to_opensearch(
                    hc, topic=TOPIC_CATALOG, key=msg_key, direction="mongo→kafka+os", payload=payload
                )
            conn.commit()

    r = redis.from_url(REDIS_URL, decode_responses=True)
    _redis_push_recent(
        r,
        {
            "topic": TOPIC_CATALOG,
            "kind": "pipeline_mongo_to_postgres",
            "mirrored_rows": mirrored,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )
    _redis_refresh_summary(r)
    return {
        "ok": True,
        "mongo_docs_processed": len(docs),
        "postgres_rows_touched": mirrored,
        "kafka_events_sent": kafka_ok,
        "opensearch_mirrored": mirrored,
        "note": "OpenSearch holds the same logical stream Connect would sink from Kafka.",
    }


def build_faker_customer_bundle() -> dict[str, Any]:
    """Random identity + coordinates for the guided-order UI."""
    return {
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "ship_lat": float(fake.latitude()),
        "ship_lon": float(fake.longitude()),
        "ship_label": fake.address().replace("\n", ", ")[:500],
    }


def op_place_order(
    lines_count: int = 3,
    cassandra_session: CassandraSession | None = None,
    *,
    customer_email: str | None = None,
    customer_name: str | None = None,
    ship_lat: float | None = None,
    ship_lon: float | None = None,
    ship_label: str | None = None,
) -> dict[str, Any]:
    """OLTP: order in Postgres + timeline in Cassandra + events (Kafka + OS) + Redis."""
    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    coll = mc["demo"][MONGO_COLL]
    skus = [d["sku"] for d in coll.find({}, {"sku": 1}).limit(50)]
    mc.close()
    if len(skus) < 1:
        return {"ok": False, "error": "Seed Mongo catalog first (Scenario → Seed)."}
    random.shuffle(skus)
    pick = skus[: min(lines_count, len(skus))]
    lines = []
    total = 0
    with psycopg.connect(PG_DSN) as conn:
        ensure_postgres_scenario_schema(conn)
        with conn.cursor() as cur:
            for sku in pick:
                cur.execute(
                    "SELECT unit_price_cents FROM scenario_catalog_mirror WHERE sku = %s",
                    (sku,),
                )
                row = cur.fetchone()
                price = row[0] if row else fake.random_int(999, 9999)
                qty = fake.random_int(1, 4)
                lines.append({"sku": sku, "qty": qty, "unit_price_cents": price})
                total += price * qty
    order_ref = f"ORD-{uuid.uuid4().hex[:10].upper()}"
    ce = (customer_email or fake.email()).strip()
    cn = (customer_name or fake.name()).strip() or fake.name()
    slat = ship_lat
    slon = ship_lon
    slab = (ship_label or "").strip()[:500] or None

    with psycopg.connect(PG_DSN) as conn:
        ensure_postgres_scenario_schema(conn)
        conn.execute(
            """
            INSERT INTO scenario_orders
              (order_ref, customer_email, customer_name, lines, total_cents, pipeline_stage,
               ship_lat, ship_lon, ship_label)
            VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
            """,
            (
                order_ref,
                ce,
                cn,
                json.dumps(lines),
                total,
                "placed",
                slat,
                slon,
                slab,
            ),
        )
        conn.commit()

    detail = json.dumps(
        {
            "lines": lines,
            "total_cents": total,
            "ship_lat": slat,
            "ship_lon": slon,
            "ship_label": slab,
        }
    )
    # Cassandra timeline (requires session from caller — use cluster in app)
    payload = {
        "action": "order.placed",
        "order_ref": order_ref,
        "customer": ce,
        "customer_name": cn,
        "total_cents": total,
        "lines": lines,
        "ship_lat": slat,
        "ship_lon": slon,
        "ship_label": slab,
    }
    sent = _producer_send(TOPIC_ORDERS, order_ref, payload)
    with httpx.Client(timeout=30.0) as hc:
        _mirror_to_opensearch(
            hc, topic=TOPIC_ORDERS, key=order_ref, direction="api→kafka+os", payload=payload
        )

    if cassandra_session:
        op_write_cassandra_timeline(
            cassandra_session,
            order_ref,
            "ORDER_PLACED",
            detail[:4000],
        )

    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.setex(
        f"scenario:order:latest:{order_ref}",
        3600,
        json.dumps(
            {
                "order_ref": order_ref,
                "customer_email": ce,
                "customer_name": cn,
                "total_cents": total,
                "lines": lines,
                "ship_lat": slat,
                "ship_lon": slon,
                "ship_label": slab,
            }
        ),
    )
    _redis_push_recent(
        r,
        {"topic": TOPIC_ORDERS, "order_ref": order_ref, "total_cents": total},
    )
    _redis_refresh_summary(r)

    return {
        "ok": True,
        "order_ref": order_ref,
        "customer_email": ce,
        "customer_name": cn,
        "ship_lat": slat,
        "ship_lon": slon,
        "ship_label": slab,
        "total_cents": total,
        "lines": lines,
        "kafka_sent": sent,
    }


def op_write_cassandra_timeline(
    session: CassandraSession, order_ref: str, event_type: str, detail: str
) -> None:
    session.execute(
        f"""
        INSERT INTO {HUB_KEYSPACE}.scenario_timeline (order_ref, event_ts, event_type, detail)
        VALUES (%s, %s, %s, %s)
        """,
        (order_ref, datetime.now(timezone.utc), event_type, detail[:4000]),
    )


def op_pipeline_postgres_to_fulfillment_and_kafka(
    cassandra_session: CassandraSession | None,
) -> dict[str, Any]:
    """
    Another path: read committed orders in Postgres, create fulfillment lines + broadcast.
    """
    with psycopg.connect(PG_DSN) as conn:
        ensure_postgres_scenario_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_ref, lines FROM scenario_orders
                WHERE order_ref NOT IN (
                  SELECT DISTINCT order_ref FROM scenario_fulfillment_lines
                )
                ORDER BY id DESC LIMIT 20
                """
            )
            pending = cur.fetchall()

        n = 0
        for order_ref, lines_blob in pending:
            if isinstance(lines_blob, str):
                lines = json.loads(lines_blob)
            else:
                lines = list(lines_blob)
            for ln in lines:
                conn.execute(
                    """
                    INSERT INTO scenario_fulfillment_lines (order_ref, sku, qty, notes)
                    VALUES (%s,%s,%s,%s)
                    """,
                    (
                        order_ref,
                        ln["sku"],
                        ln["qty"],
                        "derived_from_postgres_order",
                    ),
                )
                n += 1
            payload = {
                "action": "fulfillment.created",
                "order_ref": order_ref,
                "line_count": len(lines),
            }
            _producer_send(TOPIC_PIPELINE, order_ref, payload)
            with httpx.Client(timeout=30.0) as hc:
                _mirror_to_opensearch(
                    hc,
                    topic=TOPIC_PIPELINE,
                    key=order_ref,
                    direction="postgres→kafka+os",
                    payload=payload,
                )
            if cassandra_session:
                op_write_cassandra_timeline(
                    cassandra_session,
                    order_ref,
                    "FULFILLMENT_READY",
                    json.dumps(payload),
                )
        conn.commit()

    r = redis.from_url(REDIS_URL, decode_responses=True)
    _redis_refresh_summary(r)
    return {
        "ok": True,
        "orders_fulfilled": len(pending),
        "fulfillment_lines_inserted": n,
    }


def fetch_view_postgres() -> dict[str, Any]:
    out: dict[str, Any] = {"tables": {}}
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for name, sql in [
                ("scenario_catalog_mirror", "SELECT * FROM scenario_catalog_mirror ORDER BY id DESC LIMIT 25"),
                ("scenario_orders", "SELECT * FROM scenario_orders ORDER BY id DESC LIMIT 25"),
                ("scenario_fulfillment_lines", "SELECT * FROM scenario_fulfillment_lines ORDER BY id DESC LIMIT 25"),
            ]:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                for r in rows:
                    for k, v in list(r.items()):
                        if hasattr(v, "isoformat"):
                            r[k] = v.isoformat()
                out["tables"][name] = {"columns": cols, "rows": rows}
    return out


def fetch_view_mongo() -> dict[str, Any]:
    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    cur = (
        mc["demo"][MONGO_COLL]
        .find({}, {"_id": 1, "sku": 1, "title": 1, "category": 1, "unit_price_cents": 1, "stock_units": 1})
        .sort("_id", -1)
        .limit(30)
    )
    docs = []
    for d in cur:
        d["_id"] = str(d["_id"])
        docs.append(d)
    mc.close()
    return {"collection": f"demo.{MONGO_COLL}", "documents": docs}


def fetch_view_redis() -> dict[str, Any]:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    dash = r.get(REDIS_DASH_KEY)
    recent = r.lrange(REDIS_KAFKA_RECENT, 0, 19)
    keys = r.keys("scenario:order:latest:*")[:15]
    orders = []
    for k in keys:
        v = r.get(k)
        if v:
            try:
                orders.append(json.loads(v))
            except json.JSONDecodeError:
                orders.append({"key": k, "raw": v[:200]})
    return {
        "dashboard_summary": json.loads(dash) if dash else None,
        "recent_pipeline_events": [json.loads(x) for x in recent] if recent else [],
        "cached_latest_orders_sample": orders,
    }


def fetch_view_cassandra(session: CassandraSession) -> dict[str, Any]:
    rows = session.execute(
        f"SELECT order_ref, event_ts, event_type, detail FROM {HUB_KEYSPACE}.scenario_timeline "
        "LIMIT 50 ALLOW FILTERING"
    )
    out = []
    for row in rows:
        out.append(
            {
                "order_ref": row.order_ref,
                "event_ts": row.event_ts.isoformat() if row.event_ts else None,
                "event_type": row.event_type,
                "detail": (row.detail or "")[:300],
            }
        )
    return {"table": f"{HUB_KEYSPACE}.scenario_timeline", "rows": out}


def fetch_view_opensearch() -> dict[str, Any]:
    query = {
        "size": 20,
        "sort": [{"ts": {"order": "desc"}}],
        "query": {"match_all": {}},
    }
    with httpx.Client(timeout=30.0) as hc:
        r = hc.post(
            f"{OPENSEARCH_URL}/{SCENARIO_OS_INDEX}/_search",
            json=query,
            headers={"Content-Type": "application/json"},
        )
        if r.status_code == 404:
            return {"hits": [], "note": "Index empty or missing; run a pipeline first."}
        r.raise_for_status()
        body = r.json()
    hits = body.get("hits", {}).get("hits", [])
    slim = []
    for h in hits:
        src = h.get("_source", {})
        slim.append(
            {
                "_id": h.get("_id"),
                "topic": src.get("topic"),
                "msg_key": src.get("msg_key"),
                "direction": src.get("direction"),
                "ts": src.get("ts"),
                "payload": src.get("payload"),
            }
        )
    return {"index": SCENARIO_OS_INDEX, "hits": slim}


def fetch_view_kafka_meta() -> dict[str, Any]:
    p = _kafka_producer()
    return {
        "bootstrap": KAFKA_BOOTSTRAP,
        "topics_emitted": [TOPIC_CATALOG, TOPIC_ORDERS, TOPIC_PIPELINE],
        "producer_ready": p is not None,
        "hint": "This UI produces to Kafka and mirrors the same events into OpenSearch. "
        "Use kafka-console-consumer or Grafana Kafka dashboard to verify broker traffic.",
    }
