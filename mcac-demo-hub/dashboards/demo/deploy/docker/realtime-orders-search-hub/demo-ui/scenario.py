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

from hub_config import get_runtime_config

fake = Faker()
Faker.seed(42)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")

# Optional SQL Server (Compose: mssql-publisher) — catalog mirror + Debezium CDC.
MSSQL_HOST = os.environ.get("MSSQL_HOST", "").strip()
MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "1433") or "1433")
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_SA_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "")
MSSQL_DATABASE = os.environ.get("MSSQL_DATABASE", "demo")
# Docker SQL Server: pymssql often needs encrypt=off unless you configure TLS.
_MSSQL_ENCRYPT = os.environ.get("MSSQL_ENCRYPT", "off").strip().lower()

MONGO_COLL = "scenario_products"
MONGO_COLL_SUPPLIERS = "scenario_suppliers"
REDIS_DASH_KEY = "scenario:dashboard:summary"
REDIS_KAFKA_RECENT = "scenario:kafka:recent"
REDIS_SHIPMENTS_RECENT = "scenario:shipments:recent"
TOPIC_CATALOG = "scenario.catalog.changes"
TOPIC_ORDERS = "scenario.orders.events"
TOPIC_PIPELINE = "scenario.pipeline.sync"
TOPIC_SHIPMENTS = "scenario.shipments.events"


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_customers (
          customer_email TEXT PRIMARY KEY,
          customer_name TEXT,
          loyalty_tier TEXT NOT NULL DEFAULT 'standard',
          orders_placed INT NOT NULL DEFAULT 0,
          lifetime_cents BIGINT NOT NULL DEFAULT 0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_payments (
          id SERIAL PRIMARY KEY,
          order_ref TEXT NOT NULL REFERENCES scenario_orders(order_ref) ON DELETE CASCADE,
          payment_method TEXT NOT NULL,
          amount_cents INT NOT NULL,
          status TEXT NOT NULL DEFAULT 'captured',
          processor_ref TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_shipments (
          id SERIAL PRIMARY KEY,
          order_ref TEXT NOT NULL REFERENCES scenario_orders(order_ref) ON DELETE CASCADE,
          carrier TEXT NOT NULL,
          tracking_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'label_created',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(order_ref)
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
        # --- scenario_customers ---
        "CREATE INDEX IF NOT EXISTS idx_scenario_customers_tier ON scenario_customers (loyalty_tier)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_customers_updated ON scenario_customers (updated_at DESC)",
        # --- scenario_payments ---
        "CREATE INDEX IF NOT EXISTS idx_scenario_payments_order ON scenario_payments (order_ref)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_payments_method ON scenario_payments (payment_method, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_payments_brin_created ON scenario_payments USING BRIN (created_at)",
        # --- scenario_shipments ---
        "CREATE INDEX IF NOT EXISTS idx_scenario_shipments_carrier ON scenario_shipments (carrier)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_shipments_tracking ON scenario_shipments (tracking_id)",
        "CREATE INDEX IF NOT EXISTS idx_scenario_shipments_brin_created ON scenario_shipments USING BRIN (created_at)",
    ]
    for sql in stmts:
        conn.execute(sql)


def ensure_cassandra_scenario_schema(
    session: CassandraSession, keyspace: str | None = None
) -> None:
    ks = keyspace if keyspace is not None else get_runtime_config().cassandra_keyspace
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ks}.scenario_timeline (
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
        f"CREATE INDEX IF NOT EXISTS scenario_timeline_event_type ON {ks}.scenario_timeline (event_type)"
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ks}.scenario_carrier_shipments (
          carrier text,
          tracking_id text,
          order_ref text,
          created_ts timestamp,
          PRIMARY KEY ((carrier), tracking_id)
        )
        """
    )


def ensure_scenario_os_index(client: httpx.Client) -> None:
    cfg = get_runtime_config()
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
    r = client.head(f"{cfg.opensearch_url}/{cfg.scenario_opensearch_index}")
    if r.status_code == 200:
        return
    r = client.put(
        f"{cfg.opensearch_url}/{cfg.scenario_opensearch_index}", json=body
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(
            f"OpenSearch {cfg.scenario_opensearch_index}: {r.status_code} {r.text}"
        )


def _mirror_to_opensearch(
    client: httpx.Client, *, topic: str, key: str, direction: str, payload: dict
) -> None:
    cfg = get_runtime_config()
    doc = {
        "topic": topic,
        "msg_key": key,
        "direction": direction,
        "payload": payload,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    _id = f"{key}-{uuid.uuid4().hex[:12]}"
    r = client.put(
        f"{cfg.opensearch_url}/{cfg.scenario_opensearch_index}/_doc/{_id}",
        json=doc,
        headers={"Content-Type": "application/json"},
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"OpenSearch indexOS: {r.status_code} {r.text}")


def _redis_push_recent(r: redis.Redis, rec: dict) -> None:
    r.lpush(REDIS_KAFKA_RECENT, json.dumps(rec, default=str))
    r.ltrim(REDIS_KAFKA_RECENT, 0, 99)


def _redis_refresh_summary(r: redis.Redis) -> None:
    cfg = get_runtime_config()
    with psycopg.connect(cfg.postgres_dsn) as conn:
        ensure_postgres_scenario_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM scenario_catalog_mirror")
            n_mir = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_orders")
            n_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_fulfillment_lines")
            n_ful = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_customers")
            n_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_payments")
            n_pay = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM scenario_shipments")
            n_ship = cur.fetchone()[0]
    mc = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=10_000)
    n_mongo = mc["demo"][MONGO_COLL].count_documents({})
    n_sup = mc["demo"][MONGO_COLL_SUPPLIERS].count_documents({})
    mc.close()
    summary = {
        "postgres_catalog_mirror_rows": n_mir,
        "postgres_orders": n_ord,
        "postgres_fulfillment_lines": n_ful,
        "postgres_customers": n_cust,
        "postgres_payments": n_pay,
        "postgres_shipments": n_ship,
        "mongo_catalog_docs": n_mongo,
        "mongo_supplier_docs": n_sup,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    r.set(REDIS_DASH_KEY, json.dumps(summary))


# --- API-facing operations ---


def op_seed_catalog(count: int = 10, supplier_count: int = 0) -> dict[str, Any]:
    """Service A: product catalog + optional supplier roster in Mongo."""
    cfg = get_runtime_config()
    mc: MongoClient | None = None
    try:
        mc = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=30_000)
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
                "weight_grams": fake.random_int(50, 15000),
                "return_window_days": fake.random_element([14, 30, 60]),
                "vendor_region": fake.random_element(["NA", "EU", "APAC", "LATAM"]),
                "source": "scenario-seed",
                "updated_at": datetime.now(timezone.utc),
            }
            coll.insert_one(doc)
            inserted.append({"sku": sku, "title": doc["title"][:60]})

        sup_coll = mc["demo"][MONGO_COLL_SUPPLIERS]
        suppliers_ins = []
        if supplier_count > 0:
            regions = ["NA", "EU", "APAC", "LATAM"]
            for _ in range(supplier_count):
                code = f"VND-{uuid.uuid4().hex[:6].upper()}"
                sdoc = {
                    "supplier_code": code,
                    "company": fake.company(),
                    "region": fake.random_element(regions),
                    "rating": round(random.uniform(3.4, 5.0), 2),
                    "lead_time_days": fake.random_int(1, 21),
                    "contact_email": fake.company_email(),
                    "categories_served": fake.random_elements(
                        elements=cats,
                        length=fake.random_int(1, min(3, len(cats))),
                        unique=True,
                    ),
                    "updated_at": datetime.now(timezone.utc),
                }
                sup_coll.insert_one(sdoc)
                suppliers_ins.append({"supplier_code": code, "company": sdoc["company"][:50]})

        r = redis.from_url(cfg.redis_url, decode_responses=True)
        _redis_refresh_summary(r)

        return {
            "ok": True,
            "mongo_inserted": len(inserted),
            "mongo_products_inserted": len(inserted),
            "mongo_suppliers_inserted": len(suppliers_ins),
            "samples": inserted[:5],
            "product_samples": inserted[:5],
            "supplier_samples": suppliers_ins[:5],
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "mongo_uri": cfg.mongo_uri,
            "hint": "K8s: ensure Job mongo-demo-bootstrap is Complete (sharding + demo DB). "
            "Check: kubectl logs -n demo-hub job/mongo-demo-bootstrap --tail=80 ; "
            "kubectl logs -n demo-hub deploy/mongo-mongos1 --tail=50",
        }
    finally:
        if mc is not None:
            mc.close()


def _mssql_connect() -> tuple[Any | None, str | None]:
    """
    Returns (connection, None) on success, or (None, short_error) when SQL Server
    is skipped or unreachable. Callers can surface the message in API JSON / workload errors.
    """
    try:
        import pymssql  # type: ignore
    except ImportError as e:
        return None, f"pymssql not installed ({e})"
    if not MSSQL_HOST or not MSSQL_SA_PASSWORD:
        return None, "MSSQL_HOST or MSSQL_SA_PASSWORD is not set"
    kw: dict[str, Any] = dict(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_SA_PASSWORD,
        database=MSSQL_DATABASE,
        timeout=30,
    )
    if _MSSQL_ENCRYPT in ("off", "false", "0", "no"):
        kw["encrypt"] = "off"
    elif _MSSQL_ENCRYPT in ("on", "true", "1", "yes"):
        kw["encrypt"] = "on"
    try:
        return pymssql.connect(**kw), None
    except TypeError:
        kw.pop("encrypt", None)
        try:
            return pymssql.connect(**kw), None
        except Exception as e:
            return None, f"connect failed: {e}"
    except Exception as e:
        return None, f"connect failed: {e}"


def mssql_merge_catalog_row(
    conn: Any,
    sku: str,
    title: str,
    category: str | None,
    unit_price_cents: int,
    stock_units: int,
    source_mongo_id: str,
    kafka_msg_key: str,
) -> bool:
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE dbo.scenario_catalog_mirror_mssql WITH (HOLDLOCK) AS T
            USING (
              SELECT %s AS sku, %s AS title, %s AS category, %s AS unit_price_cents,
                     %s AS stock_units, %s AS source_mongo_id, %s AS kafka_msg_key
            ) AS S
            ON T.sku = S.sku
            WHEN MATCHED THEN UPDATE SET
              title = S.title, category = S.category, unit_price_cents = S.unit_price_cents,
              stock_units = S.stock_units, source_mongo_id = S.source_mongo_id,
              kafka_msg_key = S.kafka_msg_key, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
              INSERT (sku, title, category, unit_price_cents, stock_units, source_mongo_id, kafka_msg_key)
              VALUES (S.sku, S.title, S.category, S.unit_price_cents, S.stock_units, S.source_mongo_id, S.kafka_msg_key);
            """,
            (
                sku,
                title[:512],
                category,
                unit_price_cents,
                stock_units,
                source_mongo_id[:96] if source_mongo_id else None,
                kafka_msg_key[:160] if kafka_msg_key else None,
            ),
        )
        return True
    except Exception:
        return False


def fetch_view_mssql() -> dict[str, Any]:
    conn, err = _mssql_connect()
    if conn is None:
        return {
            "ok": False,
            "error": err or "MSSQL connection unavailable.",
        }
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT TOP 40 * FROM dbo.scenario_catalog_mirror_mssql ORDER BY id DESC"
        )
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        for r in rows:
            for k, v in list(r.items()):
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
                elif hasattr(v, "as_tuple"):  # Decimal
                    r[k] = float(v)
        return {"ok": True, "table": "dbo.scenario_catalog_mirror_mssql", "rows": rows}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


MSSQL_WORKLOAD_NAME_MAX_CHARS = int(
    os.environ.get("MSSQL_WORKLOAD_NAME_MAX_CHARS", "4000")
)


def workload_mssql_batch(
    run_id: str,
    seq_base: int,
    total_records: int,
    batch_size: int,
    pad: str,
) -> tuple[int, str | None]:
    """
    Insert synthetic rows into dbo.hub_workload_mssql (same run_id/seq convention as other stores).
    Returns (rows_committed, error_message).
    """
    conn, err = _mssql_connect()
    if conn is None:
        return 0, err or "mssql unavailable"
    lim = max(64, min(MSSQL_WORKLOAD_NAME_MAX_CHARS, 4000))
    n = 0
    try:
        cur = conn.cursor()
        for start in range(0, total_records, batch_size):
            chunk = []
            for j in range(start, min(start + batch_size, total_records)):
                i = seq_base + j
                name = (f"wl-{run_id}-{i}|{pad}")[:lim]
                chunk.append((run_id, i, name))
            cur.executemany(
                "INSERT INTO dbo.hub_workload_mssql (run_id, seq, name) VALUES (%s, %s, %s)",
                chunk,
            )
            n += len(chunk)
        conn.commit()
        return n, None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return n, str(e)
    finally:
        conn.close()


def fetch_workload_sample_mssql(run_id: str, limit: int) -> dict[str, Any]:
    conn, err = _mssql_connect()
    if conn is None:
        return {
            "ok": False,
            "error": err or "MSSQL connection unavailable.",
        }
    try:
        cur = conn.cursor()
        lim = max(1, min(int(limit), 500))
        cur.execute(
            f"SELECT TOP ({lim}) id, run_id, seq, name, created_at "
            "FROM dbo.hub_workload_mssql WHERE run_id = %s ORDER BY seq DESC",
            (run_id,),
        )
        cols = [c[0] for c in cur.description]
        rows = []
        for r in cur.fetchall():
            d = dict(zip(cols, r))
            for k, v in list(d.items()):
                if hasattr(v, "isoformat"):
                    d[k] = v.isoformat()
                elif hasattr(v, "as_tuple"):
                    d[k] = float(v)
            if d.get("name"):
                nm = d["name"]
                d["name"] = nm[:200] + ("…" if len(nm) > 200 else "")
            rows.append(d)
        return {"ok": True, "count": len(rows), "rows": rows}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def op_pipeline_mongo_to_postgres_and_kafka() -> dict[str, Any]:
    """
    Consume-from-Mongo pattern (here: synchronous pull). Writes mirror rows in Postgres,
    emits Kafka events, mirrors same payload to OpenSearch (stand-in for Kafka→OS sink).
    """
    cfg = get_runtime_config()
    mc = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=30_000)
    coll = mc["demo"][MONGO_COLL]
    docs = list(coll.find().sort("updated_at", -1).limit(80))
    mc.close()
    mirrored = 0
    mssql_ok = 0
    kafka_ok = 0
    mssql_conn, mssql_err = _mssql_connect()
    try:
        with httpx.Client(timeout=60.0) as hc:
            ensure_scenario_os_index(hc)
            with psycopg.connect(cfg.postgres_dsn) as conn:
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
                    if mssql_merge_catalog_row(
                        mssql_conn,
                        str(sku),
                        str(d.get("title", "")),
                        d.get("category"),
                        int(d.get("unit_price_cents", 0)),
                        int(d.get("stock_units", 0)),
                        str(d.get("_id", "")),
                        msg_key,
                    ):
                        mssql_ok += 1
                conn.commit()
                if mssql_conn is not None:
                    try:
                        mssql_conn.commit()
                    except Exception:
                        pass
    finally:
        if mssql_conn is not None:
            try:
                mssql_conn.close()
            except Exception:
                pass

    r = redis.from_url(cfg.redis_url, decode_responses=True)
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
    out: dict[str, Any] = {
        "ok": True,
        "mongo_docs_processed": len(docs),
        "postgres_rows_touched": mirrored,
        "mssql_rows_upserted": mssql_ok,
        "kafka_events_sent": kafka_ok,
        "opensearch_mirrored": mirrored,
        "note": "OpenSearch holds the same logical stream Connect would sink from Kafka. "
        "SQL Server mirror + Debezium CDC when mssql-publisher is configured.",
    }
    if mssql_conn is None and mssql_err and MSSQL_HOST and MSSQL_SA_PASSWORD:
        out["mssql_connect_error"] = mssql_err
    return out


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
    cfg = get_runtime_config()
    mc = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=30_000)
    coll = mc["demo"][MONGO_COLL]
    skus = [d["sku"] for d in coll.find({}, {"sku": 1}).limit(50)]
    mc.close()
    if len(skus) < 1:
        return {"ok": False, "error": "Seed Mongo catalog first (Scenario → Seed)."}
    random.shuffle(skus)
    pick = skus[: min(lines_count, len(skus))]
    lines = []
    total = 0
    with psycopg.connect(cfg.postgres_dsn) as conn:
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

    tier = random.choices(["standard", "plus", "vip"], weights=[72, 22, 6], k=1)[0]
    pm = fake.random_element(["card_visa", "card_mc", "wallet", "bnpl"])
    processor_ref = f"PAY-{uuid.uuid4().hex[:12].upper()}"

    with psycopg.connect(cfg.postgres_dsn) as conn:
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
        conn.execute(
            """
            INSERT INTO scenario_customers AS sc
              (customer_email, customer_name, loyalty_tier, orders_placed, lifetime_cents, updated_at)
            VALUES (%s, %s, %s, 1, %s, NOW())
            ON CONFLICT (customer_email) DO UPDATE SET
              customer_name = EXCLUDED.customer_name,
              orders_placed = sc.orders_placed + 1,
              lifetime_cents = sc.lifetime_cents + EXCLUDED.lifetime_cents,
              updated_at = NOW()
            """,
            (ce, cn, tier, total),
        )
        conn.execute(
            """
            INSERT INTO scenario_payments
              (order_ref, payment_method, amount_cents, status, processor_ref)
            VALUES (%s, %s, %s, 'captured', %s)
            """,
            (order_ref, pm, total, processor_ref),
        )
        conn.commit()

    detail = json.dumps(
        {
            "lines": lines,
            "total_cents": total,
            "ship_lat": slat,
            "ship_lon": slon,
            "ship_label": slab,
            "payment_method": pm,
            "processor_ref": processor_ref,
            "loyalty_tier": tier,
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
        "payment_method": pm,
        "processor_ref": processor_ref,
        "loyalty_tier": tier,
    }
    sent = _producer_send(TOPIC_ORDERS, order_ref, payload)
    with httpx.Client(timeout=30.0) as hc:
        ensure_scenario_os_index(hc)
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

    r = redis.from_url(cfg.redis_url, decode_responses=True)
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
                "payment_method": pm,
                "processor_ref": processor_ref,
                "loyalty_tier": tier,
            }
        ),
    )
    r.hset(
        f"scenario:customer:{ce}",
        mapping={
            "name": cn,
            "loyalty_tier": tier,
            "last_order_ref": order_ref,
            "last_processor_ref": processor_ref,
        },
    )
    r.expire(f"scenario:customer:{ce}", 86400)
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
        "payment_method": pm,
        "processor_ref": processor_ref,
        "loyalty_tier": tier,
        "kafka_sent": sent,
    }


def op_write_cassandra_timeline(
    session: CassandraSession, order_ref: str, event_type: str, detail: str
) -> None:
    ks = get_runtime_config().cassandra_keyspace
    session.execute(
        f"""
        INSERT INTO {ks}.scenario_timeline (order_ref, event_ts, event_type, detail)
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
    cfg = get_runtime_config()
    with psycopg.connect(cfg.postgres_dsn) as conn:
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
                ensure_scenario_os_index(hc)
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

    r = redis.from_url(cfg.redis_url, decode_responses=True)
    _redis_refresh_summary(r)
    return {
        "ok": True,
        "orders_fulfilled": len(pending),
        "fulfillment_lines_inserted": n,
    }


def op_pipeline_fulfilled_to_shipments(
    cassandra_session: CassandraSession | None,
    *,
    batch_limit: int = 15,
) -> dict[str, Any]:
    """
    Orders that already have fulfillment lines but no shipment row get a label + carrier +
    tracking id; events go to Kafka, OpenSearch, Cassandra timeline + carrier table, Redis list.
    """
    kafka_ok = 0
    ks = get_runtime_config().cassandra_keyspace
    carriers = ["UPS", "FedEx", "DHL", "USPS"]
    shipped: list[dict[str, Any]] = []
    cfg = get_runtime_config()

    with psycopg.connect(cfg.postgres_dsn) as conn:
        ensure_postgres_scenario_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.order_ref FROM scenario_orders o
                WHERE EXISTS (
                  SELECT 1 FROM scenario_fulfillment_lines f WHERE f.order_ref = o.order_ref
                )
                AND NOT EXISTS (
                  SELECT 1 FROM scenario_shipments s WHERE s.order_ref = o.order_ref
                )
                ORDER BY o.id DESC
                LIMIT %s
                """,
                (batch_limit,),
            )
            refs = [row[0] for row in cur.fetchall()]

        with httpx.Client(timeout=30.0) as hc:
            ensure_scenario_os_index(hc)
            for order_ref in refs:
                carrier = random.choice(carriers)
                tracking_id = f"TRK-{uuid.uuid4().hex[:14].upper()}"
                conn.execute(
                    """
                    INSERT INTO scenario_shipments (order_ref, carrier, tracking_id, status)
                    VALUES (%s, %s, %s, 'label_created')
                    """,
                    (order_ref, carrier, tracking_id),
                )
                payload = {
                    "action": "shipment.label_created",
                    "order_ref": order_ref,
                    "carrier": carrier,
                    "tracking_id": tracking_id,
                }
                if _producer_send(TOPIC_SHIPMENTS, tracking_id, payload):
                    kafka_ok += 1
                _mirror_to_opensearch(
                    hc,
                    topic=TOPIC_SHIPMENTS,
                    key=tracking_id,
                    direction="postgres→kafka+os",
                    payload=payload,
                )
                if cassandra_session:
                    op_write_cassandra_timeline(
                        cassandra_session,
                        order_ref,
                        "SHIPMENT_LABELED",
                        json.dumps(payload)[:4000],
                    )
                    cassandra_session.execute(
                        f"""
                        INSERT INTO {ks}.scenario_carrier_shipments
                          (carrier, tracking_id, order_ref, created_ts)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            carrier,
                            tracking_id,
                            order_ref,
                            datetime.now(timezone.utc),
                        ),
                    )
                shipped.append(payload)
        conn.commit()

    r = redis.from_url(cfg.redis_url, decode_responses=True)
    for p in shipped:
        r.lpush(REDIS_SHIPMENTS_RECENT, json.dumps(p, default=str))
    r.ltrim(REDIS_SHIPMENTS_RECENT, 0, 99)
    _redis_refresh_summary(r)

    return {
        "ok": True,
        "shipments_created": len(shipped),
        "kafka_events_sent": kafka_ok,
        "samples": shipped[:5],
        "topic": TOPIC_SHIPMENTS,
    }


def fetch_view_postgres() -> dict[str, Any]:
    out: dict[str, Any] = {"tables": {}}
    cfg = get_runtime_config()
    with psycopg.connect(cfg.postgres_dsn) as conn:
        ensure_postgres_scenario_schema(conn)
        with conn.cursor() as cur:
            for name, sql in [
                ("scenario_catalog_mirror", "SELECT * FROM scenario_catalog_mirror ORDER BY id DESC LIMIT 25"),
                ("scenario_orders", "SELECT * FROM scenario_orders ORDER BY id DESC LIMIT 25"),
                ("scenario_fulfillment_lines", "SELECT * FROM scenario_fulfillment_lines ORDER BY id DESC LIMIT 25"),
                ("scenario_customers", "SELECT * FROM scenario_customers ORDER BY updated_at DESC LIMIT 25"),
                ("scenario_payments", "SELECT * FROM scenario_payments ORDER BY id DESC LIMIT 25"),
                ("scenario_shipments", "SELECT * FROM scenario_shipments ORDER BY id DESC LIMIT 25"),
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
    cfg = get_runtime_config()
    mc = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=30_000)
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
    sup_cur = (
        mc["demo"][MONGO_COLL_SUPPLIERS]
        .find({}, {"_id": 1, "supplier_code": 1, "company": 1, "region": 1, "rating": 1, "lead_time_days": 1})
        .sort("_id", -1)
        .limit(25)
    )
    suppliers = []
    for d in sup_cur:
        d["_id"] = str(d["_id"])
        suppliers.append(d)
    mc.close()
    return {
        "collection": f"demo.{MONGO_COLL}",
        "documents": docs,
        "collection_suppliers": f"demo.{MONGO_COLL_SUPPLIERS}",
        "supplier_documents": suppliers,
    }


def fetch_view_redis() -> dict[str, Any]:
    cfg = get_runtime_config()
    r = redis.from_url(cfg.redis_url, decode_responses=True)
    dash = r.get(REDIS_DASH_KEY)
    recent = r.lrange(REDIS_KAFKA_RECENT, 0, 19)
    ship_recent = r.lrange(REDIS_SHIPMENTS_RECENT, 0, 14)
    keys = r.keys("scenario:order:latest:*")[:15]
    cust_keys = r.keys("scenario:customer:*")[:12]
    customers = []
    for ck in cust_keys:
        h = r.hgetall(ck)
        if h:
            customers.append({"key": ck, "profile": h})
    shipments_recent_parsed = []
    for x in ship_recent or []:
        try:
            shipments_recent_parsed.append(json.loads(x))
        except json.JSONDecodeError:
            shipments_recent_parsed.append({"raw": x[:200]})
    return {
        "dashboard_summary": json.loads(dash) if dash else None,
        "recent_pipeline_events": [json.loads(x) for x in recent] if recent else [],
        "recent_shipment_events": shipments_recent_parsed,
        "cached_latest_orders_sample": orders,
        "customer_profile_hashes_sample": customers,
    }


def fetch_view_cassandra(session: CassandraSession) -> dict[str, Any]:
    ks = get_runtime_config().cassandra_keyspace
    rows = session.execute(
        f"SELECT order_ref, event_ts, event_type, detail FROM {ks}.scenario_timeline "
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
    csr = session.execute(
        f"SELECT carrier, tracking_id, order_ref, created_ts FROM {ks}.scenario_carrier_shipments "
        "LIMIT 40 ALLOW FILTERING"
    )
    carriers_out = []
    for row in csr:
        carriers_out.append(
            {
                "carrier": row.carrier,
                "tracking_id": row.tracking_id,
                "order_ref": row.order_ref,
                "created_ts": row.created_ts.isoformat() if row.created_ts else None,
            }
        )
    return {
        "table": f"{ks}.scenario_timeline",
        "rows": out,
        "timeline": {"table": f"{ks}.scenario_timeline", "rows": out},
        "carrier_shipments": {
            "table": f"{ks}.scenario_carrier_shipments",
            "rows": carriers_out,
        },
    }


def fetch_view_opensearch() -> dict[str, Any]:
    cfg = get_runtime_config()
    query = {
        "size": 20,
        "sort": [{"ts": {"order": "desc"}}],
        "query": {"match_all": {}},
    }
    with httpx.Client(timeout=30.0) as hc:
        r = hc.post(
            f"{cfg.opensearch_url}/{cfg.scenario_opensearch_index}/_search",
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
    return {"index": cfg.scenario_opensearch_index, "hits": slim}


def fetch_view_kafka_meta() -> dict[str, Any]:
    p = _kafka_producer()
    return {
        "bootstrap": KAFKA_BOOTSTRAP,
        "topics_emitted": [TOPIC_CATALOG, TOPIC_ORDERS, TOPIC_PIPELINE, TOPIC_SHIPMENTS],
        "producer_ready": p is not None,
        "hint": "This UI produces to Kafka and mirrors the same events into OpenSearch. "
        "Use kafka-console-consumer or Grafana Kafka dashboard to verify broker traffic.",
    }
