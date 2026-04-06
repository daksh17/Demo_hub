-- Hub Multi-DB scenario: relational mirror + orders + fulfillment (database "demo").
-- Keep in sync with realtime-orders-search-hub/demo-ui/scenario.py:
--   ensure_postgres_scenario_schema + _ensure_postgres_scenario_indexes
-- Idempotent: safe for initdb and for `psql -f` on an existing DB.

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
);

CREATE TABLE IF NOT EXISTS scenario_orders (
  id SERIAL PRIMARY KEY,
  order_ref TEXT NOT NULL UNIQUE,
  customer_email TEXT NOT NULL,
  customer_name TEXT,
  lines JSONB NOT NULL,
  total_cents INT NOT NULL,
  pipeline_stage TEXT NOT NULL DEFAULT 'placed',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scenario_fulfillment_lines (
  id SERIAL PRIMARY KEY,
  order_ref TEXT NOT NULL REFERENCES scenario_orders(order_ref) ON DELETE CASCADE,
  sku TEXT NOT NULL,
  qty INT NOT NULL,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.scenario_catalog_mirror OWNER TO demo;
ALTER TABLE public.scenario_orders OWNER TO demo;
ALTER TABLE public.scenario_fulfillment_lines OWNER TO demo;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.scenario_catalog_mirror TO demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.scenario_orders TO demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.scenario_fulfillment_lines TO demo;
GRANT SELECT ON TABLE public.scenario_catalog_mirror TO replicator;
GRANT SELECT ON TABLE public.scenario_orders TO replicator;
GRANT SELECT ON TABLE public.scenario_fulfillment_lines TO replicator;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_scenario_catalog_category_price ON scenario_catalog_mirror (category, unit_price_cents, sku);
CREATE INDEX IF NOT EXISTS idx_scenario_catalog_updated_at ON scenario_catalog_mirror (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_scenario_catalog_brin_updated ON scenario_catalog_mirror USING BRIN (updated_at);
CREATE INDEX IF NOT EXISTS idx_scenario_catalog_in_stock ON scenario_catalog_mirror (category, sku) WHERE stock_units > 0;
CREATE INDEX IF NOT EXISTS idx_scenario_catalog_title_trgm ON scenario_catalog_mirror USING GIST (title gist_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_scenario_catalog_kafka_key_hash ON scenario_catalog_mirror USING HASH (kafka_msg_key);

CREATE INDEX IF NOT EXISTS idx_scenario_orders_stage_created ON scenario_orders (pipeline_stage, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scenario_orders_email ON scenario_orders (customer_email);
CREATE INDEX IF NOT EXISTS idx_scenario_orders_created_brin ON scenario_orders USING BRIN (created_at);
CREATE INDEX IF NOT EXISTS idx_scenario_orders_placed_partial ON scenario_orders (created_at DESC) WHERE pipeline_stage = 'placed';
CREATE INDEX IF NOT EXISTS idx_scenario_orders_lines_gin ON scenario_orders USING GIN (lines jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_scenario_orders_stage_cover ON scenario_orders (pipeline_stage) INCLUDE (order_ref, total_cents);

CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_order_ref ON scenario_fulfillment_lines (order_ref);
CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_sku ON scenario_fulfillment_lines (sku);
CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_order_sku ON scenario_fulfillment_lines (order_ref, sku);
CREATE INDEX IF NOT EXISTS idx_scenario_fulfill_brin_created ON scenario_fulfillment_lines USING BRIN (created_at);
