-- Runs on first boot of the Bitnami primary (database "demo").
-- CDC uses the existing replication user `replicator` (Bitnami) + pgoutput publication.
-- Avoids a separate `debezium` login that init cannot fix on old volumes.

GRANT CONNECT ON DATABASE demo TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replicator;

CREATE TABLE IF NOT EXISTS demo_items (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.demo_items OWNER TO demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.demo_items TO demo;

CREATE PUBLICATION dbz_publication FOR TABLE public.demo_items;

-- Seed rows for demos and Grafana/Prometheus smoke tests (CDC will see these on fresh volumes).
INSERT INTO demo_items (name) VALUES
  ('Acme Corporation'),
  ('Globex Industries'),
  ('Initech'),
  ('Umbrella Labs'),
  ('Stark Industries'),
  ('Wayne Enterprises'),
  ('Hooli'),
  ('Prestige Worldwide'),
  ('Vehement Capital Partners'),
  ('Demo seed row');
