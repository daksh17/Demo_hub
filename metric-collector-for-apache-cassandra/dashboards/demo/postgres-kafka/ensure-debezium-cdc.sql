-- Fix CDC on existing DBs (old volumes): publication + SELECT for `replicator` (Debezium connector user).
-- Run as app user `demo` (owns `demo_items`). From dashboards/demo:
--   docker compose exec -T postgresql-primary env PGPASSWORD=demopass psql -U demo -d demo -v ON_ERROR_STOP=1 -f - < postgres-kafka/ensure-debezium-cdc.sql

GRANT CONNECT ON DATABASE demo TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;
GRANT SELECT ON TABLE public.demo_items TO replicator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replicator;
ALTER DEFAULT PRIVILEGES FOR ROLE demo IN SCHEMA public GRANT SELECT ON TABLES TO replicator;

DO
$do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR TABLE public.demo_items;
  END IF;
END
$do$;
