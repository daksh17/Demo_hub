-- Idempotent: ensure application role `demo` / `demopass` exists (older primary volumes).
-- Run as superuser from dashboards/demo:
--   docker compose exec -T postgresql-primary env PGPASSWORD=postgres psql -U postgres -d demo -v ON_ERROR_STOP=1 -f - < postgres-kafka/ensure-demo-app-user.sql

DO
$do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'demo') THEN
    CREATE USER demo WITH PASSWORD 'demopass';
  ELSE
    ALTER USER demo WITH PASSWORD 'demopass';
  END IF;
END
$do$;

GRANT CONNECT ON DATABASE demo TO demo;
GRANT USAGE, CREATE ON SCHEMA public TO demo;
GRANT pg_monitor TO demo;

ALTER TABLE IF EXISTS public.demo_items OWNER TO demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.demo_items TO demo;

ALTER DEFAULT PRIVILEGES FOR ROLE demo IN SCHEMA public GRANT SELECT ON TABLES TO replicator;
