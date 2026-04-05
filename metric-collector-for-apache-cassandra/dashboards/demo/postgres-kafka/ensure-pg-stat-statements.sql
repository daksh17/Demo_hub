-- Run on primary **database postgres** as superuser, **after** the server loads `pg_stat_statements`
-- (`shared_preload_libraries` / POSTGRESQL_EXTRA_FLAGS) and has been restarted once if you changed it.
-- From dashboards/demo:
--   docker compose exec -T postgresql-primary env PGPASSWORD=postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 -f - < postgres-kafka/ensure-pg-stat-statements.sql

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
