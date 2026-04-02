-- Extra dummy rows for demo_items. Safe to run multiple times (new id each time).
-- From host: psql "postgresql://demo:demopass@127.0.0.1:15432/demo" -f seed-dummy-data.sql
-- From Docker (docker compose cwd = dashboards/demo):
--   docker compose exec -T postgresql-primary bash -lc 'PGPASSWORD=demopass psql -U demo -d demo' < postgres-kafka/seed-dummy-data.sql

INSERT INTO demo_items (name)
SELECT format('bulk-%s-%s', i, to_char(clock_timestamp(), 'HH24MISS'))
FROM generate_series(1, 30) AS s(i);
