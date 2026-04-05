#!/usr/bin/env bash
# Empty sink tables populated by Kafka Connect JDBC + Mongo sink connectors
# (demo_items_from_kafka on Postgres, demo.demo_items_from_kafka on Mongo).
#
# Run from this directory with the stack up:
#   ./clean-kafka-connect-sinks.sh
#
# Afterward: generate *new* rows (hub Workload / Single order). Connect only forwards new
# changes from the WAL / change stream — it does not replay already‑acked history unless you
# reset connector offsets or re‑register with a fresh snapshot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "Dropping Postgres public.demo_items_from_kafka (if present) so JDBC sink can recreate schema …"
docker compose exec -T postgresql-primary env PGPASSWORD=demopass psql -U demo -d demo -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE IF EXISTS public.demo_items_from_kafka CASCADE;
SQL

echo "Deleting all docs in Mongo demo.demo_items_from_kafka …"
docker compose exec -T mongo-mongos1 mongosh demo --quiet --eval '
  const n = db.demo_items_from_kafka.deleteMany({}).deletedCount;
  print("deleted: " + n);
'

echo "Done. Sink tables are empty; new hub writes to demo_items / demo_items should appear in sinks after Connect processes topics."
