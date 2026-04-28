#!/usr/bin/env bash
# Optional: native transactional replication (often flaky in Docker). Safe to ignore failures.
set -u
PW="${MSSQL_SA_PASSWORD:?missing MSSQL_SA_PASSWORD}"
set +e
sqlcmd -S mssql-publisher -U sa -P "$PW" -C -b -i /scripts/05-transactional-replication-bootstrap.sql
code=$?
set -e
if [[ "$code" != 0 ]]; then
  echo "mssql: transactional replication bootstrap returned $code — continuing without it (CDC+Kafka still works)." >&2
fi
exit 0
