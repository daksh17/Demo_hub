#!/usr/bin/env bash
# Regenerate SVGs from .mmd (requires Node + npx). Run from repo root or this directory.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
CLI="${MERMAID_CLI:-npx --yes @mermaid-js/mermaid-cli@11.4.0}"
for f in 00-component-context 01-sequence-order-flow 02-flowchart-postgres-path \
         03-flowchart-mongo-path 04-flowchart-cassandra-redis-os 05-flowchart-mssql-path \
         06-flowchart-multi-db-faker-connect-overview 07-flowchart-oracle-path; do
  echo "Rendering ${f}.mmd -> ${f}.svg"
  if [[ "${f}" == "06-flowchart-multi-db-faker-connect-overview" ]]; then
    $CLI -i "${f}.mmd" -o "${f}.svg" -b transparent -w 2400
  else
    $CLI -i "${f}.mmd" -o "${f}.svg" -b transparent
  fi
done
echo "Done."
