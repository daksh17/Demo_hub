#!/usr/bin/env bash
# Apply demo_hub.scenario_timeline + event_type index on a running Cassandra node (existing cluster).
# Usage: from dashboards/demo — ./cassandra/apply-scenario-hub-schema.sh
# Uses the first Cassandra service; schema propagates to the ring.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CQL="${ROOT}/cassandra/ensure-scenario-hub.cql"
cd "${ROOT}"
docker compose exec -i -T cassandra cqlsh localhost 9042 < "${CQL}"
echo "Cassandra demo_hub scenario_timeline + index applied"
