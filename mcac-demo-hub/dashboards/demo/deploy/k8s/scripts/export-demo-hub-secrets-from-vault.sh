#!/usr/bin/env bash
# Export shell variables from Vault KV (demo-hub paths) for use with connector registration scripts.
# Usage: source deploy/k8s/scripts/export-demo-hub-secrets-from-vault.sh
#
# Defaults target the **local** demo-hub Vault (port-forward :8200). Corporate shells often export
# VAULT_ADDR / VAULT_TOKEN for a shared Vault — those are IGNORED here unless you set
#   DEMO_HUB_RESPECT_GLOBAL_VAULT=1
# Override explicitly: DEMO_HUB_VAULT_ADDR=https://... DEMO_HUB_VAULT_TOKEN=...
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
fi

DEMO_HUB_VAULT_ADDR_DEFAULT="http://127.0.0.1:8200"
DEMO_HUB_VAULT_TOKEN_DEFAULT="demo-hub-dev-root"
if [[ "${DEMO_HUB_RESPECT_GLOBAL_VAULT:-}" == "1" ]]; then
  VAULT_ADDR="${DEMO_HUB_VAULT_ADDR:-${VAULT_ADDR:-$DEMO_HUB_VAULT_ADDR_DEFAULT}}"
  VAULT_TOKEN="${DEMO_HUB_VAULT_TOKEN:-${VAULT_TOKEN:-$DEMO_HUB_VAULT_TOKEN_DEFAULT}}"
else
  VAULT_ADDR="${DEMO_HUB_VAULT_ADDR:-$DEMO_HUB_VAULT_ADDR_DEFAULT}"
  VAULT_TOKEN="${DEMO_HUB_VAULT_TOKEN:-$DEMO_HUB_VAULT_TOKEN_DEFAULT}"
fi
MOUNT_PATH="${VAULT_KV_MOUNT:-secret}"

_vault_kv_get_json() {
  VAULT_ADDR="$VAULT_ADDR" VAULT_TOKEN="$VAULT_TOKEN" MOUNT_PATH="$MOUNT_PATH" VAULT_SECRET_PATH="$1" python3 - <<'PY'
import json, os, urllib.request
addr = os.environ["VAULT_ADDR"].rstrip("/")
tok = os.environ["VAULT_TOKEN"]
mount = os.environ["MOUNT_PATH"].strip("/")
path = os.environ["VAULT_SECRET_PATH"].strip("/")
url = f"{addr}/v1/{mount}/data/{path}"
req = urllib.request.Request(url, headers={"X-Vault-Token": tok})
with urllib.request.urlopen(req, timeout=60) as r:
    print(r.read().decode())
PY
}

_field() {
  local json="$1"
  local key="$2"
  python3 -c "import json,sys; d=json.load(sys.stdin); print(d['data']['data'][sys.argv[1]])" "$key" <<<"$json"
}

echo "export-demo-hub-secrets-from-vault: reading ${VAULT_ADDR} ..." >&2
C=$(_vault_kv_get_json "demo-hub/credentials")
export DEMO_HUB_PG_PASSWORD="$(_field "$C" postgresql-password)"
export DEMO_HUB_PG_REPLICATION_PASSWORD="$(_field "$C" postgresql-replication-password)"
export DEMO_HUB_REDIS_PASSWORD="$(_field "$C" redis-password)"
export DEMO_HUB_DEMO_USER_PASSWORD="$(_field "$C" demo-user-password)"

PGS=$(_vault_kv_get_json "demo-hub/kafka-connect/pg-source")
export PG_SOURCE_DB_HOST="$(_field "$PGS" database.hostname)"
export PG_SOURCE_DB_PORT="$(_field "$PGS" database.port)"
export PG_SOURCE_DB_USER="$(_field "$PGS" database.user)"
export PG_SOURCE_DB_PASSWORD="$(_field "$PGS" database.password)"
export PG_SOURCE_DB_NAME="$(_field "$PGS" database.dbname)"

JDBC=$(_vault_kv_get_json "demo-hub/kafka-connect/jdbc-sink")
export JDBC_SINK_URL="$(_field "$JDBC" connection.url)"
export JDBC_SINK_USER="$(_field "$JDBC" connection.username)"
export JDBC_SINK_PASSWORD="$(_field "$JDBC" connection.password)"

MSRC=$(_vault_kv_get_json "demo-hub/kafka-connect/mongo-source")
export MONGO_SOURCE_URI="$(_field "$MSRC" mongodb.connection.string)"

MSNK=$(_vault_kv_get_json "demo-hub/kafka-connect/mongo-sink")
export MONGO_SINK_URI="$(_field "$MSNK" connection.uri)"
export MONGO_SINK_DATABASE="$(_field "$MSNK" database)"
export MONGO_SINK_COLLECTION="$(_field "$MSNK" collection)"

echo "export-demo-hub-secrets-from-vault: done (PG_*, JDBC_*, MONGO_* exported)." >&2
