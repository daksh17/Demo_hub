#!/usr/bin/env bash
# List all KV v2 secret paths under the demo-hub Vault (recursive metadata walk).
# Defaults to http://127.0.0.1:8200 — ignores corporate VAULT_ADDR unless DEMO_HUB_RESPECT_GLOBAL_VAULT=1.
#
# Usage:
#   ./deploy/k8s/scripts/vault-list-secrets.sh
#   DEMO_HUB_VAULT_ADDR=https://other:8200 ./deploy/k8s/scripts/vault-list-secrets.sh --prefix demo-hub
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_HUB_VAULT_ADDR_DEFAULT="http://127.0.0.1:8200"
DEMO_HUB_VAULT_TOKEN_DEFAULT="demo-hub-dev-root"
if [[ "${DEMO_HUB_RESPECT_GLOBAL_VAULT:-}" == "1" ]]; then
  export VAULT_ADDR="${DEMO_HUB_VAULT_ADDR:-${VAULT_ADDR:-$DEMO_HUB_VAULT_ADDR_DEFAULT}}"
  export VAULT_TOKEN="${DEMO_HUB_VAULT_TOKEN:-${VAULT_TOKEN:-$DEMO_HUB_VAULT_TOKEN_DEFAULT}}"
else
  export VAULT_ADDR="${DEMO_HUB_VAULT_ADDR:-$DEMO_HUB_VAULT_ADDR_DEFAULT}"
  export VAULT_TOKEN="${DEMO_HUB_VAULT_TOKEN:-$DEMO_HUB_VAULT_TOKEN_DEFAULT}"
fi
exec python3 "${SCRIPT_DIR}/vault-recurse-list.py" "$@"
