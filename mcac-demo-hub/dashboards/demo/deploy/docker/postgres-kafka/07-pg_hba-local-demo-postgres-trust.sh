#!/usr/bin/env bash
# Demo only: Unix-socket access to database "demo" as user "postgres" without a password.
# Bitnami often has BOTH /opt/.../conf/pg_hba.conf (image layout) and the real file under
# /bitnami/postgresql/data/pg_hba.conf (what the server uses). We must patch every distinct
# canonical path, preferring the data directory first when listing candidates.
# Kubernetes: also invoked from lifecycle.postStart (see gen_demo_hub_k8s.py).
set -euo pipefail
: "${PGDATA:-/bitnami/postgresql/data}"

marker="# MCAC_DEMO: local socket postgres@demo trust"
line="local   demo   postgres   trust"

prepend_one() {
  local HBA="$1"
  grep -qF "$marker" "$HBA" 2>/dev/null && return 0
  local tmp="${HBA}.mcac.new"
  { printf '%s\n%s\n' "$marker" "$line"; cat "$HBA"; } >"$tmp" && mv "$tmp" "$HBA"
}

canon_paths() {
  local f c
  for f in \
    "/bitnami/postgresql/data/pg_hba.conf" \
    "${PGDATA}/pg_hba.conf" \
    "/bitnami/postgresql/conf/pg_hba.conf" \
    "/opt/bitnami/postgresql/conf/pg_hba.conf"
  do
    [ -f "$f" ] || continue
    c="$(readlink -f "$f" 2>/dev/null || echo "$f")"
    [ -f "$c" ] && printf '%s\n' "$c"
  done | sort -u
}

main() {
  local HBA
  while IFS= read -r HBA; do
    [ -n "$HBA" ] || continue
    prepend_one "$HBA" || true
  done < <(canon_paths)
}

main
