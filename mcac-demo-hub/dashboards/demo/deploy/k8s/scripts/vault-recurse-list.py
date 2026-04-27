#!/usr/bin/env python3
"""Recursively list HashiCorp KV v2 paths under a mount (metadata tree).

Uses the Vault HTTP API only (stdlib). Default token matches demo-hub K8s dev Vault.

Examples:
  VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=demo-hub-dev-root \\
    ./vault-recurse-list.py

  ./vault-recurse-list.py --mount secret --prefix demo-hub

Paths printed are logical secret paths (e.g. demo-hub/credentials) suitable for:
  vault kv get -mount=secret demo-hub/credentials
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def list_dir(addr: str, token: str, mount: str, meta_subpath: str) -> list[str]:
    """LIST metadata keys under mount; meta_subpath is e.g. '' or 'demo-hub' or 'demo-hub/kafka-connect'."""
    mount = mount.strip("/")
    tail = meta_subpath.strip("/")
    path = f"{mount}/metadata"
    if tail:
        path = f"{path}/{tail}"
    url = f"{addr.rstrip('/')}/v1/{path}?list=true"
    req = urllib.request.Request(url, method="LIST", headers={"X-Vault-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code in (404, 405):
            return []
        raise
    return list(body.get("data", {}).get("keys") or [])


def walk(
    addr: str,
    token: str,
    mount: str,
    prefix: str,
    *,
    show_data_paths: bool,
) -> None:
    keys = list_dir(addr, token, mount, prefix)
    for k in sorted(keys):
        is_dir = k.endswith("/")
        name = k.rstrip("/")
        sub = f"{prefix}/{name}" if prefix else name
        if is_dir:
            walk(addr, token, mount, sub, show_data_paths=show_data_paths)
        else:
            children = list_dir(addr, token, mount, sub)
            if children:
                walk(addr, token, mount, sub, show_data_paths=show_data_paths)
            elif show_data_paths:
                print(f"{mount}/data/{sub}")
            else:
                print(sub)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--addr", default="", help="Vault API (default: env VAULT_ADDR or http://127.0.0.1:8200)")
    ap.add_argument("--token", default="", help="Token (default: env VAULT_TOKEN or demo-hub-dev-root)")
    ap.add_argument("--mount", default="secret", help="KV v2 mount path (default: secret)")
    ap.add_argument("--prefix", default="", help="Metadata subpath under mount (e.g. demo-hub)")
    ap.add_argument(
        "--full-paths",
        action="store_true",
        help="Print mount/data/... paths for direct API use",
    )
    args = ap.parse_args()
    addr = args.addr or __import__("os").environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
    token = args.token or __import__("os").environ.get("VAULT_TOKEN", "demo-hub-dev-root")
    try:
        walk(addr, token, args.mount, args.prefix.strip("/"), show_data_paths=args.full_paths)
    except urllib.error.URLError as e:
        print(f"vault-recurse-list: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
