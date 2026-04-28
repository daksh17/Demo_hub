#!/usr/bin/env python3
"""Optional: download SVG from public kroki.io (can 504 or return errors — do not overwrite tracked SVGs blindly).

Preferred: ./render-all.sh (mermaid-cli) in this directory.
"""
from __future__ import annotations

import sys
import zlib
import base64
import pathlib
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent


def kroki_get_url(mermaid_source: str) -> str:
    compressed = zlib.compress(mermaid_source.encode("utf-8"), 9)
    enc = base64.urlsafe_b64encode(compressed).decode("ascii").rstrip("=")
    return f"https://kroki.io/mermaid/svg/{enc}"


def fetch(mmd_name: str, svg_name: str) -> None:
    src = ROOT.joinpath(mmd_name).read_text(encoding="utf-8")
    url = kroki_get_url(src)
    req = urllib.request.Request(url, headers={"User-Agent": "mcac-demo-hub-diagram-fetch/1"})
    try:
        data = urllib.request.urlopen(req, timeout=120).read()
    except urllib.error.HTTPError as e:
        if e.code != 414:
            raise
        post = urllib.request.Request(
            "https://kroki.io/mermaid/svg",
            data=src.encode("utf-8"),
            headers={"User-Agent": "mcac-demo-hub-diagram-fetch/1", "Content-Type": "text/plain"},
            method="POST",
        )
        data = urllib.request.urlopen(post, timeout=120).read()
    out = ROOT.joinpath(svg_name)
    out.write_bytes(data)
    ok = b"Publisher" in data or b":14331" in data or b"SQL Server" in data
    print(f"wrote {out.name} ({len(data)} bytes) sql-server-ish={ok}")


def main() -> None:
    pairs = [
        ("00-component-context.mmd", "00-component-context.svg"),
        ("01-sequence-order-flow.mmd", "01-sequence-order-flow.svg"),
    ]
    for mmd, svg in pairs:
        fetch(mmd, svg)


if __name__ == "__main__":
    main()
