"""
Whitelisted nodetool invocations: remote JMX from the hub, or kubectl exec into a Cassandra pod.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

_EXTRA_TOKEN = re.compile(r"^[a-zA-Z0-9_./\-]+$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

ALLOWED_COMMANDS: frozenset[str] = frozenset(
    {
        "status",
        "describecluster",
        "describe_ring",
        "info",
        "version",
        "ring",
        "netstats",
        "gossipinfo",
        "compactionstats",
        "compactionhistory",
        "tpstats",
        "tablestats",
        "cfstats",
        "listsnapshots",
        "viewbuildstatus",
        "proxyhistograms",
    }
)


def _split_extras(s: str | None) -> list[str]:
    if not s or not str(s).strip():
        return []
    parts = str(s).strip().split()
    out: list[str] = []
    for p in parts:
        if len(p) > 120 or not _EXTRA_TOKEN.match(p):
            raise ValueError(f"invalid extra argument token: {p!r}")
        out.append(p)
    if len(out) > 12:
        raise ValueError("too many extra arguments (max 12)")
    return out


def _validate_k8s_ident(name: str, field: str) -> str:
    n = name.strip()
    if not n or len(n) > 253:
        raise ValueError(f"invalid {field}")
    for part in n.split("."):
        if not part or len(part) > 63 or not _DNS_LABEL.match(part):
            raise ValueError(f"invalid {field}")
    return n


def resolve_transport() -> tuple[str, dict[str, str]]:
    ns = os.environ.get("CASSANDRA_NODETOOL_KUBECTL_NAMESPACE", "").strip()
    pod = os.environ.get("CASSANDRA_NODETOOL_KUBECTL_POD", "").strip()
    if ns and pod:
        cont = os.environ.get("CASSANDRA_NODETOOL_KUBECTL_CONTAINER", "").strip()
        if cont and (len(cont) > 63 or not _DNS_LABEL.match(cont)):
            raise ValueError("invalid CASSANDRA_NODETOOL_KUBECTL_CONTAINER")
        return "kubectl", {
            "namespace": _validate_k8s_ident(ns, "namespace"),
            "pod": _validate_k8s_ident(pod, "pod"),
            "container": cont,
        }
    return "remote", {}


def _default_timeout() -> float:
    return float(os.environ.get("CASSANDRA_NODETOOL_TIMEOUT_SEC", "120"))


def _validate_jmx_host(h: str) -> str:
    h = h.strip()
    if not h or len(h) > 253:
        raise ValueError("invalid jmx_host")
    if not re.match(r"^[a-zA-Z0-9.\-]+$", h):
        raise ValueError("invalid jmx_host")
    return h


def _remote_nodetool_env() -> dict[str, str]:
    """Stable env for bundled Cassandra 4 nodetool (JAVA_HOME + CASSANDRA_HOME + jvm*.options)."""
    env = os.environ.copy()
    env["CASSANDRA_HOME"] = "/opt/cassandra"
    if not env.get("JAVA_HOME", "").strip() and Path("/opt/jdk17").is_dir():
        env["JAVA_HOME"] = "/opt/jdk17"
    return env


def run_nodetool(
    cassandra_hosts: tuple[str, ...],
    command: str,
    extra_args: str | None = None,
    jmx_host: str | None = None,
) -> dict[str, Any]:
    cmd_lc = command.strip().lower()
    if cmd_lc not in ALLOWED_COMMANDS:
        raise ValueError(
            f"command not allowed: {command!r}. Allowed: {', '.join(sorted(ALLOWED_COMMANDS))}"
        )
    extras = _split_extras(extra_args)
    transport, meta = resolve_transport()
    timeout = _default_timeout()

    if transport == "kubectl":
        kc = shutil.which("kubectl")
        if not kc:
            raise RuntimeError(
                "kubectl not found in PATH. Install kubectl in the hub image or unset "
                "CASSANDRA_NODETOOL_KUBECTL_* to use remote JMX nodetool."
            )
        argv: list[str] = [
            kc,
            "exec",
            "-n",
            meta["namespace"],
            meta["pod"],
        ]
        cont = meta.get("container") or ""
        if cont:
            argv.extend(["-c", cont])
        argv.extend(["--", "nodetool", cmd_lc, *extras])
    else:
        nt = shutil.which("nodetool")
        if not nt:
            raise RuntimeError(
                "nodetool not found in PATH. Either bundle Cassandra tools in the hub image "
                "(default Dockerfile) or set CASSANDRA_NODETOOL_KUBECTL_NAMESPACE and "
                "CASSANDRA_NODETOOL_KUBECTL_POD to run nodetool inside a Cassandra pod."
            )
        jh = (jmx_host or os.environ.get("CASSANDRA_NODETOOL_JMX_HOST", "") or "").strip()
        if not jh:
            if not cassandra_hosts:
                raise ValueError(
                    "no JMX host: set jmx_host in the request, CASSANDRA_NODETOOL_JMX_HOST, "
                    "or CASSANDRA_HOSTS"
                )
            jh = cassandra_hosts[0]
        jh = _validate_jmx_host(jh)
        jp = int(os.environ.get("CASSANDRA_NODETOOL_JMX_PORT", "7199"))
        argv = [nt, "-h", jh, "-p", str(jp), cmd_lc, *extras]

    run_kw: dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "timeout": timeout,
    }
    if transport == "remote":
        run_kw["env"] = _remote_nodetool_env()

    proc = subprocess.run(argv, **run_kw)
    return {
        "transport": transport,
        "argv": argv,
        "returncode": proc.returncode,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
    }


def hints(cassandra_hosts: tuple[str, ...]) -> dict[str, Any]:
    transport, meta = resolve_transport()
    out: dict[str, Any] = {
        "transport": transport,
        "allowed_commands": sorted(ALLOWED_COMMANDS),
        "timeout_sec": _default_timeout(),
        "nodetool_on_path": bool(shutil.which("nodetool")),
        "kubectl_on_path": bool(shutil.which("kubectl")),
    }
    if transport == "kubectl":
        out["kubectl"] = {
            "namespace": meta["namespace"],
            "pod": meta["pod"],
            "container": meta.get("container") or None,
        }
    else:
        env_jmx = os.environ.get("CASSANDRA_NODETOOL_JMX_HOST", "").strip()
        default_jmx = env_jmx or (cassandra_hosts[0] if cassandra_hosts else None)
        out["remote_jmx"] = {
            "default_host": default_jmx,
            "port": int(os.environ.get("CASSANDRA_NODETOOL_JMX_PORT", "7199")),
            "cassandra_hosts": list(cassandra_hosts),
            "jvm11_clients_options_exists": Path(
                "/opt/cassandra/conf/jvm11-clients.options"
            ).is_file(),
            "java_home": os.environ.get("JAVA_HOME", "").strip()
            or ("/opt/jdk17" if Path("/opt/jdk17").is_dir() else None),
        }
    return out
