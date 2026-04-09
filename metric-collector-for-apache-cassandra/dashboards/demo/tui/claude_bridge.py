"""Bridge to Claude Code CLI for AI-powered panel editing."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _find_claude() -> str:
    """Find the claude CLI binary."""
    path = shutil.which("claude")
    if path:
        return path
    raise FileNotFoundError(
        "claude CLI not found. Install it: https://docs.anthropic.com/en/docs/claude-code"
    )


_DEFAULT_DASHBOARD_REL = "../grafana/generated-dashboards/cassandra-condensed.json"


def build_edit_prompt(
    panel_id: int,
    panel_title: str,
    user_prompt: str,
    dashboard_path: str = _DEFAULT_DASHBOARD_REL,
) -> str:
    return (
        f"In {dashboard_path}, modify the panel with id={panel_id} "
        f"titled '{panel_title}'. "
        f"User request: {user_prompt}"
    )


def build_add_panel_prompt(
    section_title: str,
    user_prompt: str,
    dashboard_path: str = _DEFAULT_DASHBOARD_REL,
) -> str:
    return (
        f"Add a new panel to the '{section_title}' section in "
        f"{dashboard_path}. "
        f"User request: {user_prompt}"
    )


def build_add_section_prompt(
    user_prompt: str,
    dashboard_path: str = _DEFAULT_DASHBOARD_REL,
) -> str:
    return (
        f"Add a new section/tab to {dashboard_path}. "
        f"User request: {user_prompt}"
    )


def _parse_stream_event(line: str) -> str | None:
    """Parse a stream-json line and return displayable text, or None."""
    try:
        data = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return None

    event_type = data.get("type", "")

    if event_type == "assistant":
        message = data.get("message", {})
        content = message.get("content", [])
        texts = []
        for block in content:
            if block.get("type") == "text":
                texts.append(block["text"])
            elif block.get("type") == "tool_use":
                tool_name = block.get("name", "unknown")
                tool_input = block.get("input", {})
                file_path = tool_input.get("file_path", "")
                command = tool_input.get("command", "")
                if file_path:
                    texts.append(f"[Tool: {tool_name}] {file_path}")
                elif command:
                    texts.append(f"[Tool: {tool_name}] {command[:80]}")
                else:
                    texts.append(f"[Tool: {tool_name}]")
        if texts:
            return "\n".join(texts)

    elif event_type == "result":
        result_text = data.get("result", "")
        if result_text:
            return result_text

    return None


def run_claude_streaming(
    prompt: str,
    on_output: Callable[[str], None] | None = None,
) -> tuple[bool, str]:
    """Run Claude Code CLI with stream-json output, parsing events in real time.

    Designed to be called from a Textual thread worker so that on_output
    callbacks can safely use app.call_from_thread() to update the UI.

    Returns (success, full_output) tuple.
    """
    claude_bin = _find_claude()

    # Strip CLAUDECODE env var to allow nested invocation
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    proc = subprocess.Popen(
        [
            claude_bin,
            "--print",
            "--verbose",
            "--output-format", "stream-json",
            "--dangerously-skip-permissions",
            prompt,
        ],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    collected_text: list[str] = []
    last_assistant_text: str = ""
    assert proc.stdout is not None

    for raw_line in proc.stdout:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        display = _parse_stream_event(line)
        if display and display != last_assistant_text:
            if on_output:
                on_output(display)
            last_assistant_text = display
            collected_text.append(display)

    proc.wait()

    full_output = "\n".join(collected_text)
    assert proc.stderr is not None
    err_output = proc.stderr.read().strip()

    if proc.returncode == 0:
        return True, full_output or "Changes applied successfully."
    else:
        return False, err_output or full_output or "Claude Code returned an error."
