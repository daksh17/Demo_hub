"""Blocking progress modal shown while Claude Code is working."""

from __future__ import annotations

import time
from dataclasses import dataclass

from textual import work
from textual.app import ComposeResult
from textual.containers import Center, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, LoadingIndicator, RichLog

from ..claude_bridge import run_claude_streaming


@dataclass
class WorkingResult:
    """Result from the working modal."""
    success: bool
    output: str


class WorkingModal(ModalScreen[WorkingResult]):
    """Blocking modal that shows Claude's streaming output."""

    DEFAULT_CSS = """
    WorkingModal {
        align: center middle;
    }
    WorkingModal #working-container {
        width: 90;
        max-width: 95%;
        height: 30;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    WorkingModal #working-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        padding-bottom: 1;
    }
    WorkingModal #working-spinner {
        height: 1;
        padding-bottom: 1;
    }
    WorkingModal #working-log {
        width: 100%;
        height: 1fr;
        border: solid $surface-lighten-2;
        padding: 0 1;
    }
    WorkingModal #error-row {
        width: 100%;
        height: 3;
        align: center middle;
        display: none;
    }
    """

    def __init__(self, prompt: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self._finished = False
        self._error_output = ""

    def compose(self) -> ComposeResult:
        with Vertical(id="working-container"):
            yield Label("Claude is working\u2026", id="working-title")
            yield LoadingIndicator(id="working-spinner")
            yield RichLog(id="working-log", wrap=True, markup=True)
            with Center(id="error-row"):
                yield Button("Close", variant="error", id="close-btn")

    def on_mount(self) -> None:
        log = self.query_one("#working-log", RichLog)
        log.write("[bold]Prompt:[/bold]")
        log.write(self.prompt)
        log.write("")
        log.write("[dim]Waiting for Claude Code response\u2026[/dim]")
        log.write("")
        self._run_claude()

    @work(thread=True, exclusive=True)
    def _run_claude(self) -> None:
        """Run Claude in a background thread, streaming output to the log."""
        log = self.query_one("#working-log", RichLog)

        def on_output(line: str) -> None:
            self.app.call_from_thread(log.write, line)

        success, output = run_claude_streaming(self.prompt, on_output)
        self._finished = True

        if success:
            self.app.call_from_thread(log.write, "")
            self.app.call_from_thread(
                log.write, "[bold green]Done! Refreshing dashboard\u2026[/bold green]"
            )
            time.sleep(1.5)
            self.app.call_from_thread(self._finish_success, output)
        else:
            self.app.call_from_thread(self._finish_error, output)

    def _finish_success(self, output: str) -> None:
        self.dismiss(WorkingResult(success=True, output=output))

    def _finish_error(self, output: str) -> None:
        self.query_one("#working-title", Label).update("Error")
        self.query_one("#working-spinner", LoadingIndicator).display = False
        self.query_one("#error-row").display = True
        log = self.query_one("#working-log", RichLog)
        log.write("")
        log.write(f"[bold red]{output}[/bold red]")
        self._error_output = output

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-btn":
            self.dismiss(WorkingResult(success=False, output=self._error_output))

    def key_escape(self) -> None:
        if self._finished:
            self.dismiss(WorkingResult(success=False, output=self._error_output))
