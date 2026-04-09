"""Startup modal for choosing which dashboard to edit."""

from __future__ import annotations

from textual import on
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label

from ..dashboard_parser import DASHBOARDS, DashboardInfo


class DashboardPicker(ModalScreen[DashboardInfo | None]):
    """Modal that lists available dashboards for selection."""

    DEFAULT_CSS = """
    DashboardPicker {
        align: center middle;
    }
    DashboardPicker #picker-container {
        width: 60;
        height: auto;
        border: thick $accent;
        background: $surface;
        padding: 2 3;
    }
    DashboardPicker #picker-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        padding-bottom: 1;
    }
    DashboardPicker .dashboard-btn {
        width: 100%;
        margin-bottom: 1;
    }
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def compose(self) -> ComposeResult:
        with Vertical(id="picker-container"):
            yield Label("Select a Dashboard", id="picker-title")
            for info in DASHBOARDS:
                yield Button(
                    info.name,
                    variant="primary",
                    id=f"pick-{info.key}",
                    classes="dashboard-btn",
                )

    @on(Button.Pressed)
    def handle_pick(self, event: Button.Pressed) -> None:
        btn_id = event.button.id or ""
        if btn_id.startswith("pick-"):
            key = btn_id[5:]
            match = next((d for d in DASHBOARDS if d.key == key), None)
            if match:
                self.dismiss(match)

    def action_cancel(self) -> None:
        self.dismiss(None)
