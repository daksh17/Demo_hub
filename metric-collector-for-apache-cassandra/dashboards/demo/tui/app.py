"""MCAC Dashboard TUI — interactive panel editor."""

from __future__ import annotations

import argparse

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import Button, Footer, Header, Static

from .claude_bridge import (
    build_add_panel_prompt,
    build_add_section_prompt,
    build_edit_prompt,
)
from .dashboard_parser import (
    DASHBOARDS,
    DashboardInfo,
    get_dashboard_by_key,
    parse_dashboard,
)
from .widgets.dashboard_picker import DashboardPicker
from .widgets.panel_cell import AddPanelCell, PanelCell
from .widgets.prompt_modal import PromptModal, PromptResult
from .widgets.section_block import SectionBlock
from .widgets.working_modal import WorkingModal, WorkingResult


class DashboardApp(App):
    """Interactive TUI for Grafana dashboard editing."""

    CSS_PATH = "styles/app.tcss"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh_dashboard", "Refresh"),
        ("d", "switch_dashboard", "Switch Dashboard"),
    ]

    def __init__(self, dashboard_info: DashboardInfo | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._dashboard_info: DashboardInfo | None = dashboard_info

    # --- Lifecycle ---

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="dashboard-scroll"):
            with Horizontal(id="add-section-row"):
                yield Button(
                    "+ New Section",
                    variant="default",
                    id="add-section-btn",
                )
        yield Static("Select a dashboard to begin", id="status-bar")
        yield Footer()

    def on_mount(self) -> None:
        if self._dashboard_info is not None:
            self._apply_dashboard(self._dashboard_info)
        else:
            self.push_screen(DashboardPicker(), callback=self._on_dashboard_picked)

    # --- Dashboard selection ---

    def _apply_dashboard(self, info: DashboardInfo) -> None:
        self._dashboard_info = info
        self.title = info.name
        self.sub_title = f"Editing: {info.relative_path}"
        self.action_refresh_dashboard()

    def _on_dashboard_picked(self, result: DashboardInfo | None) -> None:
        if result is None:
            self.exit()
        else:
            self._apply_dashboard(result)

    def action_switch_dashboard(self) -> None:
        self.push_screen(DashboardPicker(), callback=self._on_dashboard_switched)

    def _on_dashboard_switched(self, result: DashboardInfo | None) -> None:
        if result is not None:
            self._apply_dashboard(result)

    # --- Properties ---

    @property
    def dashboard_path(self):
        assert self._dashboard_info is not None
        return self._dashboard_info.path

    @property
    def dashboard_relative_path(self) -> str:
        assert self._dashboard_info is not None
        return self._dashboard_info.relative_path

    # --- Helpers ---

    def _set_status(self, text: str) -> None:
        status = self.query_one("#status-bar", Static)
        status.update(text)

    # --- Panel editing ---

    @on(PanelCell.Selected)
    def handle_panel_selected(self, event: PanelCell.Selected) -> None:
        self.push_screen(
            PromptModal(
                action="edit_panel",
                title=f"Edit: {event.panel_title}",
                subtitle=f"Panel ID: {event.panel_id}",
                panel_id=event.panel_id,
                panel_title=event.panel_title,
            ),
            callback=self._on_prompt_result,
        )

    # --- Add panel ---

    @on(AddPanelCell.Clicked)
    def handle_add_panel(self, event: AddPanelCell.Clicked) -> None:
        self.push_screen(
            PromptModal(
                action="add_panel",
                title="Add New Panel",
                subtitle=f"Section: {event.section_title}",
                section_id=event.section_id,
                section_title=event.section_title,
            ),
            callback=self._on_prompt_result,
        )

    # --- Add section ---

    @on(Button.Pressed, "#add-section-btn")
    def handle_add_section(self) -> None:
        self.push_screen(
            PromptModal(
                action="add_section",
                title="Add New Section",
                subtitle="Creates a new row/section in the dashboard",
            ),
            callback=self._on_prompt_result,
        )

    # --- Process prompt result → launch working modal ---

    def _on_prompt_result(self, result: PromptResult | None) -> None:
        if result is None:
            return

        rel_path = self.dashboard_relative_path

        if result.action == "edit_panel":
            prompt = build_edit_prompt(
                result.panel_id, result.panel_title, result.prompt,
                dashboard_path=rel_path,
            )
        elif result.action == "add_panel":
            prompt = build_add_panel_prompt(
                result.section_title, result.prompt,
                dashboard_path=rel_path,
            )
        elif result.action == "add_section":
            prompt = build_add_section_prompt(
                result.prompt,
                dashboard_path=rel_path,
            )
        else:
            self._set_status(f"Unknown action: {result.action}")
            return

        self._set_status(f"Claude is working: {result.prompt[:60]}...")
        self.push_screen(
            WorkingModal(prompt=prompt),
            callback=self._on_working_result,
        )

    def _on_working_result(self, result: WorkingResult) -> None:
        if result.success:
            self._set_status("Done! Refreshing dashboard...")
            self.action_refresh_dashboard()
        else:
            self._set_status(f"Error: {result.output[:80]}")

    # --- Refresh ---

    def action_refresh_dashboard(self) -> None:
        """Re-parse the dashboard JSON and rebuild all section widgets."""
        if self._dashboard_info is None:
            return

        scroll = self.query_one("#dashboard-scroll", VerticalScroll)

        for block in self.query(SectionBlock):
            block.remove()

        sections = parse_dashboard(self.dashboard_path)
        add_row = self.query_one("#add-section-row")
        for section in sections:
            scroll.mount(SectionBlock(section), before=add_row)

        self._set_status("Ready — click a panel to edit, [+] to add")


def main():
    parser = argparse.ArgumentParser(description="MCAC Grafana Dashboard TUI Editor")
    parser.add_argument(
        "--dashboard", "-d",
        choices=[d.key for d in DASHBOARDS],
        help="Dashboard to open directly (skip picker)",
    )
    args = parser.parse_args()

    info = get_dashboard_by_key(args.dashboard) if args.dashboard else None
    app = DashboardApp(dashboard_info=info)
    app.run()


if __name__ == "__main__":
    main()
