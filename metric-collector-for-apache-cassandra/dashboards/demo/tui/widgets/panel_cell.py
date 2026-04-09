"""Clickable panel card widget."""

from __future__ import annotations

import re

from textual.message import Message
from textual.widget import Widget
from textual.widgets import Static


def _summarize(description: str) -> str:
    """Extract first sentence from a panel description."""
    if not description:
        return ""
    first_block = description.split("\n\n")[0].strip()
    match = re.match(r"^(.+?\.)\s", first_block)
    summary = match.group(1) if match else first_block
    summary = summary.replace("**", "")
    return summary


class PanelCell(Widget):
    """A clickable card representing a single Grafana panel."""

    DEFAULT_CSS = """
    PanelCell {
        width: 1fr;
        height: auto;
        min-height: 7;
        border: solid $surface-lighten-2;
        padding: 1 2;
        content-align: left top;
    }
    PanelCell:hover {
        border: solid $accent;
        background: $surface-lighten-1;
    }
    PanelCell:focus {
        border: double $accent;
        background: $surface-lighten-1;
    }
    PanelCell .panel-title {
        text-style: bold;
    }
    PanelCell .panel-type {
        color: $text-muted;
    }
    PanelCell .panel-desc {
        color: $text-muted;
        text-style: italic;
    }
    """

    can_focus = True

    class Selected(Message):
        """Emitted when a panel cell is clicked."""

        def __init__(self, panel_id: int, panel_title: str) -> None:
            super().__init__()
            self.panel_id = panel_id
            self.panel_title = panel_title

    def __init__(
        self,
        panel_id: int,
        panel_title: str,
        panel_type: str,
        description: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.panel_id = panel_id
        self.panel_title = panel_title
        self.panel_type = panel_type
        self.description = description

    def compose(self):
        yield Static(self.panel_title, classes="panel-title")
        yield Static(self.panel_type, classes="panel-type")
        summary = _summarize(self.description)
        if summary:
            yield Static(summary, classes="panel-desc")

    def on_click(self) -> None:
        self.post_message(self.Selected(self.panel_id, self.panel_title))

    def key_enter(self) -> None:
        self.post_message(self.Selected(self.panel_id, self.panel_title))


class AddPanelCell(Widget):
    """A clickable [+] cell to add a new panel."""

    DEFAULT_CSS = """
    AddPanelCell {
        width: 1fr;
        height: auto;
        min-height: 7;
        border: dashed $surface-lighten-2;
        padding: 1 2;
        content-align: center middle;
    }
    AddPanelCell:hover {
        border: dashed $accent;
        background: $surface-lighten-1;
    }
    AddPanelCell:focus {
        border: dashed $accent;
        background: $surface-lighten-1;
    }
    """

    can_focus = True

    class Clicked(Message):
        """Emitted when the add button is clicked."""

        def __init__(self, section_id: int, section_title: str) -> None:
            super().__init__()
            self.section_id = section_id
            self.section_title = section_title

    def __init__(self, section_id: int, section_title: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.section_id = section_id
        self.section_title = section_title

    def compose(self):
        yield Static("[b]+[/b]  Add Panel", markup=True)

    def on_click(self) -> None:
        self.post_message(self.Clicked(self.section_id, self.section_title))

    def key_enter(self) -> None:
        self.post_message(self.Clicked(self.section_id, self.section_title))
