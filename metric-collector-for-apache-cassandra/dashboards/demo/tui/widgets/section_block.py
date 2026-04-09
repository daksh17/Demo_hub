"""Section container widget — header + 3-column grid of panels."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Grid
from textual.widget import Widget
from textual.widgets import Static

from ..models import Section
from .panel_cell import AddPanelCell, PanelCell


class SectionHeader(Widget):
    """Section title bar."""

    DEFAULT_CSS = """
    SectionHeader {
        width: 100%;
        height: 3;
        padding: 1 2;
        background: $primary-background;
    }
    SectionHeader .section-title {
        text-style: bold;
        width: 1fr;
    }
    """

    def __init__(self, title: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.title_text = title

    def compose(self) -> ComposeResult:
        yield Static(f"[b]{self.title_text}[/b]", markup=True, classes="section-title")


class PanelGrid(Grid):
    """3-column grid of panel cells."""

    DEFAULT_CSS = """
    PanelGrid {
        width: 100%;
        height: auto;
        grid-size: 3;
        grid-gutter: 1;
        padding: 0 1;
    }
    """


class SectionBlock(Widget):
    """A full section: header + grid of panels + add button."""

    DEFAULT_CSS = """
    SectionBlock {
        width: 100%;
        height: auto;
        margin-bottom: 1;
    }
    """

    def __init__(self, section: Section, **kwargs) -> None:
        super().__init__(**kwargs)
        self.section = section

    def compose(self) -> ComposeResult:
        yield SectionHeader(self.section.title)

        panels = self.section.panels
        cells: list[Widget] = [
            PanelCell(
                panel_id=p.id,
                panel_title=p.title,
                panel_type=p.panel_type,
                description=p.description,
            )
            for p in panels
        ]
        cells.append(AddPanelCell(self.section.id, self.section.title))

        yield PanelGrid(*cells)
