from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GridPos:
    h: int = 8
    w: int = 8
    x: int = 0
    y: int = 0


@dataclass
class Panel:
    id: int
    title: str
    panel_type: str  # "timeseries", "stat", "row", etc.
    description: str = ""
    grid_pos: GridPos = field(default_factory=GridPos)


@dataclass
class Section:
    id: int
    title: str
    panels: list[Panel] = field(default_factory=list)
