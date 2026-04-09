"""Parse Grafana dashboard JSON into Section/Panel models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .models import GridPos, Panel, Section

# Generated dashboards live one level up in ../grafana/generated-dashboards/
DASHBOARD_DIR = Path(__file__).resolve().parent.parent.parent / "grafana" / "generated-dashboards"
DEFAULT_DASHBOARD_PATH = DASHBOARD_DIR / "cassandra-condensed.json"


@dataclass
class DashboardInfo:
    """Metadata for a registered dashboard."""

    key: str          # CLI key used with --dashboard flag
    name: str         # human-readable name shown in picker
    path: Path        # absolute path to JSON file
    relative_path: str  # path for Claude prompts (relative to project root)


DASHBOARDS: list[DashboardInfo] = [
    DashboardInfo(
        key="cassandra",
        name="Cassandra Condensed",
        path=DASHBOARD_DIR / "cassandra-condensed.json",
        relative_path="../grafana/generated-dashboards/cassandra-condensed.json",
    ),
    DashboardInfo(
        key="system",
        name="System Metrics",
        path=DASHBOARD_DIR / "system-metrics.json",
        relative_path="../grafana/generated-dashboards/system-metrics.json",
    ),
    DashboardInfo(
        key="overview",
        name="Overview",
        path=DASHBOARD_DIR / "overview.json",
        relative_path="../grafana/generated-dashboards/overview.json",
    ),
    DashboardInfo(
        key="kafka",
        name="Kafka Cluster Overview",
        path=DASHBOARD_DIR / "kafka-cluster-overview.json",
        relative_path="../grafana/generated-dashboards/kafka-cluster-overview.json",
    ),
    DashboardInfo(
        key="redis",
        name="Redis Demo Overview",
        path=DASHBOARD_DIR / "redis-demo-overview.json",
        relative_path="../grafana/generated-dashboards/redis-demo-overview.json",
    ),
    DashboardInfo(
        key="mongodb",
        name="MongoDB TicTacToe Detailed",
        path=DASHBOARD_DIR / "mongodb-tictactoe-detailed.json",
        relative_path="../grafana/generated-dashboards/mongodb-tictactoe-detailed.json",
    ),
    DashboardInfo(
        key="postgres",
        name="Postgres Database",
        path=DASHBOARD_DIR / "postgres-database.json",
        relative_path="../grafana/generated-dashboards/postgres-database.json",
    ),
]


def get_dashboard_by_key(key: str) -> DashboardInfo | None:
    """Look up a dashboard by its CLI key."""
    return next((d for d in DASHBOARDS if d.key == key), None)


def _parse_panel(raw: dict) -> Panel:
    gp = raw.get("gridPos", {})
    return Panel(
        id=raw.get("id", 0),
        title=raw.get("title", "(untitled)"),
        panel_type=raw.get("type", "unknown"),
        description=raw.get("description", ""),
        grid_pos=GridPos(
            h=gp.get("h", 8),
            w=gp.get("w", 8),
            x=gp.get("x", 0),
            y=gp.get("y", 0),
        ),
    )


def parse_dashboard(path: Path | None = None) -> list[Section]:
    """Parse a Grafana dashboard JSON into a list of Sections.

    Handles two layout patterns:
    1. Modern: top-level panels array with type=row markers
    2. Legacy: top-level rows array (older Grafana format used by MCAC dashboards)
    """
    path = path or DEFAULT_DASHBOARD_PATH
    with open(path) as f:
        data = json.load(f)

    sections: list[Section] = []

    # --- Legacy format: top-level "rows" array ---
    if "rows" in data and data["rows"]:
        for row in data["rows"]:
            section = Section(
                id=row.get("id", 0),
                title=row.get("title", "(untitled)"),
            )
            for raw in row.get("panels", []):
                if raw.get("type") != "row":
                    section.panels.append(_parse_panel(raw))
            section.panels.sort(key=lambda p: (p.grid_pos.y, p.grid_pos.x))
            sections.append(section)
        return sections

    # --- Modern format: top-level "panels" array with row markers ---
    raw_panels: list[dict] = data.get("panels", [])
    current_section: Section | None = None

    for raw in raw_panels:
        if raw.get("type") == "row":
            if current_section is not None:
                sections.append(current_section)
            current_section = Section(
                id=raw.get("id", 0),
                title=raw.get("title", "(untitled)"),
            )
            for nested_raw in raw.get("panels", []):
                if nested_raw.get("type") != "row":
                    current_section.panels.append(_parse_panel(nested_raw))
        else:
            if current_section is not None:
                current_section.panels.append(_parse_panel(raw))

    if current_section is not None:
        sections.append(current_section)

    for section in sections:
        section.panels.sort(key=lambda p: (p.grid_pos.y, p.grid_pos.x))

    return sections
