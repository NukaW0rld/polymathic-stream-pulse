"""Generate the source-controlled three-page PBIR report definition.

The generated report intentionally binds only curated analytical tables. Run
this after changing page layout or model field names, then open the PBIP in
Power BI Desktop to refresh data and perform the required visual inspection.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parents[1]
PAGES = ROOT / "stream-pulse.Report" / "definition" / "pages"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
VISUAL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"


def uid(seed: str) -> str:
    return hashlib.md5(f"stream-pulse:{seed}".encode()).hexdigest()[:20]


def column(table: str, name: str) -> dict:
    return {
        "field": {"Column": {
            "Expression": {"SourceRef": {"Entity": table}}, "Property": name,
        }},
        "queryRef": f"{table}.{name}",
        "nativeQueryRef": name,
    }


def measure(table: str, name: str) -> dict:
    return {
        "field": {"Measure": {
            "Expression": {"SourceRef": {"Entity": table}}, "Property": name,
        }},
        "queryRef": f"{table}.{name}",
        "nativeQueryRef": name,
    }


def visual(seed: str, visual_type: str, x: int, y: int, width: int, height: int,
           query_state: dict | None = None, objects: dict | None = None,
           container_objects: dict | None = None, tab_order: int = 0) -> dict:
    result = {
        "$schema": VISUAL_SCHEMA,
        "name": uid(seed),
        "position": {
            "x": x, "y": y, "z": 1000, "width": width, "height": height,
            "tabOrder": tab_order,
        },
        "visual": {"visualType": visual_type, "drillFilterOtherVisuals": True},
    }
    if query_state:
        result["visual"]["query"] = {"queryState": query_state}
    if objects:
        result["visual"]["objects"] = objects
    if container_objects:
        result["visual"]["visualContainerObjects"] = container_objects
    return result


def textbox(seed: str, text: str, x: int, y: int, width: int, height: int,
            font_size: int = 18, color: str = "#172033") -> dict:
    return visual(
        seed, "textbox", x, y, width, height,
        objects={"general": [{"properties": {"paragraphs": [{"textRuns": [{
            "value": text,
            "textStyle": {
                "fontFamily": "Segoe UI Semibold",
                "fontSize": f"{font_size}px",
                "color": color,
            },
        }]}]}}]},
    )


def card(seed: str, table: str, metric: str, x: int, y: int, width: int, height: int) -> dict:
    return visual(seed, "cardVisual", x, y, width, height, {
        "Data": {"projections": [measure(table, metric)]},
    })


def slicer(seed: str, table: str, field: str, x: int, y: int, width: int, height: int) -> dict:
    return visual(seed, "slicer", x, y, width, height, {
        "Values": {"projections": [column(table, field)]},
    })


def line(seed: str, category_table: str, category: str,
         metrics: list[tuple[str, str]], x: int, y: int, width: int, height: int) -> dict:
    return visual(seed, "lineChart", x, y, width, height, {
        "Category": {"projections": [column(category_table, category)]},
        "Y": {"projections": [measure(table, metric) for table, metric in metrics]},
    })


def table(seed: str, fields: list[tuple[str, str, bool]], x: int, y: int,
          width: int, height: int) -> dict:
    projections = [measure(t, f) if is_measure else column(t, f) for t, f, is_measure in fields]
    return visual(seed, "tableEx", x, y, width, height, {
        "Values": {"projections": projections},
    })


def page(page_id: str, display_name: str, visuals: list[dict]) -> None:
    page_dir = PAGES / page_id
    (page_dir / "visuals").mkdir(parents=True)
    (page_dir / "page.json").write_text(json.dumps({
        "$schema": PAGE_SCHEMA,
        "name": page_id,
        "displayName": display_name,
        "displayOption": "FitToPage",
        "height": 720,
        "width": 1280,
        "annotations": [{"name": "purpose", "value": display_name}],
    }, indent=2) + "\n", encoding="utf-8")
    for item in visuals:
        visual_dir = page_dir / "visuals" / item["name"]
        visual_dir.mkdir()
        (visual_dir / "visual.json").write_text(
            json.dumps(item, indent=2) + "\n", encoding="utf-8",
        )


def build() -> None:
    page_ids = {
        "evolution": uid("page-stream-evolution"),
        "raid": uid("page-raid-impact"),
        "community": uid("page-community-comparison"),
    }
    for child in PAGES.iterdir():
        if child.is_dir():
            shutil.rmtree(child)

    common_header = [
        textbox("evolution-title", "Stream evolution · participation and evidence", 20, 14, 850, 48, 24),
        slicer("evolution-stream-slicer", "Stream", "started_at", 930, 12, 330, 70),
        card("evolution-quality", "Stream", "Selected Quality State", 20, 75, 300, 80),
        card("evolution-errors", "Quality Source", "Recorded Source Errors", 335, 75, 220, 80),
        card("evolution-gaps", "Quality Source", "Unresolved Gaps", 570, 75, 220, 80),
        line("evolution-viewers", "Stream Participation", "window_index",
             [("Stream Participation", "Average Viewers")], 20, 170, 610, 245),
        line("evolution-activity", "Stream Participation", "window_index",
             [("Stream Participation", "Messages"),
              ("Stream Participation", "Peak Active Chatters")], 650, 170, 610, 245),
        line("evolution-presence", "Stream Participation", "window_index",
             [("Stream Participation", "Average Presence")], 20, 430, 390, 245),
        line("evolution-concentration", "Stream Participation", "window_index",
             [("Stream Participation", "Average Top Five Share")], 430, 430, 390, 245),
        line("evolution-events", "Stream Events", "window_index",
             [("Stream Events", "Raid Events"),
              ("Stream Events", "Follow Events")], 840, 430, 420, 245),
    ]
    page(page_ids["evolution"], "Stream evolution", common_header)

    raid_visuals = [
        textbox("raid-title", "Raid impact · supported horizons only", 20, 14, 700, 48, 24),
        card("raid-quality", "Stream", "Selected Quality State", 740, 12, 170, 70),
        slicer("raid-stream-slicer", "Stream", "started_at", 930, 12, 330, 70),
        card("raid-supported", "Raid Impact", "Supported Raid Horizons", 20, 78, 290, 85),
        card("raid-change", "Raid Impact", "Average Viewer Change", 330, 78, 290, 85),
        line("raid-horizons", "Raid Impact", "horizon_minutes",
             [("Raid Impact", "Average Viewer Change")], 20, 180, 540, 480),
        table("raid-detail", [
            ("Raid Impact", "raid_at", False),
            ("Raid Impact", "context_status", False),
            ("Raid Impact", "source_category_name", False),
            ("Raid Impact", "source_title", False),
            ("Raid Impact", "horizon_minutes", False),
            ("Raid Impact", "raid_viewer_count", False),
            ("Raid Impact", "baseline_avg_viewers", False),
            ("Raid Impact", "viewer_change_from_baseline", False),
            ("Raid Impact", "overlapping_raid", False),
            ("Raid Impact", "stream_end_truncated", False),
        ], 580, 180, 680, 480),
    ]
    page(page_ids["raid"], "Raid impact", raid_visuals)

    community_visuals = [
        textbox("community-title", "Community continuity · elapsed-time comparison", 20, 14, 700, 48, 24),
        card("community-quality", "Stream", "Selected Quality State", 740, 12, 170, 70),
        slicer("community-stream-slicer", "Stream", "started_at", 930, 12, 330, 70),
        card("community-active", "Community Summary", "Active Chatters", 20, 80, 230, 82),
        card("community-first", "Community Summary", "First Observed Chatters", 265, 80, 230, 82),
        card("community-returning", "Community Summary", "Returning Chatters", 510, 80, 230, 82),
        card("community-recurring", "Community Summary", "Recurring Chatters", 755, 80, 230, 82),
        card("community-prior-n", "Historical Comparison", "Prior Stream Sample Size", 1000, 80, 125, 82),
        card("community-weekday-n", "Historical Comparison", "Weekday Sample Size", 1135, 80, 125, 82),
        line("community-prior-difference", "Historical Comparison", "window_index", [
            ("Historical Comparison", "Selected Viewer Difference vs Prior"),
            ("Historical Comparison", "Selected Viewer Difference vs Weekday"),
        ], 20, 185, 760, 475),
        table("community-baselines", [
            ("Historical Comparison", "window_index", False),
            ("Historical Comparison", "time_weighted_avg_viewers", False),
            ("Historical Comparison", "all_stream_avg_viewers", False),
            ("Historical Comparison", "all_stream_sample_size", False),
            ("Historical Comparison", "weekday_avg_viewers", False),
            ("Historical Comparison", "weekday_sample_size", False),
        ], 800, 185, 460, 475),
    ]
    page(page_ids["community"], "Community and comparison", community_visuals)

    (PAGES / "pages.json").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
        "pageOrder": [page_ids["evolution"], page_ids["raid"], page_ids["community"]],
        "activePageName": page_ids["evolution"],
    }, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    build()
