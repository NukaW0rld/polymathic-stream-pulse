"""Static privacy and structure checks for source-controlled Power BI metadata."""

import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "stream-pulse.SemanticModel" / "definition"
REPORT = ROOT / "stream-pulse.Report" / "definition"


class PowerBIProjectTests(unittest.TestCase):
    def test_model_references_only_curated_tables(self):
        model = (MODEL / "model.tmdl").read_text(encoding="utf-8")
        expected = {
            "Date", "Stream", "Stream Participation", "Raid Impact",
            "Stream Events", "Community Summary", "Historical Comparison", "Quality Source",
        }
        table_files = {path.stem for path in (MODEL / "tables").glob("*.tmdl")}
        self.assertEqual(table_files, expected)
        self.assertNotIn("public chat_messages", model)
        self.assertNotIn("LocalDateTable", model)
        self.assertIn("__PBI_TimeIntelligenceEnabled = 0", model)

    def test_model_does_not_import_raw_identity_or_chat_fields(self):
        metadata = "\n".join(
            path.read_text(encoding="utf-8") for path in (MODEL / "tables").glob("*.tmdl")
        )
        for private_field in (
            "message_text", "message_fragments", "chatter_user_id",
            "from_broadcaster_user_id", "anonymous_chatter_id",
            "anonymous_raid_source_id",
        ):
            with self.subTest(private_field=private_field):
                self.assertNotIn(private_field, metadata)

    def test_report_has_three_named_pages_and_valid_json_shapes(self):
        pages = json.loads((REPORT / "pages" / "pages.json").read_text(encoding="utf-8"))
        self.assertEqual(len(pages["pageOrder"]), 3)
        names = []
        for page_id in pages["pageOrder"]:
            page_dir = REPORT / "pages" / page_id
            page = json.loads((page_dir / "page.json").read_text(encoding="utf-8"))
            self.assertEqual(page["name"], page_id)
            self.assertEqual(page["$schema"].rsplit("/", 1)[-1], "schema.json")
            names.append(page["displayName"])
            visuals = list((page_dir / "visuals").glob("*/visual.json"))
            self.assertGreaterEqual(len(visuals), 5)
            for visual_path in visuals:
                visual = json.loads(visual_path.read_text(encoding="utf-8"))
                self.assertEqual(visual["name"], visual_path.parent.name)
                self.assertIn("visualType", visual["visual"])
        self.assertEqual(names, [
            "Stream evolution", "Raid impact", "Community and comparison",
        ])

    def test_report_bindings_reference_curated_entities(self):
        allowed = {
            "Stream", "Stream Participation", "Raid Impact", "Community Summary",
            "Stream Events", "Historical Comparison", "Quality Source",
        }
        entities = set()
        for path in (REPORT / "pages").glob("*/visuals/*/visual.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))

            def walk(value):
                if isinstance(value, dict):
                    source = value.get("SourceRef")
                    if isinstance(source, dict) and "Entity" in source:
                        entities.add(source["Entity"])
                    for child in value.values():
                        walk(child)
                elif isinstance(value, list):
                    for child in value:
                        walk(child)

            walk(payload)
        self.assertTrue(entities)
        self.assertLessEqual(entities, allowed)


if __name__ == "__main__":
    unittest.main()
