"""Synthetic tests for bounded milestone-1 Helix work; no real Twitch calls."""

from datetime import datetime, timezone
import unittest
from unittest.mock import Mock

from scripts.clock_guard import ClockReading
from scripts.enhanced_collection import (
    EnhancedWorker, PresencePageJob, RaidContextJob,
    parse_channel_information, parse_presence_page,
)
from scripts.twitch_auth import TwitchError


NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)


class EnhancedParsingTests(unittest.TestCase):
    def test_presence_page_keeps_ids_total_and_cursor(self):
        self.assertEqual(parse_presence_page({
            "data": [{"user_id": "u1"}, {"user_id": "u1"}, {"user_id": "u2"}],
            "total": 3, "pagination": {"cursor": "private-cursor"},
        }), (("u1", "u1", "u2"), 3, "private-cursor"))

    def test_presence_empty_page_and_malformed_shapes(self):
        self.assertEqual(parse_presence_page({
            "data": [], "total": 0, "pagination": {},
        }), ((), 0, None))
        for value in (
            None,
            {"data": [], "total": -1, "pagination": {}},
            {"data": [{"user_id": ""}], "total": 1, "pagination": {}},
            {"data": [], "total": 0, "pagination": {"cursor": 5}},
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_presence_page(value)

    def test_channel_information_normalizes_tags_and_handles_missing_result(self):
        self.assertIsNone(parse_channel_information({"data": []}, "source"))
        metadata = parse_channel_information({"data": [{
            "broadcaster_id": "source", "game_id": "g", "game_name": "DnB",
            "title": "Set", "broadcaster_language": "en",
            "tags": ["Music", "DnB", "Music"],
        }]}, "source")
        self.assertEqual(metadata["tags"], ["DnB", "Music"])
        with self.assertRaises(ValueError):
            parse_channel_information({"data": [{"broadcaster_id": "wrong"}]}, "source")


class EnhancedWorkerTests(unittest.TestCase):
    def clock(self):
        return ClockReading(NOW, 1.0)

    def _take(self, worker):
        worker._thread.join(2)
        self.assertFalse(worker._thread.is_alive())
        return worker.take()

    def test_worker_runs_one_presence_page_and_never_exposes_cursor_in_repr(self):
        auth = Mock()
        auth.helix_get.return_value = {
            "data": [{"user_id": "private-user"}], "total": 1,
            "pagination": {"cursor": "private-cursor"},
        }
        worker = EnhancedWorker(auth, clock=self.clock)
        job = PresencePageJob(1, 2, "private-stream", "private-channel", "private-mod", NOW, 0)
        worker.start(job)
        with self.assertRaisesRegex(RuntimeError, "overlap"):
            worker.start(job)
        result = self._take(worker)
        self.assertEqual(result.members, ("private-user",))
        self.assertEqual(result.reported_total, 1)
        self.assertNotIn("private", repr(result))

    def test_worker_classifies_403_429_and_generic_failure_safely(self):
        job = RaidContextJob("private-message", "private-channel", NOW)
        for status, expected in ((403, "access_denied"), (429, "rate_limited"), (500, "request_failed")):
            auth = Mock()
            auth.helix_get.side_effect = TwitchError("private response", status)
            worker = EnhancedWorker(auth, clock=self.clock)
            worker.start(job)
            result = self._take(worker)
            self.assertEqual(result.error, expected)
            self.assertNotIn("private", repr(result))

    def test_worker_distinguishes_missing_channel_from_success(self):
        auth = Mock()
        auth.helix_get.return_value = {"data": []}
        worker = EnhancedWorker(auth, clock=self.clock)
        worker.start(RaidContextJob("m", "source", NOW))
        self.assertTrue(self._take(worker).not_found)


if __name__ == "__main__":
    unittest.main()
