"""Synthetic tests for EventRouter: EventDelivery + session state -> sink calls."""

from datetime import datetime, timedelta, timezone
import unittest

from scripts.collect_stream import ClockReading
from scripts.database import StorageError
from scripts.event_sink import FollowSink, RaidSink
from scripts.eventsub import EventDelivery
from scripts.eventsub_router import EventRouter


def at(seconds=0):
    base = datetime(2026, 9, 8, 20, 30, tzinfo=timezone.utc)
    return ClockReading(base + timedelta(seconds=seconds), float(seconds))


def follow_frame(message_id="synthetic-msg-1"):
    return {
        "metadata": {
            "message_id": message_id, "message_type": "notification",
            "message_timestamp": "2026-09-08T20:29:59Z",
            "subscription_type": "channel.follow", "subscription_version": "2",
        },
        "payload": {"event": {"user_id": "synthetic-follower-1",
                              "followed_at": "2026-09-08T20:29:58Z"}},
    }


class FakeSession:
    def __init__(self, *ready):
        self.ready_sources = set(ready)

    def source_ready(self, source):
        return source in self.ready_sources


class FakeSink:
    def __init__(self):
        self.calls = []
        self.submit_result = True
        self.submit_error = None

    def begin(self, observed_at):
        self.calls.append(("begin", observed_at))

    def transport_ready(self, observed_at):
        self.calls.append(("transport_ready", observed_at))

    def transport_error(self, reason_code, observed_at):
        self.calls.append(("transport_error", reason_code, observed_at))

    def submit(self, message, received_at, observed_at):
        self.calls.append(("submit", message, received_at, observed_at))
        if self.submit_error is not None:
            raise self.submit_error
        return self.submit_result

    def stop(self, observed_at):
        self.calls.append(("stop", observed_at))


class EventRouterTests(unittest.TestCase):
    def setUp(self):
        self.follows = FakeSink()
        self.raids = FakeSink()
        self.diag = []
        self.router = EventRouter(
            {"follows": self.follows, "raids": self.raids}, emit=self.diag.append,
        )

    def test_begin_and_stop_reach_every_sink_once(self):
        self.router.begin(at(0))
        self.router.stop(at(9))
        self.router.stop(at(10))
        self.assertEqual(self.follows.calls, [("begin", at(0).utc), ("stop", at(9).utc)])
        self.assertEqual(self.raids.calls, [("begin", at(0).utc), ("stop", at(9).utc)])

    def test_observe_drives_only_the_not_ready_to_ready_edge(self):
        self.router.observe(FakeSession("follows"), at(1))
        self.router.observe(FakeSession("follows"), at(2))          # still ready: no repeat
        self.router.observe(FakeSession("follows", "raids"), at(3))  # raids newly ready
        self.assertEqual(self.follows.calls, [("transport_ready", at(1).utc)])
        self.assertEqual(self.raids.calls, [("transport_ready", at(3).utc)])

    def test_transport_lost_errors_ready_sinks_then_observe_re_readies(self):
        self.router.observe(FakeSession("follows", "raids"), at(1))
        self.router.transport_lost("network_error", at(2))
        self.router.transport_lost("network_error", at(3))  # nothing still marked ready
        self.router.observe(FakeSession("follows", "raids"), at(4))
        self.assertEqual(self.follows.calls, [
            ("transport_ready", at(1).utc),
            ("transport_error", "network_error", at(2).utc),
            ("transport_ready", at(4).utc),
        ])

    def test_notification_becomes_submit_with_receipt_time(self):
        frame = follow_frame()
        result = self.router.dispatch(EventDelivery("notification", "follows", frame), at(3))
        self.assertTrue(result)
        self.assertEqual(self.follows.calls, [("submit", frame, at(3).utc, at(3).utc)])
        self.assertEqual(self.raids.calls, [])

    def test_notification_returns_false_when_sink_skips_duplicate(self):
        self.follows.submit_result = False
        self.assertFalse(
            self.router.dispatch(EventDelivery("notification", "follows", follow_frame()), at(3))
        )

    def test_revocation_becomes_transport_error_and_clears_ready_mark(self):
        self.router.observe(FakeSession("raids"), at(1))
        self.router.dispatch(EventDelivery("revocation", "raids", {}), at(4))
        self.router.transport_lost("network_error", at(5))  # raids no longer marked ready
        self.assertEqual(self.raids.calls, [
            ("transport_ready", at(1).utc),
            ("transport_error", "subscription_revoked", at(4).utc),
        ])

    def test_unrouted_source_is_reported_and_touches_no_sink(self):
        self.assertFalse(self.router.dispatch(EventDelivery("notification", "chat", {}), at(5)))
        self.assertIn("capture_unrouted_chat", self.diag)
        self.assertEqual(self.follows.calls, [])

    def test_unknown_delivery_kind_is_reported(self):
        self.assertFalse(self.router.dispatch(EventDelivery("mystery", "follows", {}), at(5)))
        self.assertIn("capture_unknown_delivery_kind_follows", self.diag)

    def test_frames_after_stop_are_discarded(self):
        self.router.begin(at(0))
        self.router.stop(at(1))
        self.assertFalse(
            self.router.dispatch(EventDelivery("notification", "follows", follow_frame()), at(2))
        )
        self.assertIn("capture_frame_after_stop_discarded_follows", self.diag)
        self.assertEqual(self.follows.calls, [("begin", at(0).utc), ("stop", at(1).utc)])

    def test_storage_error_latches_storage_failed_and_quiets_further_calls(self):
        self.follows.submit_error = StorageError("synthetic-db-failure")
        self.assertFalse(
            self.router.dispatch(EventDelivery("notification", "follows", follow_frame()), at(3))
        )
        self.assertTrue(self.router.storage_failed)
        self.assertIn("capture_storage_failure_stop_required", self.diag)
        # Once latched, transport/lifecycle calls are inert (no writes to a dead DB).
        self.router.observe(FakeSession("follows", "raids"), at(4))
        self.router.transport_lost("network_error", at(5))
        self.router.stop(at(6))
        self.assertEqual(self.raids.calls, [])
        self.assertNotIn(("stop", at(6).utc), self.follows.calls)


class EventRouterWithRealSinksTests(unittest.TestCase):
    """The router drives real sinks; a fake writer captures the persistence calls."""

    class Writer:
        def __init__(self):
            self.events = []
            self.health = []

        def _event(self, **kwargs):
            self.events.append(kwargs)
            return 1

        record_follow_event = _event
        record_raid_event = _event

        def record_collection_health(self, *, run_id, source, observed_at, status, reason_code):
            self.health.append((source, status, reason_code))

    def test_end_to_end_delivery_reaches_the_writer_and_sets_health(self):
        writer = self.Writer()
        router = EventRouter(
            {"follows": FollowSink(writer, 1, emit=lambda _: None),
             "raids": RaidSink(writer, 1, emit=lambda _: None)},
            emit=lambda _: None,
        )
        router.begin(at(0))
        router.observe(FakeSession("follows"), at(1))
        stored = router.dispatch(EventDelivery("notification", "follows", follow_frame()), at(2))
        self.assertTrue(stored)
        self.assertEqual(writer.events[-1]["user_id"], "synthetic-follower-1")
        self.assertIn(("follows", "healthy", "capture_ready"), writer.health)
        router.stop(at(3))
        self.assertIn(("follows", "stopped", "orderly_shutdown"), writer.health)
        self.assertIn(("raids", "stopped", "orderly_shutdown"), writer.health)


if __name__ == "__main__":
    unittest.main()
