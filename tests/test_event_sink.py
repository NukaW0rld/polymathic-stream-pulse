"""Synthetic state-machine tests for the EventSub sinks using a fake writer.

The shared health machine is exercised once through ``_SinkContract`` and run
against both ``FollowSink`` and ``RaidSink``; source-specific parsing has its own
small checks.
"""

from datetime import datetime, timedelta, timezone
import unittest

from scripts.database import StorageError
from scripts.event_sink import EventSinkError, FollowSink, RaidSink


BASE = datetime(2026, 9, 8, 20, 30, tzinfo=timezone.utc)


def follow_message(message_id="synthetic-msg-1", user_id="synthetic-follower-1"):
    return {
        "metadata": {
            "message_id": message_id, "message_type": "notification",
            "message_timestamp": "2026-09-08T20:29:59.464757833Z",
            "subscription_type": "channel.follow", "subscription_version": "2",
        },
        "payload": {
            "subscription": {"id": "synthetic-sub-1", "type": "channel.follow", "version": "2"},
            "event": {"user_id": user_id, "followed_at": "2026-09-08T20:29:58Z"},
        },
    }


def raid_message(message_id="synthetic-msg-1", from_id="synthetic-raider-1", viewers=42):
    return {
        "metadata": {
            "message_id": message_id, "message_type": "notification",
            "message_timestamp": "2026-09-08T20:29:59.464757833Z",
            "subscription_type": "channel.raid", "subscription_version": "1",
        },
        "payload": {
            "subscription": {"id": "synthetic-sub-1", "type": "channel.raid", "version": "1"},
            "event": {
                "from_broadcaster_user_id": from_id, "from_broadcaster_user_login": "raider",
                "to_broadcaster_user_id": "synthetic-broadcaster", "viewers": viewers,
            },
        },
    }


class FakeWriter:
    def __init__(self):
        self.events = []
        self.health = []
        self.event_result = 1
        self.event_error = None

    def _event(self, **kwargs):
        self.events.append(kwargs)
        if self.event_error is not None:
            raise self.event_error
        return self.event_result

    record_follow_event = _event
    record_raid_event = _event

    def record_collection_health(self, *, run_id, source, observed_at, status, reason_code):
        self.health.append((source, status, reason_code, observed_at))


class _SinkContract:
    """State-machine tests shared by every plain-contract EventSub sink."""

    sink_class = None
    source = None

    def message(self, message_id="synthetic-msg-1"):
        raise NotImplementedError

    def broken_message(self):
        raise NotImplementedError

    def setUp(self):
        self.writer = FakeWriter()
        self.diag = []
        self.sink = self.sink_class(self.writer, 7, emit=self.diag.append)

    def codes(self):
        return [(status, reason) for (_s, status, reason, _at) in self.writer.health]

    def test_begin_then_transport_ready_reaches_capture_ready(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.assertEqual(self.codes(), [
            ("starting", "initializing"), ("healthy", "capture_ready"),
        ])
        self.assertTrue(all(row[0] == self.source for row in self.writer.health))

    def test_health_row_written_only_on_change(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.sink.submit(self.message("m1"), BASE, BASE + timedelta(seconds=2))
        self.sink.submit(self.message("m2"), BASE, BASE + timedelta(seconds=3))
        self.assertEqual(self.codes(), [
            ("starting", "initializing"), ("healthy", "capture_ready"),
        ])
        self.assertEqual(len(self.writer.events), 2)

    def test_event_persisted_before_transport_ready_does_not_claim_healthy(self):
        self.sink.begin(BASE)
        self.assertTrue(self.sink.submit(self.message(), BASE, BASE + timedelta(seconds=1)))
        self.assertEqual(len(self.writer.events), 1)
        self.assertEqual(self.codes(), [("starting", "initializing")])

    def test_duplicate_redelivery_returns_false_and_keeps_health(self):
        self.writer.event_result = 0
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.assertFalse(self.sink.submit(self.message(), BASE, BASE + timedelta(seconds=2)))
        self.assertIn(f"{self.source}_event_duplicate_skipped", self.diag)
        self.assertEqual(self.codes(), [
            ("starting", "initializing"), ("healthy", "capture_ready"),
        ])

    def test_invalid_notification_sets_error_then_valid_event_recovers(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.assertFalse(self.sink.submit(self.broken_message(), BASE, BASE + timedelta(seconds=2)))
        self.sink.submit(self.message(), BASE, BASE + timedelta(seconds=3))
        self.assertEqual(self.codes(), [
            ("starting", "initializing"),
            ("healthy", "capture_ready"),
            ("error", "invalid_notification"),
            ("healthy", "capture_ready"),
        ])
        self.assertTrue(any(d.startswith(f"{self.source}_notification_invalid_") for d in self.diag))
        self.assertEqual(len(self.writer.events), 1)

    def test_transport_ready_alone_does_not_clear_a_notification_error(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.sink.submit(self.broken_message(), BASE, BASE + timedelta(seconds=2))
        self.sink.transport_error("network_error", BASE + timedelta(seconds=3))
        self.sink.transport_ready(BASE + timedelta(seconds=4))
        self.assertEqual(self.codes()[-1], ("error", "network_error"))

    def test_repeated_invalid_notifications_write_one_error_row(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE)
        for offset in range(3):
            self.sink.submit(self.broken_message(), BASE, BASE + timedelta(seconds=offset))
        self.assertEqual(self.codes().count(("error", "invalid_notification")), 1)

    def test_transport_error_maps_unknown_reason_and_recovers(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.sink.transport_error("something_unexpected", BASE + timedelta(seconds=2))
        self.sink.transport_error("keepalive_timeout", BASE + timedelta(seconds=3))
        self.sink.transport_ready(BASE + timedelta(seconds=4))
        self.assertEqual(self.codes(), [
            ("starting", "initializing"),
            ("healthy", "capture_ready"),
            ("error", "subscription_error"),
            ("error", "keepalive_timeout"),
            ("healthy", "capture_ready"),
        ])

    def test_stop_records_shutdown_once_and_freezes_state(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE + timedelta(seconds=1))
        self.sink.stop(BASE + timedelta(seconds=2))
        self.sink.stop(BASE + timedelta(seconds=3))
        self.sink.transport_error("network_error", BASE + timedelta(seconds=4))
        self.assertEqual(self.codes(), [
            ("starting", "initializing"),
            ("healthy", "capture_ready"),
            ("stopped", "orderly_shutdown"),
        ])
        with self.assertRaises(EventSinkError):
            self.sink.submit(self.message(), BASE, BASE + timedelta(seconds=5))

    def test_storage_error_from_event_write_propagates(self):
        self.writer.event_error = StorageError("synthetic-db-failure")
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE)
        with self.assertRaises(StorageError):
            self.sink.submit(self.message(), BASE, BASE + timedelta(seconds=1))
        self.assertEqual(self.codes(), [
            ("starting", "initializing"), ("healthy", "capture_ready"),
        ])

    def test_naive_observed_at_is_rejected(self):
        for call in (
            lambda: self.sink.begin(datetime(2026, 9, 8, 20, 30)),
            lambda: self.sink.transport_ready(datetime(2026, 9, 8, 20, 30)),
            lambda: self.sink.submit(self.message(), BASE, datetime(2026, 9, 8)),
            lambda: self.sink.stop(datetime(2026, 9, 8)),
        ):
            with self.assertRaises(EventSinkError):
                call()


class FollowSinkTests(_SinkContract, unittest.TestCase):
    sink_class = FollowSink
    source = "follows"

    def message(self, message_id="synthetic-msg-1"):
        return follow_message(message_id)

    def broken_message(self):
        message = follow_message()
        message["payload"]["event"]["followed_at"] = "not-a-time"
        return message


class RaidSinkTests(_SinkContract, unittest.TestCase):
    sink_class = RaidSink
    source = "raids"

    def message(self, message_id="synthetic-msg-1"):
        return raid_message(message_id)

    def broken_message(self):
        message = raid_message()
        message["payload"]["event"]["viewers"] = -1
        return message

    def test_raid_viewer_count_reaches_the_writer(self):
        self.sink.begin(BASE)
        self.sink.transport_ready(BASE)
        self.sink.submit(raid_message(viewers=1500), BASE, BASE + timedelta(seconds=1))
        self.assertEqual(self.writer.events[-1]["raid_viewer_count"], 1500)
        self.assertNotIn("stream_id", self.writer.events[-1])


if __name__ == "__main__":
    unittest.main()
