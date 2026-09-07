"""Synthetic tests for the EventSub capture collector; no Twitch or credentials."""

import threading
import unittest
from unittest.mock import Mock

import test_eventsub as fixtures
from scripts.collect_eventsub import CAPTURED_SOURCES, Heartbeat, collect
from scripts.database import StorageError


def auth_stub():
    auth = Mock(user_id="synthetic-reader")
    auth.helix_get.return_value = {"data": [{"login": "polymathic", "id": "synthetic-channel"}]}
    return auth


def keepalive():
    return fixtures.frame("session_keepalive", {})


def follow_notification():
    spec = fixtures.specs()[2]
    return fixtures.frame("notification", {
        "subscription": fixtures.subscription(spec),
        "event": {"user_id": "synthetic-follower", "followed_at": "2026-09-07T00:00:00Z"},
    }, spec)


def raid_notification():
    spec = fixtures.specs()[1]
    return fixtures.frame("notification", {
        "subscription": fixtures.subscription(spec),
        "event": {"from_broadcaster_user_id": "synthetic-raider", "viewers": 12},
    }, spec)


class FakeWriter:
    def __init__(self):
        self.run_id = 0
        self.health = []
        self.follows = []
        self.raids = []
        self.heartbeats = []
        self.closed = None
        self.fail = set()  # method names that should raise StorageError

    def _maybe_fail(self, name):
        if name in self.fail:
            raise StorageError(f"synthetic-{name}-failure")

    def start_collector_run(self, *, started_at):
        self._maybe_fail("start_collector_run")
        self.run_id += 1
        return self.run_id

    def update_collector_heartbeat(self, *, run_id, last_heartbeat_at):
        self._maybe_fail("update_collector_heartbeat")
        self.heartbeats.append((run_id, last_heartbeat_at))

    def close_collector_run(self, *, run_id, stopped_at):
        self._maybe_fail("close_collector_run")
        self.closed = (run_id, stopped_at)

    def record_collection_health(self, *, run_id, source, observed_at, status, reason_code):
        self._maybe_fail("record_collection_health")
        self.health.append((source, status, reason_code))

    def record_follow_event(self, **kwargs):
        self._maybe_fail("record_follow_event")
        self.follows.append(kwargs)
        return 1

    def record_raid_event(self, **kwargs):
        self._maybe_fail("record_raid_event")
        self.raids.append(kwargs)
        return 1

    def health_for(self, source):
        return [(status, reason) for (s, status, reason) in self.health if s == source]


class Worker(fixtures.FakeWorker):
    def __init__(self, *args):
        super().__init__(*args)
        self.setup_complete = threading.Event()
        self.setup_complete.set()


class TimedStop:
    """Unset until the shared fake clock passes ``at`` seconds."""

    def __init__(self, seconds, at):
        self.seconds = seconds
        self.at = at

    def is_set(self):
        return self.seconds[0] >= self.at

    def set(self):
        self.at = 0

    def wait(self, duration):
        self.seconds[0] += duration
        return self.is_set()


class CollectEventsubTests(unittest.TestCase):
    def setUp(self):
        self.seconds = [0.0]
        self.events = []
        self.auth = auth_stub()
        self.writer = FakeWriter()

    def clock(self):
        return fixtures.at(self.seconds[0])

    def socket_of(self, frames):
        return fixtures.FakeSocket(list(frames), self.seconds)

    def run_collect(self, frames, *, duration=6, stop=None):
        socket = self.socket_of(frames)
        return collect(
            self.auth, self.writer, stop or threading.Event(), duration=duration,
            emit=self.events.append, clock=self.clock,
            connector=lambda *a: socket, worker_factory=Worker,
        )

    def test_only_raids_and_follows_are_subscribed(self):
        seen = []

        class Recording(Worker):
            def __init__(self, auth, specs, session_id, stop):
                seen.append(tuple(spec.source for spec in specs))
                super().__init__(auth, specs, session_id, stop)

        socket = self.socket_of([fixtures.welcome(), keepalive()])
        collect(self.auth, self.writer, threading.Event(), duration=4,
                emit=self.events.append, clock=self.clock,
                connector=lambda *a: socket, worker_factory=Recording)
        self.assertEqual(seen, [("raids", "follows")])
        self.assertEqual(CAPTURED_SOURCES, ("raids", "follows"))

    def test_quiet_run_opens_run_marks_both_sources_ready_and_closes(self):
        code = self.run_collect([fixtures.welcome(), keepalive()], duration=6)
        self.assertEqual(code, 0)
        self.assertEqual(self.writer.run_id, 1)
        self.assertEqual(self.writer.closed[0], 1)
        for source in ("raids", "follows"):
            self.assertEqual(self.writer.health_for(source), [
                ("starting", "initializing"),
                ("healthy", "capture_ready"),
                ("stopped", "orderly_shutdown"),
            ])
        self.assertIn("capture_run_closed", self.events)

    def test_follow_and_raid_notifications_are_persisted(self):
        code = self.run_collect(
            [fixtures.welcome(), keepalive(), follow_notification(), raid_notification()],
            duration=6,
        )
        self.assertEqual(code, 0)
        self.assertEqual([f["user_id"] for f in self.writer.follows], ["synthetic-follower"])
        self.assertEqual([r["raid_viewer_count"] for r in self.writer.raids], [12])
        # Association is deferred: the collector never sets a stream id.
        self.assertNotIn("stream_id", self.writer.follows[0])
        self.assertNotIn("stream_id", self.writer.raids[0])

    def test_periodic_heartbeats_are_written(self):
        def stream():
            yield fixtures.welcome()
            i = 0
            while True:
                i += 1
                yield keepalive() if i % 40 == 0 else TimeoutError()

        socket = fixtures.FakeSocket(stream(), self.seconds)
        code = collect(self.auth, self.writer, threading.Event(), duration=65,
                       emit=self.events.append, clock=self.clock,
                       connector=lambda *a: socket, worker_factory=Worker)
        self.assertEqual(code, 0)
        self.assertGreaterEqual(len(self.writer.heartbeats), 2)
        self.assertTrue(all(rid == 1 for rid, _ in self.writer.heartbeats))

    def test_duration_none_runs_until_stop_is_set(self):
        stop = TimedStop(self.seconds, at=8)
        code = self.run_collect([fixtures.welcome(), keepalive()], duration=None, stop=stop)
        self.assertEqual(code, 0)
        self.assertIsNotNone(self.writer.closed)

    def test_event_storage_failure_stops_without_closing_the_run(self):
        self.writer.fail.add("record_follow_event")
        code = self.run_collect(
            [fixtures.welcome(), keepalive(), follow_notification(),
             keepalive(), keepalive()],
            duration=20,
        )
        self.assertEqual(code, 1)
        self.assertIsNone(self.writer.closed)
        self.assertIn("capture_storage_failure_stop_required", self.events)
        self.assertIn("capture_storage_failure_restart_required_commit_may_be_uncertain", self.events)

    def test_heartbeat_storage_failure_latches_and_stops(self):
        self.writer.fail.add("update_collector_heartbeat")

        def stream():
            yield fixtures.welcome()
            i = 0
            while True:
                i += 1
                yield keepalive() if i % 40 == 0 else TimeoutError()

        socket = fixtures.FakeSocket(stream(), self.seconds)
        code = collect(self.auth, self.writer, threading.Event(), duration=200,
                       emit=self.events.append, clock=self.clock,
                       connector=lambda *a: socket, worker_factory=Worker)
        self.assertEqual(code, 1)
        self.assertIsNone(self.writer.closed)
        self.assertIn("capture_storage_failure_stop_required", self.events)


class HeartbeatUnitTests(unittest.TestCase):
    def setUp(self):
        self.writer = FakeWriter()
        self.router = Mock(storage_failed=False)
        self.hb = Heartbeat(self.writer, 3, self.router, interval=30)

    def test_first_call_arms_without_writing(self):
        self.hb(fixtures.at(0))
        self.assertEqual(self.writer.heartbeats, [])

    def test_writes_once_per_interval(self):
        for seconds in (0, 10, 30, 45, 60):
            self.hb(fixtures.at(seconds))
        # Armed at 0, written at 30 and 60; the 10s and 45s ticks are early.
        self.assertEqual([rid for rid, _ in self.writer.heartbeats], [3, 3])
        self.assertEqual([ts for _, ts in self.writer.heartbeats],
                         [fixtures.at(30).utc, fixtures.at(60).utc])

    def test_storage_failure_latches_router_and_then_stays_quiet(self):
        self.writer.fail.add("update_collector_heartbeat")
        self.hb(fixtures.at(0))
        self.hb(fixtures.at(30))
        self.router.mark_storage_failed.assert_called_once()
        self.router.storage_failed = True
        self.hb(fixtures.at(60))
        self.router.mark_storage_failed.assert_called_once()


if __name__ == "__main__":
    unittest.main()
