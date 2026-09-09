"""Synthetic runtime tests: no real credentials, Twitch calls, or production rows.

The PostgreSQL-gated ``MergedRuntimePostgresTests`` at the end drives the real
merged collector (real ``RecoveringProbe``, real loopback WebSocket, real
``DatabaseWriter`` on session-temporary tables, a fake poll worker). It contacts
no Twitch service and uses no saved credentials.
"""

from datetime import datetime, timedelta, timezone
import io
import os
import signal
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from websockets.exceptions import ConnectionClosed

import test_eventsub as fixtures
from scripts import check_eventsub as probe
from scripts import collect_stream as collect
from scripts.database import StorageError
from scripts.twitch_auth import TwitchError

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


class FakeClock:
    def __init__(self):
        self.origin = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        self.wall = 0
        self.tick = 0

    def __call__(self):
        return collect.ClockReading(self.origin + timedelta(seconds=self.wall), self.tick)

    def advance(self, seconds, *, wall=None):
        self.tick += seconds
        self.wall += seconds if wall is None else wall


class FakeWorker:
    def __init__(self):
        self.busy = False
        self.result = None
        self.starts = []
        self.finishes = 0

    def start(self, *, force_validation=False):
        if self.busy:
            raise AssertionError("Overlapping poll")
        self.starts.append(force_validation)
        self.busy = True

    def take(self):
        if self.result is None:
            return None
        result, self.result = self.result, None
        self.busy = False
        return result

    def finish(self):
        self.finishes += 1
        self.busy = False
        self.result = None


def synthetic_stream(clock, stream_id="synthetic-stream"):
    return collect.StreamObservation(stream_id, clock.origin - timedelta(hours=1), 100)


class PollingCollectorTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.worker = FakeWorker()
        self.writer = Mock()
        self.writer.start_collector_run.return_value = 1
        self.events = []
        self.collector = collect.PollingCollector(
            self.writer, self.worker, clock=self.clock, emit=self.events.append, pause=self.clock.advance,
        )

    def start(self):
        self.collector.start()
        self.collector.step()

    def complete(self, *, live=True, stream_id="synthetic-stream", error=None, fatal=False):
        self.worker.result = collect.PollResult(
            observed=self.clock(), stream=synthetic_stream(self.clock, stream_id) if live else None,
            error=error, fatal=fatal,
        )
        self.collector.step()

    def reasons(self):
        return [call.kwargs["reason_code"] for call in self.writer.record_collection_health.call_args_list]

    def chat_stream(self, notification=None):
        now = self.clock()
        return self.collector.live_status.chat_stream_id(notification or now.utc, now.utc, now.tick)

    def test_startup_live_write_health_and_eligibility_order(self):
        self.start()
        self.assertEqual(self.worker.starts, [False])
        self.assertEqual(self.reasons(), ["initializing"])
        self.assertIsNone(self.chat_stream())
        self.clock.advance(1)
        self.writer.record_live_poll.side_effect = lambda **_: self.assertIsNone(self.chat_stream())
        self.writer.record_collection_health.side_effect = lambda **_: self.assertIsNone(self.chat_stream())
        self.complete()
        self.assertEqual(self.chat_stream(), "synthetic-stream")
        self.assertEqual(self.reasons(), ["initializing", "live_poll_saved"])
        calls = [call[0] for call in self.writer.mock_calls]
        self.assertLess(calls.index("record_live_poll"), len(calls) - 1)
        self.assertEqual(calls[-1], "record_collection_health")
        self.writer.record_live_poll.assert_called_once_with(
            stream_id="synthetic-stream", started_at=self.clock.origin - timedelta(hours=1),
            observed_at=self.clock().utc, viewer_count=100,
        )

    def test_observation_timestamp_is_receipt_time_not_later_database_time(self):
        self.start()
        self.clock.advance(2)
        observation = self.clock()
        self.worker.result = collect.PollResult(observation, synthetic_stream(self.clock))
        self.clock.advance(10)
        self.collector.step()
        self.assertEqual(self.writer.record_live_poll.call_args.kwargs["observed_at"], observation.utc)
        self.assertEqual(self.writer.record_collection_health.call_args.kwargs["observed_at"], self.clock().utc)

    def test_offline_at_startup_is_healthy_without_stream_update_or_snapshot(self):
        self.start()
        self.complete(live=False)
        self.assertEqual(self.reasons(), ["initializing", "offline_poll_saved"])
        self.writer.mark_stream_offline.assert_not_called()
        self.writer.record_live_poll.assert_not_called()

    def test_failure_keeps_broadcast_for_later_offline_detection(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.complete(error="network_error")
        self.assertIsNone(self.chat_stream())
        self.assertEqual(self.collector.tracked_stream, "synthetic-stream")
        self.writer.mark_stream_offline.assert_not_called()
        self.clock.advance(60)
        self.collector.step()
        self.complete(live=False)
        self.writer.mark_stream_offline.assert_called_once_with(
            stream_id="synthetic-stream", offline_observed_at=self.clock().utc,
        )
        self.assertIsNone(self.collector.tracked_stream)
        self.assertEqual(self.writer.record_live_poll.call_count, 1)
        self.assertEqual(self.reasons()[-1], "offline_poll_saved")

    def test_same_stream_recovery_rejects_chat_from_uncertain_interval(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.complete(error="api_error")
        missed = self.clock().utc
        self.clock.advance(60)
        self.collector.step()
        self.complete()
        self.assertIsNone(self.chat_stream(missed))
        self.assertEqual(self.chat_stream(), "synthetic-stream")

    def test_new_stream_never_fabricates_offline_time_for_previous_stream(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.complete(stream_id="synthetic-next")
        self.writer.mark_stream_offline.assert_not_called()
        self.assertEqual(self.collector.tracked_stream, "synthetic-next")

    def test_pending_request_does_not_block_heartbeats_or_exact_stale_boundary(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.clock.advance(29)
        self.collector.step()
        self.assertEqual(self.chat_stream(), "synthetic-stream")
        self.clock.advance(1)
        self.collector.step()
        self.assertIsNone(self.chat_stream())
        self.assertEqual(self.reasons()[-1], "poll_stale")
        self.assertEqual(self.writer.update_collector_heartbeat.call_count, 2)
        self.clock.advance(30)
        self.collector.step()
        self.assertEqual(self.reasons().count("poll_stale"), 1)
        self.assertEqual(len(self.worker.starts), 2)

    def test_offline_polling_also_becomes_stale(self):
        self.start()
        self.complete(live=False)
        self.clock.advance(60)
        self.collector.step()
        self.clock.advance(30)
        self.collector.step()
        self.assertEqual(self.reasons()[-1], "poll_stale")

    def test_long_operation_is_discarded_and_missed_poll_slots_are_skipped(self):
        self.start()
        for _ in range(5):
            self.clock.advance(30)
            self.collector.step()
        self.complete()
        self.writer.record_live_poll.assert_not_called()
        self.assertIn("late_poll_discarded", self.events)
        self.assertEqual(len(self.worker.starts), 2)
        self.assertEqual(self.collector._next_poll, 180)

    def test_sleep_invisible_to_elapsed_clock_discards_pending_result_and_revalidates(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.worker.result = collect.PollResult(self.clock(), synthetic_stream(self.clock))
        self.clock.advance(1, wall=3601)
        self.collector.step()
        self.assertIsNone(self.chat_stream())
        self.assertIn("poll_stale", self.reasons())
        self.assertEqual(self.writer.record_live_poll.call_count, 1)
        self.assertTrue(self.worker.starts[-1])
        self.complete()
        self.assertEqual(self.chat_stream(), "synthetic-stream")

    def test_coordinator_stall_with_both_clocks_advancing_requires_fresh_poll(self):
        self.start()
        self.worker.result = collect.PollResult(self.clock(), synthetic_stream(self.clock))
        self.clock.advance(120)
        self.collector.step()
        self.writer.record_live_poll.assert_not_called()
        self.assertTrue(self.worker.starts[-1])

    def test_clock_rollback_invalidates_and_stops_before_new_database_writes(self):
        self.start()
        self.complete()
        before = list(self.writer.mock_calls)
        self.clock.advance(-1)
        with self.assertRaisesRegex(collect.CollectorError, "clock_rollback"):
            self.collector.step()
        self.assertIsNone(self.chat_stream())
        self.assertEqual(self.writer.mock_calls, before)

    def test_small_utc_rollback_waits_without_writes_then_requires_fresh_poll(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        # A pending result from before the correction must not be accepted.
        self.worker.result = collect.PollResult(self.clock(), synthetic_stream(self.clock))
        previous_utc = self.clock().utc
        before = list(self.writer.mock_calls)
        self.clock.advance(0.01, wall=-1.75)

        def pause(seconds):
            self.assertEqual(self.writer.mock_calls, before)
            self.assertIsNone(self.chat_stream())
            self.clock.advance(seconds)

        self.collector.pause = pause
        self.collector.step()
        self.assertGreaterEqual(self.clock().utc, previous_utc)
        self.assertIn("utc_clock_rollback_waiting", self.events)
        self.assertIn("utc_clock_recovered_fresh_poll_required", self.events)
        self.assertIn("late_poll_discarded", self.events)
        self.assertIsNone(self.chat_stream())
        self.assertTrue(self.worker.starts[-1])
        self.assertEqual(self.writer.record_live_poll.call_count, 1)
        self.complete()
        self.assertEqual(self.chat_stream(), "synthetic-stream")
        self.assertEqual(self.writer.record_live_poll.call_args.kwargs["observed_at"], self.clock().utc)

    def test_small_utc_rollback_returns_actual_clock_instead_of_clamping_timestamp(self):
        self.start()
        self.clock.advance(20)
        previous = self.collector._sample()
        self.clock.advance(0.01, wall=-0.01)
        current = self.collector._sample()
        self.assertEqual(current, self.clock())
        self.assertEqual((current.utc - previous.utc).total_seconds(), 0.24)

    def test_utc_rollback_of_five_seconds_stops_without_waiting_or_writing(self):
        self.start()
        before = list(self.writer.mock_calls)
        self.collector.pause = Mock()
        self.clock.advance(0.01, wall=-5)
        with self.assertRaisesRegex(collect.CollectorError, "utc_clock_rollback_restart_required"):
            self.collector.step()
        self.collector.pause.assert_not_called()
        self.assertEqual(self.writer.mock_calls, before)

    def test_utc_recovery_is_bounded_when_utc_does_not_catch_up(self):
        self.start()
        before = list(self.writer.mock_calls)
        self.clock.advance(0.01, wall=-1.75)
        self.collector.pause = Mock(side_effect=lambda seconds: self.clock.advance(seconds, wall=0))
        with self.assertRaisesRegex(collect.CollectorError, "utc_clock_recovery_timeout"):
            self.collector.step()
        self.assertEqual(self.collector.pause.call_count, 20)
        self.assertEqual(self.writer.mock_calls, before)

    def test_elapsed_clock_rollback_during_utc_recovery_still_stops(self):
        self.start()
        self.clock.advance(0.01, wall=-1)
        self.collector.pause = lambda _: self.clock.advance(-0.01, wall=0.25)
        with self.assertRaisesRegex(collect.CollectorError, "elapsed_clock_rollback"):
            self.collector.step()

    def test_delayed_wakeup_past_recovery_budget_stops_even_if_utc_caught_up(self):
        self.start()
        self.clock.advance(0.01, wall=-1.75)
        self.collector.pause = lambda _: self.clock.advance(10)
        with self.assertRaisesRegex(collect.CollectorError, "utc_clock_recovery_timeout"):
            self.collector.step()

    def test_utc_correction_during_data_commit_does_not_restore_eligibility(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.writer.record_live_poll.side_effect = lambda **_: self.clock.advance(0.01, wall=-1.75)
        self.complete()
        self.assertIsNone(self.chat_stream())
        self.assertEqual(self.reasons().count("live_poll_saved"), 1)
        self.assertIn("saved_poll_no_longer_fresh", self.events)

    def test_health_failure_after_saved_data_aborts_without_retry_or_orderly_stop(self):
        stop = Mock()
        stop.is_set.return_value = False

        def wait(_):
            self.worker.result = collect.PollResult(self.clock(), synthetic_stream(self.clock))

        stop.wait.side_effect = wait
        self.writer.record_collection_health.side_effect = [None, StorageError("synthetic-private-detail")]
        self.assertEqual(self.collector.run(stop), 1)
        self.writer.record_live_poll.assert_called_once()
        self.assertEqual(self.writer.record_collection_health.call_count, 2)
        self.writer.stop_collector_run.assert_not_called()
        self.assertIsNone(self.chat_stream())
        self.assertNotIn("synthetic-private-detail", str(self.events))

    def test_uncertain_run_creation_is_not_retried_or_closed(self):
        self.writer.start_collector_run.side_effect = StorageError("synthetic-private-detail")
        self.assertEqual(self.collector.run(threading.Event()), 1)
        self.writer.start_collector_run.assert_called_once()
        self.writer.record_collection_health.assert_not_called()
        self.writer.stop_collector_run.assert_not_called()
        self.assertEqual(self.worker.starts, [])

    def test_live_data_write_failure_never_records_healthy_or_offline(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.side_effect = lambda _: setattr(
            self.worker, "result", collect.PollResult(self.clock(), synthetic_stream(self.clock)),
        )
        self.writer.record_live_poll.side_effect = StorageError("synthetic-private-detail")
        self.assertEqual(self.collector.run(stop), 1)
        self.assertEqual(self.reasons(), ["initializing"])
        self.writer.stop_collector_run.assert_not_called()
        self.writer.mark_stream_offline.assert_not_called()

    def test_heartbeat_failure_aborts_without_retry(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.side_effect = lambda _: self.clock.advance(30)
        self.writer.update_collector_heartbeat.side_effect = StorageError("synthetic-private-detail")
        self.assertEqual(self.collector.run(stop), 1)
        self.writer.update_collector_heartbeat.assert_called_once()
        self.writer.stop_collector_run.assert_not_called()

    def test_slow_database_write_cannot_restore_stale_eligibility(self):
        self.start()
        self.writer.record_live_poll.side_effect = lambda **_: self.clock.advance(100)
        self.complete()
        self.assertIsNone(self.chat_stream())
        self.assertNotIn("live_poll_saved", self.reasons())
        self.assertIn("poll_stale", self.reasons())

    def test_database_delay_past_previous_freshness_starts_new_eligibility_interval(self):
        self.start()
        self.complete()
        self.clock.advance(60)
        self.collector.step()
        self.writer.record_live_poll.side_effect = lambda **_: self.clock.advance(35)
        self.complete()
        self.assertEqual(self.reasons()[-2:], ["poll_stale", "live_poll_saved"])
        self.assertIsNone(self.chat_stream(self.clock.origin + timedelta(seconds=30)))
        self.assertEqual(self.chat_stream(), "synthetic-stream")

    def test_shutdown_requested_during_heartbeat_does_not_dispatch_due_poll(self):
        self.start()
        self.complete()
        stop = threading.Event()
        self.writer.update_collector_heartbeat.side_effect = lambda **_: stop.set()
        self.clock.advance(60)
        self.collector.step(stop)
        self.assertTrue(stop.is_set())
        self.assertEqual(len(self.worker.starts), 1)

    def test_small_forward_clock_discrepancy_invalidates_without_fabricating_stale_health(self):
        self.start()
        self.complete()
        self.clock.advance(1, wall=10)
        self.collector.step()
        self.assertIsNone(self.chat_stream())
        self.assertNotIn("poll_stale", self.reasons())
        self.assertTrue(self.worker.starts[-1])

    def test_offline_write_failure_cannot_claim_healthy_offline(self):
        stop = Mock()
        stop.is_set.return_value = False
        results = iter([synthetic_stream(self.clock), None])

        def wait(_):
            if self.worker.busy:
                self.worker.result = collect.PollResult(self.clock(), next(results))
            self.clock.advance(30)

        stop.wait.side_effect = wait
        self.writer.mark_stream_offline.side_effect = StorageError("synthetic-private-detail")
        self.assertEqual(self.collector.run(stop, duration=180), 1)
        self.writer.mark_stream_offline.assert_called_once()
        self.assertNotIn("offline_poll_saved", self.reasons())
        self.writer.stop_collector_run.assert_not_called()

    def test_fatal_authentication_is_recorded_once_then_aborts(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.side_effect = lambda _: setattr(
            self.worker, "result", collect.PollResult(error="auth_error", fatal=True),
        )
        self.assertEqual(self.collector.run(stop), 1)
        self.assertEqual(self.reasons(), ["initializing", "auth_error"])
        self.assertEqual(len(self.worker.starts), 1)
        self.writer.stop_collector_run.assert_not_called()

    def test_orderly_shutdown_discards_inflight_result_and_never_marks_offline(self):
        stop = threading.Event()
        original_start = self.worker.start

        def submit(**kwargs):
            original_start(**kwargs)
            self.worker.result = collect.PollResult(self.clock(), synthetic_stream(self.clock))
            stop.set()

        self.worker.start = submit
        self.assertEqual(self.collector.run(stop), 0)
        self.writer.stop_collector_run.assert_called_once_with(run_id=1, stopped_at=self.clock().utc)
        self.writer.record_live_poll.assert_not_called()
        self.writer.mark_stream_offline.assert_not_called()
        self.assertFalse(self.worker.busy)

    def test_shutdown_commit_failure_is_not_retried_or_reported_as_orderly(self):
        stop = threading.Event()
        stop.set()
        self.writer.stop_collector_run.side_effect = StorageError("synthetic-private-detail")
        self.assertEqual(self.collector.run(stop), 1)
        self.writer.stop_collector_run.assert_called_once()
        self.assertNotIn("orderly_shutdown", self.events)

    def test_duration_requests_orderly_shutdown(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.side_effect = lambda _: self.clock.advance(1)
        self.assertEqual(self.collector.run(stop, duration=2), 0)
        self.assertEqual(self.writer.stop_collector_run.call_args.kwargs["stopped_at"], self.clock().utc)


class TwitchPollerTests(unittest.TestCase):
    def setUp(self):
        self.auth = Mock()
        self.user_response = {"data": [{"id": "synthetic-channel", "login": "polymathic"}]}
        self.stream = {
            "id": "synthetic-stream", "user_id": "synthetic-channel", "type": "live",
            "started_at": "2026-09-06T12:00:00Z", "viewer_count": 0,
        }

    def test_uses_cached_target_and_forced_validation_without_persisting_extra_fields(self):
        self.auth.helix_get.side_effect = [self.user_response, {"data": [self.stream]}, {"data": []}]
        poller = collect.TwitchPoller(self.auth)
        result = poller.poll(force_validation=True)
        self.assertEqual(result.viewer_count, 0)
        self.assertEqual(result.started_at.utcoffset(), timedelta(0))
        self.assertIsNone(poller.poll())
        self.assertEqual(self.auth.helix_get.call_count, 3)
        self.assertEqual(self.auth.validate_if_due.call_args_list[0].kwargs, {"force": True})
        self.assertEqual(self.auth.helix_get.call_args.args, ("streams", {"user_id": "synthetic-channel"}))

    def test_malformed_live_data_is_api_error_not_offline(self):
        changes = (
            {"viewer_count": True}, {"viewer_count": -1}, {"viewer_count": 2147483648},
            {"id": ""}, {"user_id": "synthetic-other"}, {"type": ""},
            {"started_at": "2026-09-06T12:00:00"}, {"started_at": "synthetic-private-text"},
        )
        for change in changes:
            with self.subTest(change=change):
                self.auth.helix_get.side_effect = [self.user_response, {"data": [self.stream | change]}]
                with self.assertRaises(TwitchError) as caught:
                    collect.TwitchPoller(self.auth).poll()
                self.assertEqual(caught.exception.reason_code, "api_error")
                self.assertNotIn("synthetic-private", str(caught.exception))

    def test_missing_null_or_nonlist_data_does_not_establish_offline(self):
        for response in ({}, {"data": None}, {"data": {}}, {"data": [None]}, {"data": [{}, {}]}, []):
            self.auth.helix_get.side_effect = [self.user_response, response]
            with self.assertRaises(TwitchError):
                collect.TwitchPoller(self.auth).poll()

    def test_wrong_target_lookup_never_polls_streams(self):
        self.auth.helix_get.return_value = {"data": [{"id": "synthetic-channel", "login": "synthetic-other"}]}
        with self.assertRaises(TwitchError):
            collect.TwitchPoller(self.auth).poll()
        self.auth.helix_get.assert_called_once()


class PollWorkerTests(unittest.TestCase):
    def test_real_thread_is_serial_and_timestamps_completion(self):
        entered = threading.Event()
        release = threading.Event()
        clock = FakeClock()

        def poll(**_):
            entered.set()
            if not release.wait(2):
                raise AssertionError("Synthetic release timeout")
            return synthetic_stream(clock)

        worker = collect.PollWorker(Mock(poll=poll), clock)
        try:
            worker.start()
            self.assertTrue(entered.wait(2))
            self.assertIsNone(worker.take())
            with self.assertRaises(collect.CollectorError):
                worker.start()
            clock.advance(3)
            release.set()
            worker._thread.join(2)
            self.assertFalse(worker._thread.is_alive())
            result = worker.take()
            self.assertEqual(result.observed, clock())
            self.assertEqual(result.stream.stream_id, "synthetic-stream")
            self.assertFalse(worker.busy)
        finally:
            release.set()
            worker.finish()

    def test_failures_cross_thread_as_safe_codes_only(self):
        for error, reason in (
            (TwitchError("synthetic-private-detail", reason_code="network_error"), "network_error"),
            (TwitchError("synthetic-private-detail", reason_code="auth_error", fatal=True), "auth_error"),
            (ValueError("synthetic-private-detail"), "collector_error"),
        ):
            worker = collect.PollWorker(Mock(poll=Mock(side_effect=error)))
            worker.start()
            worker._thread.join(2)
            result = worker.take()
            self.assertEqual(result.error, reason)
            self.assertNotIn("synthetic-private", repr(result))


class StubProbe:
    """Stands in for RecoveringProbe: records coordinator calls, no socket."""

    def __init__(self, *args, router=None, **kwargs):
        self.router = router
        self.begun = None
        self.steps = 0
        self.shutdowns = 0
        self.gaps = []
        self.step_error = None

    def begin(self, now):
        self.begun = now
        if self.router is not None:
            self.router.begin(now)  # the real probe opens the router here

    def step(self, now):
        self.steps += 1
        if self.step_error is not None:
            raise self.step_error

    def shutdown(self):
        self.shutdowns += 1
        return False

    def force_gap(self, now):
        self.gaps.append(now)


class MergedCollectorTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.worker = FakeWorker()
        self.writer = Mock()
        self.writer.start_collector_run.return_value = 7
        self.events = []
        self.stop = threading.Event()
        patcher = patch("scripts.eventsub_recovery.RecoveringProbe", StubProbe)
        self.addCleanup(patcher.stop)
        patcher.start()
        self.cfg = collect.EventSubConfig(
            auth=Mock(), specs=("raid-spec", "follow-spec"), socket=Mock(),
            connector=Mock(), worker_factory=Mock(), url="wss://synthetic",
        )
        self.collector = collect.PollingCollector(
            self.writer, self.worker, clock=self.clock, emit=self.events.append,
            pause=self.clock.advance, eventsub=self.cfg,
        )

    def armed(self):
        self.collector.start()
        self.collector._start_eventsub(self.stop)
        return self.collector._probe

    def test_eventsub_half_begins_with_the_run_id_and_pumps_each_step(self):
        probe = self.armed()
        self.assertIsNotNone(probe.begun)
        self.assertEqual(probe.router.source_names, ("raids", "follows"))
        self.assertEqual(probe.router._run_id, 7)
        self.collector.step(self.stop)
        self.collector.step(self.stop)
        self.assertEqual(probe.steps, 2)

    def test_probe_storage_failure_stops_the_collector(self):
        probe = self.armed()
        probe.router.storage_failed = True
        with self.assertRaisesRegex(collect.CollectorError, "capture_storage_failure_stop_required"):
            self.collector.step(self.stop)

    def test_probe_error_from_step_becomes_a_collector_error(self):
        from scripts.eventsub import ProbeError
        probe = self.armed()
        probe.step_error = ProbeError("welcome_timeout")
        with self.assertRaisesRegex(collect.CollectorError, "welcome_timeout"):
            self.collector.step(self.stop)

    def test_latched_probe_fatal_stops_the_next_step(self):
        self.armed()
        self.collector._probe_fatal = "authorization_check_failed"
        with self.assertRaisesRegex(collect.CollectorError, "authorization_check_failed"):
            self.collector.step(self.stop)

    def test_clock_gap_tears_the_eventsub_session_down(self):
        probe = self.armed()
        self.collector.step(self.stop)
        self.clock.advance(120)  # >= 90s coordinator gap
        self.collector.step(self.stop)
        self.assertEqual(len(probe.gaps), 1)
        self.assertIn("clock_gap_fresh_poll_required", self.events)

    def test_orderly_shutdown_closes_the_run_over_every_active_source(self):
        rc = self.collector.run(self.stop, duration=0)
        self.assertEqual(rc, 0)
        self.assertGreaterEqual(self.collector._probe.shutdowns, 1)
        self.writer.stop_collector_run.assert_not_called()
        self.writer.stop_collector_run_multi.assert_called_once()
        kwargs = self.writer.stop_collector_run_multi.call_args.kwargs
        self.assertEqual(kwargs["run_id"], 7)
        self.assertEqual(set(kwargs["sources"]), {"stream_poll", "raids", "follows"})
        self.assertIn("orderly_shutdown", self.events)

    def test_shutdown_after_a_probe_storage_failure_leaves_the_run_open(self):
        original = self.collector._start_eventsub

        def rigged(stop):
            original(stop)
            self.collector._probe.router.storage_failed = True

        self.collector._start_eventsub = rigged
        rc = self.collector.run(self.stop, duration=0)
        self.assertEqual(rc, 1)
        self.writer.stop_collector_run_multi.assert_not_called()
        self.assertIn("capture_storage_failure_stop_required", self.events)

    def test_pure_polling_still_closes_with_the_single_source_stop(self):
        polling_only = collect.PollingCollector(
            self.writer, self.worker, clock=self.clock, emit=self.events.append,
            pause=self.clock.advance,
        )
        rc = polling_only.run(self.stop, duration=0)
        self.assertEqual(rc, 0)
        self.writer.stop_collector_run.assert_called_once()
        self.writer.stop_collector_run_multi.assert_not_called()

    def test_default_config_wires_no_chat_sink(self):
        self.armed()
        self.assertIsNone(self.collector._chat_sink)
        self.assertEqual(self.collector._probe.router.source_names, ("raids", "follows"))


class MergedChatWiringTests(unittest.TestCase):
    """Chat as the fourth source, driven through a stub probe (real router/sinks)."""

    def setUp(self):
        self.clock = FakeClock()
        self.worker = FakeWorker()
        self.writer = Mock()
        self.writer.start_collector_run.return_value = 7
        self.events = []
        self.stop = threading.Event()
        patcher = patch("scripts.eventsub_recovery.RecoveringProbe", StubProbe)
        self.addCleanup(patcher.stop)
        patcher.start()
        self.cfg = collect.EventSubConfig(
            auth=Mock(), specs=("chat-spec", "raid-spec", "follow-spec"), socket=Mock(),
            connector=Mock(), worker_factory=Mock(), url="wss://synthetic",
            sources=("chat", "raids", "follows"),
        )
        self.collector = collect.PollingCollector(
            self.writer, self.worker, clock=self.clock, emit=self.events.append,
            pause=self.clock.advance, eventsub=self.cfg,
        )

    def arm(self):
        """Start the run, build the router, mark chat transport ready, dispatch poll 1."""
        self.collector.start()
        self.collector._start_eventsub(self.stop)
        # The real router calls this from observe(); the stub probe does not.
        self.collector._chat_sink.transport_ready(self.clock().utc)
        self.collector.step()  # dispatches the first poll (sets _job_started)
        return self.collector._chat_sink

    def chat_health(self):
        return [(c.kwargs["status"], c.kwargs["reason_code"])
                for c in self.writer.record_collection_health.call_args_list
                if c.kwargs["source"] == "chat"]

    def complete(self, *, live=True, error=None):
        self.worker.result = collect.PollResult(
            observed=self.clock(),
            stream=synthetic_stream(self.clock) if live else None, error=error,
        )
        self.collector.step()

    def test_chat_sink_is_built_and_reported_in_source_names(self):
        from scripts.event_sink import ChatSink

        self.collector.start()
        self.collector._start_eventsub(self.stop)
        self.assertIsInstance(self.collector._chat_sink, ChatSink)
        self.assertEqual(self.collector._probe.router.source_names, ("raids", "follows", "chat"))
        self.assertEqual(self.chat_health()[0], ("starting", "initializing"))

    def test_shutdown_closes_the_run_over_all_four_sources(self):
        rc = self.collector.run(self.stop, duration=0)
        self.assertEqual(rc, 0)
        self.writer.stop_collector_run.assert_not_called()
        sources = self.writer.stop_collector_run_multi.call_args.kwargs["sources"]
        self.assertEqual(set(sources), {"stream_poll", "raids", "follows", "chat"})

    def test_transport_ready_alone_only_reaches_awaiting_stream_status(self):
        self.collector.start()
        self.collector._start_eventsub(self.stop)
        self.collector._chat_sink.transport_ready(self.clock().utc)
        self.assertEqual(self.chat_health(), [
            ("starting", "initializing"), ("starting", "awaiting_stream_status"),
        ])

    def test_first_live_poll_takes_chat_to_capture_ready(self):
        self.arm()
        self.clock.advance(1)
        self.complete(live=True)
        self.assertEqual(self.chat_health()[-1], ("healthy", "capture_ready"))

    def test_offline_poll_pauses_chat(self):
        self.arm()
        self.clock.advance(1)
        self.complete(live=False)
        self.assertEqual(self.chat_health()[-1], ("paused", "offline_observed"))

    def test_failed_poll_marks_chat_poll_failed(self):
        self.arm()
        self.clock.advance(1)
        self.complete(error="network_error")
        self.assertEqual(self.chat_health()[-1], ("error", "poll_failed"))

    def test_staleness_marks_chat_poll_stale(self):
        self.arm()
        self.clock.advance(1)
        self.complete(live=True)
        self.clock.advance(90)
        self.collector.step()
        self.assertEqual(self.chat_health()[-1], ("error", "poll_stale"))

    def test_clock_gap_marks_chat_poll_failed(self):
        self.arm()
        self.clock.advance(1)
        self.complete(live=True)
        self.clock.advance(120)  # >= 90s coordinator gap -> on_gap
        self.collector.step()
        self.assertIn(("error", "poll_failed"), self.chat_health())

    def test_step_refreshes_the_chat_eligibility_tick(self):
        sink = self.arm()
        self.clock.advance(5)
        self.collector.step()
        self.assertEqual(sink.tick, self.clock().tick)


class CollectorCLITests(unittest.TestCase):
    def test_signals_request_shutdown_and_handlers_are_restored(self):
        old = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

        def run(stop, **_):
            for sig in old:
                stop.clear()
                signal.getsignal(sig)(sig, None)
                self.assertTrue(stop.is_set())
            return 0

        with patch.object(collect.TokenManager, "load"), patch.object(collect, "open_writer"), \
                patch.object(collect, "_build_eventsub"), \
                patch.object(collect.PollingCollector, "run", side_effect=run):
            self.assertEqual(collect.main([]), 0)
        self.assertEqual({sig: signal.getsignal(sig) for sig in old}, old)

    def test_startup_exception_details_are_suppressed(self):
        output = io.StringIO()
        with patch.object(collect.TokenManager, "load", side_effect=ValueError("synthetic-private-token")), \
                redirect_stdout(output):
            self.assertEqual(collect.main([]), 1)
        self.assertNotIn("synthetic-private", output.getvalue())


class OneShotPollWorker:
    """Fake poll worker: serves a single live observation, then stays idle.

    Matches the ``PollWorker`` surface the coordinator uses (``busy`` /
    ``start`` / ``take`` / ``finish``). The result's ``observed`` reading is a
    real clock sample taken at ``take`` time, so it lands after ``_job_started``.
    """

    def __init__(self, stream):
        self._stream = stream
        self.busy = False
        self._pending = False
        self._served = False
        self.starts = []

    def start(self, *, force_validation=False):
        self.starts.append(force_validation)
        self.busy = True
        self._pending = True

    def take(self):
        if not self._pending or self._served:
            return None
        self._pending = False
        self._served = True
        self.busy = False
        return collect.PollResult(observed=collect.read_clock(), stream=self._stream)

    def finish(self):
        self.busy = False
        self._pending = False


def _replacement_welcome():
    import json

    raw = json.loads(fixtures.welcome())
    raw["payload"]["session"]["id"] = "synthetic-replacement"
    return json.dumps(raw)


@unittest.skipUnless(os.environ.get("STREAM_PULSE_TEST_POSTGRES") == "1",
                     "Set STREAM_PULSE_TEST_POSTGRES=1 to test local PostgreSQL")
class MergedRuntimePostgresTests(unittest.TestCase):
    """End-to-end: the merged coordinator over one run, one loopback socket.

    Closes the gap that ``MergedCollectorTests`` uses a stub probe: here the real
    ``RecoveringProbe`` drives a real WebSocket and a real ``DatabaseWriter``.
    One test covers stream_poll + raids + follows through a synthetic disconnect
    and fresh-session recovery; another adds chat as the fourth source, storing
    an eligible chat message and discarding one outside observed-live
    eligibility. All under one ``run_id``.
    """

    TEMP_SCHEMA = ("001_create_viewer_snapshots.sql", "002_create_streams.sql",
                   "003_create_chat_messages.sql", "004_create_incoming_raids.sql",
                   "005_create_follow_events.sql", "006_create_collector_runs.sql",
                   "007_create_collection_health.sql", "009_create_reconnection_gaps.sql")

    def setUp(self):
        import psycopg
        from scripts.database import DatabaseWriter

        self.connection = psycopg.connect(
            dbname="stream_pulse", host="/var/run/postgresql", autocommit=True,
            connect_timeout=5, options="-c search_path=pg_temp -c statement_timeout=10000",
        )
        self.addCleanup(self.connection.close)
        for name in self.TEMP_SCHEMA:
            ddl = (SQL_DIR / name).read_text().replace("CREATE TABLE ", "CREATE TEMP TABLE ")
            self.connection.execute(ddl)
        self.connection.execute((SQL_DIR / "008_add_paused_health_status.sql").read_text())
        self.connection.execute((SQL_DIR / "010_align_chat_messages.sql").read_text())
        self.writer = DatabaseWriter(self.connection)

        self.auth = Mock(user_id="synthetic-reader")
        self.auth.helix_get.return_value = {"data": [{"login": "polymathic", "id": "synthetic-channel"}]}
        self.auth.helix_post.side_effect = self._enabled_subscription
        # All three specs are resolved; the run's `sources` decides which sinks
        # the coordinator builds (Phase 2 raids+follows, or the 4-source path).
        self.specs = tuple(collect.prepare_subscriptions(self.auth))
        self.events = []
        self.stream = collect.StreamObservation(
            "synthetic-merged-stream",
            collect.read_clock().utc - timedelta(hours=1), 123,
        )
        self.server_errors = []
        self.workers = []
        self.worker_created = threading.Event()

    @staticmethod
    def _enabled_subscription(endpoint, body):
        source = {"channel.chat.message": "chat", "channel.raid": "raids",
                  "channel.follow": "follows"}[body["type"]]
        return {"data": [body | {"id": f"synthetic-{source}", "status": "enabled"}]}

    def _spec(self, source):
        return next(spec for spec in self.specs if spec.source == source)

    def _notification(self, source, event):
        spec = self._spec(source)
        return fixtures.frame(
            "notification", {"subscription": fixtures.subscription(spec), "event": event}, spec,
        )

    def _worker_factory(self, auth, specs, session_id, stop):
        worker = probe.SetupWorker(auth, specs, session_id, stop)
        self.workers.append(worker)
        self.worker_created.set()
        return worker

    @staticmethod
    def _park(sock, rounds=60):
        """Keep a loopback connection open until the client closes it.

        Sends a keepalive, then blocks briefly on recv (the client never sends
        application data). Bounded so a stuck test cannot wedge a server thread.
        """
        for _ in range(rounds):
            try:
                sock.send(fixtures.frame("session_keepalive", {}))
                sock.recv(timeout=0.4)
            except TimeoutError:
                continue
            except ConnectionClosed:
                return
        return

    def _chat_frame(self, *, eventsub_message_id, message_timestamp,
                    chat_message_id="synthetic-chat-msg"):
        import json

        spec = self._spec("chat")
        raw = json.loads(fixtures.frame("notification", {
            "subscription": fixtures.subscription(spec),
            "event": {
                "broadcaster_user_id": "synthetic-channel",
                "chatter_user_id": "synthetic-chatter",
                "message_id": chat_message_id,
                "message": {"text": "synthetic-chat-text",
                            "fragments": [{"type": "text", "text": "synthetic-chat-text"}]},
                "message_type": "text", "badges": [], "source_broadcaster_user_id": None,
            },
        }, spec))
        raw["metadata"]["message_id"] = eventsub_message_id
        raw["metadata"]["message_timestamp"] = message_timestamp
        return json.dumps(raw)

    def _handler(self, *, drop, chat):
        follow = self._notification("follows", {"user_id": "synthetic-follower",
                                                "followed_at": "2026-09-08T00:00:00Z"})
        raid = self._notification("raids", {"from_broadcaster_user_id": "synthetic-raider",
                                            "viewers": 7})

        def handle(sock):
            try:
                if sock.request.path == "/primary":
                    sock.send(fixtures.welcome())
                    if not self.worker_created.wait(8) or not self.workers[0].setup_complete.wait(8):
                        raise AssertionError("synthetic setup timeout")
                    sock.send(fixtures.frame("session_keepalive", {}))
                    time.sleep(0.6)  # let the probe drain notices + run observe -> capture_ready
                    if chat:
                        time.sleep(1.0)  # let the live poll land -> chat eligibility opens
                        now = datetime.now(timezone.utc).isoformat()
                        sock.send(self._chat_frame(eventsub_message_id="chat-eligible",
                                                   message_timestamp=now))
                        sock.send(self._chat_frame(eventsub_message_id="chat-early",
                                                   message_timestamp="2020-01-01T00:00:00Z"))
                        sock.send(fixtures.frame("session_keepalive", {}))
                    if not drop:
                        self._park(sock)
                        return
                    sock.send(follow)
                    sock.send(raid)
                    sock.send(fixtures.frame("session_keepalive", {}))
                    time.sleep(0.3)
                    sock.close()  # deliberate unexpected loss
                    return
                sock.send(_replacement_welcome())
                deadline = time.monotonic() + 8
                while len(self.workers) < 2 and time.monotonic() < deadline:
                    time.sleep(0.02)
                if len(self.workers) >= 2:
                    self.workers[1].setup_complete.wait(8)
                self._park(sock)
            except ConnectionClosed:
                pass
            except Exception as error:  # pragma: no cover - surfaced via assertion
                self.server_errors.append(repr(error))

        return handle

    def _run_merged(self, *, drop=False, chat=False, duration):
        import faulthandler
        import logging
        import sys

        from websockets.sync.client import connect
        from websockets.sync.server import serve

        logger = logging.Logger("synthetic-merged-test")
        logger.disabled = True
        stop = threading.Event()
        sources = ("raids", "follows", "chat") if chat else ("raids", "follows")
        faulthandler.dump_traceback_later(duration + 45, file=sys.stderr)
        try:
            with serve(self._handler(drop=drop, chat=chat), "127.0.0.1", 0,
                       ping_interval=None, logger=logger) as server:
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                port = server.socket.getsockname()[1]

                def connector(url=None):
                    target = "/primary" if url is None else "/replacement"
                    return connect(f"ws://127.0.0.1:{port}{target}", ping_interval=None,
                                   proxy=None, logger=logger, close_timeout=3)

                cfg = collect.EventSubConfig(
                    auth=self.auth, specs=self.specs, socket=connector(),
                    connector=connector, worker_factory=self._worker_factory, url=probe.URL,
                    sources=sources,
                )
                collector = collect.PollingCollector(
                    self.writer, OneShotPollWorker(self.stream),
                    emit=self.events.append, eventsub=cfg,
                )
                try:
                    # ConnectionJob is real; only shorten the real-time reconnect backoff.
                    with patch("scripts.eventsub_recovery.BACKOFF", (0.5,) * 6):
                        rc = collector.run(stop, duration=duration)
                finally:
                    server.shutdown()
                    thread.join(3)
        finally:
            faulthandler.cancel_dump_traceback_later()
        self.assertEqual(self.server_errors, [])
        self.assertNotIn("synthetic-follower", repr(self.events))
        self.assertNotIn("synthetic-chat-text", repr(self.events))
        return rc

    def _rows(self, sql, params=None):
        return self.connection.execute(sql, params).fetchall()

    def _health(self, run_id, source):
        return [(status, reason) for (status, reason) in self.connection.execute(
            "SELECT status, reason_code FROM pg_temp.collection_health "
            "WHERE run_id = %s AND source = %s ORDER BY health_id", (run_id, source),
        ).fetchall()]

    def test_merged_run_recovers_from_a_disconnect_and_stops_over_every_source(self):
        rc = self._run_merged(drop=True, duration=6)
        self.assertEqual(rc, 0)

        run = self._rows("SELECT run_id, stopped_at IS NOT NULL, last_heartbeat_at <= stopped_at "
                         "FROM pg_temp.collector_runs")
        self.assertEqual(len(run), 1)
        run_id, stopped, ordered = run[0]
        self.assertTrue(stopped and ordered)

        self.assertEqual(self._health(run_id, "stream_poll"), [
            ("starting", "initializing"),
            ("healthy", "live_poll_saved"),
            ("stopped", "orderly_shutdown"),
        ])
        for source in ("raids", "follows"):
            rows = self._health(run_id, source)
            self.assertEqual(rows[:2], [("starting", "initializing"), ("healthy", "capture_ready")])
            self.assertEqual(rows[-1], ("stopped", "orderly_shutdown"))
            self.assertIn(("error", "network_error"), rows)
            self.assertGreaterEqual(rows.count(("healthy", "capture_ready")), 2)

        # stop_collector_run_multi wrote one stopped row per source at one instant;
        # the polling-only stop_collector_run would touch stream_poll alone.
        stopped_at = self._rows(
            "SELECT observed_at FROM pg_temp.collection_health "
            "WHERE run_id = %s AND status = 'stopped'", (run_id,))
        self.assertEqual(len(stopped_at), 3)
        self.assertEqual(len(set(stopped_at)), 1)

        self.assertEqual(self._rows("SELECT count(*), count(*) FILTER (WHERE stream_id IS NULL) "
                                    "FROM pg_temp.follow_events"), [(1, 1)])
        self.assertEqual(self._rows("SELECT count(*), count(*) FILTER (WHERE stream_id IS NULL) "
                                    "FROM pg_temp.incoming_raids"), [(1, 1)])
        self.assertEqual(self._rows("SELECT count(*) FROM pg_temp.streams"), [(1,)])
        self.assertEqual(self._rows("SELECT count(*) FROM pg_temp.viewer_snapshots"), [(1,)])

        self.assertEqual(self._rows(
            "SELECT count(*), count(recovered_at), min(reason_code) FROM pg_temp.reconnection_gaps "
            "WHERE run_id = %s", (run_id,)), [(1, 1, "network_error")])

        self.assertIn("probe_gap_detected_no_replay", self.events)
        self.assertIn("orderly_shutdown", self.events)
        self.assertNotIn("clock_gap_fresh_poll_required", self.events)

    def test_quiet_merged_run_has_the_minimal_health_sequence_and_no_gaps(self):
        rc = self._run_merged(drop=False, duration=3)
        self.assertEqual(rc, 0)

        (run_id,) = self._rows("SELECT run_id FROM pg_temp.collector_runs")[0]
        for source in ("raids", "follows"):
            self.assertEqual(self._health(run_id, source), [
                ("starting", "initializing"),
                ("healthy", "capture_ready"),
                ("stopped", "orderly_shutdown"),
            ])
        self.assertEqual(self._rows("SELECT count(*) FROM pg_temp.reconnection_gaps"), [(0,)])
        self.assertEqual(self._rows("SELECT count(*) FROM pg_temp.follow_events"), [(0,)])
        self.assertEqual(self._rows("SELECT count(*) FROM pg_temp.incoming_raids"), [(0,)])
        self.assertEqual(self._rows("SELECT count(*), count(stopped_at) FROM pg_temp.collector_runs"),
                         [(1, 1)])

    def test_four_source_run_captures_an_eligible_chat_message_and_drops_the_rest(self):
        rc = self._run_merged(chat=True, duration=6)
        self.assertEqual(rc, 0)

        (run_id,) = self._rows("SELECT run_id FROM pg_temp.collector_runs")[0]

        # Every source, including chat, closed at one instant via
        # stop_collector_run_multi.
        stopped = self._rows(
            "SELECT source, observed_at FROM pg_temp.collection_health "
            "WHERE run_id = %s AND status = 'stopped'", (run_id,))
        self.assertEqual({source for source, _ in stopped},
                         {"stream_poll", "raids", "follows", "chat"})
        self.assertEqual(len({moment for _, moment in stopped}), 1)

        chat = self._health(run_id, "chat")
        self.assertEqual(chat[0], ("starting", "initializing"))
        self.assertEqual(chat[-1], ("stopped", "orderly_shutdown"))
        self.assertIn(("healthy", "capture_ready"), chat)
        self.assertTrue(all(status != "error" for status, _ in chat))
        allowed = {("starting", "initializing"), ("starting", "awaiting_stream_status"),
                   ("healthy", "capture_ready"), ("stopped", "orderly_shutdown")}
        self.assertTrue(set(chat) <= allowed)

        # Exactly the in-eligibility message stored, with the resolved stream and
        # no message text or fragments leaked into diagnostics.
        rows = self._rows(
            "SELECT eventsub_message_id, stream_id, chatter_user_id, chat_message_id, "
            "message_fragments, source_broadcaster_user_id FROM pg_temp.chat_messages")
        self.assertEqual(rows, [(
            "chat-eligible", "synthetic-merged-stream", "synthetic-chatter",
            "synthetic-chat-msg", [{"type": "text", "text": "synthetic-chat-text"}], None,
        )])
        self.assertIn("chat_message_outside_eligibility_discarded", self.events)
        self.assertIn("chat_event_stored", self.events)


if __name__ == "__main__":
    unittest.main()
