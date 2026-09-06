"""Synthetic runtime tests: no real credentials, Twitch calls, or production rows."""

from datetime import datetime, timedelta, timezone
import io
import signal
import threading
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

from scripts import collect_stream as collect
from scripts.database import StorageError
from scripts.twitch_auth import TwitchError


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
            self.writer, self.worker, clock=self.clock, emit=self.events.append,
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
                patch.object(collect.PollingCollector, "run", side_effect=run):
            self.assertEqual(collect.main([]), 0)
        self.assertEqual({sig: signal.getsignal(sig) for sig in old}, old)

    def test_startup_exception_details_are_suppressed(self):
        output = io.StringIO()
        with patch.object(collect.TokenManager, "load", side_effect=ValueError("synthetic-private-token")), \
                redirect_stdout(output):
            self.assertEqual(collect.main([]), 1)
        self.assertNotIn("synthetic-private", output.getvalue())


if __name__ == "__main__":
    unittest.main()
