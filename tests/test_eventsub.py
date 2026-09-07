"""Synthetic EventSub envelopes and API responses; no private local files."""

from datetime import datetime, timedelta, timezone
import io
import json
import logging
import signal
import threading
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect
from websockets.sync.server import serve

from scripts import check_eventsub as probe
from scripts.collect_stream import ClockReading
from scripts.eventsub import (
    EventDelivery, ProbeError, SessionReadiness, SetupNotice,
    create_subscription, prepare_subscriptions,
)
from scripts.twitch_auth import TwitchError


def specs():
    auth = Mock(user_id="synthetic-reader")
    auth.helix_get.return_value = {"data": [{"login": "polymathic", "id": "synthetic-channel"}]}
    return prepare_subscriptions(auth)


def at(seconds=0):
    return ClockReading(datetime(2026, 9, 7, tzinfo=timezone.utc) + timedelta(seconds=seconds), seconds)


def frame(kind, payload, spec=None):
    metadata = {"message_id": "synthetic-envelope", "message_type": kind,
                "message_timestamp": "2026-09-07T00:00:00.123456789Z"}
    if spec:
        metadata.update(subscription_type=spec.event_type, subscription_version=spec.version)
    return json.dumps({"metadata": metadata, "payload": payload})


def welcome(timeout=30):
    return frame("session_welcome", {"session": {
        "id": "synthetic-session", "status": "connected",
        "connected_at": "2026-09-07T00:00:00Z", "keepalive_timeout_seconds": timeout,
    }})


def subscription(spec):
    return spec.request("synthetic-session") | {"id": f"synthetic-{spec.source}", "status": "enabled"}


class SubscriptionTests(unittest.TestCase):
    def test_incoming_raid_accepts_empty_unused_origin_without_changing_request(self):
        spec = specs()[1]
        response = subscription(spec)
        response["condition"]["from_broadcaster_user_id"] = ""
        auth = Mock()
        auth.helix_post.return_value = {"data": [response]}
        self.assertEqual(create_subscription(auth, spec, "synthetic-session"), "synthetic-raids")
        self.assertEqual(auth.helix_post.call_args.args[1]["condition"],
                         {"to_broadcaster_user_id": "synthetic-channel"})
        self.assertEqual(response["condition"]["from_broadcaster_user_id"], "")

    def test_raid_empty_origin_exception_rejects_other_condition_changes(self):
        spec = specs()[1]
        for condition in (
            {"to_broadcaster_user_id": "synthetic-channel", "from_broadcaster_user_id": "synthetic-origin"},
            {"to_broadcaster_user_id": "synthetic-channel", "from_broadcaster_user_id": None},
            {"to_broadcaster_user_id": "synthetic-other", "from_broadcaster_user_id": ""},
            {"from_broadcaster_user_id": ""},
            {"to_broadcaster_user_id": "synthetic-channel", "from_broadcaster_user_id": "", "unknown": ""},
        ):
            auth = Mock()
            auth.helix_post.return_value = {"data": [subscription(spec) | {"condition": condition}]}
            with self.subTest(condition=condition), self.assertRaises(TwitchError):
                create_subscription(auth, spec, "synthetic-session")
        for spec in (specs()[0], specs()[2]):
            response = subscription(spec)
            response["condition"]["from_broadcaster_user_id"] = ""
            self.assertFalse(spec.matches(response, "synthetic-session"))

    def test_preflight_binds_reader_and_destination_with_correct_versions(self):
        chat, raids, follows = specs()
        self.assertEqual((chat.event_type, chat.version), ("channel.chat.message", "1"))
        self.assertEqual(chat.condition, {"broadcaster_user_id": "synthetic-channel", "user_id": "synthetic-reader"})
        self.assertEqual(raids.condition, {"to_broadcaster_user_id": "synthetic-channel"})
        self.assertEqual((follows.event_type, follows.version), ("channel.follow", "2"))
        self.assertEqual(follows.condition["moderator_user_id"], "synthetic-reader")
        self.assertNotIn("synthetic-reader", repr(chat))

    def test_bad_target_or_failed_authorization_prevents_subscription_setup(self):
        auth = Mock(user_id="synthetic-reader")
        for response in ({}, {"data": []}, {"data": [{"login": "other", "id": "synthetic-channel"}]}):
            auth.helix_get.return_value = response
            with self.assertRaises(ProbeError):
                prepare_subscriptions(auth)
        auth.validate_if_due.side_effect = TwitchError("Synthetic failure", fatal=True)
        auth.helix_get.reset_mock()
        with self.assertRaises(TwitchError):
            prepare_subscriptions(auth)
        auth.helix_get.assert_not_called()

    def test_creation_requires_exact_enabled_subscription_for_current_session(self):
        spec = specs()[0]
        good = subscription(spec)
        for change in ({"status": "webhook_callback_verification_pending"}, {"id": ""},
                       {"version": "2"}, {"type": "channel.follow"},
                       {"condition": {}},
                       {"transport": {"method": "websocket", "session_id": "synthetic-old-session"}}):
            auth = Mock()
            auth.helix_post.return_value = {"data": [good | change]}
            with self.subTest(change=change), self.assertRaises(TwitchError):
                create_subscription(auth, spec, "synthetic-session")
            auth.helix_post.assert_called_once()
        auth.helix_post.return_value = {"data": [good]}
        self.assertEqual(create_subscription(auth, spec, "synthetic-session"), "synthetic-chat")


class SessionReadinessSubsetTests(unittest.TestCase):
    def test_a_source_subset_is_accepted_and_drives_ready(self):
        chat, raids, follows = specs()
        state = SessionReadiness((raids, follows), emit=lambda _: None)
        self.assertEqual(state.expected, frozenset({"raids", "follows"}))
        state.accept(welcome(), at())
        state.accept(fixtures_frame_keepalive(), at(1))
        state.setup_result(SetupNotice(source="raids", subscription_id="synthetic-raids"))
        self.assertFalse(state.ready)  # follows still missing
        state.setup_result(SetupNotice(source="follows", subscription_id="synthetic-follows"))
        self.assertTrue(state.ready)

    def test_empty_duplicate_or_unknown_specs_are_rejected(self):
        chat, raids, follows = specs()
        for bad in ((), (raids, raids), (raids, raids, follows)):
            with self.assertRaises(ProbeError):
                SessionReadiness(bad, emit=lambda _: None)


def fixtures_frame_keepalive():
    return frame("session_keepalive", {})


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.specs = specs()
        self.events = []
        self.state = SessionReadiness(self.specs, emit=self.events.append)
        self.state.accept(welcome(), at())

    def enable_all(self):
        for spec in self.specs:
            self.state.setup_result(SetupNotice(source=spec.source, subscription_id=f"synthetic-{spec.source}"))

    def test_socket_and_quiet_keepalives_do_not_prove_all_subscriptions_enabled(self):
        self.assertFalse(self.state.ready)
        self.state.accept(frame("session_keepalive", {}), at(20))
        self.assertFalse(self.state.ready)
        self.enable_all()
        self.assertTrue(self.state.ready)
        self.assertEqual(self.state.delivery_seen, set())
        self.state.accept(frame("session_keepalive", {}), at(40))
        self.assertEqual(self.events.count("session_responsive"), 1)

    def test_keepalive_expiration_uses_both_clocks(self):
        self.state.check_liveness(at(30))
        with self.assertRaisesRegex(ProbeError, "keepalive_timeout"):
            self.state.check_liveness(at(32.01))
        with self.assertRaises(ProbeError):
            self.state.check_liveness(ClockReading(at(33).utc, 1))

    def test_late_keepalive_within_bounded_grace_restores_readiness(self):
        self.enable_all()
        self.state.accept(frame("session_keepalive", {}), at(1))
        self.state.check_liveness(at(31.001))
        self.assertFalse(self.state.ready)
        self.state.check_liveness(at(31.1))
        self.assertEqual(self.events.count("keepalive_waiting_within_grace"), 1)
        self.state.accept(frame("session_keepalive", {}), at(31.2))
        self.assertTrue(self.state.ready)
        self.assertIn("liveness_received_within_grace", self.events)

    def test_grace_never_refreshes_the_deadline_without_a_valid_frame(self):
        for seconds in (30.01, 31, 32):
            self.state.check_liveness(at(seconds))
        self.assertEqual(self.state.last_liveness, at())
        with self.assertRaisesRegex(ProbeError, "keepalive_timeout"):
            self.state.accept(frame("session_keepalive", {}), at(32.001))
        self.assertEqual(self.state.last_liveness, at())

    def test_revocation_before_post_ack_cannot_restore_source_readiness(self):
        spec = self.specs[2]
        revoked = subscription(spec) | {"status": "authorization_revoked"}
        self.state.accept(frame("revocation", {"subscription": revoked}, spec), at(1))
        self.enable_all()
        self.state.accept(frame("session_keepalive", {}), at(2))
        self.assertEqual(self.state.failed, {"follows"})
        self.assertEqual(set(self.state.ids), {"chat", "raids"})
        self.assertNotIn("follows_subscription_enabled", self.events)
        self.assertFalse(self.state.ready)

    def test_notification_only_reports_envelope_once_and_discards_event_contents(self):
        self.enable_all()
        spec = self.specs[0]
        raw = frame("notification", {"subscription": subscription(spec),
                                     "event": {"message": {"text": "synthetic-private-message"}}}, spec)
        self.state.accept(raw, at(1))
        self.state.accept(raw, at(2))
        self.assertEqual(self.events.count("chat_notification_envelope_received"), 1)
        self.assertNotIn("synthetic-private-message", repr(vars(self.state)))
        self.assertNotIn("synthetic", repr(self.events))

    def test_raid_notification_and_revocation_accept_same_empty_origin_representation(self):
        self.enable_all()
        spec = self.specs[1]
        response = subscription(spec)
        response["condition"]["from_broadcaster_user_id"] = ""
        self.state.accept(frame("notification", {"subscription": response, "event": {}}, spec), at(1))
        self.assertIn("raids_notification_envelope_received", self.events)
        response["status"] = "authorization_revoked"
        self.state.accept(frame("revocation", {"subscription": response}, spec), at(2))
        self.assertEqual(self.state.failed, {"raids"})
        self.assertIn("raids_subscription_revoked", self.events)

    def test_accept_returns_event_delivery_for_notification_and_revocation(self):
        self.enable_all()
        spec = self.specs[2]
        note = frame("notification", {"subscription": subscription(spec),
                                      "event": {"user_id": "synthetic-follower",
                                                "followed_at": "2026-09-07T00:00:00Z"}}, spec)
        delivered = self.state.accept(note, at(1))
        self.assertIsInstance(delivered, EventDelivery)
        self.assertEqual((delivered.kind, delivered.source), ("notification", "follows"))
        # The whole decoded frame is carried through for the capture parser.
        self.assertEqual(delivered.message["payload"]["event"]["user_id"], "synthetic-follower")
        self.assertNotIn("synthetic-follower", repr(delivered))

        revoked = subscription(spec) | {"status": "authorization_revoked"}
        gone = self.state.accept(frame("revocation", {"subscription": revoked}, spec), at(2))
        self.assertEqual((gone.kind, gone.source), ("revocation", "follows"))
        # A keepalive still returns nothing to act on.
        self.assertIsNone(self.state.accept(frame("session_keepalive", {}), at(3)))

    def test_wrong_subscription_envelope_is_rejected_without_payload_in_error(self):
        self.enable_all()
        spec = self.specs[0]
        for change in ({"id": "synthetic-other"}, {"condition": {"private": "synthetic-private"}}):
            raw = frame("notification", {"subscription": subscription(spec) | change, "event": {}}, spec)
            with self.assertRaises(ProbeError) as caught:
                self.state.accept(raw, at(1))
            self.assertEqual(str(caught.exception), "invalid_eventsub_message")

    def test_malformed_messages_do_not_refresh_liveness(self):
        for raw in ("synthetic-private", "[]", "null", frame("unknown", {}),
                    frame("session_keepalive", {"unexpected": True}), welcome()):
            with self.assertRaises(ProbeError):
                self.state.accept(raw, at(25))
        self.assertEqual(self.state.last_liveness, at())

    def test_invalid_welcome_is_not_connection_readiness(self):
        for timeout in (True, 0, 601, "30"):
            state = SessionReadiness(self.specs, emit=self.events.append)
            with self.assertRaises(ProbeError):
                state.accept(welcome(timeout), at())
            self.assertIsNone(state.session_id)

    def test_reconnect_request_validates_host_and_preserves_private_url_exactly(self):
        for url in ("ws://eventsub.wss.twitch.tv/ws", "wss://synthetic-private.invalid/",
                    "wss://eventsub.wss.twitch.tv.evil.invalid/", "wss://user@eventsub.wss.twitch.tv/ws",
                    "wss://eventsub.wss.twitch.tv:444/ws", "wss://eventsub.wss.twitch.tv/ws#fragment"):
            with self.assertRaises(ProbeError):
                self.state.accept(frame("session_reconnect", {"session": {
                    "id": "synthetic-session", "status": "reconnecting", "reconnect_url": url,
                }}), at(1))
        url = "wss://eventsub.wss.twitch.tv/ws?synthetic-private=a%2Fb&x=1"
        request = self.state.accept(frame("session_reconnect", {"session": {
            "id": "synthetic-session", "status": "reconnecting", "reconnect_url": url,
        }}), at(1))
        self.assertEqual(request.url, url)
        self.assertNotIn("synthetic-private", repr(request))
        self.assertNotIn("synthetic-private", repr(self.events))


class FakeSocket:
    def __init__(self, frames, clock):
        self.frames = iter(frames)
        self.clock = clock
        self.closed = False

    def recv(self, *, timeout):
        self.clock[0] += timeout
        item = next(self.frames, TimeoutError())
        if callable(item):
            item = item()
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True


class FakeWorker:
    def __init__(self, auth, specs, session_id, stop):
        self.notices = [SetupNotice(source=spec.source, subscription_id=f"synthetic-{spec.source}")
                        for spec in specs]
        self.finished = False

    def start(self):
        pass

    def take(self):
        return self.notices.pop(0) if self.notices else None

    def finish(self):
        self.finished = True


class FakeRouter:
    """Records the capture-wiring calls RecoveringProbe makes; no persistence."""

    def __init__(self):
        self.calls = []
        self.storage_failed = False

    def begin(self, now):
        self.calls.append(("begin",))

    def observe(self, session, now):
        self.calls.append(("observe", session.ready))

    def dispatch(self, delivery, now):
        self.calls.append(("dispatch", delivery.kind, delivery.source))
        return True

    def transport_lost(self, reason_code, now):
        self.calls.append(("transport_lost", reason_code))

    def stop(self, now):
        self.calls.append(("stop",))

    def kinds(self):
        return [call[0] for call in self.calls]


class ProbeTests(unittest.TestCase):
    def run_fake(self, frames, *, duration=2, factory=FakeWorker, router=None):
        self.seconds = [0]
        self.events = []
        self.socket = FakeSocket(frames, self.seconds)
        return probe.run_session(
            self.socket, Mock(), specs(), threading.Event(), duration=duration,
            clock=lambda: at(self.seconds[0]), emit=self.events.append, worker_factory=factory,
            router=router,
        )

    def test_router_sees_lifecycle_and_notification_delivery(self):
        spec = specs()[2]
        note = frame("notification", {"subscription": subscription(spec),
                                      "event": {"user_id": "x", "followed_at": "2026-09-07T00:00:00Z"}}, spec)
        router = FakeRouter()
        self.assertEqual(self.run_fake(
            [welcome(), frame("session_keepalive", {}), note], duration=3, router=router,
        ), 0)
        self.assertEqual(router.kinds()[0], "begin")
        self.assertEqual(router.kinds()[-1], "stop")
        self.assertIn(("dispatch", "notification", "follows"), router.calls)
        self.assertTrue(any(call == ("observe", True) for call in router.calls))

    def test_quiet_session_succeeds_without_claiming_event_capture(self):
        self.assertEqual(self.run_fake([welcome(), frame("session_keepalive", {})]), 0)
        self.assertTrue(self.socket.closed)
        self.assertIn("probe_finished_subscriptions_confirmed", self.events)
        self.assertFalse(any("capture_ready" in event or "healthy" in event for event in self.events))

    def test_scheduled_keepalive_received_just_after_thirty_seconds_is_processed(self):
        # Reproduce the old post-recv check: welcome at .25, first keepalive
        # arrives .001 seconds beyond its 30-second nominal interval.
        frames = [welcome()] + [TimeoutError()] * 119

        def just_after_deadline():
            self.seconds[0] = 30.251
            return frame("session_keepalive", {})

        frames.append(just_after_deadline)
        self.assertEqual(self.run_fake(frames, duration=31), 0)
        self.assertIn("liveness_received_within_grace", self.events)
        self.assertNotIn("keepalive_timeout", self.events)

    def test_same_delayed_frame_is_rejected_without_receive_tolerance(self):
        frames = [welcome()] + [TimeoutError()] * 119

        def just_after_deadline():
            self.seconds[0] = 30.251
            return frame("session_keepalive", {})

        frames.append(just_after_deadline)
        with patch("scripts.eventsub.KEEPALIVE_GRACE_SECONDS", 0):
            self.assertEqual(self.run_fake(frames, duration=31), 1)
        self.assertIn("keepalive_timeout", self.events)

    def test_partial_failure_keeps_other_sources_enabled_but_probe_returns_failure(self):
        class PartialWorker(FakeWorker):
            def __init__(self, *args):
                super().__init__(*args)
                self.notices[-1] = SetupNotice(source="follows", error="auth_error")
        self.assertEqual(self.run_fake([welcome(), frame("session_keepalive", {})], factory=PartialWorker), 1)
        self.assertIn("chat_subscription_enabled", self.events)
        self.assertIn("raids_subscription_enabled", self.events)
        self.assertIn("follows_auth_error", self.events)
        self.assertNotIn("probe_finished_subscriptions_confirmed", self.events)

    def test_missing_welcome_and_keepalive_timeout_close_socket(self):
        self.assertEqual(self.run_fake([], duration=40), 1)
        self.assertIn("welcome_timeout", self.events)
        self.assertTrue(self.socket.closed)
        self.assertEqual(self.run_fake([welcome(10)], duration=40), 1)
        self.assertIn("keepalive_timeout", self.events)
        self.assertTrue(self.socket.closed)

    def test_buffered_keepalive_after_stall_cannot_restore_readiness(self):
        def stalled():
            self.seconds[0] += 40
            return frame("session_keepalive", {})
        self.assertEqual(self.run_fake([welcome(), stalled], duration=60), 1)
        self.assertIn("keepalive_timeout", self.events)

    def test_clock_rollback_ends_probe(self):
        def rollback():
            self.seconds[0] -= 1
            return frame("session_keepalive", {})
        self.assertEqual(self.run_fake([welcome(), rollback]), 1)
        self.assertIn("clock_uncertain", self.events)

    def test_socket_failure_details_are_suppressed(self):
        self.assertEqual(self.run_fake([welcome(), OSError("synthetic-private-error")]), 1)
        self.assertIn("network_error", self.events)
        self.assertNotIn("synthetic-private", repr(self.events))

    def test_late_fatal_token_error_during_shutdown_prevents_success(self):
        class LateFailure(FakeWorker):
            def finish(self):
                super().finish()
                self.notices.append(SetupNotice(error="auth_error", fatal=True))
        self.assertEqual(self.run_fake([welcome(), frame("session_keepalive", {})], factory=LateFailure), 1)
        self.assertIn("probe_worker_failed_during_shutdown", self.events)

    def test_stalled_subscription_request_does_not_block_socket_keepalives(self):
        entered, release = threading.Event(), threading.Event()
        auth = Mock()

        def post(endpoint, body):
            entered.set()
            if not release.wait(2):
                raise AssertionError("Synthetic worker timeout")
            return {"data": [body | {"id": "synthetic-id", "status": "enabled"}]}

        auth.helix_post.side_effect = post
        seconds = [0]
        events = []

        def while_pending():
            self.assertTrue(entered.wait(2))
            seconds[0] = 8
            return frame("session_keepalive", {})

        def release_worker():
            self.assertIn("session_responsive", events)
            release.set()
            seconds[0] = 12
            raise TimeoutError

        socket = FakeSocket([welcome(10), while_pending, release_worker], seconds)
        try:
            probe.run_session(socket, auth, specs(), threading.Event(), duration=12,
                              emit=events.append, clock=lambda: at(seconds[0]))
        finally:
            release.set()
        self.assertNotIn("keepalive_timeout", events)
        self.assertTrue(socket.closed)


class WorkerTests(unittest.TestCase):
    def run_worker(self, auth, stop=None):
        worker = probe.SetupWorker(auth, specs(), "synthetic-session", stop or threading.Event())
        worker.start()
        worker.thread.join(2)
        if worker.thread.is_alive():
            worker.finish()
            self.fail("Synthetic worker did not finish")
        notices = []
        while (notice := worker.take()) is not None:
            notices.append(notice)
        return notices

    def test_idle_validation_runs_without_notifications(self):
        auth = Mock()
        auth.helix_post.side_effect = lambda endpoint, body: {"data": [body | {
            "id": "synthetic-id", "status": "enabled",
        }]}
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.side_effect = [False, False, True]
        notices = self.run_worker(auth, stop)
        self.assertEqual([notice.source for notice in notices], ["chat", "raids", "follows"])
        self.assertEqual(auth.validate_if_due.call_count, 2)
        self.assertEqual(stop.wait.call_args.args, (30,))

    def test_fatal_authentication_stops_before_other_subscriptions(self):
        auth = Mock()
        auth.helix_post.side_effect = TwitchError("synthetic-private", reason_code="auth_error", fatal=True)
        notices = self.run_worker(auth)
        self.assertEqual(len(notices), 1)
        self.assertTrue(notices[0].fatal)
        auth.helix_post.assert_called_once()
        self.assertNotIn("synthetic-private", repr(notices))

    def test_idle_validation_failure_crosses_thread_without_private_details(self):
        auth = Mock()
        auth.helix_post.side_effect = lambda endpoint, body: {"data": [body | {
            "id": "synthetic-id", "status": "enabled",
        }]}
        auth.validate_if_due.side_effect = TwitchError("synthetic-private", reason_code="network_error")
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.return_value = False
        notices = self.run_worker(auth, stop)
        self.assertIsNone(notices[-1].source)
        self.assertTrue(notices[-1].fatal)
        self.assertNotIn("synthetic-private", repr(notices))

    def test_http_failure_and_local_response_mismatch_have_distinct_safe_diagnostics(self):
        for raid_response, expected in (
            (TwitchError("synthetic-private-body", 409), "raids_subscription_error_http_409"),
            ({"condition": {"to_broadcaster_user_id": "synthetic-other"}},
             "raids_subscription_response_condition_mismatch"),
        ):
            auth = Mock()

            def post(endpoint, body):
                response = body | {"id": "synthetic-id", "status": "enabled"}
                if body["type"] == "channel.raid":
                    if isinstance(raid_response, Exception):
                        raise raid_response
                    response.update(raid_response)
                return {"data": [response]}

            auth.helix_post.side_effect = post
            stop = Mock()
            stop.is_set.return_value = False
            stop.wait.return_value = True
            notices = self.run_worker(auth, stop)
            events = []
            state = SessionReadiness(specs(), emit=events.append)
            for notice in notices:
                state.setup_result(notice)
            self.assertIn(expected, events)
            self.assertIn("follows_subscription_enabled", events)
            self.assertNotIn("synthetic", repr(events))
            self.assertEqual(auth.helix_post.call_count, 3)  # No blind retry.


class ProbeCLITests(unittest.TestCase):
    def test_socket_disables_client_ping_and_private_library_logs(self):
        with patch.object(probe, "connect") as mocked:
            probe.open_socket()
        options = mocked.call_args.kwargs
        self.assertIsNone(options["ping_interval"])
        self.assertTrue(options["logger"].disabled)
        self.assertEqual(options["max_queue"], 16)

    def test_signals_and_private_startup_failures(self):
        handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        output = io.StringIO()
        with patch.object(probe.TokenManager, "load", side_effect=ValueError("synthetic-private")), redirect_stdout(output):
            self.assertEqual(probe.main([]), 1)
        self.assertNotIn("synthetic-private", output.getvalue())

        def run(socket, auth, specs, stop, **kwargs):
            for sig in handlers:
                stop.clear()
                signal.getsignal(sig)(sig, None)
                self.assertTrue(stop.is_set())
            return 0

        with patch.object(probe.TokenManager, "load"), patch.object(probe, "prepare_subscriptions"), \
                patch.object(probe, "open_socket"), patch.object(probe, "run_session", side_effect=run):
            self.assertEqual(probe.main([]), 0)
        self.assertEqual({sig: signal.getsignal(sig) for sig in handlers}, handlers)


class LocalWebSocketTests(unittest.TestCase):
    def test_real_library_receives_frames_and_answers_server_ping(self):
        """Loopback-only protocol check: synthetic API, no Twitch or credentials."""
        pong_received = threading.Event()
        server_errors = []

        def handler(socket):
            try:
                socket.send(welcome())
                if socket.ping(b"synthetic-ping").wait(2):
                    pong_received.set()
                socket.send(frame("session_keepalive", {}))
                try:
                    socket.recv(timeout=2)
                    server_errors.append("Unexpected client application message")
                except ConnectionClosed:
                    pass
            except Exception:
                server_errors.append("Synthetic server failure")

        logger = logging.Logger("synthetic-websocket-test")
        logger.disabled = True
        with serve(handler, "127.0.0.1", 0, ping_interval=None, logger=logger) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                port = server.socket.getsockname()[1]
                socket = connect(f"ws://127.0.0.1:{port}", ping_interval=None, proxy=None, logger=logger)
                events = []
                result = probe.run_session(socket, Mock(), specs(), threading.Event(), duration=0.5,
                                           emit=events.append, worker_factory=FakeWorker)
            finally:
                server.shutdown()
                thread.join(2)
        self.assertEqual(result, 0)
        self.assertTrue(pong_received.is_set())
        self.assertEqual(server_errors, [])
