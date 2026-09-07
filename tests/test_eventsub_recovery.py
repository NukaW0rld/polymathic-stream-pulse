"""Recovery tests use synthetic sockets/clocks; no Twitch or saved credentials."""

import json
import logging
import threading
import unittest
from unittest.mock import Mock

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect
from websockets.sync.server import serve

import test_eventsub as fixtures
from scripts import check_eventsub as probe
from scripts.eventsub import SetupNotice
from scripts.eventsub_recovery import ConnectionJob, ConnectionResult
from scripts.twitch_auth import TwitchError


RECONNECT_URL = "wss://eventsub.wss.twitch.tv/ws?synthetic-private=a%2Fb&x=1"


def reconnect():
    return fixtures.frame("session_reconnect", {"session": {
        "id": "synthetic-session", "status": "reconnecting", "reconnect_url": RECONNECT_URL,
    }})


def welcome(session_id):
    raw = json.loads(fixtures.welcome())
    raw["payload"]["session"]["id"] = session_id
    return json.dumps(raw)


class Stop:
    def __init__(self, seconds):
        self.seconds = seconds
        self.stopped = False

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, duration):
        self.seconds[0] += duration
        return self.stopped


class Worker(fixtures.FakeWorker):
    def __init__(self, *args):
        super().__init__(*args)
        self.setup_complete = threading.Event()
        self.setup_complete.set()


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.seconds = [0]
        self.events = []
        self.workers = []
        self.jobs = []
        self.attempts = []
        self.stop = Stop(self.seconds)
        self.auth = Mock()

    def socket(self, frames):
        return fixtures.FakeSocket(frames, self.seconds)

    def worker(self, auth, specs, session_id, stop):
        worker = Worker(auth, specs, session_id, stop)
        self.workers.append((session_id, tuple(spec.source for spec in specs), worker))
        return worker

    def factory(self, outcomes):
        outcomes = iter(outcomes)
        owner = self

        class Job:
            def __init__(self, connector, url, auth, clock):
                self.url = url
                self.auth = auth
                self.delay, self.socket, self.error = next(outcomes)
                self.started = owner.seconds[0]
                self.finished_at = self.started + self.delay
                owner.jobs.append(self)
                owner.attempts.append((url, auth, self.started))

            def start(self):
                pass

            @property
            def done(self):
                return owner.seconds[0] >= self.finished_at

            def finish(self):
                owner.seconds[0] = max(owner.seconds[0], self.finished_at)
                return ConnectionResult(self.socket, welcome("synthetic-replacement"),
                                        fixtures.at(self.finished_at), self.error)

        return Job

    def run_probe(self, socket, outcomes, duration=8, worker=None):
        result = probe.run_session(
            socket, self.auth, fixtures.specs(), self.stop, duration=duration,
            emit=self.events.append, clock=lambda: fixtures.at(self.seconds[0]),
            worker_factory=worker or self.worker, connector=Mock(), job_factory=self.factory(outcomes),
        )
        self.assertNotIn("synthetic-private", repr(self.events))
        return result

    def test_handover_reads_old_socket_and_preserves_subscriptions_without_posts(self):
        spec = fixtures.specs()[0]
        notification = fixtures.frame("notification", {"subscription": fixtures.subscription(spec), "event": {}}, spec)
        old = self.socket([fixtures.welcome(), fixtures.frame("session_keepalive", {}), reconnect(), notification])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(1, new, None)]), 0)
        self.assertEqual(len(self.workers), 1)
        self.assertIsNone(self.attempts[0][1])  # Handover does not touch authorization.
        self.assertEqual(self.attempts[0][0], RECONNECT_URL)
        self.assertLess(self.events.index("chat_notification_envelope_received"),
                        self.events.index("handover_complete_subscriptions_preserved"))
        self.assertNotIn("probe_gap_detected_no_replay", self.events)
        self.assertTrue(old.closed and new.closed)

    def test_revocation_during_handover_is_not_lost(self):
        spec = fixtures.specs()[2]
        revoked = fixtures.frame("revocation", {"subscription": fixtures.subscription(spec) | {
            "status": "authorization_revoked",
        }}, spec)
        old = self.socket([fixtures.welcome(), reconnect(), revoked])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(0, new, None)]), 1)
        self.assertIn("follows_subscription_revoked", self.events)
        self.assertIn("handover_complete_subscriptions_preserved", self.events)
        self.assertEqual(len(self.workers), 1)

    def test_disconnect_recreates_subscriptions_and_keeps_gap_in_final_result(self):
        old = self.socket([fixtures.welcome(), OSError("synthetic-private-error")])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(0, new, None)]), 0)
        self.assertEqual(len(self.workers), 2)
        self.assertTrue(self.workers[0][2].finished)
        self.assertIs(self.attempts[0][1], self.auth)
        self.assertIn("probe_gap_detected_no_replay", self.events)
        self.assertIn("probe_subscriptions_reconfirmed_after_gap", self.events)
        self.assertIn("probe_finished_subscriptions_confirmed_after_gap", self.events)
        self.assertTrue(old.closed and new.closed)

    def test_failed_source_stays_failed_while_others_reconnect(self):
        def partial_worker(*args):
            worker = self.worker(*args)
            if len(self.workers) == 1:
                worker.notices[-1] = SetupNotice(source="follows", error="auth_error", http_status=403)
            return worker

        old = self.socket([fixtures.welcome(), OSError()])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(0, new, None)], worker=partial_worker), 1)
        self.assertEqual(self.workers[1][1], ("chat", "raids"))
        self.assertIn("follows_auth_error_http_403", self.events)
        self.assertNotIn("follows_subscription_enabled", self.events)

    def test_failed_handover_falls_back_to_new_session_with_gap(self):
        old = self.socket([fixtures.welcome(), reconnect()])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(0, None, "connection_attempt_failed"), (0, new, None)]), 0)
        self.assertIn("handover_failed_gap", self.events)
        self.assertIn("probe_finished_subscriptions_confirmed_after_gap", self.events)
        self.assertEqual(len(self.workers), 2)

    def test_repeated_connection_failure_uses_capped_backoff(self):
        old = self.socket([fixtures.welcome(), OSError()])
        self.assertEqual(self.run_probe(old, [(0, None, "connection_attempt_failed")] * 8, duration=100), 1)
        waits = [event for event in self.events if event.startswith("reconnect_wait_")]
        self.assertEqual(waits[:7], [f"reconnect_wait_{delay}_seconds" for delay in (1, 2, 4, 8, 16, 30, 30)])
        self.assertEqual(self.events.count("probe_gap_detected_no_replay"), 1)

    def test_duration_expiring_during_handover_closes_both_sockets(self):
        old = self.socket([fixtures.welcome(), reconnect()])
        new = self.socket([])
        self.assertEqual(self.run_probe(old, [(10, new, None)], duration=2), 1)
        self.assertTrue(old.closed and new.closed)
        self.assertNotIn("handover_complete_subscriptions_preserved", self.events)

    def test_duplicate_directed_request_does_not_open_another_socket(self):
        old = self.socket([fixtures.welcome(), reconnect(), reconnect()])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(1, new, None)]), 0)
        self.assertEqual(len(self.jobs), 1)

    def test_reconnect_during_setup_uses_gap_instead_of_ambiguous_transfer(self):
        def pending(*args):
            worker = self.worker(*args)
            worker.setup_complete.clear()
            return worker

        old = self.socket([fixtures.welcome(), reconnect()])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(0, new, None)], worker=pending), 0)
        self.assertIn("handover_during_setup_gap", self.events)
        self.assertIs(self.attempts[0][1], self.auth)

    def test_shared_authorization_failure_during_reconnect_is_terminal(self):
        old = self.socket([fixtures.welcome(), OSError()])
        self.assertEqual(self.run_probe(old, [(0, None, "authorization_check_failed")]), 1)
        self.assertEqual(len(self.jobs), 1)
        self.assertIn("authorization_check_failed", self.events)

    def test_late_successes_from_lost_session_are_discarded_before_new_setup(self):
        def delayed(*args):
            worker = self.worker(*args)
            if len(self.workers) == 1:
                pending, worker.notices = worker.notices, []

                def finish():
                    worker.finished = True
                    worker.notices.extend(pending)

                worker.finish = finish
            return worker

        old = self.socket([fixtures.welcome(), OSError()])
        new = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(0, new, None)], worker=delayed), 0)
        for source in ("chat", "raids", "follows"):
            self.assertEqual(self.events.count(f"{source}_subscription_enabled"), 1)

    def test_handover_deadline_failure_closes_candidate_and_uses_cold_recovery(self):
        old = self.socket([fixtures.welcome(), reconnect()])
        abandoned = self.socket([])
        replacement = self.socket([fixtures.frame("session_keepalive", {})])
        self.assertEqual(self.run_probe(old, [(26, abandoned, None), (0, replacement, None)], duration=40), 0)
        self.assertTrue(abandoned.closed)
        self.assertIn("handover_timeout", self.events)
        self.assertIn("probe_finished_subscriptions_confirmed_after_gap", self.events)

    def test_stop_during_backoff_does_not_dispatch_a_connection(self):
        def stop_wait(duration):
            self.stop.set()
            return True

        self.stop.wait = stop_wait
        old = self.socket([fixtures.welcome(), OSError()])
        self.assertEqual(self.run_probe(old, []), 1)
        self.assertEqual(self.attempts, [])

    def test_clock_uncertainty_is_terminal_even_with_recovery_enabled(self):
        def rollback():
            self.seconds[0] -= 1
            return fixtures.frame("session_keepalive", {})

        old = self.socket([fixtures.welcome(), rollback])
        self.assertEqual(self.run_probe(old, []), 1)
        self.assertIn("clock_uncertain", self.events)
        self.assertEqual(self.attempts, [])


class ConnectionJobTests(unittest.TestCase):
    def test_cold_connection_forces_validation_before_dial_and_handover_does_not(self):
        calls = []
        auth = Mock()
        auth.validate_if_due.side_effect = lambda **_: calls.append("validation")
        socket = Mock()
        socket.recv.return_value = fixtures.welcome()
        connector = Mock(side_effect=lambda _: calls.append("connect") or socket)
        for token_manager in (auth, None):
            job = ConnectionJob(connector, RECONNECT_URL, token_manager, fixtures.at)
            job.start()
            result = job.finish()
            self.assertIsNone(result.error)
        self.assertEqual(calls, ["validation", "connect", "connect"])
        auth.validate_if_due.assert_called_once_with(force=True)

    def test_auth_failure_prevents_connection_and_suppresses_private_details(self):
        auth = Mock()
        auth.validate_if_due.side_effect = TwitchError("synthetic-private", fatal=True)
        connector = Mock()
        job = ConnectionJob(connector, RECONNECT_URL, auth, fixtures.at)
        job.start()
        result = job.finish()
        self.assertEqual(result.error, "authorization_check_failed")
        self.assertNotIn("synthetic-private", repr(result))
        connector.assert_not_called()


class LocalRecoveryTests(unittest.TestCase):
    def test_real_two_socket_handover_preserves_subscriptions(self):
        """Both sockets use loopback; the mocked Helix client never contacts Twitch."""
        created = threading.Event()
        workers = []
        errors = []
        events = []
        auth = Mock()

        def post(endpoint, body):
            source = {"channel.chat.message": "chat", "channel.raid": "raids", "channel.follow": "follows"}[body["type"]]
            return {"data": [body | {"id": f"synthetic-{source}", "status": "enabled"}]}

        auth.helix_post.side_effect = post

        def worker_factory(*args):
            worker = probe.SetupWorker(*args)
            workers.append(worker)
            created.set()
            return worker

        def handler(socket):
            try:
                if socket.request.path == "/old":
                    socket.send(fixtures.welcome())
                    if not created.wait(2) or not workers[0].setup_complete.wait(2):
                        raise AssertionError("Synthetic setup timeout")
                    socket.send(fixtures.frame("session_keepalive", {}))
                    socket.send(reconnect())
                else:
                    socket.send(welcome("synthetic-replacement"))
                    socket.send(fixtures.frame("session_keepalive", {}))
                try:
                    socket.recv(timeout=3)
                    errors.append("Unexpected client application message")
                except ConnectionClosed:
                    pass
            except Exception:
                errors.append("Synthetic socket handler failure")

        logger = logging.Logger("synthetic-recovery-test")
        logger.disabled = True
        with serve(handler, "127.0.0.1", 0, ping_interval=None, logger=logger) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                port = server.socket.getsockname()[1]

                def local_connector(url):
                    self.assertEqual(url, RECONNECT_URL)
                    return connect(f"ws://127.0.0.1:{port}/replacement", ping_interval=None,
                                   proxy=None, logger=logger)

                old = connect(f"ws://127.0.0.1:{port}/old", ping_interval=None, proxy=None, logger=logger)
                result = probe.run_session(old, auth, fixtures.specs(), threading.Event(), duration=1,
                                           emit=events.append, connector=local_connector,
                                           worker_factory=worker_factory)
            finally:
                server.shutdown()
                thread.join(2)
        self.assertEqual(result, 0)
        self.assertEqual(auth.helix_post.call_count, 3)
        self.assertEqual(len(workers), 1)
        self.assertIn("handover_complete_subscriptions_preserved", events)
        self.assertNotIn("probe_gap_detected_no_replay", events)
        self.assertEqual(errors, [])
