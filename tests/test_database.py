"""Opt-in PostgreSQL integration tests using synthetic session-temporary tables."""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import unittest


@unittest.skipUnless(os.environ.get("STREAM_PULSE_TEST_POSTGRES") == "1",
                     "Set STREAM_PULSE_TEST_POSTGRES=1 to test local PostgreSQL")
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from scripts.database import DatabaseWriter

        self.connection = psycopg.connect(
            dbname="stream_pulse", host="/var/run/postgresql", autocommit=True,
            connect_timeout=5, options="-c search_path=pg_temp -c statement_timeout=10000",
        )
        self.addCleanup(self.connection.close)
        # Apply the actual definitions to temporary tables only. With pg_temp as
        # the sole search path, missing tables cannot fall back to public data.
        sql_dir = Path(__file__).resolve().parents[1] / "sql"
        for name in ("001_create_viewer_snapshots.sql", "002_create_streams.sql",
                     "004_create_incoming_raids.sql", "005_create_follow_events.sql",
                     "006_create_collector_runs.sql", "007_create_collection_health.sql",
                     "009_create_reconnection_gaps.sql"):
            ddl = (sql_dir / name).read_text().replace("CREATE TABLE ", "CREATE TEMP TABLE ")
            self.connection.execute(ddl)
        # The real migration resolves only to our session-temporary table.
        self.connection.execute((sql_dir / "008_add_paused_health_status.sql").read_text())
        self.writer = DatabaseWriter(self.connection)
        self.started = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
        self.observed = self.started + timedelta(minutes=10)

    def write(self, stream_id="synthetic-stream", observed_at=None, viewer_count=100):
        self.writer.record_live_poll(
            stream_id=stream_id, started_at=self.started,
            observed_at=self.observed if observed_at is None else observed_at,
            viewer_count=viewer_count,
        )

    def counts(self):
        return self.connection.execute(
            "SELECT (SELECT count(*) FROM pg_temp.streams), "
            "(SELECT count(*) FROM pg_temp.viewer_snapshots)"
        ).fetchone()

    def follow(self, message_id="synthetic-follow-1", user_id="synthetic-user-1",
               followed_at=None, notification_at=None, received_at=None, **overrides):
        base = followed_at or self.observed
        return self.writer.record_follow_event(
            eventsub_message_id=message_id, user_id=user_id,
            followed_at=base,
            notification_at=notification_at or base + timedelta(seconds=1),
            received_at=received_at or base + timedelta(seconds=2),
            **overrides,
        )

    def raid(self, message_id="synthetic-raid-1", from_broadcaster_user_id="synthetic-raider-1",
             raid_viewer_count=50, notification_at=None, received_at=None, **overrides):
        return self.writer.record_raid_event(
            eventsub_message_id=message_id,
            from_broadcaster_user_id=from_broadcaster_user_id,
            raid_viewer_count=raid_viewer_count,
            notification_at=notification_at or self.observed,
            received_at=received_at or self.observed + timedelta(seconds=2),
            **overrides,
        )

    def test_first_poll_commits_both_rows_and_returns_idle(self):
        from psycopg.pq import TransactionStatus
        self.write(viewer_count=0)
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        self.assertEqual(self.counts(), (1, 1))
        row = self.connection.execute(
            "SELECT first_observed_at, offline_observed_at FROM pg_temp.streams"
        ).fetchone()
        self.assertEqual(row, (self.observed, None))

    def test_retries_skip_duplicates_and_later_polls_preserve_first_observed(self):
        self.write()
        self.write()
        self.assertEqual(self.counts(), (1, 1))
        self.write(observed_at=self.observed + timedelta(seconds=60), viewer_count=101)
        self.assertEqual(self.counts(), (1, 2))
        self.assertEqual(self.connection.execute(
            "SELECT first_observed_at FROM pg_temp.streams"
        ).fetchone()[0], self.observed)

    def test_snapshot_constraint_failure_rolls_back_new_stream_and_connection_recovers(self):
        from scripts.database import StorageError
        with self.assertRaises(StorageError) as caught:
            self.write(stream_id="synthetic-private-value", viewer_count=-1)
        self.assertNotIn("synthetic-private-value", str(caught.exception))
        self.assertEqual(self.counts(), (0, 0))
        self.write()
        self.assertEqual(self.counts(), (1, 1))

    def test_failed_later_poll_keeps_previous_records(self):
        from scripts.database import StorageError
        self.write()
        with self.assertRaises(StorageError):
            self.write(observed_at=self.observed + timedelta(seconds=60), viewer_count=-1)
        self.assertEqual(self.counts(), (1, 1))

    def test_outer_transaction_is_rejected_instead_of_reporting_uncommitted_success(self):
        from scripts.database import StorageError
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.write()
        self.assertEqual(self.counts(), (0, 0))

    def test_naive_time_and_boolean_count_are_rejected(self):
        from scripts.database import StorageError
        for params in ({"observed_at": datetime(2026, 9, 6)}, {"viewer_count": True}):
            with self.assertRaises(StorageError):
                self.write(**params)
        self.assertEqual(self.counts(), (0, 0))

    def test_offline_detection_targets_one_stream_and_preserves_first_detection(self):
        from psycopg.pq import TransactionStatus
        self.write()
        self.write(stream_id="synthetic-other")
        detected = self.observed + timedelta(hours=1)
        self.assertEqual(self.writer.mark_stream_offline(
            stream_id="synthetic-stream", offline_observed_at=detected,
        ), 1)
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        self.assertEqual(self.writer.mark_stream_offline(
            stream_id="synthetic-stream", offline_observed_at=detected + timedelta(minutes=1),
        ), 0)
        self.assertEqual(self.connection.execute(
            "SELECT stream_id, offline_observed_at FROM pg_temp.streams ORDER BY stream_id"
        ).fetchall(), [("synthetic-other", None), ("synthetic-stream", detected)])
        self.assertEqual(self.counts(), (2, 2))

    def test_offline_detection_for_absent_stream_creates_no_records(self):
        self.assertEqual(self.writer.mark_stream_offline(
            stream_id="synthetic-absent", offline_observed_at=self.observed,
        ), 0)
        self.assertEqual(self.counts(), (0, 0))

    def test_offline_rejects_naive_timestamp_and_outer_transaction(self):
        from scripts.database import StorageError
        with self.assertRaises(StorageError):
            self.writer.mark_stream_offline(
                stream_id="synthetic-stream", offline_observed_at=datetime(2026, 9, 6),
            )
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.mark_stream_offline(
                    stream_id="synthetic-stream", offline_observed_at=self.observed,
                )

    def test_offline_database_error_is_safe_and_connection_recovers(self):
        from scripts.database import StorageError
        # Remove only the session's synthetic tables to exercise a real DB error.
        # CASCADE also clears the follow_events FK that now references streams.
        self.connection.execute("DROP TABLE pg_temp.viewer_snapshots, pg_temp.streams CASCADE")
        with self.assertRaises(StorageError) as caught:
            self.writer.mark_stream_offline(
                stream_id="synthetic-private-value", offline_observed_at=self.observed,
            )
        self.assertNotIn("synthetic-private-value", str(caught.exception))
        self.assertEqual(self.connection.execute("SELECT 1").fetchone(), (1,))

    def test_follow_event_stores_once_with_null_stream_and_skips_redelivery(self):
        from psycopg.pq import TransactionStatus
        self.assertEqual(self.follow(), 1)
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        # A redelivery carries the same message ID but a later receipt; it must not
        # overwrite the first-stored row.
        self.assertEqual(self.follow(
            user_id="synthetic-user-changed",
            received_at=self.observed + timedelta(minutes=5),
        ), 0)
        self.assertEqual(self.connection.execute(
            "SELECT eventsub_message_id, stream_id, user_id, followed_at, notification_at, received_at "
            "FROM pg_temp.follow_events"
        ).fetchall(), [(
            "synthetic-follow-1", None, "synthetic-user-1",
            self.observed, self.observed + timedelta(seconds=1), self.observed + timedelta(seconds=2),
        )])

    def test_follow_event_keeps_distinct_notifications_including_same_user(self):
        # user_id is deliberately not unique: a genuine unfollow/refollow arrives
        # as a second notification with a new message ID and is kept.
        self.assertEqual(self.follow(message_id="synthetic-follow-a"), 1)
        self.assertEqual(self.follow(
            message_id="synthetic-follow-b",
            followed_at=self.observed + timedelta(hours=2),
        ), 1)
        self.assertEqual(self.connection.execute(
            "SELECT count(*), count(DISTINCT user_id) FROM pg_temp.follow_events"
        ).fetchone(), (2, 1))

    def test_follow_event_accepts_optional_existing_stream_association(self):
        self.write(stream_id="synthetic-stream")
        self.assertEqual(self.follow(stream_id="synthetic-stream"), 1)
        self.assertEqual(self.connection.execute(
            "SELECT stream_id FROM pg_temp.follow_events"
        ).fetchone(), ("synthetic-stream",))

    def test_follow_event_rejects_naive_times_empty_ids_and_bad_stream(self):
        from scripts.database import StorageError
        rejected = (
            {"message_id": ""}, {"user_id": ""},
            {"followed_at": datetime(2026, 9, 6)},
            {"notification_at": datetime(2026, 9, 6)},
            {"received_at": datetime(2026, 9, 6)},
            {"stream_id": ""}, {"stream_id": 5},
        )
        for changes in rejected:
            with self.subTest(changes=changes), self.assertRaises(StorageError):
                self.follow(**changes)
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.follow_events"
        ).fetchone(), (0,))

    def test_follow_event_rejects_outer_transaction(self):
        from scripts.database import StorageError
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.follow()
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.follow_events"
        ).fetchone(), (0,))

    def test_follow_event_database_error_is_safe_and_connection_recovers(self):
        from scripts.database import StorageError
        self.connection.execute("DROP TABLE pg_temp.follow_events")
        with self.assertRaises(StorageError) as caught:
            self.follow(user_id="synthetic-private-value")
        self.assertNotIn("synthetic-private-value", str(caught.exception))
        self.assertEqual(self.connection.execute("SELECT 1").fetchone(), (1,))

    def test_raid_event_stores_once_with_null_stream_and_skips_redelivery(self):
        from psycopg.pq import TransactionStatus
        self.assertEqual(self.raid(), 1)
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        self.assertEqual(self.raid(raid_viewer_count=999,
                                   received_at=self.observed + timedelta(minutes=5)), 0)
        self.assertEqual(self.connection.execute(
            "SELECT eventsub_message_id, stream_id, from_broadcaster_user_id, raid_viewer_count "
            "FROM pg_temp.incoming_raids"
        ).fetchall(), [("synthetic-raid-1", None, "synthetic-raider-1", 50)])

    def test_raid_event_accepts_zero_viewers_and_optional_stream(self):
        self.write(stream_id="synthetic-stream")
        self.assertEqual(self.raid(raid_viewer_count=0, stream_id="synthetic-stream"), 1)
        self.assertEqual(self.connection.execute(
            "SELECT raid_viewer_count, stream_id FROM pg_temp.incoming_raids"
        ).fetchone(), (0, "synthetic-stream"))

    def test_raid_event_rejects_bad_counts_naive_times_and_empty_ids(self):
        from scripts.database import StorageError
        rejected = (
            {"message_id": ""}, {"from_broadcaster_user_id": ""},
            {"raid_viewer_count": -1}, {"raid_viewer_count": True},
            {"raid_viewer_count": 2 ** 31}, {"raid_viewer_count": 1.0},
            {"notification_at": datetime(2026, 9, 6)},
            {"received_at": datetime(2026, 9, 6)},
            {"stream_id": ""}, {"stream_id": 5},
        )
        for changes in rejected:
            with self.subTest(changes=changes), self.assertRaises(StorageError):
                self.raid(**changes)
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.incoming_raids"
        ).fetchone(), (0,))

    def test_raid_event_database_error_is_safe_and_connection_recovers(self):
        from scripts.database import StorageError
        self.connection.execute("DROP TABLE pg_temp.incoming_raids")
        with self.assertRaises(StorageError) as caught:
            self.raid(from_broadcaster_user_id="synthetic-private-value")
        self.assertNotIn("synthetic-private-value", str(caught.exception))
        self.assertEqual(self.connection.execute("SELECT 1").fetchone(), (1,))

    def test_follow_sink_persists_events_and_follows_health_transitions(self):
        from scripts.event_sink import FollowSink

        run_id = self.writer.start_collector_run(started_at=self.started)
        events = []
        sink = FollowSink(self.writer, run_id, emit=events.append)

        def message(message_id, user_id="synthetic-user-1"):
            return {
                "metadata": {
                    "message_id": message_id, "message_type": "notification",
                    "message_timestamp": "2026-09-06T12:09:59Z",
                    "subscription_type": "channel.follow", "subscription_version": "2",
                },
                "payload": {
                    "subscription": {"id": "s", "type": "channel.follow", "version": "2"},
                    "event": {"user_id": user_id, "followed_at": "2026-09-06T12:09:58Z"},
                },
            }

        at = lambda n: self.observed + timedelta(seconds=n)
        sink.begin(at(0))
        sink.transport_ready(at(1))
        self.assertTrue(sink.submit(message("m1"), at(2), at(2)))
        self.assertFalse(sink.submit(message("m1", user_id="synthetic-changed"), at(3), at(3)))
        broken = message("m2")
        broken["payload"]["event"].pop("followed_at")
        self.assertFalse(sink.submit(broken, at(4), at(4)))
        sink.transport_error("network_error", at(5))
        sink.transport_ready(at(6))
        self.assertTrue(sink.submit(message("m3"), at(7), at(7)))
        sink.stop(at(8))

        self.assertEqual(self.connection.execute(
            "SELECT eventsub_message_id, stream_id, user_id FROM pg_temp.follow_events ORDER BY eventsub_message_id"
        ).fetchall(), [("m1", None, "synthetic-user-1"), ("m3", None, "synthetic-user-1")])
        self.assertEqual(self.connection.execute(
            "SELECT source, status, reason_code FROM pg_temp.collection_health "
            "WHERE source = 'follows' ORDER BY health_id"
        ).fetchall(), [
            ("follows", "starting", "initializing"),
            ("follows", "healthy", "capture_ready"),
            ("follows", "error", "invalid_notification"),
            ("follows", "error", "network_error"),
            ("follows", "healthy", "capture_ready"),
            ("follows", "stopped", "orderly_shutdown"),
        ])

    def test_raid_sink_persists_events_and_raids_health_transitions(self):
        from scripts.event_sink import RaidSink

        run_id = self.writer.start_collector_run(started_at=self.started)
        sink = RaidSink(self.writer, run_id, emit=lambda _: None)

        def message(message_id, viewers=75):
            return {
                "metadata": {
                    "message_id": message_id, "message_type": "notification",
                    "message_timestamp": "2026-09-06T12:09:59Z",
                    "subscription_type": "channel.raid", "subscription_version": "1",
                },
                "payload": {
                    "subscription": {"id": "s", "type": "channel.raid", "version": "1"},
                    "event": {"from_broadcaster_user_id": "synthetic-raider-1", "viewers": viewers},
                },
            }

        at = lambda n: self.observed + timedelta(seconds=n)
        sink.begin(at(0))
        sink.transport_ready(at(1))
        self.assertTrue(sink.submit(message("r1"), at(2), at(2)))
        self.assertFalse(sink.submit(message("r1", viewers=999), at(3), at(3)))
        sink.stop(at(4))

        self.assertEqual(self.connection.execute(
            "SELECT eventsub_message_id, stream_id, from_broadcaster_user_id, raid_viewer_count "
            "FROM pg_temp.incoming_raids"
        ).fetchall(), [("r1", None, "synthetic-raider-1", 75)])
        self.assertEqual(self.connection.execute(
            "SELECT source, status, reason_code FROM pg_temp.collection_health "
            "WHERE source = 'raids' ORDER BY health_id"
        ).fetchall(), [
            ("raids", "starting", "initializing"),
            ("raids", "healthy", "capture_ready"),
            ("raids", "stopped", "orderly_shutdown"),
        ])

    def test_reconnection_gap_opens_with_null_recovery_then_resolves_once(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        detected = self.observed
        gap_id = self.writer.record_reconnection_gap(
            run_id=run_id, detected_at=detected, reason_code="keepalive_timeout")
        self.assertIsInstance(gap_id, int)
        self.assertEqual(self.connection.execute(
            "SELECT run_id, detected_at, recovered_at, reason_code FROM pg_temp.reconnection_gaps"
        ).fetchone(), (run_id, detected, None, "keepalive_timeout"))
        recovered = detected + timedelta(seconds=12)
        self.assertEqual(self.writer.resolve_reconnection_gap(
            gap_id=gap_id, recovered_at=recovered), 1)
        self.assertEqual(self.writer.resolve_reconnection_gap(
            gap_id=gap_id, recovered_at=recovered + timedelta(seconds=1)), 0)
        self.assertEqual(self.connection.execute(
            "SELECT recovered_at FROM pg_temp.reconnection_gaps"
        ).fetchone(), (recovered,))
        # The CHECK forbids recovery before detection.
        second = self.writer.record_reconnection_gap(
            run_id=run_id, detected_at=detected, reason_code="network_error")
        with self.assertRaises(StorageError):
            self.writer.resolve_reconnection_gap(
                gap_id=second, recovered_at=detected - timedelta(seconds=1))

    def test_reconnection_gap_validates_inputs_and_unknown_reason(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        for changes in ({"detected_at": datetime(2026, 9, 6)}, {"run_id": True},
                        {"reason_code": ""}):
            params = dict(run_id=run_id, detected_at=self.observed, reason_code="network_error")
            params.update(changes)
            with self.assertRaises(StorageError):
                self.writer.record_reconnection_gap(**params)
        with self.assertRaises(StorageError):  # reason not in the table CHECK
            self.writer.record_reconnection_gap(
                run_id=run_id, detected_at=self.observed, reason_code="mystery")
        for bad in ({"gap_id": 0}, {"recovered_at": datetime(2026, 9, 6)}):
            params = dict(gap_id=1, recovered_at=self.observed)
            params.update(bad)
            with self.assertRaises(StorageError):
                self.writer.resolve_reconnection_gap(**params)
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.record_reconnection_gap(
                    run_id=run_id, detected_at=self.observed, reason_code="network_error")
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.reconnection_gaps"
        ).fetchone(), (0,))

    def test_router_records_and_resolves_a_reconnection_gap(self):
        from scripts.collect_stream import ClockReading
        from scripts.event_sink import FollowSink, RaidSink
        from scripts.eventsub_router import EventRouter

        run_id = self.writer.start_collector_run(started_at=self.started)
        router = EventRouter(
            {"raids": RaidSink(self.writer, run_id, emit=lambda _: None),
             "follows": FollowSink(self.writer, run_id, emit=lambda _: None)},
            emit=lambda _: None, writer=self.writer, run_id=run_id,
        )

        class Session:
            def __init__(self, *ready):
                self.ready = set(ready)

            def source_ready(self, source):
                return source in self.ready

        at = lambda n: ClockReading(self.observed + timedelta(seconds=n), float(n))
        router.begin(at(0))
        router.observe(Session("raids", "follows"), at(1))
        router.transport_lost("network_error", at(2))
        router.observe(Session("raids", "follows"), at(4))
        router.stop(at(5))
        self.assertEqual(self.connection.execute(
            "SELECT reason_code, detected_at, recovered_at FROM pg_temp.reconnection_gaps"
        ).fetchall(), [("network_error", at(2).utc, at(4).utc)])

    def test_close_collector_run_stops_run_without_writing_health(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        other = self.writer.start_collector_run(started_at=self.started)
        self.writer.update_collector_heartbeat(run_id=run_id, last_heartbeat_at=self.observed)
        stopped = self.observed + timedelta(seconds=5)
        self.writer.close_collector_run(run_id=run_id, stopped_at=stopped)
        self.assertEqual(self.connection.execute(
            "SELECT run_id, last_heartbeat_at, stopped_at FROM pg_temp.collector_runs ORDER BY run_id"
        ).fetchall(), [(run_id, self.observed, stopped), (other, self.started, None)])
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))
        for target, moment in ((run_id, stopped + timedelta(seconds=1)),      # already stopped
                               (run_id + 99, stopped),                        # missing run
                               (other, self.started - timedelta(seconds=1))):  # before its heartbeat
            with self.subTest(target=target), self.assertRaisesRegex(StorageError, "Run close rejected"):
                self.writer.close_collector_run(run_id=target, stopped_at=moment)

    def test_close_collector_run_validates_inputs_and_transaction_boundary(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        for bad in (datetime(2026, 9, 6), None):
            with self.assertRaises(StorageError):
                self.writer.close_collector_run(run_id=run_id, stopped_at=bad)
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.close_collector_run(run_id=run_id, stopped_at=self.observed)

    def test_stop_collector_run_multi_stops_run_and_writes_one_stopped_row_per_source(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        other = self.writer.start_collector_run(started_at=self.started)
        self.writer.update_collector_heartbeat(run_id=run_id, last_heartbeat_at=self.observed)
        stopped = self.observed + timedelta(seconds=5)
        self.writer.stop_collector_run_multi(
            run_id=run_id, stopped_at=stopped,
            sources=["follows", "raids", "stream_poll", "raids"],  # duplicates collapse
        )
        self.assertEqual(self.connection.execute(
            "SELECT run_id, stopped_at FROM pg_temp.collector_runs ORDER BY run_id"
        ).fetchall(), [(run_id, stopped), (other, None)])
        self.assertEqual(self.connection.execute(
            "SELECT source, status, reason_code, observed_at FROM pg_temp.collection_health "
            "WHERE run_id = %s ORDER BY health_id", (run_id,)
        ).fetchall(), [("follows", "stopped", "orderly_shutdown", stopped),
                       ("raids", "stopped", "orderly_shutdown", stopped),
                       ("stream_poll", "stopped", "orderly_shutdown", stopped)])
        for target, moment in ((run_id, stopped + timedelta(seconds=1)),       # already stopped
                               (run_id + 99, stopped),                         # missing run
                               (other, self.started - timedelta(seconds=1))):   # before its heartbeat
            with self.subTest(target=target), self.assertRaisesRegex(StorageError, "Shutdown rejected"):
                self.writer.stop_collector_run_multi(
                    run_id=target, stopped_at=moment, sources=["stream_poll"])

    def test_stop_collector_run_multi_is_atomic_and_recovers_when_a_health_insert_fails(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        self.connection.execute("DROP TABLE pg_temp.collection_health")
        with self.assertRaises(StorageError):
            self.writer.stop_collector_run_multi(
                run_id=run_id, stopped_at=self.observed, sources=["stream_poll", "raids"])
        # The run stop rolled back with the failed health insert.
        self.assertEqual(self.connection.execute(
            "SELECT stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (None,))
        self.assertEqual(self.connection.execute("SELECT 1").fetchone(), (1,))

    def test_stop_collector_run_multi_validates_sources_times_and_transaction_boundary(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        for sources in ([], ["stream_poll", "unknown_source"], ["chat", 7]):
            with self.subTest(sources=sources), \
                    self.assertRaisesRegex(StorageError, "shutdown source set"):
                self.writer.stop_collector_run_multi(
                    run_id=run_id, stopped_at=self.observed, sources=sources)
        for bad in (datetime(2026, 9, 6), None):
            with self.assertRaises(StorageError):
                self.writer.stop_collector_run_multi(
                    run_id=run_id, stopped_at=bad, sources=["stream_poll"])
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.stop_collector_run_multi(
                    run_id=run_id, stopped_at=self.observed, sources=["stream_poll"])
        # Nothing above stopped the run or wrote health.
        self.assertEqual(self.connection.execute(
            "SELECT stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (None,))
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))

    def test_run_start_returns_distinct_committed_ids_and_initial_checkins(self):
        from psycopg.pq import TransactionStatus
        first = self.writer.start_collector_run(started_at=self.started)
        second = self.writer.start_collector_run(started_at=self.started)
        self.assertNotEqual(first, second)
        self.assertEqual(self.connection.info.transaction_status, TransactionStatus.IDLE)
        self.assertEqual(self.connection.execute(
            "SELECT run_id, started_at, last_heartbeat_at, stopped_at "
            "FROM pg_temp.collector_runs ORDER BY run_id"
        ).fetchall(), [(first, self.started, self.started, None),
                       (second, self.started, self.started, None)])

    def test_heartbeat_advances_only_target_run_and_accepts_same_timestamp(self):
        first = self.writer.start_collector_run(started_at=self.started)
        second = self.writer.start_collector_run(started_at=self.started)
        for _ in range(2):
            self.writer.update_collector_heartbeat(run_id=first, last_heartbeat_at=self.observed)
        self.assertEqual(self.connection.execute(
            "SELECT run_id, last_heartbeat_at FROM pg_temp.collector_runs ORDER BY run_id"
        ).fetchall(), [(first, self.observed), (second, self.started)])

    def test_older_heartbeat_is_rejected_and_connection_recovers(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        self.writer.update_collector_heartbeat(run_id=run_id, last_heartbeat_at=self.observed)
        with self.assertRaisesRegex(StorageError, "Heartbeat rejected"):
            self.writer.update_collector_heartbeat(run_id=run_id, last_heartbeat_at=self.started)
        self.assertEqual(self.connection.execute(
            "SELECT last_heartbeat_at FROM pg_temp.collector_runs"
        ).fetchone(), (self.observed,))
        self.writer.update_collector_heartbeat(
            run_id=run_id, last_heartbeat_at=self.observed + timedelta(seconds=30),
        )

    def test_stopped_and_missing_runs_reject_heartbeats(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        self.writer.stop_collector_run(run_id=run_id, stopped_at=self.observed)
        for target in (run_id, run_id + 1):
            with self.assertRaisesRegex(StorageError, "Heartbeat rejected"):
                self.writer.update_collector_heartbeat(run_id=target, last_heartbeat_at=self.observed)
        self.assertEqual(self.connection.execute(
            "SELECT last_heartbeat_at, stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (self.started, self.observed))

    def test_run_operations_reject_naive_times_and_outer_transactions(self):
        from scripts.database import StorageError
        naive = datetime(2026, 9, 6)
        operations = (
            lambda: self.writer.start_collector_run(started_at=naive),
            lambda: self.writer.update_collector_heartbeat(run_id=1, last_heartbeat_at=naive),
            lambda: self.writer.update_collector_heartbeat(run_id=True, last_heartbeat_at=self.started),
        )
        for operation in operations:
            with self.assertRaises(StorageError):
                operation()
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.start_collector_run(started_at=self.started)
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.update_collector_heartbeat(run_id=1, last_heartbeat_at=self.started)
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collector_runs"
        ).fetchone(), (0,))

    def test_health_accepts_poll_combinations_and_keeps_periodic_observations(self):
        from scripts.database import POLL_HEALTH_REASONS
        run_id = self.writer.start_collector_run(started_at=self.started)
        expected = 0
        for status, reasons in POLL_HEALTH_REASONS.items():
            for reason in reasons:
                self.writer.record_collection_health(
                    run_id=run_id, source="stream_poll", observed_at=self.observed,
                    status=status, reason_code=reason,
                )
                expected += 1
        self.writer.record_collection_health(
            run_id=run_id, source="stream_poll", observed_at=self.observed + timedelta(seconds=60),
            status="healthy", reason_code="offline_poll_saved",
        )
        self.assertEqual(self.connection.execute(
            "SELECT count(*), count(DISTINCT health_id) FROM pg_temp.collection_health"
        ).fetchone(), (expected + 1, expected + 1))

    def test_health_rejects_mismatched_codes_and_unknown_sources_privately(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        for changes in ({"reason_code": "network_error"}, {"source": "chat"},
                        {"reason_code": "synthetic-private-detail"}, {"status": []},
                        {"source": []}, {"reason_code": []}, {"source": "unknown"}):
            params = dict(run_id=run_id, source="stream_poll", observed_at=self.observed,
                          status="healthy", reason_code="live_poll_saved")
            params.update(changes)
            with self.assertRaises(StorageError) as caught:
                self.writer.record_collection_health(**params)
            self.assertNotIn("synthetic-private-detail", str(caught.exception))
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))

    def test_eventsub_health_keeps_sources_independent_and_preserves_chat_pause(self):
        run_id = self.writer.start_collector_run(started_at=self.started)
        observations = [
            ("chat", "starting", "awaiting_stream_status"),
            ("chat", "paused", "offline_observed"),
            ("raids", "healthy", "capture_ready"),
            ("follows", "error", "subscription_revoked"),
            ("chat", "error", "poll_failed"),
            ("chat", "error", "poll_stale"),
            ("chat", "healthy", "capture_ready"),
        ]
        for offset, (source, status, reason) in enumerate(observations):
            self.writer.record_collection_health(
                run_id=run_id, source=source,
                observed_at=self.observed + timedelta(seconds=offset),
                status=status, reason_code=reason,
            )
        self.assertEqual(self.connection.execute(
            "SELECT source, status, reason_code FROM pg_temp.collection_health ORDER BY health_id"
        ).fetchall(), observations)
        # This checks storage of caller-supplied claims, not real readiness.
        self.assertEqual(self.counts(), (0, 0))

    def test_chat_only_reasons_cannot_be_used_for_polling_raids_or_follows(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        for source in ("stream_poll", "raids", "follows"):
            for status, reason in (("paused", "offline_observed"),
                                   ("starting", "awaiting_stream_status"),
                                   ("error", "poll_failed")):
                with self.subTest(source=source, reason=reason), self.assertRaises(StorageError):
                    self.writer.record_collection_health(
                        run_id=run_id, source=source, observed_at=self.observed,
                        status=status, reason_code=reason,
                    )
        for source in ("raids", "follows"):
            with self.assertRaises(StorageError):
                self.writer.record_collection_health(
                    run_id=run_id, source=source, observed_at=self.observed,
                    status="error", reason_code="poll_stale",
                )
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))

    def test_eventsub_transport_errors_and_shutdown_can_be_persisted(self):
        run_id = self.writer.start_collector_run(started_at=self.started)
        reasons = ("network_error", "keepalive_timeout", "subscription_error",
                   "subscription_revoked", "auth_error", "invalid_notification", "clock_uncertain")
        for source in ("chat", "raids", "follows"):
            for status, reason in (("starting", "initializing"),
                                   *(("error", reason) for reason in reasons),
                                   ("stopped", "orderly_shutdown")):
                self.writer.record_collection_health(
                    run_id=run_id, source=source, observed_at=self.observed,
                    status=status, reason_code=reason,
                )
        self.assertEqual(self.connection.execute(
            "SELECT source, count(*) FROM pg_temp.collection_health GROUP BY source ORDER BY source"
        ).fetchall(), [("chat", 9), ("follows", 9), ("raids", 9)])

    def test_health_missing_run_fails_safely_and_connection_recovers(self):
        from scripts.database import StorageError
        with self.assertRaisesRegex(StorageError, "Health storage failed"):
            self.writer.record_collection_health(
                run_id=123, source="stream_poll", observed_at=self.observed,
                status="starting", reason_code="initializing",
            )
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))
        self.writer.start_collector_run(started_at=self.started)

    def test_shutdown_preserves_heartbeat_targets_run_and_records_stopped_health(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        other = self.writer.start_collector_run(started_at=self.started)
        self.writer.update_collector_heartbeat(run_id=run_id, last_heartbeat_at=self.observed)
        stopped = self.observed + timedelta(seconds=10)
        self.writer.stop_collector_run(run_id=run_id, stopped_at=stopped)
        with self.assertRaisesRegex(StorageError, "Shutdown rejected"):
            self.writer.stop_collector_run(run_id=run_id, stopped_at=stopped + timedelta(seconds=1))
        self.assertEqual(self.connection.execute(
            "SELECT run_id, last_heartbeat_at, stopped_at FROM pg_temp.collector_runs ORDER BY run_id"
        ).fetchall(), [(run_id, self.observed, stopped), (other, self.started, None)])
        self.assertEqual(self.connection.execute(
            "SELECT run_id, source, observed_at, status, reason_code FROM pg_temp.collection_health"
        ).fetchall(), [(run_id, "stream_poll", stopped, "stopped", "orderly_shutdown")])
        self.assertEqual(self.counts(), (0, 0))

    def test_shutdown_rejects_missing_run_and_time_before_heartbeat(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        self.writer.update_collector_heartbeat(run_id=run_id, last_heartbeat_at=self.observed)
        for target in (run_id, run_id + 1):
            with self.assertRaisesRegex(StorageError, "Shutdown rejected"):
                self.writer.stop_collector_run(run_id=target, stopped_at=self.started)
        self.assertEqual(self.connection.execute(
            "SELECT stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (None,))
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))
        # Equality with the heartbeat is allowed by the table and query.
        self.writer.stop_collector_run(run_id=run_id, stopped_at=self.observed)

    def test_shutdown_health_failure_rolls_back_run_stop(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        self.connection.execute("DROP TABLE pg_temp.collection_health")
        with self.assertRaisesRegex(StorageError, "Shutdown storage failed"):
            self.writer.stop_collector_run(run_id=run_id, stopped_at=self.observed)
        self.assertEqual(self.connection.execute(
            "SELECT stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (None,))

    def test_health_and_shutdown_validate_inputs_and_transaction_boundary(self):
        from scripts.database import StorageError
        for changes in ({"run_id": True}, {"observed_at": datetime(2026, 9, 6)}):
            params = dict(run_id=1, observed_at=self.observed)
            params.update(changes)
            with self.assertRaises(StorageError):
                self.writer.record_collection_health(
                    **params, source="stream_poll", status="starting", reason_code="initializing",
                )
            with self.assertRaises(StorageError):
                self.writer.stop_collector_run(run_id=params["run_id"], stopped_at=params["observed_at"])
        with self.connection.transaction():
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.record_collection_health(
                    run_id=1, observed_at=self.observed, source="stream_poll",
                    status="starting", reason_code="initializing",
                )
            with self.assertRaisesRegex(StorageError, "idle autocommit"):
                self.writer.stop_collector_run(run_id=1, stopped_at=self.observed)

    def test_polling_runtime_persists_lifecycle_through_network_gap(self):
        from unittest.mock import Mock
        from scripts.collect_stream import ClockReading, PollingCollector, PollWorker, StreamObservation
        from scripts.twitch_auth import TwitchError

        seconds = [0]

        def clock():
            return ClockReading(self.observed + timedelta(seconds=seconds[0]), seconds[0])

        poller = Mock()
        poller.poll.side_effect = [
            StreamObservation("synthetic-stream", self.started, 100),
            TwitchError("Synthetic network failure.", reason_code="network_error"),
            None,
        ]
        worker = PollWorker(poller, clock)
        stop = Mock()
        stop.is_set.return_value = False

        def wait(_):
            if worker.busy:
                worker._thread.join(2)
                self.assertFalse(worker._thread.is_alive())
            seconds[0] += 15

        stop.wait.side_effect = wait
        collector = PollingCollector(self.writer, worker, clock=clock, emit=lambda _: None)
        self.assertEqual(collector.run(stop, duration=150), 0)
        self.assertEqual(self.counts(), (1, 1))
        self.assertEqual(self.connection.execute(
            "SELECT first_observed_at, offline_observed_at FROM pg_temp.streams"
        ).fetchone(), (self.observed, self.observed + timedelta(seconds=120)))
        self.assertEqual(self.connection.execute(
            "SELECT started_at, last_heartbeat_at, stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (self.observed, self.observed + timedelta(seconds=120),
                      self.observed + timedelta(seconds=150)))
        self.assertEqual([row[0] for row in self.connection.execute(
            "SELECT reason_code FROM pg_temp.collection_health ORDER BY health_id"
        ).fetchall()], ["initializing", "live_poll_saved", "network_error", "poll_stale",
                       "offline_poll_saved", "orderly_shutdown"])

    def test_runtime_health_failure_keeps_saved_snapshot_but_leaves_run_unclosed(self):
        from unittest.mock import Mock
        from scripts.collect_stream import ClockReading, PollingCollector, PollWorker, StreamObservation

        # Force only healthy writes to fail in the synthetic table. The already
        # committed snapshot must remain; the runtime must not claim shutdown.
        self.connection.execute(
            "ALTER TABLE pg_temp.collection_health ADD CHECK (status <> 'healthy')"
        )
        seconds = [0]
        clock = lambda: ClockReading(self.observed + timedelta(seconds=seconds[0]), seconds[0])
        worker = PollWorker(Mock(poll=Mock(return_value=StreamObservation(
            "synthetic-stream", self.started, 100,
        ))), clock)
        events = []
        collector = PollingCollector(self.writer, worker, clock=clock, emit=events.append)
        stop = Mock()
        stop.is_set.return_value = False

        def wait(_):
            if worker.busy:
                worker._thread.join(2)
                self.assertFalse(worker._thread.is_alive())
            seconds[0] += 1

        stop.wait.side_effect = wait
        self.assertEqual(collector.run(stop, duration=3), 1)
        self.assertEqual(self.counts(), (1, 1))
        self.assertEqual(self.connection.execute(
            "SELECT stopped_at FROM pg_temp.collector_runs"
        ).fetchone(), (None,))
        self.assertEqual(self.connection.execute(
            "SELECT reason_code FROM pg_temp.collection_health"
        ).fetchall(), [("initializing",)])
        self.assertNotIn("live_poll_saved", events)
        self.assertNotIn("orderly_shutdown", events)
