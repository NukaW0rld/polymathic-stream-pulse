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
                     "006_create_collector_runs.sql", "007_create_collection_health.sql"):
            ddl = (sql_dir / name).read_text().replace("CREATE TABLE ", "CREATE TEMP TABLE ")
            self.connection.execute(ddl)
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
        self.connection.execute("DROP TABLE pg_temp.viewer_snapshots, pg_temp.streams")
        with self.assertRaises(StorageError) as caught:
            self.writer.mark_stream_offline(
                stream_id="synthetic-private-value", offline_observed_at=self.observed,
            )
        self.assertNotIn("synthetic-private-value", str(caught.exception))
        self.assertEqual(self.connection.execute("SELECT 1").fetchone(), (1,))

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

    def test_health_rejects_mismatched_codes_and_unimplemented_sources_privately(self):
        from scripts.database import StorageError
        run_id = self.writer.start_collector_run(started_at=self.started)
        for changes in ({"reason_code": "network_error"}, {"source": "chat"},
                        {"reason_code": "synthetic-private-detail"}, {"status": []}):
            params = dict(run_id=run_id, source="stream_poll", observed_at=self.observed,
                          status="healthy", reason_code="live_poll_saved")
            params.update(changes)
            with self.assertRaises(StorageError) as caught:
                self.writer.record_collection_health(**params)
            self.assertNotIn("synthetic-private-detail", str(caught.exception))
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM pg_temp.collection_health"
        ).fetchone(), (0,))

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
