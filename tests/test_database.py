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
        for name in ("001_create_viewer_snapshots.sql", "002_create_streams.sql"):
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
