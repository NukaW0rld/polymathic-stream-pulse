"""Persist one successful live poll using the project's SQL query files."""

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.pq import TransactionStatus


QUERY_DIR = Path(__file__).resolve().parents[1] / "sql" / "queries"


class StorageError(Exception):
    """Safe storage diagnostic; never include database error details or row values."""


class DatabaseWriter:
    """Use one dedicated autocommit connection, serialized by the caller."""

    def __init__(self, connection):
        self._connection = connection
        try:
            self._insert_stream = (QUERY_DIR / "insert_stream.sql").read_text()
            self._insert_snapshot = (QUERY_DIR / "insert_viewer_snapshot.sql").read_text()
        except OSError:
            raise StorageError("Could not load database query files.") from None

    def record_live_poll(self, *, stream_id, started_at, observed_at, viewer_count):
        """Commit the stream and snapshot together. Preserve inputs on any retry.

        observed_at is captured by the caller when the API observation succeeds,
        not when these writes execute. No failed request should call this method.
        """
        if not isinstance(stream_id, str) or not stream_id:
            raise StorageError("Live poll requires a nonempty stream ID.")
        for value in (started_at, observed_at):
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise StorageError("Live poll timestamps must be timezone-aware datetimes.")
        if type(viewer_count) is not int:
            raise StorageError("Viewer count must be an integer.")

        connection = self._connection
        if connection.closed:
            raise StorageError("Database connection is closed.")
        if not connection.autocommit or connection.info.transaction_status != TransactionStatus.IDLE:
            raise StorageError("Database writer requires an idle autocommit connection.")

        params = {
            "stream_id": stream_id,
            "started_at": started_at,
            "first_observed_at": observed_at,
            "observed_at": observed_at,
            "viewer_count": viewer_count,
        }
        try:
            # Explicit transaction overrides autocommit for these two statements.
            with connection.transaction():
                connection.execute(self._insert_stream, params)
                connection.execute(self._insert_snapshot, params)
        except psycopg.Error:
            # PostgreSQL errors may include private IDs or entire failing rows.
            raise StorageError("Live poll storage failed; do not mark collection healthy.") from None


@contextmanager
def open_writer():
    """Use local WSL peer authentication, matching psql -d stream_pulse."""
    try:
        connection = psycopg.connect(
            dbname="stream_pulse", host="/var/run/postgresql", autocommit=True,
            connect_timeout=5,
            options="-c search_path=public -c statement_timeout=10000 -c lock_timeout=3000",
        )
    except psycopg.Error:
        raise StorageError("Could not connect to local PostgreSQL.") from None
    try:
        yield DatabaseWriter(connection)
    finally:
        connection.close()
