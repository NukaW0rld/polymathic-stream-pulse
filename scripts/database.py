"""Persist stream observations and collector check-ins using project SQL files."""

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.pq import TransactionStatus


QUERY_DIR = Path(__file__).resolve().parents[1] / "sql" / "queries"

# These allowlists validate claims; the runtime must establish the evidence.
POLL_HEALTH_REASONS = {
    "starting": frozenset({"initializing"}),
    "healthy": frozenset({"live_poll_saved", "offline_poll_saved"}),
    "error": frozenset({"network_error", "auth_error", "api_error", "poll_stale"}),
    "stopped": frozenset({"orderly_shutdown"}),
}

EVENTSUB_HEALTH_REASONS = {
    "starting": frozenset({"initializing"}),
    "healthy": frozenset({"capture_ready"}),
    "error": frozenset({
        "network_error", "keepalive_timeout", "subscription_error",
        "subscription_revoked", "auth_error", "invalid_notification", "clock_uncertain",
    }),
    "stopped": frozenset({"orderly_shutdown"}),
}

CHAT_HEALTH_REASONS = {
    **EVENTSUB_HEALTH_REASONS,
    "starting": EVENTSUB_HEALTH_REASONS["starting"] | {"awaiting_stream_status"},
    "paused": frozenset({"offline_observed"}),
    "error": EVENTSUB_HEALTH_REASONS["error"] | {"poll_failed", "poll_stale"},
}

HEALTH_REASONS = {
    "stream_poll": POLL_HEALTH_REASONS,
    "chat": CHAT_HEALTH_REASONS,
    "raids": EVENTSUB_HEALTH_REASONS,
    "follows": EVENTSUB_HEALTH_REASONS,
}


class StorageError(Exception):
    """Safe storage diagnostic; never include database error details or row values."""


class DatabaseWriter:
    """Use one dedicated autocommit connection, serialized by the caller."""

    def __init__(self, connection):
        self._connection = connection
        try:
            self._insert_stream = (QUERY_DIR / "insert_stream.sql").read_text()
            self._insert_snapshot = (QUERY_DIR / "insert_viewer_snapshot.sql").read_text()
            self._mark_offline = (QUERY_DIR / "mark_stream_offline.sql").read_text()
            self._start_run = (QUERY_DIR / "start_collector_run.sql").read_text()
            self._heartbeat = (QUERY_DIR / "update_collector_heartbeat.sql").read_text()
            self._stop_run = (QUERY_DIR / "stop_collector_run.sql").read_text()
            self._insert_health = (QUERY_DIR / "insert_collection_health.sql").read_text()
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

        connection = self._idle_connection()

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

    def mark_stream_offline(self, *, stream_id, offline_observed_at):
        """Record a successful offline detection for a previously tracked stream.

        Return the number of updated rows. Zero means already marked OR absent;
        it does not establish that the requested stream exists. The caller must
        never invoke this for failed polls, outgoing raids, or collector shutdown.
        """
        if not isinstance(stream_id, str) or not stream_id:
            raise StorageError("Offline detection requires a nonempty stream ID.")
        if not isinstance(offline_observed_at, datetime) or offline_observed_at.utcoffset() is None:
            raise StorageError("Offline detection requires a timezone-aware datetime.")
        connection = self._idle_connection()
        try:
            with connection.transaction():
                cursor = connection.execute(self._mark_offline, {
                    "stream_id": stream_id,
                    "offline_observed_at": offline_observed_at,
                })
                updated = cursor.rowcount
            return updated
        except psycopg.Error:
            raise StorageError("Offline detection storage failed; do not mark collection healthy.") from None

    def start_collector_run(self, *, started_at):
        """Commit a new execution and return its generated ID; never auto-retry.

        A lost commit acknowledgement can leave an unknown committed run. Retrying
        this INSERT would create another run, not recover the original ID.
        """
        if not isinstance(started_at, datetime) or started_at.utcoffset() is None:
            raise StorageError("Run start requires a timezone-aware datetime.")
        connection = self._idle_connection()
        try:
            with connection.transaction():
                row = connection.execute(self._start_run, {"started_at": started_at}).fetchone()
                if row is None or type(row[0]) is not int:
                    raise StorageError("Run start did not return a generated ID.")
                run_id = row[0]
            return run_id
        except psycopg.Error:
            raise StorageError("Run start storage failed; commit outcome may be uncertain. Stop startup.") from None

    def update_collector_heartbeat(self, *, run_id, last_heartbeat_at):
        """Commit a check-in or raise if no eligible run was updated.

        Missing, stopped, and newer-heartbeat cases all reject the check-in.
        Successful persistence is process evidence, not source readiness.
        """
        if type(run_id) is not int or run_id <= 0:
            raise StorageError("Heartbeat requires a positive integer run ID.")
        if not isinstance(last_heartbeat_at, datetime) or last_heartbeat_at.utcoffset() is None:
            raise StorageError("Heartbeat requires a timezone-aware datetime.")
        connection = self._idle_connection()
        try:
            with connection.transaction():
                cursor = connection.execute(self._heartbeat, {
                    "run_id": run_id, "last_heartbeat_at": last_heartbeat_at,
                })
                if cursor.rowcount != 1:
                    raise StorageError(
                        "Heartbeat rejected: run missing, stopped, or timestamp older than last check-in."
                    )
        except psycopg.Error:
            raise StorageError("Heartbeat storage failed; check-in not confirmed.") from None

    def record_collection_health(self, *, run_id, source, observed_at, status, reason_code):
        """Append a validated health claim; the caller must establish its truth.

        EventSub capture_ready requires working event persistence, not just a
        subscription. This method neither probes Twitch nor verifies readiness,
        transition precedence, or run lifecycle/time boundaries. Inserts
        have no retry key: do not auto-retry an uncertain commit.
        """
        self._validate_run_time(run_id, observed_at)
        if (not all(isinstance(value, str) for value in (source, status, reason_code))
                or reason_code not in HEALTH_REASONS.get(source, {}).get(status, ())):
            raise StorageError("Unsupported health source, status, or reason combination.")
        connection = self._idle_connection()
        try:
            with connection.transaction():
                connection.execute(self._insert_health, {
                    "run_id": run_id, "source": source, "observed_at": observed_at,
                    "status": status, "reason_code": reason_code,
                })
        except psycopg.Error:
            raise StorageError("Health storage failed; coverage evidence not confirmed.") from None

    def stop_collector_run(self, *, run_id, stopped_at):
        """Commit run shutdown and stream_poll stopped evidence atomically.

        For the initial polling-only runtime. Stop polling before calling; do not
        use after crashes or to infer broadcast end. Other sources are not closed
        by this method and must be integrated before an EventSub runtime uses it.
        """
        self._validate_run_time(run_id, stopped_at)
        connection = self._idle_connection()
        try:
            with connection.transaction():
                cursor = connection.execute(self._stop_run, {
                    "run_id": run_id, "stopped_at": stopped_at,
                })
                if cursor.rowcount != 1:
                    raise StorageError(
                        "Shutdown rejected: run missing, already stopped, or timestamp before last heartbeat."
                    )
                connection.execute(self._insert_health, {
                    "run_id": run_id, "source": "stream_poll", "observed_at": stopped_at,
                    "status": "stopped", "reason_code": "orderly_shutdown",
                })
        except psycopg.Error:
            raise StorageError("Shutdown storage failed; orderly shutdown not confirmed.") from None

    @staticmethod
    def _validate_run_time(run_id, observed_at):
        if type(run_id) is not int or run_id <= 0:
            raise StorageError("Run operation requires a positive integer run ID.")
        if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
            raise StorageError("Run operation requires a timezone-aware datetime.")

    def _idle_connection(self):
        connection = self._connection
        if connection.closed:
            raise StorageError("Database connection is closed.")
        if not connection.autocommit or connection.info.transaction_status != TransactionStatus.IDLE:
            raise StorageError("Database writer requires an idle autocommit connection.")
        return connection


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
