"""Persist EventSub events and own their per-source collection-health row.

Covers the sources whose readiness is the plain EventSub contract -- ``raids``
and ``follows`` -- where health combines the coordinator's transport/subscription
signal with this sink's own persistence outcomes. Chat is deliberately not here:
it adds observed-live eligibility, ``awaiting_stream_status``,
``paused``/``offline_observed``, and ``poll_failed``/``poll_stale``, so it needs
its own sink.

A coordinator that owns the EventSub socket feeds decoded notification frames to
``submit`` and reports the transport side through ``transport_ready`` /
``transport_error``. Health rows are written only when ``(status, reason_code)``
changes (the EventSub contract records health on change, not per message). The
sink never inspects sockets, tokens, or clocks, and does not decide stream
association. ``StorageError`` is not caught: a failed database cannot record its
own health, so it propagates and the caller stops the collector.
"""

from datetime import datetime

from scripts.eventsub_capture import (
    CaptureError, parse_follow_notification, parse_raid_notification,
)


# EventSub error reasons describing the transport/subscription side rather than a
# bad notification body. ``invalid_notification`` is handled separately.
TRANSPORT_ERROR_REASONS = frozenset({
    "network_error", "keepalive_timeout", "subscription_error",
    "subscription_revoked", "auth_error", "clock_uncertain",
})


class EventSinkError(Exception):
    """Internal misuse code only; never carries event contents or row values."""


class EventSink:
    """Shared health state machine for the plain-contract EventSub sources.

    Subclasses set ``SOURCE`` and implement ``_parse`` (raise ``CaptureError`` on
    a bad body) and ``_persist`` (return an inserted-row count; a redelivery
    skipped by its message key returns 0).
    """

    SOURCE = None

    def __init__(self, writer, run_id, *, emit):
        if type(self).SOURCE is None:
            raise EventSinkError("event_sink_source_undefined")
        self._writer = writer
        self._run_id = run_id
        self._emit = emit
        self._health = None            # last written (status, reason_code)
        self._transport_ready = False
        self._processing_error = False
        self._stopped = False

    def _parse(self, message, received_at):
        raise NotImplementedError

    def _persist(self, record):
        raise NotImplementedError

    def begin(self, observed_at):
        """Record the initial ``starting`` / ``initializing`` health row."""
        self._guard_time(observed_at)
        if self._stopped:
            raise EventSinkError("begin_after_stop")
        self._set_health("starting", "initializing", observed_at)

    def transport_ready(self, observed_at):
        """Coordinator asserts: subscription enabled, transport responsive, auth current.

        Combined with successful persistence and no unresolved notification error
        this reaches ``healthy`` / ``capture_ready``. A quiet channel stays ready.
        """
        self._guard_time(observed_at)
        if self._stopped:
            return
        self._transport_ready = True
        self._reconcile(observed_at)

    def transport_error(self, reason_code, observed_at):
        """Coordinator reports a transport/subscription failure for this source."""
        self._guard_time(observed_at)
        if self._stopped:
            return
        if reason_code not in TRANSPORT_ERROR_REASONS:
            reason_code = "subscription_error"
        self._transport_ready = False
        self._set_health("error", reason_code, observed_at)

    def submit(self, message, received_at, observed_at):
        """Validate and persist one decoded notification frame.

        ``received_at`` is the caller's receipt time (stamped before storage);
        ``observed_at`` times any resulting health row. Returns True if a new row
        was stored, False for a redelivery skipped by its message key or a
        rejected notification. ``StorageError`` propagates.
        """
        self._guard_time(observed_at)
        if self._stopped:
            raise EventSinkError("submit_after_stop")
        try:
            record = self._parse(message, received_at)
        except CaptureError as error:
            self._emit(f"{self.SOURCE}_notification_invalid_{error.reason_code}")
            self._processing_error = True
            self._set_health("error", "invalid_notification", observed_at)
            return False

        stored = self._persist(record)
        self._emit(f"{self.SOURCE}_event_stored" if stored
                   else f"{self.SOURCE}_event_duplicate_skipped")
        self._processing_error = False
        self._reconcile(observed_at)
        return bool(stored)

    def stop(self, observed_at):
        """Record deliberate source shutdown once; earlier error rows remain."""
        self._guard_time(observed_at)
        if self._stopped:
            return
        self._stopped = True
        self._set_health("stopped", "orderly_shutdown", observed_at)

    def _reconcile(self, observed_at):
        if not self._stopped and self._transport_ready and not self._processing_error:
            self._set_health("healthy", "capture_ready", observed_at)

    def _set_health(self, status, reason_code, observed_at):
        if self._health == (status, reason_code):
            return
        self._writer.record_collection_health(
            run_id=self._run_id, source=self.SOURCE, observed_at=observed_at,
            status=status, reason_code=reason_code,
        )
        self._health = (status, reason_code)
        self._emit(reason_code)

    @staticmethod
    def _guard_time(observed_at):
        if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
            raise EventSinkError("observed_at_invalid")


class FollowSink(EventSink):
    SOURCE = "follows"

    def _parse(self, message, received_at):
        return parse_follow_notification(message, received_at)

    def _persist(self, record):
        return self._writer.record_follow_event(
            eventsub_message_id=record.eventsub_message_id,
            user_id=record.user_id,
            followed_at=record.followed_at,
            notification_at=record.notification_at,
            received_at=record.received_at,
        )


class RaidSink(EventSink):
    SOURCE = "raids"

    def _parse(self, message, received_at):
        return parse_raid_notification(message, received_at)

    def _persist(self, record):
        return self._writer.record_raid_event(
            eventsub_message_id=record.eventsub_message_id,
            from_broadcaster_user_id=record.from_broadcaster_user_id,
            raid_viewer_count=record.raid_viewer_count,
            notification_at=record.notification_at,
            received_at=record.received_at,
        )
