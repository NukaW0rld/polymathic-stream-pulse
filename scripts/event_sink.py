"""Persist EventSub events and own their per-source collection-health row.

``EventSink`` covers the sources whose readiness is the plain EventSub contract
-- ``raids`` and ``follows`` -- where health combines the coordinator's
transport/subscription signal with this sink's own persistence outcomes.
``ChatSink`` extends it: chat also needs observed-live eligibility, so its
resting health is polling-driven (``awaiting_stream_status`` /
``paused``/``offline_observed`` / ``poll_failed`` / ``poll_stale``), and a chat
notification is stored only when eligibility resolves a stream.

A coordinator that owns the EventSub socket feeds decoded notification frames to
``submit`` and reports the transport side through ``transport_ready`` /
``transport_error``. Health rows are written only when ``(status, reason_code)``
changes (the EventSub contract records health on change, not per message). The
sink never inspects sockets, tokens, or clocks. ``EventSink`` does not decide
stream association; ``ChatSink`` reads it from an injected ``LiveStatus``.
``StorageError`` is not caught: a failed database cannot record its own health,
so it propagates and the caller stops the collector.
"""

from dataclasses import replace
from datetime import datetime

from scripts.eventsub_capture import (
    CaptureError, parse_chat_notification, parse_follow_notification,
    parse_raid_notification,
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

    def _prepare(self, record, received_at, observed_at):
        """Hook between a valid parse and persistence.

        Default: store the record as parsed. ``ChatSink`` overrides this to
        attach the eligible stream, or return ``None`` to drop a message that
        arrived outside observed-live eligibility (a policy discard, not an
        error).
        """
        return record

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

        record = self._prepare(record, received_at, observed_at)
        if record is None:
            return False  # Dropped before persistence; leaves health unchanged.

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


class ChatSink(EventSink):
    """Chat health is polling-driven; a message is stored only when live.

    Beyond the shared transport signal, this sink has a resting state set by the
    coordinator from its polling half -- ``polling_live`` / ``polling_offline`` /
    ``polling_failed`` / ``polling_stale`` -- starting at
    ``starting``/``awaiting_stream_status`` until the first accepted poll.
    Precedence: a deliberate stop, then an unresolved transport error or bad
    notification (``invalid_notification``), then the polling state. A transport
    gap therefore never hides behind an offline pause, and a bad event still
    holds until a later valid, persisted one.

    ``submit`` resolves each notification's stream through the injected
    ``LiveStatus``; a message outside observed-live eligibility is discarded, not
    persisted and not an error. The coordinator refreshes ``tick`` (its
    authoritative elapsed clock) before pumping the socket each loop.
    """

    SOURCE = "chat"

    _POLLING_HEALTH = {
        "awaiting": ("starting", "awaiting_stream_status"),
        "live": ("healthy", "capture_ready"),
        "offline": ("paused", "offline_observed"),
        "failed": ("error", "poll_failed"),
        "stale": ("error", "poll_stale"),
    }

    def __init__(self, writer, run_id, *, live_status, emit):
        super().__init__(writer, run_id, emit=emit)
        self._live_status = live_status
        self._polling = "awaiting"
        self.tick = 0.0

    def _parse(self, message, received_at):
        return parse_chat_notification(message, received_at)

    def _prepare(self, record, received_at, observed_at):
        stream_id = self._live_status.chat_stream_id(record.notification_at, received_at, self.tick)
        if stream_id is None:
            self._emit("chat_message_outside_eligibility_discarded")
            return None
        return replace(record, stream_id=stream_id)

    def _persist(self, record):
        return self._writer.record_chat_message(
            eventsub_message_id=record.eventsub_message_id,
            stream_id=record.stream_id,
            chatter_user_id=record.chatter_user_id,
            chat_message_id=record.chat_message_id,
            message_text=record.message_text,
            message_fragments=record.message_fragments,
            notification_at=record.notification_at,
            received_at=record.received_at,
            source_broadcaster_user_id=record.source_broadcaster_user_id,
        )

    def _reconcile(self, observed_at):
        # An unresolved transport failure (transport_ready is False) or a held
        # invalid_notification owns the row until its own recovery path. Only
        # when transport is up and processing is clean does the polling state
        # decide the resting health.
        if self._stopped or not self._transport_ready or self._processing_error:
            return
        status, reason_code = self._POLLING_HEALTH[self._polling]
        self._set_health(status, reason_code, observed_at)

    def polling_live(self, observed_at):
        self._set_polling("live", observed_at)

    def polling_offline(self, observed_at):
        self._set_polling("offline", observed_at)

    def polling_failed(self, observed_at):
        self._set_polling("failed", observed_at)

    def polling_stale(self, observed_at):
        self._set_polling("stale", observed_at)

    def _set_polling(self, state, observed_at):
        self._guard_time(observed_at)
        if self._stopped:
            return
        self._polling = state
        self._reconcile(observed_at)
