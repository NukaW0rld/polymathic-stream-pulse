"""Route validated EventSub deliveries to per-source persistence sinks.

This is the seam between the socket layer (``SessionReadiness`` and the capture
loop in ``scripts/eventsub_recovery.py``) and the sinks. It owns no socket,
clock, or token state: a caller that has just sampled its clock passes an
``EventDelivery`` from ``SessionReadiness.accept`` plus that ``ClockReading``,
and calls ``observe`` each tick so the sinks learn when their transport is ready.

With ``writer`` + ``run_id`` supplied, it also records reconnection gaps: an
unexpected transport loss (``transport_lost``) opens a ``reconnection_gaps`` row,
and the first ``observe`` where transport is live again resolves it. A gap left
open means capture did not observably recover before the run ended.

Chat is intentionally unrouted: it needs observed-live eligibility and its own
sink. A ``StorageError`` from any write is trapped and latched in
``storage_failed`` -- a failed database cannot record its own health, so the
capture loop checks this flag and stops instead of writing further.
"""

from scripts.database import StorageError


GAP_REASONS = frozenset({"network_error", "keepalive_timeout"})


class EventRouter:
    def __init__(self, sinks, *, emit, writer=None, run_id=None):
        # sinks: {"follows": FollowSink, "raids": RaidSink}; chat is not included.
        self._sinks = dict(sinks)
        self._emit = emit
        self._writer = writer
        self._run_id = run_id
        self._ready = {source: False for source in self._sinks}
        self._unrouted_reported = set()
        self._open_gap_id = None
        self._stopped = False
        self.storage_failed = False

    @property
    def source_names(self):
        """The routed sources, for the coordinator's multi-source run stop."""
        return tuple(self._sinks)

    def begin(self, now):
        for sink in self._sinks.values():
            self._guard(sink.begin, now.utc)

    def observe(self, session, now):
        """Reconcile each sink's transport signal from current SessionReadiness state.

        Only the ``not ready -> ready`` edge is driven here; losses come through
        ``transport_lost`` (which carries the reason) and revocations through
        ``dispatch``. An open reconnection gap is resolved once any subscription
        is live again. Idempotent: the sinks already collapse unchanged health.
        """
        if self._stopped or self.storage_failed:
            return
        for source, sink in self._sinks.items():
            if session.source_ready(source) and not self._ready[source]:
                self._ready[source] = True
                self._guard(sink.transport_ready, now.utc)
        if (self._open_gap_id is not None
                and any(session.source_ready(source) for source in self._sinks)):
            gap_id, self._open_gap_id = self._open_gap_id, None
            self._guard(self._writer.resolve_reconnection_gap,
                        gap_id=gap_id, recovered_at=now.utc)

    def transport_lost(self, reason_code, now):
        """The socket layer reports a transport gap affecting every live subscription."""
        if self._stopped or self.storage_failed:
            return
        for source, sink in self._sinks.items():
            if self._ready[source]:
                self._ready[source] = False
                self._guard(sink.transport_error, reason_code, now.utc)
        if self._writer is not None and self._open_gap_id is None:
            gap_reason = reason_code if reason_code in GAP_REASONS else "network_error"
            self._open_gap_id = self._guard(
                self._writer.record_reconnection_gap,
                run_id=self._run_id, detected_at=now.utc, reason_code=gap_reason,
            )

    def dispatch(self, delivery, now):
        """Act on one EventDelivery. Returns True when a notification stored a new row."""
        if self._stopped:
            self._emit(f"capture_frame_after_stop_discarded_{delivery.source}")
            return False
        if self.storage_failed:
            return False
        sink = self._sinks.get(delivery.source)
        if sink is None:
            if delivery.source not in self._unrouted_reported:
                self._unrouted_reported.add(delivery.source)
                self._emit(f"capture_unrouted_{delivery.source}")
            return False
        if delivery.kind == "revocation":
            self._ready[delivery.source] = False
            self._guard(sink.transport_error, "subscription_revoked", now.utc)
            return False
        if delivery.kind == "notification":
            return bool(self._guard(sink.submit, delivery.message, now.utc, now.utc))
        self._emit(f"capture_unknown_delivery_kind_{delivery.source}")
        return False

    def stop(self, now):
        if self._stopped or self.storage_failed:
            return
        self._stopped = True
        for sink in self._sinks.values():
            self._guard(sink.stop, now.utc)

    def mark_storage_failed(self):
        """Latch the no-more-writes state (also used for a failed heartbeat write)."""
        if not self.storage_failed:
            self.storage_failed = True
            self._emit("capture_storage_failure_stop_required")

    def _guard(self, fn, *args, **kwargs):
        """Run a write, latching a storage failure instead of propagating it."""
        try:
            return fn(*args, **kwargs)
        except StorageError:
            self.mark_storage_failed()
            return None
