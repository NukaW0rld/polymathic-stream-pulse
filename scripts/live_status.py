"""Observed-live eligibility for chat; no API calls or database writes.

The collector must serialize updates and chat checks in its event loop (or lock).
Pass timezone-aware UTC datetimes and elapsed-clock seconds. The caller owns clocks.
"""

from datetime import datetime


class LiveStatus:
    def __init__(self):
        self._stream_id = None
        self._eligible_since = None
        self._last_poll_tick = None

    def _expire(self, tick):
        if self._last_poll_tick is not None:
            age = tick - self._last_poll_tick
            if age < 0 or age >= 90:
                self.poll_failed()

    def poll_live(self, stream_id: str, observed_at: datetime, tick: float):
        """Call after a successful live poll and its required database writes."""
        if not isinstance(stream_id, str) or not stream_id:
            raise ValueError("A stream ID is required.")
        self._check_time(observed_at)
        # Detect an elapsed gap even if no chat arrived while polling stalled.
        self._expire(tick)
        if self._stream_id != stream_id or self._eligible_since is None:
            self._eligible_since = observed_at
        self._stream_id = stream_id
        self._last_poll_tick = tick

    def poll_failed(self):
        """Invalidate chat eligibility; this does not establish broadcast end."""
        self._stream_id = None
        self._eligible_since = None
        self._last_poll_tick = None

    def poll_offline(self):
        """Stop chat eligibility; the collector separately records offline detection."""
        self.poll_failed()

    def chat_stream_id(self, notification_at: datetime, received_at: datetime, tick: float):
        """Return an eligible stream ID, or None to discard the message."""
        self._check_time(notification_at)
        self._check_time(received_at)
        self._expire(tick)
        if self._eligible_since is None:
            return None
        if self._eligible_since <= notification_at <= received_at:
            return self._stream_id
        return None

    @staticmethod
    def _check_time(value):
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("A timezone-aware datetime is required.")
