"""Suspend-aware clock sampling with an independent UTC cross-check.

Extracted unchanged from the polling collector so the merged polling + EventSub
runtime can drive one clock authority. Linux ``CLOCK_BOOTTIME`` (or a monotonic
fallback) is the elapsed reference; it is cross-checked against aware UTC. A
small backward UTC step is waited out; anything worse is fatal.

``ClockGuard`` owns only clock state. A detected gap -- a >=90s advance on either
clock, a >=5s divergence between them, or a recoverable small UTC rollback -- is
reported through the injected ``on_gap(now)`` callback (also called again once
such a rollback finishes correcting). ``on_gap`` must not raise; it is where the
caller invalidates eligibility, forces revalidation, and schedules a fresh poll.
The caller maps :class:`ClockError` to its own fatal shutdown.
"""

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone


STALE_SECONDS = 90
TICK_SECONDS = 0.25
CLOCK_TOLERANCE_SECONDS = 5
UTC_RECOVERY_SECONDS = 5


@dataclass(frozen=True)
class ClockReading:
    utc: datetime
    tick: float


def read_clock():
    # Linux suspend-aware clock, with an independent UTC cross-check in sample().
    # Windows suspending the WSL VM still needs a real-machine rehearsal.
    tick = (time.clock_gettime(time.CLOCK_BOOTTIME)
            if hasattr(time, "CLOCK_BOOTTIME") else time.monotonic())
    return ClockReading(datetime.now(timezone.utc), tick)


class ClockError(Exception):
    """Fatal clock condition; internal constant code only, no private detail."""


class ClockGuard:
    """Sample the clock, reacting to gaps through ``on_gap``.

    ``on_gap(now)`` runs on a detected gap and again after a small UTC rollback
    corrects; it must not raise. ``emit`` takes a fixed diagnostic code.
    ``pause`` is the recovery-wait sleep and stays settable after construction
    (the polling tests swap it per case).
    """

    def __init__(self, *, on_gap, emit, clock=read_clock, pause=time.sleep):
        self._on_gap = on_gap
        self._emit = emit
        self._clock = clock
        self.pause = pause
        self._previous = None

    def sample(self):
        now = self._clock()
        previous = self._previous
        if previous is not None:
            elapsed = now.tick - previous.tick
            wall = (now.utc - previous.utc).total_seconds()
            if elapsed < 0:
                self._on_gap(now)
                raise ClockError("elapsed_clock_rollback_restart_required")
            if wall < 0:
                now = self._recover_utc(now, previous)
                elapsed = now.tick - previous.tick
                wall = (now.utc - previous.utc).total_seconds()
            if (elapsed >= STALE_SECONDS or wall >= STALE_SECONDS
                    or abs(wall - elapsed) >= CLOCK_TOLERANCE_SECONDS):
                self._on_gap(now)
                self._emit("clock_gap_fresh_poll_required")
        self._previous = now
        return now

    def _recover_utc(self, now, previous):
        """Wait briefly for real UTC to catch up; never clamp or rewrite a time.

        The caller does no database work or result acceptance during this wait.
        A fixed number of short waits bounds recovery even if a clock stalls.
        """
        self._on_gap(now)
        if (previous.utc - now.utc).total_seconds() >= CLOCK_TOLERANCE_SECONDS:
            raise ClockError("utc_clock_rollback_restart_required")
        self._emit("utc_clock_rollback_waiting")
        recovery_started = now.tick
        for _ in range(math.ceil(UTC_RECOVERY_SECONDS / TICK_SECONDS)):
            last_tick = now.tick
            self.pause(TICK_SECONDS)
            now = self._clock()
            if now.tick < last_tick:
                raise ClockError("elapsed_clock_rollback_restart_required")
            if now.tick - recovery_started > UTC_RECOVERY_SECONDS:
                raise ClockError("utc_clock_recovery_timeout_restart_required")
            if now.utc >= previous.utc:
                self._on_gap(now)
                self._emit("utc_clock_recovered_fresh_poll_required")
                return now
        raise ClockError("utc_clock_recovery_timeout_restart_required")
