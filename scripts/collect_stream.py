"""Polling-only collector. Run from the repository root with -m scripts.collect_stream.

The coordinator owns the database and LiveStatus. One worker at a time owns all
Twitch/token calls. No EventSub subscriptions, production reads, or retry queue.
"""

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import signal
import sys
import threading
import time

from scripts.database import StorageError, open_writer
from scripts.live_status import LiveStatus
from scripts.twitch_auth import TokenManager, TwitchError


POLL_SECONDS = 60
STALE_SECONDS = 90
HEARTBEAT_SECONDS = 30
TICK_SECONDS = 0.25
CLOCK_TOLERANCE_SECONDS = 5


@dataclass(frozen=True)
class ClockReading:
    utc: datetime
    tick: float


def read_clock():
    # Linux suspend-aware clock, with an independent UTC cross-check in _sample.
    # Windows suspending the WSL VM still needs a real-machine rehearsal.
    tick = (time.clock_gettime(time.CLOCK_BOOTTIME)
            if hasattr(time, "CLOCK_BOOTTIME") else time.monotonic())
    return ClockReading(datetime.now(timezone.utc), tick)


def age(now, earlier):
    return max(now.tick - earlier.tick, (now.utc - earlier.utc).total_seconds())


def diagnostic(code):
    # Only internal constant codes are passed here. Never stringify exceptions,
    # responses, IDs, headers, SQL parameters, or config.
    print(f"{datetime.now(timezone.utc).isoformat()} {code}", flush=True)


class CollectorError(Exception):
    """Internal failure code only; no private exception details."""


@dataclass(frozen=True)
class StreamObservation:
    stream_id: str = field(repr=False)
    started_at: datetime
    viewer_count: int


class TwitchPoller:
    """Resolve the target once, then validate and poll through one token manager."""

    def __init__(self, auth):
        self.auth = auth
        self._broadcaster_id = None

    def poll(self, *, force_validation=False):
        self.auth.validate_if_due(force=force_validation)
        if self._broadcaster_id is None:
            response = self.auth.helix_get("users", {"login": "polymathic"})
            users = response.get("data") if isinstance(response, dict) else None
            if (not isinstance(users, list) or len(users) != 1
                    or not isinstance(users[0], dict)
                    or not isinstance(users[0].get("id"), str) or not users[0]["id"]
                    or users[0].get("login") != "polymathic"):
                raise TwitchError("Invalid target lookup response.")
            self._broadcaster_id = users[0]["id"]

        response = self.auth.helix_get("streams", {"user_id": self._broadcaster_id})
        streams = response.get("data") if isinstance(response, dict) else None
        if not isinstance(streams, list) or len(streams) > 1:
            raise TwitchError("Invalid stream response.")
        if not streams:
            return None  # Only a valid empty data list establishes offline.
        stream = streams[0]
        if (not isinstance(stream, dict)
                or stream.get("user_id") != self._broadcaster_id
                or stream.get("type") != "live"
                or not isinstance(stream.get("id"), str) or not stream["id"]
                or type(stream.get("viewer_count")) is not int
                or not 0 <= stream["viewer_count"] <= 2147483647):
            raise TwitchError("Invalid live observation.")
        try:
            started = datetime.fromisoformat(stream["started_at"].replace("Z", "+00:00"))
            if started.utcoffset() is None:
                raise ValueError
        except (KeyError, TypeError, AttributeError, ValueError):
            raise TwitchError("Invalid stream start timestamp.") from None
        return StreamObservation(stream["id"], started.astimezone(timezone.utc), stream["viewer_count"])


@dataclass(frozen=True)
class PollResult:
    observed: ClockReading | None = None
    stream: StreamObservation | None = field(default=None, repr=False)
    error: str | None = None
    fatal: bool = False


class PollWorker:
    """At most one in-flight operation, including refresh and token persistence.

    Threads are not forcibly cancelled: shutdown waits for the operation to end.
    Results are consumed only after the thread exits. No database access here.
    """

    def __init__(self, poller, clock=read_clock):
        self.poller = poller
        self.clock = clock
        self._thread = None
        self._result = None

    @property
    def busy(self):
        return self._thread is not None

    def start(self, *, force_validation=False):
        if self.busy:
            raise CollectorError("overlapping_poll_rejected")
        self._result = None
        self._thread = threading.Thread(
            target=self._poll, args=(force_validation,), name="twitch-poll", daemon=False,
        )
        self._thread.start()

    def _poll(self, force_validation):
        try:
            stream = self.poller.poll(force_validation=force_validation)
            self._result = PollResult(observed=self.clock(), stream=stream)
        except TwitchError as error:
            reason = error.reason_code
            if reason not in {"network_error", "auth_error", "api_error"}:
                reason = "api_error"
            self._result = PollResult(error=reason, fatal=error.fatal)
        except Exception:
            # No thread traceback: unexpected exceptions can contain private data.
            self._result = PollResult(error="collector_error", fatal=True)

    def take(self):
        if not self.busy or self._thread.is_alive():
            return None
        self._thread.join()
        self._thread = None
        if self._result is None:
            raise CollectorError("worker_result_missing")
        result, self._result = self._result, None
        return result

    def finish(self):
        if self.busy:
            self._thread.join()
            self._thread = None
            self._result = None  # Shutdown never accepts a late observation.


class PollingCollector:
    """Serialized state transitions; injected clock/worker support synthetic tests."""

    def __init__(self, writer, worker, *, clock=read_clock, emit=diagnostic):
        self.writer = writer
        self.worker = worker
        self.clock = clock
        self.emit = emit
        self.live_status = LiveStatus()
        self.run_id = None
        # Deliberately independent of chat eligibility; no previous-run DB reads.
        self.tracked_stream = None
        self._previous_clock = None
        self._last_success = None
        self._stale_reported = False
        self._discard_job = False
        self._force_validation = False
        self._generation = 0

    def _sample(self):
        now = self.clock()
        previous = self._previous_clock
        if previous is not None:
            elapsed = now.tick - previous.tick
            wall = (now.utc - previous.utc).total_seconds()
            if elapsed < 0 or wall < 0:
                # Do not invent increasing timestamps or backdate health evidence.
                self.live_status.poll_failed()
                raise CollectorError("clock_rollback_restart_required")
            if (elapsed >= STALE_SECONDS or wall >= STALE_SECONDS
                    or abs(wall - elapsed) >= CLOCK_TOLERANCE_SECONDS):
                self.live_status.poll_failed()
                self._generation += 1
                self._discard_job = True
                self._force_validation = True
                self._next_poll = now.tick
                self.emit("clock_gap_fresh_poll_required")
        self._previous_clock = now
        return now

    def start(self):
        self.started = self._sample()
        self._next_poll = self.started.tick
        self._next_heartbeat = self.started.tick + HEARTBEAT_SECONDS
        self.run_id = self.writer.start_collector_run(started_at=self.started.utc)
        self._health("starting", "initializing", self._sample())

    def _health(self, status, reason, now):
        self.writer.record_collection_health(
            run_id=self.run_id, source="stream_poll", observed_at=now.utc,
            status=status, reason_code=reason,
        )
        self.emit(reason)

    def _check_stale(self, now):
        baseline = self._last_success or self.started
        if age(now, baseline) >= STALE_SECONDS:
            self.live_status.poll_failed()
            if not self._stale_reported:
                self._health("error", "poll_stale", now)
                self._stale_reported = True

    def step(self, stop=None):
        now = self._sample()
        self._check_stale(now)
        now = self._sample()
        if now.tick >= self._next_heartbeat:
            self.writer.update_collector_heartbeat(run_id=self.run_id, last_heartbeat_at=now.utc)
            self._next_heartbeat = now.tick + HEARTBEAT_SECONDS
            now = self._sample()
            self._check_stale(now)

        if stop is not None and stop.is_set():
            return
        result = self.worker.take()
        if result is not None:
            now = self._sample()
            if result.error == "collector_error":
                raise CollectorError("worker_failed_restart_required")
            if result.fatal:
                self.live_status.poll_failed()
                self._health("error", result.error, now)
                raise CollectorError("authorization_blocked_restart_required")
            if self._discard_job or age(now, self._job_started) >= STALE_SECONDS:
                self.live_status.poll_failed()
                self.emit("late_poll_discarded")
            elif result.error:
                self.live_status.poll_failed()
                self._health("error", result.error, now)
            else:
                self._save(result, now)

        now = self._sample()
        self._check_stale(now)
        if (not self.worker.busy and now.tick >= self._next_poll
                and (stop is None or not stop.is_set())):
            self._job_started = now
            self._discard_job = False
            self.worker.start(force_validation=self._force_validation)
            self._force_validation = False
            # Advance past missed scheduled slots; never queue catch-up polls.
            missed = math.floor((now.tick - self._next_poll) / POLL_SECONDS)
            self._next_poll += (missed + 1) * POLL_SECONDS

    def _save(self, result, now):
        observed = result.observed
        if (observed is None or observed.utc < self._job_started.utc
                or observed.tick < self._job_started.tick or observed.utc > now.utc
                or observed.tick > now.tick):
            self.live_status.poll_failed()
            raise CollectorError("observation_clock_invalid")
        if age(now, observed) >= STALE_SECONDS:
            self.live_status.poll_failed()
            self.emit("late_poll_discarded")
            return

        generation = self._generation
        stream = result.stream
        if stream is None:
            self.live_status.poll_offline()
            if self.tracked_stream is not None:
                self.writer.mark_stream_offline(
                    stream_id=self.tracked_stream, offline_observed_at=observed.utc,
                )
                self.tracked_stream = None
            reason = "offline_poll_saved"
        else:
            if stream.started_at > observed.utc:
                self.live_status.poll_failed()
                self._health("error", "api_error", now)
                return
            self.writer.record_live_poll(
                stream_id=stream.stream_id, started_at=stream.started_at,
                observed_at=observed.utc, viewer_count=stream.viewer_count,
            )
            self.tracked_stream = stream.stream_id
            reason = "live_poll_saved"

        # Database work can also block. Recheck after data and health commits.
        now = self._sample()
        self._check_stale(now)
        if generation != self._generation or age(now, observed) >= STALE_SECONDS:
            self.live_status.poll_failed()
            self.emit("saved_poll_no_longer_fresh")
            self._check_stale(now)
            return
        self._health("healthy", reason, now)
        self._last_success = observed
        self._stale_reported = False
        now = self._sample()
        if generation != self._generation or age(now, observed) >= STALE_SECONDS:
            self.live_status.poll_failed()
            self._check_stale(now)
            return
        if stream is not None:
            self.live_status.poll_live(stream.stream_id, observed.utc, observed.tick)

    def run(self, stop, *, duration=None):
        try:
            self.start()
            while not stop.is_set():
                if duration is not None and age(self._sample(), self.started) >= duration:
                    break
                self.step(stop)
                stop.wait(TICK_SECONDS)
            self.live_status.poll_failed()
            if self.worker.busy:
                self.emit("shutdown_waiting_for_twitch")
            self.worker.finish()
            self.writer.stop_collector_run(run_id=self.run_id, stopped_at=self._sample().utc)
            self.emit("orderly_shutdown")
            return 0
        except StorageError:
            self.emit("storage_failure_restart_required_commit_may_be_uncertain")
            return 1
        except CollectorError as error:
            self.emit(str(error))  # Only this module constructs CollectorError.
            return 1
        except Exception:
            self.emit("collector_failure_restart_required")
            return 1
        finally:
            self.live_status.poll_failed()
            if self.worker.busy:
                self.emit("exit_waiting_for_twitch")
            self.worker.finish()


def positive_seconds(value):
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Use a positive duration in seconds.") from None
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("Use a positive duration in seconds.")
    return seconds


def main(argv=None):
    parser = argparse.ArgumentParser(description="Collect stream status, viewer snapshots, and polling health.")
    parser.add_argument("--duration", type=positive_seconds,
                        help="Stop after this many seconds; omitted means run until Ctrl+C/SIGTERM.")
    args = parser.parse_args(argv)
    stop = threading.Event()
    previous_handlers = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, lambda *_: stop.set())
        auth = TokenManager.load()
        with open_writer() as writer:
            return PollingCollector(writer, PollWorker(TwitchPoller(auth))).run(stop, duration=args.duration)
    except TwitchError:
        diagnostic("authorization_load_failed_restart_required")
        return 1
    except StorageError:
        diagnostic("database_startup_failed_restart_required")
        return 1
    except Exception:
        diagnostic("collector_startup_failed_restart_required")
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
