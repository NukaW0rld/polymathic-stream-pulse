"""Merged collector. Run from the repository root with -m scripts.collect_stream.

One cooperative loop: the coordinator owns the database, LiveStatus, and the
clock, running viewer polling and -- unless ``--no-eventsub`` -- EventSub chat,
raid, and follow capture under one collector run. ``--no-chat`` keeps EventSub at
raids + follows. Poll, setup, and enhanced Helix work share the token manager's
request lock; enhanced work is page-bounded and queued so it cannot create an
unbounded backlog. No worker writes to PostgreSQL.
"""

import argparse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import signal
import sys
import threading
import time

from scripts.clock_guard import (
    CLOCK_TOLERANCE_SECONDS, STALE_SECONDS, TICK_SECONDS, UTC_RECOVERY_SECONDS,
    ClockError, ClockGuard, ClockReading, read_clock,
)
from scripts.database import StorageError, open_writer
from scripts.eventsub import ProbeError, prepare_subscriptions
from scripts.enhanced_collection import (
    PRESENCE_MAX_ATTEMPT_SECONDS, PRESENCE_MAX_PAGES, PRESENCE_SECONDS,
    RAID_CONTEXT_QUEUE_LIMIT, EnhancedResult, EnhancedWorker, PresencePageJob, RaidContextJob,
)
from scripts.live_status import LiveStatus
from scripts.twitch_auth import TokenManager, TwitchError

# scripts.check_eventsub and scripts.eventsub_recovery import from this module, so
# their symbols are imported lazily inside _start_eventsub()/main() to avoid a cycle.


POLL_SECONDS = 60
HEARTBEAT_SECONDS = 30
# Order matches prepare_subscriptions(); chat is dropped by --no-chat.
CAPTURED_EVENTSUB_SOURCES = ("chat", "raids", "follows")


@dataclass(frozen=True)
class EventSubConfig:
    """Everything the coordinator needs to build the EventSub half in start().

    Assembled by main() before the run opens: the socket is opened and the
    subscription specs resolved (a Helix call) up front, like the standalone
    capture collector, so they do not consume Twitch's post-welcome window.
    ``sources`` names which per-source sinks the coordinator builds; it defaults
    to the raid + follow pair so tests and the isolated path stay unchanged.
    """

    auth: object = field(repr=False)
    specs: tuple = field(repr=False)
    socket: object = field(repr=False)
    connector: object = field(repr=False)
    worker_factory: object = field(repr=False)
    url: str = field(repr=False)
    sources: tuple = ("raids", "follows")


@dataclass(frozen=True)
class EnhancedConfig:
    """Optional milestone-1 Helix collection wired by the production CLI."""

    auth: object = field(repr=False)
    presence: bool = True
    raid_context: bool = True
    worker_factory: object = field(default=EnhancedWorker, repr=False)


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
    metadata: dict | None = field(default=None, repr=False)


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
        metadata = None
        if (all(isinstance(stream.get(key), str)
                for key in ("title", "game_id", "game_name", "language"))
                and isinstance(stream.get("tags"), list)
                and all(isinstance(tag, str) and tag for tag in stream["tags"])):
            metadata = {
                "title": stream["title"], "category_id": stream["game_id"],
                "category_name": stream["game_name"], "language": stream["language"],
                "tags": sorted(set(stream["tags"])),
            }
        return StreamObservation(
            stream["id"], started.astimezone(timezone.utc), stream["viewer_count"], metadata,
        )

    @property
    def broadcaster_id(self):
        """Resolved target ID; private and only available after a successful poll."""
        return self._broadcaster_id


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

    def __init__(self, writer, worker, *, clock=read_clock, emit=diagnostic, pause=time.sleep,
                 eventsub=None, enhanced=None):
        self.writer = writer
        self.worker = worker
        self.clock = clock
        self.emit = emit
        self.live_status = LiveStatus()
        self.run_id = None
        # Deliberately independent of chat eligibility; no previous-run DB reads.
        self.tracked_stream = None
        self._last_success = None
        self._stale_reported = False
        self._discard_job = False
        self._force_validation = False
        self._generation = 0
        self._clock_guard = ClockGuard(
            on_gap=self._invalidate_clock, emit=emit, clock=clock, pause=pause,
        )
        # EventSub half. None -> pure polling, behaviour unchanged. Otherwise an
        # EventSubConfig; the RecoveringProbe is built in _start_eventsub() once
        # start_collector_run() has produced the run_id the sinks need.
        self._eventsub = eventsub
        self._probe = None
        self._probe_fatal = None
        # Set in _start_eventsub() when chat is a configured source. The coordinator
        # owns its polling-derived health and refreshes its clock tick each loop.
        self._chat_sink = None
        self._enhanced = enhanced
        self._enhanced_worker = None
        self._presence_available = False
        self._presence_live = False
        self._presence_health_state = None
        self._presence_active = None
        self._next_presence = None
        self._raid_context_queue = deque()
        self._aux_last = None

    @property
    def pause(self):
        # The clock-recovery wait; polling tests swap it after construction.
        return self._clock_guard.pause

    @pause.setter
    def pause(self, value):
        self._clock_guard.pause = value

    def _invalidate_clock(self, now):
        """Clock-gap reaction: drop eligibility, force revalidation, poll now.

        Passed to ``ClockGuard`` as its ``on_gap``, so it also drives the
        EventSub half: a coordinator clock gap tears down the session the same
        way an unexpected transport loss does. Also runs on the fatal
        ``elapsed``-rollback path (where only ``poll_failed`` mattered before);
        the extra resets are harmless because the process then exits.
        """
        self.live_status.poll_failed()
        self._chat_polling("failed", now)
        self._generation += 1
        self._discard_job = True
        self._force_validation = True
        self._next_poll = now.tick
        if self._probe is not None:
            # force_gap -> lose() can re-raise a ProbeError (e.g. a pending
            # reconnect job that failed authorization). Latch it; step()/run()
            # turn it into a clean coordinator stop rather than letting it
            # surface from inside ClockGuard.sample().
            try:
                self._probe.force_gap(now)
            except ProbeError as error:
                self._probe_fatal = str(error)

    def _sample(self):
        # Maps the guard's fatal ClockError back to this module's CollectorError
        # so existing callers and tests see one failure type.
        try:
            return self._clock_guard.sample()
        except ClockError as error:
            raise CollectorError(str(error)) from None

    def start(self):
        self.started = self._sample()
        self._next_poll = self.started.tick
        self._next_heartbeat = self.started.tick + HEARTBEAT_SECONDS
        if self._enhanced is not None:
            self.run_id = self.writer.start_collector_run_with_capabilities(
                started_at=self.started.utc, capabilities=self._initial_capabilities(),
            )
            self._start_enhanced(self.started)
        else:
            self.run_id = self.writer.start_collector_run(started_at=self.started.utc)
        self._health("starting", "initializing", self._sample())

    def _initial_capabilities(self):
        event_sources = set(self._eventsub.sources) if self._eventsub is not None else set()
        return {
            "stream_poll": True,
            "chat": "chat" in event_sources,
            "raids": "raids" in event_sources,
            "follows": "follows" in event_sources,
            "chatter_presence": self._enhanced.presence,
            "chat_context": "chat" in event_sources,
            "stream_metadata_history": True,
            "raid_source_context": self._enhanced.raid_context and "raids" in event_sources,
        }

    def _start_enhanced(self, now):
        self._enhanced_worker = self._enhanced.worker_factory(
            self._enhanced.auth, clock=self.clock,
        )
        if not self._enhanced.presence:
            return
        self._presence_health("starting", "initializing", now)
        failure_reason = "missing_scope"
        try:
            self._presence_available = self._enhanced.auth.has_scope(
                "moderator:read:chatters"
            )
        except TwitchError as error:
            if error.fatal:
                raise CollectorError("authorization_blocked_restart_required") from None
            self._presence_available = False
            failure_reason = "authorization_check_failed"
        if not self._presence_available:
            self.writer.record_run_capability(
                run_id=self.run_id, capability="chatter_presence",
                status="failed_to_initialize", observed_at=now.utc,
                reason_code=failure_reason,
            )
            self._presence_health(
                "error", "missing_scope" if failure_reason == "missing_scope"
                else "request_failed", now,
            )
            self.emit(f"chatter_presence_{failure_reason}_fallback_active")

    def _presence_health(self, status, reason, now):
        state = (status, reason)
        if self._presence_health_state == state:
            return
        self.writer.record_collection_health(
            run_id=self.run_id, source="chatter_presence", observed_at=now.utc,
            status=status, reason_code=reason,
        )
        self._presence_health_state = state
        self.emit(reason)

    def _start_eventsub(self, stop):
        """Build the EventSub half against the now-known run_id and open its router.

        No-op for pure polling. The RecoveringProbe reuses this collector's clock
        and shares ``stop``; ``external_clock`` hands gap detection to ClockGuard.
        """
        if self._eventsub is None:
            return
        from scripts.event_sink import ChatSink, FollowSink, RaidSink
        from scripts.eventsub_router import EventRouter
        from scripts.eventsub_recovery import RecoveringProbe

        cfg = self._eventsub
        sinks = {}
        if "raids" in cfg.sources:
            callback = (self._raid_stored if self._enhanced is not None
                        and self._enhanced.raid_context else None)
            sinks["raids"] = RaidSink(
                self.writer, self.run_id, emit=self.emit, on_stored=callback,
            )
        if "follows" in cfg.sources:
            sinks["follows"] = FollowSink(self.writer, self.run_id, emit=self.emit)
        if "chat" in cfg.sources:
            self._chat_sink = ChatSink(
                self.writer, self.run_id, live_status=self.live_status, emit=self.emit,
            )
            sinks["chat"] = self._chat_sink
        router = EventRouter(
            sinks, emit=self.emit, writer=self.writer, run_id=self.run_id,
        )
        self._probe = RecoveringProbe(
            cfg.socket, cfg.auth, cfg.specs, stop,
            duration=math.inf, emit=self.emit, clock=self.clock,
            worker_factory=cfg.worker_factory, connector=cfg.connector, url=cfg.url,
            router=router, external_clock=True,
        )
        self._probe.begin(self._sample())

    def _pump_eventsub(self):
        """Advance the EventSub half one tick; no-op for pure polling."""
        if self._probe is None:
            return
        now = self._sample()
        if self._chat_sink is not None:
            # The chat sink's eligibility check needs the coordinator's
            # authoritative elapsed clock; refresh it before the socket pump.
            self._chat_sink.tick = now.tick
        if self._probe_fatal is not None:
            raise CollectorError(self._probe_fatal)
        if self._probe.router.storage_failed:
            raise CollectorError("capture_storage_failure_stop_required")
        try:
            self._probe.step(now)
        except ProbeError as error:
            raise CollectorError(str(error)) from None

    def _chat_polling(self, state, now):
        """Mirror a polling-half eligibility transition onto the chat sink.

        No-op unless chat is a configured source. ``state`` is one of
        ``live`` / ``offline`` / ``failed`` / ``stale``; the sink collapses
        unchanged health, so calling this alongside every ``LiveStatus``
        transition is safe.
        """
        if self._chat_sink is not None:
            getattr(self._chat_sink, f"polling_{state}")(now.utc)
        if self._enhanced is not None:
            self._presence_live = state == "live"
            if self._presence_available and state == "offline":
                self._presence_health("paused", "offline_observed", now)
            elif self._presence_available and state in {"failed", "stale"}:
                self._presence_health("error", f"poll_{state}", now)

    def _raid_stored(self, record, observed_at):
        """Persist enrichment intent only after the raw raid is durable."""
        inserted = self.writer.start_raid_source_context(
            eventsub_message_id=record.eventsub_message_id, run_id=self.run_id,
            requested_at=observed_at,
        )
        if not inserted:
            return
        job = RaidContextJob(
            eventsub_message_id=record.eventsub_message_id,
            broadcaster_id=record.from_broadcaster_user_id,
            requested_at=observed_at,
        )
        if len(self._raid_context_queue) >= RAID_CONTEXT_QUEUE_LIMIT:
            self.writer.finalize_raid_source_context(
                eventsub_message_id=record.eventsub_message_id,
                observed_at=observed_at, status="failed", reason_code="queue_full",
                attempt_count=0,
            )
            self.emit("raid_source_context_queue_full")
            return
        self._raid_context_queue.append((job, self.clock().tick))

    def _poll_target_id(self):
        poller = getattr(self.worker, "poller", None)
        return getattr(poller, "broadcaster_id", None)

    def _begin_presence_if_due(self, now):
        if (not self._presence_available or not self._presence_live
                or self._presence_active is not None
                or self.tracked_stream is None or self._next_presence is None
                or now.tick < self._next_presence):
            return
        broadcaster_id = self._poll_target_id()
        moderator_id = getattr(self._enhanced.auth, "user_id", None)
        if not isinstance(broadcaster_id, str) or not broadcaster_id:
            return
        snapshot_id = self.writer.start_chatter_presence_snapshot(
            run_id=self.run_id, stream_id=self.tracked_stream, requested_at=now.utc,
        )
        job = PresencePageJob(
            snapshot_id=snapshot_id, run_id=self.run_id,
            stream_id=self.tracked_stream, broadcaster_id=broadcaster_id,
            moderator_id=moderator_id, requested_at=now.utc,
            generation=self._generation,
        )
        self._presence_active = {
            "snapshot_id": snapshot_id, "stream_id": self.tracked_stream,
            "generation": self._generation, "requested_at": now.utc,
            "started_tick": now.tick, "members": set(), "first_total": None,
            "total_changed": False, "next_job": job,
        }
        # Cadence follows attempt starts; missed slots are skipped, never caught up.
        self._next_presence = now.tick + PRESENCE_SECONDS
        self._presence_health("starting", "snapshot_requested", now)

    def _finalize_presence(self, now, *, status, reason, health_reason=None):
        active = self._presence_active
        if active is None:
            return
        self.writer.finalize_chatter_presence_snapshot(
            snapshot_id=active["snapshot_id"], completed_at=now.utc,
            status=status, reason_code=reason, members=active["members"],
            reported_total=active["first_total"],
            reported_total_changed=(active["total_changed"]
                                    if active["first_total"] is not None else None),
        )
        if status == "complete":
            self._presence_health(
                "healthy", "empty_snapshot_complete" if not active["members"]
                else "snapshot_complete", now,
            )
        else:
            self._presence_health("error", health_reason or "partial_result", now)
        self._presence_active = None

    def _handle_presence_result(self, result, now):
        active = self._presence_active
        job = result.job
        if active is None or job.snapshot_id != active["snapshot_id"]:
            return
        if job.generation != self._generation:
            self._finalize_presence(now, status="rejected", reason="clock_gap",
                                    health_reason="clock_gap")
            return
        if not self._presence_live:
            self._finalize_presence(now, status="rejected", reason="poll_failed",
                                    health_reason="poll_failed")
            return
        if job.stream_id != self.tracked_stream:
            self._finalize_presence(now, status="rejected", reason="stream_changed",
                                    health_reason="stream_changed")
            return
        if result.fatal:
            raise CollectorError("authorization_blocked_restart_required")
        if result.error:
            self._finalize_presence(
                now, status="partial", reason=result.error,
                health_reason=result.error if result.error in {
                    "access_denied", "rate_limited", "request_failed"
                } else "partial_result",
            )
            return
        if active["first_total"] is None:
            active["first_total"] = result.reported_total
        elif result.reported_total != active["first_total"]:
            active["total_changed"] = True
        active["members"].update(result.members)
        elapsed = max(
            now.tick - active["started_tick"],
            (now.utc - active["requested_at"]).total_seconds(),
        )
        if result.after is not None and job.page_number >= PRESENCE_MAX_PAGES:
            self._finalize_presence(now, status="partial", reason="page_limit")
        elif result.after is not None and elapsed >= PRESENCE_MAX_ATTEMPT_SECONDS:
            self._finalize_presence(now, status="partial", reason="attempt_timeout")
        elif result.after is not None:
            active["next_job"] = PresencePageJob(
                snapshot_id=job.snapshot_id, run_id=job.run_id,
                stream_id=job.stream_id, broadcaster_id=job.broadcaster_id,
                moderator_id=job.moderator_id, requested_at=job.requested_at,
                generation=job.generation, page_number=job.page_number + 1,
                after=result.after,
            )
        else:
            self._finalize_presence(now, status="complete", reason="pagination_complete")

    def _handle_raid_context_result(self, result, now):
        job = result.job
        if result.fatal:
            raise CollectorError("authorization_blocked_restart_required")
        if result.error and job.attempt < 2 and result.error in {"request_failed", "rate_limited"}:
            retry = RaidContextJob(
                eventsub_message_id=job.eventsub_message_id,
                broadcaster_id=job.broadcaster_id, requested_at=job.requested_at,
                attempt=job.attempt + 1,
            )
            self._raid_context_queue.appendleft((retry, now.tick + 5))
            self.emit("raid_source_context_retry_scheduled")
            return
        if result.error:
            self.writer.finalize_raid_source_context(
                eventsub_message_id=job.eventsub_message_id,
                observed_at=now.utc, status="failed", reason_code=result.error,
                attempt_count=job.attempt,
            )
        elif result.not_found:
            self.writer.finalize_raid_source_context(
                eventsub_message_id=job.eventsub_message_id,
                observed_at=now.utc, status="not_found", reason_code="channel_not_found",
                attempt_count=job.attempt,
            )
        else:
            self.writer.finalize_raid_source_context(
                eventsub_message_id=job.eventsub_message_id,
                observed_at=now.utc, status="complete", reason_code="metadata_observed",
                attempt_count=job.attempt, metadata=result.metadata,
            )
        self.emit("raid_source_context_finalized")

    def _advance_enhanced(self, stop=None):
        if self._enhanced_worker is None:
            return
        now = self._sample()
        result = self._enhanced_worker.take()
        if result is not None:
            if isinstance(result.job, PresencePageJob):
                self._handle_presence_result(result, now)
                self._aux_last = "presence"
            else:
                self._handle_raid_context_result(result, now)
                self._aux_last = "raid"
            now = self._sample()

        if self._presence_active is not None:
            active = self._presence_active
            if active["generation"] != self._generation:
                self._finalize_presence(now, status="rejected", reason="clock_gap",
                                        health_reason="clock_gap")
            elif active["stream_id"] != self.tracked_stream:
                self._finalize_presence(now, status="rejected", reason="stream_changed",
                                        health_reason="stream_changed")
            elif not self._presence_live:
                self._finalize_presence(now, status="rejected", reason="poll_failed",
                                        health_reason="poll_failed")
        self._begin_presence_if_due(now)
        if self._enhanced_worker.busy or (stop is not None and stop.is_set()):
            return
        # Leave a small scheduling lane before the next viewer poll. Each page
        # is otherwise one bounded request, allowing the token lock to yield.
        if self.worker.busy or self._next_poll - now.tick < 5:
            return
        presence_job = (self._presence_active or {}).get("next_job")
        raid_ready = bool(self._raid_context_queue and self._raid_context_queue[0][1] <= now.tick)
        job = None
        if presence_job is not None and (not raid_ready or self._aux_last != "presence"):
            job = presence_job
            self._presence_active["next_job"] = None
        elif raid_ready:
            job, _due = self._raid_context_queue.popleft()
        elif presence_job is not None:
            job = presence_job
            self._presence_active["next_job"] = None
        if job is not None:
            self._enhanced_worker.start(job)

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
                self._chat_polling("stale", now)
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
                self._chat_polling("failed", now)
                self._health("error", result.error, now)
                raise CollectorError("authorization_blocked_restart_required")
            if self._discard_job or age(now, self._job_started) >= STALE_SECONDS:
                self.live_status.poll_failed()
                self._chat_polling("failed", now)
                self.emit("late_poll_discarded")
            elif result.error:
                self.live_status.poll_failed()
                self._chat_polling("failed", now)
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

        self._pump_eventsub()
        self._advance_enhanced(stop)

    def _save(self, result, now):
        observed = result.observed
        if (observed is None or observed.utc < self._job_started.utc
                or observed.tick < self._job_started.tick or observed.utc > now.utc
                or observed.tick > now.tick):
            self.live_status.poll_failed()
            self._chat_polling("failed", now)
            raise CollectorError("observation_clock_invalid")
        if age(now, observed) >= STALE_SECONDS:
            self.live_status.poll_failed()
            self._chat_polling("failed", now)
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
                self._chat_polling("failed", now)
                self._health("error", "api_error", now)
                return
            poll_params = {
                "stream_id": stream.stream_id, "started_at": stream.started_at,
                "observed_at": observed.utc, "viewer_count": stream.viewer_count,
            }
            if self._enhanced is not None:
                poll_params.update(run_id=self.run_id, metadata=stream.metadata)
            self.writer.record_live_poll(**poll_params)
            if self._enhanced is not None and stream.metadata is None:
                self.emit("stream_metadata_invalid_skipped")
            if self._enhanced is not None and self._presence_available:
                if self.tracked_stream != stream.stream_id or self._next_presence is None:
                    self._next_presence = observed.tick
            self.tracked_stream = stream.stream_id
            reason = "live_poll_saved"

        # Database work can also block. Recheck after data and health commits.
        now = self._sample()
        self._check_stale(now)
        if generation != self._generation or age(now, observed) >= STALE_SECONDS:
            self.live_status.poll_failed()
            self._chat_polling("failed", now)
            self.emit("saved_poll_no_longer_fresh")
            self._check_stale(now)
            return
        self._health("healthy", reason, now)
        self._last_success = observed
        self._stale_reported = False
        now = self._sample()
        if generation != self._generation or age(now, observed) >= STALE_SECONDS:
            self.live_status.poll_failed()
            self._chat_polling("failed", now)
            self._check_stale(now)
            return
        if stream is not None:
            self.live_status.poll_live(stream.stream_id, observed.utc, observed.tick)
            self._chat_polling("live", now)
        else:
            self._chat_polling("offline", now)

    def run(self, stop, *, duration=None):
        try:
            self.start()
            self._start_eventsub(stop)
            while not stop.is_set():
                if duration is not None and age(self._sample(), self.started) >= duration:
                    break
                self.step(stop)
                stop.wait(TICK_SECONDS)
            self.live_status.poll_failed()
            if self.worker.busy:
                self.emit("shutdown_waiting_for_twitch")
            self.worker.finish()
            self._shutdown_enhanced()
            return self._stop_run()
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
            if self._enhanced_worker is not None:
                self._enhanced_worker.finish()
            if self._probe is not None:
                self._probe.shutdown()

    def _shutdown_enhanced(self):
        if self._enhanced_worker is None:
            return
        result = self._enhanced_worker.finish()
        now = self._sample()
        if result is not None:
            if isinstance(result.job, PresencePageJob):
                self._handle_presence_result(result, now)
            else:
                self._handle_raid_context_result(result, now)
            now = self._sample()
        if self._presence_active is not None:
            self._finalize_presence(
                now, status="interrupted", reason="shutdown",
                health_reason="interrupted",
            )
        while self._raid_context_queue:
            job, _due = self._raid_context_queue.popleft()
            self.writer.finalize_raid_source_context(
                eventsub_message_id=job.eventsub_message_id,
                observed_at=now.utc, status="interrupted", reason_code="shutdown",
                attempt_count=max(0, job.attempt - 1),
            )
        self.emit("enhanced_collection_shutdown_complete")

    def _stop_run(self):
        """Orderly shutdown: close the run and every active source's health."""
        if self._probe is None:
            if self._presence_available:
                self.writer.stop_collector_run_multi(
                    run_id=self.run_id, stopped_at=self._sample().utc,
                    sources=("stream_poll", "chatter_presence"),
                )
            else:
                self.writer.stop_collector_run(run_id=self.run_id, stopped_at=self._sample().utc)
            self.emit("orderly_shutdown")
            return 0
        # Quiesce the socket and its workers so no sink writes after the stop rows.
        self._probe.shutdown()
        if self._probe_fatal is not None or self._probe.router.storage_failed:
            self.emit("capture_storage_failure_stop_required" if self._probe.router.storage_failed
                      else self._probe_fatal)
            return 1
        sources = ["stream_poll", *self._probe.router.source_names]
        if self._presence_available:
            sources.append("chatter_presence")
        self.writer.stop_collector_run_multi(
            run_id=self.run_id, stopped_at=self._sample().utc,
            sources=tuple(sources),
        )
        self.emit("orderly_shutdown")
        return 0


def positive_seconds(value):
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Use a positive duration in seconds.") from None
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("Use a positive duration in seconds.")
    return seconds


def _build_eventsub(auth, *, chat=True):
    """Resolve the subscription specs and open the initial socket before the run.

    A Helix lookup and one socket open, done up front like the standalone capture
    collector so they do not eat Twitch's post-welcome subscription window.
    ``chat=False`` (``--no-chat``) keeps the merged runtime at raids + follows.
    """
    from scripts.check_eventsub import URL, SetupWorker, open_socket

    sources = CAPTURED_EVENTSUB_SOURCES if chat else tuple(
        source for source in CAPTURED_EVENTSUB_SOURCES if source != "chat")
    specs = tuple(spec for spec in prepare_subscriptions(auth) if spec.source in sources)
    return EventSubConfig(
        auth=auth, specs=specs, socket=open_socket(),
        connector=open_socket, worker_factory=SetupWorker, url=URL, sources=sources,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Collect stream status, viewer snapshots, polling health, and EventSub "
                    "chat/raids/follows with enhanced collection context.")
    parser.add_argument("--duration", type=positive_seconds,
                        help="Stop after this many seconds; omitted means run until Ctrl+C/SIGTERM.")
    parser.add_argument("--no-eventsub", action="store_true",
                        help="Polling only: skip chat/raid/follow capture (no EventSub socket).")
    parser.add_argument("--no-chat", action="store_true",
                        help="Merged runtime without chat: raids and follows only.")
    parser.add_argument("--no-presence", action="store_true",
                        help="Disable five-minute chatter-presence snapshots.")
    args = parser.parse_args(argv)
    stop = threading.Event()
    previous_handlers = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, lambda *_: stop.set())
        auth = TokenManager.load()
        eventsub = None if args.no_eventsub else _build_eventsub(auth, chat=not args.no_chat)
        enhanced = EnhancedConfig(
            auth=auth, presence=not args.no_presence,
            raid_context=not args.no_eventsub,
        )
        with open_writer() as writer:
            return PollingCollector(
                writer, PollWorker(TwitchPoller(auth)), eventsub=eventsub,
                enhanced=enhanced,
            ).run(stop, duration=args.duration)
    except TwitchError:
        diagnostic("authorization_load_failed_restart_required")
        return 1
    except ProbeError as error:
        diagnostic(str(error))
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
