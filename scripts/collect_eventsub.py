"""EventSub capture collector for incoming raids and follows.

Run from the repository root, with the polling collector and every other
token-writing program stopped:

    .venv/bin/python -m scripts.collect_eventsub
    .venv/bin/python -m scripts.collect_eventsub --duration 300

This is a separate process from ``collect_stream``: it opens its own collector
run. It makes real Twitch calls, creates ``channel.raid`` and ``channel.follow``
subscriptions, persists events and per-source EventSub health to the private
local database, and may refresh the private token file. It does NOT capture chat
(that needs observed-live eligibility) and it does not poll stream status.

The socket, recovery, keepalive, and clock rules are the readiness probe's,
reused unchanged; this module adds the run lifecycle and the persistence router.
"""

import argparse
import math
import signal
import sys
import threading

from websockets.exceptions import WebSocketException

from scripts.check_eventsub import SetupWorker, open_socket, run_session
from scripts.collect_stream import diagnostic, positive_seconds, read_clock
from scripts.database import StorageError, open_writer
from scripts.event_sink import FollowSink, RaidSink
from scripts.eventsub import ProbeError, prepare_subscriptions
from scripts.eventsub_router import EventRouter
from scripts.twitch_auth import TokenManager, TwitchError


CAPTURED_SOURCES = ("raids", "follows")
HEARTBEAT_SECONDS = 30


class Heartbeat:
    """Per-tick collector-run check-in. Never raises out of ``__call__``.

    A failed heartbeat write is a storage failure: it latches the router so the
    session loop stops without writing further to an unreachable database.
    """

    def __init__(self, writer, run_id, router, *, interval=HEARTBEAT_SECONDS):
        self._writer = writer
        self._run_id = run_id
        self._router = router
        self._interval = interval
        self._due_at = None

    def __call__(self, now):
        if self._router.storage_failed:
            return
        if self._due_at is None:
            self._due_at = now.tick + self._interval  # First check-in one interval in.
            return
        if now.tick < self._due_at:
            return
        try:
            self._writer.update_collector_heartbeat(
                run_id=self._run_id, last_heartbeat_at=now.utc)
            self._due_at = now.tick + self._interval
        except StorageError:
            self._router.mark_storage_failed()


def collect(auth, writer, stop, *, duration, emit=diagnostic, clock=read_clock,
            connector=open_socket, worker_factory=SetupWorker, job_factory=None):
    """Open a run, capture raids + follows until stop/duration, then close the run."""
    specs = tuple(spec for spec in prepare_subscriptions(auth)
                  if spec.source in CAPTURED_SOURCES)
    if stop.is_set():
        emit("capture_cancelled_before_connection")
        return 1

    run_id = writer.start_collector_run(started_at=clock().utc)
    emit("capture_run_started")
    router = EventRouter(
        {"raids": RaidSink(writer, run_id, emit=emit),
         "follows": FollowSink(writer, run_id, emit=emit)},
        emit=emit,
    )
    heartbeat = Heartbeat(writer, run_id, router)

    socket = connector()
    try:
        code = run_session(
            socket, auth, specs, stop,
            duration=math.inf if duration is None else duration,
            emit=emit, clock=clock, connector=connector,
            worker_factory=worker_factory, job_factory=job_factory,
            router=router, heartbeat=heartbeat,
        )
    except StorageError:
        emit("capture_storage_failure_restart_required_commit_may_be_uncertain")
        return 1

    if router.storage_failed:
        emit("capture_storage_failure_restart_required_commit_may_be_uncertain")
        return 1
    try:
        writer.close_collector_run(run_id=run_id, stopped_at=clock().utc)
    except StorageError:
        emit("capture_run_close_failed_restart_required")
        return 1
    emit("capture_run_closed")
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Capture incoming raids and follows from EventSub into the local database.")
    parser.add_argument("--duration", type=positive_seconds,
                        help="Stop this many seconds after the socket opens; "
                             "omitted runs until Ctrl+C/SIGTERM.")
    args = parser.parse_args(argv)
    stop = threading.Event()
    previous_handlers = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, lambda *_: stop.set())
        auth = TokenManager.load()
        with open_writer() as writer:
            return collect(auth, writer, stop, duration=args.duration)
    except TwitchError:
        diagnostic("capture_authorization_or_lookup_failed_restart_required")
        return 1
    except ProbeError as error:
        diagnostic(str(error))
        return 1
    except StorageError:
        diagnostic("capture_database_startup_failed_restart_required")
        return 1
    except (WebSocketException, OSError):
        diagnostic("capture_connection_failed_restart_required")
        return 1
    except Exception:
        diagnostic("capture_startup_failed_restart_required")
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
