"""Recovering EventSub readiness probe; no database access or event storage.

Run separately from the collector and other token-writing programs. A real run
creates Twitch subscriptions and may refresh private tokens. Socket recovery is
implemented here; event capture and its health integration remain separate.
"""

import argparse
import logging
from queue import Empty, SimpleQueue
import signal
import sys
import threading

from websockets.exceptions import WebSocketException
from websockets.sync.client import connect

from scripts.collect_stream import diagnostic, positive_seconds, read_clock
from scripts.eventsub import (
    ProbeError, SetupNotice, SUBSCRIPTION_DIAGNOSTICS,
    create_subscription, prepare_subscriptions,
)
from scripts.twitch_auth import TokenManager, TwitchError


URL = "wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds=30"
TICK_SECONDS = 0.25
VALIDATION_CHECK_SECONDS = 30


def open_socket(url=URL):
    # Debug logs can include raw frames. Use a private disabled logger regardless
    # of global logging settings. Automatic client Ping is prohibited by Twitch;
    # the library still responds to server Ping with Pong.
    logger = logging.Logger("stream_pulse.eventsub.private")
    logger.disabled = True
    return connect(
        url, ping_interval=None, proxy=None, compression=None,
        open_timeout=10, close_timeout=5, max_size=1048576, max_queue=16,
        logger=logger,
    )


class SetupWorker:
    """One thread owns all token/API work while the caller reads the socket.

    At most three setup results plus a terminal error are queued. No event data
    enters this queue. Shutdown waits for token rotation/persistence to finish.
    """

    def __init__(self, auth, specs, session_id, stop):
        self.auth, self.specs = auth, specs
        self.session_id, self.stop = session_id, stop
        self.results = SimpleQueue()
        self.setup_complete = threading.Event()
        self.thread = threading.Thread(target=self._run, name="eventsub-setup", daemon=False)

    def start(self):
        self.thread.start()

    def take(self):
        try:
            return self.results.get_nowait()
        except Empty:
            return None

    def finish(self):
        self.stop.set()
        if self.thread.ident is not None:
            self.thread.join()

    def _run(self):
        try:
            for spec in self.specs:
                if self.stop.is_set():
                    return
                try:
                    subscription_id = create_subscription(self.auth, spec, self.session_id)
                    notice = SetupNotice(source=spec.source, subscription_id=subscription_id)
                except TwitchError as error:
                    allowed = SUBSCRIPTION_DIAGNOSTICS | {"network_error", "auth_error"}
                    reason = (error.reason_code if error.reason_code in allowed
                              else "subscription_error")
                    status = (error.status if type(error.status) is int
                              and 100 <= error.status <= 599 else None)
                    notice = SetupNotice(source=spec.source, error=reason, fatal=error.fatal,
                                         http_status=status)
                self.results.put(notice)
                if notice.fatal:
                    return
            self.setup_complete.set()
            # No dependence on chat/raid/follow activity. The manager performs
            # actual validation only when its hourly deadline is due.
            while not self.stop.wait(VALIDATION_CHECK_SECONDS):
                self.auth.validate_if_due()
        except TwitchError:
            # Even a transient idle-validation failure ends this probe. An
            # integrated collector will need an explicit auth recovery policy.
            self.results.put(SetupNotice(error="auth_error", fatal=True))
        except Exception:
            # Thread tracebacks and exception details can contain private values.
            self.results.put(SetupNotice(error="internal_error", fatal=True))


def run_session(socket, auth, specs, stop, *, duration, emit=diagnostic,
                clock=read_clock, worker_factory=SetupWorker, connector=None,
                job_factory=None, router=None, heartbeat=None):
    """Run the probe; a connector enables recovery, a router enables capture."""
    from scripts.eventsub_recovery import ConnectionJob, RecoveringProbe

    return RecoveringProbe(
        socket, auth, specs, stop, duration=duration, emit=emit, clock=clock,
        worker_factory=worker_factory, connector=connector, url=URL,
        job_factory=ConnectionJob if job_factory is None else job_factory,
        router=router, heartbeat=heartbeat,
    ).run()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check EventSub readiness with socket recovery, without storing events.")
    parser.add_argument("--duration", type=positive_seconds, default=60,
                        help="Seconds after socket opening before requesting shutdown (default: 60).")
    args = parser.parse_args(argv)
    stop = threading.Event()
    previous_handlers = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, lambda *_: stop.set())
        auth = TokenManager.load()
        specs = prepare_subscriptions(auth)
        if stop.is_set():
            diagnostic("probe_cancelled_before_connection")
            return 1
        socket = open_socket()
        return run_session(socket, auth, specs, stop, duration=args.duration, connector=open_socket)
    except TwitchError:
        diagnostic("probe_authorization_or_lookup_failed")
        return 1
    except ProbeError as error:
        diagnostic(str(error))
        return 1
    except (WebSocketException, OSError):
        diagnostic("probe_connection_failed")
        return 1
    except Exception:
        diagnostic("probe_startup_failed")
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
