"""Socket recovery for the readiness probe, without event or health persistence."""

from dataclasses import dataclass, field
import threading

from websockets.exceptions import WebSocketException

from scripts.collect_stream import age
from scripts.eventsub import ProbeError, ReconnectRequest, SessionReadiness
from scripts.twitch_auth import TwitchError


TICK = 0.25
BACKOFF = (1, 2, 4, 8, 16, 30)
HANDOVER_DEADLINE = 25  # Leave five seconds for closing the old socket before 30.


@dataclass(frozen=True)
class ConnectionResult:
    socket: object = field(default=None, repr=False)
    welcome: str | None = field(default=None, repr=False)
    received: object = field(default=None, repr=False)
    error: str | None = None


class ConnectionJob:
    """Open and receive welcome without blocking the old socket's reader.

    Cold reconnects validate authorization first, after the old token worker has
    finished. Directed handovers never touch tokens or subscription creation.
    """

    def __init__(self, connector, url, auth, clock):
        self.connector, self.url, self.auth, self.clock = connector, url, auth, clock
        self.result = None
        self.thread = threading.Thread(target=self._run, name="eventsub-connect", daemon=False)

    def start(self):
        self.thread.start()

    @property
    def done(self):
        return not self.thread.is_alive()

    def finish(self):
        if self.thread.ident is not None:
            self.thread.join()
        return self.result or ConnectionResult(error="probe_internal_error")

    def _run(self):
        socket = None
        try:
            if self.auth is not None:
                self.auth.validate_if_due(force=True)
            socket = self.connector(self.url)
            raw = socket.recv(timeout=10)
            self.result = ConnectionResult(socket, raw, self.clock())
        except TwitchError:
            self.result = ConnectionResult(socket=socket, error="authorization_check_failed")
        except (WebSocketException, OSError):
            self.result = ConnectionResult(socket=socket, error="connection_attempt_failed")
        except Exception:
            self.result = ConnectionResult(socket=socket, error="probe_internal_error")


class RecoveringProbe:
    def __init__(self, socket, auth, specs, stop, *, duration, emit, clock,
                 worker_factory, connector=None, url=None, job_factory=ConnectionJob):
        self.socket, self.auth, self.specs, self.stop = socket, auth, specs, stop
        self.duration, self.emit, self.clock = duration, emit, clock
        self.worker_factory, self.connector, self.url = worker_factory, connector, url
        self.job_factory = job_factory
        self.state = SessionReadiness(specs, emit=emit)
        self.worker = None
        self.job = None
        self.handover = False
        self.had_gap = False
        self.gap_pending = False
        self.retry_count = 0
        self.previous = None

    def sample(self):
        now = self.clock()
        if self.previous is not None:
            tick = now.tick - self.previous.tick
            utc = (now.utc - self.previous.utc).total_seconds()
            if tick < 0 or utc < 0 or abs(tick - utc) >= 5:
                raise ProbeError("clock_uncertain")
        self.previous = now
        return now

    def close(self, socket):
        if socket is not None:
            try:
                socket.close()
            except Exception:
                self.emit("socket_close_failed")
                return False
        return True

    def notices(self, *, discard_success=False):
        if self.worker is not None:
            while (notice := self.worker.take()) is not None:
                if notice.error or not discard_success:
                    self.state.setup_result(notice)

    def stop_worker(self):
        if self.worker is not None:
            self.emit("probe_waiting_for_token_worker")
            self.worker.finish()
            self.notices(discard_success=True)
            self.worker = None

    def start_worker(self):
        if self.stop.is_set():
            return
        # Source setup failures and revocations remain visible for this probe.
        active = tuple(spec for spec in self.specs if spec.source not in self.state.failed)
        if not active:
            raise ProbeError("no_usable_subscriptions")
        self.worker = self.worker_factory(self.auth, active, self.state.session_id, threading.Event())
        self.worker.start()

    def schedule_retry(self):
        delay = BACKOFF[min(self.retry_count, len(BACKOFF) - 1)]
        self.retry_count += 1
        self.retry_at = self.sample().tick + delay
        self.emit(f"reconnect_wait_{delay}_seconds")

    def lose(self, code):
        if self.connector is None:
            raise ProbeError(code)
        self.emit(code)
        if not self.gap_pending:
            self.emit("probe_gap_detected_no_replay")
        self.had_gap = self.gap_pending = True
        self.state.responsive = False
        self.state.ids.clear()
        self.close(self.socket)
        self.socket = None
        if self.job is not None:
            result = self.job.finish()
            self.job = None
            self.close(result.socket)
            if result.error in {"authorization_check_failed", "probe_internal_error"}:
                raise ProbeError(result.error)
        self.handover = False
        # Never overlap old token work with new authorization or setup.
        self.stop_worker()
        self.schedule_retry()

    def start_job(self, url, *, handover):
        if self.stop.is_set():
            return
        self.job_started = self.sample()
        self.handover = handover
        self.job = self.job_factory(self.connector, url, None if handover else self.auth, self.clock)
        self.job.start()
        self.emit("handover_connecting" if handover else "reconnect_connecting")

    def request_handover(self, request):
        if self.connector is None:
            raise ProbeError("reconnect_required_probe_ended")
        if self.job is not None:
            if request.url != self.job.url:
                self.lose("handover_conflicting_request")
            return  # A duplicate reconnect does not restart its deadline.
        if self.worker is None or not self.worker.setup_complete.is_set():
            self.lose("handover_during_setup_gap")
            return
        self.start_job(request.url, handover=True)

    def complete_job(self, now):
        job, self.job = self.job, None
        result = job.finish()
        now = self.sample()
        candidate = result.socket
        try:
            if self.stop.is_set() or age(now, self.started) >= self.duration:
                self.state.responsive = False
                self.stop.set()
                self.close(candidate)
                return
            if result.error:
                raise ProbeError(result.error)
            received = result.received
            if (received is None or received.tick < self.job_started.tick
                    or received.utc < self.job_started.utc or received.tick > now.tick
                    or received.utc > now.utc):
                raise ProbeError("clock_uncertain")
            fresh = SessionReadiness(self.specs, emit=self.emit)
            fresh.accept(result.welcome, received)
            fresh.check_liveness(now)
            if self.handover:
                self.state.check_liveness(now)
                if age(now, self.job_started) >= HANDOVER_DEADLINE:
                    raise ProbeError("handover_timeout")
                # Preserve revocations and other buffered old-socket evidence
                # before closing it. Bound draining to avoid starving deadlines.
                for _ in range(256):
                    try:
                        raw = self.socket.recv(timeout=0)
                    except TimeoutError:
                        break
                    request = self.state.accept(raw, self.sample())
                    if isinstance(request, ReconnectRequest) and request.url != job.url:
                        raise ProbeError("handover_conflicting_request")
                else:
                    raise ProbeError("handover_drain_limit")
                if not self.close(self.socket):
                    raise ProbeError("handover_close_failed")
                self.socket = None
                now = self.sample()
                if self.stop.is_set() or age(now, self.started) >= self.duration:
                    self.state.responsive = False
                    self.stop.set()
                    self.close(candidate)
                    return
                if age(now, self.job_started) >= 30:
                    raise ProbeError("handover_timeout")
                fresh.check_liveness(now)
                fresh.ids = self.state.ids.copy()
                fresh.delivery_seen = self.state.delivery_seen.copy()
            fresh.failed = self.state.failed.copy()
            self.state = fresh
            self.socket, candidate = candidate, None
            self.connected = received
            if self.handover:
                self.emit("handover_complete_subscriptions_preserved")
            else:
                self.start_worker()
            self.handover = False
        except (ProbeError, WebSocketException, OSError) as error:
            self.close(candidate)
            code = str(error) if isinstance(error, ProbeError) else "connection_attempt_failed"
            if code in {"authorization_check_failed", "probe_internal_error", "clock_uncertain"}:
                raise ProbeError(code) from None
            if self.handover:
                self.lose("handover_failed_gap")
            elif code in {"connection_attempt_failed", "keepalive_timeout"}:
                self.emit(code)
                self.schedule_retry()
            else:
                raise ProbeError(code) from None

    def step(self, now):
        self.notices()
        if self.job is not None:
            if self.job.done:
                self.complete_job(now)
                now = self.sample()
            elif self.handover and age(now, self.job_started) >= HANDOVER_DEADLINE:
                self.lose("handover_timeout")
        if self.socket is None:
            if self.job is None and now.tick >= self.retry_at and not self.stop.is_set():
                self.start_job(self.url, handover=False)
            self.stop.wait(TICK)
            return
        try:
            self.state.check_liveness(now)
            if self.state.session_id is None and age(now, self.connected) > 10:
                raise ProbeError("welcome_timeout")
            if self.state.ready:
                if self.gap_pending:
                    self.emit("probe_subscriptions_reconfirmed_after_gap")
                    self.gap_pending = False
                if age(now, self.connected) >= 60:
                    self.retry_count = 0
            try:
                raw = self.socket.recv(timeout=TICK)
            except TimeoutError:
                return
            now = self.sample()
            self.state.check_liveness(now)
            if self.stop.is_set() or age(now, self.started) >= self.duration:
                return
            if self.state.session_id is None and age(now, self.connected) > 10:
                raise ProbeError("welcome_timeout")
            request = self.state.accept(raw, now)
            if self.worker is None:
                self.start_worker()
            if isinstance(request, ReconnectRequest):
                self.request_handover(request)
        except (WebSocketException, OSError):
            self.lose("network_error")
        except ProbeError as error:
            if str(error) not in {"keepalive_timeout", "welcome_timeout"}:
                raise
            self.lose(str(error))

    def run(self):
        failed = False
        try:
            self.started = self.connected = self.sample()
            while not self.stop.is_set():
                now = self.sample()
                if age(now, self.started) >= self.duration:
                    break
                self.step(now)
            if self.socket is not None:
                self.state.check_liveness(self.sample())
            failed = self.job is not None or self.socket is None
        except ProbeError as error:
            self.emit(str(error))
            failed = True
        except Exception:
            self.emit("probe_internal_error")
            failed = True
        finally:
            self.stop.set()
            if not self.close(self.socket):
                failed = True
            if self.job is not None:
                result = self.job.finish()
                if not self.close(result.socket) or result.error:
                    failed = True
            try:
                self.stop_worker()
            except Exception:
                self.emit("probe_worker_failed_during_shutdown")
                failed = True
            self.emit("probe_socket_closed")
        if not failed and self.state.ready:
            self.emit("probe_finished_subscriptions_confirmed_after_gap" if self.had_gap
                      else "probe_finished_subscriptions_confirmed")
            return 0
        self.emit("probe_finished_readiness_not_confirmed")
        return 1
