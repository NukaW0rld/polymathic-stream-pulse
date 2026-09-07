"""EventSub subscription setup and single-session readiness, without persistence.

The caller serializes SessionReadiness access. This is not chat eligibility or
event-time association. Event bodies are discarded; no capture health is emitted.
"""

from dataclasses import dataclass, field
from datetime import datetime
import json
from urllib.parse import urlsplit

from scripts.twitch_auth import TwitchError


SOURCES = ("chat", "raids", "follows")
# Probe-only network/scheduler tolerance, not an extension of chat eligibility.
KEEPALIVE_GRACE_SECONDS = 2
SUBSCRIPTION_DIAGNOSTICS = frozenset({
    "subscription_response_shape_invalid", "subscription_response_id_invalid",
    "subscription_response_type_mismatch", "subscription_response_version_mismatch",
    "subscription_response_condition_mismatch",
    "subscription_response_transport_mismatch", "subscription_response_session_mismatch",
    "subscription_response_not_enabled",
})


class ProbeError(Exception):
    """Only internal diagnostic codes; no response contents."""


@dataclass(frozen=True)
class ReconnectRequest:
    url: str = field(repr=False)


@dataclass(frozen=True)
class EventDelivery:
    """A validated notification/revocation envelope for a confirmed subscription.

    ``accept`` returns this so a capture consumer can persist the event; the
    readiness probe ignores it. ``message`` is the decoded frame and is kept out
    of ``repr`` so event contents are never printed.
    """

    kind: str
    source: str
    message: dict = field(repr=False)


def _reconnect_url(value):
    if not _text(value) or any(ord(char) < 33 or char == "\\" for char in value):
        raise ValueError
    parsed = urlsplit(value)
    if (parsed.scheme != "wss" or parsed.hostname != "eventsub.wss.twitch.tv"
            or parsed.port not in (None, 443) or parsed.username is not None
            or parsed.password is not None or parsed.fragment):
        raise ValueError
    return value  # Preserve Twitch's URL exactly, including query parameters.


def _text(value):
    return isinstance(value, str) and bool(value)


def _timestamp(value):
    if not _text(value):
        raise ValueError
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError


@dataclass(frozen=True)
class SubscriptionSpec:
    source: str
    event_type: str
    version: str
    condition: dict = field(repr=False)

    def request(self, session_id):
        return {
            "type": self.event_type, "version": self.version,
            "condition": self.condition.copy(),
            "transport": {"method": "websocket", "session_id": session_id},
        }

    def matches(self, subscription, session_id):
        return self.mismatch(subscription, session_id) is None

    def mismatch(self, subscription, session_id):
        """Return a fixed field diagnostic, never actual response values."""
        if not isinstance(subscription, dict):
            return "subscription_response_shape_invalid"
        if not _text(subscription.get("id")):
            return "subscription_response_id_invalid"
        if subscription.get("type") != self.event_type:
            return "subscription_response_type_mismatch"
        if subscription.get("version") != self.version:
            return "subscription_response_version_mismatch"
        condition = subscription.get("condition")
        # The live API represents the unused incoming-raid origin as an empty
        # string. Accept only that observed representation, without mutating the
        # response or allowing an origin filter, wrong target, or unknown keys.
        if (self.source == "raids" and self.event_type == "channel.raid"
                and set(self.condition) == {"to_broadcaster_user_id"}
                and isinstance(condition, dict)
                and condition == self.condition | {"from_broadcaster_user_id": ""}):
            condition = self.condition
        if condition != self.condition:
            return "subscription_response_condition_mismatch"
        transport = subscription.get("transport")
        if not isinstance(transport, dict) or transport.get("method") != "websocket":
            return "subscription_response_transport_mismatch"
        if transport.get("session_id") != session_id:
            return "subscription_response_session_mismatch"
        return None


def prepare_subscriptions(auth):
    """Validate and resolve before opening the socket's subscription deadline."""
    auth.validate_if_due()
    response = auth.helix_get("users", {"login": "polymathic"})
    users = response.get("data") if isinstance(response, dict) else None
    if (not isinstance(users, list) or len(users) != 1
            or not isinstance(users[0], dict) or not _text(users[0].get("id"))
            or users[0].get("login") != "polymathic" or not _text(auth.user_id)):
        raise ProbeError("target_lookup_failed")
    target = users[0]["id"]
    return (
        SubscriptionSpec("chat", "channel.chat.message", "1",
                         {"broadcaster_user_id": target, "user_id": auth.user_id}),
        SubscriptionSpec("raids", "channel.raid", "1", {"to_broadcaster_user_id": target}),
        SubscriptionSpec("follows", "channel.follow", "2",
                         {"broadcaster_user_id": target, "moderator_user_id": auth.user_id}),
    )


def create_subscription(auth, spec, session_id):
    """Return a private subscription ID only after matching enabled evidence.

    No retry on an uncertain creation or conflict. The probe reports this source
    as unconfirmed and can still check the other subscriptions.
    """
    response = auth.helix_post("eventsub/subscriptions", spec.request(session_id))
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, list) or len(data) != 1:
        reason = "subscription_response_shape_invalid"
    else:
        reason = spec.mismatch(data[0], session_id)
        if reason is None and data[0].get("status") != "enabled":
            reason = "subscription_response_not_enabled"
    if reason is not None:
        raise TwitchError("Subscription readiness not confirmed.", reason_code=reason)
    return data[0]["id"]


@dataclass(frozen=True)
class SetupNotice:
    source: str | None = None
    subscription_id: str | None = field(default=None, repr=False)
    error: str | None = None
    fatal: bool = False
    http_status: int | None = None


class SessionReadiness:
    """Track a single socket; all diagnostic values come from fixed allowlists."""

    def __init__(self, specs, *, emit):
        specs = tuple(specs)
        self.specs = {spec.source: spec for spec in specs}
        # A session may carry any non-empty subset of the sources (the readiness
        # probe uses all three; the capture collector uses raids + follows).
        if len(self.specs) != len(specs) or not self.specs or not set(self.specs) <= set(SOURCES):
            raise ProbeError("invalid_subscription_specs")
        self.expected = frozenset(self.specs)
        self.emit = emit
        self.session_id = None
        self.timeout = None
        self.last_liveness = None
        self.ids = {}
        self.failed = set()
        self.delivery_seen = set()
        self.responsive = False
        self.in_grace = False

    @property
    def ready(self):
        return (self.responsive and not self.in_grace and not self.failed
                and set(self.ids) == self.expected)

    def source_ready(self, source):
        """One source's transport readiness, independent of the other subscriptions."""
        return (self.responsive and not self.in_grace
                and source in self.ids and source not in self.failed)

    def setup_result(self, notice):
        if notice.source is None or notice.fatal:
            raise ProbeError("authorization_check_failed" if notice.error != "internal_error"
                             else "probe_internal_error")
        if notice.source not in self.specs:
            raise ProbeError("probe_internal_error")
        if notice.error:
            self.failed.add(notice.source)
            allowed = SUBSCRIPTION_DIAGNOSTICS | {"auth_error", "network_error"}
            code = notice.error if notice.error in allowed else "subscription_error"
            suffix = (f"_http_{notice.http_status}" if type(notice.http_status) is int
                      and 100 <= notice.http_status <= 599 else "")
            self.emit(f"{notice.source}_{code}{suffix}")
        elif notice.source not in self.failed:
            # A revocation can arrive before its POST acknowledgement. Never
            # restore readiness from that later acknowledgement.
            if not _text(notice.subscription_id):
                raise ProbeError("probe_internal_error")
            self.ids[notice.source] = notice.subscription_id
            self.emit(f"{notice.source}_subscription_enabled")

    def check_liveness(self, now):
        if self.last_liveness is not None:
            age = max(now.tick - self.last_liveness.tick,
                      (now.utc - self.last_liveness.utc).total_seconds())
            if age > self.timeout + KEEPALIVE_GRACE_SECONDS:
                raise ProbeError("keepalive_timeout")
            if age > self.timeout and not self.in_grace:
                self.in_grace = True
                self.emit("keepalive_waiting_within_grace")

    def _received_liveness(self, now):
        if self.in_grace:
            self.emit("liveness_received_within_grace")
        self.in_grace = False
        self.last_liveness = now
        self.responsive = True

    def accept(self, raw, now):
        """Validate transport/subscription envelope, then discard event contents.

        Notification timestamps are syntax-checked only. Event-body validation,
        duplicates, and event-time eligibility belong to later capture work.
        """
        self.check_liveness(now)
        try:
            if not isinstance(raw, str):
                raise ValueError
            message = json.loads(raw)
            metadata, payload = message["metadata"], message["payload"]
            if not isinstance(metadata, dict) or not isinstance(payload, dict):
                raise ValueError
            if not _text(metadata.get("message_id")):
                raise ValueError
            _timestamp(metadata.get("message_timestamp"))
            kind = metadata["message_type"]
            if self.session_id is None:
                session = payload["session"]
                if (kind != "session_welcome" or session.get("status") != "connected"
                        or not _text(session.get("id"))
                        or type(session.get("keepalive_timeout_seconds")) is not int
                        or not 10 <= session["keepalive_timeout_seconds"] <= 600):
                    raise ValueError
                _timestamp(session.get("connected_at"))
                self.session_id = session["id"]
                self.timeout = session["keepalive_timeout_seconds"]
                self.last_liveness = now
                self.emit("session_welcome_received")
                return
            if kind == "session_reconnect":
                session = payload["session"]
                if session.get("id") != self.session_id or session.get("status") != "reconnecting":
                    raise ValueError
                return ReconnectRequest(_reconnect_url(session.get("reconnect_url")))
            if kind == "session_keepalive":
                if payload:
                    raise ValueError
                if not self.responsive:
                    self.emit("session_responsive")
                self._received_liveness(now)
                return
            if kind not in {"notification", "revocation"}:
                raise ValueError
            spec = next((spec for spec in self.specs.values()
                         if spec.event_type == metadata.get("subscription_type")
                         and spec.version == metadata.get("subscription_version")), None)
            subscription = payload["subscription"]
            if spec is None or not spec.matches(subscription, self.session_id):
                raise ValueError
            known_id = self.ids.get(spec.source)
            if known_id is not None and subscription["id"] != known_id:
                raise ValueError
            if kind == "revocation":
                if not _text(subscription.get("status")) or subscription["status"] == "enabled":
                    raise ValueError
                if spec.source not in self.failed:
                    self.emit(f"{spec.source}_subscription_revoked")
                self.failed.add(spec.source)
                return EventDelivery("revocation", spec.source, message)
            if subscription.get("status") != "enabled" or not isinstance(payload.get("event"), dict):
                raise ValueError
            self._received_liveness(now)
            if known_id is not None and spec.source not in self.failed and spec.source not in self.delivery_seen:
                self.delivery_seen.add(spec.source)
                self.emit(f"{spec.source}_notification_envelope_received")
            return EventDelivery("notification", spec.source, message)
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            raise ProbeError("invalid_eventsub_message") from None
