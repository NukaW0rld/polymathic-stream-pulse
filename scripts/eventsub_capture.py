"""Validate EventSub notification bodies into records ready for persistence.

Pure functions only: no socket, no database, no coordinator wiring. The caller
decodes the frame, has already checked the transport/session envelope (see
``scripts/eventsub.py``), and supplies the local receipt time. This module adds
the event-body validation that the readiness probe deliberately skips.

Stream association is not decided here: every parser leaves ``stream_id`` unset.
Follows and raids keep it NULL for good; the chat sink fills it from
observed-live eligibility before storage. ``CaptureError.reason_code`` is a
fine-grained diagnostic for local logs; it is distinct from the coarser
``invalid_notification`` EventSub health reason a coordinator would record.
"""

from dataclasses import dataclass, field
from datetime import datetime


FOLLOW_EVENT_TYPE = "channel.follow"
FOLLOW_EVENT_VERSION = "2"
RAID_EVENT_TYPE = "channel.raid"
RAID_EVENT_VERSION = "1"
CHAT_EVENT_TYPE = "channel.chat.message"
CHAT_EVENT_VERSION = "1"

# ``raid_viewer_count`` must fit the schema's INTEGER column. Rejecting an
# oversized value here keeps it from becoming a StorageError that halts the
# collector; real raids are far below this ceiling.
MAX_INT4 = 2_147_483_647


class CaptureError(Exception):
    """Safe capture diagnostic: a fixed reason code, never event contents."""

    def __init__(self, reason_code):
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class FollowNotification:
    """Fields for ``DatabaseWriter.record_follow_event``; ``stream_id`` stays None."""

    eventsub_message_id: str
    user_id: str
    followed_at: datetime
    notification_at: datetime
    received_at: datetime
    stream_id: None = None


@dataclass(frozen=True)
class RaidNotification:
    """Fields for ``DatabaseWriter.record_raid_event``; ``stream_id`` stays None."""

    eventsub_message_id: str
    from_broadcaster_user_id: str
    raid_viewer_count: int
    notification_at: datetime
    received_at: datetime
    stream_id: None = None


@dataclass(frozen=True)
class ChatNotification:
    """Fields for ``DatabaseWriter.record_chat_message``.

    ``message_text`` and ``message_fragments`` are private and kept out of
    ``repr`` so message content is never printed. ``stream_id`` is left None
    here: the chat sink fills it from observed-live eligibility, because chat --
    unlike follows and raids -- is only stored once a live stream is known.
    """

    eventsub_message_id: str
    chatter_user_id: str
    chat_message_id: str
    message_text: str = field(repr=False)
    message_fragments: list = field(repr=False)
    notification_at: datetime
    received_at: datetime
    source_broadcaster_user_id: str | None = None
    stream_id: str | None = None


def _text(value):
    return isinstance(value, str) and bool(value)


def _aware_timestamp(value, reason_code):
    """Parse an ISO 8601 instant to an aware datetime.

    Twitch sends UTC with a ``Z`` suffix and sub-microsecond precision; Python
    truncates the fractional part to microseconds. A missing offset is rejected.
    """
    if not _text(value):
        raise CaptureError(reason_code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise CaptureError(reason_code) from None
    if parsed.utcoffset() is None:
        raise CaptureError(reason_code)
    return parsed


def _notification_common(message, received_at, event_type, event_version):
    """Validate the parts every notification shares; return (id, notification_at, event)."""
    if not isinstance(received_at, datetime) or received_at.utcoffset() is None:
        raise CaptureError("received_at_invalid")
    if not isinstance(message, dict):
        raise CaptureError("message_not_dict")

    metadata = message.get("metadata")
    payload = message.get("payload")
    if not isinstance(metadata, dict) or not isinstance(payload, dict):
        raise CaptureError("envelope_invalid")
    if metadata.get("message_type") != "notification":
        raise CaptureError("message_type_unexpected")
    if (metadata.get("subscription_type") != event_type
            or metadata.get("subscription_version") != event_version):
        raise CaptureError("subscription_mismatch")

    message_id = metadata.get("message_id")
    if not _text(message_id):
        raise CaptureError("message_id_invalid")
    notification_at = _aware_timestamp(metadata.get("message_timestamp"), "notification_timestamp_invalid")

    event = payload.get("event")
    if not isinstance(event, dict):
        raise CaptureError("event_invalid")
    return message_id, notification_at, event


def parse_follow_notification(message, received_at):
    """Return a ``FollowNotification`` for a ``channel.follow`` v2 notification.

    ``received_at`` is the caller's timezone-aware local receipt time, captured
    before storage. Raises ``CaptureError`` with a fixed reason code on any
    structural problem; the exception never carries event contents.
    """
    message_id, notification_at, event = _notification_common(
        message, received_at, FOLLOW_EVENT_TYPE, FOLLOW_EVENT_VERSION,
    )
    user_id = event.get("user_id")
    if not _text(user_id):
        raise CaptureError("user_id_invalid")
    followed_at = _aware_timestamp(event.get("followed_at"), "followed_at_invalid")
    return FollowNotification(
        eventsub_message_id=message_id,
        user_id=user_id,
        followed_at=followed_at,
        notification_at=notification_at,
        received_at=received_at,
    )


def parse_raid_notification(message, received_at):
    """Return a ``RaidNotification`` for an incoming ``channel.raid`` v1 notification.

    ``channel.raid`` carries no event-time field, so ``notification_at`` is the
    earliest time available. ``raid_viewer_count`` comes from ``event.viewers``
    and must be a non-negative int within the schema's INTEGER range.
    """
    message_id, notification_at, event = _notification_common(
        message, received_at, RAID_EVENT_TYPE, RAID_EVENT_VERSION,
    )
    from_broadcaster_user_id = event.get("from_broadcaster_user_id")
    if not _text(from_broadcaster_user_id):
        raise CaptureError("from_broadcaster_user_id_invalid")
    viewers = event.get("viewers")
    if type(viewers) is not int or not 0 <= viewers <= MAX_INT4:
        raise CaptureError("viewer_count_invalid")
    return RaidNotification(
        eventsub_message_id=message_id,
        from_broadcaster_user_id=from_broadcaster_user_id,
        raid_viewer_count=viewers,
        notification_at=notification_at,
        received_at=received_at,
    )


def parse_chat_notification(message, received_at):
    """Return a ``ChatNotification`` for a ``channel.chat.message`` v1 notification.

    ``channel.chat.message`` carries no event-time field, so ``notification_at``
    (the envelope timestamp) is the earliest time available, as with
    ``channel.raid``. ``received_at`` is the caller's timezone-aware local
    receipt time, captured before storage. Fragments are stored verbatim for
    later reconstruction; this parser checks their outer shape only. A
    ``CaptureError`` reason code never carries message text, fragments, or
    identities.
    """
    message_id, notification_at, event = _notification_common(
        message, received_at, CHAT_EVENT_TYPE, CHAT_EVENT_VERSION,
    )
    chatter_user_id = event.get("chatter_user_id")
    if not _text(chatter_user_id):
        raise CaptureError("chatter_user_id_invalid")
    chat_message_id = event.get("message_id")
    if not _text(chat_message_id):
        raise CaptureError("chat_message_id_invalid")

    body = event.get("message")
    if not isinstance(body, dict):
        raise CaptureError("message_invalid")
    text = body.get("text")
    if not isinstance(text, str):
        raise CaptureError("message_text_invalid")
    fragments = body.get("fragments")
    if not isinstance(fragments, list) or not all(isinstance(part, dict) for part in fragments):
        raise CaptureError("message_fragments_invalid")

    source = event.get("source_broadcaster_user_id")
    if source is not None and not _text(source):
        raise CaptureError("source_broadcaster_user_id_invalid")

    return ChatNotification(
        eventsub_message_id=message_id,
        chatter_user_id=chatter_user_id,
        chat_message_id=chat_message_id,
        message_text=text,
        message_fragments=fragments,
        notification_at=notification_at,
        received_at=received_at,
        source_broadcaster_user_id=source,
    )
