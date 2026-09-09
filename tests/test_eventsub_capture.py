"""Synthetic tests for EventSub notification-body validation (follow, raid)."""

from datetime import datetime, timezone
import unittest

from scripts.eventsub_capture import (
    CaptureError, ChatNotification, FollowNotification, RaidNotification,
    parse_chat_notification, parse_follow_notification, parse_raid_notification,
)


RECEIVED_AT = datetime(2026, 9, 8, 20, 30, tzinfo=timezone.utc)


def follow_message(**event_overrides):
    """A structurally valid channel.follow v2 notification with synthetic values."""
    event = {
        "user_id": "synthetic-follower-1",
        "user_login": "synthetic_follower_1",
        "user_name": "Synthetic_Follower_1",
        "broadcaster_user_id": "synthetic-broadcaster",
        "broadcaster_user_login": "polymathic",
        "broadcaster_user_name": "POLYMATHIC",
        "followed_at": "2026-09-08T20:29:58.17106713Z",
    }
    event.update(event_overrides)
    return {
        "metadata": {
            "message_id": "synthetic-msg-1",
            "message_type": "notification",
            "message_timestamp": "2026-09-08T20:29:59.464757833Z",
            "subscription_type": "channel.follow",
            "subscription_version": "2",
        },
        "payload": {
            "subscription": {"id": "synthetic-sub-1", "type": "channel.follow", "version": "2"},
            "event": event,
        },
    }


class ParseFollowNotificationTests(unittest.TestCase):
    def test_parses_valid_notification_into_writer_fields(self):
        result = parse_follow_notification(follow_message(), RECEIVED_AT)
        self.assertIsInstance(result, FollowNotification)
        self.assertEqual(result.eventsub_message_id, "synthetic-msg-1")
        self.assertEqual(result.user_id, "synthetic-follower-1")
        self.assertEqual(result.received_at, RECEIVED_AT)
        self.assertIsNone(result.stream_id)
        # Sub-microsecond precision from Twitch is truncated to microseconds.
        self.assertEqual(
            result.followed_at,
            datetime(2026, 9, 8, 20, 29, 58, 171067, tzinfo=timezone.utc),
        )
        self.assertEqual(
            result.notification_at,
            datetime(2026, 9, 8, 20, 29, 59, 464757, tzinfo=timezone.utc),
        )

    def test_record_is_immutable(self):
        result = parse_follow_notification(follow_message(), RECEIVED_AT)
        with self.assertRaises(Exception):
            result.user_id = "synthetic-other"

    def test_non_utc_offset_timestamp_is_accepted_as_aware(self):
        result = parse_follow_notification(
            follow_message(followed_at="2026-09-08T22:29:58+02:00"), RECEIVED_AT
        )
        self.assertEqual(
            result.followed_at,
            datetime(2026, 9, 8, 20, 29, 58, tzinfo=timezone.utc),
        )

    def test_received_at_must_be_timezone_aware_datetime(self):
        for bad in (datetime(2026, 9, 8, 20, 30), "2026-09-08T20:30:00Z", None, 1_725_827_400):
            with self.subTest(bad=bad), self.assertRaises(CaptureError) as caught:
                parse_follow_notification(follow_message(), bad)
            self.assertEqual(caught.exception.reason_code, "received_at_invalid")

    def test_structural_problems_raise_fixed_reason_codes(self):
        cases = {
            "message_not_dict": lambda m: "not a dict",
            "envelope_invalid": lambda m: {**m, "metadata": None},
            "message_type_unexpected": lambda m: _set(m, ["metadata", "message_type"], "session_keepalive"),
            "subscription_mismatch": lambda m: _set(m, ["metadata", "subscription_type"], "channel.raid"),
            "message_id_invalid": lambda m: _set(m, ["metadata", "message_id"], ""),
            "notification_timestamp_invalid": lambda m: _set(m, ["metadata", "message_timestamp"], "not-a-time"),
            "event_invalid": lambda m: _set(m, ["payload", "event"], ["synthetic-follower-1"]),
            "user_id_invalid": lambda m: _set(m, ["payload", "event", "user_id"], ""),
            "followed_at_invalid": lambda m: _set(m, ["payload", "event", "followed_at"], "2026-09-08 20:29"),
        }
        for expected, mutate in cases.items():
            with self.subTest(expected=expected), self.assertRaises(CaptureError) as caught:
                parse_follow_notification(mutate(follow_message()), RECEIVED_AT)
            self.assertEqual(caught.exception.reason_code, expected)

    def test_version_mismatch_is_rejected(self):
        with self.assertRaises(CaptureError) as caught:
            parse_follow_notification(
                _set(follow_message(), ["metadata", "subscription_version"], "1"), RECEIVED_AT
            )
        self.assertEqual(caught.exception.reason_code, "subscription_mismatch")

    def test_naive_followed_at_is_rejected(self):
        with self.assertRaises(CaptureError) as caught:
            parse_follow_notification(
                follow_message(followed_at="2026-09-08T20:29:58"), RECEIVED_AT
            )
        self.assertEqual(caught.exception.reason_code, "followed_at_invalid")

    def test_error_does_not_leak_event_contents(self):
        message = follow_message(user_id="synthetic-secret-identity")
        _set(message, ["payload", "event", "followed_at"], "broken")
        with self.assertRaises(CaptureError) as caught:
            parse_follow_notification(message, RECEIVED_AT)
        self.assertNotIn("synthetic-secret-identity", str(caught.exception))
        self.assertEqual(str(caught.exception), "followed_at_invalid")

    def test_missing_keys_are_rejected_without_keyerror(self):
        message = follow_message()
        del message["payload"]["event"]["followed_at"]
        with self.assertRaises(CaptureError) as caught:
            parse_follow_notification(message, RECEIVED_AT)
        self.assertEqual(caught.exception.reason_code, "followed_at_invalid")


def raid_message(**event_overrides):
    """A structurally valid incoming channel.raid v1 notification, synthetic values."""
    event = {
        "from_broadcaster_user_id": "synthetic-raider-1",
        "from_broadcaster_user_login": "synthetic_raider_1",
        "from_broadcaster_user_name": "Synthetic_Raider_1",
        "to_broadcaster_user_id": "synthetic-broadcaster",
        "to_broadcaster_user_login": "polymathic",
        "to_broadcaster_user_name": "POLYMATHIC",
        "viewers": 4321,
    }
    event.update(event_overrides)
    return {
        "metadata": {
            "message_id": "synthetic-msg-1",
            "message_type": "notification",
            "message_timestamp": "2026-09-08T20:29:59.464757833Z",
            "subscription_type": "channel.raid",
            "subscription_version": "1",
        },
        "payload": {
            "subscription": {"id": "synthetic-sub-1", "type": "channel.raid", "version": "1"},
            "event": event,
        },
    }


class ParseRaidNotificationTests(unittest.TestCase):
    def test_parses_valid_notification_into_writer_fields(self):
        result = parse_raid_notification(raid_message(), RECEIVED_AT)
        self.assertIsInstance(result, RaidNotification)
        self.assertEqual(result.eventsub_message_id, "synthetic-msg-1")
        self.assertEqual(result.from_broadcaster_user_id, "synthetic-raider-1")
        self.assertEqual(result.raid_viewer_count, 4321)
        self.assertEqual(result.received_at, RECEIVED_AT)
        self.assertIsNone(result.stream_id)
        self.assertEqual(
            result.notification_at,
            datetime(2026, 9, 8, 20, 29, 59, 464757, tzinfo=timezone.utc),
        )

    def test_zero_viewers_is_allowed(self):
        result = parse_raid_notification(raid_message(viewers=0), RECEIVED_AT)
        self.assertEqual(result.raid_viewer_count, 0)

    def test_structural_and_value_problems_raise_fixed_reason_codes(self):
        cases = {
            "subscription_mismatch": lambda m: _set(m, ["metadata", "subscription_version"], "2"),
            "message_type_unexpected": lambda m: _set(m, ["metadata", "message_type"], "revocation"),
            "message_id_invalid": lambda m: _set(m, ["metadata", "message_id"], ""),
            "notification_timestamp_invalid": lambda m: _set(m, ["metadata", "message_timestamp"], "nope"),
            "event_invalid": lambda m: _set(m, ["payload", "event"], None),
            "from_broadcaster_user_id_invalid": lambda m: _set(m, ["payload", "event", "from_broadcaster_user_id"], ""),
            "viewer_count_invalid": lambda m: _set(m, ["payload", "event", "viewers"], -1),
        }
        for expected, mutate in cases.items():
            with self.subTest(expected=expected), self.assertRaises(CaptureError) as caught:
                parse_raid_notification(mutate(raid_message()), RECEIVED_AT)
            self.assertEqual(caught.exception.reason_code, expected)

    def test_non_integer_or_boolean_or_oversized_viewers_are_rejected(self):
        for bad in (True, 1.0, "10", None, 2 ** 31):
            with self.subTest(bad=bad), self.assertRaises(CaptureError) as caught:
                parse_raid_notification(raid_message(viewers=bad), RECEIVED_AT)
            self.assertEqual(caught.exception.reason_code, "viewer_count_invalid")

    def test_missing_viewers_key_is_rejected_without_keyerror(self):
        message = raid_message()
        del message["payload"]["event"]["viewers"]
        with self.assertRaises(CaptureError) as caught:
            parse_raid_notification(message, RECEIVED_AT)
        self.assertEqual(caught.exception.reason_code, "viewer_count_invalid")

    def test_error_does_not_leak_event_contents(self):
        message = raid_message(from_broadcaster_user_id="synthetic-secret-raider")
        _set(message, ["payload", "event", "viewers"], -5)
        with self.assertRaises(CaptureError) as caught:
            parse_raid_notification(message, RECEIVED_AT)
        self.assertNotIn("synthetic-secret-raider", str(caught.exception))
        self.assertEqual(str(caught.exception), "viewer_count_invalid")


FRAGMENTS = [
    {"type": "text", "text": "hello ", "cheermote": None, "emote": None, "mention": None},
    {"type": "emote", "text": "synthetic_emote",
     "emote": {"id": "synthetic-emote-1", "emote_set_id": "0"}},
]


def chat_message(**event_overrides):
    """A structurally valid channel.chat.message v1 notification, synthetic values."""
    event = {
        "broadcaster_user_id": "synthetic-broadcaster",
        "broadcaster_user_login": "polymathic",
        "broadcaster_user_name": "POLYMATHIC",
        "chatter_user_id": "synthetic-chatter-1",
        "chatter_user_login": "synthetic_chatter_1",
        "chatter_user_name": "Synthetic_Chatter_1",
        "message_id": "synthetic-chat-msg-1",
        "message": {"text": "hello synthetic_emote", "fragments": FRAGMENTS},
        "message_type": "text",
        "badges": [],
        "color": "#1E90FF",
        "source_broadcaster_user_id": None,
    }
    event.update(event_overrides)
    return {
        "metadata": {
            "message_id": "synthetic-msg-1",
            "message_type": "notification",
            "message_timestamp": "2026-09-08T20:29:59.464757833Z",
            "subscription_type": "channel.chat.message",
            "subscription_version": "1",
        },
        "payload": {
            "subscription": {"id": "synthetic-sub-1", "type": "channel.chat.message", "version": "1"},
            "event": event,
        },
    }


class ParseChatNotificationTests(unittest.TestCase):
    def test_parses_valid_notification_into_writer_fields(self):
        result = parse_chat_notification(chat_message(), RECEIVED_AT)
        self.assertIsInstance(result, ChatNotification)
        self.assertEqual(result.eventsub_message_id, "synthetic-msg-1")
        self.assertEqual(result.chat_message_id, "synthetic-chat-msg-1")
        self.assertEqual(result.chatter_user_id, "synthetic-chatter-1")
        self.assertEqual(result.message_text, "hello synthetic_emote")
        self.assertEqual(result.message_fragments, FRAGMENTS)
        self.assertEqual(result.received_at, RECEIVED_AT)
        self.assertIsNone(result.source_broadcaster_user_id)
        self.assertIsNone(result.stream_id)
        self.assertEqual(
            result.notification_at,
            datetime(2026, 9, 8, 20, 29, 59, 464757, tzinfo=timezone.utc),
        )

    def test_record_is_immutable_and_hides_message_content_in_repr(self):
        result = parse_chat_notification(chat_message(), RECEIVED_AT)
        with self.assertRaises(Exception):
            result.message_text = "synthetic-other"
        self.assertNotIn("hello synthetic_emote", repr(result))
        self.assertNotIn("synthetic_emote", repr(result))

    def test_empty_text_is_allowed_but_non_string_is_rejected(self):
        result = parse_chat_notification(
            _set(chat_message(), ["payload", "event", "message", "text"], ""), RECEIVED_AT,
        )
        self.assertEqual(result.message_text, "")
        with self.assertRaises(CaptureError) as caught:
            parse_chat_notification(
                _set(chat_message(), ["payload", "event", "message", "text"], None), RECEIVED_AT,
            )
        self.assertEqual(caught.exception.reason_code, "message_text_invalid")

    def test_shared_chat_source_broadcaster_is_kept_when_present(self):
        result = parse_chat_notification(
            chat_message(source_broadcaster_user_id="synthetic-source-channel"), RECEIVED_AT,
        )
        self.assertEqual(result.source_broadcaster_user_id, "synthetic-source-channel")

    def test_empty_fragment_list_is_allowed(self):
        result = parse_chat_notification(
            _set(chat_message(), ["payload", "event", "message", "fragments"], []), RECEIVED_AT,
        )
        self.assertEqual(result.message_fragments, [])

    def test_received_at_must_be_timezone_aware_datetime(self):
        for bad in (datetime(2026, 9, 8, 20, 30), "2026-09-08T20:30:00Z", None):
            with self.subTest(bad=bad), self.assertRaises(CaptureError) as caught:
                parse_chat_notification(chat_message(), bad)
            self.assertEqual(caught.exception.reason_code, "received_at_invalid")

    def test_structural_problems_raise_fixed_reason_codes(self):
        cases = {
            "message_not_dict": lambda m: "not a dict",
            "envelope_invalid": lambda m: {**m, "payload": None},
            "message_type_unexpected": lambda m: _set(m, ["metadata", "message_type"], "session_keepalive"),
            "subscription_mismatch": lambda m: _set(m, ["metadata", "subscription_type"], "channel.follow"),
            "message_id_invalid": lambda m: _set(m, ["metadata", "message_id"], ""),
            "notification_timestamp_invalid": lambda m: _set(m, ["metadata", "message_timestamp"], "nope"),
            "event_invalid": lambda m: _set(m, ["payload", "event"], None),
            "chatter_user_id_invalid": lambda m: _set(m, ["payload", "event", "chatter_user_id"], ""),
            "chat_message_id_invalid": lambda m: _set(m, ["payload", "event", "message_id"], ""),
            "message_invalid": lambda m: _set(m, ["payload", "event", "message"], "text"),
            "message_text_invalid": lambda m: _set(m, ["payload", "event", "message", "text"], 5),
            "message_fragments_invalid": lambda m: _set(m, ["payload", "event", "message", "fragments"], "x"),
            "source_broadcaster_user_id_invalid":
                lambda m: _set(m, ["payload", "event", "source_broadcaster_user_id"], ""),
        }
        for expected, mutate in cases.items():
            with self.subTest(expected=expected), self.assertRaises(CaptureError) as caught:
                parse_chat_notification(mutate(chat_message()), RECEIVED_AT)
            self.assertEqual(caught.exception.reason_code, expected)

    def test_version_mismatch_is_rejected(self):
        with self.assertRaises(CaptureError) as caught:
            parse_chat_notification(
                _set(chat_message(), ["metadata", "subscription_version"], "2"), RECEIVED_AT,
            )
        self.assertEqual(caught.exception.reason_code, "subscription_mismatch")

    def test_non_dict_fragment_entry_is_rejected(self):
        with self.assertRaises(CaptureError) as caught:
            parse_chat_notification(
                _set(chat_message(), ["payload", "event", "message", "fragments"],
                     [{"type": "text", "text": "ok"}, "not-a-fragment"]), RECEIVED_AT,
            )
        self.assertEqual(caught.exception.reason_code, "message_fragments_invalid")

    def test_missing_keys_are_rejected_without_keyerror(self):
        message = chat_message()
        del message["payload"]["event"]["message"]["fragments"]
        with self.assertRaises(CaptureError) as caught:
            parse_chat_notification(message, RECEIVED_AT)
        self.assertEqual(caught.exception.reason_code, "message_fragments_invalid")

    def test_error_does_not_leak_message_text_or_identity(self):
        message = chat_message(chatter_user_id="synthetic-secret-chatter")
        _set(message, ["payload", "event", "message", "text"], "synthetic-secret-message")
        _set(message, ["payload", "event", "message", "fragments"], "broken")
        with self.assertRaises(CaptureError) as caught:
            parse_chat_notification(message, RECEIVED_AT)
        self.assertEqual(str(caught.exception), "message_fragments_invalid")
        self.assertNotIn("synthetic-secret", str(caught.exception))


def _set(message, path, value):
    node = message
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return message


if __name__ == "__main__":
    unittest.main()
