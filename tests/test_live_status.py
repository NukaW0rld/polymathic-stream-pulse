from datetime import datetime, timedelta, timezone
import unittest

from scripts.live_status import LiveStatus


class LiveStatusTests(unittest.TestCase):
    def setUp(self):
        self.status = LiveStatus()
        self.start = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)

    def at(self, seconds):
        return self.start + timedelta(seconds=seconds)

    def chat(self, notification, received, tick):
        return self.status.chat_stream_id(self.at(notification), self.at(received), tick)

    def test_startup_and_offline_exclude_chat(self):
        self.assertIsNone(self.chat(0, 0, 0))
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.status.poll_offline()
        self.assertIsNone(self.chat(1, 1, 1))

    def test_regular_polls_preserve_interval_for_delayed_notifications(self):
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.status.poll_live('synthetic-a', self.at(60), 60)
        self.assertEqual(self.chat(30, 61, 61), 'synthetic-a')
        self.assertIsNone(self.chat(-1, 61, 61))

    def test_failure_pauses_immediately_and_recovery_starts_new_interval(self):
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.status.poll_failed()
        self.assertIsNone(self.chat(10, 10, 10))
        self.status.poll_live('synthetic-a', self.at(60), 60)
        self.assertIsNone(self.chat(30, 61, 61))
        self.assertEqual(self.chat(60, 61, 61), 'synthetic-a')

    def test_exact_freshness_boundary_excludes_chat(self):
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.assertEqual(self.chat(89, 89, 89), 'synthetic-a')
        self.assertIsNone(self.chat(90, 90, 90))

    def test_stalled_poll_resets_interval_even_without_intervening_chat(self):
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.status.poll_live('synthetic-a', self.at(120), 120)
        self.assertIsNone(self.chat(100, 121, 121))
        self.assertEqual(self.chat(120, 121, 121), 'synthetic-a')

    def test_changed_stream_excludes_old_notifications(self):
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.status.poll_live('synthetic-b', self.at(60), 60)
        self.assertIsNone(self.chat(50, 61, 61))
        self.assertEqual(self.chat(60, 61, 61), 'synthetic-b')

    def test_future_notifications_and_naive_datetimes_are_not_accepted(self):
        self.status.poll_live('synthetic-a', self.at(0), 0)
        self.assertIsNone(self.chat(20, 10, 10))
        with self.assertRaises(ValueError):
            self.status.chat_stream_id(datetime(2026, 9, 6), self.at(10), 10)

    def test_elapsed_clock_rollback_invalidates_eligibility(self):
        self.status.poll_live('synthetic-a', self.at(0), 100)
        self.assertIsNone(self.chat(1, 1, 99))
