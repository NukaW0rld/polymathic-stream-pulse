"""Unit tests for the extracted suspend-aware clock guard. No clock, no I/O."""

from datetime import datetime, timedelta, timezone
import unittest

from scripts.clock_guard import ClockError, ClockGuard, ClockReading, read_clock


class FakeClock:
    def __init__(self):
        self.origin = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
        self.wall = 0.0
        self.tick = 0.0

    def __call__(self):
        return ClockReading(self.origin + timedelta(seconds=self.wall), self.tick)

    def advance(self, seconds, *, wall=None):
        self.tick += seconds
        self.wall += seconds if wall is None else wall


class ClockGuardTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.gaps = []
        self.events = []
        self.guard = ClockGuard(
            on_gap=self.gaps.append, emit=self.events.append,
            clock=self.clock, pause=self.clock.advance,
        )

    def test_first_and_steady_samples_report_no_gap(self):
        first = self.guard.sample()
        self.assertEqual(first, self.clock())
        self.clock.advance(0.25)
        self.guard.sample()
        self.clock.advance(60)
        self.guard.sample()
        self.assertEqual((self.gaps, self.events), ([], []))

    def test_backward_elapsed_clock_is_fatal_after_invoking_on_gap(self):
        self.guard.sample()
        self.clock.advance(-1)
        with self.assertRaisesRegex(ClockError, "elapsed_clock_rollback_restart_required"):
            self.guard.sample()
        self.assertEqual(len(self.gaps), 1)

    def test_ninety_second_advance_on_either_clock_is_a_gap(self):
        for delta in ({"seconds": 90}, {"seconds": 1, "wall": 90}):
            with self.subTest(delta=delta):
                clock = FakeClock()
                gaps, events = [], []
                guard = ClockGuard(on_gap=gaps.append, emit=events.append,
                                   clock=clock, pause=clock.advance)
                guard.sample()
                clock.advance(**delta)
                now = guard.sample()
                self.assertEqual(gaps, [now])
                self.assertEqual(events, ["clock_gap_fresh_poll_required"])

    def test_five_second_divergence_without_a_long_gap_is_a_gap(self):
        self.guard.sample()
        self.clock.advance(1, wall=6)  # elapsed 1, wall 6 -> 5s divergence
        now = self.guard.sample()
        self.assertEqual(self.gaps, [now])
        self.assertEqual(self.events, ["clock_gap_fresh_poll_required"])

    def test_small_utc_rollback_waits_then_reports_recovery_without_clamping(self):
        before = self.guard.sample()
        self.clock.advance(0.01, wall=-1.75)
        recovered = self.guard.sample()
        self.assertEqual(recovered, self.clock())              # actual reading, never clamped
        self.assertGreaterEqual(recovered.utc, before.utc)     # real UTC caught up
        self.assertEqual(self.events,
                         ["utc_clock_rollback_waiting", "utc_clock_recovered_fresh_poll_required"])
        # on_gap runs twice: first with the reading that detected the rollback,
        # then with the post-wait reading the caller polls from.
        self.assertEqual(len(self.gaps), 2)
        self.assertLess(self.gaps[0].tick, self.gaps[1].tick)
        self.assertEqual(self.gaps[1], recovered)

    def test_utc_rollback_of_five_seconds_is_fatal_without_waiting(self):
        self.guard.sample()
        paused = []
        self.guard.pause = paused.append
        self.clock.advance(0.01, wall=-5)
        with self.assertRaisesRegex(ClockError, "utc_clock_rollback_restart_required"):
            self.guard.sample()
        self.assertEqual(paused, [])
        self.assertEqual(len(self.gaps), 1)   # on_gap ran before the fatal raise
        self.assertEqual(self.events, [])     # raised before emitting the wait notice

    def test_utc_recovery_is_bounded_to_twenty_waits_when_utc_never_catches_up(self):
        self.guard.sample()
        self.clock.advance(0.01, wall=-1.75)
        calls = []

        def pause(seconds):
            calls.append(seconds)
            self.clock.advance(seconds, wall=0)

        self.guard.pause = pause
        with self.assertRaisesRegex(ClockError, "utc_clock_recovery_timeout_restart_required"):
            self.guard.sample()
        self.assertEqual(len(calls), 20)

    def test_elapsed_rollback_during_recovery_is_fatal(self):
        self.guard.sample()
        self.clock.advance(0.01, wall=-1)
        self.guard.pause = lambda _: self.clock.advance(-0.01, wall=0.25)
        with self.assertRaisesRegex(ClockError, "elapsed_clock_rollback_restart_required"):
            self.guard.sample()

    def test_delayed_wakeup_past_budget_is_fatal_even_if_utc_recovered(self):
        self.guard.sample()
        self.clock.advance(0.01, wall=-1.75)
        self.guard.pause = lambda _: self.clock.advance(10)
        with self.assertRaisesRegex(ClockError, "utc_clock_recovery_timeout_restart_required"):
            self.guard.sample()

    def test_previous_reading_advances_so_a_later_gap_is_measured_from_the_last_sample(self):
        self.guard.sample()
        self.clock.advance(50)
        self.guard.sample()          # 50s: under the 90s threshold, no gap
        self.clock.advance(50)
        now = self.guard.sample()    # another 50s from the last sample, still no gap
        self.assertEqual((self.gaps, self.events), ([], []))
        self.assertEqual(now, self.clock())


class ReadClockTests(unittest.TestCase):
    def test_read_clock_returns_aware_utc_and_a_non_decreasing_tick(self):
        first = read_clock()
        second = read_clock()
        self.assertIsNotNone(first.utc.utcoffset())
        self.assertGreaterEqual(second.tick, first.tick)
        self.assertGreaterEqual(second.utc, first.utc)


if __name__ == "__main__":
    unittest.main()
