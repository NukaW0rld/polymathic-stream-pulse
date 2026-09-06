import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock

from scripts import check_twitch_access as check
from scripts.twitch_auth import TwitchError


class AccessCheckTests(unittest.TestCase):
    def manager(self, followers, live=False):
        auth = Mock()
        auth.helix_get.side_effect = [
            {"data": [{"id": "private-channel"}]},
            {"data": [{"id": "private-stream"}] if live else []},
            followers,
        ]
        return auth

    def test_live_and_offline_success_do_not_disclose_records(self):
        for live in (True, False):
            output = io.StringIO()
            auth = self.manager({"data": [{"user_id": "private-follower", "followed_at": "synthetic-date"}]}, live)
            with redirect_stdout(output):
                check.check_access(auth)
            self.assertIn("LIVE." if live else "OFFLINE.", output.getvalue())
            self.assertIn("follower access confirmed", output.getvalue())
            self.assertNotIn("private-", output.getvalue())
            self.assertEqual(auth.helix_get.call_args.args[1]["first"], 1)
            auth.validate_if_due.assert_called_once()

    def test_public_total_does_not_prove_moderator_access(self):
        auth = self.manager({"data": [], "total": 15000})
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(check.CheckFailed, "NOT confirmed"):
            check.check_access(auth)

    def test_validation_failure_stops_before_channel_requests(self):
        auth = Mock()
        auth.validate_if_due.side_effect = TwitchError("Authorization invalid.")
        with self.assertRaises(TwitchError):
            check.check_access(auth)
        auth.helix_get.assert_not_called()
