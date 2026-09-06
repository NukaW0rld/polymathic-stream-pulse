import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from urllib.error import HTTPError

from scripts import check_twitch_access as check


class AccessCheckTests(unittest.TestCase):
    def responses(self, followers, live=False):
        return [
            {"client_id": "synthetic-app", "user_id": "private-user", "scopes": list(check.SCOPES)},
            {"data": [{"id": "private-channel"}]},
            {"data": [{"id": "private-stream"}] if live else []},
            followers,
        ]

    def test_live_and_offline_success_do_not_disclose_records(self):
        for live in (True, False):
            output = io.StringIO()
            followers = {"data": [{"user_id": "private-follower", "followed_at": "synthetic-date"}]}
            with patch.object(check, "get_json", side_effect=self.responses(followers, live)) as request:
                with redirect_stdout(output):
                    check.check_access("synthetic-app", "private-token")
            self.assertIn("LIVE." if live else "OFFLINE.", output.getvalue())
            self.assertIn("follower access confirmed", output.getvalue())
            self.assertNotIn("private-", output.getvalue())
            self.assertIn("first=1", request.call_args.args[0])

    def test_public_total_does_not_prove_moderator_access(self):
        with patch.object(check, "get_json", side_effect=self.responses({"data": [], "total": 15000})):
            with redirect_stdout(io.StringIO()), self.assertRaisesRegex(check.CheckFailed, "NOT confirmed"):
                check.check_access("synthetic-app", "private-token")

    def test_wrong_application_stops_before_channel_requests(self):
        with patch.object(check, "get_json", return_value={"client_id": "other", "user_id": "private-user"}) as request:
            with self.assertRaises(check.CheckFailed):
                check.check_access("synthetic-app", "private-token")
            self.assertEqual(request.call_count, 1)

    def test_http_error_suppresses_sensitive_response(self):
        error = HTTPError("https://example.test", 401, "private-error", {}, io.BytesIO(b"private-body"))
        with patch.object(check, "urlopen", side_effect=error):
            with self.assertRaises(check.CheckFailed) as caught:
                check.get_json("https://example.test", {}, "Token validation")
        self.assertIn("401", str(caught.exception))
        self.assertNotIn("private", str(caught.exception))
