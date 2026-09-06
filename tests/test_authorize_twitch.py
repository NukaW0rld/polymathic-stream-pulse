import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import authorize_twitch as auth


class AuthorizationTests(unittest.TestCase):
    def test_callback_rejects_missing_wrong_and_duplicate_state(self):
        for query in ("code=fake", "code=fake&state=wrong", "code=fake&state=ok&state=ok"):
            status, _, code, finished = auth.callback_result("/callback?" + query, "ok")
            self.assertEqual(status, 400)
            self.assertIsNone(code)
            self.assertFalse(finished)

    def test_callback_accepts_code_only_with_matching_state(self):
        status, _, code, finished = auth.callback_result("/callback?code=fake&state=ok", "ok")
        self.assertEqual((status, code, finished), (200, "fake", True))

    def test_declining_authorization_finishes_without_code(self):
        result = auth.callback_result("/callback?error=access_denied&state=ok", "ok")
        self.assertEqual((result[0], result[2], result[3]), (400, None, True))

    def test_unrelated_browser_request_does_not_finish_flow(self):
        self.assertEqual(auth.callback_result("/favicon.ico", "ok")[0], 404)
        self.assertFalse(auth.callback_result("/favicon.ico", "ok")[3])

    def test_validation_rejects_wrong_app_or_missing_scopes(self):
        config = {"TWITCH_CLIENT_ID": "app", "TWITCH_CLIENT_SECRET": "fake"}
        tokens = {"access_token": "fake", "refresh_token": "fake"}
        for identity in (
            {"client_id": "other", "user_id": "synthetic", "scopes": list(auth.SCOPES)},
            {"client_id": "app", "user_id": "synthetic", "scopes": []},
        ):
            with patch.object(auth, "request_json", side_effect=[tokens, identity]):
                with self.assertRaises(ValueError):
                    auth.exchange_code(config, "fake")

    def test_private_file_and_atomic_failure_preserve_existing_tokens(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tokens.json"
            auth.save_tokens(path, {"synthetic": "original"})
            original = path.read_text()
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            with patch.object(auth.os, "replace", side_effect=OSError):
                with self.assertRaises(OSError):
                    auth.save_tokens(path, {"synthetic": "replacement"})
            self.assertEqual(path.read_text(), original)
            self.assertEqual(list(Path(directory).iterdir()), [path])


if __name__ == "__main__":
    unittest.main()
