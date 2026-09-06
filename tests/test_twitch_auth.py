import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs

from scripts import twitch_auth as auth


class TokenManagerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'tokens.json'
        self.config = {'TWITCH_CLIENT_ID': 'synthetic-app', 'TWITCH_CLIENT_SECRET': 'private-secret'}
        self.saved = {'tokens': {'access_token': 'private-old-access', 'refresh_token': 'private+refresh&'},
                      'identity': {'user_id': 'synthetic-user'}}
        auth.save_tokens(self.path, self.saved)
        self.manager = auth.TokenManager(self.config, self.saved, self.path)
        self.identity = {'client_id': 'synthetic-app', 'user_id': 'synthetic-user', 'scopes': list(auth.SCOPES)}
        self.new_tokens = {'access_token': 'private-new-access', 'refresh_token': 'private-new-refresh'}

    def test_startup_and_hourly_validation_even_after_reload(self):
        with patch.object(auth, 'request_json', return_value=self.identity) as request:
            with patch.object(auth.time, 'monotonic', return_value=0):
                self.manager.validate_if_due()
            with patch.object(auth.time, 'monotonic', return_value=3599):
                self.manager.validate_if_due()
            self.assertEqual(request.call_count, 1)
            with patch.object(auth.time, 'monotonic', return_value=3600):
                self.manager.validate_if_due()
            self.assertEqual(request.call_count, 2)

    def test_expired_token_refreshes_encodes_and_saves_rotated_credentials(self):
        responses = [auth.TwitchError('Unauthorized', 401), self.new_tokens, self.identity]
        with patch.object(auth, 'request_json', side_effect=responses) as request:
            self.manager.validate_if_due()
        form = parse_qs(request.call_args_list[1].args[0].data.decode())
        self.assertEqual(form['refresh_token'], ['private+refresh&'])
        self.assertEqual(json.loads(self.path.read_text())['tokens'], self.new_tokens)
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_helix_401_retries_once_using_new_token(self):
        responses = [self.identity, auth.TwitchError('Unauthorized', 401), self.new_tokens,
                     self.identity, {'data': []}]
        with patch.object(auth, 'request_json', side_effect=responses) as request:
            self.assertEqual(self.manager.helix_get('streams', {}), {'data': []})
        self.assertEqual(request.call_count, 5)
        self.assertEqual(request.call_args.args[0].get_header('Authorization'), 'Bearer private-new-access')

    def test_repeated_401_does_not_loop(self):
        responses = [self.identity, auth.TwitchError('Unauthorized', 401), self.new_tokens,
                     self.identity, auth.TwitchError('Unauthorized', 401)]
        with patch.object(auth, 'request_json', side_effect=responses) as request:
            with self.assertRaises(auth.TwitchError):
                self.manager.helix_get('streams', {})
        self.assertEqual(request.call_count, 5)

    def test_non_authentication_failures_do_not_refresh(self):
        for status in (None, 403, 429, 500):
            with patch.object(auth, 'request_json', side_effect=auth.TwitchError('Unavailable', status)) as request:
                with self.assertRaises(auth.TwitchError):
                    self.manager.validate_if_due()
                self.assertEqual(request.call_count, 1)
        self.assertEqual(json.loads(self.path.read_text()), self.saved)

    def test_wrong_app_user_or_scopes_block_requests(self):
        for change in ({'client_id': 'other'}, {'user_id': 'other'}, {'scopes': []}, {'scopes': [{}]}):
            manager = auth.TokenManager(self.config, self.saved, self.path)
            with patch.object(auth, 'request_json', return_value=self.identity | change) as request:
                with self.assertRaises(auth.TwitchError):
                    manager.helix_get('streams', {})
                with self.assertRaises(auth.TwitchError):
                    manager.helix_get('streams', {})
                self.assertEqual(request.call_count, 1)

    def test_refresh_rejection_preserves_file_and_blocks(self):
        with patch.object(auth, 'request_json', side_effect=[auth.TwitchError('Unauthorized', 401),
                                                           auth.TwitchError('Rejected', 400)]) as request:
            with self.assertRaisesRegex(auth.TwitchError, 'refresh rejected'):
                self.manager.validate_if_due()
            with self.assertRaisesRegex(auth.TwitchError, 'blocked'):
                self.manager.validate_if_due()
            self.assertEqual(request.call_count, 2)
        self.assertEqual(json.loads(self.path.read_text()), self.saved)

    def test_rotated_tokens_survive_validation_outage_and_reload_validates(self):
        responses = [auth.TwitchError('Unauthorized', 401), self.new_tokens, auth.TwitchError('Offline')]
        with patch.object(auth, 'request_json', side_effect=responses):
            with self.assertRaises(auth.TwitchError):
                self.manager.validate_if_due()
        saved = json.loads(self.path.read_text())
        self.assertEqual(saved['tokens'], self.new_tokens)
        manager = auth.TokenManager(self.config, saved, self.path)
        with patch.object(auth, 'request_json', return_value=self.identity) as request:
            manager.validate_if_due()
            self.assertEqual(request.call_count, 1)

    def test_save_failure_preserves_file_and_blocks_further_calls(self):
        responses = [auth.TwitchError('Unauthorized', 401), self.new_tokens]
        with patch.object(auth, 'request_json', side_effect=responses) as request:
            with patch('scripts.authorize_twitch.os.replace', side_effect=OSError('private-path')):
                with self.assertRaisesRegex(auth.TwitchError, 'Could not save'):
                    self.manager.validate_if_due()
            with self.assertRaisesRegex(auth.TwitchError, 'blocked'):
                self.manager.validate_if_due()
            self.assertEqual(request.call_count, 2)
        self.assertEqual(json.loads(self.path.read_text()), self.saved)

    def test_transport_errors_suppress_sensitive_details(self):
        errors = [HTTPError('https://example.test/private', 401, 'private-error', {}, io.BytesIO(b'private-body')),
                  URLError('private-detail'), ValueError('private-detail')]
        for error in errors:
            with patch.object(auth, 'urlopen', side_effect=error):
                with self.assertRaises(auth.TwitchError) as caught:
                    auth.request_json(auth.Request('https://example.test'))
            self.assertNotIn('private', str(caught.exception))

    def test_malformed_refresh_does_not_overwrite_file(self):
        with patch.object(auth, 'request_json', side_effect=[auth.TwitchError('Unauthorized', 401), {}]):
            with self.assertRaisesRegex(auth.TwitchError, 'incomplete'):
                self.manager.validate_if_due()
        self.assertEqual(json.loads(self.path.read_text()), self.saved)
