"""Private token management for one process; no background thread or event loop."""

import json
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.authorize_twitch import ROOT, SCOPES, TOKEN_FILE, read_config, save_tokens


class TwitchError(Exception):
    """Safe diagnostics only; never include request or response contents."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


def request_json(request):
    try:
        with urlopen(request, timeout=20) as response:
            result = json.load(response)
        if not isinstance(result, dict):
            raise ValueError
        return result
    except HTTPError as error:
        raise TwitchError(f"Twitch request failed: HTTP {error.code}.", error.code) from None
    except (URLError, TimeoutError, OSError):
        raise TwitchError("Could not reach Twitch; check connectivity.") from None
    except ValueError:
        raise TwitchError("Twitch returned an unexpected response format.") from None


class TokenManager:
    """Share one instance across callers. Do not run concurrent token-file writers."""

    def __init__(self, config, saved, token_path=TOKEN_FILE):
        try:
            self.client_id = config["TWITCH_CLIENT_ID"]
            self._client_secret = config["TWITCH_CLIENT_SECRET"]
            self._tokens = saved["tokens"].copy()
            self._user_id = saved["identity"]["user_id"]
            if not all(isinstance(value, str) and value for value in (
                self.client_id, self._client_secret, self._user_id,
                self._tokens["access_token"], self._tokens["refresh_token"],
            )):
                raise ValueError
        except (KeyError, TypeError, ValueError, AttributeError):
            raise TwitchError("Invalid saved authorization; run the authorization helper.") from None
        self._token_path = token_path
        self._validated_at = None
        self._lock = threading.RLock()
        self._blocked = False

    @classmethod
    def load(cls):
        try:
            return cls(read_config(ROOT / ".env"), json.loads(TOKEN_FILE.read_text()))
        except (OSError, ValueError):
            raise TwitchError("Could not load credentials; check .env and saved authorization.") from None

    def _validate(self):
        identity = request_json(Request(
            "https://id.twitch.tv/oauth2/validate",
            headers={"Authorization": "OAuth " + self._tokens["access_token"]},
        ))
        scopes = identity.get("scopes")
        if (identity.get("client_id") != self.client_id
                or identity.get("user_id") != self._user_id
                or not isinstance(scopes, list)
                or not all(isinstance(scope, str) for scope in scopes)
                or not SCOPES.issubset(scopes)):
            self._blocked = True
            raise TwitchError("Authorization identity or permissions changed; authorize again.")
        self._validated_at = time.monotonic()
        return identity

    def _refresh(self):
        self._validated_at = None
        try:
            tokens = request_json(Request(
                "https://id.twitch.tv/oauth2/token",
                data=urlencode({
                    "client_id": self.client_id,
                    "client_secret": self._client_secret,
                    "grant_type": "refresh_token",
                    "refresh_token": self._tokens["refresh_token"],
                }).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ))
        except TwitchError as error:
            if error.status in (400, 401, 403):
                self._blocked = True
                raise TwitchError("Token refresh rejected; check app settings and authorize again.") from None
            raise
        if not all(isinstance(tokens.get(key), str) and tokens[key]
                   for key in ("access_token", "refresh_token")):
            self._blocked = True
            raise TwitchError("Token refresh returned incomplete credentials; authorize again.")
        # Preserve a rotated refresh token even if subsequent validation is unavailable.
        # A fresh manager always validates on startup; this is not readiness evidence.
        try:
            save_tokens(self._token_path, {
                "tokens": tokens,
                "identity": {"user_id": self._user_id},
                "validated_at": None,
            })
        except (OSError, ValueError, TypeError):
            self._blocked = True
            raise TwitchError(
                "Could not save refreshed credentials; collection must stop. "
                "Fix local file access; reauthorization may be needed."
            ) from None
        self._tokens = tokens
        self._validate()

    def validate_if_due(self):
        """Call at startup and regularly, including during an idle WebSocket session."""
        with self._lock:
            if self._blocked:
                raise TwitchError("Authorization is blocked; resolve the previous failure and restart.")
            if self._validated_at is None or time.monotonic() - self._validated_at >= 3600:
                try:
                    self._validate()
                except TwitchError as error:
                    if error.status != 401:
                        raise
                    self._refresh()

    def helix_get(self, endpoint, params):
        """GET a relative Helix endpoint; refresh on 401 and retry once only."""
        if not endpoint or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789/_" for char in endpoint):
            raise TwitchError("Invalid Helix endpoint.")
        with self._lock:
            self.validate_if_due()

            def send():
                return request_json(Request(
                    "https://api.twitch.tv/helix/" + endpoint + "?" + urlencode(params),
                    headers={"Authorization": "Bearer " + self._tokens["access_token"],
                             "Client-Id": self.client_id},
                ))

            try:
                return send()
            except TwitchError as error:
                if error.status != 401:
                    raise
                self._refresh()
                return send()
