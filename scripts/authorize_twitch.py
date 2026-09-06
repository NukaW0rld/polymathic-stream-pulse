"""One-time local Twitch authorization. Run with Python 3.10+ in WSL2."""

import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
REDIRECT_URI = "http://localhost:3000/callback"
SCOPES = {"user:read:chat", "moderator:read:followers"}
TOKEN_FILE = ROOT / ".env.tokens.json"


def read_config(path):
    """Read simple KEY=value entries, optionally wrapped in matching quotes."""
    config = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError("Invalid .env line; use KEY=value entries.")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        config[key.strip()] = value
    for key in ("TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET", "TWITCH_REDIRECT_URI"):
        if not config.get(key):
            raise ValueError(f"Missing {key} in .env.")
    if config["TWITCH_REDIRECT_URI"] != REDIRECT_URI:
        raise ValueError(f"This helper requires redirect URI {REDIRECT_URI}.")
    return config


def callback_result(path, expected_state):
    """Return (HTTP status, browser message, code, finished)."""
    parsed = urlsplit(path)
    if parsed.path != "/callback":
        return 404, "Not found.", None, False
    query = parse_qs(parsed.query)
    states = query.get("state", [])
    if len(states) != 1 or not secrets.compare_digest(
        states[0].encode(), expected_state.encode()
    ):
        return 400, "Invalid authorization state. Use the link in your terminal.", None, False
    if "error" in query:
        return 400, "Authorization declined. You can close this tab.", None, True
    codes = query.get("code", [])
    if len(codes) != 1:
        return 400, "Missing or invalid authorization code.", None, False
    return 200, "Authorization received. Check your WSL terminal for the result.", codes[0], True


def receive_code(client_id):
    state = secrets.token_urlsafe(32)
    result = {"finished": False, "code": None}

    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            # Default HTTP logs would expose the authorization code in the URL.
            pass

        def do_GET(self):
            status, message, code, finished = callback_result(self.path, state)
            body = message.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(body)
            if finished:
                result.update(finished=True, code=code)

    # Loopback only: this temporary server is not exposed to the LAN.
    with HTTPServer(("127.0.0.1", 3000), CallbackHandler) as server:
        server.timeout = 1
        url = "https://id.twitch.tv/oauth2/authorize?" + urlencode({
            "client_id": client_id,
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "scope": " ".join(sorted(SCOPES)),
            "state": state,
            "force_verify": "true",
        })
        print("Open this link in your Windows browser, using your moderator account:\n")
        print(url, flush=True)
        print("\nWaiting up to five minutes. Press Ctrl+C to cancel.", flush=True)
        deadline = time.monotonic() + 300
        while not result["finished"] and time.monotonic() < deadline:
            server.handle_request()
    if not result["code"]:
        raise ValueError("Authorization declined or timed out. Run the helper again to retry.")
    return result["code"]


def request_json(request):
    try:
        with urlopen(request, timeout=20) as response:
            return json.load(response)
    except HTTPError as error:
        # Never print response bodies, request headers, or credential payloads.
        raise ValueError(f"Twitch returned HTTP {error.code}. Check app settings and retry.") from None
    except (URLError, TimeoutError):
        raise ValueError("Could not reach Twitch. Check your connection and retry.") from None


def exchange_code(config, code):
    tokens = request_json(Request(
        "https://id.twitch.tv/oauth2/token",
        data=urlencode({
            "client_id": config["TWITCH_CLIENT_ID"],
            "client_secret": config["TWITCH_CLIENT_SECRET"],
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
        }).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    ))
    if not tokens.get("access_token") or not tokens.get("refresh_token"):
        raise ValueError("Twitch did not return both required tokens.")
    identity = request_json(Request(
        "https://id.twitch.tv/oauth2/validate",
        headers={"Authorization": "OAuth " + tokens["access_token"]},
    ))
    if identity.get("client_id") != config["TWITCH_CLIENT_ID"]:
        raise ValueError("Token belongs to a different application.")
    if not identity.get("user_id") or not SCOPES.issubset(identity.get("scopes", [])):
        raise ValueError("Token is missing user identity or required permissions.")
    return {"tokens": tokens, "identity": identity, "validated_at": int(time.time())}


def save_tokens(path, data):
    """Replace atomically with an owner-only file, preserving any old file on failure."""
    fd, temporary = tempfile.mkstemp(prefix=".env.oauth-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(data, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    config = read_config(ROOT / ".env")
    if TOKEN_FILE.exists():
        if input("A token file already exists. Replace it? [y/N] ").strip().lower() != "y":
            print("Existing token file kept.")
            return
    code = receive_code(config["TWITCH_CLIENT_ID"])
    data = exchange_code(config, code)
    save_tokens(TOKEN_FILE, data)
    print("Authorization succeeded; tokens validated and saved privately to .env.tokens.json.")
    print("Permissions: " + ", ".join(sorted(SCOPES)))
    print("Moderator access to POLYMATHIC still needs a separate API check.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAuthorization cancelled.")
        sys.exit(1)
    except (ValueError, OSError):
        # Avoid tracebacks and unexpected exception details containing private data.
        print("Authorization failed. Check .env settings, network access, and whether port 3000 is free.")
        print("No new token file was saved. You can run the helper again.")
        sys.exit(1)
