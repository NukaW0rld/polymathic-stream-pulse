"""Read-only access check. Run: python3 -m scripts.check_twitch_access"""

import json
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.authorize_twitch import ROOT, SCOPES, TOKEN_FILE, read_config


class CheckFailed(Exception):
    """A safe diagnostic message containing no response bodies or credentials."""


def get_json(url, headers, stage):
    try:
        with urlopen(Request(url, headers=headers), timeout=20) as response:
            return json.load(response)
    except HTTPError as error:
        hints = {
            401: "Token may be expired, invalid, or unauthorized for this request.",
            403: "The account lacks permission for this request.",
            429: "Rate limited; wait before retrying.",
        }
        hint = hints.get(error.code, "Twitch rejected the request; retry later.")
        raise CheckFailed(f"{stage}: HTTP {error.code}. {hint}") from None
    except (URLError, TimeoutError, OSError):
        raise CheckFailed(f"{stage}: network request failed; check connectivity.") from None
    except ValueError:
        raise CheckFailed(f"{stage}: unexpected response format.") from None


def check_access(client_id, access_token):
    identity = get_json(
        "https://id.twitch.tv/oauth2/validate",
        {"Authorization": "OAuth " + access_token}, "Token validation",
    )
    if identity.get("client_id") != client_id or not identity.get("user_id"):
        raise CheckFailed("Token validation: expected a user token for this application.")
    if not SCOPES.issubset(identity.get("scopes", [])):
        raise CheckFailed("Token validation: required permissions are missing; authorize again.")
    print("PASS: saved user token is valid for this application and required permissions.")

    headers = {"Authorization": "Bearer " + access_token, "Client-Id": client_id}

    def helix(endpoint, params, stage):
        return get_json("https://api.twitch.tv/helix/" + endpoint + "?" + urlencode(params), headers, stage)

    users = helix("users", {"login": "polymathic"}, "Channel lookup")["data"]
    if len(users) != 1 or not users[0].get("id"):
        raise CheckFailed("Channel lookup: expected exactly one matching channel.")
    broadcaster_id = users[0]["id"]
    print("PASS: target channel resolved.")

    streams = helix("streams", {"user_id": broadcaster_id}, "Stream status")["data"]
    if not isinstance(streams, list) or len(streams) > 1:
        raise CheckFailed("Stream status: unexpected response structure.")
    print("PASS: stream status request succeeded; channel is " + ("LIVE." if streams else "OFFLINE."))

    # A 200 response alone is insufficient: Twitch can return only a public total
    # when the account lacks access to individual follower information.
    followers = helix("channels/followers", {"broadcaster_id": broadcaster_id, "first": 1}, "Follower access")
    records = followers["data"]
    if not isinstance(records, list) or len(records) != 1 or not records[0].get("user_id") or not records[0].get("followed_at"):
        raise CheckFailed(
            "Follower access: no individual follower record returned; access is NOT confirmed. "
            "Check the authorized account's channel role. An empty follower list is also inconclusive."
        )
    print("PASS: channel-specific follower access confirmed; record discarded without printing or saving.")
    print("EventSub delivery and token refresh are not tested by this check.")


def main():
    try:
        config = read_config(ROOT / ".env")
        saved = json.loads(TOKEN_FILE.read_text())
        token = saved["tokens"]["access_token"]
        if not isinstance(token, str) or not token:
            raise ValueError
    except (OSError, ValueError, KeyError, TypeError):
        raise CheckFailed("Could not load credentials. Check .env and complete authorization first.") from None
    check_access(config["TWITCH_CLIENT_ID"], token)


if __name__ == "__main__":
    try:
        main()
    except CheckFailed as error:
        print("FAIL: " + str(error))
        sys.exit(1)
    except (KeyError, TypeError, ValueError, AttributeError):
        print("FAIL: unexpected Twitch response structure; private details suppressed.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Check cancelled.")
        sys.exit(1)
