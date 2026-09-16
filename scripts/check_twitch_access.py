"""Access check; may update the private token file when refresh is needed. Run: python3 -m scripts.check_twitch_access"""

import sys

from scripts.twitch_auth import TokenManager, TwitchError


class CheckFailed(Exception):
    """A safe diagnostic message containing no response bodies or credentials."""


def check_access(auth):
    auth.validate_if_due()
    print("PASS: saved user token is valid for this application and required permissions.")

    def helix(endpoint, params, stage):
        try:
            return auth.helix_get(endpoint, params)
        except TwitchError as error:
            raise CheckFailed(f"{stage}: {error}") from None

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
    if not auth.has_scope("moderator:read:chatters"):
        raise CheckFailed(
            "Chatter access: moderator:read:chatters is missing; reauthorize with the collector stopped."
        )
    chatters = helix("chat/chatters", {
        "broadcaster_id": broadcaster_id, "moderator_id": auth.user_id, "first": 1,
    }, "Chatter access")
    if (not isinstance(chatters.get("data"), list)
            or type(chatters.get("total")) is not int or chatters["total"] < 0
            or not isinstance(chatters.get("pagination"), dict)):
        raise CheckFailed("Chatter access: unexpected response structure.")
    print("PASS: channel-specific chatter access confirmed; records discarded without printing or saving.")
    print("EventSub delivery is not tested. Token refresh occurs only if authentication requires it.")


def main():
    check_access(TokenManager.load())


if __name__ == "__main__":
    try:
        main()
    except (CheckFailed, TwitchError) as error:
        print("FAIL: " + str(error))
        sys.exit(1)
    except (KeyError, TypeError, ValueError, AttributeError):
        print("FAIL: unexpected Twitch response structure; private details suppressed.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Check cancelled.")
        sys.exit(1)
