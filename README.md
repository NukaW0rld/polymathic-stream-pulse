# POLYMATHIC Stream Pulse

A Twitch analytics project that captures live stream data and analyzes how audience and community activity evolve throughout long-form DJ streams.

The project is being built for POLYMATHIC, a Drum & Bass DJ and Twitch Ambassador, with the goal of producing insights beyond Twitch's standard Creator Dashboard while serving as a practical data-engineering and analytics portfolio project.

## Goals

The system will collect live Twitch data during streams and use it to analyze areas such as:

* audience and chat activity over the course of a stream;
* differences between stream days and stream durations;
* incoming raid impact;
* returning and recurring chat participants;
* follower and engagement patterns where the available data supports them.

## Planned stack

* Python
* PostgreSQL
* SQL
* Pandas
* Power BI

Development and stream collection are planned in WSL2 on a Windows PC, with Power BI Desktop running on Windows on the same machine. The collector will be started locally for streams; collection requires the PC to stay awake and the collector to remain running. Collection gaps will be recorded explicitly.

## Project status

Early development.

The initial priority is building a reliable data-collection pipeline and collecting trustworthy live data before developing the final analytical model and dashboard.

## Data and privacy

Production data may contain Twitch usernames, user IDs, and raw chat messages and will remain private.

This public repository will contain only anonymized, aggregated, sanitized, or synthetic data.

Credentials, OAuth tokens, raw production data, and identifiable user data will never be committed.

## Scope

The first version is intentionally focused on descriptive and diagnostic analytics rather than machine learning, prediction, or a public web application.

More detailed architecture, methodology, findings, and dashboard documentation will be added as the project develops.

## Local Twitch authorization

Register a confidential application in the [Twitch Developer Console](https://dev.twitch.tv/console/apps) with `http://localhost:3000/callback` as its OAuth redirect URL. In an ignored local `.env` file at the repository root, set `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, and `TWITCH_REDIRECT_URI` using one `KEY=value` entry per line.

From WSL2, run with Python 3.10 or newer (no extra packages required):

```bash
python3 scripts/authorize_twitch.py
```

Keep the terminal running and open the printed link in your Windows browser. Sign in with your own moderator account and authorize chat reading and follower reading. The temporary callback listener uses loopback port 3000 and waits up to five minutes. If the browser cannot reach the callback, check Windows-to-WSL localhost connectivity and port conflicts; do not share the callback URL, which contains a temporary authorization code.

The helper follows Twitch's [authorization-code flow](https://dev.twitch.tv/docs/authentication/getting-tokens-oauth/#authorization-code-grant-flow), validates the returned token, and saves tokens and private identity information in the ignored, owner-only `.env.tokens.json` file. It does not print tokens or account identity. Re-running asks before replacing an existing token file. Credentials and token files must remain private.

This is an initial authorization helper, not a running collector. Channel-specific moderator access still needs verification. Automatic token refresh and ongoing validation must be added before long-running collection.

Run the synthetic authorization tests with:

```bash
python3 -m unittest discover -s tests -v
```

After authorization, check channel access from the repository root:

```bash
python3 -m scripts.check_twitch_access
```

This read-only check validates the saved token, resolves the target channel, checks live/offline status, and requests at most one follower record to verify channel-specific access. It prints only diagnostic results and live/offline status; it does not save API responses or print identities. A successful HTTP response containing only a public follower total does not establish moderator access ([Twitch reference](https://dev.twitch.tv/docs/api/reference/#get-channel-followers)). An expired token must currently be renewed through the authorization helper; automatic refresh and EventSub verification are still pending.
