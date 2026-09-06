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

Implemented components include OAuth/token refresh, a safe API access probe,
observed-live chat eligibility, seven schema files, and a database writer for one
successful live poll. These components are not yet connected into a running
collector. EventSub delivery and collection-health recording remain unimplemented.

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

This is an initial authorization helper, not a running collector. Use the access probe below to verify channel-specific moderator access. Shared token management now supports refresh and validation; the collector and EventSub handling remain unimplemented.

Run the synthetic authorization tests with:

```bash
python3 -m unittest discover -s tests -v
```

After authorization, check channel access from the repository root:

```bash
python3 -m scripts.check_twitch_access
```

This check reads Twitch data and may update the private token file when refresh is needed. It validates the saved token, resolves the target channel, checks live/offline status, and requests at most one follower record to verify channel-specific access. It prints only diagnostic results and live/offline status; it does not save API responses or print identities. A successful HTTP response containing only a public follower total does not establish moderator access ([Twitch reference](https://dev.twitch.tv/docs/api/reference/#get-channel-followers)). An invalid access token triggers a refresh attempt and validation of the replacement. If refresh is rejected, check app settings and rerun the authorization helper. EventSub verification is still pending.


### Shared token management

`scripts/twitch_auth.py` provides one `TokenManager` instance to share within a process. It checks the application, authorized user, and required scopes at startup. Helix GET requests check whether hourly validation is due, refresh on HTTP 401, and retry the request once. Network errors, rate limits, and server errors do not trigger token refresh.

A future collector must call `validate_if_due()` regularly even when only listening to an idle WebSocket; this module does not start a background scheduler. Authentication failure must feed into source health handling, which is not implemented yet.

Replacement access and refresh tokens are saved atomically with owner-only permissions before the validation request, so a validation outage does not discard a rotated refresh token. A saved replacement is not proof of readiness: startup always validates it. If saving fails, the manager blocks further use; the old local file remains, but Twitch may already have rotated its refresh token, so reauthorization may be necessary. Identity or scope mismatches also block use.

Use one process that writes the token file at a time. Do not run the authorization helper or access probe alongside a future collector: the in-process lock does not coordinate separate processes. No database credentials or Twitch event records are handled by this module.

The synthetic tests cover refresh, bounded retries, hourly validation, identity/scope checks, private diagnostics, and token-file failure handling. A passing access probe confirms current API access; it does not prove EventSub delivery, collection coverage, or that live token refresh occurred if the token was already valid.

### Local database writer

Create a local virtual environment and install the PostgreSQL driver:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

`scripts/database.py` loads the two queries in `sql/queries/` and executes them
separately inside one transaction. `record_live_poll()` inserts the stream first,
then its viewer snapshot; a statement failure rolls back both writes. It preserves
the existing stream's first-observed time. The supplied observation timestamp is
also used as first-observed time when inserting a new stream. The caller must
supply an actual successful observation, with timezone-aware timestamps, and
serialize access to the writer's dedicated connection.

`open_writer()` connects to `stream_pulse` through the local WSL PostgreSQL socket
using the current operating-system user. It sets finite connection, statement,
and lock timeouts. It neither creates tables nor starts collection. The caller
must treat storage failures as coverage failures; that integration is still pending.

Retrying an observation must reuse its original ID, timestamp, and count. Duplicate
keys are skipped, not corrected. A connection failure during commit can leave the
outcome uncertain; do not replace the original timestamp with the retry time.

Run all tests, including PostgreSQL integration tests:

```bash
STREAM_PULSE_TEST_POSTGRES=1 .venv/bin/python -m unittest discover -s tests -v
```

These database tests connect to local PostgreSQL and use synthetic session-temporary
tables based on migrations 001 and 002. Their search path excludes public tables,
and the temporary tables disappear on disconnect. They do not inspect production
rows or rerun migrations against the production schema. Without the environment
flag, database tests are skipped and the remaining synthetic tests still run.

The agreed chat boundary rules and remaining integration work are documented in
[the collection policy](docs/collection-policy.md).
