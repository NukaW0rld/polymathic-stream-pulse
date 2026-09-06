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

The polling-only collector connects OAuth/token refresh, stream status, viewer
snapshots, observed-live eligibility, run heartbeats, and polling-health records.
It uses seven schema files and the separately maintained SQL queries. Synthetic
runtime and PostgreSQL integration tests pass; live operation and Windows/WSL
sleep behavior still require a rehearsal. This is not yet the full first-collection
scope: EventSub delivery and chat/raid/follow persistence remain unimplemented.

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

This is an initial authorization helper. Use the access probe below to verify channel-specific moderator access. Shared token management supports refresh and validation; the polling collector is described below. EventSub handling remains unimplemented.

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

The polling collector calls `validate_if_due()` through its worker every poll, including while the channel is offline. It forces validation after detected forward clock gaps. `TwitchError` exposes safe health categories and a fatal flag for blocked authorization. A future EventSub runtime must also arrange validation while listening to an idle WebSocket; the token module does not start a background scheduler.

Replacement access and refresh tokens are saved atomically with owner-only permissions before the validation request, so a validation outage does not discard a rotated refresh token. A saved replacement is not proof of readiness: startup always validates it. If saving fails, the manager blocks further use; the old local file remains, but Twitch may already have rotated its refresh token, so reauthorization may be necessary. Identity or scope mismatches also block use.

Use one process that writes the token file at a time. Do not run the authorization helper, access probe, or a second collector alongside the collector: the in-process lock does not coordinate separate processes. No database credentials or Twitch event records are handled by this module.

The synthetic tests cover refresh, bounded retries, hourly validation, identity/scope checks, private diagnostics, and token-file failure handling. A passing access probe confirms current API access; it does not prove EventSub delivery, collection coverage, or that live token refresh occurred if the token was already valid.

### Local database writer

Create a local virtual environment and install the PostgreSQL driver:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

`scripts/database.py` loads the queries in `sql/queries/`. For a live poll it executes the two insert queries
separately inside one transaction. `record_live_poll()` inserts the stream first,
then its viewer snapshot; a statement failure rolls back both writes. It preserves
the existing stream's first-observed time. The supplied observation timestamp is
also used as first-observed time when inserting a new stream. The caller must
supply an actual successful observation, with timezone-aware timestamps, and
serialize access to the writer's dedicated connection.

`open_writer()` connects to `stream_pulse` through the local WSL PostgreSQL socket
using the current operating-system user. It sets finite connection, statement,
and lock timeouts. It neither creates tables nor starts collection. The polling
collector treats storage failures as coverage failures and exits with a safe
diagnostic. It does not reconnect or retry uncertain writes automatically.

Retrying an observation must reuse its original ID, timestamp, and count. Duplicate
keys are skipped, not corrected. A connection failure during commit can leave the
outcome uncertain; do not replace the original timestamp with the retry time.

Run all tests, including PostgreSQL integration tests:

```bash
STREAM_PULSE_TEST_POSTGRES=1 .venv/bin/python -m unittest discover -s tests -v
```

These database tests connect to local PostgreSQL and use synthetic session-temporary
tables based on migrations 001, 002, 006, and 007. Their search path excludes public tables,
and the temporary tables disappear on disconnect. They do not inspect production
rows or rerun migrations against the production schema. Without the environment
flag, database tests are skipped and the remaining synthetic tests still run.

The writer also exposes `mark_stream_offline()` using the offline update query.
It records the first successful offline detection for the supplied broadcast and
leaves an existing detection unchanged. Its return value is the number of updated
rows: zero can mean either already marked or no matching stream. It does not create
a stream or viewer snapshot. Only a successful offline response for a previously
tracked stream should trigger this operation; failures, outgoing raids, and shutdown
must not. The polling collector enforces these rules for the broadcast it tracks
within the current execution.

`start_collector_run()` commits a new execution and returns its generated run ID.
`update_collector_heartbeat()` records a check-in only for an unstopped run with no
newer stored heartbeat. A zero-row update raises a safe error rather than silently
reporting success. Neither operation establishes source readiness. Both require
timezone-aware timestamps; the polling collector schedules check-ins and handles
Ctrl+C/SIGTERM.
Run creation is not automatically retried: if a connection fails during commit,
the database may contain a run whose ID the caller never received. Startup must
stop on that uncertainty; another INSERT would create a distinct run.

`record_collection_health()` validates the agreed `stream_poll` source/status/reason
combinations before inserting an observation. It rejects EventSub sources until
their readiness rules are implemented. See the [reason-code contract](docs/collection-policy.md#polling-health-reason-codes).
Validation does not prove the claim: the caller must record healthy only after
successful Twitch responses and required data writes, and enforce run/time boundaries.
Health inserts have no retry key; an uncertain commit must not be blindly retried.

`stop_collector_run()` executes the shutdown update and inserts a `stream_poll`
`stopped` / `orderly_shutdown` observation in one transaction, at the same timestamp.
Failure to write the health record rolls back the run update. Missing, already
stopped, or invalidly timed runs raise a safe error without adding health records.
The last heartbeat remains unchanged. This method supports a polling-only runtime;
it must be extended to close other active sources before use by a full EventSub
collector. It does not mark any broadcast offline or close old runs after crashes.

The database writer raises safe errors but does not implement local logging,
database reconnection, or recovery coverage inference. A database outage cannot
reliably be recorded in that same database while it is unavailable.

The agreed chat boundary rules and remaining integration work are documented in
[the collection policy](docs/collection-policy.md).

### Run the polling collector

Prerequisites: installed Python requirements, the existing schema applied to local
PostgreSQL, and saved Twitch authorization. Do not rerun schema creation files for
an already initialized database. Keep the Windows PC awake and WSL running.

From the repository root:

```bash
.venv/bin/python -m scripts.collect_stream
```

This writes **real observations and collector-run records to the private local
database**. It may refresh the private token file. Run only one token-writing
program at a time. It collects stream identity, live/offline observations, viewer
snapshots, and polling health; it does not collect chat, raids, or follows yet.

For a three-minute rehearsal, use:

```bash
.venv/bin/python -m scripts.collect_stream --duration 180
```

The first poll starts immediately; subsequent polls target 60-second slots.
Heartbeat check-ins target 30 seconds. After 90 seconds without a fresh successful
poll, health becomes stale. Only a valid empty stream response establishes offline;
failed polls do not produce zero-viewer snapshots. Missed poll slots are skipped.

Ctrl+C or SIGTERM requests shutdown. The collector waits for the in-flight Twitch
operation to finish, discards its result, then commits the run stop and stopped
health together. This wait lets token refresh/persistence finish. Request timeouts
are not a hard overall shutdown deadline; `--duration` also requests shutdown and
does not guarantee the process has exited at that instant.

Terminal output contains UTC timestamps and safe diagnostic codes, without
identities or raw responses. To retain diagnostics across terminal sessions in an
ignored local file, Bash can use:

```bash
set -o pipefail
.venv/bin/python -m scripts.collect_stream | tee -a .env.collector.log
```

`live_poll_saved` / `offline_poll_saved` means required data and healthy evidence
writes were acknowledged. `poll_stale` closes eligibility while the worker may
still be waiting. A storage failure, blocked authorization, clock rollback, or
unexpected internal failure exits with status 1; fix the cause and restart.
Successful orderly shutdown exits with status 0.

On storage failure, the collector makes no further database writes and does not
retry an uncertain commit. A saved snapshot may exist even if its health write
failed. A run may remain unclosed; an uncertain shutdown commit might have closed
it despite the error. Preserve uncertainty from the last durable evidence.
Restart starts a new run and does not repair previous runs or infer old broadcast
end times. See [runtime behavior and limitations](docs/collection-policy.md#polling-runtime).
