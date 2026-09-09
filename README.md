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

The polling half of the collector connects OAuth/token refresh, stream status,
viewer snapshots, observed-live eligibility, run heartbeats, and polling-health
records. It uses the numbered schema files and the separately maintained SQL
queries. Synthetic runtime and PostgreSQL integration tests pass. Short live and
offline polling rehearsals have completed, including SQL verification of
offline-run lifecycle and health. Sustained collection and Windows/WSL sleep
behavior still need verification.

A separate EventSub capture collector (`collect_eventsub.py`) persists incoming
raids and follows, with per-source EventSub health and durable reconnection-gap
coverage rows. It reuses the readiness probe's socket, recovery, keepalive, and
clock handling. It stays raids + follows only; chat capture lives in the merged
`collect_stream.py` below. A real single-session EventSub probe confirmed all three
chat/raid/follow subscriptions enabled and received two keepalives on
September 7, 2026; automatic socket recovery has synthetic and loopback tests.
The capture collector then ran against Twitch for a full stream on
September 8, 2026 (about 8 hours): raid and follow subscriptions enabled,
keepalives on an unbroken 30-second cadence, 42 follow and 2 raid rows persisted
with no duplicate or rejected notification, a clean six-row per-source health
sequence, no reconnection-gap rows, and an orderly shutdown verified by SQL.
Socket recovery, directed handover, and live token refresh were not exercised
(no gap occurred and the access token never expired); Windows/WSL sleep during a
capture run remains unverified.

`scripts/collect_stream.py` is now the merged collector: one cooperative loop
runs viewer polling and EventSub chat, raid, and follow capture in the same
process and collector run, sharing one clock, heartbeat, and database writer.
Chat messages are stored only for a stream observed live at the message's
notification time, and chat's health is polling-driven
(`awaiting_stream_status` / `offline_observed` / `poll_failed` / `poll_stale`).
`--no-eventsub` keeps polling only; `--no-chat` keeps EventSub at raids +
follows; `collect_eventsub.py` stays as an isolated raids + follows tool. This
merged runtime has **synthetic and PostgreSQL integration tests only** -- a real
loopback WebSocket driving the actual recovery/coordinator path, a synthetic
disconnect and recovery, and a four-source run that stores an eligible chat
message and discards one outside eligibility -- and has **not** run against
Twitch. The first merged live rehearsal is planned for the Sunday
September 13, 2026 stream.

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

This is an initial authorization helper. Use the access probe below to verify channel-specific moderator access. Shared token management supports refresh and validation; the polling collector and separate EventSub readiness probe are described below. EventSub capture integration remains unimplemented.

After installing project dependencies in the virtual environment as described
below, run the synthetic tests with:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

After authorization, check channel access from the repository root:

```bash
python3 -m scripts.check_twitch_access
```

This check reads Twitch data and may update the private token file when refresh is needed. It validates the saved token, resolves the target channel, checks live/offline status, and requests at most one follower record to verify channel-specific access. It prints only diagnostic results and live/offline status; it does not save API responses or print identities. A successful HTTP response containing only a public follower total does not establish moderator access ([Twitch reference](https://dev.twitch.tv/docs/api/reference/#get-channel-followers)). An invalid access token triggers a refresh attempt and validation of the replacement. If refresh is rejected, check app settings and rerun the authorization helper. This access probe does not test EventSub; use the separate readiness probe below.


### Shared token management

`scripts/twitch_auth.py` provides one `TokenManager` instance to share within a process. It checks the application, authorized user, and required scopes at startup. Helix GET and JSON POST requests check whether hourly validation is due, refresh on HTTP 401, and retry the request once. Network errors, rate limits, and server errors do not trigger token refresh. POST also does not retry conflicts or uncertain creation outcomes.

The polling collector calls `validate_if_due()` through its worker every poll, including while the channel is offline. It forces validation after detected forward clock gaps. `TwitchError` exposes safe health categories and a fatal flag for blocked authorization. The EventSub probe checks whether validation is due every 30 seconds after subscription setup, even during idle sessions. The integrated EventSub runtime must preserve this scheduling; the token module does not start a background scheduler.

Replacement access and refresh tokens are saved atomically with owner-only permissions before the validation request, so a validation outage does not discard a rotated refresh token. A saved replacement is not proof of readiness: startup always validates it. If saving fails, the manager blocks further use; the old local file remains, but Twitch may already have rotated its refresh token, so reauthorization may be necessary. Identity or scope mismatches also block use.

Use one process that writes the token file at a time. Do not run the authorization helper, either access/readiness probe, or a second collector alongside the collector: the in-process lock does not coordinate separate processes. No database credentials are handled by this module; callers must keep returned API responses private.

The synthetic tests cover refresh, bounded retries, hourly validation, identity/scope checks, private diagnostics, and token-file failure handling. A passing access probe confirms current API access; it does not prove EventSub delivery, collection coverage, or that live token refresh occurred if the token was already valid.

### Local database writer

Create a local virtual environment and install the pinned project dependencies:

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
tables based on migrations 001, 002, 006, and 007, with migration 008 applied only
to the temporary health table. Their search path excludes public tables,
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

`record_collection_health()` validates the agreed polling and EventSub
source/status/reason combinations before inserting an observation. Migration 008
adds the `paused` status for chat's offline exclusion (`offline_observed`); chat
also accepts `awaiting_stream_status`, `poll_failed`, and `poll_stale`. Accepting
a health claim does not establish its truth. See the
[EventSub contract](docs/collection-policy.md#eventsub-health-reason-codes)
and [polling contract](docs/collection-policy.md#polling-health-reason-codes).
Validation does not prove the claim: the caller must record healthy only after
successful Twitch responses and required data writes, and enforce run/time boundaries.
Health inserts have no retry key; an uncertain commit must not be blindly retried.

`stop_collector_run()` executes the shutdown update and inserts a `stream_poll`
`stopped` / `orderly_shutdown` observation in one transaction, at the same timestamp.
Failure to write the health record rolls back the run update. Missing, already
stopped, or invalidly timed runs raise a safe error without adding health records.
The last heartbeat remains unchanged. This method serves the polling-only runtime,
whose only source is `stream_poll`. It does not mark any broadcast offline or close
old runs after crashes.

`record_follow_event()` and `record_raid_event()` insert one EventSub notification
each, keyed by `eventsub_message_id`, and skip a redelivery without changing the
stored row. They return `1` for a new row and `0` for a skipped duplicate.
`stream_id` stays NULL: follow and raid event-time association is a separate
decision. `raid_viewer_count` is range-checked in Python so an out-of-range value
is rejected rather than overflowing the `INTEGER` column.

`record_chat_message()` stores one `channel.chat.message` notification the same
way (dedupe on `eventsub_message_id`, `1`/`0` return), but requires a nonempty
`stream_id`: the chat sink calls it only after observed-live eligibility resolves
a stream. `message_fragments` is stored as `jsonb`; message text and fragments
are private and never appear in diagnostics. Migration 010 renames the
pre-existing `chat_messages` primary key to `eventsub_message_id` and adds
`chat_message_id` (the chat message's own id).

`record_reconnection_gap()` opens a coverage row for an unexpected EventSub
transport loss and returns its `gap_id`; `resolve_reconnection_gap()` records
recovery and returns the rows updated (`0` when the gap was already resolved).
`close_collector_run()` stops a run with the shutdown update only, no health
insert: the standalone EventSub capture collector's sinks each write their own
`stopped` / `orderly_shutdown` health before this final close.
`stop_collector_run_multi()` is the merged runtime's shutdown: in one
transaction it stops the run and writes one `stopped` / `orderly_shutdown` row
per active source, so a failed health insert also rolls back the run stop.

The database writer raises safe errors but does not implement local logging,
database reconnection, or recovery coverage inference. A database outage cannot
reliably be recorded in that same database while it is unavailable.

The agreed chat boundary rules and remaining integration work are documented in
[the collection policy](docs/collection-policy.md).

### Check EventSub subscription readiness

With the collector and all other token-writing programs stopped:

```bash
.venv/bin/python -m scripts.check_eventsub --duration 60
```

This makes **real Twitch API calls, creates WebSocket subscriptions, and may
refresh the private token file**. It makes no database connection or writes and
discards event contents. It checks subscription readiness and socket recovery.
It does not poll stream status or establish chat eligibility.

The probe validates authorization and resolves the target before connecting.
After welcome, one worker creates chat, incoming-raid, and follow subscriptions
sequentially while the main thread reads the socket. Each
`chat_subscription_enabled`, `raids_subscription_enabled`, or
`follows_subscription_enabled` diagnostic requires a matching enabled response
for that session, event type, version, and condition. Incoming-raid responses may include an empty unused
`from_broadcaster_user_id`; the matcher accepts that observed representation while
requiring the requested destination and rejecting nonempty origins or extra keys.
A single failed subscription does not prevent checks of the others.
Conflicts, timeouts, and server errors are
not blindly retried; an unacknowledged creation remains unconfirmed.
HTTP failures include only a numeric HTTP status in the diagnostic, such as
`raids_subscription_error_http_409`. A `subscription_response_*` diagnostic instead
identifies a local validation mismatch using a fixed code, without response values.

Keepalives maintain transport evidence without requiring activity in each source.
The probe allows two seconds beyond the advertised keepalive interval for receive
and scheduling delay. It reports `keepalive_waiting_within_grace` and cannot finish
successfully during that wait. A valid notification/keepalive received within the
allowance reports `liveness_received_within_grace`; otherwise the probe times out.
This bounded probe tolerance does not change polling or chat eligibility thresholds.
An optional `<source>_notification_envelope_received` diagnostic means an envelope
matched a confirmed subscription. Event fields are not fully validated or saved;
this does not prove usable event persistence. No identities, subscription/session
IDs, tokens, reconnect URLs, or raw messages are printed. WebSocket library logs
are disabled to prevent frame disclosure.

Exit 0 and `probe_finished_subscriptions_confirmed` mean all three subscriptions
were confirmed, subsequent keepalive/notification transport evidence was observed,
and no readiness failure was detected before deliberate closure. Quiet raids or
follows do not cause failure. Exit 1 means readiness was not fully confirmed;
earlier source diagnostics still describe the evidence obtained. Neither exit
status proves complete capture, live refresh, or sustained reliability.

Twitch-directed handover keeps reading the old socket while opening its replacement,
then preserves existing subscriptions without new POSTs. Unexpected connection
loss or keepalive timeout reports `probe_gap_detected_no_replay`, waits for the old
token worker, revalidates authorization, and creates a fresh session and subscriptions.
Retries back off from one to 30 seconds. Failed or revoked sources stay failed for
this probe; recovery rebuilds only unaffected sources. Initial connection failure,
malformed envelopes, clock uncertainty, and shared authorization failure end the probe.
See [socket recovery details](docs/collection-policy.md#socket-recovery).

After a gap, exit 0 uses `probe_finished_subscriptions_confirmed_after_gap`: current
subscriptions are confirmed, but the earlier gap remains. No events are replayed.

Ctrl+C/SIGTERM or duration requests shutdown. The sockets close and the probe
waits for connection and API/token workers, preserving token rotation. Duration
starts after initial socket opening, includes recovery waits, and is not a hard
exit deadline. Closing the final socket disables its associated subscriptions;
another run creates a new session
([Twitch WebSocket documentation](https://dev.twitch.tv/docs/eventsub/handling-websocket-events/)).
The probe never claims orderly collector-run shutdown or writes capture health.

Synthetic tests include local loopback WebSocket servers for actual frame receipt,
automatic Pong replies, and two-socket handover without extra subscription POSTs.
They do not contact Twitch or use saved authorization. A real probe confirmed all
three subscriptions and keepalives before recovery was implemented. Actual Twitch
reconnection, event delivery, and capture readiness remain unverified.

### Run the merged collector

Prerequisites: installed Python requirements, every numbered schema file applied
to local PostgreSQL (through `sql/010_align_chat_messages.sql`), and saved Twitch
authorization. Do not rerun schema creation files for an already initialized
database. Keep the Windows PC awake and WSL running. Run only one token-writing
program at a time; do not run `collect_eventsub.py` or a probe alongside this.

From the repository root:

```bash
.venv/bin/python -m scripts.collect_stream                 # polling + chat + raids + follows
.venv/bin/python -m scripts.collect_stream --no-chat       # polling + raids + follows
.venv/bin/python -m scripts.collect_stream --no-eventsub   # polling only
```

This writes **real observations, events, and collector-run records to the private
local database** and may refresh the private token file. Viewer polling,
live/offline status, viewer snapshots, and polling health run every time; unless
`--no-eventsub`, one shared EventSub socket also captures raids and follows and
(unless `--no-chat`) chat. Chat messages are stored only for a stream observed
live at the message's notification time; a message outside that window is
discarded, not stored. Every active source's health lands under one collector
run. **This merged runtime has not run against Twitch yet** -- the first live
rehearsal is the Sunday September 13, 2026 stream.

For a three-minute rehearsal, use:

```bash
.venv/bin/python -m scripts.collect_stream --duration 180
```

The first poll starts immediately; subsequent polls target 60-second slots.
Heartbeat check-ins target 30 seconds. After 90 seconds without a fresh successful
poll, health becomes stale and chat eligibility closes. Only a valid empty stream
response establishes offline; failed polls do not produce zero-viewer snapshots.
Missed poll slots are skipped. An unexpected EventSub disconnect opens a
`reconnection_gaps` row and reconnects with backoff; a coordinator clock gap tears
down and rebuilds the EventSub session as well as forcing a fresh poll.

Ctrl+C or SIGTERM requests shutdown. The collector stops dispatching polls,
closes chat eligibility, waits for the in-flight Twitch operation and the
EventSub socket/workers to finish, discards late results, then in one
transaction stops the run and writes one `stopped` / `orderly_shutdown` health
row per active source (`stop_collector_run_multi`; the `--no-eventsub` path uses
the single-source `stop_collector_run`). This wait lets token
refresh/persistence finish. Request timeouts are not a hard overall shutdown
deadline; `--duration` also requests shutdown and does not guarantee the process
has exited at that instant.

Terminal output contains UTC timestamps and safe diagnostic codes, without
identities or raw responses. To retain diagnostics across terminal sessions in an
ignored local file, Bash can use:

```bash
set -o pipefail
.venv/bin/python -m scripts.collect_stream | tee -a .env.collector.log
```

`live_poll_saved` / `offline_poll_saved` means required data and healthy evidence
writes were acknowledged. `poll_stale` closes eligibility while the worker may
still be waiting. `utc_clock_rollback_waiting` pauses writes for a small UTC
correction; `utc_clock_recovered_fresh_poll_required` means real UTC caught up
and a fresh poll is required. No timestamps are clamped or rewritten. A storage
failure, blocked authorization, unrecoverable clock rollback, or unexpected
internal failure exits with status 1; fix the cause and restart.
Successful orderly shutdown exits with status 0.

On storage failure -- from the polling side or from an EventSub event, health,
gap, or heartbeat write -- the collector latches, makes no further database
writes, does not retry an uncertain commit, and leaves the run **open** (it skips
the multi-source stop), exiting 1. A saved snapshot or event row may exist even
though its health write failed. Preserve uncertainty from the last durable
evidence. Restart starts a new run and does not repair previous runs or infer old
broadcast end times. See
[runtime behavior and limitations](docs/collection-policy.md#polling-runtime).

### Run the EventSub capture collector

Prerequisites: installed Python requirements, every schema file applied to local
PostgreSQL (including `sql/009_create_reconnection_gaps.sql`), and saved Twitch
authorization. This is a **separate process from the polling collector** with its
own collector run; do not run them, or any other token-writing program, at the
same time.

From the repository root:

```bash
.venv/bin/python -m scripts.collect_eventsub
.venv/bin/python -m scripts.collect_eventsub --duration 300
```

This makes **real Twitch API calls, creates `channel.raid` and `channel.follow`
subscriptions, writes events and EventSub health to the private local database,
and may refresh the private token file**. It captures incoming raids and follows
only. It does **not** capture chat (that needs observed-live eligibility) and does
not poll stream status. Omitting `--duration` runs until Ctrl+C/SIGTERM.

Each source's health follows the [EventSub contract](docs/collection-policy.md#eventsub-health-reason-codes):
`starting` / `initializing` at setup, `healthy` / `capture_ready` once the
subscription is enabled, transport is responsive, and a write has succeeded, and
`stopped` / `orderly_shutdown` at shutdown. A rejected notification records
`error` / `invalid_notification` and holds there until a later valid event; a
transport gap records `error` / `network_error` or `keepalive_timeout` and clears
when transport returns. Health rows are written only when a source's
`(status, reason_code)` changes. A quiet channel with no raids or follows stays
`healthy`. `healthy` never proves complete capture.

An unexpected socket loss writes a `reconnection_gaps` row (`probe_gap_detected_no_replay`);
its `recovered_at` is filled once transport is live again, and stays NULL if the
run ends first. Events during a gap are not replayed. Twitch-directed reconnects
keep the old socket open and are not gaps.

The socket, recovery, keepalive, and clock rules are the readiness probe's,
reused unchanged. On a storage failure the collector stops without further writes
and leaves the run open (`capture_storage_failure_stop_required`), exiting with
status 1; a clean shutdown closes the run and exits 0. It does not reconnect to
the database or repair earlier runs. See
[EventSub capture runtime](docs/collection-policy.md#eventsub-capture-runtime).
