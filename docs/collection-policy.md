# Initial collection policy

This policy supports within-stream chat activity and viewer timelines without
silently assigning messages through known collection uncertainty. The merged
collector implements viewer polling and EventSub chat, raid, and follow capture
in one process. Synthetic and PostgreSQL tests pass; the merged runtime has not
run against Twitch, so the policy and tests do not establish real-world capture
completeness.

## Observed-live eligibility

- Poll stream status provisionally every 60 seconds.
- Start chat eligibility after successfully observing a live broadcast and
  persisting the required stream record. Associate messages with that stream ID.
- A failed poll immediately closes eligibility without declaring the stream offline.
- At 90 seconds since the last successful live observation, eligibility expires.
  Check freshness on every chat callback and during collector health checks.
- Successful offline responses close eligibility and establish an offline
  detection time, not an exact broadcast end time.
- After uncertainty, a successful live response starts a new eligibility interval,
  even if the stream ID is unchanged. Do not backfill chat from the uncertain interval.
- A changed stream ID starts a new interval. It does not establish an exact end
  time for the preceding stream.
- An outgoing raid alone does not change eligibility.
- Accept a chat notification only when its notification timestamp is at or after
  the current interval's start and at or before receipt, and eligibility is still
  fresh at receipt. Reject malformed timestamps. Comparisons use aware UTC times.

This conservative policy can discard legitimate delayed messages. Clock differences
can also cause exclusions. Polling may admit messages between actual broadcast end
and offline detection; it cannot enforce exact video-live boundaries.

`scripts/live_status.py` implements only eligibility decisions. The caller must
serialize updates and checks. Elapsed time is supplied separately from UTC timestamps;
the polling runtime cross-checks UTC and elapsed time to detect clock gaps. Its
Windows/WSL suspend behavior still needs a real-machine rehearsal.

## Integration requirements and status

- Insert a stream before its first viewer snapshot or chat message. Repeated live
  observations must preserve the original first-observed timestamp.
- Failed viewer requests produce no raw snapshot, including no invented zero.
- Database persistence failures must close eligibility and affect coverage, even
  if Twitch connectivity is healthy. A failed database cannot reliably record its
  own outage; recovery must preserve uncertainty from the last durable evidence.
- Keep broadcast lifecycle tracking separate from chat eligibility: pausing chat
  must not forget which prior broadcast might later receive offline detection.
- Record standardized health reasons for uncertain live status and chat pauses.
  A connected subscription alone is insufficient for chat collection health.
- Follows and incoming raids may have NULL stream associations. Their association
  rules are not implemented by the chat eligibility component.
- Database transactions cover live observations, run operations, polling health,
  EventSub event, and per-source health writes. The merged runtime schedules
  heartbeats, handles shutdown signals, records polling and EventSub source
  health, and captures chat/raid/follow events. Database recovery is still not
  implemented; a storage failure latches and leaves the run open.

## EventSub readiness contract

Use one EventSub WebSocket with separate subscriptions for chat
(`channel.chat.message`, version `1`), incoming raids (`channel.raid`, version `1`,
targeting the destination broadcaster), and follows (`channel.follow`, version
`2`). Reuse the process's shared user-token manager. The existing chat-reading and
follower-reading scopes match this approach; actual subscription acceptance and
delivery still require verification. See Twitch's
[subscription requirements](https://dev.twitch.tv/docs/eventsub/eventsub-subscription-types/).

The [EventSub capture runtime](#eventsub-capture-runtime) implements this contract:
`raids` and `follows` in both the standalone and merged collectors, and `chat`
with its polling-linked states in the merged collector. A session may carry any
non-empty subset of the three sources.

Source readiness has the following agreed meanings:

| Status | Meaning |
| --- | --- |
| `starting` | Establishing the session and confirming the source's subscription |
| `healthy` | Correct subscription confirmed enabled, transport responsive, authorization current, and persistence available; chat also requires fresh observed-live eligibility |
| `paused` | Chat intentionally excluded following a successful offline observation, using reason `offline_observed` |
| `error` | A required readiness condition failed, including transport, subscription, authorization, or chat's failed/stale polling |
| `stopped` | Deliberate source shutdown, with durable stopped evidence when storage works |

Migration `008_add_paused_health_status.sql` adds `paused` to the health status
CHECK constraint. The developer applied it successfully to the local database.
The standalone capture runtime drives these transitions for `raids` and
`follows`; the merged runtime (below) adds `chat`, whose `ChatSink` drives the
chat-only states (`awaiting_stream_status`, `paused` / `offline_observed`,
`poll_failed`, `poll_stale`) from the coordinator's polling half. The grain
remains one health observation for one source during a collector run.
An offline chat pause must not conceal a transport or authorization failure.
Follows and raids do not inherit chat's observed-live eligibility requirement.

Subscription readiness alone must not be recorded as healthy event collection
before event persistence is implemented. Quiet event streams do not themselves
indicate failure; transport keepalives provide separate evidence. Healthy does
not guarantee complete capture or prove that an actual event has been delivered.

Distinguish an unexpected disconnect, which requires new subscriptions and has
no event replay, from Twitch's directed reconnect flow, which transfers existing
subscriptions while the old socket remains open until the replacement welcome.
See Twitch's [WebSocket handling rules](https://dev.twitch.tv/docs/eventsub/handling-websocket-events/).

### EventSub health reason codes

These combinations apply to `chat`, `raids`, and `follows`:

| Status | Reason code | Evidence required from the caller |
| --- | --- | --- |
| `starting` | `initializing` | Initial connection/subscription setup is underway |
| `healthy` | `capture_ready` | All source readiness conditions above hold, including event persistence |
| `error` | `network_error` | Connection failed or unexpectedly closed |
| `error` | `keepalive_timeout` | Transport liveness deadline expired |
| `error` | `subscription_error` | Subscription creation failed or its response could not establish readiness |
| `error` | `subscription_revoked` | Twitch revoked this source's subscription |
| `error` | `auth_error` | Required authorization could not be established |
| `error` | `invalid_notification` | A notification for this source could not be safely processed |
| `error` | `clock_uncertain` | A detected clock discontinuity prevents trustworthy processing |
| `stopped` | `orderly_shutdown` | This source was deliberately stopped |

Only `chat` additionally accepts:

| Status | Reason code | Evidence required from the caller |
| --- | --- | --- |
| `starting` | `awaiting_stream_status` | Subscription ready, but no accepted initial live/offline observation |
| `paused` | `offline_observed` | Fresh successful polling says offline and other readiness conditions hold |
| `error` | `poll_failed` | Failed poll immediately invalidated chat eligibility |
| `error` | `poll_stale` | No accepted successful poll within 90 seconds |

For EventSub sources, record health when status or reason changes, not on every
message or keepalive. After an error, remain in error while retrying; `starting`
describes initial setup. Failures take precedence over an offline chat pause.
A quiet channel remains ready while transport and authorization checks succeed.
A duplicate safely skipped by its message key is normal behavior.

A permanent failure of one subscription leaves unaffected sources collecting,
with an explicit error for the failed source. Shared fatal authorization or
storage failure stops the entire collector. Storage failure keeps the existing
no-further-writes behavior and safe local diagnostics; it cannot reliably record
its own health in the failed database. A later orderly shutdown does not erase
earlier source errors.

The writer validates allowed combinations only. It does not establish readiness,
apply failure precedence, suppress unchanged observations, or perform recovery.
Those are the capture runtime's responsibilities. `stop_collector_run()` serves
the polling-only path; the standalone EventSub collector uses
`close_collector_run()` (run stop, no health insert) after its sinks each write
their own `stopped` health; the merged runtime uses `stop_collector_run_multi()`
(run stop plus one `stopped` / `orderly_shutdown` row per active source, in one
transaction).

Probe transport recovery and idle authorization scheduling are defined below, and
the [EventSub capture runtime](#eventsub-capture-runtime) section covers event
validation, persistence, the per-source health state machine, reconnection-gap
coverage, and the merged polling + EventSub process, which adds `chat` as a
fourth source. Still unspecified: recovery after malformed notifications once
persistence is running, and queued event treatment at collector shutdown.
Follow and raid event-time associations remain a separate analytical decision.
Everything below the readiness probe is synthetically and PostgreSQL tested only;
none of it establishes real-world collection readiness, and the merged runtime
has not run against Twitch.

### Readiness probe

`scripts/eventsub.py` defines the three subscription requests and validates
matching enabled responses. `scripts/check_eventsub.py` and
`scripts/eventsub_recovery.py` manage sockets and check readiness without
connecting to PostgreSQL or persisting events. It emits
diagnostic codes, never collection-health records. Even an enabled chat
subscription does not establish observed-live eligibility.

Authorization and target lookup happen before socket opening so they do not
consume Twitch's initial subscription window. One worker performs serialized
subscription POSTs and subsequent idle token-validation checks; the main thread
continues receiving during these potentially blocking operations. The manager
checks validation eligibility every 30 seconds after setup and performs actual
validation when due hourly. POST retries are limited to one retry after explicit
401 and token refresh; other failures, including uncertain creation and 409
conflict, leave that source unconfirmed. Other subscriptions are still attempted.
See the [subscription API](https://dev.twitch.tv/docs/api/reference/#create-eventsub-subscription)
and [token validation requirements](https://dev.twitch.tv/docs/authentication/validate-tokens/).

The pinned `websockets` client has automatic outgoing Ping disabled while retaining
server Ping/Pong handling. It requests a 30-second keepalive timeout and uses the
actual timeout returned by welcome, with a two-second probe receive tolerance.
During that tolerance the probe reports `keepalive_waiting_within_grace` and is
not ready to finish successfully. A valid frame within the allowance restores
transport evidence and reports `liveness_received_within_grace`. Waiting alone
never moves the deadline. This operational tolerance is not a Twitch guarantee
and does not extend the polling/chat 90-second freshness boundary.
EventSub notifications and keepalives refresh
liveness; Pong frames do not. Messages are bounded to 1 MiB with a 16-frame receive
queue, and private library logging is disabled. Event bodies are discarded after
envelope validation. See the [client reference](https://websockets.readthedocs.io/en/stable/reference/sync/client.html).

Readiness is tracked separately for each source. Revocation arriving before a
POST acknowledgement cannot be undone by that late acknowledgement. Actual
notification envelopes are optional evidence, logged at most once per source;
the probe does not implement event-body validation, event deduplication, or
follow/raid stream associations. A quiet session can confirm subscription
readiness using keepalives without seeing any actual follows or raids.

Transport loss and directed handover use the recovery rules below. Malformed
envelopes or idle validation failure end the probe, even if validation failure is
transient. Clock checks reuse polling clock readings but conservatively end the
probe on any UTC/elapsed rollback or at least five seconds of disagreement.
Buffered messages cannot restore a session after its liveness deadline plus
receive tolerance expires. Polling rollback recovery is unchanged.

Shutdown closes sockets and waits for connection and token workers; it does not
forcibly cancel a refresh. Late successful setup results are discarded, and late
worker failures prevent a successful probe result. No database shutdown or health
writes are attempted.

### Socket recovery

The probe maintains one active socket and at most one replacement attempt.
For Twitch-directed handover, a connection worker opens the supplied URL and waits
for welcome while the coordinator continues reading the old socket. URLs must use
`wss`, the exact `eventsub.wss.twitch.tv` host, and the default or 443 port, with no
embedded credentials, fragments, control characters, or backslashes. Accepted URLs
are used unchanged and never logged. The old socket remains open until a valid
replacement welcome, following [Twitch's reconnect flow](https://dev.twitch.tv/docs/eventsub/handling-websocket-events/#reconnect-message).

Before switching, the coordinator drains up to 256 buffered old-socket frames so
revocations are retained. Subscription IDs and failed-source state transfer;
there are no new subscription POSTs or extra authorization calls for clean
handover. The existing idle validation worker continues. The replacement needs
subsequent keepalive/notification evidence before final readiness confirmation.
A replacement must be ready within 25 seconds, reserving five seconds for old
socket closure within Twitch's 30-second window. Duplicate reconnect instructions
do not reset that deadline. Failed handover, excessive buffered frames, lost old
socket liveness, or handover during unfinished setup falls back to fresh-session
recovery with a gap diagnostic.

Unexpected transport loss, keepalive timeout, or missing welcome after socket
opening emits `probe_gap_detected_no_replay`. This is detection time, not an exact
outage onset. Recovery closes the old socket and finishes its token worker before
starting new authorization work. Late successes from the old session are discarded;
source errors and fatal worker failures still apply. Failed or revoked sources
remain failed for the probe; only unaffected subscriptions are recreated. There
is no source-only retry policy yet.

Fresh-session attempts wait 1, 2, 4, 8, 16, then at most 30 seconds between failures.
The retry counter resets once the connection is at least 60 seconds old and all
sources are currently ready. Each attempt forces token validation before dialing;
validation failure ends the probe. Initial preflight or initial connection failure
also ends the probe. Stop requests interrupt backoff and prevent further dispatch.

The duration includes recovery time after initial socket opening. Shutdown closes
all sockets and waits for connection/token work. A pending handover at shutdown
cannot produce successful readiness. After recovery, the final success diagnostic
is `probe_finished_subscriptions_confirmed_after_gap`; it describes current
readiness and does not erase the earlier gap. The probe writes no durable coverage
records. Collector integration must persist those gaps separately.

Tests cover handover, buffered revocation, late old-session results, failed sources,
fresh sessions, bounded backoff, deadlines, shutdown, and clock rollback. A local
two-socket server verifies handover retains only the three original subscription
POSTs. Actual Twitch reconnection and Windows sleep/resume remain unverified.
The real rehearsals below predate recovery implementation.

The developer's first real probe on September 7, 2026 confirmed enabled chat and
follow subscription responses, but reported a generic raid subscription error
and timed out almost exactly 30 seconds after welcome. The original check ran
before processing a just-received frame and could reject a keepalive arriving
slightly beyond the nominal interval. A synthetic boundary test reproduces that
failure without tolerance and passes with the bounded allowance. The first log
does not prove a keepalive actually arrived; the subsequent rehearsals below
provide separate liveness evidence.
Fixed response-mismatch codes and safe numeric HTTP statuses distinguish local
validation from API rejection without exposing raw responses or real identities.

The second real probe confirmed session keepalive receipt, including one frame
within the bounded receive tolerance, and again confirmed chat/follow subscription
creation. Its raid diagnostic isolated the local mismatch: the response condition
contained the requested destination plus an empty unused `from_broadcaster_user_id`.
The matcher now accepts exactly that representation for incoming raids, including
notification/revocation envelopes. Requests still specify only the destination;
nonempty origins, wrong destinations, NULL origins, and unknown condition keys
remain rejected. Synthetic tests cover these boundaries. Neither of the first
two rehearsals established full subscription readiness or event capture.

The third real probe on September 7, 2026 succeeded. Its diagnostic log recorded
welcome at approximately 15:49:12 UTC, enabled responses for all three selected
subscriptions by 15:49:13 UTC, and keepalives at approximately 15:49:42 and
15:50:12 UTC. Both keepalives were accepted within the bounded receive tolerance.
The probe closed its socket and reported `probe_finished_subscriptions_confirmed`
at approximately 15:50:32 UTC. This confirms subscription creation and transport
liveness during that short session. No notification-envelope diagnostics appeared
in the supplied log; it does not establish actual chat/raid/follow event delivery,
event persistence, live token refresh, reconnection continuity, sustained
reliability, or full first-collection readiness. The probe made no database writes.

### EventSub capture runtime

`scripts/collect_eventsub.py` runs as a **separate process** from the merged
collector, with its own collector run. It captures `raids` and `follows` only and
subscribes to just those two event types. It does not poll stream status and does
not capture chat; the merged runtime below is where chat lives. Run only one
token-writing program at a time.

**Merged runtime (synthetic + PostgreSQL tested only; not run against Twitch).**
`scripts/collect_stream.py` runs viewer polling and EventSub chat + raid + follow
capture in one process and collector run, so both halves share one clock
authority (`ClockGuard`), one heartbeat, and one `DatabaseWriter`. Its `step()`
drives the polling half then `RecoveringProbe.step(now)` (externally driven,
`external_clock=True`); a coordinator clock gap tears down and rebuilds the
EventSub session as well as forcing a fresh poll; shutdown closes the run over
every active source with `stop_collector_run_multi`. `--no-eventsub` keeps
polling-only behaviour; `--no-chat` keeps EventSub at raids + follows;
`collect_eventsub.py` stays as an isolated raids + follows tool. See
[merged-collector-design.md](merged-collector-design.md).

**Chat as the fourth source.** `channel.chat.message` v1 uses the spec already in
`eventsub.py`. `parse_chat_notification` (in `eventsub_capture.py`) validates the
body; it has no event-time field, so `notification_at` is the envelope time.
`ChatSink` stores a message only when
`LiveStatus.chat_stream_id(notification_at, received_at, tick)` resolves a
stream -- a message outside observed-live eligibility is discarded, not stored
and not an error. `chat_messages` is keyed by `eventsub_message_id` (migration
`010`); message text and fragments are private and never logged. Chat's resting
health is polling-driven: `starting` / `awaiting_stream_status` until the first
accepted poll, then `healthy` / `capture_ready` while live, `paused` /
`offline_observed` on a fresh offline, and `error` / `poll_failed` or `poll_stale`
on lost eligibility. A transport or `invalid_notification` error, and a
deliberate stop, take precedence over the polling state, so an offline pause
never hides a transport failure. The coordinator drives these transitions from
the same points it updates `LiveStatus`.

**Reused transport.** The socket, fresh-session recovery, directed handover,
keepalive tolerance, and clock-uncertainty rules are the readiness probe's,
unchanged. The probe loop takes an optional persistence router and an optional
per-tick heartbeat callback; with neither it is exactly the probe.

**Event validation.** `scripts/eventsub_capture.py` turns a decoded notification
frame plus a receipt time into a validated record. `channel.follow` v2 yields
`user_id` and `followed_at`; `channel.raid` v1 has no event-time field, so only
the notification and receipt times are stored, and `viewers` is range-checked so
an out-of-range value is rejected rather than overflowing `INTEGER`. Errors carry
a fixed reason code and never event contents. The transport/session envelope is
validated separately by `SessionReadiness.accept`, which returns the frame as an
`EventDelivery` for the router.

**Persistence and grain.** `insert_follow_event.sql` and `insert_incoming_raid.sql`
insert one row per notification, keyed by `eventsub_message_id`, and skip a
redelivery without altering the stored row (dedup at persistence). `stream_id`
stays NULL: follow and raid event-time association is a separate analytical
decision and does not reuse chat's eligibility.

**Per-source health state machine.** One sink per source is the sole writer of
that source's health. It combines the coordinator's transport signal
(`transport_ready` / `transport_error`) with its own persistence outcomes:

- `starting` / `initializing` at setup.
- `healthy` / `capture_ready` once the subscription is enabled and responsive
  **and** a write has succeeded (or the channel is simply quiet). A persisted
  event before transport is confirmed is stored but does not claim `healthy`.
- `error` / `invalid_notification` on a rejected notification; it holds there
  until a later valid, persisted notification proves processing recovered.
  Transport being healthy does not by itself clear it.
- `error` / `network_error` or `keepalive_timeout` on a transport gap; cleared by
  `observe` when transport returns.
- `error` / `subscription_revoked` on a revocation (sticky for the run).
- `stopped` / `orderly_shutdown` at shutdown.

Health rows are written **only when `(status, reason_code)` changes** — not per
message or keepalive. Unknown transport reasons map to `subscription_error`. A
`StorageError` from any write latches a no-more-writes state; the loop then stops
with `capture_storage_failure_stop_required` and does not write the run stop or
source `stopped` health, because a failed database cannot record its own state.

**Reconnection-gap coverage.** An unexpected socket loss opens a
`reconnection_gaps` row (`probe_gap_detected_no_replay`) with `detected_at` — the
detection time, up to roughly the keepalive interval after delivery actually
stopped, not the exact onset. `recovered_at` is filled once any subscription is
live again, and stays NULL if the run ends first (capture did not observably
recover). One row per outage; repeated losses without recovery do not stack.
Twitch-directed reconnects keep the old socket open and are **not** gaps. Events
during a gap are never replayed. This table records EventSub transport gaps only;
polling gaps stay implicit as before.

**Run lifecycle.** `start_collector_run` opens the run; a per-tick `Heartbeat`
writes a check-in on a 30-second cadence and latches the storage-failure state on
a failed write. On clean shutdown the sinks write their `stopped` health and then
`close_collector_run` stops the run (no health insert). A latched storage failure
skips both. Every restart is a new run; it does not repair earlier runs.

**Test status.** Synthetic and PostgreSQL integration tests cover event
validation (chat, raid, follow), idempotent persistence, the raid/follow and
chat health state machines, gap open/resolve, heartbeats, the merged coordinator
wiring, and full loopback-socket runs: one across a synthetic disconnect and
fresh-session recovery, and a four-source run that stores an eligible chat
message and discards one outside eligibility. Nothing here has run against
Twitch.

**First live rehearsal.** On September 8, 2026 the capture collector ran against
Twitch for the first time, for a full stream: run start at approximately
17:12:03 UTC, orderly shutdown at approximately 01:19:06 UTC on September 9
(about 8 hours 7 minutes). `channel.raid` v1 and `channel.follow` v2 responded
enabled by 17:12:04 UTC; both sources reached `healthy` / `capture_ready` at the
first keepalive (17:12:33 UTC). The session received keepalives on an unbroken
30-second cadence for the whole run with no `keepalive_timeout`, no
`reconnection_gaps` row, and no directed reconnect. It persisted 42 rows to
`follow_events` and 2 to `incoming_raids`, every row with `stream_id` NULL, with
no duplicate-skip and no rejected notification. `collection_health` recorded
exactly six rows for the run — `starting` / `initializing`, `healthy` /
`capture_ready`, and `stopped` / `orderly_shutdown` for each source, written only
on change. Heartbeats held the 30-second cadence, the last about 17 seconds
before stop. Shutdown wrote both sinks' `stopped` health, then
`close_collector_run` set `stopped_at`; the process exited 0 with
`probe_finished_subscriptions_confirmed`. Observed delivery latency
(`notification_at` to `received_at`) was roughly 0.3 to 1.5 seconds. The
developer verified the run lifecycle, the six health rows, the empty
`reconnection_gaps` result, and the two event counts by SQL, selecting only
counts, timestamps, statuses, reason codes, and IDs.

This rehearsal confirms real subscription creation, sustained multi-hour
transport, real event delivery and idempotent persistence for both sources, the
per-source health state machine on real data, the heartbeat cadence, and clean
multi-source shutdown. It does **not** exercise socket recovery or fresh-session
reconnect (no gap occurred), directed handover (Twitch sent none),
`invalid_notification` recovery, or the storage-failure latch. Live token
refresh also remained untested: `.env.tokens.json` was written once at startup
and never rewritten, so the access token stayed valid for the entire session and
no 401/refresh/retry cycle ran. Windows/WSL sleep and resume during a capture
run are still unverified, as is the merged polling + EventSub process.

## Polling health reason codes

The writer accepts only these combinations for `source = stream_poll`:

| Status | Reason code | Evidence required from the caller |
| --- | --- | --- |
| `starting` | `initializing` | Polling initialization is underway |
| `healthy` | `live_poll_saved` | Live response and required data writes succeeded |
| `healthy` | `offline_poll_saved` | Offline response and any required offline update succeeded |
| `error` | `network_error` | Request failed because of connectivity or timeout |
| `error` | `auth_error` | Required authorization could not be established |
| `error` | `api_error` | Twitch rejected the request or returned unusable data |
| `error` | `poll_stale` | No successful poll within 90 seconds |
| `stopped` | `orderly_shutdown` | Polling was stopped deliberately |

The writer validates the combination without echoing rejected values. EventSub
sources use their separate allowlists above. The collector must enforce
observation timestamps and run lifecycle;
the health INSERT itself enforces neither. A healthy offline response remains
valid polling evidence even if no previous broadcast is known and no stream
update is needed.

Orderly polling-run shutdown writes the run stop and its stopped-health observation
atomically. It leaves the heartbeat unchanged and does not fabricate offline
detection. A rejected stop creates no new health row. The merged runtime uses
`stop_collector_run_multi`, which extends this transaction to one stopped-health
row per active source (`stream_poll`, `raids`, `follows`, and `chat`).

During database failure, the polling runtime emits safe local diagnostics and
stops; automatic recovery remains deferred. Do not claim healthy capture or an exact outage onset based on a
successful API response whose data could not be persisted. Preserve uncertainty
from the last durable evidence. Health INSERTs are append-only without an
idempotency key; avoid automatic retries when commit outcome is unknown.

## Polling runtime

`scripts/collect_stream.py` supports the viewer timeline and its collection
coverage. It reuses the existing table grains and SQL unchanged. No additional
Twitch fields are persisted: stream ID/start time, observation time, and aggregate
viewer count are sufficient for this step. It resolves the target channel once
per process and validates responses before using them.

### Scheduling and ownership

- One coordinator owns `DatabaseWriter`, broadcast lifecycle, and `LiveStatus`.
  One worker at a time performs Twitch requests and token validation/refresh.
  No worker writes to PostgreSQL. There are no overlapping polls or queued retries.
- Poll immediately, then target 60-second slots. Skip missed slots and issue at
  most one new poll when available. A failed request is retried at a later polling
  slot; blocked authorization stops the process. HTTP rate limits and server
  failures are API errors and do not trigger token refresh.
- The coordinator checks about every 250 ms while idle, including when a Twitch
  request is pending. Heartbeats target 30 seconds and prove a process check-in,
  not successful polling. Database operations still block the coordinator; the
  existing statement/lock timeouts limit normal waits. These are not hard
  real-time scheduling guarantees.
- The 90-second stale boundary applies to live and offline polling. Before the
  first success, measure from run start. Record stale once per stale episode;
  record each accepted success and each processed request failure separately.
  Health timestamps describe when evidence was assessed, not an exact outage onset.
- Timestamp observations in the worker after the successful response is parsed,
  before database persistence. Never substitute the later insertion timestamp.
  Discard results from operations taking 90 seconds or longer, results already
  90 seconds old, and results spanning a detected clock gap. This conservative
  rule can discard a legitimate observation following slow authentication.
- Persist required data before healthy evidence and eligibility. Check clocks and
  freshness again after database writes. A durable snapshot can remain even when
  delayed/failed health persistence prevents current eligibility.

### Clocks and eligibility

The runtime uses Linux `CLOCK_BOOTTIME` when available, with a monotonic fallback,
and compares its progress with aware UTC timestamps. Python documents BOOTTIME as
including system suspend time ([Python time documentation](https://docs.python.org/3/library/time.html#time.CLOCK_BOOTTIME));
that does not prove how Windows suspending this WSL VM behaves.

A coordinator gap of at least 90 seconds on either clock, or a difference of at
least five seconds between clock advances, invalidates eligibility. Discard any
pending result, force token validation, and request a fresh poll when the worker
is available. The five-second tolerance avoids reacting to tiny sampling/skew
differences; smaller discontinuities may go undetected. If fewer than 90 seconds
have passed, a clock discrepancy is a local diagnostic, not an invented
`poll_stale` observation.

UTC can be adjusted independently of elapsed time. A clock-only measurement on
the development machine observed UTC stepping backward by approximately 1.74
seconds while elapsed time continued forward. The initial guard stopped on any
negative delta; it now distinguishes a small UTC correction from an unusable clock.

For a UTC rollback of less than five seconds, immediately invalidate eligibility,
discard pending results, and pause new database writes. Sample every 250 ms for
at most five seconds, waiting for real UTC to reach the last coordinator UTC
sample. On recovery, force token validation and a fresh poll. Never clamp timestamps
to previous values, invent replacement timestamps, or accept an observation that
spanned the correction. A data write may have committed before the correction
was detected; this does not restore eligibility. Small correction pauses appear
in local diagnostics; they do not invent `poll_stale` health before the 90-second
boundary.

A UTC rollback of five seconds or more, failure to catch up within five seconds,
or any backward elapsed-clock step stops the process. Diagnostic codes distinguish
UTC rollback, recovery timeout, and elapsed-clock rollback. Check the system clock
before restarting after these failures. No system time-synchronization settings
are changed by the collector.

Freshness uses the larger of UTC and elapsed age. No code can run while the PC/VM
is suspended; detection happens after execution resumes. Neither clock checks nor
healthy records guarantee complete coverage. Actual Windows sleep/resume behavior
has not yet been verified.

### Recurring WSL clock corrections

The first completed live rehearsal saved seven polls and shut down orderly, but
six UTC corrections caused extra polls at roughly 30-second intervals. Read-only
diagnostics found Hyper-V implicit time synchronization enabled alongside active
`systemd-timesyncd`, a 32-second NTP polling interval, and approximately -1.84 seconds
of reported NTP offset. This suggested competing synchronization sources.

[Ubuntu's WSL time synchronization guidance](https://ubuntu.com/wsl/docs/stable/explanation/time-sync/)
recommends disabling `systemd-timesyncd` on Ubuntu 24.04 when using the default
Windows/Hyper-V synchronization. With the collector stopped, the local administrator
can apply that configuration using `sudo systemctl disable --now systemd-timesyncd.service`.
Keep Windows time synchronization enabled. The local administrator applied this
change on September 6, 2026; service checks confirmed the NTP client inactive and
disabled, with Hyper-V synchronization still enabled. A subsequent 90-second,
1,789-sample clock-only check recorded no UTC or elapsed-clock rollbacks, with
approximately 16 microseconds of difference between total UTC and elapsed progress.
This supports the competing-synchronization diagnosis and resolves the recurring
steps during that test window. It does not establish absolute UTC accuracy,
long-run stability, or sleep/resume behavior. A subsequent three-minute offline
rehearsal completed with three polls approximately 60 seconds apart, no reported
clock corrections, and orderly shutdown. The developer's SQL inspection confirmed
the saved run lifecycle and five expected health observations. Sustained live
collection after the synchronization change remains unverified. The collector
itself never changes system services.

### Lifecycle, failures, and shutdown

Keep the last persisted live broadcast separate from eligibility. Request failures
and stale health close eligibility but retain that broadcast for a later successful
offline detection. A changed stream ID replaces the tracked broadcast without
assigning an offline timestamp to its predecessor. An offline response with no
tracked broadcast is healthy polling and requires no stream update.

Every restart creates a new run. It does not query production rows to reconstruct
prior state. Therefore a broadcast seen only before restart may retain a NULL
offline timestamp indefinitely. A subsequent live observation of the same stream
uses the existing conflict-safe INSERT and preserves its original timestamps.

Any storage failure stops database work immediately. Do not reconnect, retry a
health INSERT, recreate an uncertain run, or attempt an orderly stop on a failed
connection. Raw observations may have committed before a later health failure;
unknown commit outcomes stay unknown. Local diagnostics identify failure detection,
while database coverage remains uncertain from the last durable evidence. New
healthy evidence after manual restart does not fill the intervening gap.

Ctrl+C/SIGTERM or the optional duration limit requests orderly shutdown. Stop
dispatching polls, close eligibility, wait for in-flight Twitch/token work to end,
discard its result, then atomically write the run stop and polling stopped health.
The worker cannot be forcibly cancelled safely during token rotation. The existing
20-second request timeout covers blocking operations, not the entire sequence of
validation, refresh, and polling ([Python urllib documentation](https://docs.python.org/3/library/urllib.request.html#urllib.request.urlopen)).
A stalled operation can delay process exit; do not launch another token writer
until the old process has exited. Fatal exits do not claim orderly shutdown, and
shutdown never marks a broadcast offline. An uncertain shutdown commit is not retried.

Automated tests use fake clocks, synthetic responses, a real worker thread, and
session-temporary PostgreSQL tables. They cover failure/recovery transitions,
staleness while requests are pending, clock gaps, write ordering/failures,
shutdown, and -- with a real loopback WebSocket -- the merged EventSub chat,
raid, and follow capture alongside polling. Live Twitch polling, sustained
collection, actual sleep/resume, and the merged runtime against Twitch are still
operational checks to perform. Follow/raid ↔ stream association remains a
separate analytical decision.
