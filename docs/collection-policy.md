# Initial collection policy

This policy supports within-stream chat activity and viewer timelines without
silently assigning messages through known collection uncertainty. A polling-only
runtime now implements the polling portion. EventSub collection remains pending;
the policy and tests do not establish real-world capture completeness.

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
- Database transactions cover live observations, run operations, and polling
  health writes. The polling runtime schedules heartbeats, handles shutdown
  signals, and records polling health. Database recovery, EventSub delivery, and
  EventSub source health remain unimplemented.

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
sources remain unsupported by this method until their readiness contracts are
defined. The collector must enforce observation timestamps and run lifecycle;
the health INSERT itself enforces neither. A healthy offline response remains
valid polling evidence even if no previous broadcast is known and no stream
update is needed.

Orderly polling-run shutdown writes the run stop and its stopped-health observation
atomically. It leaves the heartbeat unchanged and does not fabricate offline
detection. A rejected stop creates no new health row. This transaction must be
extended for other active sources before use in a full EventSub collector.

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
`poll_stale` observation. Backward UTC or elapsed time stops the process without
fabricating increasing timestamps. Check the system clock before restarting.

Freshness uses the larger of UTC and elapsed age. No code can run while the PC/VM
is suspended; detection happens after execution resumes. Neither clock checks nor
healthy records guarantee complete coverage. Actual Windows sleep/resume behavior
has not yet been verified.

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
staleness while requests are pending, clock gaps, write ordering/failures, and
shutdown. Live Twitch polling, sustained collection, and actual sleep/resume are
still operational checks to perform. Chat, incoming raids, follows, and their
readiness/association rules remain separate implementation work.
