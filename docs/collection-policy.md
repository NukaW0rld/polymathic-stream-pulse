# Initial collection policy

This policy supports within-stream chat activity and viewer timelines without
silently assigning messages through known collection uncertainty. It is agreed
policy, not evidence that a running collector exists.

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
the eventual runtime must account for suspend/resume on WSL rather than assuming
that its elapsed clock includes every period of Windows sleep.

## Remaining integration requirements

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
- Database transactions now cover live observations, run operations, and polling
  health writes. Heartbeat scheduling, shutdown signal/recovery handling, EventSub
  delivery, and automatic health transitions remain unimplemented.

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

During database failure, safe local diagnostics and later recovery handling are
still required. Do not claim healthy capture or an exact outage onset based on a
successful API response whose data could not be persisted. Preserve uncertainty
from the last durable evidence. Health INSERTs are append-only without an
idempotency key; avoid automatic retries when commit outcome is unknown.
