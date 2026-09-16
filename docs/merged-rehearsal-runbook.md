# Merged collector rehearsal runbook

Current operational checklist for running the merged `scripts/collect_stream`
against Twitch — viewer polling **and** EventSub chat + raid + follow capture in
one process and one `collector_run`. It also preserves the acceptance criteria
used for the first completed full-stream rehearsal.

Design: [merged-collector-design.md](merged-collector-design.md).
Health-code contract: [collection-policy.md](collection-policy.md#eventsub-capture-runtime).
Milestone 1 release evidence: [milestone-1-release-handoff.md](milestone-1-release-handoff.md).
The standalone raids+follows runbook is
[eventsub-rehearsal-runbook.md](eventsub-rehearsal-runbook.md); this file supersedes
it for the merged path.

Normal scope: `stream_poll` + `raids` + `follows` + `chat`. The `--no-chat`
three-source mode is retained for bounded diagnosis, not as the normal product
collection path.

---

## 1. Pre-flight (before the stream)

| Check | How | Expected |
| --- | --- | --- |
| Working tree state | `git status` | clean; local `main` synchronized with `origin/main` |
| Tests | `STREAM_PULSE_TEST_POSTGRES=1 .venv/bin/python -m unittest discover -s tests -q` | `Ran 348 tests ... OK`, exit 0 |
| Core schema applied | `psql -d stream_pulse -tAc "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('viewer_snapshots','streams','chat_messages','incoming_raids','follow_events','collector_runs','collection_health','reconnection_gaps');"` | all eight listed (001–010 applied) |
| Milestone 1 schema applied | `psql -d stream_pulse -tAc "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('collector_run_capabilities','collector_run_streams','chatter_presence_snapshots','chatter_presence_members','stream_metadata_history','raid_source_context');"` | all six listed (011–015 applied) |
| `chat_messages` shape | `psql -d stream_pulse -tAc "SELECT column_name FROM information_schema.columns WHERE table_name='chat_messages' AND column_name IN ('eventsub_message_id','chat_message_id','message_type','reply_parent_message_id','reply_parent_user_id','badges','context_complete');"` | all seven listed (010 and 013 applied) |
| `paused` health status | `psql -d stream_pulse -tAc "SELECT 1 FROM pg_constraint WHERE conname LIKE '%collection_health%status%';"` | constraint present (008); `paused` accepted |
| No other token writer | `ps aux \| grep -E "collect_stream\|collect_eventsub\|check_eventsub\|authorize_twitch\|check_twitch_access" \| grep -v grep` | no rows |
| PC / VM stays awake | Windows power settings; keep WSL terminal open | no sleep during stream |
| Clock sync | `systemctl is-active systemd-timesyncd` | `inactive` (disabled Sept 6, 2026; Hyper-V sync stays on) |
| Network | channel reachable in browser | stream is live |
| Pre-run counts (for single-run deltas) | see §6d | record `follow_events`, `incoming_raids`, `chat_messages` totals now |

Do **not** rerun any `sql/00*.sql` schema file. Do **not** start
`collect_eventsub.py` or a probe alongside this — shared token file, one token
writer rule.

---

## 2. Start sequence

From the repository root, in a kept-open WSL terminal:

```bash
set -o pipefail
.venv/bin/python -m scripts.collect_stream 2>&1 | tee -a .env.collector.log
# 3-source first pass instead:
# .venv/bin/python -m scripts.collect_stream --no-chat 2>&1 | tee -a .env.collector.log
```

- No `--duration`: runs until Ctrl+C / SIGTERM (what we want for a full stream).
- Add `--duration N` only for a short bounded smoke test.
- `.env.collector.log` is git-ignored; timestamped diagnostic codes only, no
  identities or payloads.

Leave it in the foreground so Ctrl+C is the clean stop. If it must run detached,
stop it with SIGTERM (never SIGKILL).

### Expected healthy startup ordering

```
<ts> initializing                        # x5 with presence: stream_poll, raids, follows, chat, chatter_presence
<ts> live_poll_saved  (or offline_poll_saved)   # first poll persisted
<ts> session_welcome_received
<ts> session_responsive
<ts> raids_subscription_enabled
<ts> follows_subscription_enabled
<ts> chat_subscription_enabled
<ts> capture_ready                       # x2: raids, follows (after transport + first reconcile)
<ts> awaiting_stream_status  ->  capture_ready   # chat: once the first poll is mirrored in
```

The polling half's first poll runs immediately; EventSub setup follows. Chat's
resting health is **polling-driven**: it sits at `starting/awaiting_stream_status`
until the first accepted poll is mirrored onto the chat sink, then `capture_ready`
while the stream is observed live. A quiet channel sits at `capture_ready`
indefinitely — that is success, not a stall.

Keepalives (~every 30 s) and heartbeats (~every 30 s) are silent unless they slip.

---

## 3. Diagnostic codes to watch

### Normal / expected

| Code | Meaning |
| --- | --- |
| `initializing` | a source health row `starting/initializing` written (normally x5 with presence; x4 when unavailable/disabled); the `collector_run` row opens first |
| `session_welcome_received` / `session_responsive` | socket up, first liveness |
| `raids_subscription_enabled` / `follows_subscription_enabled` / `chat_subscription_enabled` | subscription confirmed enabled |
| `capture_ready` | a source reached `healthy/capture_ready` |
| `live_poll_saved` / `offline_poll_saved` | poll persisted + healthy `stream_poll` evidence written |
| `awaiting_stream_status` | chat: subscription ready, no accepted poll yet |
| `offline_observed` | chat: fresh offline poll → chat `paused` (not a fault) |
| `raids_notification_envelope_received` / `follows_notification_envelope_received` / `chat_notification_envelope_received` | first valid envelope for that source |
| `raids_event_stored` / `follows_event_stored` / `chat_event_stored` | new row persisted |
| `raids_event_duplicate_skipped` / `follows_event_duplicate_skipped` / `chat_event_duplicate_skipped` | redelivery skipped by message key — normal |
| `chat_message_outside_eligibility_discarded` | a chat message fell outside observed-live eligibility → dropped, not stored, **not an error** |
| `keepalive_waiting_within_grace` → `liveness_received_within_grace` | keepalive a bit late, then recovered — tolerated |

### Transient — watch, act only if it persists

| Code | Meaning | Action |
| --- | --- | --- |
| `clock_gap_fresh_poll_required` | ClockGuard saw a ≥90 s gap or ≥5 s UTC/elapsed divergence; forces a fresh poll **and** tears down + rebuilds the EventSub session | let `on_gap` cascade run; note the time |
| `utc_clock_rollback_waiting` → `utc_clock_recovered_fresh_poll_required` | small (<5 s) UTC correction; writes paused, catches up, resumes | wait ≤5 s |
| `probe_gap_detected_no_replay` | unexpected socket loss; a `reconnection_gaps` row opens | let recovery run; note the time |
| `network_error` / `keepalive_timeout` | EventSub transport error health for each ready source | expect recovery to follow |
| `reconnect_wait_<n>_seconds` | fresh-session backoff (1,2,4,8,16,30) | wait |
| `reconnect_connecting` / `handover_connecting` | reconnect attempt in flight | wait |
| `handover_complete_subscriptions_preserved` | Twitch-directed reconnect done cleanly (**not** a gap) | none |
| `probe_subscriptions_reconfirmed_after_gap` | recovery succeeded; gap row resolves on next observe | none |
| `session_welcome_received` (again) + `*_subscription_enabled` (again) | fresh session rebuilt after a gap | none |
| `poll_failed` | a failed poll closed chat eligibility; `stream_poll` also records `network_error`/`api_error`/`auth_error` | one failed poll is retried next slot |
| `poll_stale` | no accepted poll in 90 s; chat eligibility closed | expect a later `live_poll_saved` to clear it |
| `saved_poll_no_longer_fresh` | snapshot persisted but too old to grant eligibility | conservative discard; no action |

A gap that recovers should show `probe_gap_detected_no_replay` … `reconnect_*` …
`session_welcome_received` … `*_subscription_enabled` … `probe_subscriptions_reconfirmed_after_gap`
and `capture_ready` again per source. The `reconnection_gaps` row gets
`recovered_at` filled on the first tick a subscription is live again.

### Bad — check with the operator before acting

| Code | Meaning | Likely action |
| --- | --- | --- |
| `raids_notification_invalid_<reason>` / `follows_notification_invalid_<reason>` / `chat_notification_invalid_<reason>` | an event body failed validation; that source health goes `error/invalid_notification` and holds until a later valid event | capture the `<reason>` verbatim; do **not** restart reflexively — one bad event does not stop the run. Investigate after. |
| `capture_ready` never appears for a source | subscription not enabled / transport not confirmed | check earlier `*_subscription_*` / `*_http_<status>` lines |
| `*_subscription_error_http_<status>` | subscription POST rejected (409, 401, 5xx) | one bad source does not block the others; note status; re-auth if 401 recurs |
| `subscription_revoked` | Twitch revoked a subscription (sticky for the run) | stop after stream; investigate scopes / channel role |
| `auth_error` / `authorization_blocked_restart_required` | token/authorization failure | stop; a second token writer may be running, or re-auth needed. **Do not** run `authorize_twitch` while the collector is up. |
| `clock_uncertain` | (EventSub side) UTC/elapsed disagreement inside the probe that ClockGuard did not absorb | should be rare now that ClockGuard owns clock gaps; note it |
| `utc_clock_rollback_restart_required` / `utc_clock_recovery_timeout_restart_required` / `elapsed_clock_rollback_restart_required` | unrecoverable clock fault; process ends, run left **open** | check system clock before restart |
| `capture_storage_failure_stop_required` | an EventSub-side DB write (event, health, gap, heartbeat) raised `StorageError` | loop latches, process stops, run left **open** on purpose. See §5. |
| `storage_failure_restart_required_commit_may_be_uncertain` | polling-side storage failure; last commit outcome unknown | §5 |
| `worker_failed_restart_required` / `collector_failure_restart_required` / `collector_startup_failed_restart_required` / `database_startup_failed_restart_required` | unexpected failure (details suppressed by design) | restart once; if it repeats, stop and investigate |

### Shutdown (clean)

```
<ts> shutdown_waiting_for_twitch          # polls stopped, chat eligibility closed, waiting on in-flight work
                                          #   (only if a Twitch request was in flight; `exit_waiting_for_twitch` is the finally-block variant)
<ts> probe_waiting_for_token_worker
<ts> probe_socket_closed
<ts> orderly_shutdown                     # ONE log line; stop_collector_run_multi closes every active source atomically
<ts> probe_finished_subscriptions_confirmed   # (_after_gap if a gap occurred)
```

Exit 0 = orderly (`stop_collector_run_multi` wrote the run stop and every active
source's `stopped/orderly_shutdown` row atomically — normally five with chatter
presence configured; verify the rows in §6b, not the log).
Exit 1 **with** a `*_storage_failure_*` / `*_restart_required` code and **no**
`orderly_shutdown` line = run left open (§5). Exit 1 after a clock fault = run
left open, check the clock.

---

## 4. Live adjustments

- **Clean crash / non-zero exit, `orderly_shutdown` present:** safe to relaunch.
  Every start is a new `collector_run`; restart does not repair the old run.
- **Crash mid-stream, transient cause** (network blip that didn't self-recover):
  relaunch; the gap between runs is real lost coverage — expect an unresolved
  `reconnection_gaps` row on the old run plus a plain time gap between run stop
  and the new run start.
- **`on_gap` cascade fired but recovered** (`clock_gap_fresh_poll_required` then
  a fresh poll and `probe_subscriptions_reconfirmed_after_gap`): no action, note
  the window for the recap.
- **Anything non-obvious** (repeated `*_invalid_*`, repeated auth errors, storage
  failure, clock fault): stop and check with the operator before relaunching.
- Never launch a second `collect_stream`, `collect_eventsub`, or any probe / auth
  helper while one is running.

---

## 5. Storage-failure state (run left open)

On `capture_storage_failure_stop_required` or
`storage_failure_restart_required_commit_may_be_uncertain`:

- The collector makes no further DB writes, does not retry the uncertain commit,
  does not write source `stopped` health, and **skips** `stop_collector_run_multi`.
- The `collector_run` row therefore has `stopped_at` NULL, possibly forever.
- A raw snapshot or event row may exist even though its health row write failed;
  an uncertain commit may or may not have landed. Preserve that uncertainty — do
  not reconstruct or backfill.
- Recovery is manual and deferred by design. After the stream, with the operator:
  inspect the open run, decide whether to leave it open as an honest coverage
  record, and only then consider restarting.

---

## 6. Post-run verification (SQL)

Run after the collector has exited. **Privacy:** select only counts, timestamps,
statuses, reason codes, and `run_id` / `gap_id` / `health_id`. Never select
`user_id`, `chatter_user_id`, `from_broadcaster_user_id`, `stream_id`,
`eventsub_message_id`, `chat_message_id`, `message_text`, `message_fragments`, or
any identity. No `SELECT *` on the event tables.

All the `inspect_*` files below are hand-run with `psql -f … -v …` — `psql -c`
does **not** interpolate `:vars`.

### Historical release procedure for migrations 011–015

Migrations 011–015 were applied September 16, 2026. Do not rerun them against
the initialized database. For historical release context, the procedure first
confirmed no collector, probe, access check, or authorization helper was running
and created a restricted private recovery artifact outside the repository:

```bash
backup_path="$(mktemp /tmp/stream-pulse-pre-m1-XXXXXX.dump)"
chmod 600 "$backup_path"
pg_dump --format=custom --file="$backup_path" stream_pulse
pg_restore --list "$backup_path" >/dev/null
```

The dump contains private production data: do not commit, attach, or share it.
The release applied only migrations 011–015, once and in order, with
`ON_ERROR_STOP=1`, then reauthorized with `moderator:read:chatters` while the
collector was stopped. A token without that optional scope records presence as
`failed_to_initialize/missing_scope` while core collection continues.

The release ran this privacy-safe dry run before historical bridge derivation:

```bash
psql -X -d stream_pulse -f sql/analysis/inspect_historical_run_stream_derivation.sql
```

Only after reviewing its ambiguous count was the idempotent derivation query
run. It never fills raw `run_id`; derived bridge rows stay labeled.

### 6a. Run lifecycle

```bash
psql -d stream_pulse -f sql/queries/inspect_latest_collector_run.sql
```

Returns `run_id, started_at, last_heartbeat_at, stopped_at` for the newest run.
Expect: `stopped_at` non-NULL and `last_heartbeat_at` within ~30 s of `stopped_at`
for a clean run; `stopped_at` NULL after a storage failure.

Note the `run_id` — call it `:rid`, and `started_at` / `stopped_at` — call them
`:start` / `:stop` below.

### 6b. Per-source health transitions

```bash
psql -d stream_pulse -v rid=<run_id> -f sql/queries/inspect_collector_run_health.sql
```

Grain: one row per health observation, per source, per run. Rows only on change.
Expected clean sequence:

- `stream_poll`: `starting/initializing` → `healthy/live_poll_saved` (and/or
  `healthy/offline_poll_saved`) → `stopped/orderly_shutdown`
- `raids`, `follows`: `starting/initializing` → `healthy/capture_ready` →
  `stopped/orderly_shutdown`
- `chat`: `starting/initializing` → `starting/awaiting_stream_status` →
  `healthy/capture_ready` → (`paused/offline_observed` if the stream went offline
  before shutdown) → `stopped/orderly_shutdown`
- `chatter_presence`: `starting/initializing` →
  `starting/snapshot_requested` →
  `healthy/snapshot_complete` or `healthy/empty_snapshot_complete` during live
  collection, or `paused/offline_observed` while offline →
  `stopped/orderly_shutdown`

Any `error/*` row in between is a real transient worth explaining in the recap.
`error/invalid_notification` that never returns to `capture_ready` = a source
stuck on a bad event. `error/poll_stale` or `error/poll_failed` on `chat` mirrors
a `stream_poll` problem at the same time.

### 6c. Reconnection gaps

```bash
psql -d stream_pulse -v rid=<run_id> -f sql/queries/inspect_reconnection_gaps.sql
```

Grain: one row per unexpected EventSub transport loss for this run. Columns
`gap_id, detected_at, recovered_at, reason_code`. `recovered_at IS NULL` = capture
never observably recovered before the run ended; `recovered_at - detected_at` is
the detected-to-recovered interval only. It is not an upper bound on the full
outage because actual loss can precede detection by an uncertain amount.
**Zero rows is the expected clean result.** Twitch-directed reconnects
(`handover_complete_subscriptions_preserved`) do not appear here.

### 6d. Event + chat counts

```bash
psql -d stream_pulse -v start='<started_at>' -v stop='<stopped_at>' \
  -f sql/queries/inspect_eventsub_capture_counts.sql
psql -d stream_pulse -v start='<started_at>' -v stop='<stopped_at>' \
  -f sql/queries/inspect_chat_message_count.sql
```

Rows captured after migration 011 have direct `run_id`; historical rows retain
NULL. These compatibility queries still use the
`received_at BETWEEN :'start' AND :'stop'` window so old rehearsals remain
inspectable without pretending their provenance was directly captured.
`null_stream_id` should equal `row_count` for follows/raids (association is
deliberately deferred). `distinct_streams` should be `1` for a single-broadcast
rehearsal (2+ if the stream id rolled mid-run).

For an absolute cross-check against the pre-run totals from §1, subtract those
from a windowless `COUNT(*)`.

### 6e. Cross-check log vs. DB

```bash
grep -cE "follows_event_stored" .env.collector.log
grep -cE "raids_event_stored" .env.collector.log
grep -cE "chat_event_stored" .env.collector.log
grep -cE "chat_message_outside_eligibility_discarded" .env.collector.log
grep -cE "_notification_invalid_" .env.collector.log
grep -nE "probe_gap_detected_no_replay|clock_gap_fresh_poll_required|capture_storage_failure|storage_failure_restart_required|subscription_revoked" .env.collector.log
```

`*_event_stored` counts should match the DB row deltas from 6d;
`*_event_duplicate_skipped` and `chat_message_outside_eligibility_discarded`
lines should **not** add rows. Every `_notification_invalid_` /
`probe_gap_detected` / storage / revoked line should correspond to a health
`error/*` row (6b) or a `reconnection_gaps` row (6c).

### 6f. Enhanced collection

```bash
psql -X -d stream_pulse -v rid=<run_id> -f sql/queries/inspect_enhanced_capture.sql
```

This outputs only capabilities, outcome/status counts, timestamps, and safe
reason codes. Confirm no `in_progress` presence or raid-context row remains after
an orderly shutdown. A live verification must still exercise a successful empty
or populated presence snapshot and, when a raid occurs, its context result.

### 6g. Full-stream Milestone 1 gate

The bounded live check does not replace this gate. Start the unchanged production
collector before POLYMATHIC goes live, keep the PC and WSL2 awake, and leave the
same process running through the broadcast. After Twitch reports the channel
offline, wait for `offline_poll_saved` and `offline_observed`, then request clean
shutdown with Ctrl+C. Do not start authorization, an access check, a probe, or a
second collector during the run.

Apply §6a–§6f after exit. The release gate requires:

- a closed collector run and orderly shutdown rows for every active source;
- at least one directly attributed stream and viewer snapshot;
- at least one complete empty or populated chatter-presence snapshot;
- stream metadata history for the observed stream;
- no unexplained source error, unresolved reconnection gap, or unfinished
  enhanced attempt;
- chat/follow/raid results reported honestly according to what occurred. A quiet
  source is not a failure, and raid-source context is live-verified only if an
  incoming raid actually occurs.

Record any live branch that did not occur as unverified rather than manufacturing
an event or weakening the acceptance condition.

---

## 7. What this rehearsal does and does not establish

Establishes (if clean): the merged runtime against real Twitch — one process,
one run, the four core sources plus configured enhanced collection sharing one
clock / heartbeat / writer; real subscription
creation; real transport liveness over a full stream; real viewer polling
alongside EventSub; real chat / raid / follow delivery + idempotent persistence;
observed-live chat eligibility on real timing; the per-source health state
machine (incl. chat's polling-driven states) on real data; and clean shutdown of
every active source via `stop_collector_run_multi`.

Establishes **only if they actually occur**: socket recovery / fresh-session
reconnect, directed handover, the `on_gap` clock cascade, `invalid_notification`
recovery, live token refresh (401 → refresh → retry), and the storage-failure
latch.

Does **not** establish: Windows/WSL sleep/resume behaviour, or long-run stability
beyond this session. `healthy` never proves complete capture.
