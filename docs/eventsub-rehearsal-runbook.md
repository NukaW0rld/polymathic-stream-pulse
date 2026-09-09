# EventSub capture rehearsal runbook

Operational checklist for the first live run of `scripts/collect_eventsub` against
Twitch. This is the first time the capture collector touches real Twitch calls,
real subscriptions, the real local database, and possibly the private token file.
The polling collector's rehearsals are separate; see
[collection-policy.md](collection-policy.md#eventsub-capture-runtime) for the design.

Scope of this run: incoming raids + follows only. No chat, no stream-status
polling. This is **not** the full first-collection scope.

---

## 1. Pre-flight (before the stream)

| Check | How | Expected |
| --- | --- | --- |
| Working tree state | `git status` | clean; branch ahead of origin by local commits, nothing pushed |
| Tests | `STREAM_PULSE_TEST_POSTGRES=1 .venv/bin/python -m unittest discover -s tests -q` | `Ran 243 tests ... OK`, exit 0 |
| Schema applied | `psql -d stream_pulse -tAc "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('reconnection_gaps','follow_events','incoming_raids','collector_runs','collection_health');"` | all five listed (009 + 008 already applied) |
| No other token writer | `ps aux \| grep -E "collect_stream\|collect_eventsub\|check_eventsub\|authorize_twitch\|check_twitch_access" \| grep -v grep` | no rows |
| PC / VM stays awake | Windows power settings; keep WSL terminal open | no sleep during stream |
| Clock sync | `systemctl is-active systemd-timesyncd` | `inactive` (disabled Sept 6, 2026; Hyper-V sync stays on) |
| Network | channel reachable in browser | stream is live |

Do **not** rerun any `sql/00*.sql` schema file. Do **not** start the polling
collector — the two cannot run at once (shared token file, one token writer rule).

---

## 2. Start sequence

From the repository root, in a kept-open WSL terminal:

```bash
set -o pipefail
.venv/bin/python -m scripts.collect_eventsub 2>&1 | tee -a .env.collector.log
```

- No `--duration`: runs until Ctrl+C / SIGTERM (what we want for a full stream).
- Add `--duration N` only for a short bounded smoke test.
- `.env.collector.log` is git-ignored; it holds only timestamped diagnostic codes,
  no identities or payloads.

Leave it in the foreground so Ctrl+C is the clean stop. If it must run detached,
use SIGTERM (not SIGKILL) to stop it.

### Expected healthy startup ordering

```
<ts> capture_run_started
<ts> initializing                       # x2: one per source (raids, follows)
<ts> session_welcome_received
<ts> session_responsive
<ts> raids_subscription_enabled
<ts> follows_subscription_enabled
<ts> capture_ready                      # x2: once per source, after transport + first reconcile
```

`capture_ready` for a source means: subscription enabled, transport responsive,
and either a write has succeeded or the channel is simply quiet. A quiet channel
sits at `capture_ready` indefinitely — that is success, not a stall.

Keepalives (~every 30 s) are silent unless they slip into the grace window.

---

## 3. Diagnostic codes to watch

### Normal / expected

| Code | Meaning |
| --- | --- |
| `capture_run_started` | collector run row opened |
| `initializing` | source health row `starting/initializing` written |
| `session_welcome_received` / `session_responsive` | socket up, first liveness |
| `raids_subscription_enabled` / `follows_subscription_enabled` | subscription confirmed enabled |
| `capture_ready` | source health `healthy/capture_ready` |
| `raids_notification_envelope_received` / `follows_notification_envelope_received` | first valid envelope for that source (once per source) |
| `raids_event_stored` / `follows_event_stored` | new row persisted |
| `raids_event_duplicate_skipped` / `follows_event_duplicate_skipped` | redelivery skipped by message key — normal |
| `keepalive_waiting_within_grace` → `liveness_received_within_grace` | keepalive arrived a bit late, then recovered — tolerated |

### Transient — watch, act only if it persists

| Code | Meaning | Action |
| --- | --- | --- |
| `probe_gap_detected_no_replay` | unexpected socket loss; a `reconnection_gaps` row is opened | let recovery run; note the time |
| `network_error` / `keepalive_timeout` | transport error health row for each ready source | expect recovery to follow |
| `reconnect_wait_<n>_seconds` | backoff between fresh-session attempts (1,2,4,8,16,30) | wait |
| `reconnect_connecting` / `handover_connecting` | reconnect attempt in flight | wait |
| `handover_complete_subscriptions_preserved` | Twitch-directed reconnect done cleanly (not a gap) | none |
| `probe_subscriptions_reconfirmed_after_gap` | recovery succeeded; gap row will be resolved on next observe | none |
| `session_welcome_received` (again) + `*_subscription_enabled` (again) | fresh session rebuilt after a gap | none |

A gap that recovers should show `probe_gap_detected_no_replay` … `reconnect_*` …
`session_welcome_received` … `*_subscription_enabled` … `probe_subscriptions_reconfirmed_after_gap`,
and `capture_ready` again per source. The `reconnection_gaps` row gets its
`recovered_at` filled on the first tick where a subscription is live again.

### Bad — check with the operator before acting

| Code | Meaning | Likely action |
| --- | --- | --- |
| `raids_notification_invalid_<reason>` / `follows_notification_invalid_<reason>` | an event body failed validation; source health goes `error/invalid_notification` and holds until a later valid event | capture the `<reason>` suffix verbatim; do **not** restart reflexively — a single malformed event does not stop the run. Investigate after. |
| `capture_ready` never appears for a source | subscription not enabled / transport not confirmed | check earlier `*_subscription_*_http_<status>` lines |
| `raids_subscription_error_http_<status>` / `follows_subscription_error_http_<status>` | subscription POST rejected (e.g. 409, 401, 5xx) | one bad source does not block the other; note status; may need re-auth if 401 recurs |
| `subscription_revoked` | Twitch revoked a subscription (sticky for the run) | stop after stream; investigate scopes / channel role |
| `auth_error` / `capture_authorization_or_lookup_failed_restart_required` | token/authorization failure | stop; a second token writer may be running, or re-authorization needed. **Do not** run `authorize_twitch` while the collector is up. |
| `clock_uncertain` | UTC/elapsed disagreement ≥ 5 s or a backward step | process ends; check system clock before restart |
| `capture_storage_failure_stop_required` | a DB write (event, health, gap, or heartbeat) raised `StorageError` | the loop latches and stops; run is left **open** on purpose. See §5. |
| `capture_storage_failure_restart_required_commit_may_be_uncertain` | storage failure surfaced at collect() level; last commit outcome unknown | §5 |
| `capture_run_close_failed_restart_required` | session ended clean but the final `close_collector_run` UPDATE failed | run may be open; §5 |
| `probe_internal_error` / `capture_startup_failed_restart_required` | unexpected exception (details suppressed by design) | restart once; if it repeats, stop and investigate |

### Shutdown (clean)

```
<ts> probe_waiting_for_token_worker
<ts> orderly_shutdown          # x2: sink stopped health per source
<ts> probe_socket_closed
<ts> probe_finished_subscriptions_confirmed        # (or _after_gap if a gap occurred)
<ts> capture_run_closed
```

Exit 0 = orderly. Exit 1 with `capture_run_closed` = run closed cleanly but
readiness was not confirmed at the instant of exit (e.g. stopped mid-gap) — the
run row is still properly closed; only the exit status flags the unconfirmed
state. Exit 1 with one of the `capture_storage_failure_*` / `*_restart_required`
codes and **no** `capture_run_closed` = run left open (§5).

---

## 4. Live adjustments

- **Crash / non-zero exit, run was closed** (`capture_run_closed` present): safe to
  relaunch. Every start is a new `collector_run`; restart does not repair the old
  run. Expect a fresh run_id.
- **Crash mid-stream, transient cause** (network blip that didn't self-recover):
  relaunch; the gap between runs is real lost coverage and is expected to show as
  an unresolved `reconnection_gaps` row on the old run plus a plain time gap
  between run stop and the new run start.
- **Anything non-obvious** (repeated `*_invalid_*`, repeated auth errors, storage
  failure, clock failure): stop and check with the operator before relaunching.
- Never launch a second `collect_eventsub`, the polling collector, or any probe /
  auth helper while one is running.

---

## 5. Storage-failure state (run left open)

On `capture_storage_failure_*`:

- The collector makes no further DB writes, does not retry the uncertain commit,
  does not write source `stopped` health, and does not call `close_collector_run`.
- The `collector_run` row therefore has `stopped_at` NULL, possibly forever.
- A raw event row may exist even though its health row write failed; an uncertain
  commit may or may not have landed. Preserve that uncertainty — do not
  reconstruct or backfill.
- Recovery is manual and deferred by design. After the stream, with the operator:
  inspect the open run, decide whether to leave it open as an honest coverage
  record, and only then consider restarting.

---

## 6. Post-run verification (SQL)

Run after the collector has exited. **Privacy:** select only counts, timestamps,
statuses, reason codes, and run/gap/health IDs. Never select `user_id`,
`from_broadcaster_user_id`, `stream_id`, `eventsub_message_id`, or any identity.
Do not `SELECT *` on `follow_events` / `incoming_raids`.

`psql -d stream_pulse`

### 6a. Run lifecycle — reuse existing query

`sql/queries/inspect_latest_collector_run.sql` already returns
`run_id, started_at, last_heartbeat_at, stopped_at` for the newest run. Use it
as-is. Expect: `stopped_at` non-NULL and `last_heartbeat_at` within ~30 s of
`stopped_at` for a clean run; `stopped_at` NULL after a storage failure.

Note the `run_id` it returns — call it `:rid` below.

### 6b. Per-source health transitions

`sql/queries/inspect_collector_run_health.sql` has a hard-coded `run_id = 3`
(a learning-check artifact). You own this file — either edit the literal to the
new run_id for this check, or parameterise it. Grain: one row per health
observation for one source in one run.

Columns to keep: `health_id, source, observed_at, status, reason_code`.
Order: `observed_at ASC, health_id ASC`.

Expected clean-run sequence per source (`raids`, `follows`), rows only on change:

```
starting  / initializing
healthy   / capture_ready
stopped   / orderly_shutdown
```

Any `error/*` row in between is a real transient worth explaining in the recap.
`error/invalid_notification` that never returns to `capture_ready` = a source
stuck on a bad event.

### 6c. Reconnection gaps — new query for you to write

- **File:** `sql/queries/inspect_reconnection_gaps.sql` (new; you own it)
- **Table:** `reconnection_gaps`
- **Grain:** one row per unexpected EventSub transport loss per run.
- **Input:** the run_id (literal or `%(run_id)s` — runtime `.sql` files use
  unquoted `%(name)s` and are loaded with `.read_text()`; a pure inspection file
  you run by hand in `psql` can use a literal).
- **Select:** `gap_id, detected_at, recovered_at, reason_code`.
- **Filter:** `WHERE run_id = <rid>`.
- **Order:** `detected_at ASC, gap_id ASC`.
- **Interpretation:** `recovered_at IS NULL` = capture never observably recovered
  before the run ended (unrecovered gap). `recovered_at` set = coverage resumed;
  `recovered_at - detected_at` is an upper bound on the outage (detection lags
  onset by up to ~one keepalive interval). Zero rows for a clean run is the
  expected result.

### 6d. Event counts — new query (or two) for you to write

- **File:** `sql/queries/inspect_eventsub_capture_counts.sql` (new; you own it)
- **Tables:** `follow_events`, `incoming_raids`
- **Goal:** `COUNT(*)` per table, nothing else. No identities, no per-row output.
- **Optional, still safe:** `MIN(received_at)`, `MAX(received_at)`,
  `MIN(notification_at)`, `MAX(notification_at)` to bound the capture window;
  `COUNT(*) FILTER (WHERE stream_id IS NULL)` should equal `COUNT(*)`
  (stream association is deliberately deferred, so every row is NULL).
- These tables are not run-scoped (no `run_id` column), so a count is cumulative
  across every capture run so far. For a single-run delta, compare against the
  counts you record **before** starting this run.
- **Sanity checks:** `follow_events` count ≈ number of `follows_event_stored`
  lines in the log; `incoming_raids` count ≈ `raids_event_stored` lines;
  `*_event_duplicate_skipped` lines should **not** add rows.

### 6e. Cross-check log vs. DB

From `.env.collector.log` for this run's window:

```bash
grep -cE "follows_event_stored" .env.collector.log
grep -cE "raids_event_stored" .env.collector.log
grep -cE "_notification_invalid_" .env.collector.log
grep -nE "probe_gap_detected_no_replay|capture_storage_failure|subscription_revoked" .env.collector.log
```

Stored-line counts should match the DB row deltas from 6d; invalid/gap/failure
lines should each correspond to a health `error/*` row (6b) or a
`reconnection_gaps` row (6c).

---

## 7. What this rehearsal does and does not establish

Establishes (if clean): real subscription creation, real transport liveness over a
full stream, real event delivery + idempotent persistence, per-source health
transitions on real data, and — only if a gap actually occurs — real socket
recovery and gap coverage.

Does not establish: chat capture (not in scope), merged polling+EventSub process,
Windows/WSL sleep/resume behaviour, or long-run stability beyond this session.
`healthy` never proves complete capture.
