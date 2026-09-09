# Merged polling + EventSub collector — design

Design for folding the standalone EventSub capture runtime into the polling
collector so one process runs viewer polling, live-status, raids, follows, and
chat under a single `collector_run`. This was the contract to review before
implementation; the sections below are annotated with what was built. The code
lives in `scripts/collect_stream.py`, `scripts/event_sink.py`,
`scripts/eventsub_router.py`, and `scripts/eventsub_capture.py`.

Decision (implemented): **one cooperative loop** (Option C). The EventSub work is
folded into the polling collector's tick rather than run on a second thread, so
the coordinator stays the sole owner of the database, `LiveStatus`, and the
clock — no writer locks, one heartbeat, one clock authority.

Related: [collection-policy.md](collection-policy.md) (the source-of-truth for
health reason codes and the EventSub contract),
[eventsub-rehearsal-runbook.md](eventsub-rehearsal-runbook.md).

Status: Phases 0–2 and 4 built and tested (synthetic + PostgreSQL only). Phase 3
is the first live rehearsal, planned for Sunday September 13, 2026; it has not
happened. The merged runtime has **not** run against Twitch.

---

## 1. Shape

**Entry point:** `scripts/collect_stream.py` keeps its name. `main()` keeps
`--duration` and SIGINT/SIGTERM → `stop.set()`, but constructs one merged
`Collector` wired to both the `PollWorker` and the EventSub pieces.
`scripts/collect_eventsub.py` stays as an isolated-EventSub test tool.

**One class** (working name `Collector`, superseding `PollingCollector`) owns:

| Owns | Notes |
| --- | --- |
| `DatabaseWriter` | sole caller, single-threaded, no locks |
| `LiveStatus` | polling writes it; the chat sink reads it (Phase 4) |
| `TokenManager` | shared; `PollWorker` and the EventSub `SetupWorker` both call it (already `RLock`-safe) |
| `run_id` | one run for every source |
| `ClockGuard` | the single clock authority (Phase 1) |
| polling half | `PollWorker`, `tracked_stream`, `_next_poll`, `_generation`, `_last_success`, `_discard_job`, `_force_validation` — unchanged from `PollingCollector` |
| EventSub half | socket, `SessionReadiness`, `EventRouter` + `RaidSink`/`FollowSink` (+ `ChatSink` later), reconnect/backoff state — reusing `eventsub_recovery.py` |

`collection_health` already accepts `stream_poll`, `chat`, `raids`, `follows` for
one `run_id` — **no schema change** for Phases 1–3.

---

## 2. The loop

Today:

- `PollingCollector.run`: `while not stop: step(stop); stop.wait(0.25)`
- `RecoveringProbe.run`: `while not stop: sample(); heartbeat(now); step(now)` where `step` blocks on `socket.recv(timeout=0.25)`

Merged `run(stop, duration)`:

```
start()                       # one clock sample; start_collector_run;
                              # initial starting/initializing health for every source
while not stop.is_set():
    now = clock_guard.sample()   # ONE clock authority; may raise ClockError; may fire on_gap
    if duration reached: break
    step(now, stop)              # the blocking wait happens INSIDE step
shutdown()
```

### `step(now, stop)` — order is deliberate

1. **(clock already sampled)** — `now` is passed in.
2. **stale check + heartbeat.** One `_check_stale(now)` (polling's — drives
   `stream_poll` health and `LiveStatus`). One heartbeat scheduler (30 s) doing a
   single `update_collector_heartbeat` per due tick. The EventSub `Heartbeat`
   object is removed; its rule "failed heartbeat write ⇒ latch storage failure"
   merges into this one scheduler, which on `StorageError` latches the router
   **and** aborts the poll side.
3. **polling half** (from `PollingCollector.step`): `worker.take()` → classify
   (fatal auth / late-discard / error health / `_save`) → maybe dispatch the next
   poll on its 60 s slot. `_save` writes stream+snapshot or offline, `stream_poll`
   health, and updates `LiveStatus`.
4. **EventSub half** (from `RecoveringProbe.step`): drain `SetupWorker` notices;
   advance / `complete_job` any `ConnectionJob`; if the socket is up →
   `socket.recv(timeout=TICK)` → `SessionReadiness.accept` → `EventRouter.dispatch`
   / `.observe`; handle `ReconnectRequest` (directed handover) vs. unexpected loss
   (`lose` → `reconnection_gaps` row + backoff); keepalive/liveness checks. If the
   socket is down (backoff) → `stop.wait(TICK)`.

`socket.recv(timeout=TICK)` in step 4 is the loop's pacing wait while the socket
is up (it replaces the standalone `stop.wait(0.25)`); during backoff the
`stop.wait(TICK)` fallback keeps the loop ticking at ~4 Hz.

`RecoveringProbe` gets refactored so an external loop can drive its `step()`
instead of copy-pasting the recovery/handover state machine.

---

## 3. Phase 1 — the one clock guard

New `scripts/clock_guard.py`. `ClockReading` and `read_clock` move here;
`collect_stream` re-exports them so existing tests keep importing
`collect.ClockReading`.

`class ClockGuard(clock=read_clock, emit, pause=time.sleep, on_gap)` with
`sample() -> ClockReading` — **identical thresholds and semantics to today's
`PollingCollector._sample` / `_recover_utc`**:

| Condition (vs. previous sample) | Result |
| --- | --- |
| `elapsed < 0` | raise `ClockError("elapsed_clock_rollback_restart_required")` — fatal |
| `wall < 0`, drop `>= 5 s` | fire `on_gap`, then raise `ClockError("utc_clock_rollback_restart_required")` |
| `wall < 0`, drop `< 5 s` | fire `on_gap`; emit `utc_clock_rollback_waiting`; poll every 250 ms up to 5 s for `now.utc >= prev.utc`; on catch-up fire `on_gap` again + emit `utc_clock_recovered_fresh_poll_required`; on timeout / elapsed rollback during the wait → `ClockError` |
| `elapsed >= 90` or `wall >= 90` or `abs(wall - elapsed) >= 5` | emit `clock_gap_fresh_poll_required` + fire `on_gap` |
| otherwise | return the reading |

**`on_gap(now)` in the merged collector does both halves:**

1. **polling reaction** (today's `_invalidate_clock`): `live_status.poll_failed()`,
   `_generation += 1`, `_discard_job = True`, `_force_validation = True`,
   `_next_poll = now.tick`.
2. **EventSub reaction** (new): force `TokenManager.validate_if_due(force=True)`
   on the next worker turn and tear down + rebuild the EventSub session —
   equivalent to `RecoveringProbe.lose("network_error")`: opens a
   `reconnection_gaps` row, closes the socket, backoff-reconnects with forced
   revalidation. Events during the gap are not replayed.

This **replaces `RecoveringProbe.sample()` entirely** — the EventSub side no
longer keeps its own clock guard; it consumes `now` from the coordinator.
`RecoveringProbe`'s current `abs(tick - utc) >= 5` → `ProbeError("clock_uncertain")`
(which *ends* the process) becomes the `ClockGuard` divergence branch, which
**rebuilds the session** instead. A clock wobble should re-establish the socket,
not kill an 8-hour capture.

`ClockError` is fatal: `run()` catches it like `CollectorError` today — emit the
code, **skip** the orderly multi-source stop, exit 1, run left open.

---

## 4. Phase 2 — the merged coordinator

### Startup

`start()` calls `start_collector_run` once, then writes the initial
`starting` / `initializing` health for `stream_poll` (polling) and, via
`EventRouter.begin()`, for `raids` and `follows` (sinks) — all under the one
`run_id`.

### Shutdown

On `stop.is_set()` or duration reached:

1. stop dispatching polls; `live_status.poll_failed()` (close chat eligibility).
2. close the EventSub socket; finish any `ConnectionJob`.
3. `worker.finish()` (poll worker); finish the `SetupWorker` — this waits for any
   in-flight token rotation/persistence.
4. `active = { sources that reached at least starting and are NOT storage-latched }`.
5. **if** the router is storage-latched **or** the poll side hit a storage
   failure → emit the storage-failure code, **do not** call the multi-stop,
   exit 1, run left open.
6. **else** `writer.stop_collector_run_multi(run_id, now, sources=active)` → emit
   `orderly_shutdown`, exit 0.

The sinks' own `EventSink.stop()` (per-sink `stopped` health) is **not** used
here — `stop_collector_run_multi` writes every `stopped` / `orderly_shutdown`
row atomically in one transaction. `EventRouter.stop()` is bypassed on the merged
path (a flag, or the router simply isn't told to self-write on shutdown). The
standalone `collect_eventsub.py` keeps its existing per-sink stop +
`close_collector_run`.

### Failure handling

| Failure | Merged behaviour |
| --- | --- |
| Poll auth fatal (`result.fatal`) | `stream_poll` error health, stop process, run left open |
| EventSub `subscription_revoked` | that source sticky-error; other sources + polling continue |
| EventSub transport gap | `reconnection_gaps` row, backoff reconnect; polling unaffected |
| Storage failure (any writer call, incl. heartbeat) | latch: no further writes, stop process, run left open, exit 1 |
| Recoverable UTC rollback | `on_gap`: fresh poll + EventSub session rebuild; continue |
| Unrecoverable clock rollback | `ClockError`: stop process, run left open, exit 1 |
| Clean stop / duration | `stop_collector_run_multi` over the active source set, exit 0 |

---

## 5. Phase 3 — verify the merged runtime live (not yet done)

A real stream (Sunday Sept 13 earliest), `stream_poll` + `raids` + `follows` +
`chat` in one process / one run (or `--no-chat` for a 3-source first pass). SQL
verification (queries are the developer's to write): one run row with
`stopped_at` set, viewer snapshots present, per-source health for every active
source, event counts, `chat_messages` count, `reconnection_gaps`. Counts /
timestamps / statuses / reason codes / IDs only. Until this happens the merged
runtime has only synthetic and PostgreSQL evidence.

---

## 6. Phase 4 — chat as the fourth source (built)

**Developer (SQL / modeling), done:** `sql/010_align_chat_messages.sql` renames
the pre-existing `chat_messages` primary key to `eventsub_message_id` and adds
`chat_message_id`; `message_fragments` stays a `jsonb` column (no separate
table). `sql/queries/insert_chat_message.sql` mirrors `insert_follow_event.sql`.

**Plumbing, done:**

- `parse_chat_notification` in `eventsub_capture.py` -> a `ChatNotification`
  record; `channel.chat.message` has no event-time field, so `notification_at`
  is the envelope time. Message text and fragments are kept out of `repr` and
  never appear in a `CaptureError`.
- `ChatSink(EventSink)`: resting health is polling-driven
  (`awaiting_stream_status` / `capture_ready` / `offline_observed` /
  `poll_failed` / `poll_stale`); precedence is stop > transport error /
  `invalid_notification` > polling state. A new `EventSink._prepare` hook
  (identity for follows/raids) lets `ChatSink` resolve each notification's
  stream through `LiveStatus` and drop a message outside eligibility -- not
  persisted, not an error.
- `EventSubConfig.sources` selects the sink set; `_build_eventsub(chat=...)` and
  `--no-chat` gate it. The coordinator mirrors every `LiveStatus` transition
  onto the chat sink (`_chat_polling`) and refreshes the sink's eligibility
  tick before each socket pump. `router.source_names` and
  `stop_collector_run_multi` pick chat up automatically.
- Tests: `parse_chat_notification` and `ChatSink` unit tests; router chat
  routing; `MergedChatWiringTests` (stub probe, real router/sinks); a
  four-source `MergedRuntimePostgresTests` case with a real loopback socket
  that stores an eligible chat message and discards an early one.

---

## 7. Ownership

**You review and must be able to explain** (not necessarily write):

- the merged `step()` order and why (polling half before EventSub half; one clock
  sample feeds both; the shutdown sequence)
- how a clock gap cascades to both halves via `on_gap`
- why a storage failure leaves the run open and skips the multi-stop
- the one-run / N-source health model

**You write** (SQL / data modeling):

- Phase 4 chat schema + migration + message-storage SQL — done (`010`,
  `insert_chat_message.sql`).
- Phase 3 verification `inspect_*` queries — still to write for the live rehearsal.

**I build** (plumbing), explaining behaviour and failure modes — done:

- `scripts/clock_guard.py` + `tests/test_clock_guard.py`
- the merged coordinator (`PollingCollector` + EventSub wiring) + tests
- the `RecoveringProbe` refactor so an external loop drives its `step()`
- `main()` wiring and signal handling (`--no-eventsub`, `--no-chat`)
- `parse_chat_notification`, `ChatSink`, `record_chat_message`, and the
  coordinator/router chat wiring (Phase 4)

---

## 8. Testing (all synthetic / PostgreSQL; nothing against Twitch)

- **`tests/test_clock_guard.py`**: every clock scenario against the extracted
  unit (small/large UTC rollback, elapsed rollback, recovery timeout, 90 s gap,
  5 s divergence) plus `on_gap` firing both reactions.
- **`tests/test_collect_stream.py`**: the polling tests stay green (the merged
  coordinator preserves polling behaviour); `MergedCollectorTests` and
  `MergedChatWiringTests` drive the merged loop through a stub probe (fake clock,
  real router/sinks) — shutdown source set, storage latch skipping the
  multi-stop, clock gap cascading to chat, chat polling health transitions.
- **`MergedRuntimePostgresTests`** (opt-in): the real `RecoveringProbe` over a
  real loopback WebSocket and a real `DatabaseWriter` — one run across a
  synthetic disconnect and recovery (`stream_poll` + `raids` + `follows`), and a
  four-source run that stores an eligible chat message and discards an early one.
- `scripts/collect_eventsub.py` and its tests keep working (isolated path).

---

## 9. Deferred / open

- **Phase 3 live rehearsal** — the merged runtime has never touched Twitch.
- Retiring `collect_eventsub.py` (kept for now).
- `EventSink.stop()` is kept for the standalone path and bypassed on the merged
  path (`stop_collector_run_multi` writes every `stopped` row).
- Windows/WSL sleep/resume — still needs a real-machine rehearsal, independent of
  this work; a polling-only run is the first chance.
- Follow/raid ↔ stream association (still deliberately NULL).
- `chat_message_id` uniqueness / fragment normalisation — revisit if analysis
  needs them.
