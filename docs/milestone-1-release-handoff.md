# Milestone 1 release handoff

Release preparation performed September 16, 2026. This handoff records only
privacy-safe operational evidence. It excludes backup locations, credentials,
Twitch identities, raw messages, stream identifiers, and production event totals.

## Current state

Milestone 1 is implemented, migrated, authorized, regression-tested, and
bounded-live validated. It is ready for the next production stream, but it is not yet
fully production-verified because no post-migration collector run has covered an
entire POLYMATHIC broadcast.

Milestone 2 is implemented against the installed schema with synthetic and
verified historical fixtures. Its enhanced production-data acceptance, and
Milestone 1's final live gate, remain dependent on a clean closed-stream run.

## Completed release steps

| Gate | Evidence | Result |
|---|---|---|
| Private recovery artifact | Custom-format PostgreSQL dump created outside the repository with owner-only permissions; archive listing validated | Complete |
| Schema release | Migrations 011–015 applied once, in order, with fail-fast error handling | Complete |
| Historical bridge review | 1,357 uniquely attributable snapshots, 0 ambiguous, 2 unmatched | Complete |
| Historical bridge derivation | 5 derived run/stream rows created; historical raw `run_id` values remained NULL | Complete |
| Twitch authorization | Required core scopes plus `moderator:read:chatters` validated; production-channel follower and chatter access passed | Complete |
| Bounded live check | Real live polling, presence, metadata, EventSub setup/liveness, persistence, and orderly shutdown exercised in an isolated schema | Complete |
| Exact production path | Unchanged collector command ran against the production schema and intended offline target; all configured capabilities and shutdown rows persisted | Complete |
| Milestone 1 regression suite | 325 tests passed with PostgreSQL integration enabled | Complete |
| Combined Milestone 1 and 2 suite | 348 tests passed with PostgreSQL integration enabled | Complete |
| Full-stream production verification | Requires a collector run spanning the next POLYMATHIC stream | Outstanding |

## Bounded live-check findings

The alternate channel was already streaming. A process-local target adapter and
isolated database schema prevented its observations from entering production
while retaining the production collector's scheduling, Twitch request,
EventSub, parsing, and storage behavior.

The three-minute run produced:

- three successful live polls;
- enabled chat, raid, and follow EventSub subscriptions;
- one complete populated chatter-presence snapshot;
- one stream-metadata history version;
- zero reconnection gaps, invalid notifications, storage failures, or revocations;
- enhanced-worker completion and orderly shutdown for all active sources.

No chat message, follow, or incoming raid arrived during the window. Therefore
live chat-context persistence, follow persistence, raid persistence, and
raid-source enrichment are not established by this check. Their synthetic and
PostgreSQL coverage passed, but live acceptance remains event-dependent.

The isolated schema was removed after verification. Production collector-run and
event counts were unchanged by the alternate-account test.

## Production-path smoke findings

The main POLYMATHIC channel was offline. A 90-second run used the unchanged
production command and schema. It recorded all eight capabilities as configured,
two successful offline polls, expected paused/offline states for chat and chatter
presence, ready raid/follow subscriptions, zero gaps, zero unfinished enhanced
attempts, and orderly shutdown for `stream_poll`, `chat`, `raids`, `follows`, and
`chatter_presence`.

Historical collector runs 1, 7, and 8 already had NULL `stopped_at`. They were
left unchanged because the available evidence does not justify invented closure.

## September 17 production acceptance

Follow [the full-stream gate](merged-rehearsal-runbook.md#6g-full-stream-milestone-1-gate).
Start the collector before the channel goes live, keep the PC and WSL2 awake,
and stop only after a successful offline observation. Then run every privacy-safe
inspection in runbook §6.

The final handoff must distinguish:

- capabilities that were configured from observations that actually occurred;
- a quiet event source from a failed source;
- synthetic coverage from live evidence;
- direct run provenance from historical derived bridge rows;
- observable chat presence and participation from individual video viewership.
