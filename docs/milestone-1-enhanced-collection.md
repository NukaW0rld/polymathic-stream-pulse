# Milestone 1: reliable enhanced collection

Status: implementation, production migrations, historical bridge derivation,
reauthorization, and bounded live validation complete. Full-stream production
verification remains. Implementation and release preparation started September
16, 2026. See the [release handoff](milestone-1-release-handoff.md).
Subsequent milestone: [repeatable post-stream reporting](milestone-2-post-stream-reporting.md),
implemented for pre-stream release on September 16, 2026.

## Goal and execution contract

Capture the newly agreed live observations without compromising existing
viewer, chat, raid, and follow capture. Produce enough provenance for reliable
post-stream quality assessment.

The user directs the product; AI implements the entire authorized milestone,
including schema, SQL, collector code, tests, and documentation. Do not require
user coding attempts or teaching checkpoints. Explain significant decisions
and resolve routine choices autonomously. This file preserves the approved plan
alongside its implementation and release record.

Read AGENTS.md, PROJECT_BRIEF.md, docs/product-and-reporting-direction.md,
docs/collection-policy.md, and the current merged runbook before implementation.
Recheck the worktree and current code; preserve unrelated edits.

## Starting point

At planning start, the merged collector already owned one clock, database writer,
and collector run for viewer polling and EventSub chat, raids, and follows.
Schema files 001–010 existed. Follows and raids had NULL stream associations;
raw event tables and viewer snapshots did not record `run_id`. The parser
discarded chat context, and presence/context history tables did not exist.

Tuesday September 15 was collected under the old design, according to the user.
Its quality has not been verified as part of planning. The documented successful
September 13–14 rehearsal is separate evidence.

## Implementation record

Migrations 011–015 implement run attribution/capabilities, presence snapshots,
chat context, stream metadata history, and raid-source context. New raw rows get
nullable direct `run_id` provenance; old rows remain NULL. Bridge rows record
`direct_observation` or `historical_unique_time_match`, so downstream reporting
cannot confuse derivation with direct capture.

The September 15 baseline was checked read-only before migration work. Run 8 is
open (`stopped_at` NULL) and is not treated as closed. Between its start and last
durable heartbeat it has 502 viewer snapshots (60.00-second mean cadence, no gap
over 90 seconds), 2,782 chat messages, 8 raids, 17 follows, and zero recorded
EventSub reconnection gaps. Health reached successful offline polls, but no
orderly-shutdown evidence exists. Those old observations support time-windowed
viewer/chat/raid/follow analysis; they do not support direct run provenance,
presence, chat context, metadata history, or raid-source context.

The auxiliary Helix worker performs one bounded request at a time. Presence
pagination yields the token lock after every page; raid enrichment uses a capped
queue and at most one retry. The coordinator remains the only database writer.
Attempt rows are committed before dispatch, so a crash leaves visible
`in_progress` evidence rather than an invented failure or success.

Analytical chat deduplication starts from the persisted EventSub delivery key
(`eventsub_message_id`, already the storage dedupe key). `chat_message_id` is
retained as a separate Twitch message identity for later investigation; no
cross-delivery collapse is performed during collection.

## Release record

On September 16, 2026, a private pre-migration custom-format PostgreSQL backup
was created outside the repository, restricted to the owner, and validated with
`pg_restore --list`. Migrations 011–015 then applied in order with
`ON_ERROR_STOP=1`. The privacy-safe historical derivation dry run found 1,357
uniquely attributable snapshots, zero ambiguous snapshots, and two unmatched
snapshots. The idempotent derivation created five bridge rows and did not fill
historical raw `run_id` values.

The Twitch moderator account was reauthorized with
`moderator:read:chatters`, `moderator:read:followers`, and `user:read:chat`.
Channel-specific follower and chatter access succeeded for the production
channel. A three-minute authorized live check against an already-live alternate
channel used an isolated schema and process-local target adapter so its
observations could not enter production. That check
saved live polls, completed a populated chatter-presence snapshot, saved stream
metadata, enabled all three EventSub subscriptions, recorded zero gaps or
errors, and shut down orderly.

A separate 90-second run used the unchanged production command, production
schema, and intended `polymathic` target while the channel was offline. It
recorded all eight Milestone 1 capabilities as configured, followed the expected
offline/paused state transitions, left no enhanced row in progress, recorded no
gap, and stopped all five active health sources orderly. The Milestone 1 suite
then passed 325 tests with PostgreSQL integration enabled; after Milestone 2 was
added, the combined suite passed 348.

The bounded live window contained no chat, follow, or raid delivery. Live chat
context, follow persistence, and raid-source enrichment therefore remain
unexercised for Milestone 1, as do failure/recovery paths that did not occur.
The alternate stream was already underway, so it is not evidence for the
required full-stream production verification. Existing open historical runs 1,
7, and 8 were preserved rather than assigned invented closure. The next
POLYMATHIC stream must complete the full-stream gate in the merged rehearsal
runbook before this milestone is labeled fully production-verified.

## 1. Verify and preserve the existing baseline

Use read-only checks for the September 15 collection: run closure, source
transitions, viewer cadence, gaps, and aggregate event counts. Output only safe
counts, timestamps, statuses, reason codes, and operational run/gap IDs.
Do not expose raw chat, Twitch identifiers, credentials, or payloads.

Record which analyses the old capture can support. New fields remain unavailable
for historical rows; do not reconstruct presence, badges, or past metadata from
today's Twitch responses. Do not invent closure for an open run.

Before production migrations, verify the collector has exited and establish a
private backup/recovery procedure. Add new numbered migrations; do not rerun
001–010. Test upgrades on synthetic existing rows and clean schema setup.

## 2. Add run-to-stream attribution and capabilities

Create a bridge at one row per run × observed stream, with a composite unique
key, foreign keys, and first/last successful observation times. Update it in
the live-poll transaction, preserving the earliest observation on repeat calls.
Neither timestamp is an inferred broadcast boundary or continuous-coverage claim.

Record collector version and configured sources/capabilities per new run so
reporting can distinguish not implemented, disabled, and failed to initialize.
Prefer a small explicit representation over a generic configuration framework.

Decide whether nullable run_id provenance on new raw observations is needed to
remove ambiguity in future attribution. If added, populate it directly for new
rows; historical values stay unknown unless uniquely supported by evidence.
Document the decision and keep historical derivations distinguishable from
direct capture. Never infer success merely from overlapping run time ranges.

Historical bridge derivation is a separate, repeatable operation based on
durable evidence, with ambiguity reported. Follow/raid-to-stream association
is a downstream analytical rule in milestone 2, not chat eligibility reused.

Acceptance: repeated observations, multiple runs per stream, multiple streams
per run, offline-only runs, and ambiguous historical cases behave correctly.

## 3. Add chatter-presence snapshots

Business question: how does Twitch-reported presence differ from active chat?

| Data | Grain and required evidence |
|---|---|
| Snapshot | One attempted snapshot: run/stream, request/completion times, reported total, collected distinct count, status, reason |
| Membership | One snapshot × Twitch user ID; no additional identity fields without a justified need |

Add moderator:read:chatters to authorization and validation, then reauthorize
with the collector stopped. Ensure a missing optional capability cannot silently
invalidate an intended fallback collection mode through the global scope check.

Sample approximately every five minutes during fresh observed-live eligibility.
Use bounded pagination, no overlapping attempts, no catch-up backlog, and
deduplication within each snapshot. Define which page's reported total is saved
and preserve a changed-total diagnostic. Completion means pagination completed,
not an exact simultaneous census. Empty success, partial result, request failure,
and interrupted attempt must remain distinguishable.

Persist attempt-start evidence before dispatch and finalize metadata/membership
consistently. An abandoned attempt remains visibly incomplete after a crash.
Reject or mark results spanning a clock gap or stream change. Exclude incomplete
snapshots from metrics requiring complete membership.

Keep network requests off the coordinator; it remains the sole DB writer.
Account for the token manager's request lock: schedule bounded page work so
presence cannot starve viewer polling or subscription setup/validation. Retain
the no-further-writes latch after storage failure. Add presence health reasons,
source allowlists, and orderly shutdown behavior.

Acceptance: empty response, multiple pages, duplicate users, changing totals,
partial failure, missing scope, 403, 429, slow requests, stream switch, clock
gap, stop mid-attempt, and storage failure. Existing capture remains responsive.

## 4. Preserve chat context

Add message type, reply-parent message/user identifiers, and observed badges to
the parser, record type, insert query, and writer. Old rows must distinguish
unknown context from an observed message with no reply or no badges.

Default proposal: badges as JSONB with analytical SQL projections. Use a child
table instead only if the concrete query requirements warrant it. Do not add a
foreign key requiring a reply's parent to have been captured. Keep current
delivery deduplication and explicitly define analytical message deduplication
before reporting. Preserve source-broadcaster context for shared-chat analysis.
Staff and subscriber messages remain included by default.

Acceptance: historical rows, absent reply, uncaptured parent, multiple badges,
duplicate delivery, malformed context, and privacy-safe diagnostics.

## 5. Capture stream metadata history

Use title, category, language, and tags already returned by live stream polls.
Create an initial history row and append only when observed values change.
Normalize tag ordering for comparison. Grain: one observed metadata version
per stream, with observation time and provenance.

This is a proposed implementation refinement to the product document's
channel.update suggestion: that event does not include tags. Existing polling
provides the complete selected field set without another subscription. Record
polling resolution honestly; transitions between polls may be missed.
On restart compare against the last durable version to avoid false changes.

Acceptance: unchanged values, changed title/category/language/tags, reordered
tags, restart, and missing/malformed optional context. Do not turn a context-only
problem into invented offline evidence or an unnecessary loss of viewer data.

## 6. Capture raid-source context

Persist each raid before scheduling a bounded Get Channel Information request.
Link its metadata observation to the raid, with request/observation times,
outcome/reason, category, title, language, and tags. Default to one logical
enrichment result per raid; define bounded retry behavior without overwriting an
earlier successful observation with later context.

Network enrichment failure must leave the raid intact and core capture running.
Queue saturation and shutdown before enrichment must be visible. Storage errors
still obey the global failure latch. Metadata is observed near the raid, not
guaranteed event-time truth. Do not infer genre or audience-fit labels.

Acceptance: success, missing channel result, request failure, duplicate raid,
back-to-back raids, delayed result, full queue, and shutdown.

## Release and definition of done

Release in tested slices: attribution/capabilities → presence → chat context →
stream history → raid context. Prioritize irreversible live observations before
the next stream, but retain a tested fallback if a slice is not ready.

Run relevant synthetic and isolated PostgreSQL integration tests, including
old-schema data migration and existing collector regression coverage. Confirm
new requests do not block EventSub reception, clock handling, or heartbeat work.
Perform a bounded authorized live check and then a full-stream verification;
label live failure paths unverified if they did not occur.

Update the collection contract, runbook, README status, and schema diagram.
Keep production data and Power BI caches private. Deliver a concise handoff
listing migrations applied, capability start dates, verification, and known
limits. Milestone completion requires all five collection additions working;
a partial release is progress, not completion.

Excluded: reporting execution, chat notifications, manual annotations, follower
snapshots, NLP, automatic delivery, and cloud infrastructure.

## API references

Verified during planning on September 16, 2026; recheck if implementation is later:

* [Get Chatters](https://dev.twitch.tv/docs/api/reference/#get-chatters)
* [Get Streams](https://dev.twitch.tv/docs/api/reference/#get-streams)
* [Get Channel Information](https://dev.twitch.tv/docs/api/reference/#get-channel-information)
* [Channel Update event](https://dev.twitch.tv/docs/eventsub/eventsub-reference/#channel-update-event)
