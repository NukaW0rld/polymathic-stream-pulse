# Milestone 2: repeatable post-stream reporting

Status: implementation complete for pre-stream release. The Power BI project
opens successfully and its page layouts were visually accepted September 16;
September 17 production data was collected and reviewed in Power BI on
September 18. Final acceptance remains open while the resulting report-redesign
recommendations are unimplemented. See the
[production-data review](power-bi-review-notes-2026-09-18.md). This milestone
record continues to describe the implemented version; the review governs the
next report iteration where its presentation decisions differ.
Implemented September 16, 2026 from the September 16 plan.
Predecessor: [reliable enhanced collection](milestone-1-enhanced-collection.md).
Predecessor evidence: [Milestone 1 handoff](milestone-1-release-handoff.md).
Release evidence: [Milestone 2 pre-stream handoff](milestone-2-release-handoff.md).

Implementation record: migrations 016–017 add a traceable execution manifest and
curated analytical views for deterministic event association, source intervals,
five-minute participation, raid horizons, chatter continuity, historical
comparison, report dimensions, and quality detail. `scripts.post_stream`
implements explicit target resolution, conservative quality classification,
input fingerprinting, per-target advisory locking, private recap generation,
manual-refresh recording, and owner review. The metric contract is in the
[versioned dictionary](metric-dictionary.md), and the official-run procedure is
in the [post-stream runbook](post-stream-runbook.md).

The PBIP source now uses a curated star-like model and three PBIR pages rather
than raw collection-table imports. PBIR files pass their published JSON schemas;
numerical SQL fixtures and workflow transitions are tested. The project has also
opened successfully in Power BI Desktop and the owner accepted the pre-stream
page layouts. The official enhanced stream still requires a fresh data load and
reconciliation of visible values against the briefing and SQL.

## Goal and execution contract

Turn closed-stream observations into a trustworthy interactive Power BI report
and concise owner-reviewed briefing through an idempotent local workflow.
Explain participation, raid impact, and community continuity beyond native totals.

The user directs the product; AI implements SQL, metrics, Pandas where useful,
Power BI definitions, orchestration, tests, and documentation. Do not require
user coding attempts or teaching checkpoints. Resolve routine choices and explain
material assumptions. Desktop interactions or credentials that require the user
are operational dependencies, not reasons to delegate coding back to the user.

Implementation starts when requested. Read the current project instructions,
product direction, collection contracts, and milestone 1 handoff. Reinspect code
and schema rather than assuming every planned collection change exists.

## Dependencies and historical compatibility

The full product depends on milestone 1, but quality-gate and viewer/chat/raid
dataset development can use September 15 data after verification. Historical
streams have different collection capabilities; new fields remain not collected,
not zero. Never fetch current metadata and label it historical.

As of September 16, migrations 011–017, historical bridge derivation,
reauthorization, bounded live validation, and Milestone 2 implementation and
testing are complete. Its first enhanced production-data acceptance must wait
for the next cleanly closed
POLYMATHIC stream; that run also completes Milestone 1's outstanding full-stream
verification. Do not weaken the quality gate merely to make pre-stream fixtures
publishable.

Keep collection and reporting separate. A closed collector run does not establish
that the stream ended. Missing closure, uncertain storage commits, and ambiguous
stream attribution must remain explicit; do not repair raw evidence to pass a gate.

## 1. Build the post-stream command and quality gate

Start with an explicit stream-targeted command that only resolves inputs and
produces a quality assessment. Extend the same command through later steps.
Select by stable stream identity privately; date-based lookup must use an
explicit timezone and handle multiple candidate streams.

Resolve every associated run, expected capabilities, stream closure evidence,
source initialization/shutdown, polling observations, EventSub health intervals,
reconnection gaps, restart gaps, and presence outcomes. Account for a failed
source separately from healthy transport and a quiet source separately from failure.

Do not treat bridge first/last times as continuous coverage. Derive conservative
intervals from durable evidence, with uncertain boundaries labeled. Disconnect
detection can lag actual loss; detected-to-recovered duration is not a guaranteed
upper bound on the entire outage. Correct conflicting runbook wording as part
of formalizing the quality contract.

| Overall state | Contract |
|---|---|
| Publishable | Required evidence supports the requested report |
| Publishable with warnings | Supported sections remain useful and limitations are explicit |
| Blocked | Closure, attribution, or consistency prevents a trustworthy report |

Also assign per-source and per-metric availability. Missing historical presence
suppresses presence metrics rather than invalidating unrelated valid analysis.
A short gap around a raid can invalidate that raid's comparison even if overall
coverage is high. Define thresholds from each metric's needs and test cases;
do not adopt an arbitrary universal coverage percentage.

Acceptance: clean run; multiple runs; late start; early stop; missing stream
closure; open failed run; unresolved gap; disabled versus failed source;
historical capabilities; incomplete snapshot; quiet chat; ambiguous association.

## 2. Formalize event association and metric contracts

Implement deterministic follow/raid-to-stream association separately from chat
eligibility. Use event times where available (followed_at for follows), envelope
time for raids, and receipt time for provenance. Use explicit half-open intervals
and preserve unresolved or ambiguous boundary cases. Derived associations must
be reproducible and must not manufacture raw observations.

Write a versioned metric dictionary before analytical SQL. Each metric declares
business question, grain, population, numerator/denominator where applicable,
time window, timestamp basis, deduplication, NULL/zero behavior, minimum evidence,
and overlap/gap treatment. AI proposes and implements justified defaults.

Initial choices to resolve and document:

* Five-minute participation windows, retaining one-minute viewer observations.
* Viewer aggregation with explicit treatment of irregular sampling and missing time.
* Message concentration, initially top-five participants' message share per window,
  with small-population context and undefined share when there are no messages.
* Active chatters counted distinctly per requested window, never summed as if
  distinct counts were additive across windows.
* Presence aligned using explicit snapshot age/completion criteria. Do not divide
  unrelated populations or imply all active chatters appear in a delayed snapshot.
* Raid baseline and observation/averaging windows at +5/+15/+30/+60 minutes.
  Distinguish point-horizon changes from cumulative post-event activity.
* Suppress unsupported horizons; flag overlapping raids and collection gaps.
  Report observed changes without assigning individual raiders or causality.
* First observed means since tracking began. Returning means observed in a prior
  stream; define recurrence threshold and lookback explicitly. Separate active
  chatter history from presence history and show the available historical coverage.
* Store UTC; default stream weekday to Europe/Amsterdam based on stream start,
  not each event's calendar date. Compare elapsed time from reported stream start.
* Decide how existing shared-chat provenance affects the analytical population;
  do not silently label all captured participants as local-channel participants.

## 3. Build reusable SQL analytical datasets

| Dataset | Grain | Purpose |
|---|---|---|
| Stream participation | Stream × five-minute window | Viewers, messages, active chatters, presence, coverage |
| Raid impact | Raid × horizon | Baseline, observable response, overlap/availability |
| Chatter participation | Stream × chatter | First-observed, returning, recurring participation |
| Historical comparison | Stream × elapsed-time window | Comparable segments and weekday baselines |
| Quality summary/detail | Stream × source; separate intervals | Report state and localized limitations |

Prefer reusable SQL views initially; materialize only for demonstrated needs.
Aggregate facts independently before joining them to prevent message/snapshot/
membership fan-out. Deduplicate analytical messages according to the documented
identity contract, not by text. Produce zero activity only where coverage supports
it; leave missing windows visibly unavailable.

Use synthetic fixtures with hand-verifiable answers: gaps, NULLs, duplicate
deliveries, boundary events, overlapping raids, missing baseline, stream-end
truncation, changing capabilities, and multi-stream participants. Compare safe
aggregate results with exploratory queries on verified real data.

## 4. Add Pandas only for a concrete analytical benefit

Implementation outcome: version 1 does not add a production Pandas step. The
aligned baselines and event-window metrics remain transparent and reusable in
SQL, so a second implementation would add drift risk without analytical value.

Candidate: explore between-stream variation and sensitivity of raid-window or
historical-baseline choices. Consume curated SQL results; do not independently
reimplement authoritative metric definitions. Keep any necessary outputs keyed,
versioned, repeatable, and covered by meaningful numerical fixtures.

Insufficient history is a valid report outcome. Show sample sizes and avoid
strong weekday or raid-source conclusions from a few streams. A documented
decision that no production Pandas step is needed is acceptable.

## 5. Build the curated Power BI model and three report pages

Replace raw-table-oriented imports with the curated datasets, explicit dimensions,
correct relationship cardinality, and intentional filter direction. Inspect the
existing run-to-gap relationship: one run can have multiple gaps. Replace implicit
date hierarchies with an explicit date/time design. Avoid exposing raw chat and
unneeded identity fields in the reporting model.

Pages:

1. Stream evolution: aligned viewer/presence/activity timelines, breadth and
   concentration, event markers, visible quality state and gaps.
2. Raid impact: reported size, observed source metadata, baseline and horizons,
   overlapping-event warnings, comparisons where evidence permits.
3. Community and stream comparison: first-observed/returning/recurring activity,
   elapsed-time alignment, weekday baselines, sample size, capability limitations.

Native totals remain subordinate context. Ensure filters preserve metric meaning
and unavailable values do not become zeros. DAX handles report-context measures
without duplicating SQL business rules. Validate numerical results against SQL
and inspect the actual report in Power BI Desktop; source-file edits alone do
not establish a successful refresh or usable visuals.

## 6. Complete orchestration and the reviewed briefing

Workflow: resolve stream/runs → validate → prepare SQL/Pandas outputs → refresh
Power BI → prepare recap → owner review. A blocked gate produces diagnostics,
not a misleading deliverable. Keep technical build/refresh status separate from
analytical publishability and delivery-review status.

Record target stream, input evidence/version, analysis version, quality reasons,
step outcomes, and artifact identity in a small execution manifest. Prevent
concurrent processing of the same target and avoid partial output replacement.
Rerunning unchanged inputs with the same analysis version yields equivalent
results without duplicate derived rows or artifacts. Changed inputs or definitions
must produce a traceable new result. Raw observations remain unchanged.

Initially allow manual Power BI Desktop refresh/export and record it as pending
until completed. Do not label an old export current after a failed refresh.
Prepare a concise written recap first; support a PDF/image export from the
validated report. Include supported participation findings, relevant raid results,
comparison context where available, and limitations. Interpretation is clearly
separate from collected facts. Owner review precedes external delivery.

Measure closure-to-ready time on a real run, including manual steps. Treat the
roughly native-summary delivery timescale as a target to verify, not a promise.

## Completion and handoff

Demonstrate one complete closed-stream workflow, a warning case, a blocked case,
and an unchanged-input rerun. Use a verified historical stream to demonstrate
missing-capability behavior and an enhanced stream for presence/context features.
If an enhanced stream is not yet available, synthetic validation is useful but
the corresponding live acceptance remains outstanding.

Deliver the quality rules, metric dictionary, tested SQL, Power BI model and
three report pages, post-stream command, reviewed-output procedure, and concise
operating documentation. Produce synthetic/sanitized public examples and inspect
screenshots/exports before publication. Keep real identities, raw chat, tokens,
production exports, recaps, and Power BI caches private.

The first usable slice may be quality gate + stream-evolution page + written
recap within the five-to-seven-focused-day MVP target. Full milestone completion
also requires raid/community pages and all acceptance checks; do not equate the
first slice with the entire milestone or promise mature historical conclusions.

Excluded: automatic sending, Power BI Service/gateway deployment, a public web
dashboard, cloud infrastructure, subjective annotations, NLP, and prediction.
