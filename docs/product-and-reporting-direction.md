# Product and reporting direction

> **September 18, 2026 review update:** This document records the direction that
> led to the current implementation. The
> [September 18 production-data review](power-bi-review-notes-2026-09-18.md)
> governs the next Power BI iteration where its agreed recommendations differ,
> including removal of immature local chatter-history classifications and the
> current artist-facing historical-comparison visuals. Current schema and metric
> documentation remains authoritative until those changes are implemented.

## Decision

POLYMATHIC Stream Pulse is an operational analytics product, not a disposable
coding exercise. It will collect trustworthy live observations, produce
analysis that is meaningfully different from Twitch Creator Analytics, and
prepare repeatable post-stream deliverables for POLYMATHIC.

It is simultaneously the owner's primary portfolio data project for summer
2027 internship applications. Delivery is the priority, with clear engineering,
analytical reasoning, and honest attribution of AI assistance.
As clarified September 16, 2026, the owner supplies product direction and AI
handles all implementation, including SQL, Pandas, modeling, metrics, and Power
BI/DAX. The previous mentor/reviewer and user-first-attempt policy is superseded.
The owner retains control of private credentials and data, reviews material
product decisions, and approves any briefing before it is sent externally.

This collaboration policy does not change the agreed analytical scope or
delivery mechanism below.

## Native Twitch baseline

Twitch Creator Analytics already provides a broad set of creator-facing
information. Its documented metrics include average and maximum viewers, live
views, follows, subscriptions, minutes watched, time streamed, unique viewers,
raid viewers, unique chatters, chat messages, clips, ads, promotions, revenue,
and date-range comparisons. Stream Summary supplies per-stream key statistics,
previous-stream comparisons, an engagement panel, emote performance, and top
clips. Discovery and Engagement analytics add traffic sources and information
about other categories and channels watched by the audience.

For long streams, Twitch documents progressively coarser Viewer Engagement
panel intervals: five minutes for streams between four and ten hours and twenty
minutes for streams between ten and twenty-four hours. Twitch says Stream
Summary data is normally updated within fifteen minutes of a stream ending.

Sources:

* [Twitch Analytics Overview](https://help.twitch.tv/s/article/channel-analytics)
* [Twitch Stream Summary](https://help.twitch.tv/s/article/stream-summary)
* [Twitch Creator Camp: Introduction to Twitch Analytics](https://www.twitch.tv/creatorcamp/en/paths/establish-your-brand/channel-analytics/)

These public pages may not enumerate every Partner-, Ambassador-, or
experiment-specific feature. The product should therefore assume that a simple
summary is duplicative unless Stream Pulse clearly adds a different question,
grain, joined context, historical comparison, or quality guarantee.

## Differentiated value

Native metrics may appear as orientation, but the product's value is not its
average-viewer, peak-viewer, duration, message, chatter, follow, subscription,
or live-view totals.

Stream Pulse should explain:

* how viewer count, Twitch-reported chat presence, message rate, and unique
  active chatters change together;
* whether a busy period reflects broad participation or concentrated posting;
* how incoming raids correspond with viewer, presence, participation, and
  follow changes across multiple time windows;
* whether raid-source context is associated with different observable outcomes;
* how first-observed, returning, and recurring chat participation develops;
* how streams compare at equivalent elapsed stream time and against relevant
  weekday or historical baselines;
* which collection gaps or overlapping events weaken a conclusion.

The viewer-count timeline remains useful as a shared time axis, not as a
standalone product:

```text
viewer movement
    + chat presence
    + active participation
    + raids and contextual events
    + collection quality
    = an evidence-based explanation of how the stream developed
```

## Delivery contract

The target workflow begins only after the collector has safely closed:

1. Resolve the completed stream and all associated collector runs.
2. Validate run closure, expected source initialization and shutdown, polling
   coverage, EventSub transport gaps, and data consistency.
3. Assign one of three delivery states:
   * **publishable** — expected coverage and orderly closure;
   * **publishable with warnings** — bounded limitations that can be displayed
     accurately;
   * **blocked** — incomplete or inconsistent evidence that would make the
     report misleading.
4. Refresh reusable SQL analytical datasets.
5. Run Pandas only for transformations or analyses that are better expressed
   outside reusable relational queries.
6. Refresh the Power BI semantic model and report.
7. Prepare a concise briefing for owner review.

The deliverables are complementary:

* an interactive Power BI report for exploration and longitudinal analysis;
* a one-page PDF, image, or short written recap suitable for prompt delivery.

Automatic sending is not part of the first release. Calculations and factual
observations may be automated, but the owner reviews interpretation before the
briefing is delivered. The target is to be ready on roughly the same
post-stream timescale as Twitch's native summary while answering materially
different questions.

The first release may refresh and export through Power BI Desktop manually.
Later automation can publish to Power BI Service, use an on-premises gateway to
reach PostgreSQL, trigger semantic-model refresh through the Power BI API, and
automate export where licensing permits. Those operational additions should not
drive the analytical design.

References:

* [Power BI refresh for local data sources](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-desktop-file-local-drive)
* [Power BI semantic-model refresh API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset)
* [Power BI export-to-file requirements](https://learn.microsoft.com/en-us/power-bi/developer/embedded/export-to)

## Accepted and implemented collection changes

### Collector-run-to-stream bridge

Business need: apply run-level health and reconnection evidence to the correct
stream, including a stream captured across restarted collector runs.

Grain:

```text
one row per collector run × observed Twitch stream
```

Implemented fields include `run_id`, `stream_id`, `first_observed_at`, and
`last_observed_at`. Exact timestamps must reflect successful observations rather
than inferred broadcast boundaries.

### Chatter-presence snapshots

Business need: distinguish Twitch-reported chat presence from people actively
sending messages.

The Get Chatters API is available to a moderator with the
`moderator:read:chatters` scope. Twitch warns that its list is delayed relative
to joins and leaves and may change while paginated. Five-minute sampling is a
reasonable starting cadence; one-minute sampling would imply unsupported
precision and create unnecessary volume.

Implemented grains:

```text
chatter_presence_snapshots: one row per attempted snapshot
chatter_presence_members:   one row per snapshot × Twitch user ID
```

Snapshot metadata should preserve request and completion times, the reported
total, collected membership count, status, and a safe reason code. This enables
coverage analysis as well as presence/participation analysis.

Source: [Twitch Get Chatters API](https://dev.twitch.tv/docs/api/reference/#get-chatters)

Terminology must remain explicit: chat presence is not confirmed video
viewership, and delayed snapshots do not provide exact join/leave times.

### Chat-message context

Business need: separate broad participation from concentrated activity and
understand conversational structure without using sentiment analysis.

The `channel.chat.message` payload contains useful fields that the collector now
persists for new messages:

* message type;
* reply-parent message ID;
* reply-parent user ID;
* badge or role context as observed at message time.

Source: [Twitch Channel Chat Message event](https://dev.twitch.tv/docs/eventsub/eventsub-reference/#channel-chat-message-event)

Badges are stored as normalized JSONB on the message. Staff and subscriber
messages remain legitimate community activity; the fields permit segmentation
rather than automatic exclusion. Historical rows retain unknown context.

### Raid-source context

Business need: test whether raids from different source contexts correspond
with different viewer and participation responses.

The raid event already identifies the source broadcaster and reported raid
size. Near the event, query Twitch Channel Information and store an observation
of source category, title, language, and tags with its own timestamp.

Sources:

* [Twitch Channel Raid event](https://dev.twitch.tv/docs/eventsub/eventsub-reference/#channel-raid-event)
* [Twitch Get Channel Information API](https://dev.twitch.tv/docs/api/reference/#get-channel-information)

This is observed context, not guaranteed event-time truth. Names and IDs remain
private. Do not add manually assigned genre or audience-fit labels.

### Stream metadata history

Business need: preserve reproducible context when title, category, language, or
tags differ or change during a stream.

The collector captures the selected metadata from successful stream polls and
adds a history row only when an observed value changes. Polling was selected
because it already returns the complete title, category, language, and tag set;
`channel.update` does not include tags. Changes between polls may be missed.

Source: [Twitch Channel Update event](https://dev.twitch.tv/docs/eventsub/eventsub-subscription-types/#channel-update)

### Deferred chat-visible contextual events

The existing `user:read:chat` authorization and moderator status can support
`channel.chat.notification`, which includes chat-visible subscriptions, gifts,
announcements, watch streaks, raids, and related notices.

Source: [Twitch Channel Chat Notification event](https://dev.twitch.tv/docs/eventsub/eventsub-subscription-types/#channel-chat-notification)

These events would be used to identify overlapping context, not to reproduce
Twitch subscription totals. This addition is deferred until the existing
collector and the higher-priority sources remain stable because it adds another
EventSub source, persistence contract, and recovery path.

## Explicit exclusions

The product will not store subjective operator annotations. There will be no
`stream_annotations` table and no manually assigned labels such as same-genre,
off-genre, set quality, or audience fit. Human context may be discussed as
clearly labeled interpretation, never represented as collected fact.

Also excluded unless a new business requirement justifies them:

* full follower snapshots solely to infer follower losses;
* subscription identity and lifecycle collection that duplicates Twitch;
* ad events requiring POLYMATHIC's broadcaster-only authorization;
* moderation surveillance;
* thumbnails, frames, or video capture;
* generic sentiment analysis or elaborate NLP;
* person-level viewer or viewer-retention claims;
* fields collected only because the API exposes them.

## Report direction

The implemented report has three focused pages:

1. **Stream evolution**
   * collection-quality state and warnings;
   * synchronized viewer, presence, message-rate, and active-chatter timelines;
   * participation breadth versus concentration;
   * contextual event markers.
2. **Raid impact**
   * raid size and source context;
   * pre-event baseline;
   * +5/+15/+30/+60-minute observable changes;
   * overlapping-event warnings;
   * comparison across raids and recurring sources.
3. **Community and stream comparison**
   * first-observed, returning, and recurring chat participation;
   * normalized elapsed-stream comparisons;
   * weekday and historical baselines;
   * presence-versus-participation behavior.

Simple duration, average-viewer, peak-viewer, message, chatter, follow, and raid
totals may orient the reader, but should be visually subordinate to the
differentiated analysis.

## Analytical language

Use:

* aggregate viewer count;
* Twitch-reported chat presence;
* active chatter;
* first observed since tracking began;
* returning chatter;
* chat-presence retention;
* raid impact;
* observed association or change.

Avoid:

* identified viewer;
* new viewer when only first observation is known;
* viewer retention derived from aggregate counts or chat presence;
* causal claims from event timing alone.

## Implementation sequence

The executable specifications and implementation records are split into
[enhanced collection](milestone-1-enhanced-collection.md) and
[post-stream reporting](milestone-2-post-stream-reporting.md).
When implementation is requested, AI owns the selected milestone end to end.
Make business questions, grains, metric contracts, and limitations explicit;
do not require user coding attempts or teaching checkpoints. Their status and
release-handoff sections distinguish implemented work from pending live evidence.

1. Design and migrate the run-to-stream relationship.
2. Design chatter-presence snapshot and membership tables.
3. Extend OAuth, polling, persistence, health, and tests for presence capture.
4. Extend chat-message persistence with selected context.
5. Add raid-source and stream-metadata observations.
6. Define and test post-stream quality-gate rules.
7. Build reusable analytical datasets and metric definitions.
8. Replace the exploratory Power BI model with the curated semantic model and
   report pages.
9. Build the idempotent post-stream runner and reviewed recap output.
10. Evaluate Power BI Service and delivery automation after the local workflow
    proves useful.

Items 1–9 are implemented in the source and documented in the two milestone
handoffs. Full-stream enhanced-data acceptance and the first official-data
Power BI reconciliation remain pending. Item 10 is optional and deferred.
