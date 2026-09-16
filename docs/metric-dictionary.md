# Metric dictionary

Version: `milestone-2-v1.0.0`

Effective: September 17, 2026

Authoritative implementation: `sql/016_create_post_stream_analytics.sql`

This dictionary defines the first post-stream analytical contract. UTC is the
storage and event-window basis. Stream weekday and local date use
`Europe/Amsterdam` at reported stream start. SQL produces authoritative metric
values; DAX only responds to report filter context.

`NULL` means the evidence does not support a value. Zero means collection was
available and no qualifying activity was observed. Event windows are half-open:
`[start, end)`.

## Stream participation

Grain: one stream × elapsed five-minute window. Windows begin at Twitch's
reported `streams.started_at`; the final window may be shorter.

| Metric | Business question and contract |
|---|---|
| Time-weighted average viewers | How did aggregate viewer count evolve? Each one-minute observation carries forward only until the next observation, stream end, or 90 seconds, whichever comes first. Numerator: viewer count × supported seconds. Denominator: supported seconds. A window is available when at most 90 seconds are uncovered. This does not identify viewers. |
| Messages | How much local-channel chat activity was observed? Count distinct persisted EventSub deliveries, already deduplicated by `eventsub_message_id`, in the window. Available healthy chat with no messages is zero. A window is unavailable when more than 30 seconds lacks healthy chat coverage. |
| Active chatters | How broad was active participation? Distinct `chatter_user_id` among eligible messages in the window. Distinct counts are recalculated at every requested grain and are never summed across windows. |
| Top-five message share | Was activity broad or concentrated? Numerator: messages from the five highest-volume active chatters in the window. Denominator: all eligible messages in the window. Ties are deterministically ordered by private user ID. Undefined for zero messages or unavailable chat. Always show message and chatter counts as small-population context. |
| Twitch-reported presence | How many accounts did Twitch report connected to chat? Use the most recent complete, membership-consistent snapshot completed by window end, no more than seven minutes old, whose request completed within 90 seconds. Population is Twitch's delayed chatter list, not video viewers. |
| Presence/activity alignment | How many active chatters in the window appear in the aligned snapshot? Numerator: distinct active chatters also present in snapshot membership. Denominator: distinct active chatters in the window. This is an alignment diagnostic, not the share of present users who chatted and not a retention metric. |
| Shared-chat messages | How much captured activity came with shared-chat source provenance? Count rows with `source_broadcaster_user_id IS NOT NULL` separately. Version 1 excludes them from local participation, concentration, and continuity metrics rather than silently treating them as local-channel participants. |
| Raid/follow event markers | Counts of deterministically resolved events in the same elapsed five-minute windows. A healthy quiet source yields zero; insufficient source coverage yields NULL. These markers provide timeline context and do not assign causality. |

## Raid impact

Grain: one deterministically associated incoming raid × horizon (`+5`, `+15`,
`+30`, `+60` minutes). Raid event time is the EventSub envelope timestamp;
receipt time remains provenance. Reported raid size and measured net viewer
change are separate values.

| Metric | Contract |
|---|---|
| Pre-raid viewer baseline | Arithmetic mean of viewer observations in `[raid - 5 minutes, raid)`. Requires at least three samples, no inter-sample gap over 90 seconds, a first sample within 90 seconds of the baseline start, and a last sample within 90 seconds of the raid. |
| Point-horizon viewer count | Nearest viewer observation within ±90 seconds of the exact horizon. Suppressed when the stream ended at or before the horizon. |
| Viewer change from baseline | Point-horizon viewer count minus pre-raid average. It is an observed aggregate change, not retained raiders. |
| Cumulative messages / active chatters | Eligible local messages and distinct active chatters in `[raid, horizon)`. Available only when healthy chat coverage misses no more than 30 seconds across the full horizon. These are cumulative post-event measures, not point measures. |
| Observed follows | Deterministically associated follow events in `[raid, horizon)`, using `followed_at`. Requires follow-source coverage missing no more than 30 seconds across the full horizon. This is an observed event count, not attribution to the raid. |
| Overlapping raid | True when another resolved raid occurs in `[raid, horizon)`. The result remains visible but must not be used as a clean single-event comparison. |
| Source context | Category, title, language, and tags observed near the raid. Missing or failed enrichment remains unavailable; current metadata is never backfilled as historical truth. |

## Event association

Follows use `followed_at`; raids use the EventSub envelope timestamp. A closed
stream is a candidate when `event_at >= started_at AND event_at <
offline_observed_at`. Exactly one candidate resolves the event. Zero candidates
is unresolved; multiple candidates is ambiguous. A non-NULL captured stream ID
that disagrees with the sole time candidate is a conflict. Ambiguous,
unresolved, and conflicting events are preserved but excluded from stream facts.

## Community continuity

Grain: one stream × active local-channel chatter. Power BI imports only the
stream-level summary, not raw or pseudonymous participant rows.

| Classification | Contract |
|---|---|
| First observed | The chatter's first eligible message since local active-chat tracking began occurs in this stream. This does not mean new to the Twitch channel. |
| Returning | At least one earlier stream contains an eligible message from the chatter within the 180-day lookback. |
| Recurring | At least two earlier streams contain eligible messages from the chatter within the 180-day lookback; including the current stream, this is participation in at least three streams. |
| Prior presence | The active chatter appeared in at least one complete presence snapshot from an earlier stream. Presence history is tracked separately from active-chat history and does not change active classifications. |

## Historical comparison

Grain: stream × elapsed five-minute window. A current window is compared only
with earlier closed streams at the same elapsed-window index. The all-stream
baseline and the Europe/Amsterdam stream-start weekday baseline are arithmetic
means of available window metrics. Sample size is the distinct prior-stream
count and must be displayed. Zero prior streams yields an unavailable baseline,
not a zero difference. No production Pandas step is used in version 1 because
the required aligned baselines remain transparent and reusable in SQL; a later
sensitivity analysis may consume these curated results without redefining them.

## Quality thresholds

There is no universal coverage percentage.

- Closure, missing run attribution, an open associated run, direct-provenance
  conflict, or inconsistent lifecycle timestamps blocks publication.
- Viewer windows use their 90-second continuity rule.
- Chat windows and raid participation horizons allow at most 30 seconds without
  healthy coverage.
- Raid viewer endpoints require a sample within ±90 seconds; baselines require
  three samples across two minutes.
- Presence requires complete pagination, member-count consistency, completion
  within 90 seconds, and maximum snapshot age of seven minutes.
- Missing historical enhanced capabilities suppress their metrics and warn;
  they do not invalidate supported viewer, chat, raid, or follow analysis.
- A healthy quiet source supports a true zero. A disabled or failed source is
  unavailable, never zero.

Reconnection `detected_at` is the time loss was detected, not proof of the
actual outage start. Source intervals end conservatively at the next durable
health transition, orderly run close, or last heartbeat; open-run boundaries
remain uncertain.
