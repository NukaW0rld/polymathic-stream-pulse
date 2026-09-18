# Power BI review notes — September 18, 2026

## Session purpose

Dissect the current Power BI semantic model and report using the September 17
production stream. Record owner feedback and change recommendations for a later
implementation session. No report or semantic-model changes are in scope for
this review session.

## Review status

- **Stream evolution:** Review complete
- **Raid impact:** Review complete
- **Community and comparison:** Review complete

## Review principles

- Separate observed report behavior from proposed changes.
- Preserve the distinction between UTC event-window calculations and
  Europe/Amsterdam stream-local date and weekday semantics.
- Do not treat missing or quality-suppressed values as zero.
- Record the business question and expected behavior for each recommendation.
- Leave implementation status as proposed until the corresponding change is
  made and verified.

## Document status and precedence

This file is the owner-reviewed change record produced from the September 17
production data review. It does not claim that the recommendations are already
implemented. The README, metric dictionary, milestone specifications, and
release handoffs continue to describe the source-controlled implementation as
it exists before the redesign. Where an older document's proposed report
presentation conflicts with an agreed recommendation here, this review governs
the next report iteration; current metric and schema documentation remains
authoritative until the corresponding implementation changes are completed.

## Change recommendations

### REC-002 — Redesign the report for POLYMATHIC as the primary audience

- **Status:** Proposed — governing requirement for the redesign
- **Area:** Entire report: information architecture, metrics, visual hierarchy,
  labels, explanatory text, and navigation
- **Business question:** What does POLYMATHIC need to understand about how a
  stream developed, how the community participated, and what happened around
  raids—without needing to understand the data model or collection system?
- **Observed behavior:** The current report reads primarily as an analytical and
  engineering validation tool for the project owner. Technical implementation
  concepts are exposed directly in visual titles, axes, and supporting text.
  For example, plots refer to `window_index`, which is a modeling detail rather
  than a concept useful to POLYMATHIC.
- **Recommendation:** Fully redesign the report around the questions and
  decisions POLYMATHIC cares about. Present the report as an artist-facing
  post-stream story, not as a database or model inspection interface.
- **Authorized design freedom:** The later implementation may replace the
  current report from scratch. Existing pages, visuals, navigation, layout, and
  page count are not constraints; pages may be added, removed, combined, or
  rebuilt as needed. There is no fixed maximum page count, provided every page
  follows the agreed audience, language, analytical, privacy, and quality
  principles captured in these notes. Recommendations discovered while
  reviewing one current page are not required to remain on that page; place
  each resulting insight wherever it best supports the redesigned report's
  information architecture.
- **Content direction:** Prioritize useful explanations of stream development,
  the relationship between audience size and chat participation, whether chat
  was broad or concentrated, raid impact, and meaningful community
  participation. Native Twitch totals may provide context but should not be the
  report's main value claim. Do not make short-history local classifications
  such as returning or recurring chatters a headline merely because the current
  model can calculate them.
- **Language direction:** Replace technical field and pipeline language with
  concise audience-facing terms. Examples include elapsed stream time instead
  of `window_index`, data coverage note instead of source-quality state, and
  chat activity instead of internal aggregation terminology.
- **Quality communication:** Keep collection limitations visible when they
  affect interpretation, but express them in plain language. Detailed source,
  gap, and execution diagnostics should not dominate the main report pages and
  may belong in a separate owner-only diagnostic view.
- **Design requirement:** Every page and visual should have a clear question it
  answers for POLYMATHIC. Remove or demote elements that exist mainly to prove
  the implementation rather than support that audience. Design freedom does
  not require using more pages: prefer the smallest coherent report that tells
  the agreed story without crowding or duplication.
- **Implementation status:** Not started

### REC-001 — Make the stream-date slicer explicitly Amsterdam-local

- **Status:** Proposed
- **Area:** All report pages / stream selection
- **Observed behavior:** The visible slicer uses `Stream[started_at]`, a raw
  timestamp, while the model already provides `Stream[stream_local_date]` and
  relates it to the Date dimension.
- **Recommendation:** Use the Amsterdam-local stream date for date-based stream
  selection so the slicer's time-zone meaning is explicit and streams remain
  assigned to the date on which they started in Europe/Amsterdam.
- **Limitation:** A date filter alone cannot distinguish multiple streams that
  start on the same Amsterdam-local date. A separate stream selector may be
  required if that becomes a real operating case.
- **Implementation status:** Not started

### REC-003 — Separate Airplane emote bursts from ordinary chat activity

- **Status:** Proposed
- **Area:** Stream evolution / chat activity
- **Business question:** How did ordinary participation develop across the
  stream, and where did exceptional community moments occur?
- **Observed behavior:** A linear message-count series is dominated by one or
  two `polymathicAirplane` spam periods, making the rest of the stream appear
  nearly flat. In the September 13 stream, the largest five-minute period had
  314 messages and 304 of them contained the Airplane emote. In the September
  17 stream, the largest period had 730 messages and 421 contained the emote; a
  second period had 76 messages and 59 contained it.
- **Recommendation:** Do not normalize each stream to its own 0–100 scale and
  do not use a logarithmic axis in the artist-facing report. Both would make the
  display harder to interpret, and per-stream normalization would destroy
  meaningful comparisons of absolute activity.
- **Proposed presentation:** Make distinct active chatters the primary measure
  of participation breadth. Show ordinary message activity separately from
  messages containing the Airplane emote, and highlight large observed
  Airplane-emote bursts as named community moments. Retain access to total
  message volume so the burst is not erased or hidden.
- **Potential visual treatment:** Use a readable elapsed-time chart for active
  chatters and non-Airplane messages, with a separate Airplane-emote series or
  event marker and a short callout summarizing the burst. Avoid placing raw
  message volume and active-chatters counts on one unlabeled shared scale.
- **Metric definitions to decide:** Distinguish (1) messages containing at least
  one `polymathicAirplane` emote from (2) total Airplane emote uses, because a
  single message can contain the emote multiple times.
- **Interpretation guardrail:** The collector structurally observes emote
  fragments, not the song being played. Label the analytical fact as an
  “Airplane emote burst”; describing it as “The Thing” or tying it to the song
  is contextual interpretation unless an observed timeline event establishes
  the song context.
- **Implementation status:** Not started

### REC-004 — Use one synchronized focal stream across post-stream pages

- **Status:** Proposed
- **Area:** Report-wide selection and navigation
- **Business question:** Which single stream is this report currently
  explaining?
- **Observed behavior:** The report exposes a between-date range on each page,
  allowing multiple streams to enter filter context. That makes the Stream
  evolution timeline incoherent and creates invalid aggregations such as
  summing messages at the same elapsed bucket across streams while taking the
  maximum active-chatter count from only one stream.
- **Recommendation:** Replace the date-range controls with one single-select
  focal-stream selector and synchronize that selection across the Stream
  evolution, Raid impact, and Community/comparison experiences.
- **Display:** Present a human-readable label using the Amsterdam-local start
  date and time, for example `Thu 17 Sep 2026 · 19:03`. Keep the stable
  `stream_id` hidden as the actual key. A date alone is insufficient if two
  streams can begin on the same Amsterdam date.
- **Page behavior:** Stream evolution shows only the selected stream's elapsed
  timeline. Raid impact shows only raids associated with that stream. Community
  metrics describe that stream's participants. Historical comparison still
  uses prior streams behind the scenes as baselines; those prior streams are
  comparison evidence, not additional focal selections.
- **Longitudinal exception:** If the redesign includes a dedicated trends-over-
  time page, that page may use a date range or multi-stream cohort because its
  business question is explicitly cross-stream. Do not reuse that interaction
  on single-stream review pages.
- **Implementation status:** Not started

### REC-005 — Remove the standalone chat-presence plot

- **Status:** Agreed
- **Area:** Stream evolution
- **Observed problem:** “Average Presence” exposes an intermediate analytical
  measure without giving POLYMATHIC a clear insight or decision. Its label is
  misleading, and connected-to-chat accounts can easily be mistaken for
  viewers.
- **Recommendation:** Remove the standalone presence timeline from the artist-
  facing report. Do not replace it merely to preserve the current layout.
- **Retained analytical role:** Keep the underlying presence snapshots in the
  model. Use them only when a specific, plainly framed comparison requires
  them—for example, establishing how many chat-connected accounts actively
  participated during an exceptional moment. Never present presence as
  viewership, audience retention, or a general headline KPI.
- **Implementation status:** Not started

### REC-006 — Replace top-five share with a whole-stream chat-breadth statement

- **Status:** Proposed
- **Area:** Community participation / stream summary
- **Business question:** Was conversation broadly shared across the community,
  or was most activity carried by a small group?
- **Recommendation:** Remove the per-five-minute top-five-share timeline. For
  the selected stream, rank active chatters by total eligible local-channel
  messages and calculate the smallest number whose cumulative messages reach
  50% of all observed messages. Present both the count and its share of active
  chatters in a sentence, accompanied by total active chatters and messages.
- **Audience-facing example:** `8 of 148 active chatters produced half of all
  observed messages.` Lower proportions indicate greater concentration; higher
  proportions indicate broader participation.
- **Why this alternative:** It uses the whole participation distribution and
  adapts to different community sizes instead of fixing an unsupported number
  of “top” people. The 50% boundary remains a chosen threshold, but it has a
  direct, familiar interpretation and supports a complete plain-language
  statement.
- **Observed production values:** September 13: 11 of 154 chatters (7.1%)
  produced half of 2,301 messages. September 17: 8 of 148 (5.4%) produced half
  of 2,846 messages. Excluding messages containing the Airplane emote changes
  those values to 14 of 152 (9.2%) and 10 of 148 (6.8%), respectively.
- **Community-moment treatment:** Use the all-message value as the honest
  whole-stream result, then separately explain how identified Airplane-emote
  bursts affected concentration. Do not silently remove those messages from the
  headline metric. A secondary “ordinary conversation” version may exclude
  them only if its population is clearly labeled.
- **Visual treatment:** Prefer a concise insight card or narrative over another
  five-minute line. A future longitudinal view may trend the percentage of
  active chatters required to reach half of messages across streams, with
  collection-coverage context.
- **Implementation status:** Not started

### REC-007 — Exclude known automated chat accounts from participation analytics

- **Status:** Agreed
- **Area:** Analytical SQL, semantic model, briefings, and all chat-derived
  report metrics
- **Observed problem:** The current analytical views include every eligible
  local-channel chat message regardless of sender identity. Nightbot and
  StreamElements are therefore currently included in message counts, active-
  chatter counts, concentration/breadth measures, continuity classifications,
  raid-window chat activity, and generated briefing summaries.
- **Recommendation:** Exclude the stable Twitch user IDs for the known Nightbot
  and StreamElements accounts from human/community participation analytics.
  Apply one reusable eligibility rule upstream of every derived chat metric so
  pages and briefings cannot disagree.
- **Data handling:** Preserve the raw captured messages unchanged for provenance
  and reproducibility. Perform exclusion only in curated analytical views.
  Identify accounts by stable Twitch user ID, not display name, message text,
  moderator badge, posting cadence, or inferred behavior.
- **Governance:** Document each excluded account, its automation reason, and
  the source/date used to resolve its stable ID. Treat this as an explicit,
  auditable known-service-account rule rather than a subjective classification
  of ordinary participants. Do not generalize the rule to suspected bots
  without equivalent evidence and owner agreement.
- **Recalculation scope:** Recompute total messages, active chatters, chat
  breadth/concentration, any validated Twitch-identified first-time-chatter
  metric retained under REC-012, Airplane-burst context, raid cumulative
  messages/chatters, historical baselines still used outside the removed
  artist-facing comparison, briefing values, and any downstream DAX measures
  after the filter is introduced. Do not spend work recalculating the retired
  local first-observed/returning/recurring headline classifications.
- **Implementation status:** Not started

### REC-008 — Do not plot raid counts and follow counts as two lines

- **Status:** Proposed
- **Area:** Stream evolution / event context
- **Observed problem:** The current line chart aggregates raids and follows into
  five-minute counts and plots both against the same event-count axis. These are
  discrete, semantically different events. A line interpolates activity between
  points where no event occurred; equal-height spikes make one raid and one
  follow look equivalent; and raid size and source context—the useful parts of
  a raid—are discarded. Follows occur at a different frequency and can visually
  obscure rare raids.
- **Recommendation:** Remove the combined event-count line chart. Show incoming
  raids as annotated moments on the selected stream timeline, including the
  observed raid size and useful source context. If follow timing proves useful,
  show it separately as restrained interval bars or callouts aligned to the
  same elapsed-time axis. Do not imply that temporally nearby follows were
  caused by a raid.
- **Page ownership:** Keep concise raid markers on Stream evolution only as
  context for changes in viewers and participation. Put detailed raid outcomes
  and comparisons on the Raid impact page. Retain follow timing only where it
  supports a question beyond Twitch's native follow total.
- **Implementation status:** Not started

### REC-009 — Add aggregate follower-loss estimation for future streams

- **Status:** Recommended after research; requires collector/model work
- **Area:** Collection, quality gates, analytical SQL, Community reporting
- **Business question:** How much gross follower acquisition remained as net
  follower growth during the observed stream period?
- **Platform finding:** Twitch EventSub exposes `channel.follow` but no
  corresponding unfollow event. The Get Channel Followers endpoint returns the
  current total and current follower list; it does not provide historical
  unfollow events. Twitch's documented Creator Analytics metric is follows
  received in a selected range, not unfollows.
- **Recommendation:** Capture only the aggregate follower total at collector
  start, on a modest live cadence (approximately five minutes), immediately
  after offline observation, and at orderly shutdown. Continue capturing gross
  follow events through EventSub. Do not collect or diff complete follower
  identity snapshots.
- **Derived metric:** For a fully covered interval, estimate follower losses as
  `observed follow events - follower-total change`. Example: 14 observed follows
  and net total growth of 11 imply 3 follower losses during that observed
  interval.
- **Terminology:** Report `gross follows`, `net follower change`, and
  `estimated follower losses`. Do not claim individual unfollow events, exact
  loss timestamps, or that a particular new follower unfollowed. A decrease can
  also reflect another removal from Twitch's current follower set, such as an
  account becoming unavailable.
- **Quality gates:** Publish the estimate only when both boundary totals exist,
  follow EventSub coverage is complete for the full interval, the stream/run
  association is unambiguous, and the arithmetic is internally consistent.
  Otherwise show the metric as unavailable. Collection starting after the
  stream began limits the result to the observed period and must be labeled.
- **Privacy:** Persist timestamped totals and operational provenance only. The
  existing `moderator:read:followers` authorization already supports the
  endpoint. Never persist the endpoint's returned follower identity merely to
  calculate a total.
- **Reporting:** Prefer a concise Community-page summary such as `14 followed ·
  net +11 · about 3 follower losses during the observed period`. Do not add
  inferred losses to the existing raid/follow spike chart, and do not attribute
  losses to a raid or stream moment without evidence.
- **Historical limitation:** This cannot be reconstructed reliably for past
  streams because the required boundary totals were not captured. Coverage
  begins only after implementation.
- **Implementation status:** Not started

### REC-010 — Rebuild Raid impact around one readable story per raid

- **Status:** Proposed
- **Area:** Raid impact
- **Information-architecture freedom:** “Raid impact” is an analytical subject,
  not a required page boundary. The redesign may remove, rename, split, or
  combine the current page. Concise raid markers may support the selected
  stream's overall story, detailed outcomes may appear in a focused raid
  comparison experience, and collection diagnostics may live in an owner-only
  quality view. Do not force every raid-related element onto one page merely to
  preserve the current layout, and do not add pages unless they improve the
  audience's understanding.
- **Business question:** After each incoming raid, what observable change occurred
  in POLYMATHIC's audience and chat activity, how long did that change remain
  visible, and what limitations affect the comparison?
- **Observed problem:** The current page exposes the grain and field names of
  `analytics_raid_impact` rather than explaining a raid. One incoming raid is
  expanded into four rows at `+5`, `+15`, `+30`, and `+60` minutes. This makes
  source metadata and the raid time repeat, makes one raid look like four
  events, and leaves the audience to decode implementation terms such as
  “supported horizon,” `context_status`, `overlapping_raid`, and
  `stream_end_truncated`.
- **Remove the headline horizon count:** “Supported Raid Horizons” counts
  raid-horizon rows for which both the five-minute pre-raid baseline and the
  point-in-time viewer observation exist and no other raid overlaps the window.
  It does not count raids or viewers. A fully supported single raid contributes
  four to this count. This is a quality-control total with no useful
  artist-facing interpretation; remove it from the main report.
- **Replace the average-change card and plot:** “Average Viewer Change” is the
  arithmetic mean of `viewer count at the selected horizon - average viewer
  count during the five minutes before the raid`. The leading `+` or `-` is
  merely the signed number format, not a second delta calculation. In the plot,
  the x-axis is minutes after a raid and the y-axis is that viewer-count
  difference, averaged across all raids in filter context. On a selected stream
  with one raid, the card averages four different moments into one number while
  the plot shows those same moments separately; the card is therefore ambiguous
  and duplicative. Across multiple raids, the plot additionally combines events
  with different sizes and circumstances. Remove the aggregate card and do not
  average unlike raids into an unexplained line.
- **Proposed presentation:** Use one compact section or small multiple per raid,
  labeled with the Amsterdam-local raid time and reported incoming raid size.
  State the five-minute pre-raid viewer baseline once, then show supported
  `+5`, `+15`, `+30`, and `+60` outcomes with plain labels such as `viewers vs.
  pre-raid level`, `people who chatted`, and `messages after the raid`. Prefer a
  compact time-path visual plus a one-sentence takeaway over a wide raw table.
  Use actual viewer level as well as the change from baseline so the comparison
  has scale and context. Do not call the change retained raiders or causal
  impact.
- **Source context:** `source_title` is the raiding channel's public Twitch
  channel title returned by Get Channel Information shortly after the raid. It
  is neither the source broadcaster's display name nor POLYMATHIC's title. Its
  current label therefore invites misinterpretation. It repeats because all
  source fields are copied onto each of the four horizon rows. Show useful
  source context once per raid. Relabel it `Raiding channel's stream title` if
  retained, and pair it with source category and reported raid size. A title
  may be stale or generic by the time it is fetched, so category and title are
  context observed near the raid, not EventSub facts fixed at the event instant.
- **Source identity and privacy:** The analytical model deliberately exposes
  only a deterministic anonymous source ID, not the real source username. That
  makes the title/category alone hard for POLYMATHIC to recognize. Decide during
  redesign whether the private owner-reviewed report may display the observed
  source channel name from ephemeral/private data without placing it in the
  public repository. Do not weaken the repository's privacy rules merely to
  make this page recognizable.
- **Timestamp:** `raid_at` is the EventSub envelope timestamp and the analytical
  contract uses UTC. The semantic model imports it as an unqualified Power BI
  `dateTime`, so the current visible rendering does not communicate a reliable
  time-zone contract. Do not expose the raw timestamp. Derive and label an
  explicit Europe/Amsterdam local raid time, and optionally add elapsed stream
  time for narrative context.
- **Table field disposition:** Keep raid time, reported raid size, source
  category, clearly labeled source title when informative, horizon, viewer
  baseline, viewer level/change, and post-raid chat measures—but reorganize them
  into one raid story rather than a raw row dump. Remove `context_status` from
  the artist-facing view. Translate `overlapping_raid`, missing evidence, and
  stream-end truncation into concise warnings only when they affect a displayed
  result. Do not show Boolean diagnostic columns. `observed_follow_events`
  should not be a primary raid outcome because temporal proximity does not
  establish raid attribution and Twitch already reports follows; omit it unless
  a later question justifies a carefully worded secondary context measure.
- **Quality behavior:** Unsupported values remain unavailable, not zero. If a
  horizon overlaps another raid, show the observation only with a plain warning
  and exclude it from any clean single-raid comparison. If the stream ends
  before a horizon, say `Stream ended before +60 min` rather than exposing a
  truncation flag.
- **Implementation status:** Not started

### REC-011 — Add a report-wide display-time-zone selector

- **Status:** Proposed
- **Area:** Report-wide selection, timestamps, tables, tooltips, and clock-time
  axes
- **Business question:** Can the owner and POLYMATHIC each read event times in
  their own familiar local time without changing the underlying event-window
  calculations?
- **Recommendation:** Place a single-select display-time-zone control beside the
  focal-stream selector. Offer `POLYMATHIC time — Europe/Amsterdam` and `Owner
  time — America/Chicago`; retain `UTC` only if it is useful for an owner-only
  diagnostic view. Default the artist-facing report to Europe/Amsterdam.
- **Feasibility and implementation direction:** Keep UTC as the authoritative
  stored timestamp and calculation basis. Derive DST-aware Amsterdam and
  Chicago display timestamps upstream in PostgreSQL for every timestamp that
  can appear in the report. Use a Power BI field parameter or equivalent
  controlled field-switching pattern so the selection changes the timestamp
  column used by clock-time axes, table columns, tooltips, and narrative labels.
  Do not implement this as a fixed numeric UTC offset: Amsterdam and Chicago
  both observe daylight saving time and change clocks on different dates.
- **Scope:** Apply the selection consistently to raid times, stream start/end
  times, metadata/event timestamps, and any other visible wall-clock time.
  Elapsed-stream-time axes do not change with time zone and should remain the
  primary axis where the question is how a stream developed. Date-only labels
  derived from an event timestamp must switch with the selected time zone too,
  because an event can fall on different calendar dates in the two zones.
- **Labeling:** Always display the active zone near the selector and in exported
  or standalone outputs where the slicer may not be visible. Prefer audience
  labels such as `Amsterdam time` and `Chicago time`; include the canonical IANA
  zone names in help text. Avoid unexplained UTC offsets and ambiguous
  abbreviations such as `CST`.
- **Interaction:** Synchronize the time-zone selection across all report pages,
  just like the proposed focal-stream selection. It changes presentation only:
  it must not change stream association, horizon membership, five-minute
  windows, baselines, coverage decisions, or any other metric result.
- **Design limitation:** Supporting an arbitrary user-entered list of world time
  zones would add disproportionate model and testing complexity. The two known
  audience zones answer the actual product need; add another named zone only
  when a real report audience requires it.
- **Verification:** Test timestamps around both European and United States DST
  transitions, events near local midnight, synchronized slicer behavior, table
  and tooltip consistency, and PDF/image exports. Confirm that metric values do
  not change when only the display zone changes.
- **Implementation status:** Not started

### REC-012 — Remove immature local chatter-history classifications

- **Status:** Agreed
- **Area:** Community and comparison / analytical SQL / collection
- **Business question:** Did Twitch identify anyone who spoke during this stream
  as a first-time chatter in POLYMATHIC's channel?
- **Observed behavior:** The current cards classify an active chatter as `First
  Observed` when their earliest eligible message in the local Stream Pulse
  database occurs in the selected stream, `Returning` when they messaged in at
  least one earlier locally collected stream during the preceding 180 days,
  and `Recurring` when they messaged in at least two. Recurring is therefore a
  subset of returning, not a separate population. These counts depend on how
  long this project has collected chat, not on POLYMATHIC's established Twitch
  community history.
- **Product failure:** The project will not run for months before producing a
  useful deliverable for POLYMATHIC. With only a small number of locally
  collected streams, the cards mostly describe collection maturity and can
  misrepresent established community members as newly observed. Twitch already
  maintains first-time- and returning-chatter classifications for its moderator
  experience, so recreating them from a new local history is weaker and
  duplicative.
- **Platform finding:** Twitch IRC `PRIVMSG` metadata can include the Twitch-
  supplied `first-msg` and `returning-chatter` tags. The preferred EventSub
  `channel.chat.message` payload does not expose those Boolean fields, but it
  does expose a documented `message_type` value of `user_intro`. There is no
  documented Helix endpoint for retrospectively querying whether an arbitrary
  user is a first-time or returning chatter. These signals must be captured on
  the live message when Twitch supplies them. Sources: Twitch's official
  [IRC documentation](https://dev.twitch.tv/docs/chat/irc) and
  [EventSub reference](https://dev.twitch.tv/docs/eventsub/eventsub-reference/).
- **September 17 evidence:** The collector persisted EventSub `message_type`.
  An aggregate production check found 2,845 `text` messages and one
  `power_ups_gigantified_emote` message, with no `user_intro` rows. The captured
  data therefore cannot support a Twitch-identified first-time-chatter result
  for this stream. This aggregate check did not inspect or expose usernames,
  user IDs, or message text.
- **Recommendation:** Remove the current `First Observed Chatters`, `Returning
  Chatters`, and `Recurring Chatters` cards from the artist-facing report and
  retire their current local-history definitions as headline product metrics.
  Do not replace them for the September 17 stream. For a future stream, consider
  one clearly labeled `Twitch-identified first-time chatters` result only after
  a controlled live validation establishes which captured Twitch field, if
  any, reliably corresponds to the moderator-interface classification. Do not
  infer or backfill that status from local history when the Twitch signal is
  absent.
- **Returning-chatter decision:** Do not add a second IRC chat listener solely
  to obtain Twitch's `returning-chatter` tag. The owner does not consider that
  classification important enough to justify duplicate-message reconciliation
  and an additional live collection failure surface. Reconsider IRC only if a
  future business question makes the Twitch-supplied field materially useful.
- **Recurring-chatter decision:** Remove it. It is a Stream Pulse-defined
  longitudinal threshold, not a Twitch classification, and the available local
  history is insufficient for the intended immediate deliverable.
- **Historical limitation:** Twitch's live-message flags cannot be fetched
  retroactively through a documented API. The September 17 EventSub data does
  not contain adequate classification evidence, so the result is unavailable
  rather than zero.
- **Implementation status:** Not started

### REC-013 — Remove the immature viewer-difference comparison plot

- **Status:** Agreed
- **Area:** Community and comparison / historical comparison
- **Business question:** At the same point in a stream, was POLYMATHIC's
  observed audience above or below a meaningful historical norm?
- **Independence from chatter cards:** This plot does not use first-observed,
  returning, or recurring chatter classifications. Removing those cards does
  not change its calculations. The only connection is that both currently
  occupy the same report page.
- **Observed behavior:** At each five-minute elapsed-stream window, `Selected
  Viewer Difference vs Prior` subtracts the mean viewer level of all available
  earlier streams at that same elapsed window from the selected stream's viewer
  level. Despite its name, `Prior` does not mean the immediately preceding
  stream. `Selected Viewer Difference vs Weekday` performs the same calculation
  using only earlier streams that began on the same Europe/Amsterdam weekday.
  Positive values mean the selected stream was above the applicable historical
  mean; negative values mean it was below.
- **Observed presentation failure:** The title requires the audience to decode
  `selected`, `difference`, `prior`, and `weekday` without stating the baseline,
  unit, population, or meaning of a positive value. The zero-based
  `window_index` axis further obscures that each point represents a five-minute
  period at the same elapsed position in different streams. The visual asks
  POLYMATHIC to interpret two abstract deviations rather than showing the
  audience level or the streams supporting the comparison.
- **Sample-size behavior:** Historical sample size varies by elapsed window
  because shorter earlier streams stop contributing to later windows. The two
  headline cards use `MAX` across visible windows, so they show the largest
  available sample rather than the evidence count supporting every plotted
  point. `Prior Stream Sample Size` is also misleading wording because the
  baseline includes all available earlier streams, not one prior stream.
- **Usefulness assessment:** An elapsed-time historical comparison could
  eventually show whether audience development was unusually strong or weak
  independent of stream length. The current dataset is too young for either
  baseline to be stable, and the same-weekday subset is smaller still. For the
  immediate POLYMATHIC deliverable, the plot primarily exposes an immature
  analytical mechanism rather than a trustworthy, actionable insight.
- **Recommendation:** Remove the current two-line viewer-difference plot and
  the standalone prior-stream and weekday sample-size cards from the artist-
  facing report. Do not retain them merely because they are independent of the
  chatter-classification change.
- **Future reconsideration:** After enough comparable streams exist, consider
  a plainly labeled view that overlays the selected stream's actual viewer
  level against one justified historical range or baseline at elapsed stream
  time. Display the changing number of contributing streams with the comparison
  and explain the takeaway in ordinary language. Do not restore a weekday
  baseline unless there is enough evidence that weekday is a useful comparison
  cohort for POLYMATHIC rather than an arbitrary segmentation.
- **Implementation status:** Not started

### REC-014 — Remove the raw historical-comparison table

- **Status:** Agreed
- **Area:** Community and comparison / historical comparison table
- **Observed behavior:** The table places one row per elapsed five-minute
  window beside the viewer-difference plot and exposes `window_index`, the
  selected stream's `time_weighted_avg_viewers`, the all-earlier-stream average
  and sample size, and the same-weekday average and sample size. The sample-size
  values repeat on every row and can decline in later windows as shorter
  historical streams stop contributing.
- **Elapsed-time problem:** `window_index` is an internal zero-based bucket key,
  not an audience-facing time value. Index 0 means 00:00–00:05, index 1 means
  00:05–00:10, and so on. Fixed elapsed buckets are appropriate for aligning
  differently timed streams in SQL, but displaying the integer key forces the
  reader to convert indices into time and makes the table look like arbitrary
  ticks. Any retained elapsed-stream analysis must display real elapsed labels
  such as `00:00–00:05` or an elapsed-time axis.
- **Usefulness failure:** The table is a row-level inspection of the dataset
  feeding the adjacent plot, not a separate POLYMATHIC insight. It repeats the
  same immature baselines in a denser form, exposes technical field names,
  provides no narrative or actionable interpretation, and asks the reader to
  scan roughly one row for every five minutes of a multi-hour stream. Relabeling
  its columns would not fix the absence of a useful business question.
- **Recommendation:** Remove the table completely from the artist-facing
  report. Do not replace it one-for-one and do not retain it as a tooltip for
  the removed comparison plot. If row-level baseline diagnostics remain useful
  during development, keep them in SQL validation queries or a clearly
  separated owner-only diagnostic view rather than the POLYMATHIC report.
- **Implementation status:** Not started

## Questions and findings

- Analytical storage and event-window calculations use UTC.
- Stream-local date and weekday semantics use `Europe/Amsterdam` at stream
  start.
- **Raid-page measure finding:** “Supported Raid Horizons” counts qualifying
  raid × horizon rows, not distinct raids. With four configured horizons, one
  raid can contribute as many as four supported rows.
- **Raid-page chart finding:** The line chart's x-axis is `horizon_minutes`
  (`5`, `15`, `30`, `60`) and its y-axis is the average point-horizon viewer
  count minus the average viewer count in the five minutes before the raid.
  The headline card averages the same changes across every horizon and raid in
  filter context.
- **Raid-source finding:** Source category, title, language, and tags come from
  a Get Channel Information request for the source broadcaster issued after the
  incoming raid is stored. They are observations near the raid, not fields in
  the raid EventSub payload. The same values repeat once per configured horizon.
- The September 17 stream started on September 17 in UTC, Amsterdam, and
  Chicago, so the current slicer ambiguity does not change its selected date.
- **“Messages and Peak Active Chatters” semantic finding:** For each five-minute
  elapsed-stream period, `Messages` counts captured local-channel messages and
  `Peak Active Chatters` is effectively the number of distinct people who sent
  at least one of those messages. There is no smaller-grain peak calculation
  within a five-minute period; “peak” comes from a `MAX` DAX aggregation and is
  misleading when the chart is already grouped by period. Shared-chat messages
  are excluded from both local participation measures. Unsupported chat periods
  are blank rather than zero. When multiple streams are selected, messages are
  summed at the same elapsed period while active chatters take the maximum from
  one stream, so the combined comparison is not a meaningful cross-stream
  aggregation.
- **`window_index` axis finding:** The x-axis is a zero-based sequence of
  five-minute periods from the Twitch-reported stream start, not minutes, clock
  time, or a percentage. Index 0 represents elapsed 00:00–00:05; index 100
  represents 08:20–08:25. The prior stream lasted about 8 hours 29 minutes and
  contains indices 0–101; Power BI labels the major tick at 100 while the final
  partial period runs from 08:25 to approximately 08:29. Replace this model key
  with a plainly formatted elapsed-stream-time axis in the redesign.
- **“Average Presence” semantic finding:** For one selected stream and one
  five-minute period, the plotted value is not an average. It is Twitch's
  reported total number of accounts connected to the channel's chat from the
  latest complete presence snapshot available near the end of that period. The
  snapshot must be no more than seven minutes old; otherwise the value is
  blank. The `AVERAGE` DAX aggregation only matters when filter context contains
  multiple rows, including the currently permitted multi-stream case. Chat
  presence is not video viewership, may include inactive connections, and may
  lag joins and leaves. Its useful analytical role is comparison with people
  actively messaging, not a proxy for viewers or person-level retention. Per
  REC-005, remove the standalone plot from the artist-facing report; retain the
  underlying snapshots only for a specifically justified comparison.
- **“Average Top Five Share” semantic finding:** In each supported, non-empty
  five-minute period, rank active chatters by their message counts and divide
  the messages sent by the five highest-volume chatters by all local-channel
  messages in that period. The five chatters are recalculated for every period.
  With one selected stream, each plotted point is that period's concentration
  ratio, not an average. A period with five or fewer active chatters is
  automatically 100%, making quiet periods structurally look highly
  concentrated. The report-level mean is an unweighted average of period-level
  ratios, so a low-volume period influences it as much as a 730-message period.
  On September 17 the 730-message burst had 17 active chatters and an 87.4%
  top-five share, meaning the five most active chatters produced 638 messages.
  This can distinguish broad participation from activity dominated by a few
  people, but the current technical label and standalone line plot do not make
  that question clear. There is no documented empirical or POLYMATHIC-specific
  rationale for choosing five. The milestone design called it an “initial”
  choice; it is a consistently implemented AI-selected heuristic, not a
  validated business threshold. Reconsider the metric rather than preserving
  five merely because the current model uses it.
- **Briefing consistency issue:** The generated sentence “Peak observed five-
  minute activity was 730 messages from 18 active chatters” combines two
  independent maxima. The 730-message period had 17 active chatters; 18 was the
  maximum in a different period. The later implementation must not present
  independently aggregated maxima as if they describe the same period.
- **Community-card semantic finding:** `First Observed`, `Returning`, and
  `Recurring` are derived from locally collected eligible chat messages rather
  than Twitch's channel-history classifications. First observed searches the
  available local history; returning and recurring use a 180-day lookback.
  This makes the current values measures of a young dataset as much as measures
  of the community, so they are unsuitable as artist-facing headline results
  for the near-term deliverable.
- **Twitch chatter-classification finding:** Twitch IRC message metadata can
  include `first-msg` and `returning-chatter`. EventSub chat messages document
  `user_intro` as a possible `message_type` but do not expose the two IRC
  Boolean fields, and no documented historical lookup endpoint replaces live
  capture. September 17 contained no `user_intro` rows, so it cannot produce a
  Twitch-identified first-time-chatter result. A future controlled live check
  is required before treating `user_intro` or another captured field as
  equivalent to the moderator-interface classification.
- **Viewer-difference plot semantic finding:** The plot is independent of the
  community-classification cards. For each elapsed five-minute window, the
  selected stream's time-weighted average viewers are compared with the
  arithmetic mean for the same window across (1) all earlier streams and (2)
  earlier streams beginning on the same Amsterdam-local weekday. The lines are
  viewer-count deviations from those means, not percentages, changes from the
  previous five minutes, or comparisons with one immediately prior stream.
- **Historical sample-size finding:** The number of contributing earlier
  streams can decrease later in the elapsed timeline as shorter streams end.
  The current cards use the maximum window-level sample in filter context and
  therefore do not communicate that changing evidence base. With the project's
  short history, especially within the same-weekday subset, these baselines are
  not mature enough for an artist-facing performance claim.
- **Historical-comparison table finding:** The adjacent table exposes the raw
  stream × elapsed-five-minute-window dataset behind the removed comparison
  plot: internal `window_index`, current viewer level, two historical averages,
  and their repeated sample sizes. It adds no independent answer for
  POLYMATHIC. The integer index is valid as an internal alignment key but is not
  a meaningful display unit; remove the table instead of merely formatting the
  index as time.

## Open decisions

- Decide whether date selection is sufficient or whether the report should also
  expose a unique, human-readable stream selector.
- Define the smallest set of artist-facing headline questions and metrics that
  should appear immediately after a stream.
- Decide whether engineering and collection diagnostics remain in the PBIP as a
  clearly separated owner-only page or live outside the main report entirely.
- Decide whether chat concentration belongs in the artist-facing story and, if
  so, replace the arbitrary per-period top-five threshold with a measure whose
  meaning remains useful across different participation sizes. Candidates
  include the number or share of chatters required to produce half of messages,
  or a whole-stream “most active 10%” share accompanied by participant and
  message counts. Avoid a technical concentration index unless it can be
  translated into an immediately understandable statement.

## Implementation log

Review completed September 18, 2026 across Stream evolution, Raid impact, and
Community and comparison. Documentation was reconciled after the review. No
report, semantic-model, SQL, collector, or workflow recommendations in this
file were implemented during the review session.
