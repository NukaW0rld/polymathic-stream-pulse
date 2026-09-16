# Milestone 2 pre-stream release handoff

Prepared September 16, 2026. This handoff covers implementation readiness for
the September 17 official run. It is not the final live-acceptance record.

## Released locally

- A validated, owner-only PostgreSQL backup was created outside the repository.
- Migrations 016–017 were applied successfully. They add the execution-manifest
  table and curated analytical views without changing raw observations.
- The post-stream command supports explicit assessment, packet preparation,
  refresh/export recording, and owner approval/rejection.
- Input fingerprints plus the analysis version provide idempotent reuse;
  per-stream PostgreSQL advisory locks prevent concurrent preparation.
- The semantic model now imports only curated dimensions and facts. Raw chat
  text, raw participant IDs, and raw raid-source IDs are absent.
- The PBIR report contains Stream evolution, Raid impact, and Community and
  comparison pages. Page and visual JSON validates against Microsoft's
  published schemas.
- The project opens successfully in Power BI Desktop, and the owner accepted
  the pre-stream page layouts on September 16.

## Evidence

- The full suite passes 348 tests with PostgreSQL integration enabled.
- Hand-verifiable fixtures cover time-weighted viewers, chat coverage gaps,
  concentration, presence alignment, event boundaries, raid overlap/truncation,
  historical baselines, return/recurrence, and fan-out prevention.
- Quality tests cover clean, multi-run, late/early, open-run, missing closure,
  restart gap, disabled source, failed/partial source, historical capability,
  incomplete presence, quiet source, and ambiguous association behavior.
- A closed September 13 historical stream prepared as publishable with warnings.
  Repeating the command reused the same analysis and artifact identity.
- The September 15 stream remained blocked because its associated run is open;
  the command created diagnostics only and did not weaken the gate.

## Required September 17 acceptance

1. Complete the full Milestone 1 collector run through successful offline
   observation and orderly shutdown.
2. Run `assess`, then `prepare`, using the Amsterdam date and preserve the
   analysis ID.
3. Confirm enhanced presence, context, metadata, and raid enrichment appear as
   collected rather than historical unavailable fields.
4. Refresh in Power BI Desktop, inspect all three pages, and reconcile visible
   values to the generated briefing/SQL.
5. Export privately, record the current refresh artifact, complete owner review,
   and record closure-to-ready timestamps.

## Known limits

- Power BI Desktop remains a Windows-only operational step. The pre-stream
  project/page inspection is complete, but metadata validation and that check do
  not replace the required refresh and value reconciliation on official data.
- The first enhanced live stream may provide too little history for meaningful
  weekday or raid-source comparisons. Sample sizes remain visible and no strong
  conclusion is generated automatically.
- Reconnection detection time does not bound the true outage start.
- Automatic delivery, Power BI Service/gateway operation, NLP, prediction, and
  subjective annotations remain excluded.
