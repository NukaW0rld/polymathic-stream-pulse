-- Privacy-safe post-run verification. Supply :rid with psql -v rid=<run_id>.
SELECT capability AS item, status, reason_code, observed_at, NULL::BIGINT AS row_count
FROM collector_run_capabilities
WHERE run_id = :rid
UNION ALL
SELECT 'chatter_presence', status, reason_code, max(completed_at), count(*)
FROM chatter_presence_snapshots
WHERE run_id = :rid
GROUP BY status, reason_code
UNION ALL
SELECT 'raid_source_context', context.status, context.reason_code,
	max(context.observed_at), count(*)
FROM raid_source_context context
WHERE context.run_id = :rid
GROUP BY context.status, context.reason_code
UNION ALL
SELECT 'stream_metadata_history', 'observed', 'version_saved', max(observed_at), count(*)
FROM stream_metadata_history
WHERE run_id = :rid
HAVING count(*) > 0
ORDER BY item, observed_at NULLS FIRST;
