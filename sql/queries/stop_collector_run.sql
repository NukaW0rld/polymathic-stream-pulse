UPDATE collector_runs
SET stopped_at = %(stopped_at)s
WHERE run_id = %(run_id)s
  AND stopped_at IS NULL
  AND last_heartbeat_at <= %(stopped_at)s;
