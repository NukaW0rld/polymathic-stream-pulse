UPDATE collector_runs
SET last_heartbeat_at = %(last_heartbeat_at)s
WHERE run_id = %(run_id)s
  AND stopped_at IS NULL
  AND last_heartbeat_at <= %(last_heartbeat_at)s;
