SELECT run_id, started_at, last_heartbeat_at, stopped_at
FROM collector_runs
ORDER BY started_at DESC, run_id DESC
LIMIT 1;
