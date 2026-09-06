INSERT INTO collector_runs (
	started_at,
	last_heartbeat_at
) VALUES (
	%(started_at)s,
	%(started_at)s
)
RETURNING run_id;
