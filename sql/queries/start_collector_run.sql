INSERT INTO collector_runs (
	started_at,
	last_heartbeat_at,
	collector_version
) VALUES (
	%(started_at)s,
	%(started_at)s,
	%(collector_version)s
)
RETURNING run_id;
