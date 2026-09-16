INSERT INTO chatter_presence_snapshots (
	run_id, stream_id, requested_at, status, reason_code
) VALUES (
	%(run_id)s, %(stream_id)s, %(requested_at)s, 'in_progress', 'request_started'
)
RETURNING snapshot_id;
