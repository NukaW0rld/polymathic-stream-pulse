INSERT INTO viewer_snapshots (stream_id, observed_at, viewer_count, run_id)
VALUES (
	%(stream_id)s,
	%(observed_at)s,
	%(viewer_count)s,
	%(run_id)s
)
ON CONFLICT (stream_id, observed_at)
DO NOTHING;
