INSERT INTO viewer_snapshots (stream_id, observed_at, viewer_count)
VALUES (
	%(stream_id)s,
	%(observed_at)s,
	%(viewer_count)s
)
ON CONFLICT (stream_id, observed_at)
DO NOTHING;
