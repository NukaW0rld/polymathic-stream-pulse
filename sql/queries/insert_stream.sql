INSERT INTO streams (
	stream_id,
	started_at,
	first_observed_at
) VALUES (
	%(stream_id)s,
	%(started_at)s,
	%(first_observed_at)s
)
ON CONFLICT (stream_id)
DO NOTHING;
