INSERT INTO collector_run_streams (
	run_id, stream_id, first_successful_observed_at, last_successful_observed_at,
	attribution_method
) VALUES (
	%(run_id)s, %(stream_id)s, %(observed_at)s, %(observed_at)s, 'direct_observation'
)
ON CONFLICT (run_id, stream_id) DO UPDATE
SET first_successful_observed_at = LEAST(
	collector_run_streams.first_successful_observed_at,
	EXCLUDED.first_successful_observed_at
),
last_successful_observed_at = GREATEST(
	collector_run_streams.last_successful_observed_at,
	EXCLUDED.last_successful_observed_at
),
attribution_method = 'direct_observation';
