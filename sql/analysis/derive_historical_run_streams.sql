-- Idempotently add only bridge rows supported by snapshots that match exactly
-- one durable collector-run interval. This does not populate raw run_id fields:
-- those remain NULL so derived historical attribution cannot masquerade as
-- direct capture. Run inspect_historical_run_stream_derivation.sql first.
WITH matches AS (
	SELECT v.stream_id, v.observed_at, r.run_id
	FROM viewer_snapshots v
	JOIN collector_runs r
	  ON v.observed_at BETWEEN r.started_at AND r.last_heartbeat_at
	WHERE v.run_id IS NULL
), unique_snapshots AS (
	SELECT stream_id, observed_at, min(run_id) AS run_id
	FROM matches
	GROUP BY stream_id, observed_at
	HAVING count(*) = 1
), supported AS (
	SELECT run_id, stream_id, min(observed_at) AS first_observed_at,
		max(observed_at) AS last_observed_at
	FROM unique_snapshots
	GROUP BY run_id, stream_id
)
INSERT INTO collector_run_streams (
	run_id, stream_id, first_successful_observed_at,
	last_successful_observed_at, attribution_method
)
SELECT run_id, stream_id, first_observed_at, last_observed_at,
	'historical_unique_time_match'
FROM supported
ON CONFLICT (run_id, stream_id) DO NOTHING;
