-- Privacy-safe dry run for historical run/stream attribution.
-- A snapshot is eligible only when its observed_at falls inside exactly one
-- durable run interval [started_at, last_heartbeat_at]. Open runs are not
-- treated as closed; their last heartbeat is merely the evidence boundary.
WITH classified AS (
	SELECT v.stream_id, v.observed_at, count(r.run_id) AS matching_run_count
	FROM viewer_snapshots v
	LEFT JOIN collector_runs r
	  ON v.observed_at BETWEEN r.started_at AND r.last_heartbeat_at
	WHERE v.run_id IS NULL
	GROUP BY v.stream_id, v.observed_at
)
SELECT
	count(*) FILTER (WHERE matching_run_count = 0) AS unmatched_snapshots,
	count(*) FILTER (WHERE matching_run_count = 1) AS uniquely_attributable_snapshots,
	count(*) FILTER (WHERE matching_run_count > 1) AS ambiguous_snapshots,
	count(DISTINCT stream_id) FILTER (WHERE matching_run_count = 1) AS uniquely_supported_streams
FROM classified;
