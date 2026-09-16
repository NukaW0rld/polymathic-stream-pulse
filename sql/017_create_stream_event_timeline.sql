BEGIN;

CREATE VIEW analytics_stream_events AS
WITH windows AS (
	SELECT stream_id, window_index, window_start, window_end,
		extract(epoch FROM (window_end - window_start)) AS window_seconds
	FROM analytics_stream_participation
), event_counts AS (
	SELECT w.stream_id, w.window_index,
		count(a.event_id) FILTER (WHERE a.event_type = 'raid') AS observed_raid_count,
		count(a.event_id) FILTER (WHERE a.event_type = 'follow') AS observed_follow_count
	FROM windows w
	LEFT JOIN analytics_event_associations a
	  ON a.association_status = 'resolved'
	 AND a.associated_stream_id = w.stream_id
	 AND a.event_at >= w.window_start
	 AND a.event_at < w.window_end
	GROUP BY w.stream_id, w.window_index
), source_coverage AS (
	SELECT w.stream_id, w.window_index, src.source,
		least(w.window_seconds,
			coalesce(sum(extract(epoch FROM
				least(i.interval_end, w.window_end) - greatest(i.interval_start, w.window_start))), 0)
		) AS healthy_seconds,
		w.window_seconds
	FROM windows w
	CROSS JOIN (VALUES ('raids'::TEXT), ('follows'::TEXT)) src(source)
	LEFT JOIN analytics_healthy_source_intervals i
	  ON i.stream_id = w.stream_id
	 AND i.source = src.source
	 AND i.interval_start < w.window_end
	 AND i.interval_end > w.window_start
	GROUP BY w.stream_id, w.window_index, src.source, w.window_start,
		w.window_end, w.window_seconds
), coverage AS (
	SELECT stream_id, window_index, max(window_seconds) AS window_seconds,
		max(healthy_seconds) FILTER (WHERE source = 'raids') AS raid_healthy_seconds,
		max(healthy_seconds) FILTER (WHERE source = 'follows') AS follow_healthy_seconds
	FROM source_coverage
	GROUP BY stream_id, window_index
)
SELECT w.stream_id, w.window_index, w.window_start,
	(c.raid_healthy_seconds >= greatest(0, c.window_seconds - 30)) AS raid_events_available,
	CASE WHEN c.raid_healthy_seconds >= greatest(0, c.window_seconds - 30)
		THEN e.observed_raid_count END AS raid_event_count,
	(c.follow_healthy_seconds >= greatest(0, c.window_seconds - 30)) AS follow_events_available,
	CASE WHEN c.follow_healthy_seconds >= greatest(0, c.window_seconds - 30)
		THEN e.observed_follow_count END AS follow_event_count
FROM windows w
JOIN event_counts e USING (stream_id, window_index)
JOIN coverage c USING (stream_id, window_index);

COMMENT ON VIEW analytics_stream_events IS
'One stream x elapsed five-minute window. Resolved raid/follow markers are NULL when their source lacks sufficient coverage and zero only when a healthy window was quiet.';

COMMIT;
