BEGIN;

CREATE TABLE post_stream_analysis_runs (
	analysis_id TEXT PRIMARY KEY,
	stream_id TEXT NOT NULL REFERENCES streams (stream_id),
	analysis_version TEXT NOT NULL,
	input_fingerprint TEXT NOT NULL,
	quality_state TEXT NOT NULL CHECK (
		quality_state IN ('publishable', 'publishable_with_warnings', 'blocked')
	),
	quality_reasons JSONB NOT NULL,
	workflow_status TEXT NOT NULL CHECK (
		workflow_status IN (
			'blocked', 'refresh_pending', 'review_pending', 'approved',
			'rejected', 'failed'
		)
	),
	started_at TIMESTAMPTZ NOT NULL,
	completed_at TIMESTAMPTZ,
	artifact_path TEXT,
	refresh_artifact_path TEXT,
	refreshed_at TIMESTAMPTZ,
	reviewed_at TIMESTAMPTZ,
	UNIQUE (stream_id, analysis_version, input_fingerprint),
	CHECK (completed_at IS NULL OR completed_at >= started_at),
	CHECK (refreshed_at IS NULL OR workflow_status IN ('review_pending', 'approved', 'rejected')),
	CHECK (reviewed_at IS NULL OR workflow_status IN ('approved', 'rejected'))
);

COMMENT ON TABLE post_stream_analysis_runs IS
'Idempotent post-stream execution manifest. Raw observations remain unchanged.';

CREATE VIEW analytics_event_associations AS
WITH events AS (
	SELECT 'follow'::TEXT AS event_type,
		eventsub_message_id AS event_id,
		stream_id AS captured_stream_id,
		followed_at AS event_at,
		notification_at,
		received_at,
		run_id
	FROM follow_events
	UNION ALL
	SELECT 'raid'::TEXT,
		eventsub_message_id,
		stream_id,
		notification_at,
		notification_at,
		received_at,
		run_id
	FROM incoming_raids
), candidates AS (
	SELECT e.event_type, e.event_id, e.captured_stream_id, e.event_at,
		e.notification_at, e.received_at, e.run_id,
		count(s.stream_id) AS candidate_count,
		min(s.stream_id) AS sole_candidate_stream_id
	FROM events e
	LEFT JOIN streams s
	  ON e.event_at >= s.started_at
	 AND e.event_at < s.offline_observed_at
	GROUP BY e.event_type, e.event_id, e.captured_stream_id, e.event_at,
		e.notification_at, e.received_at, e.run_id
)
SELECT event_type, event_id, captured_stream_id, event_at, notification_at,
	received_at, run_id, candidate_count,
	CASE
		WHEN candidate_count = 1
		 AND (captured_stream_id IS NULL OR captured_stream_id = sole_candidate_stream_id)
		THEN sole_candidate_stream_id
		ELSE NULL
	END AS associated_stream_id,
	CASE
		WHEN candidate_count = 0 THEN 'unresolved'
		WHEN candidate_count > 1 THEN 'ambiguous'
		WHEN captured_stream_id IS NOT NULL
		 AND captured_stream_id <> sole_candidate_stream_id THEN 'conflict'
		ELSE 'resolved'
	END AS association_status,
	CASE event_type
		WHEN 'follow' THEN 'followed_at'
		ELSE 'eventsub_envelope_time'
	END AS timestamp_basis
FROM candidates;

COMMENT ON VIEW analytics_event_associations IS
'Deterministic half-open stream association. Follow uses followed_at; raid uses EventSub envelope time. Receipt time is provenance only.';

CREATE VIEW analytics_source_intervals AS
WITH ordered AS (
	SELECT crs.stream_id, h.run_id, h.source, h.observed_at AS interval_start,
		lead(h.observed_at) OVER (
			PARTITION BY h.run_id, h.source ORDER BY h.observed_at, h.health_id
		) AS next_observed_at,
		r.stopped_at, r.last_heartbeat_at, h.status, h.reason_code
	FROM collector_run_streams crs
	JOIN collector_runs r USING (run_id)
	JOIN collection_health h USING (run_id)
), bounded AS (
	SELECT o.stream_id, o.run_id, o.source,
		greatest(o.interval_start, s.started_at) AS interval_start,
		least(coalesce(o.next_observed_at, o.stopped_at, o.last_heartbeat_at),
			s.offline_observed_at) AS interval_end,
		o.status, o.reason_code,
		(o.next_observed_at IS NULL AND o.stopped_at IS NULL) AS uncertain_end
	FROM ordered o
	JOIN streams s USING (stream_id)
	WHERE s.offline_observed_at IS NOT NULL
)
SELECT stream_id, run_id, source, interval_start, interval_end, status,
	reason_code, uncertain_end
FROM bounded
WHERE interval_end > interval_start;

COMMENT ON VIEW analytics_source_intervals IS
'Conservative source-state intervals clipped to a closed stream. An open run ends only at its last durable heartbeat and is marked uncertain.';

CREATE VIEW analytics_healthy_source_intervals AS
WITH healthy AS (
	SELECT stream_id, source, interval_start, interval_end,
		max(interval_end) OVER (
			PARTITION BY stream_id, source ORDER BY interval_start, interval_end
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
		) AS prior_max_end
	FROM analytics_source_intervals
	WHERE status = 'healthy'
), islands AS (
	SELECT *, sum(CASE WHEN prior_max_end IS NULL OR prior_max_end < interval_start
		THEN 1 ELSE 0 END) OVER (
			PARTITION BY stream_id, source ORDER BY interval_start, interval_end
		) AS island_id
	FROM healthy
)
SELECT stream_id, source, min(interval_start) AS interval_start,
	max(interval_end) AS interval_end
FROM islands
GROUP BY stream_id, source, island_id;

COMMENT ON VIEW analytics_healthy_source_intervals IS
'Unioned healthy intervals. Overlapping collector runs never double-count coverage or conceal a later uncovered interval.';

CREATE VIEW analytics_stream_participation AS
WITH closed_streams AS (
	SELECT stream_id, started_at, offline_observed_at
	FROM streams
	WHERE offline_observed_at IS NOT NULL
), windows AS (
	SELECT s.stream_id, s.started_at, s.offline_observed_at,
		g.window_index,
		s.started_at + g.window_index * interval '5 minutes' AS window_start,
		least(s.started_at + (g.window_index + 1) * interval '5 minutes',
			s.offline_observed_at) AS window_end
	FROM closed_streams s
	CROSS JOIN LATERAL generate_series(
		0,
		greatest(0, ceil(extract(epoch FROM (s.offline_observed_at - s.started_at)) / 300.0)::INT - 1)
	) AS g(window_index)
), viewer_segments AS (
	SELECT v.stream_id, v.observed_at AS segment_start,
		least(
			coalesce(lead(v.observed_at) OVER (
				PARTITION BY v.stream_id ORDER BY v.observed_at
			), s.offline_observed_at),
			v.observed_at + interval '90 seconds',
			s.offline_observed_at
		) AS segment_end,
		v.viewer_count
	FROM viewer_snapshots v
	JOIN closed_streams s USING (stream_id)
	WHERE v.observed_at >= s.started_at AND v.observed_at < s.offline_observed_at
), viewer_window AS (
	SELECT w.stream_id, w.window_index,
		count(v.segment_start) AS viewer_observation_count,
		coalesce(sum(greatest(0, extract(epoch FROM
			least(v.segment_end, w.window_end) - greatest(v.segment_start, w.window_start)))), 0) AS viewer_covered_seconds,
		sum(v.viewer_count * greatest(0, extract(epoch FROM
			least(v.segment_end, w.window_end) - greatest(v.segment_start, w.window_start)))) AS viewer_weighted_sum
	FROM windows w
	LEFT JOIN viewer_segments v
	  ON v.stream_id = w.stream_id
	 AND v.segment_start < w.window_end
	 AND v.segment_end > w.window_start
	GROUP BY w.stream_id, w.window_index
), chat_window AS (
	SELECT w.stream_id, w.window_index,
		count(m.eventsub_message_id) FILTER (
			WHERE m.source_broadcaster_user_id IS NULL
		) AS observed_message_count,
		count(DISTINCT m.chatter_user_id) FILTER (
			WHERE m.source_broadcaster_user_id IS NULL
		) AS observed_active_chatter_count,
		count(m.eventsub_message_id) FILTER (
			WHERE m.source_broadcaster_user_id IS NOT NULL
		) AS shared_chat_message_count
	FROM windows w
	LEFT JOIN chat_messages m
	  ON m.stream_id = w.stream_id
	 AND m.notification_at >= w.window_start
	 AND m.notification_at < w.window_end
	GROUP BY w.stream_id, w.window_index
), chatter_counts AS (
	SELECT w.stream_id, w.window_index, m.chatter_user_id, count(*) AS message_count
	FROM windows w
	JOIN chat_messages m
	  ON m.stream_id = w.stream_id
	 AND m.notification_at >= w.window_start
	 AND m.notification_at < w.window_end
	 AND m.source_broadcaster_user_id IS NULL
	GROUP BY w.stream_id, w.window_index, m.chatter_user_id
), concentration AS (
	SELECT stream_id, window_index,
		sum(message_count) AS total_messages,
		sum(message_count) FILTER (WHERE chatter_rank <= 5) AS top_five_messages
	FROM (
		SELECT *, row_number() OVER (
			PARTITION BY stream_id, window_index
			ORDER BY message_count DESC, chatter_user_id
		) AS chatter_rank
		FROM chatter_counts
	) ranked
	GROUP BY stream_id, window_index
), chat_coverage AS (
	SELECT w.stream_id, w.window_index,
		least(extract(epoch FROM (w.window_end - w.window_start)),
			coalesce(sum(extract(epoch FROM
				least(i.interval_end, w.window_end) - greatest(i.interval_start, w.window_start))), 0)
		) AS healthy_seconds
	FROM windows w
	LEFT JOIN analytics_healthy_source_intervals i
	  ON i.stream_id = w.stream_id
	 AND i.source = 'chat'
	 AND i.interval_start < w.window_end
	 AND i.interval_end > w.window_start
	GROUP BY w.stream_id, w.window_index, w.window_start, w.window_end
), chosen_presence AS (
	SELECT w.stream_id, w.window_index, p.snapshot_id, p.completed_at,
		p.reported_total, p.collected_distinct_count
	FROM windows w
	LEFT JOIN LATERAL (
		SELECT p.*
		FROM chatter_presence_snapshots p
		WHERE p.stream_id = w.stream_id
		  AND p.status = 'complete'
		  AND p.completed_at <= w.window_end
		  AND p.completed_at > w.window_end - interval '7 minutes'
		  AND p.completed_at - p.requested_at <= interval '90 seconds'
		  AND p.collected_distinct_count = (
			SELECT count(*) FROM chatter_presence_members pm
			WHERE pm.snapshot_id = p.snapshot_id
		  )
		ORDER BY p.completed_at DESC, p.snapshot_id DESC
		LIMIT 1
	) p ON TRUE
), presence_active AS (
	SELECT cp.stream_id, cp.window_index,
		count(DISTINCT m.chatter_user_id) AS active_chatter_in_presence_count
	FROM chosen_presence cp
	JOIN chatter_presence_members pm ON pm.snapshot_id = cp.snapshot_id
	JOIN windows w ON w.stream_id = cp.stream_id AND w.window_index = cp.window_index
	JOIN chat_messages m
	  ON m.stream_id = cp.stream_id
	 AND m.chatter_user_id = pm.chatter_user_id
	 AND m.source_broadcaster_user_id IS NULL
	 AND m.notification_at >= w.window_start
	 AND m.notification_at < w.window_end
	GROUP BY cp.stream_id, cp.window_index
)
SELECT w.stream_id, w.window_index,
	w.window_start, w.window_end,
	extract(epoch FROM (w.window_end - w.window_start))::INT AS window_seconds,
	(v.viewer_covered_seconds >= greatest(0,
		extract(epoch FROM (w.window_end - w.window_start)) - 90)) AS viewer_available,
	v.viewer_observation_count,
	v.viewer_covered_seconds::INT,
	CASE WHEN v.viewer_covered_seconds >= greatest(0,
		extract(epoch FROM (w.window_end - w.window_start)) - 90)
		AND v.viewer_covered_seconds > 0
		THEN round((v.viewer_weighted_sum / v.viewer_covered_seconds)::NUMERIC, 2)
	END AS time_weighted_avg_viewers,
	(cc.healthy_seconds >= greatest(0,
		extract(epoch FROM (w.window_end - w.window_start)) - 30)) AS chat_available,
	cc.healthy_seconds::INT AS chat_healthy_seconds,
	CASE WHEN cc.healthy_seconds >= greatest(0,
		extract(epoch FROM (w.window_end - w.window_start)) - 30)
		THEN cw.observed_message_count
	END AS message_count,
	CASE WHEN cc.healthy_seconds >= greatest(0,
		extract(epoch FROM (w.window_end - w.window_start)) - 30)
		THEN cw.observed_active_chatter_count
	END AS active_chatter_count,
	CASE WHEN cc.healthy_seconds >= greatest(0,
		extract(epoch FROM (w.window_end - w.window_start)) - 30)
		AND coalesce(c.total_messages, 0) > 0
		THEN round(c.top_five_messages::NUMERIC / c.total_messages, 4)
	END AS top_five_message_share,
	cw.shared_chat_message_count,
	(cp.snapshot_id IS NOT NULL) AS presence_available,
	cp.completed_at AS presence_observed_at,
	cp.reported_total AS twitch_reported_presence,
	cp.collected_distinct_count AS collected_presence_count,
	pa.active_chatter_in_presence_count,
	CASE WHEN cp.snapshot_id IS NOT NULL
		AND cw.observed_active_chatter_count > 0
		THEN round(coalesce(pa.active_chatter_in_presence_count, 0)::NUMERIC
			/ cw.observed_active_chatter_count, 4)
	END AS active_chatter_presence_alignment
FROM windows w
JOIN viewer_window v USING (stream_id, window_index)
JOIN chat_window cw USING (stream_id, window_index)
JOIN chat_coverage cc USING (stream_id, window_index)
LEFT JOIN concentration c USING (stream_id, window_index)
LEFT JOIN chosen_presence cp USING (stream_id, window_index)
LEFT JOIN presence_active pa USING (stream_id, window_index);

COMMENT ON VIEW analytics_stream_participation IS
'One stream x five-minute elapsed window. Local-channel messages exclude shared-chat rows; shared activity remains separately counted. NULL means unavailable, not zero.';

CREATE VIEW analytics_raid_impact AS
WITH raids AS (
	SELECT a.event_id, a.associated_stream_id AS stream_id, a.event_at,
		r.raid_viewer_count, r.from_broadcaster_user_id,
		c.status AS context_status, c.category_name, c.title, c.language, c.tags
	FROM analytics_event_associations a
	JOIN incoming_raids r ON r.eventsub_message_id = a.event_id
	LEFT JOIN raid_source_context c ON c.eventsub_message_id = a.event_id
	WHERE a.event_type = 'raid' AND a.association_status = 'resolved'
), horizons AS (
	SELECT r.*, h.horizon_minutes,
		r.event_at + h.horizon_minutes * interval '1 minute' AS horizon_at
	FROM raids r
	CROSS JOIN (VALUES (5), (15), (30), (60)) AS h(horizon_minutes)
), viewer_baseline AS (
	SELECT h.event_id, h.horizon_minutes, b.sample_count, b.avg_viewers,
		b.first_sample_at, b.last_sample_at, b.max_sample_gap
	FROM horizons h
	LEFT JOIN LATERAL (
		SELECT count(*) AS sample_count, avg(viewer_count)::NUMERIC AS avg_viewers,
			min(observed_at) AS first_sample_at, max(observed_at) AS last_sample_at,
			max(observed_at - prior_observed_at) AS max_sample_gap
		FROM (
			SELECT v.*, lag(v.observed_at) OVER (ORDER BY v.observed_at) AS prior_observed_at
			FROM viewer_snapshots v
			WHERE v.stream_id = h.stream_id
			  AND v.observed_at >= h.event_at - interval '5 minutes'
			  AND v.observed_at < h.event_at
		) samples
	) b ON TRUE
), viewer_point AS (
	SELECT h.event_id, h.horizon_minutes, p.viewer_count, p.observed_at
	FROM horizons h
	LEFT JOIN LATERAL (
		SELECT v.viewer_count, v.observed_at
		FROM viewer_snapshots v
		WHERE v.stream_id = h.stream_id
		  AND v.observed_at BETWEEN h.horizon_at - interval '90 seconds'
			AND h.horizon_at + interval '90 seconds'
		ORDER BY abs(extract(epoch FROM (v.observed_at - h.horizon_at))), v.observed_at
		LIMIT 1
	) p ON TRUE
), chat_counts AS (
	SELECT h.event_id, h.horizon_minutes,
		count(m.eventsub_message_id) FILTER (
			WHERE m.source_broadcaster_user_id IS NULL
		) AS message_count,
		count(DISTINCT m.chatter_user_id) FILTER (
			WHERE m.source_broadcaster_user_id IS NULL
		) AS active_chatter_count
	FROM horizons h
	LEFT JOIN chat_messages m
	  ON m.stream_id = h.stream_id
	 AND m.notification_at >= h.event_at
	 AND m.notification_at < h.horizon_at
	GROUP BY h.event_id, h.horizon_minutes
), follow_counts AS (
	SELECT h.event_id, h.horizon_minutes, count(f.event_id) AS follow_count
	FROM horizons h
	LEFT JOIN analytics_event_associations f
	  ON f.event_type = 'follow'
	 AND f.association_status = 'resolved'
	 AND f.associated_stream_id = h.stream_id
	 AND f.event_at >= h.event_at
	 AND f.event_at < h.horizon_at
	GROUP BY h.event_id, h.horizon_minutes
), chat_coverage AS (
	SELECT h.event_id, h.horizon_minutes,
		least(h.horizon_minutes * 60.0,
			coalesce(sum(extract(epoch FROM
				least(i.interval_end, h.horizon_at) - greatest(i.interval_start, h.event_at))), 0)
		) AS chat_healthy_seconds
	FROM horizons h
	LEFT JOIN analytics_healthy_source_intervals i
	  ON i.stream_id = h.stream_id
	 AND i.source = 'chat'
	 AND i.interval_start < h.horizon_at
	 AND i.interval_end > h.event_at
	GROUP BY h.event_id, h.horizon_minutes
), follow_coverage AS (
	SELECT h.event_id, h.horizon_minutes,
		least(h.horizon_minutes * 60.0,
			coalesce(sum(extract(epoch FROM
				least(i.interval_end, h.horizon_at) - greatest(i.interval_start, h.event_at))), 0)
		) AS follow_healthy_seconds
	FROM horizons h
	LEFT JOIN analytics_healthy_source_intervals i
	  ON i.stream_id = h.stream_id
	 AND i.source = 'follows'
	 AND i.interval_start < h.horizon_at
	 AND i.interval_end > h.event_at
	GROUP BY h.event_id, h.horizon_minutes
)
SELECT h.event_id AS raid_id, h.stream_id, h.event_at AS raid_at,
	h.horizon_minutes, h.horizon_at, h.raid_viewer_count,
	md5('stream-pulse:raid-source:' || h.from_broadcaster_user_id) AS anonymous_raid_source_id,
	h.context_status, h.category_name AS source_category_name,
	h.title AS source_title, h.language AS source_language, h.tags AS source_tags,
	(b.sample_count >= 3
		AND b.first_sample_at <= h.event_at - interval '3 minutes 30 seconds'
		AND b.last_sample_at >= h.event_at - interval '90 seconds'
		AND b.max_sample_gap <= interval '90 seconds') AS viewer_baseline_available,
	CASE WHEN b.sample_count >= 3
		AND b.first_sample_at <= h.event_at - interval '3 minutes 30 seconds'
		AND b.last_sample_at >= h.event_at - interval '90 seconds'
		AND b.max_sample_gap <= interval '90 seconds'
		THEN round(b.avg_viewers, 2) END AS baseline_avg_viewers,
	(vp.viewer_count IS NOT NULL AND h.horizon_at < s.offline_observed_at) AS viewer_horizon_available,
	vp.viewer_count AS horizon_viewer_count,
	CASE WHEN b.sample_count >= 3
		AND b.first_sample_at <= h.event_at - interval '3 minutes 30 seconds'
		AND b.last_sample_at >= h.event_at - interval '90 seconds'
		AND b.max_sample_gap <= interval '90 seconds'
		AND vp.viewer_count IS NOT NULL AND h.horizon_at < s.offline_observed_at
		THEN round(vp.viewer_count - b.avg_viewers, 2)
	END AS viewer_change_from_baseline,
	(c.chat_healthy_seconds >= h.horizon_minutes * 60 - 30
		AND h.horizon_at <= s.offline_observed_at) AS participation_horizon_available,
	CASE WHEN c.chat_healthy_seconds >= h.horizon_minutes * 60 - 30
		AND h.horizon_at <= s.offline_observed_at THEN e.message_count END AS cumulative_messages,
	CASE WHEN c.chat_healthy_seconds >= h.horizon_minutes * 60 - 30
		AND h.horizon_at <= s.offline_observed_at THEN e.active_chatter_count END AS cumulative_active_chatters,
	(fc.follow_healthy_seconds >= h.horizon_minutes * 60 - 30
		AND h.horizon_at <= s.offline_observed_at) AS follow_horizon_available,
	CASE WHEN fc.follow_healthy_seconds >= h.horizon_minutes * 60 - 30
		AND h.horizon_at <= s.offline_observed_at THEN f.follow_count END AS observed_follow_events,
	EXISTS (
		SELECT 1 FROM raids other
		WHERE other.stream_id = h.stream_id AND other.event_id <> h.event_id
		  AND other.event_at >= h.event_at - interval '5 minutes'
		  AND other.event_at < h.horizon_at
	) AS overlapping_raid,
	(h.horizon_at > s.offline_observed_at) AS stream_end_truncated
FROM horizons h
JOIN streams s ON s.stream_id = h.stream_id
JOIN viewer_baseline b USING (event_id, horizon_minutes)
JOIN viewer_point vp USING (event_id, horizon_minutes)
JOIN chat_counts e USING (event_id, horizon_minutes)
JOIN follow_counts f USING (event_id, horizon_minutes)
JOIN chat_coverage c USING (event_id, horizon_minutes)
JOIN follow_coverage fc USING (event_id, horizon_minutes);

COMMENT ON VIEW analytics_raid_impact IS
'One resolved raid x +5/+15/+30/+60 minute horizon. Changes are observed associations, not individual retention or causation.';

CREATE VIEW analytics_chatter_participation AS
WITH local_messages AS (
	SELECT stream_id, chatter_user_id, notification_at
	FROM chat_messages
	WHERE source_broadcaster_user_id IS NULL
), per_stream AS (
	SELECT stream_id, chatter_user_id, min(notification_at) AS first_message_at,
		max(notification_at) AS last_message_at, count(*) AS message_count
	FROM local_messages
	GROUP BY stream_id, chatter_user_id
), history AS (
	SELECT ps.*,
		(SELECT min(lm.notification_at) FROM local_messages lm
		 WHERE lm.chatter_user_id = ps.chatter_user_id) AS first_observed_at,
		(SELECT count(DISTINCT prior.stream_id) FROM per_stream prior
		 JOIN streams prior_stream ON prior_stream.stream_id = prior.stream_id
		 JOIN streams current_stream ON current_stream.stream_id = ps.stream_id
		 WHERE prior.chatter_user_id = ps.chatter_user_id
		   AND prior_stream.started_at < current_stream.started_at
		   AND prior_stream.started_at >= current_stream.started_at - interval '180 days'
		) AS prior_streams_180d,
		EXISTS (
			SELECT 1
			FROM chatter_presence_members pm
			JOIN chatter_presence_snapshots snap USING (snapshot_id)
			JOIN streams prior_stream ON prior_stream.stream_id = snap.stream_id
			JOIN streams current_stream ON current_stream.stream_id = ps.stream_id
			WHERE pm.chatter_user_id = ps.chatter_user_id
			  AND snap.status = 'complete'
			  AND prior_stream.started_at < current_stream.started_at
		) AS observed_in_prior_presence
	FROM per_stream ps
)
SELECT h.stream_id,
	md5('stream-pulse:chatter:' || h.chatter_user_id) AS anonymous_chatter_id,
	h.first_message_at, h.last_message_at, h.message_count,
	h.first_observed_at,
	(h.first_message_at = h.first_observed_at) AS first_observed_chatter,
	(h.prior_streams_180d >= 1) AS returning_chatter,
	(h.prior_streams_180d >= 2) AS recurring_chatter,
	h.prior_streams_180d,
	h.observed_in_prior_presence,
	(SELECT min(notification_at) FROM chat_messages
	 WHERE source_broadcaster_user_id IS NULL) AS active_history_begins_at,
	(SELECT min(completed_at) FROM chatter_presence_snapshots
	 WHERE status = 'complete') AS presence_history_begins_at
FROM history h;

COMMENT ON VIEW analytics_chatter_participation IS
'One stream x active local-channel chatter. Returning means at least one prior stream; recurring means at least two prior streams in the 180-day lookback.';

CREATE VIEW analytics_historical_comparison AS
SELECT p.*,
	to_char(s.started_at AT TIME ZONE 'Europe/Amsterdam', 'FMDay') AS stream_weekday_amsterdam,
	base.all_stream_sample_size,
	base.all_stream_avg_viewers,
	base.all_stream_avg_messages,
	weekday.weekday_sample_size,
	weekday.weekday_avg_viewers,
	weekday.weekday_avg_messages
FROM analytics_stream_participation p
JOIN streams s USING (stream_id)
LEFT JOIN LATERAL (
	SELECT count(DISTINCT other.stream_id) AS all_stream_sample_size,
		round(avg(other.time_weighted_avg_viewers), 2) AS all_stream_avg_viewers,
		round(avg(other.message_count), 2) AS all_stream_avg_messages
	FROM analytics_stream_participation other
	JOIN streams os USING (stream_id)
	WHERE other.stream_id <> p.stream_id
	  AND other.window_index = p.window_index
	  AND os.started_at < s.started_at
	) base ON TRUE
LEFT JOIN LATERAL (
	SELECT count(DISTINCT other.stream_id) AS weekday_sample_size,
		round(avg(other.time_weighted_avg_viewers), 2) AS weekday_avg_viewers,
		round(avg(other.message_count), 2) AS weekday_avg_messages
	FROM analytics_stream_participation other
	JOIN streams os USING (stream_id)
	WHERE other.stream_id <> p.stream_id
	  AND other.window_index = p.window_index
	  AND os.started_at < s.started_at
	  AND extract(isodow FROM os.started_at AT TIME ZONE 'Europe/Amsterdam') =
		extract(isodow FROM s.started_at AT TIME ZONE 'Europe/Amsterdam')
	) weekday ON TRUE;

COMMENT ON VIEW analytics_historical_comparison IS
'Stream x elapsed five-minute window with prior-stream baselines. Weekday is based on stream start in Europe/Amsterdam; sample sizes must accompany comparisons.';

CREATE VIEW analytics_quality_source AS
WITH sources(source) AS (
	VALUES ('stream_poll'::TEXT), ('chat'), ('raids'), ('follows'), ('chatter_presence')
), stream_runs AS (
	SELECT crs.stream_id, r.run_id, r.started_at, r.last_heartbeat_at, r.stopped_at,
		crs.first_successful_observed_at, crs.last_successful_observed_at,
		crs.attribution_method
	FROM collector_run_streams crs
	JOIN collector_runs r USING (run_id)
), capabilities AS (
	SELECT sr.stream_id, sr.run_id, src.source,
		coalesce(c.status,
			CASE WHEN EXISTS (
				SELECT 1 FROM collection_health h
				WHERE h.run_id = sr.run_id AND h.source = src.source
			) THEN 'configured' ELSE 'not_recorded' END
		) AS capability_status
	FROM stream_runs sr
	CROSS JOIN sources src
	LEFT JOIN collector_run_capabilities c
	  ON c.run_id = sr.run_id AND c.capability = src.source
)
SELECT c.stream_id, c.source,
	count(DISTINCT c.run_id) AS associated_run_count,
	count(DISTINCT c.run_id) FILTER (WHERE c.capability_status = 'configured') AS configured_run_count,
	bool_and(sr.stopped_at IS NOT NULL) AS all_runs_closed,
	bool_or(c.capability_status = 'configured') AS configured_in_any_run,
	bool_or(c.capability_status = 'disabled') AS disabled_in_any_run,
	bool_or(c.capability_status = 'failed_to_initialize') AS failed_in_any_run,
	bool_or(c.capability_status = 'not_recorded') AS capability_history_missing,
	count(DISTINCT h.health_id) FILTER (WHERE h.status = 'error') AS error_observation_count,
	count(DISTINCT h.health_id) FILTER (WHERE h.status = 'healthy') AS healthy_observation_count,
	count(DISTINCT h.health_id) FILTER (WHERE h.status = 'stopped' AND h.reason_code = 'orderly_shutdown') AS orderly_stop_count,
	count(DISTINCT g.gap_id) FILTER (
		WHERE g.recovered_at IS NULL AND c.source IN ('chat', 'raids', 'follows')
	) AS unresolved_gap_count,
	bool_or(sr.attribution_method = 'historical_unique_time_match') AS has_derived_attribution
FROM capabilities c
JOIN stream_runs sr ON sr.stream_id = c.stream_id AND sr.run_id = c.run_id
LEFT JOIN collection_health h ON h.run_id = c.run_id AND h.source = c.source
LEFT JOIN reconnection_gaps g ON g.run_id = c.run_id
GROUP BY c.stream_id, c.source;

COMMENT ON VIEW analytics_quality_source IS
'One stream x source summary. Detailed conservative intervals are in analytics_source_intervals; the post-stream command owns final classification.';

CREATE VIEW analytics_stream_dimension AS
SELECT s.stream_id, s.started_at, s.offline_observed_at,
	(s.started_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS stream_local_date,
	to_char(s.started_at AT TIME ZONE 'Europe/Amsterdam', 'FMDay') AS stream_weekday_amsterdam,
	CASE WHEN s.offline_observed_at IS NOT NULL
		THEN round((extract(epoch FROM (s.offline_observed_at - s.started_at)) / 3600.0)::NUMERIC, 2)
	END AS duration_hours,
	latest.quality_state AS latest_quality_state,
	latest.workflow_status AS latest_workflow_status,
	latest.analysis_version AS latest_analysis_version
FROM streams s
LEFT JOIN LATERAL (
	SELECT p.quality_state, p.workflow_status, p.analysis_version
	FROM post_stream_analysis_runs p
	WHERE p.stream_id = s.stream_id
	ORDER BY p.started_at DESC, p.analysis_id DESC
	LIMIT 1
) latest ON TRUE;

CREATE VIEW analytics_date_dimension AS
SELECT d::DATE AS date,
	extract(year FROM d)::INT AS year,
	extract(month FROM d)::INT AS month_number,
	to_char(d, 'FMMonth') AS month_name,
	extract(isodow FROM d)::INT AS iso_weekday_number,
	to_char(d, 'FMDay') AS weekday_name
FROM generate_series(
	coalesce((SELECT min((started_at AT TIME ZONE 'Europe/Amsterdam')::DATE) FROM streams), current_date),
	coalesce((SELECT max((started_at AT TIME ZONE 'Europe/Amsterdam')::DATE) FROM streams), current_date),
	interval '1 day'
) d;

CREATE VIEW analytics_community_summary AS
SELECT stream_id,
	count(*) AS active_chatter_count,
	count(*) FILTER (WHERE first_observed_chatter) AS first_observed_chatter_count,
	count(*) FILTER (WHERE returning_chatter) AS returning_chatter_count,
	count(*) FILTER (WHERE recurring_chatter) AS recurring_chatter_count,
	min(active_history_begins_at) AS active_history_begins_at,
	min(presence_history_begins_at) AS presence_history_begins_at
FROM analytics_chatter_participation
GROUP BY stream_id;

COMMIT;
