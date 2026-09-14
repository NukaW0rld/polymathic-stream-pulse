SELECT notification_at, raid_viewer_count, from_broadcaster_user_id FROM incoming_raids
WHERE notification_at BETWEEN
	(SELECT first_observed_at FROM streams WHERE stream_id = :'stream_id')
	AND
	(SELECT offline_observed_at FROM streams WHERE stream_id = :'stream_id')
ORDER BY notification_at ASC;
