SELECT followed_at, user_id FROM follow_events
WHERE followed_at BETWEEN
	(SELECT first_observed_at FROM streams WHERE stream_id = :'stream_id')
	AND
	(SELECT offline_observed_at FROM streams WHERE stream_id = :'stream_id')
ORDER BY followed_at ASC;
