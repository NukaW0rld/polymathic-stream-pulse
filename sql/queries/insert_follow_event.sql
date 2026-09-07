INSERT INTO follow_events (
	eventsub_message_id,
	stream_id,
	user_id,
	followed_at,
	notification_at,
	received_at
) VALUES (
	%(eventsub_message_id)s,
	%(stream_id)s,
	%(user_id)s,
	%(followed_at)s,
	%(notification_at)s,
	%(received_at)s
)
ON CONFLICT (eventsub_message_id)
DO NOTHING;
