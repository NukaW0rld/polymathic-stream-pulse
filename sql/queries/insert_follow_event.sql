INSERT INTO follow_events (
	eventsub_message_id,
	stream_id,
	user_id,
	followed_at,
	notification_at,
	received_at,
	run_id
) VALUES (
	%(eventsub_message_id)s,
	%(stream_id)s,
	%(user_id)s,
	%(followed_at)s,
	%(notification_at)s,
	%(received_at)s,
	%(run_id)s
)
ON CONFLICT (eventsub_message_id)
DO NOTHING;
