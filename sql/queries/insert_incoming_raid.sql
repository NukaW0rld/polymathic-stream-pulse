INSERT INTO incoming_raids (
	eventsub_message_id,
	stream_id,
	from_broadcaster_user_id,
	raid_viewer_count,
	notification_at,
	received_at
) VALUES (
	%(eventsub_message_id)s,
	%(stream_id)s,
	%(from_broadcaster_user_id)s,
	%(raid_viewer_count)s,
	%(notification_at)s,
	%(received_at)s
)
ON CONFLICT (eventsub_message_id) DO NOTHING;
