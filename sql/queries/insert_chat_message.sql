INSERT INTO chat_messages (
	eventsub_message_id,
	stream_id,
	chatter_user_id,
	chat_message_id,
	message_text,
	message_fragments,
	notification_at,
	received_at,
	source_broadcaster_user_id
) VALUES (
	%(eventsub_message_id)s,
	%(stream_id)s,
	%(chatter_user_id)s,
	%(chat_message_id)s,
	%(message_text)s,
	%(message_fragments)s,
	%(notification_at)s,
	%(received_at)s,
	%(source_broadcaster_user_id)s
)
ON CONFLICT (eventsub_message_id) DO NOTHING;
