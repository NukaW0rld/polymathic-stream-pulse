INSERT INTO chat_messages (
	eventsub_message_id,
	stream_id,
	chatter_user_id,
	chat_message_id,
	message_text,
	message_fragments,
	notification_at,
	received_at,
	source_broadcaster_user_id,
	run_id,
	message_type,
	reply_parent_message_id,
	reply_parent_user_id,
	badges,
	context_complete
) VALUES (
	%(eventsub_message_id)s,
	%(stream_id)s,
	%(chatter_user_id)s,
	%(chat_message_id)s,
	%(message_text)s,
	%(message_fragments)s,
	%(notification_at)s,
	%(received_at)s,
	%(source_broadcaster_user_id)s,
	%(run_id)s,
	%(message_type)s,
	%(reply_parent_message_id)s,
	%(reply_parent_user_id)s,
	%(badges)s,
	%(context_complete)s
)
ON CONFLICT (eventsub_message_id) DO NOTHING;
